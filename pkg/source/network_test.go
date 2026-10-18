package source

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func networkTimeout() error { return &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded} }

func networkGet(ctx context.Context, client *http.Client, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err == nil {
		_, err = io.Copy(io.Discard, resp.Body)
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func requireNetworkTerminal(t *testing.T, err, cause error, attempted bool) {
	t.Helper()
	terminal, ok := errors.AsType[*cache.TerminalError](err)
	if !ok || terminal.Attempted != attempted || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("error=%v, terminal=%+v; want cause=%v attempted=%t", err, terminal, cause, attempted)
	}
}

func TestNetwork_AdmissionWave(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }))
			t.Cleanup(srv.Close)
			var n Network
			tr, _ := NewHTTPTransport(nil, nil)
			tr.Proxy = nil
			tr.MaxIdleConnsPerHost = concurrency
			var calls atomic.Int32
			entered := make(chan struct{}, concurrency)
			release := make([]chan struct{}, concurrency)
			results := make([]chan error, concurrency)
			causes := make([]error, concurrency)
			for i := range concurrency {
				release[i], results[i], causes[i] = make(chan struct{}), make(chan error, 1), networkTimeout()
			}
			tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				i := int(calls.Add(1)) - 1
				if i < concurrency {
					entered <- struct{}{}
					<-release[i]
					if i != 1 {
						return nil, causes[i]
					}
				}
				return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
			}
			client := &http.Client{Transport: n.HTTPTransport(tr)}
			t.Cleanup(tr.CloseIdleConnections)
			for i := range concurrency {
				go func() { results[i] <- networkGet(t.Context(), client, "http://wave.test") }()
				<-entered
			}
			close(release[0])
			requireNetworkTerminal(t, <-results[0], causes[0], true)
			for i := 2; i < concurrency; i++ {
				close(release[i])
				requireNetworkTerminal(t, <-results[i], causes[i], true)
			}
			close(release[1])
			if err := <-results[1]; err != nil {
				t.Fatalf("admitted success: %v", err)
			}
			for range concurrency {
				requireNetworkTerminal(t, networkGet(t.Context(), client, "http://wave.test"), causes[0], false)
			}
			if err := networkGet(t.Context(), client, "http://healthy.test"); err != nil {
				t.Fatalf("independent endpoint: %v", err)
			}
			if got := calls.Load(); got != int32(concurrency+1) {
				t.Fatalf("physical dials=%d, want %d", got, concurrency+1)
			}
		})
		t.Run(fmt.Sprintf("late-dial-%d", concurrency), func(t *testing.T) {
			t.Parallel()
			var n Network
			var calls atomic.Int32
			cause := networkTimeout()
			tr, _ := NewHTTPTransport(nil, nil)
			tr.Proxy = nil
			tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, cause
			}
			client := &http.Client{Transport: n.HTTPTransport(tr)}
			entered, release := make(chan struct{}, concurrency), make(chan struct{})
			done := make(chan error, concurrency)
			for range concurrency {
				go func() {
					ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{GetConn: func(string) { entered <- struct{}{}; <-release }})
					done <- networkGet(ctx, client, "http://late.test")
				}()
			}
			for range concurrency {
				<-entered
			}
			requireNetworkTerminal(t, networkGet(t.Context(), client, "http://late.test"), cause, true)
			close(release)
			for range concurrency {
				requireNetworkTerminal(t, <-done, cause, false)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("guarded dials=%d, want 1", got)
			}
		})
	}
}

func TestNetwork_RouteIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, first, second, proxy, nextProxy string
		suppressed                            bool
	}{
		{name: "hostname-default-port", first: "http://EXAMPLE.test", second: "http://example.test:80/next", suppressed: true},
		{name: "ascii-hostname", first: "http://WITH_UNDERSCORE.test", second: "http://with_underscore.test:80", suppressed: true},
		{name: "ipv6", first: "http://[0:0:0:0:0:0:0:1]", second: "http://[::1]:80", suppressed: true},
		{name: "idna", first: "http://bücher.test", second: "http://xn--bcher-kva.test:80", suppressed: true},
		{name: "port", first: "http://example.test:80", second: "http://example.test:81"},
		{name: "numeric-port", first: "http://example.test:080", second: "http://example.test:80", suppressed: true},
		{name: "ipv6-zone", first: "http://[fe80::1%25ETH0]", second: "http://[fe80::1%25eth0]"},
		{name: "forward-proxy", first: "http://origin.test", second: "http://other.test", proxy: "http://PROXY.test", nextProxy: "http://proxy.test:80", suppressed: true},
		{name: "connect-proxy", first: "https://origin.test", second: "https://other.test", proxy: "http://proxy.test", nextProxy: "http://proxy.test:80", suppressed: true},
		{name: "socks5", first: "http://origin.test", second: "http://other.test", proxy: "socks5://proxy.test", nextProxy: "socks5://proxy.test:1080", suppressed: true},
		{name: "socks5h", first: "https://origin.test", second: "https://other.test", proxy: "socks5h://proxy.test", nextProxy: "socks5h://proxy.test:1080", suppressed: true},
		{name: "direct-after-proxy", first: "http://origin.test", second: "http://proxy.test", proxy: "http://proxy.test"},
		{name: "proxy-after-direct", first: "http://proxy.test", second: "http://origin.test", nextProxy: "http://proxy.test"},
		{name: "proxy-scheme", first: "http://origin.test", second: "http://origin.test", proxy: "http://proxy.test:443", nextProxy: "https://proxy.test:443"},
		{name: "proxy-port", first: "http://origin.test", second: "http://origin.test", proxy: "http://proxy.test:80", nextProxy: "http://proxy.test:81"},
		{name: "proxy-username", first: "http://origin.test", second: "http://origin.test", proxy: "http://a:secret@proxy.test", nextProxy: "http://b:secret@proxy.test"},
		{name: "proxy-password", first: "http://origin.test", second: "http://origin.test", proxy: "http://a:first@proxy.test", nextProxy: "http://a:second@proxy.test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var n Network
			cause, observed := networkTimeout(), errors.New("healthy route attempted")
			calls, proxyCalls := 0, 0
			client := func(proxy string, failure error) *http.Client {
				tr, _ := NewHTTPTransport(nil, nil)
				tr.Proxy = func(*http.Request) (*url.URL, error) {
					proxyCalls++
					if proxy == "" {
						return nil, nil
					}
					if proxyCalls%2 == 0 {
						return url.Parse("http://wrong.test:9000")
					}
					return url.Parse(proxy)
				}
				tr.DialContext = func(_ context.Context, _, address string) (net.Conn, error) {
					calls++
					if address == "wrong.test:9000" {
						return nil, errors.New("Proxy evaluated twice")
					}
					return nil, failure
				}
				return &http.Client{Transport: n.HTTPTransport(tr)}
			}
			requireNetworkTerminal(t, networkGet(t.Context(), client(tc.proxy, cause), tc.first), cause, true)
			proxyCalls = 0
			err := networkGet(t.Context(), client(tc.nextProxy, observed), tc.second)
			want := 2
			if tc.suppressed {
				want = 1
				requireNetworkTerminal(t, err, cause, false)
			} else if !errors.Is(err, observed) {
				t.Fatalf("independent route: %v", err)
			}
			if calls != want || proxyCalls != 1 {
				t.Fatalf("dials=%d want=%d, proxy calls=%d want=1", calls, want, proxyCalls)
			}
		})
	}
}

func TestNetwork_AttemptLiveness(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/deadline-%t", concurrency, deadline), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					var n Network
					entered, release, finished := make(chan struct{}, concurrency), make(chan struct{}), make(chan struct{}, concurrency)
					calls := 0
					observed := errors.New("live attempt reached dial")
					tr, _ := NewHTTPTransport(nil, nil)
					tr.Proxy = nil
					tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
						calls++
						if calls > concurrency {
							return nil, observed
						}
						entered <- struct{}{}
						<-release
						if ctx.Err() != nil {
							t.Error("net/http did not detach the dial")
						}
						finished <- struct{}{}
						return nil, networkTimeout()
					}
					client := &http.Client{Transport: n.HTTPTransport(tr)}
					ctx, cancel := context.WithCancel(t.Context())
					if deadline {
						cancel()
						ctx, cancel = context.WithTimeout(t.Context(), time.Second)
					}
					defer cancel()
					done := make(chan error, concurrency)
					for range concurrency {
						go func() { done <- networkGet(ctx, client, "http://cancel.test") }()
						<-entered
					}
					if !deadline {
						cancel()
					}
					for range concurrency {
						err := <-done
						if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("original attempt: %v", err)
						}
					}
					close(release)
					for range concurrency {
						<-finished
					}
					synctest.Wait()
					if err := networkGet(t.Context(), client, "http://cancel.test"); !errors.Is(err, observed) {
						t.Fatalf("canceled attempt latched: %v", err)
					}
				})
			})
		}
	}
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"deadline", &net.OpError{Op: "dial", Err: context.DeadlineExceeded}},
		{"canceled", &net.OpError{Op: "dial", Err: context.Canceled}},
		{"canceled-timeout", &net.OpError{Op: "dial", Err: timeoutContextError{context.Canceled}}},
		{"read", &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}},
		{"write", &net.OpError{Op: "write", Err: os.ErrDeadlineExceeded}},
		{"untyped", os.ErrDeadlineExceeded},
		{"non-timeout", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}},
		{"tls", &tls.CertificateVerificationError{Err: errors.New("certificate rejected")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var n Network
			calls := 0
			tr, _ := NewHTTPTransport(nil, nil)
			tr.Proxy = nil
			tr.DialContext = func(context.Context, string, string) (net.Conn, error) { calls++; return nil, tc.err }
			client := &http.Client{Transport: n.HTTPTransport(tr)}
			for range 2 {
				err := networkGet(t.Context(), client, "http://excluded.test")
				if _, ok := errors.AsType[*cache.TerminalError](err); ok || !errors.Is(err, tc.err) {
					t.Fatalf("excluded error=%v", err)
				}
			}
			if calls != 2 {
				t.Fatalf("dials=%d want=2", calls)
			}
		})
	}
}

type timeoutContextError struct{ error }

func (e timeoutContextError) Unwrap() error { return e.error }
func (timeoutContextError) Timeout() bool   { return true }

// Deadline cancellation can lag its timestamp; Done remains open in this race.
type pendingDeadline struct {
	deadline time.Time
	done     <-chan struct{}
}

func (c pendingDeadline) Deadline() (time.Time, bool) { return c.deadline, true }
func (c pendingDeadline) Done() <-chan struct{}       { return c.done }
func (pendingDeadline) Err() error                    { return nil }
func (pendingDeadline) Value(any) any                 { return nil }

func TestNetwork_DeadlinePublicationRace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var network Network
		tr, _ := NewHTTPTransport(nil, nil)
		tr.Proxy = nil
		calls := 0
		cause := networkTimeout()
		tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
			calls++
			if calls == 1 {
				time.Sleep(2 * time.Second)
			}
			return nil, cause
		}
		client := &http.Client{Transport: network.HTTPTransport(tr)}
		ctx := pendingDeadline{deadline: time.Now().Add(time.Second), done: make(chan struct{})}
		err := networkGet(ctx, client, "http://deadline.test")
		if _, terminal := errors.AsType[*cache.TerminalError](err); terminal || !errors.Is(err, cause) {
			t.Fatalf("expired original attempt latched: %v", err)
		}
		requireNetworkTerminal(t, networkGet(t.Context(), client, "http://deadline.test"), cause, true)
		if calls != 2 {
			t.Fatalf("dials=%d want=2", calls)
		}
	})
}

func TestNetwork_RedirectEndpoint(t *testing.T) {
	t.Parallel()
	var network Network
	var dials atomic.Int32
	cause := networkTimeout()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "http://late.test/resource", http.StatusTemporaryRedirect)
			return
		}
		_, _ = io.WriteString(w, "healthy")
	}))
	t.Cleanup(srv.Close)
	tr, _ := NewHTTPTransport(nil, nil)
	tr.Proxy = nil
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "late.test:80" {
			dials.Add(1)
			return nil, cause
		}
		return dial(ctx, network, address)
	}
	client := &http.Client{Transport: network.HTTPTransport(tr)}
	t.Cleanup(tr.CloseIdleConnections)
	requireNetworkTerminal(t, networkGet(t.Context(), client, srv.URL+"/redirect"), cause, true)
	if err := networkGet(t.Context(), client, srv.URL+"/healthy"); err != nil {
		t.Fatalf("origin poisoned by redirect: %v", err)
	}
	requireNetworkTerminal(t, networkGet(t.Context(), client, "http://late.test/next"), cause, false)
	if dials.Load() != 1 {
		t.Fatalf("redirect dials=%d want=1", dials.Load())
	}
}

func TestNetwork_ResponseFailures(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"auth", "status", "body"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				switch failure {
				case "auth":
					w.WriteHeader(http.StatusUnauthorized)
				case "status":
					w.WriteHeader(http.StatusServiceUnavailable)
				case "body":
					w.Header().Set("Content-Length", "100")
					_, _ = io.WriteString(w, "short")
				}
			}))
			t.Cleanup(srv.Close)
			var network Network
			tr, _ := NewHTTPTransport(nil, nil)
			client := &http.Client{Transport: network.HTTPTransport(tr)}
			t.Cleanup(tr.CloseIdleConnections)
			for range 2 {
				err := networkGet(t.Context(), client, srv.URL)
				if _, terminal := errors.AsType[*cache.TerminalError](err); terminal {
					t.Fatalf("response failure latched: %v", err)
				}
				if failure == "body" && !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("body error=%v", err)
				}
			}
			if hits.Load() != 2 {
				t.Fatalf("response requests=%d want=2", hits.Load())
			}
		})
	}
}
