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
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
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
	assert.FatalIf(
		t,
		!ok || terminal.Attempted != attempted || !errors.Is(err, cause) || !errors.Is(err, manifest.ErrFlux),
		"error=%v, terminal=%+v; want cause=%v attempted=%t",
		err,
		terminal,
		cause,
		attempted,
	)
}

func TestNetwork_AdmissionWave(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(strconv.Itoa(concurrency), func(t *testing.T) {
			t.Parallel()
			testNetworkAdmissionWaveCase(t, &concurrency)
		},
		)
		t.Run(fmt.Sprintf("late-dial-%d", concurrency), func(t *testing.T) {
			t.Parallel()
			testNetworkAdmissionWaveCase2(t, &concurrency)
		},
		)
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
			testNetworkRouteIdentityCase(t, &tc)
		},
		)
	}
}

func TestNetwork_AttemptLiveness(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/deadline-%t", concurrency, deadline), func(t *testing.T) {
				t.Parallel()
				testNetworkAttemptLivenessCase(
					t,
					&concurrency, &deadline,
				)
			},
			)
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
			testNetworkAttemptLivenessCase2(t, &tc)
		},
		)
	}
}

type timeoutContextError struct{ error }

func (e timeoutContextError) Unwrap() error { return e.error }

func (timeoutContextError) Timeout() bool { return true }

// Deadline cancellation can lag its timestamp; Done remains open in this race.
type pendingDeadline struct {
	deadline time.Time
	done     <-chan struct{}
}

func (c pendingDeadline) Deadline() (time.Time, bool) { return c.deadline, true }

func (c pendingDeadline) Done() <-chan struct{} { return c.done }

func (pendingDeadline) Err() error { return nil }

func (pendingDeadline) Value(any) any { return nil }

func TestNetwork_DeadlinePublicationRace(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var network Network
		tr := NewHTTPTransport(TransportMaterial{})
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
		client := &http.Client{Transport: network.wrapHTTPTransport(tr)}
		ctx := pendingDeadline{deadline: time.Now().Add(time.Second), done: make(chan struct{})}
		err := networkGet(ctx, client, "http://deadline.test")
		{
			_, terminal := errors.AsType[*cache.TerminalError](err)
			assert.FatalIf(t, terminal || !errors.Is(err, cause), "expired original attempt latched: %v", err)
		}
		requireNetworkTerminal(t, networkGet(t.Context(), client, "http://deadline.test"), cause, true)
		assert.FatalIf(t, calls != 2, "dials=%d want=2", calls)
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
	tr := NewHTTPTransport(TransportMaterial{})
	tr.Proxy = nil
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "late.test:80" {
			dials.Add(1)
			return nil, cause
		}
		return dial(ctx, network, address)
	}
	client := &http.Client{Transport: network.wrapHTTPTransport(tr)}
	t.Cleanup(tr.CloseIdleConnections)
	requireNetworkTerminal(t, networkGet(t.Context(), client, srv.URL+"/redirect"), cause, true)
	{
		err := networkGet(t.Context(), client, srv.URL+"/healthy")
		assert.FatalIf(t, err != nil, "origin poisoned by redirect: %v", err)
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
			testNetworkResponseFailuresCase(t, &failure)
		},
		)
	}
}

func TestNetwork_TransportReuse(t *testing.T) {
	t.Parallel()
	t.Run("explicit-proxy", func(t *testing.T) { t.Parallel(); testNetworkTransportReuseExplicitProxy(t) })
	t.Run("default-first-use", func(t *testing.T) { t.Parallel(); testNetworkTransportReuseDefaultFirstUse(t) })
	t.Run(
		"concurrent-first-selection",
		func(t *testing.T) { t.Parallel(); testNetworkTransportReuseConcurrentFirstSelection(t) },
	)
	t.Run("effective-material", func(t *testing.T) { t.Parallel(); testNetworkTransportReuseEffectiveMaterial(t) })
}

func testNetworkAdmissionWaveCase(t *testing.T, concurrency *int,
) {
	t.Helper()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") }),
	)
	t.Cleanup(srv.Close)
	var n Network
	tr := NewHTTPTransport(TransportMaterial{})
	tr.Proxy = nil
	tr.MaxIdleConnsPerHost = (*concurrency)
	var calls atomic.Int32
	entered := make(chan struct{}, (*concurrency))
	release := make([]chan struct{}, (*concurrency))
	results := make([]chan error, (*concurrency))
	causes := make([]error, (*concurrency))
	for i := range *concurrency {
		release[i], results[i], causes[i] = make(chan struct{}), make(chan error, 1), networkTimeout()
	}
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		i := int(calls.Add(1)) - 1
		if i < (*concurrency) {
			entered <- struct{}{}
			<-release[i]
			if i != 1 {
				return nil, causes[i]
			}
		}
		return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
	}
	client := &http.Client{Transport: n.wrapHTTPTransport(tr)}
	t.Cleanup(tr.CloseIdleConnections)
	for i := range *concurrency {
		go func() { results[i] <- networkGet(t.Context(), client, "http://wave.test") }()
		<-entered
	}
	close(release[0])
	requireNetworkTerminal(t, <-results[0], causes[0], true)
	for i := 2; i < (*concurrency); i++ {
		close(release[i])
		requireNetworkTerminal(t, <-results[i], causes[i], true)
	}
	close(release[1])
	{
		err := <-results[1]
		assert.FatalIf(t, err != nil, "admitted success: %v", err)
	}
	for range *concurrency {
		requireNetworkTerminal(t, networkGet(t.Context(), client, "http://wave.test"), causes[0], false)
	}
	{
		err := networkGet(t.Context(), client, "http://healthy.test")
		assert.FatalIf(t, err != nil, "independent endpoint: %v", err)
	}
	if got := calls.Load(); got != int32((*concurrency)+1) {
		t.Fatalf("physical dials=%d, want %d", got, (*concurrency)+1)
	}
}

func testNetworkAdmissionWaveCase2(t *testing.T, concurrency *int,
) {
	t.Helper()

	var n Network
	var calls atomic.Int32
	cause := networkTimeout()
	tr := NewHTTPTransport(TransportMaterial{})
	tr.Proxy = nil
	tr.DialContext = func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, cause
	}
	client := &http.Client{Transport: n.wrapHTTPTransport(tr)}
	entered, release := make(chan struct{}, (*concurrency)), make(chan struct{})
	done := make(chan error, (*concurrency))
	for range *concurrency {
		go func() {
			ctx := httptrace.WithClientTrace(
				t.Context(),
				&httptrace.ClientTrace{GetConn: func(string) { entered <- struct{}{}; <-release }},
			)
			done <- networkGet(ctx, client, "http://late.test")
		}()
	}
	for range *concurrency {
		<-entered
	}
	requireNetworkTerminal(t, networkGet(t.Context(), client, "http://late.test"), cause, true)
	close(release)
	for range *concurrency {
		requireNetworkTerminal(t, <-done, cause, false)
	}
	{
		got := calls.Load()
		assert.FatalIf(t, got != 1, "guarded dials=%d, want 1", got)
	}
}

func testNetworkRouteIdentityCase(t *testing.T, tc *struct {
	name       string
	first      string
	second     string
	proxy      string
	nextProxy  string
	suppressed bool
},
) {
	t.Helper()

	var n Network
	cause, observed := networkTimeout(), errors.New("healthy route attempted")
	calls, proxyCalls := 0, 0
	client := func(proxy string, failure error) *http.Client {
		tr := NewHTTPTransport(TransportMaterial{})
		tr.Proxy = func(*http.Request) (*url.URL, error) {
			proxyCalls++
			if proxy == "" {
				return nil, nil //nolint:nilnil // A nil value and nil error represent an optional absence or a reusable-cache miss.
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
		return &http.Client{Transport: n.wrapHTTPTransport(tr)}
	}
	requireNetworkTerminal(
		t,
		networkGet(t.Context(), client(tc.proxy, cause), tc.first),
		cause,
		true,
	)
	proxyCalls = 0
	err := networkGet(t.Context(), client(tc.nextProxy, observed), tc.second)
	want := 2
	if tc.suppressed {
		want = 1
		requireNetworkTerminal(t, err, cause, false)
	} else {
		assert.FatalIf(t, !errors.Is(err, observed), "independent route: %v", err)
	}
	assert.FatalIf(
		t,
		calls != want || proxyCalls != 1,
		"dials=%d want=%d, proxy calls=%d want=1",
		calls,
		want,
		proxyCalls,
	)
}

func testNetworkAttemptLivenessCase(t *testing.T, concurrency *int,
	deadline *bool,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testNetworkAttemptLivenessCaseCallback(
			t,
			testNetworkAttemptLivenessCaseCallbackInputs{concurrency: &concurrency, deadline: &deadline},
		)
	},
	)
}

func testNetworkAttemptLivenessCase2(t *testing.T, tc *struct {
	name string
	err  error
},
) {
	t.Helper()

	var n Network
	calls := 0
	tr := NewHTTPTransport(TransportMaterial{})
	tr.Proxy = nil
	tr.DialContext = func(context.Context, string, string) (net.Conn, error) { calls++; return nil, tc.err }
	client := &http.Client{Transport: n.wrapHTTPTransport(tr)}
	for range 2 {
		err := networkGet(t.Context(), client, "http://excluded.test")
		{
			_, ok := errors.AsType[*cache.TerminalError](err)
			assert.FatalIf(t, ok || !errors.Is(err, tc.err), "excluded error=%v", err)
		}
	}
	assert.FatalIf(t, calls != 2, "dials=%d want=2", calls)
}

func testNetworkResponseFailuresCase(t *testing.T, failure *string,
) {
	t.Helper()

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, callbackArg2 *http.Request) {
		testNetworkResponseFailuresCaseCallback(
			w,
			callbackArg2,
			testNetworkResponseFailuresCaseCallbackInputs{failure: &failure, hits: &hits},
		)
	},
	))
	t.Cleanup(srv.Close)
	var network Network
	tr := NewHTTPTransport(TransportMaterial{})
	client := &http.Client{Transport: network.wrapHTTPTransport(tr)}
	t.Cleanup(tr.CloseIdleConnections)
	for range 2 {
		err := networkGet(t.Context(), client, srv.URL)
		{
			_, terminal := errors.AsType[*cache.TerminalError](err)
			assert.FatalIf(t, terminal, "response failure latched: %v", err)
		}
		assert.FatalIf(
			t,
			(*failure) == "body" && !errors.Is(err, io.ErrUnexpectedEOF),
			"body error=%v",
			err,
		)
	}
	if hits.Load() != 2 {
		t.Fatalf("response requests=%d want=2", hits.Load())
	}
}

func testNetworkTransportReuseExplicitProxy(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name, address, password string
		equal                   bool
	}{
		{name: "equivalent", address: "http://proxy.invalid", password: "first", equal: true},
		{name: "host", address: "http://other.invalid", password: "first"},
		{name: "credentials", address: "http://proxy.invalid", password: "rotated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var n Network
			t.Cleanup(n.Close)
			first, err := n.HTTPTransport(
				HTTPTransportConfig{
					Material: TransportMaterial{
						proxyURL: &url.URL{
							Scheme: "http",
							Host:   "proxy.invalid",
							User:   url.UserPassword("user", "first"),
						},
					},
				},
			)
			assert.NoError(t, err)

			second, err := n.HTTPTransport(HTTPTransportConfig{Material: func() TransportMaterial {
				selected, parseErr := url.Parse(tc.address)
				assert.NoError(t, parseErr)

				selected.User = url.UserPassword("user", tc.password)
				return TransportMaterial{proxyURL: selected}
			}()})
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				(first == second) != tc.equal,
				"%s",
				"transport selection did not follow explicit proxy material",
			)
		})
	}
}

type testNetworkAttemptLivenessCaseCallbackInputs struct {
	concurrency **int
	deadline    **bool
}

func testNetworkAttemptLivenessCaseCallback(t *testing.T, testCaseInput testNetworkAttemptLivenessCaseCallbackInputs) {
	t.Helper()
	var n Network
	entered, release, finished := make(
		chan struct{}, (*(*testCaseInput.concurrency)),
	), make(
		chan struct{},
	), make(
		chan struct{}, (*(*testCaseInput.concurrency)),
	)
	calls := 0
	observed := errors.New("live attempt reached dial")
	tr := NewHTTPTransport(TransportMaterial{})
	tr.Proxy = nil
	tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		calls++
		if calls > (*(*testCaseInput.concurrency)) {
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
	client := &http.Client{Transport: n.wrapHTTPTransport(tr)}
	ctx, cancel := context.WithCancel(t.Context())
	if *(*testCaseInput.deadline) {
		cancel()
		ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	}
	defer cancel()
	done := make(chan error, (*(*testCaseInput.concurrency)))
	for range *(*testCaseInput.concurrency) {
		go func() { done <- networkGet(ctx, client, "http://cancel.test") }()
		<-entered
	}
	if !(*(*testCaseInput.deadline)) {
		cancel()
	}
	for range *(*testCaseInput.concurrency) {
		err := <-done
		assert.FatalIf(
			t,
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded),
			"original attempt: %v",
			err,
		)
	}
	close(release)
	for range *(*testCaseInput.concurrency) {
		<-finished
	}
	synctest.Wait()
	{
		err := networkGet(t.Context(), client, "http://cancel.test")
		assert.FatalIf(t, !errors.Is(err, observed), "canceled attempt latched: %v", err)
	}
}

type testNetworkResponseFailuresCaseCallbackInputs struct {
	failure **string
	hits    *atomic.Int32
}

func testNetworkResponseFailuresCaseCallback(
	w http.ResponseWriter,
	_ *http.Request,
	testCaseInput testNetworkResponseFailuresCaseCallbackInputs,
) {
	testCaseInput.hits.
		Add(1)
	switch *(*testCaseInput.failure) {
	case "auth":
		w.WriteHeader(http.StatusUnauthorized)
	case "status":
		w.WriteHeader(http.StatusServiceUnavailable)
	case "body":
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, "short")
	}
}
