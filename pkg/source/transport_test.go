package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/pkg/source/cache"
)

type requestBoundTransport struct{}

func (requestBoundTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: requestBoundBody{ctx: r.Context()}, Header: make(http.Header), Request: r}, nil
}

type requestBoundBody struct{ ctx context.Context }

func (b requestBoundBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (requestBoundBody) Close() error { return nil }

func TestNewHTTPClient_RequestBounds(t *testing.T) {
	t.Parallel()
	for _, purpose := range []HTTPPurpose{HTTPStream, HTTPChart, HTTPResource} {
		t.Run([]string{"stream", "chart", "resource"}[purpose], func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				client := NewHTTPClient(requestBoundTransport{}, purpose)
				want := time.Duration(0)
				if purpose == HTTPChart {
					want = 120 * time.Second
				}
				if client.Timeout != want {
					t.Fatalf("client timeout = %v, want %v", client.Timeout, want)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://fixture.invalid", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				done := make(chan error, 1)
				go func() { _, err := io.ReadAll(resp.Body); done <- err }()
				synctest.Wait()
				time.Sleep(120 * time.Second)
				synctest.Wait()
				if purpose != HTTPChart {
					select {
					case err := <-done:
						t.Fatalf("stream gained a whole-body deadline: %v", err)
					default:
					}
					cancel()
				}
				err = <-done
				if purpose == HTTPChart && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("chart body was not bounded: %v", err)
				}
				if purpose != HTTPChart && !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation was lost: %v", err)
				}
			})
		})
	}
	if HTTPResource.Timeout() != 5*time.Second || HTTPStream.Timeout() != 0 {
		t.Fatal("context operation bounds changed")
	}
}

func TestNewHTTPTransport_ResponseHeaderTimeout(t *testing.T) {
	// A server that reads the request but never writes a response header.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-block
	}))
	// LIFO cleanup: unblock the handler BEFORE Close so Close doesn't wait on it.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(block) })

	orig := requestBounds.header
	requestBounds.header = 50 * time.Millisecond
	t.Cleanup(func() { requestBounds.header = orig })

	tr := NewHTTPTransport(TransportMaterial{})
	if tr == nil {
		t.Fatal("NewHTTPTransport(TransportMaterial{}) = nil; want a bounded transport")
	}
	if tr.ResponseHeaderTimeout != 50*time.Millisecond {
		t.Fatalf("requestBounds.header = %v; want 50ms", tr.ResponseHeaderTimeout)
	}

	start := time.Now()
	var network Network
	client := &http.Client{Transport: network.wrapHTTPTransport(tr)}
	resp, err := client.Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a response-header timeout error; got a response")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("request took %v; expected to time out at ~50ms (before the fix it hangs)", elapsed)
	}
	for range 2 {
		_, err := client.Get(srv.URL)
		if _, terminal := errors.AsType[*cache.TerminalError](err); err == nil || terminal {
			t.Fatalf("response-header timeout latched: %v", err)
		}
	}
}

func TestNewHTTPTransport_AppliesTLSAndStaysBounded(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr := NewHTTPTransport(TransportMaterial{config: srv.Client().Transport.(*http.Transport).TLSClientConfig})
	if tr.ResponseHeaderTimeout != requestBounds.header {
		t.Fatalf("requestBounds.header = %v; want %v", tr.ResponseHeaderTimeout, requestBounds.header)
	}
	if tr.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig not applied onto the bounded clone")
	}
	var network Network
	resp, err := (&http.Client{Transport: network.wrapHTTPTransport(tr)}).Get(srv.URL)
	if err != nil {
		t.Fatalf("GET over custom TLS: %v", err)
	}
	_ = resp.Body.Close()
}
