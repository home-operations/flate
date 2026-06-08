package source

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestNewHTTPTransport_ResponseHeaderTimeout is the liveness-backstop
// regression: a host that accepts the connection but never sends response
// headers (a black-hole) must not hang the fetch forever — the consumer's
// dependency wait is now bound to fetch completion, so an unbounded fetch
// would wedge the whole run. NewHTTPTransport always returns a transport
// carrying ResponseHeaderTimeout; the request fails fast instead of hanging.
func TestNewHTTPTransport_ResponseHeaderTimeout(t *testing.T) {
	// A server that reads the request but never writes a response header.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-block
	}))
	// LIFO cleanup: unblock the handler BEFORE Close so Close doesn't wait on it.
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(block) })

	orig := ResponseHeaderTimeout
	ResponseHeaderTimeout = 50 * time.Millisecond
	t.Cleanup(func() { ResponseHeaderTimeout = orig })

	tr, err := NewHTTPTransport(nil, nil)
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}
	if tr == nil {
		t.Fatal("NewHTTPTransport(nil, nil) = nil; want a bounded transport")
	}
	if tr.ResponseHeaderTimeout != 50*time.Millisecond {
		t.Fatalf("ResponseHeaderTimeout = %v; want 50ms", tr.ResponseHeaderTimeout)
	}

	start := time.Now()
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a response-header timeout error; got a response")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("request took %v; expected to time out at ~50ms (before the fix it hangs)", elapsed)
	}
}

// TestNewHTTPTransport_AppliesTLSAndStaysBounded confirms TLS customization
// still composes onto the bounded clone (the custom-CA / OCI / bucket paths).
func TestNewHTTPTransport_AppliesTLSAndStaysBounded(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr, err := NewHTTPTransport(srv.Client().Transport.(*http.Transport).TLSClientConfig, nil)
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}
	if tr.ResponseHeaderTimeout != ResponseHeaderTimeout {
		t.Fatalf("ResponseHeaderTimeout = %v; want %v", tr.ResponseHeaderTimeout, ResponseHeaderTimeout)
	}
	if tr.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig not applied onto the bounded clone")
	}
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	if err != nil {
		t.Fatalf("GET over custom TLS: %v", err)
	}
	_ = resp.Body.Close()
}
