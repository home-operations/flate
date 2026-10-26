package source

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewHTTPTransport_AppliesTLS(t *testing.T) {
	t.Parallel()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	tr := NewHTTPTransport(TransportMaterial{config: srv.Client().Transport.(*http.Transport).TLSClientConfig})
	if tr.ResponseHeaderTimeout != 0 {
		t.Fatalf("transport overrides operation deadline: %v", tr.ResponseHeaderTimeout)
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
