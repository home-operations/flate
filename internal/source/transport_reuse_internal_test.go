package source

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/net/http/httpproxy"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNetworkTransportReuseDefaultFirstUse(t *testing.T) {
	t.Helper()

	var n Network
	t.Cleanup(n.Close)
	var connections atomic.Int32
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	var wg sync.WaitGroup
	var errs [2]error
	for i := range errs {
		wg.Go(func() {
			errs[i] = networkGet(t.Context(), &http.Client{Transport: &n}, srv.URL)
		})
	}
	<-entered
	<-entered
	close(release)
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
	selected, err := n.HTTPTransport(HTTPTransportConfig{})
	assert.NoError(t, err)
	assert.FatalIf(t, selected != n.defaultHTTP, "%s", "default requests did not use the shared transport")
	{
		err := networkGet(t.Context(), &http.Client{Transport: selected}, srv.URL)
		assert.NoError(t, err)
	}

	if connections.Load() != 2 {
		t.Fatalf("accepted connections=%d, want 2", connections.Load())
	}
}

func testNetworkTransportReuseConcurrentFirstSelection(t *testing.T) {
	t.Helper()

	var n Network
	t.Cleanup(n.Close)
	var connections atomic.Int32
	closed := make(chan struct{}, 1)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.ProtoMajor == 2 {
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	srv.EnableHTTP2 = true
	srv.StartTLS()
	material := TransportMaterial{
		config:      srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone(),
		environment: &httpproxy.Config{},
	}
	material.config.NextProtos = []string{"h2", "http/1.1"}
	t.Cleanup(srv.Close)
	start := make(chan struct{})
	var clients [2]http.Client
	var errs [2]error
	var wg sync.WaitGroup
	for i := range clients {
		wg.Go(func() {
			<-start
			clients[i].Transport, errs[i] = n.HTTPTransport(HTTPTransportConfig{Material: material})
		})
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.FatalIf(
		t,
		clients[0].Transport != clients[1].Transport,
		"%s",
		"concurrent selection escaped different transports",
	)
	assert.FatalIf(
		t,
		clients[0].Transport.(*networkTransport).Cache == nil,
		"%s",
		"published transport has no upstream auth cache",
	)

	for i := range clients {
		{
			err := networkGet(t.Context(), &clients[i], srv.URL)
			assert.NoError(t, err)
		}
	}
	for i := range clients {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodHead, srv.URL, http.NoBody)
		assert.NoError(t, err)

		resp, err := clients[i].Do(req)
		assert.FatalIf(t, err != nil, "shared registry HEAD: %v", err)
		{
			err := resp.Body.Close()
			assert.NoError(t, err)
		}
	}
	if connections.Load() != 1 {
		t.Fatalf("accepted connections=%d, want 1", connections.Load())
	}
	t.Logf("compatible clients: requests=4 accepted_connections=%d", connections.Load())
	n.Close()
	select {
	case <-closed:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func testNetworkTransportReuseEffectiveMaterial(t *testing.T) {
	t.Helper()

	ca, otherCA := testutil.SelfSignedCA(t), testutil.SelfSignedCA(t)
	certificate2 := testutil.SelfSignedClientCert(t)

	certificate3 := testutil.SelfSignedClientCert(t)

	resolve := func(crt, key, ca string) TransportMaterial {
		cfg, err := buildTLSConfig(crt, key, ca)
		assert.NoError(t, err)

		return TransportMaterial{config: cfg, certificate: [3]string{crt, key, ca}}
	}
	for _, tc := range []struct {
		name   string
		change func(*HTTPTransportConfig)
		equal  bool
	}{
		{name: "equivalent-certificates", equal: true, change: func(cfg *HTTPTransportConfig) {
			cfg.Material.config = resolve(certificate2.Cert, certificate2.Key, ca).config
		}},
		{name: "ca", change: func(cfg *HTTPTransportConfig) {
			cfg.Material.config = resolve(certificate2.Cert, certificate2.Key, otherCA).config
			cfg.Material.certificate[2] = otherCA
		}},
		{name: "client-certificate", change: func(cfg *HTTPTransportConfig) {
			cfg.Material.config = resolve(certificate3.Cert, certificate3.Key, ca).config
			cfg.Material.certificate = [3]string{certificate3.Cert, certificate3.Key, ca}
		}},
		{name: "credentials", change: func(cfg *HTTPTransportConfig) { cfg.Credentials = [2]string{"user", "rotated"} }},
		{name: "insecure", change: func(cfg *HTTPTransportConfig) { cfg.Insecure = true }},
		{name: "proxy", change: func(cfg *HTTPTransportConfig) {
			cfg.Material.proxyURL = &url.URL{Scheme: "http", Host: "proxy.invalid", User: url.UserPassword("user", "first")}
			cfg.Material.environment = nil
		}},
		{name: "proxy-credentials", change: func(cfg *HTTPTransportConfig) {
			cfg.Material.environment = &httpproxy.Config{HTTPProxy: "http://user:rotated@proxy.invalid"}
		}},
		{name: "no-proxy", change: func(cfg *HTTPTransportConfig) {
			cfg.Material.environment = &httpproxy.Config{HTTPProxy: "http://user:first@proxy.invalid", NoProxy: "*.example.com"}
		}},
		{name: "compression", change: func(cfg *HTTPTransportConfig) { cfg.DisableCompression = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var n Network
			t.Cleanup(n.Close)
			cfg := HTTPTransportConfig{
				Material:    resolve(certificate2.Cert, certificate2.Key, ca),
				Credentials: [2]string{"user", "first"},
			}
			cfg.Material.environment = &httpproxy.Config{HTTPProxy: "http://user:first@proxy.invalid"}
			first, err := n.HTTPTransport(cfg)
			assert.NoError(t, err)

			tc.change(&cfg)
			second, err := n.HTTPTransport(cfg)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				(first == second) != tc.equal,
				"%s",
				"transport selection did not follow effective material",
			)
			assert.FatalIf(
				t,
				cfg.Material.config.InsecureSkipVerify || len(cfg.Material.config.NextProtos) != 0,
				"%s",
				"transport publication mutated borrowed TLS configuration",
			)
		})
	}
}
