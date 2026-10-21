package gittransport

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

func TestDefaultClientIsBounded(t *testing.T) {
	t.Parallel()
	if boundedHTTPSTransport == nil {
		t.Fatal("bounded default transport not initialized")
	}
	if got, want := boundedHTTPSTransport.ResponseHeaderTimeout, 120*time.Second; got != want {
		t.Fatalf("default HTTPS transport ResponseHeaderTimeout = %v; want 120*time.Second %v", got, want)
	}
	if client.Protocols["https"] != boundedHTTPSClient {
		t.Fatal("bounded HTTPS client is not installed")
	}
}

func TestDefaultClient_PerCallControls(t *testing.T) {
	bound := boundedHTTPSTransport.ResponseHeaderTimeout
	boundedHTTPSTransport.ResponseHeaderTimeout = 100 * time.Millisecond
	t.Cleanup(func() { boundedHTTPSTransport.ResponseHeaderTimeout = bound })
	for _, name := range []string{"ca-timeout", "insecure-timeout", "restricted"} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
			}))
			defer srv.Close()
			remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{Name: "origin", URLs: []string{srv.URL}})
			opts := &git.ListOptions{CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})}
			if name == "insecure-timeout" {
				opts.CABundle, opts.InsecureSkipTLS = nil, true
			}
			if name == "restricted" {
				ssrfguard.Restrict(true)
				defer ssrfguard.Restrict(false)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := remote.ListContext(ctx, opts); done <- err }()
			err := <-done
			switch name {
			case "restricted":
				if dial, ok := errors.AsType[*net.OpError](err); !ok || dial.Op != "dial" {
					t.Fatalf("egress was not blocked at dial: %v", err)
				}
			default:
				if timeout, ok := errors.AsType[net.Error](err); !ok || !timeout.Timeout() {
					t.Fatalf("header timeout: %v", err)
				}
			}
			if boundedHTTPSTransport.TLSClientConfig != nil && boundedHTTPSTransport.TLSClientConfig.InsecureSkipVerify {
				t.Fatal("per-call TLS mutated the installed transport")
			}
		})
	}
}
