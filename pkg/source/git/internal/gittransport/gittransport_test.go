package gittransport

import (
	"context"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

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
	if boundedHTTPSTransport.ResponseHeaderTimeout != 0 {
		t.Fatal("transport overrides operation deadline")
	}
	if client.Protocols["https"] != boundedHTTPSClient {
		t.Fatal("bounded HTTPS client is not installed")
	}
}

func TestDefaultClient_PerCallControls(t *testing.T) {
	for _, name := range []string{"ca-cancel", "insecure-cancel", "restricted"} {
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				close(started)
				<-release
			}))
			defer srv.Close()
			defer close(release)
			remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{Name: "origin", URLs: []string{srv.URL}})
			opts := &git.ListOptions{CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})}
			if name == "insecure-cancel" {
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
			var err error
			select {
			case <-started:
				if name == "restricted" {
					t.Fatal("restricted egress allowed a request")
				}
				cancel()
				err = <-done
			case err = <-done:
				if name != "restricted" {
					t.Fatalf("operation returned before request arrival: %v", err)
				}
			}
			switch name {
			case "restricted":
				if dial, ok := errors.AsType[*net.OpError](err); !ok || dial.Op != "dial" {
					t.Fatalf("egress was not blocked at dial: %v", err)
				}
			default:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation: %v", err)
				}
			}
			if boundedHTTPSTransport.TLSClientConfig != nil && boundedHTTPSTransport.TLSClientConfig.InsecureSkipVerify {
				t.Fatal("per-call TLS mutated the installed transport")
			}
		})
	}
}
