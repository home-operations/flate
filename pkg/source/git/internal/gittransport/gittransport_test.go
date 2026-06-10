package gittransport

import (
	"crypto/tls"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/transport/client"

	"github.com/home-operations/flate/pkg/source"
)

// TestDefaultClientIsBounded confirms init installed a go-git HTTPS default
// whose transport carries ResponseHeaderTimeout, so an anonymous git fetch
// against a black-holed host can't hang the run. The transport is a
// source.NewHTTPTransport(nil, nil) clone — its blocking behavior is covered
// by source.TestNewHTTPTransport_ResponseHeaderTimeout; here we pin that the
// default actually carries the backstop.
func TestDefaultClientIsBounded(t *testing.T) {
	if boundedHTTPSTransport == nil {
		t.Fatal("bounded default transport not initialized")
	}
	if got, want := boundedHTTPSTransport.ResponseHeaderTimeout, source.ResponseHeaderTimeout; got != want {
		t.Fatalf("default HTTPS transport ResponseHeaderTimeout = %v; want source.ResponseHeaderTimeout %v", got, want)
	}
}

// TestInstallHTTPS_AnonymousNoOp confirms the anonymous path is a no-op that
// does not acquire the install lock (it relies on the bounded default), and
// the returned restore is safe to call.
func TestInstallHTTPS_AnonymousNoOp(t *testing.T) {
	restore, err := InstallHTTPS(nil, nil)
	if err != nil {
		t.Fatalf("InstallHTTPS(nil, nil): %v", err)
	}
	if restore == nil {
		t.Fatal("restore func is nil")
	}
	restore() // must not panic / must not unlock an unheld mutex
}

// TestInstallHTTPS_RestoresBoundedDefault pins the liveness contract: after a
// custom-CA fetch's restore runs, go-git's process-global https client is the
// BOUNDED default again — not go-git's unbounded githttp.DefaultClient. Without
// this, the first mTLS/custom-CA GitRepository fetch would strip the
// ResponseHeaderTimeout backstop off every subsequent anonymous fetch in the
// process, re-exposing the hang this package exists to prevent.
func TestInstallHTTPS_RestoresBoundedDefault(t *testing.T) {
	restore, err := InstallHTTPS(&tls.Config{}, nil) // non-nil cfg → takes the install path
	if err != nil {
		t.Fatalf("InstallHTTPS: %v", err)
	}
	// Mid-install the custom transport is active, not the bounded default.
	if client.Protocols["https"] == boundedHTTPSClient {
		t.Fatal("custom transport was not installed under the lock")
	}
	restore()
	// Restore must reinstall the bounded default, not githttp.DefaultClient.
	if client.Protocols["https"] != boundedHTTPSClient {
		t.Fatal("restore did not reinstall the bounded default HTTPS client")
	}
}
