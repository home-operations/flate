package gittransport

import (
	"testing"

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
