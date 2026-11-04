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

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

func TestDefaultClientIsBounded(t *testing.T) {
	t.Parallel()
	assert.FatalIf(t, boundedHTTPSTransport == nil, "%s", "bounded default transport not initialized")
	assert.FatalIf(t, boundedHTTPSTransport.ResponseHeaderTimeout != 0, "%s", "transport overrides operation deadline")
	assert.FatalIf(t, client.Protocols["https"] != boundedHTTPSClient, "%s", "bounded HTTPS client is not installed")
}

//nolint:paralleltest // The test changes the process egress policy.
//nolint:paralleltest // The test changes process egress policy.
func TestDefaultClient_PerCallControls(t *testing.T) {
	for _, name := range []string{"ca-cancel", "insecure-cancel", "restricted"} {
		t.Run(name, func(t *testing.T) {
			testDefaultClientPerCallControlsCase(t, &name)
		},
		)
	}
}

func testDefaultClientPerCallControlsCase(t *testing.T, name *string,
) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	}))
	defer srv.Close()
	defer close(release)
	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{Name: "origin", URLs: []string{srv.URL}})
	opts := &git.ListOptions{
		CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}),
	}
	if (*name) == "insecure-cancel" {
		opts.CABundle, opts.InsecureSkipTLS = nil, true
	}
	if (*name) == "restricted" {
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
		assert.FatalIf(t, (*name) == "restricted", "%s", "restricted egress allowed a request")

		cancel()
		err = <-done
	case err = <-done:
		assert.FatalIf(t, (*name) != "restricted", "operation returned before request arrival: %v", err)
	}
	switch *name {
	case "restricted":
		{
			dial, ok := errors.AsType[*net.OpError](err)
			assert.FatalIf(t, !ok || dial.Op != "dial", "egress was not blocked at dial: %v", err)
		}
	default:
		assert.FatalIf(t, !errors.Is(err, context.Canceled), "caller cancellation: %v", err)
	}
	assert.FatalIf(t, boundedHTTPSTransport.TLSClientConfig != nil &&
		boundedHTTPSTransport.TLSClientConfig.InsecureSkipVerify, "%s", "per-call TLS mutated the installed transport")
}
