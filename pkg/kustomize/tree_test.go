package kustomize

import (
	"context"
	"testing"

	"github.com/home-operations/flate/pkg/store"
)

// TestTreeCache_SetGitBaseFetcher confirms the one remaining injected seam wires
// through (pkg/kustomize cannot import pkg/source/git — import cycle).
func TestTreeCache_SetGitBaseFetcher(t *testing.T) {
	c := NewTreeCache()
	called := false
	c.SetGitBaseFetcher(func(context.Context, GitBaseRequest) (*store.SourceArtifact, error) {
		called = true
		return nil, nil
	})
	if c.gitBase == nil {
		t.Fatal("git base fetcher not set")
	}
	_, _ = c.gitBase(t.Context(), GitBaseRequest{URL: "https://example.com/x", Ref: "main"})
	if !called {
		t.Fatal("wired fetcher was not invoked")
	}
}
