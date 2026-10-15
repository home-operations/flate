package kustomize

import (
	"context"
	"testing"

	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// TestTreeCache_SetGitBaseFetcher confirms the one remaining injected seam wires
// through (pkg/kustomize cannot import pkg/source/git — import cycle).
func TestTreeCache_SetGitBaseFetcher(t *testing.T) {
	c := NewTreeCache(newTestCache(t, ""))
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

func newTestCache(t testing.TB, root string) *cache.Cache {
	t.Helper()
	owner, err := cache.New(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	return owner
}
