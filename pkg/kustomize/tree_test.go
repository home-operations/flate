package kustomize

import (
	"context"
	"testing"
)

// TestTreeCache_SetGitBaseFetcher confirms the one remaining injected seam wires
// through (pkg/kustomize cannot import pkg/source/git — import cycle).
func TestTreeCache_SetGitBaseFetcher(t *testing.T) {
	c := NewTreeCache()
	called := false
	c.SetGitBaseFetcher(func(context.Context, string, string) (string, string, error) {
		called = true
		return "", "", nil
	})
	if c.gitBase == nil {
		t.Fatal("git base fetcher not set")
	}
	_, _, _ = c.gitBase(context.Background(), "https://example.com/x", "main")
	if !called {
		t.Fatal("wired fetcher was not invoked")
	}
}
