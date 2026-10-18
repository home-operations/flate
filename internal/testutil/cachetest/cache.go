// Package cachetest provides constructor-injected cache fixtures.
package cachetest

import (
	"context"
	"testing"
)

// New opens a cache and registers checked cleanup with the caller.
func New[C interface{ Close() error }](tb testing.TB, root string, open func(context.Context, string) (C, error)) C {
	tb.Helper()
	c, err := open(tb.Context(), root)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := c.Close(); err != nil {
			tb.Error(err)
		}
	})
	return c
}
