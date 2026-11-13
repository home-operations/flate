// Package networktest binds invocation networks to test cleanup.
package networktest

import (
	"testing"

	"github.com/home-operations/flate/pkg/source"
)

// New keeps the network alive until test cleanup, including deferred source reads.
func New(tb testing.TB) *source.Network {
	tb.Helper()
	network := new(source.Network)
	tb.Cleanup(network.Close)
	return network
}
