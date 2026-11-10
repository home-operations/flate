package depwait

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func captureState(tb testing.TB, s *store.Store) *store.ReadSet {
	tb.Helper()
	reads := new(store.ReadSet)
	reads.Init(s, manifest.NamedResource{}, nil)
	tb.Cleanup(reads.Retire)
	return reads
}

func crdMissingPolicy(enabled bool) *MissingPolicy {
	if enabled {
		return NewMissingPolicy([]string{manifest.KindCustomResourceDefinition})
	}
	return NewMissingPolicy(nil)
}
