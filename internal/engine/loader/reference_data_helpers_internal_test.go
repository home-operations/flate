package loader

import (
	"testing"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
)

func lookupDataForTest[T interface {
	manifest.BaseManifest
	manifest.Data
}](tb testing.TB, s *store.Store, id manifest.NamedResource) (T, bool) {
	tb.Helper()
	data, ok := values.NewStoreProvider(s, manifest.NamedResource{}).Lookup(id).Data.(T)
	return data, ok
}
