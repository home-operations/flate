package loader

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func lookupDataForTest[T interface {
	manifest.BaseManifest
	manifest.Data
}](tb testing.TB, s *store.Store, id manifest.NamedResource) (T, bool) {
	tb.Helper()
	data, ok := values.NewStoreProvider(s, true).Data(id).(T)
	return data, ok
}
