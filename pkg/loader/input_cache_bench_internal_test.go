package loader

import (
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/cache"
)

func BenchmarkInputCache_Restore(b *testing.B) {
	root := b.TempDir()
	files := fstest.MapFS{}
	for i := range 128 {
		files[fmt.Sprintf("object-%03d.yaml", i)] = &fstest.MapFile{
			Data: fmt.Appendf(
				nil,
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: object-%03d}\ndata: {value: before}\n",
				i,
			),
		}
	}
	owner, err := cache.New(b.Context(), b.TempDir())
	assert.NoError(b, err)
	b.Cleanup(func() { assert.NoError(b, owner.Close()) })
	if !owner.CanPersistRenders() {
		b.Fatal("input cache benchmark requires a clean stamped test binary")
	}
	first := cachedFixtureLoader(b, root, files, owner)
	assert.NoError(b, first.PersistInputs(b.Context()))
	first.Close()
	b.ReportAllocs()
	for b.Loop() {
		restored := cachedFixtureLoader(b, root, files, owner)
		restored.Close()
	}
}
