package source

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/store"
)

func detectChanges(tb testing.TB, before, after []string) *change.Set {
	tb.Helper()
	inputs := [2]*store.SourceArtifact{}
	for side, paths := range [2][]string{before, after} {
		files := fstest.MapFS{}
		for _, file := range paths {
			files[filepath.ToSlash(file)] = &fstest.MapFile{Data: []byte{byte(side)}}
		}
		inputs[side] = &store.SourceArtifact{FS: files}
	}
	changes, err := change.Detect(tb.Context(), inputs[0], inputs[1], change.DetectConfig{})
	if err != nil {
		tb.Fatal(err)
	}
	return changes
}
