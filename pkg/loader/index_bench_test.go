package loader

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkFinalizeIndex_WideDirectory(b *testing.B) {
	root := b.TempDir()
	var resources strings.Builder
	files := make(map[manifest.NamedResource]string, 4000)
	for i := range 4000 {
		file := fmt.Sprintf("cm-%d.yaml", i)
		fmt.Fprintf(&resources, "  - %s\n", file)
		files[manifest.NamedResource{Kind: manifest.KindConfigMap, Name: file}] = file
	}
	testutil.WriteFile(b, root, "kustomization.yaml", "resources:\n"+resources.String())
	documents := readerForTest(root)
	documents.read(root)
	b.ReportAllocs()
	for b.Loop() {
		l := loaderFixture(store.New(), files)
		l.documents = documents
		for id, r := range l.Index.Records {
			r.Path = filepath.Join(root, r.File)
			l.Index.Records[id] = r
		}
		if err := l.FinalizeIndex(b.Context(), root); err != nil {
			b.Fatal(err)
		}
	}
}
