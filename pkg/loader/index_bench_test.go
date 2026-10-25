package loader

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

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

func BenchmarkFinalizeIndex_LocalChart(b *testing.B) {
	root := b.TempDir()
	input := fstest.MapFS{"charts/demo/Chart.yaml": {}, "charts/demo/.helmignore": {Data: []byte("excluded/\n")}}
	for i := range 200 {
		input[fmt.Sprintf("charts/demo/templates/cm-%d.yaml", i)] = &fstest.MapFile{Data: []byte("payload")}
	}
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input})
	files := make(map[manifest.NamedResource]string, 100)
	for i := range 100 {
		hr := &manifest.HelmRelease{Name: fmt.Sprintf("release-%d", i), Namespace: "apps", Chart: manifest.HelmChart{Name: "./charts/demo", Source: source.Named()}}
		st.AddObject(hr)
		files[hr.Named()] = hr.Name + ".yaml"
	}
	b.ReportAllocs()
	for b.Loop() {
		l := loaderFixture(st, files)
		for id, r := range l.Index.Records {
			r.Refs = []manifest.NamedResource{source.Named()}
			l.Index.Records[id] = r
		}
		if err := l.FinalizeIndex(b.Context(), root); err != nil {
			b.Fatal(err)
		}
	}
}
