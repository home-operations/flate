package change_test

import (
	"context"
	"errors"
	"io/fs"
	"path"
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type chartInputFS struct {
	fs.FS
	opens, directories map[string]int
	deny               string
	cancel             func()
}

func (f chartInputFS) Open(name string) (fs.File, error) {
	f.opens[name]++
	if name == f.deny {
		return nil, fs.ErrPermission
	}
	return f.FS.Open(name)
}

func (f chartInputFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.FS, name)
}

func (f chartInputFS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.directories[name]++
	if name == f.deny {
		return nil, fs.ErrPermission
	}
	if f.cancel != nil {
		f.cancel()
	}
	return fs.ReadDir(f.FS, name)
}

func TestFinalizeIndex_ChartInputs(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"directory", "root", "archive", "absent policy", "self-excluded policy", "malformed policy", "unreadable policy", "unreadable payload", "unreadable directory", "irregular", "missing chart", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			location := "charts/demo"
			input := fstest.MapFS{
				"Chart.yaml": {Data: []byte("metadata")}, "values.yaml": {Data: []byte("defaults")},
				"templates/cm.yaml": {Data: []byte("payload")}, "charts/child.tgz": {Data: []byte("archive")},
				".helmignore": {Data: []byte("excluded/\n")}, "excluded/invalid.yaml": {Data: []byte("excluded")},
			}
			want := []string{"Chart.yaml", "values.yaml", "templates/cm.yaml", "charts/child.tgz", ".helmignore"}
			switch mode {
			case "root":
				location = "."
			case "archive":
				location = "charts/demo.tgz"
			case "absent policy":
				delete(input, ".helmignore")
				delete(input, "excluded/invalid.yaml")
			case "self-excluded policy":
				input[".helmignore"].Data = []byte(".helmignore\nexcluded/\n")
			case "malformed policy":
				input[".helmignore"].Data = []byte("**\n")
				want = []string{".helmignore"}
			case "unreadable policy", "unreadable directory":
				want = []string{".helmignore"}
			case "irregular":
				input["z-irregular"] = &fstest.MapFile{Mode: fs.ModeSymlink}
			case "missing chart":
				want = nil
			}
			mounted := fstest.MapFS{}
			for name, file := range input {
				mounted[path.Join(location, name)] = file
			}
			switch mode {
			case "archive":
				mounted = fstest.MapFS{location: {Data: []byte("physical archive")}}
				want = []string{location}
			case "missing chart":
				mounted = fstest.MapFS{}
			default:
				for i, name := range want {
					want[i] = path.Join(location, name)
				}
			}
			want = append(want, "config/explicit.yaml")
			slices.Sort(want)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observed := chartInputFS{FS: mounted, opens: map[string]int{}, directories: map[string]int{}}
			if mode == "unreadable policy" {
				observed.deny = path.Join(location, ".helmignore")
			}
			if mode == "unreadable directory" {
				observed.deny = location
			}
			if mode == "unreadable payload" {
				observed.deny = path.Join(location, "templates/cm.yaml")
			}
			if mode == "canceled" {
				observed.cancel = cancel
			}
			st := store.New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			st.AddObject(source)
			st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: observed})
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: observed})
			release := &manifest.HelmRelease{Name: "demo", Namespace: "apps", Chart: manifest.HelmChart{Name: location, Source: source.Named()}, ChartValuesFiles: []string{"config/explicit.yaml"}}
			second := release.Clone()
			second.Name = "second"
			sibling := &manifest.ConfigMap{Name: "independent", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{release, second, sibling})
			l := readerLoaderForFilter(st, map[manifest.NamedResource]string{release.Named(): "hr.yaml", second.Named(): "second.yaml", sibling.Named(): "independent.yaml", source.Named(): "source.yaml"})
			for _, id := range []manifest.NamedResource{release.Named(), second.Named()} {
				r := l.Index.Records[id]
				r.Refs = []manifest.NamedResource{source.Named()}
				l.Index.Records[id] = r
			}
			err := l.FinalizeIndex(ctx, root)
			if mode == "canceled" {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
				return
			}
			assert.NoError(t, err)
			for _, id := range []manifest.NamedResource{release.Named(), second.Named()} {
				r, _ := l.Index.Lookup(id)
				assert.Diff(t, readFilesForFilter(l.Index, id), want)
				assert.Equal(t, len(r.Productions), 0)
			}
			r, _ := l.Index.Lookup(source.Named())
			assert.Equal(t, len(r.Inputs), 0)
			assert.Equal(t, len(l.Index.Errors), 0)
			assert.Equal(t, len(l.Index.Records[release.Named()].Inputs), 0)
			assert.Equal(t, observed.opens[path.Join(location, "templates/cm.yaml")], 0)
			assert.Equal(t, observed.directories[location] <= 1, true)
			if mode != "archive" && mode != "missing chart" {
				assert.Equal(t, observed.opens[path.Join(location, ".helmignore")], 1)
			}
			for _, file := range want {
				assert.Equal(t, len(ownersForFilter(l.Index, file)), 0)
				filter := change.NewFilter(change.NewSet([]string{file}), l.Index, st, "")
				assert.Equal(t, filter.ShouldReconcile(release.Named()), true)
				assert.Equal(t, filter.ShouldReconcile(source.Named()), true)
				assert.Equal(t, filter.ShouldReconcile(sibling.Named()), false)
			}
			filter := change.NewFilter(change.NewSet([]string{path.Join(location, "excluded/invalid.yaml")}), l.Index, st, "")
			assert.Equal(t, filter.ShouldReconcile(release.Named()), false)
		})
	}
}

func TestFinalizeIndex_TransitiveChartInputs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps", Chart: ".", SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindGitRepository, Name: "source"}}
	first := &manifest.HelmRelease{Name: "first", Namespace: "apps", Chart: manifest.HelmChart{Name: ".", Source: source.Named()}, ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmRelease, Name: "second"}}
	second := &manifest.HelmRelease{Name: "second", Namespace: "apps", Chart: manifest.HelmChart{Name: ".", Source: source.Named()}, ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindHelmRelease, Name: "first"}}
	build := func() *loader.Loader {
		t.Helper()
		st := store.New()
		st.AddObjects([]manifest.BaseManifest{source, chart, first, second})
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"Chart.yaml": {}, "templates/cm.yaml": {}}})
		l := readerLoaderForFilter(st, map[manifest.NamedResource]string{first.Named(): "first.yaml", second.Named(): "second.yaml", chart.Named(): "chart.yaml"})
		for id, r := range l.Index.Records {
			r.Refs = []manifest.NamedResource{chart.Named()}
			r.Inputs = []string{id.Name + ".input"}
			l.Index.Records[id] = r
		}
		assert.NoError(t, l.FinalizeIndex(t.Context(), root))
		return l
	}
	l := build()
	want := []string{".helmignore", "Chart.yaml", "first.input", "second.input", "templates/cm.yaml"}
	for _, id := range []manifest.NamedResource{first.Named(), second.Named()} {
		assert.Diff(t, readFilesForFilter(l.Index, id), want)
	}
	snapshot := snapshotIndexForFilter(l.Index)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root))
	assert.Diff(t, *l.Index, snapshot)
	assert.Diff(t, *build().Index, snapshot)
}

func readerLoaderForFilter(st *store.Store, files map[manifest.NamedResource]string) *loader.Loader {
	l := loader.New(st)
	for id, file := range files {
		l.Index.Records[id] = manifest.DiscoveryRecord{File: file}
	}
	return l
}

func readFilesForFilter(index *manifest.DiscoveryIndex, id manifest.NamedResource) []string {
	var files []string
	for file, readers := range index.Readers {
		if slices.ContainsFunc(readers, func(r manifest.FileReader) bool { return r.Reader == id }) {
			files = append(files, file)
		}
	}
	slices.Sort(files)
	return files
}
