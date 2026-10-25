package loader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_SourceValuesReaders(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"working", "alias-subtree", "committed", "external", "unavailable", "missing", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "config/prod.yaml", "invalid: [")
			chartFiles := fstest.MapFS{
				"charts/demo/Chart.yaml":        {Data: []byte("metadata")},
				"charts/demo/values.yaml":       {Data: []byte("defaults")},
				"charts/demo/templates/cm.yaml": {Data: []byte("payload")},
				"charts/demo/charts/child.tgz":  {Data: []byte("archive")},
				"charts/demo/.helmignore":       {Data: []byte("excluded/\n")},
				"charts/demo/excluded/no.yaml":  {Data: []byte("excluded")},
			}
			for name, file := range chartFiles {
				testutil.WriteFile(t, root, name, string(file.Data))
			}
			if mode != "working" {
				testutil.WriteFile(t, root, "charts/demo/host-only.yaml", "must not be read")
			}
			files, err := json.Marshal([]string{"./config/prod.yaml", "config/prod.yaml", "config/missing.yaml", "../escape.yaml", "/absolute.yaml", "config/../../escape.yaml", "", ".", "bad\x00.yaml"})
			assert.NoError(t, err)
			group := manifest.SourceDomain
			if mode == "foreign" {
				group = "foreign.example"
			}
			testutil.WriteFile(t, root, "manifests/kustomization.yaml", "namespace: apps\nresources: [hr.yaml]\n")
			testutil.WriteFile(t, root, "manifests/hr.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {apiVersion: %s/v1, kind: GitRepository, name: cluster, namespace: flux-system}
      valuesFiles: %s
      ignoreMissingValuesFiles: true
`, group, files))
			newLoader := func() (*Loader, *missingResourceFS, []string) {
				t.Helper()
				st := store.New()
				l := New(st)
				l.SourceRoot = root
				admitForTest(st, root)
				source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
				chartFiles["config/prod.yaml"] = &fstest.MapFile{Data: []byte("invalid: [")}
				input := &missingResourceFS{FS: chartFiles, missing: "config/prod.yaml"}
				art := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: input}
				want := []string{"config/prod.yaml", "config/missing.yaml", "charts/demo/.helmignore", "charts/demo/Chart.yaml", "charts/demo/values.yaml", "charts/demo/templates/cm.yaml", "charts/demo/charts/child.tgz"}
				switch mode {
				case "alias-subtree":
					art.LocalPath = filepath.Join(root, ".artifacts", "alias")
					art.LocalRoot = filepath.Join(root, "subtree")
					art.InputAlias, art.Key = true, "derived"
					var aliased []string
					for _, file := range want {
						aliased = append(aliased, path.Join(".artifacts/alias", file), path.Join("subtree", file))
					}
					want = aliased
				case "committed":
					art.LocalPath, art.Key = filepath.Join(root, ".artifacts", "commit"), "committed"
					for i, file := range want {
						want[i] = path.Join(".artifacts/commit", file)
					}
				case "external":
					art.LocalPath, art.LocalRoot = filepath.Join(root, ".artifacts", "remote"), t.TempDir()
					want = nil
				case "unavailable":
					art.FS = nil
					want = nil
				case "missing", "foreign":
					want = nil
				}
				if mode != "missing" {
					st.AddObject(source)
					st.SetArtifact(source.Named(), art)
				}
				if mode == "working" {
					input.FS = os.DirFS(root)
					st.SetArtifact(manifest.BootstrapSourceID, art)
				}
				_, err := loadForTest(t.Context(), t, l, filepath.Join(root, "manifests"))
				assert.NoError(t, err)
				return l, input, want
			}
			l, input, want := newLoader()
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			id := testutil.HelmReleaseID("apps", "demo")
			record, _ := l.Index.Lookup(id)
			slices.Sort(want)
			assert.Diff(t, record.Inputs, want)
			assert.Equal(t, len(l.Index.Readers), 0)
			assert.Equal(t, input.opens, 0)
			assert.Equal(t, len(l.Index.Claims), 0)
			assert.Equal(t, len(l.Index.Records), 1)
			record, _ = l.Index.Lookup(id)
			assert.Equal(t, len(record.Productions), 1)
			filter := change.NewFilter(change.NewSet([]string{"config/prod.yaml"}), l.Index, l.Store)
			assert.Equal(t, filter.ShouldReconcile(id), mode == "working")
			snapshot := snapshotIndexForTest(l.Index)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *l.Index, snapshot)
			assert.Equal(t, input.opens, 0)
			fresh, freshInput, _ := newLoader()
			if err := fresh.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, *fresh.Index, snapshot)
			assert.Equal(t, freshInput.opens, 0)
		})
	}
}

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
			l := loaderFixture(st, map[manifest.NamedResource]string{release.Named(): "hr.yaml", second.Named(): "second.yaml", sibling.Named(): "independent.yaml", source.Named(): "source.yaml"})
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
				assert.Diff(t, r.Inputs, want)
				assert.Equal(t, len(r.Productions), 0)
			}
			r, _ := l.Index.Lookup(source.Named())
			assert.Equal(t, len(r.Inputs), 0)
			assert.Equal(t, len(l.Index.Errors), 0)
			assert.Equal(t, len(l.Index.Readers), 0)
			assert.Equal(t, observed.opens[path.Join(location, "templates/cm.yaml")], 0)
			assert.Equal(t, observed.directories[location] <= 1, true)
			if mode != "archive" && mode != "missing chart" {
				assert.Equal(t, observed.opens[path.Join(location, ".helmignore")], 1)
			}
			for _, file := range want {
				assert.Equal(t, len(ownersForTest(l.Index, file)), 0)
				filter := change.NewFilter(change.NewSet([]string{file}), l.Index, st)
				assert.Equal(t, filter.ShouldReconcile(release.Named()), true)
				assert.Equal(t, filter.ShouldReconcile(source.Named()), true)
				assert.Equal(t, filter.ShouldReconcile(sibling.Named()), false)
			}
			filter := change.NewFilter(change.NewSet([]string{path.Join(location, "excluded/invalid.yaml")}), l.Index, st)
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
	build := func() *Loader {
		t.Helper()
		st := store.New()
		st.AddObjects([]manifest.BaseManifest{source, chart, first, second})
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"Chart.yaml": {}, "templates/cm.yaml": {}}})
		l := loaderFixture(st, map[manifest.NamedResource]string{first.Named(): "first.yaml", second.Named(): "second.yaml", chart.Named(): "chart.yaml"})
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
		r, _ := l.Index.Lookup(id)
		assert.Diff(t, r.Inputs, want)
	}
	snapshot := snapshotIndexForTest(l.Index)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root))
	assert.Diff(t, *l.Index, snapshot)
	assert.Diff(t, *build().Index, snapshot)
}
