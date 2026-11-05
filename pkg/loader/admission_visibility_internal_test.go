package loader

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func duplicateAdmissionInputPair(t *testing.T, scenario string) ([2]*Loader, [2]*store.Store) {
	t.Helper()
	var loaders [2]*Loader
	var stores [2]*store.Store
	for side := range loaders {
		root := t.TempDir()
		data := []byte(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: old}\n",
		)
		files := fstest.MapFS{
			"flux/consumer.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"consumer",
						"apps",
						"  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
					),
				),
			},
			"app/kustomization.yaml": {Data: []byte("resources: []\n")},
			"a/settings.yaml":        {Data: data},
			"b/settings.yaml":        {Data: data},
			".sourceignore":          {},
		}
		if side == 1 {
			ignore := "a/settings.yaml\n"
			if scenario == "last_hidden" {
				ignore += "b/settings.yaml\n"
			}
			files[".sourceignore"].Data = []byte(ignore)
			if scenario == "changed_content" {
				files["b/settings.yaml"].Data = []byte(strings.ReplaceAll(string(data), "old", "new"))
			}
		}
		view, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: files},
		)
		assert.NoError(t, err)
		st := store.New()
		stores[side] = st
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: view})
		l := New(st)
		loaders[side] = l
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	}
	return loaders, stores
}

func admissionVisibilityPair(t *testing.T, policy string, hide bool) ([2]*Loader, [2]*store.Store) {
	t.Helper()
	var loaders [2]*Loader
	var stores [2]*store.Store
	for side := range loaders {
		root := t.TempDir()
		st := store.New()
		stores[side] = st
		ignore := "# unchanged visibility\n"
		if side == 1 {
			ignore = "# new comment\n"
			if hide {
				ignore += "settings.yaml\n"
				if policy == "krmignore" {
					ignore += "unused.yaml\nready.yaml\n"
				}
			}
		}
		files := fstest.MapFS{
			".krmignore": {Data: []byte(ignore)},
			"opaque.yaml": {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\n" + "kind: HelmRelease\n" +
				"metadata: {name: opaque, namespace: apps}\n" +
				"spec: {chartRef: {kind: OCIRepository, name: absent}}\n")},
			"consumer.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"consumer",
						"apps",
						"  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n",
					),
				),
			},
			"stable.yaml": {Data: []byte(testutil.KustomizationYAML("stable", "apps", "  path: ./stable\n"))},
			"ordering.yaml": {
				Data: []byte(
					testutil.KustomizationYAML(
						"ordering",
						"apps",
						"  path: ./ordering\n  dependsOn: [{name: ready}]\n",
					),
				),
			},
			"ready.yaml": {Data: []byte(testutil.KustomizationYAML("ready", "apps", "  path: ./ready\n"))},
			"settings.yaml": {
				Data: []byte(
					"apiVersion: v1\n" + "kind: ConfigMap\n" + "metadata: {name: settings, namespace: apps}\n" +
						"data: {VALUE: unchanged}\n",
				),
			},
			"unused.yaml": {Data: []byte("apiVersion: source.toolkit.fluxcd.io/v1\n" + "kind: GitRepository\n" +
				"metadata: {name: unused, namespace: apps}\n" +
				"spec: {url: https://fixture.invalid/unused}\n")},
		}
		for _, directory := range []string{"app", "stable", "ordering", "ready"} {
			files[directory+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: []\n")}
		}
		input, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: files},
		)
		assert.NoError(t, err)
		if policy != "krmignore" {
			delete(files, ".krmignore")
			view := gittree.ViewOptions{Mode: gittree.Source, Current: files}
			if policy == "sourceignore" {
				files[".sourceignore"] = &fstest.MapFile{Data: []byte(ignore)}
			} else {
				view.Ignore = ignore
			}
			input, _, err = gittree.New(t.Context(), nil, plumbing.ZeroHash, view)
			assert.NoError(t, err)
		}
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
		l := New(st)
		loaders[side] = l
		l.SourceRoot = root
		l.Options.Selection = manifest.Selection{Changed: true}
		l.Options.Filter = change.NewFilter()
		_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	}
	return loaders, stores
}
