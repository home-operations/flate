package change_test

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ImplicitAndTerminalInputs(t *testing.T) {
	t.Parallel()
	for _, overlay := range []string{"entry", "entry/nested", "entry/deep/nested"} {
		t.Run(overlay, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			base, _ := filepath.Rel(overlay, "shared")
			testutil.WriteFile(t, root, "entry/README.md", "not a manifest\n")
			testutil.WriteFile(t, root, "entry/sibling.json", `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"sibling"}}`)
			testutil.WriteFile(t, root, overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(base)+"]\n")
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\nconfigMapGenerator: [{name: values, files: [key=../inputs/value], env: ../inputs/env}]\nreplacements: [{path: ../inputs/missing}]\n")
			testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
			testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
			testutil.WriteFile(t, root, "inputs/value", testutil.KustomizationYAML("terminal", "apps", "  path: ./hidden\n"))
			testutil.WriteFile(t, root, "inputs/env", "KEY=value\n")
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			s.AddObject(owner)
			l := loader.New(s)
			l.SourceRoot = root
			admitForFilter(s, root)
			if _, err := l.Load(t.Context(), filepath.Join(root, "entry")); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if overlay != "entry" {
				assert.Diff(t, l.Index.Readers["entry/sibling.json"], []manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}})
				if len(l.Index.Readers["entry/README.md"]) != 0 {
					t.Fatal("non-manifest implicit sibling became a reader")
				}
			}
			for _, file := range []string{overlay + "/kustomization.yaml", "shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch", "inputs/value", "inputs/env", "inputs/missing"} {
				assert.Diff(t, l.Index.Readers[file], []manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}})
				if !slices.Contains(ownersForFilter(l.Index, file), owner.Named()) {
					t.Errorf("owner missing for %s", file)
				}
				filter := change.NewFilter(change.NewSet([]string{file}), l.Index, s, "")
				if !filter.ShouldReconcile(owner.Named()) {
					t.Errorf("changed %s did not select owner", file)
				}
			}
			if id := testutil.KustomizationID("apps", "terminal"); s.GetObject(id) != nil || len(l.Index.Records[id].Productions) != 0 {
				t.Fatal("terminal input became an object or producer")
			}
			if change.NewFilter(change.NewSet([]string{"inputs/unrelated"}), l.Index, s, "").ShouldReconcile(owner.Named()) {
				t.Fatal("unrelated input selected owner")
			}
		})
	}
}

func TestFilter_DuplicateDocumentKeys(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../shared]\nnamespace: one\nnamespace: two\n")
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../inputs/ignored, path: ../inputs/patch}]\n")
	testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
	testutil.WriteFile(t, root, "inputs/ignored", "[]\n")
	testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
	testutil.WriteFile(t, root, "alias/kustomization.yaml", "namespace: one\nx: &namespace y\n*namespace: z\n")
	testutil.WriteFile(t, root, "merge/kustomization.yaml", "a: &a {namespace: one}\nb: &b {namePrefix: pre-}\n<<: *a\n<<: *b\n")
	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
	s.AddObject(owner)
	l := loader.New(s)
	l.SourceRoot = root
	admitForFilter(s, root)
	_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
	assert.NoError(t, err)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch"} {
		assert.Diff(t, l.Index.Readers[file], []manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}})
		if !slices.Contains(ownersForFilter(l.Index, file), owner.Named()) {
			t.Errorf("owner missing for %s", file)
		}
	}
	if !change.NewFilter(change.NewSet([]string{"inputs/patch"}), l.Index, s, "").ShouldReconcile(owner.Named()) {
		t.Error("changed terminal patch did not select owner")
	}
}

func TestFinalizeIndex_InputSourceViews(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mounted", "alias", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			local := filepath.Join(root, ".artifacts", mode)
			input := fstest.MapFS{
				"entry/nested/kustomization.yaml": {Data: []byte("resources: [../../shared]\n")},
				"shared/kustomization.yaml":       {Data: []byte("patches: [{path: ../inputs/patch}]\n")},
			}
			testutil.WriteFile(t, local, "shared/kustomization.yaml", "patches: [{path: ../inputs/host-only}]\n")
			s := store.New()
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
			s.AddObject(source)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: source.Name, Namespace: source.Namespace}}
			s.AddObject(owner)
			art := &store.SourceArtifact{LocalRoot: root, LocalPath: local, FS: input}
			if mode == "alias" {
				art.InputAlias, art.LocalRoot = true, filepath.Join(root, "subtree")
			}
			if mode == "pinned" {
				art.Key = "pinned"
			}
			s.SetArtifact(source.Named(), art)
			l := loader.New(s)
			l.SourceRoot = root
			admitForFilter(s, root)
			_, err := l.Load(t.Context(), filepath.Join(local, "entry"))
			assert.NoError(t, err)
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			prefix := ".artifacts/" + mode + "/"
			assert.Diff(t, l.Index.Readers[prefix+"inputs/patch"], []manifest.FileReader{{Reader: owner.Named(), Source: source.Named()}})
			if len(l.Index.Readers[prefix+"inputs/host-only"]) != 0 {
				t.Fatal("discovery read host files outside the mounted view")
			}
			file := "inputs/patch"
			if mode == "alias" {
				file = "subtree/inputs/patch"
			}
			filter := change.NewFilter(change.NewSet([]string{file}), l.Index, s, "")
			assert.Equal(t, filter.ShouldReconcile(owner.Named()), mode != "pinned")
			assert.Equal(t, filter.ShouldReconcile(source.Named()), mode != "pinned")
			if mode == "pinned" {
				assert.Diff(t, l.Index.Readers[file], []manifest.FileReader(nil))
			}
		})
	}
}

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
			newLoader := func() (*loader.Loader, *readerForFilter, []string) {
				t.Helper()
				st := store.New()
				l := loader.New(st)
				l.SourceRoot = root
				admitForFilter(st, root)
				source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
				chartFiles["config/prod.yaml"] = &fstest.MapFile{Data: []byte("invalid: [")}
				input := &readerForFilter{FS: chartFiles, missing: "config/prod.yaml"}
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
				_, err := l.Load(t.Context(), filepath.Join(root, "manifests"))
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
			assert.Diff(t, readFilesForFilter(l.Index, id), want)
			assert.Equal(t, len(record.Inputs), 0)
			for _, readers := range l.Index.Readers {
				assert.Diff(t, readers, []manifest.FileReader{{Reader: id, Source: (manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "cluster", Namespace: "flux-system"}), Exact: true}})
			}
			assert.Equal(t, input.opens, 0)
			assert.Equal(t, len(l.Index.Claims), 0)
			assert.Equal(t, len(l.Index.Records), 1)
			record, _ = l.Index.Lookup(id)
			assert.Equal(t, len(record.Productions), 1)
			filter := change.NewFilter(change.NewSet([]string{"config/prod.yaml"}), l.Index, l.Store, "")
			assert.Equal(t, filter.ShouldReconcile(id), mode == "working")
			snapshot := snapshotIndexForFilter(l.Index)
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
	t.Run("chartRef", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [chart.yaml]\n")
		testutil.WriteFile(t, root, "entry/chart.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: chart, namespace: apps}\nspec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: values}, valuesFiles: [config/prod.yaml, config/prod.yaml, entry/kustomization.yaml]}\n")
		testutil.WriteFile(t, root, "hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {chartRef: {kind: HelmChart, name: chart}}\n")
		st := store.New()
		admitForFilter(st, root)
		source := &manifest.GitRepository{Name: "values", Namespace: "apps"}
		st.AddObject(source)
		input := &readerForFilter{FS: fstest.MapFS{"config/prod.yaml": {Data: []byte("invalid: [")}, "entry/kustomization.yaml": {Data: []byte("not bootstrap")}}, missing: "config/prod.yaml"}
		st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: filepath.Join(root, ".selected"), LocalRoot: root, InputAlias: true, FS: input})
		l := loader.New(st)
		l.SourceRoot = root
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
		chartID := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "chart"}
		chart := &manifest.HelmChartSource{Name: chartID.Name, Namespace: chartID.Namespace}
		chart.Chart = "charts/demo"
		chart.SourceRef.Kind, chart.SourceRef.Name = source.Named().Kind, source.Name
		chart.ValuesFiles = []string{"config/prod.yaml", "config/prod.yaml", "entry/kustomization.yaml"}
		l.RegisterBuilt(filepath.Join(root, "entry"), []store.Child{{Obj: chart}})
		assert.NoError(t, l.FinalizeIndex(t.Context(), root))
		consumer := testutil.HelmReleaseID("apps", "demo")
		want := []manifest.FileReader{{Reader: consumer, Source: source.Named(), Exact: true}, {Reader: chartID, Source: source.Named(), Exact: true}}
		slices.SortFunc(want, func(a, b manifest.FileReader) int { return a.Reader.Compare(b.Reader) })
		assert.Diff(t, l.Index.Readers["config/prod.yaml"], want)
		assert.Diff(t, l.Index.Readers[".selected/config/prod.yaml"], want)
		want = []manifest.FileReader{{Reader: consumer, Source: source.Named(), Exact: true}, {Reader: consumer, Source: manifest.BootstrapSourceID}, {Reader: chartID, Source: source.Named(), Exact: true}}
		slices.SortFunc(want, func(a, b manifest.FileReader) int {
			if order := a.Reader.Compare(b.Reader); order != 0 {
				return order
			}
			return a.Source.Compare(b.Source)
		})
		assert.Diff(t, l.Index.Readers["entry/kustomization.yaml"], want)
		record, _ := l.Index.Lookup(chartID)
		assert.Equal(t, slices.Contains(record.Inputs, "entry/kustomization.yaml"), true)
		assert.Equal(t, slices.Contains(l.Index.Readers["entry/kustomization.yaml"], manifest.FileReader{Reader: chartID, Source: manifest.BootstrapSourceID}), false)
		assert.Equal(t, input.opens, 0)
		snapshot := snapshotIndexForFilter(l.Index)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root))
		assert.Diff(t, *l.Index, snapshot)
		assert.Equal(t, input.opens, 0)
	})
}

func TestFinalizeIndex_OwnersOfFileResourcesEscape(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, overlay string
		mixed         bool
	}{
		{"explicit", "apps/homelab", false},
		{"shallow", "apps/homelab/nested", false},
		{"deep", "apps/homelab/deep/nested", false},
		{"mixed", "apps/homelab/deep/nested", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			base, err := filepath.Rel(tc.overlay, "apps/base/postgres")
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, tc.overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(base)+"]\n")
			if tc.mixed {
				testutil.WriteFile(t, dir, "apps/homelab/sibling.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\n")
			}
			testutil.WriteFile(t, dir, "apps/base/postgres/kustomization.yaml",
				"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cluster.yaml\npatches: [{path: ../../../inputs/patch}]\n")
			testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
				"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

			s := store.New()
			var want []manifest.FileReader
			for i := range 2 {
				ks := &manifest.Kustomization{Name: fmt.Sprintf("apps-%d", i), Namespace: "flux-system", Path: "./apps/homelab"}
				s.AddObject(ks)
				want = append(want, manifest.FileReader{Reader: ks.Named(), Source: manifest.BootstrapSourceID})
			}

			idx := rendererForFilter(t, loader.New(s), dir)

			for _, file := range []string{
				tc.overlay + "/kustomization.yaml",
				"apps/base/postgres/kustomization.yaml",
				"apps/base/postgres/cluster.yaml",
				"inputs/patch",
			} {
				if got := idx.Readers[file]; !slices.Equal(got, want) {
					t.Errorf("Readers[%s] = %v, want %v", file, got, want)
				}
			}
			filter := change.NewFilter(change.NewSet([]string{"apps/base/postgres/cluster.yaml"}), idx, s, "")
			for _, id := range want {
				if !filter.ShouldReconcile(id.Reader) {
					t.Errorf("changed shared base did not select %s", id.Reader)
				}
			}
		})
	}
}

func TestFinalizeIndex_ArtifactGenerators(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, resources, scan string
		component, alias      bool
	}{
		{"generator_first", "generator.yaml, consumer.yaml", ".", false, false},
		{"consumer_first", "consumer.yaml, generator.yaml", ".", false, false},
		{"component_full", "consumer.yaml", ".", true, false},
		{"component_changed_only", "consumer.yaml", "apps", true, false},
		{"component_input_alias", "consumer.yaml", "apps", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: ["+tc.resources+"]\n")
			generatorFile := "apps/generator.yaml"
			if tc.component {
				generatorFile = "shared/generator.yaml"
				testutil.WriteFile(t, root, "shared/kustomization.yaml", "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [generator.yaml]\n")
			}
			testutil.WriteFile(t, root, generatorFile, "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: authored, uid: authored}\nspec:\n  sources: [{alias: foreign, kind: OCIRepository, name: foreign, namespace: external}, {alias: repo, kind: GitRepository, name: input}, {alias: bucket, kind: Bucket, name: bucket, namespace: storage}]\n  pathPattern: '@repo/apps/{app}'\n  artifacts: [{name: 'app-{app}'}]\n")
			testutil.WriteFile(t, root, "apps/consumer.yaml", testutil.KustomizationYAML("consumer", "authored", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n"))
			s := store.New()
			source := manifest.BootstrapSourceID
			if tc.alias {
				repository := &manifest.GitRepository{Name: "alias", Namespace: "flux-system"}
				source = repository.Named()
				s.AddObject(repository)
				s.SetArtifact(source, &store.SourceArtifact{LocalRoot: root, LocalPath: filepath.Join(t.TempDir(), "alias"), InputAlias: true, FS: os.DirFS(root)})
			}
			for _, ns := range []string{"apps", "staging"} {
				owner := &manifest.Kustomization{Name: ns, Namespace: "flux-system", Path: "./apps", TargetNamespace: ns}
				if tc.alias {
					owner.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: source.Kind, Name: source.Name, Namespace: source.Namespace}
				}
				if tc.component {
					owner.Components = []string{"../shared"}
				}
				s.AddObject(owner)
			}
			l := loader.New(s)
			l.SourceRoot = root
			admitForFilter(s, root)
			if _, err := l.Load(t.Context(), filepath.Join(root, tc.scan)); err != nil {
				t.Fatal(err)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			idx := l.Index
			for _, ns := range []string{"apps", "staging"} {
				generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: ns, Name: "generator"}
				record, ok := idx.Lookup(generator)
				if !ok || record.File != generatorFile {
					t.Fatalf("generator coordinates before filtering = %+v", record)
				}
				assert.Diff(t, record.Refs, []manifest.NamedResource{
					{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "external", Name: "foreign"},
					{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: ns, Name: "input"},
					{Group: manifest.SourceDomain, Kind: manifest.KindBucket, Namespace: "storage", Name: "bucket"},
				})
				target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: ns, Name: "app-api"}
				r, ok := idx.Lookup(target)
				if !ok || r.Path != "" || s.GetObject(target) != nil || s.GetArtifact(target) != nil {
					t.Fatalf("prediction supplied content: %+v", r)
				}
				want := manifest.Production{Owner: testutil.KustomizationID("flux-system", ns), Declaration: generator, Authored: manifest.NamedResource{Group: generator.Group, Kind: generator.Kind, Namespace: "authored", Name: generator.Name}, Source: source, File: generatorFile}
				assert.Diff(t, r.Productions, []manifest.Production{want})
			}
			filter := change.NewFilter(change.NewSet([]string{generatorFile}), idx, s, "")
			assert.Diff(t, idx.Readers[generatorFile], []manifest.FileReader{{Reader: testutil.KustomizationID("flux-system", "apps"), Source: source}, {Reader: testutil.KustomizationID("flux-system", "staging"), Source: source}})
			for _, ns := range []string{"apps", "staging"} {
				owner := testutil.KustomizationID("flux-system", ns)
				if !filter.ShouldReconcile(owner) {
					t.Fatal("changed declaration did not select its contributing owner")
				}
				generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: ns, Name: "generator"}
				obj := s.GetObject(generator)
				assert.Equal(t, obj != nil, !tc.component)
				if obj != nil && (!manifest.KindOf(obj).Discovery || !manifest.KindOf(obj).Runnable) {
					t.Fatalf("generator admission = %v", obj)
				}
				if !filter.ShouldReconcile(testutil.KustomizationID(ns, "consumer")) {
					t.Fatal("changed generator declaration did not select its source consumer")
				}
				if r, ok := idx.Lookup(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: ns, Name: "generator"}); ok && len(r.Productions) != 0 {
					t.Fatal("generator name became an output")
				}
			}
		})
	}
}

func admitForFilter(s *store.Store, root string) {
	s.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: os.DirFS(root)})
}

func ownersForFilter(index *manifest.DiscoveryIndex, file string) []manifest.NamedResource {
	owners, _ := index.OwnersOfFile(file)
	return owners
}

func rendererForFilter(t *testing.T, l *loader.Loader, root string) *manifest.DiscoveryIndex {
	t.Helper()
	admitForFilter(l.Store, root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root))
	return l.Index
}

type readerForFilter struct {
	fs.FS
	missing string
	opens   int
}

func (f *readerForFilter) Open(name string) (fs.File, error) {
	if name == f.missing {
		f.opens++
	}
	return f.FS.Open(name)
}

func (f *readerForFilter) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(f.FS, name)
}

func snapshotIndexForFilter(index *manifest.DiscoveryIndex) manifest.DiscoveryIndex {
	snapshot := *index
	snapshot.Records = maps.Clone(index.Records)
	for id, r := range snapshot.Records {
		r.Refs = slices.Clone(r.Refs)
		r.Productions = slices.Clone(r.Productions)
		snapshot.Records[id] = r
	}
	snapshot.Claims = maps.Clone(index.Claims)
	for file, ids := range snapshot.Claims {
		snapshot.Claims[file] = slices.Clone(ids)
	}
	snapshot.Readers = maps.Clone(index.Readers)
	for file, ids := range snapshot.Readers {
		snapshot.Readers[file] = slices.Clone(ids)
	}
	snapshot.Errors = maps.Clone(index.Errors)
	return snapshot
}

func TestFinalizeIndex_StaticArtifactGeneratorConsumer(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/generator.yaml", "apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps, uid: authored}\nspec:\n  sources: [{alias: foreign, kind: OCIRepository, name: foreign, namespace: external}, {alias: repo, kind: GitRepository, name: input}]\n  pathPattern: '@repo/{app}'\n  artifacts: [{name: 'app-{app}'}]\n")
	testutil.WriteFile(t, root, "entry/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-api}\n"))
	testutil.WriteFile(t, root, "entry/other.yaml", testutil.KustomizationYAML("other", "other", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-other}\n"))
	testutil.WriteFile(t, root, "entry/foreign-owner.yaml", testutil.KustomizationYAML("foreign-owner", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: foreign}\n"))
	view := filepath.Join(root, "views", "foreign")
	testutil.WriteFile(t, view, "apps/kustomization.yaml", "resources: [consumer.yaml]\n")
	testutil.WriteFile(t, view, "apps/consumer.yaml", testutil.KustomizationYAML("foreign", "apps", "  path: ./leaf\n  sourceRef: {kind: ExternalArtifact, name: app-foreign}\n"))
	s := store.New()
	admitForFilter(s, root)
	source := &manifest.GitRepository{Name: "foreign", Namespace: "flux-system"}
	s.AddObject(source)
	s.SetArtifact(source.Named(), &store.SourceArtifact{LocalRoot: root, LocalPath: view, Key: "foreign-pin", FS: os.DirFS(view)})
	l := loader.New(s)
	l.SourceRoot = root
	for _, scan := range []string{filepath.Join(root, "entry"), view} {
		if _, err := l.Load(t.Context(), scan); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator"}
	output := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
	record, _ := l.Index.Lookup(generator)
	assert.Diff(t, record.Refs, []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "external", Name: "foreign"},
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "input"},
	})
	assert.Diff(t, s.Productions(output, l.Index), []manifest.Production{{Owner: generator, Declaration: generator, Authored: generator, Source: manifest.BootstrapSourceID, File: "entry/generator.yaml"}})
	for _, unrelated := range []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "other", Name: "app-other"},
		{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-foreign"},
	} {
		if slices.ContainsFunc(s.Productions(unrelated, l.Index), func(p manifest.Production) bool { return p.Declaration == generator }) {
			t.Fatalf("static declaration escaped its namespace or source view: %s", unrelated)
		}
	}
	filter := change.NewFilter(change.NewSet([]string{"entry/consumer.yaml"}), l.Index, s, "")
	assert.Equal(t, filter.ShouldReconcile(generator), true)
	assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("apps", "consumer")), true)
	assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("other", "other")), false)
	assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("apps", "foreign")), false)
}
