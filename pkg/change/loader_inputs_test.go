package change_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
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
			testutil.WriteFile(
				t,
				root,
				"entry/sibling.json",
				`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"sibling"}}`,
			)
			testutil.WriteFile(t, root, overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(base)+"]\n")
			testutil.WriteFile(
				t,
				root,
				"shared/kustomization.yaml",
				"resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\nconfigMapGenerator: [{name: values, files: [key=../inputs/value], env: ../inputs/env}]\nreplacements: [{path: ../inputs/missing}]\n",
			)
			testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
			testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
			testutil.WriteFile(
				t,
				root,
				"inputs/value",
				testutil.KustomizationYAML("terminal", "apps", "  path: ./hidden\n"),
			)
			testutil.WriteFile(t, root, "inputs/env", "KEY=value\n")
			s := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
			s.AddObject(owner)
			l := loader.New(s)
			l.SourceRoot = root
			admitForFilter(s, root)

			{
				_, err := l.Load(t.Context(), filepath.Join(root, "entry"), nil)
				assert.FatalIf(t, err != nil, "%v", err)
			}

			{
				err := l.FinalizeIndex(t.Context(), root, true)
				assert.FatalIf(t, err != nil, "%v", err)
			}

			if overlay != "entry" {
				assert.Diff(
					t,
					l.Index.Readers["entry/sibling.json"],
					[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
				)

				assert.FatalIf(
					t,
					len(l.Index.Readers["entry/README.md"]) != 0,
					"%v",
					"non-manifest implicit sibling became a reader",
				)
			}
			for _, file := range []string{overlay + "/kustomization.yaml", "shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch", "inputs/value", "inputs/env", "inputs/missing"} {
				assert.Diff(
					t,
					l.Index.Readers[file],
					[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
				)

				assert.ErrorIf(
					t,
					!slices.Contains(ownersForFilter(l.Index, file), owner.Named()),
					"owner missing for %s",
					file,
				)

				filter := admissiontest.Filter(
					t,
					admissiontest.Config{
						Changes:   change.NewSet([]string{file}),
						Index:     l.Index,
						Inventory: s,
						Root:      "",
					},
				)

				assert.ErrorIf(t, !filter.ShouldReconcile(owner.Named()), "changed %s did not select owner", file)
			}

			{
				id := testutil.KustomizationID(
					"apps",
					"terminal",
				)
				assert.FatalIf(t, s.GetObject(id) != nil ||
					len(l.Index.Records[id].Productions) != 0, "%v", "terminal input became an object or producer")
			}

			assert.FatalIf(
				t,
				admissiontest.Filter(t, admissiontest.Config{Changes: change.NewSet([]string{"inputs/unrelated"}), Index: l.Index, Inventory: s, Root: ""}).
					ShouldReconcile(owner.Named()),
				"%v",
				"unrelated input selected owner",
			)
		})
	}
}

func TestFilter_DuplicateDocumentKeys(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [../shared]\nnamespace: one\nnamespace: two\n")
	testutil.WriteFile(
		t,
		root,
		"shared/kustomization.yaml",
		"resources: [cm.yaml]\npatches: [{path: ../inputs/ignored, path: ../inputs/patch}]\n",
	)
	testutil.WriteFile(t, root, "shared/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n")
	testutil.WriteFile(t, root, "inputs/ignored", "[]\n")
	testutil.WriteFile(t, root, "inputs/patch", "- op: add\n  path: /data\n  value: {key: changed}\n")
	testutil.WriteFile(t, root, "alias/kustomization.yaml", "namespace: one\nx: &namespace y\n*namespace: z\n")
	testutil.WriteFile(
		t,
		root,
		"merge/kustomization.yaml",
		"a: &a {namespace: one}\nb: &b {namePrefix: pre-}\n<<: *a\n<<: *b\n",
	)
	s := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"}
	s.AddObject(owner)
	l := loader.New(s)
	l.SourceRoot = root
	admitForFilter(s, root)
	_, err := l.Load(t.Context(), filepath.Join(root, "entry"), nil)
	assert.NoError(t, err)

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	for _, file := range []string{"shared/kustomization.yaml", "shared/cm.yaml", "inputs/patch"} {
		assert.Diff(
			t,
			l.Index.Readers[file],
			[]manifest.FileReader{{Reader: owner.Named(), Source: manifest.BootstrapSourceID}},
		)

		assert.ErrorIf(
			t,
			!slices.Contains(ownersForFilter(l.Index, file), owner.Named()),
			"owner missing for %s",
			file,
		)
	}

	assert.ErrorIf(
		t,
		!admissiontest.Filter(t, admissiontest.Config{Changes: change.NewSet([]string{"inputs/patch"}), Index: l.Index, Inventory: s, Root: ""}).
			ShouldReconcile(owner.Named()),
		"%v",
		"changed terminal patch did not select owner",
	)
}

func TestFinalizeIndex_InputSourceViews(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"mounted", "alias", "pinned"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexInputSourceViewsCase(t, &mode)
		},
		)
	}
}

func TestFinalizeIndex_SourceValuesReaders(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"working", "alias-subtree", "committed", "external", "unavailable", "missing", "foreign"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFinalizeIndexSourceValuesReadersCase(t, &mode)
		},
		)
	}
	t.Run("chartRef", func(t *testing.T) { t.Parallel(); testFinalizeIndexSourceValuesReadersChartRef(t) })
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
			testFinalizeIndexOwnersOfFileResourcesEscapeCase(
				t,
				&tc,
			)
		},
		)
	}
}

func testFinalizeIndexInputSourceViewsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	local := filepath.Join(root, ".artifacts", (*testCaseInput))
	input := fstest.MapFS{
		"entry/nested/kustomization.yaml": {Data: []byte("resources: [../../shared]\n")},
		"shared/kustomization.yaml":       {Data: []byte("patches: [{path: ../inputs/patch}]\n")},
	}
	testutil.WriteFile(t, local, "shared/kustomization.yaml", "patches: [{path: ../inputs/host-only}]\n")
	s := store.New()
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	s.AddObject(source)
	owner := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "apps",
		Path:      "./entry",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      source.Name,
			Namespace: source.Namespace,
		},
	}
	s.AddObject(owner)
	art := &store.SourceArtifact{LocalRoot: root, LocalPath: local, FS: input}
	if (*testCaseInput) == "alias" {
		art.InputAlias, art.LocalRoot = true, filepath.Join(root, "subtree")
	}
	if (*testCaseInput) == "pinned" {
		art.Key, art.Content = "pinned", "pinned"
	}
	s.SetArtifact(source.Named(), art)
	l := loader.New(s)
	l.SourceRoot = root
	admitForFilter(s, root)
	_, err := l.Load(t.Context(), filepath.Join(local, "entry"), nil)
	assert.NoError(t, err)

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	prefix := ".artifacts/" + (*testCaseInput) + "/"
	assert.Diff(
		t,
		l.Index.Readers[prefix+"inputs/patch"],
		[]manifest.FileReader{{Reader: owner.Named(), Source: source.Named()}},
	)

	assert.FatalIf(
		t,
		len(l.Index.Readers[prefix+"inputs/host-only"]) != 0,
		"%v",
		"discovery read host files outside the mounted view",
	)

	file := "inputs/patch"
	if (*testCaseInput) == "alias" {
		file = "subtree/inputs/patch"
	}
	filter := admissiontest.Filter(
		t,
		admissiontest.Config{Changes: change.NewSet([]string{file}), Index: l.Index, Inventory: s, Root: ""},
	)
	assert.Equal(t, filter.ShouldReconcile(owner.Named()), (*testCaseInput) != "pinned")
	assert.Equal(t, filter.ShouldReconcile(source.Named()), (*testCaseInput) != "pinned")
	if (*testCaseInput) == "pinned" {
		assert.Diff(t, l.Index.Readers[file], []manifest.FileReader(nil))
	}
}

func testFinalizeIndexSourceValuesReadersCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

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
	if (*testCaseInput) != "working" {
		testutil.WriteFile(t, root, "charts/demo/host-only.yaml", "must not be read")
	}
	files, err := json.Marshal(
		[]string{
			"./config/prod.yaml",
			"config/prod.yaml",
			"config/missing.yaml",
			"../escape.yaml",
			"/absolute.yaml",
			"config/../../escape.yaml",
			"",
			".",
			"bad\x00.yaml",
		},
	)
	assert.NoError(t, err)
	group := manifest.SourceDomain
	if (*testCaseInput) == "foreign" {
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
		want := []string{
			"config/prod.yaml",
			"config/missing.yaml",
			"charts/demo/.helmignore",
			"charts/demo/Chart.yaml",
			"charts/demo/values.yaml",
			"charts/demo/templates/cm.yaml",
			"charts/demo/charts/child.tgz",
		}
		switch *testCaseInput {
		case "alias-subtree":
			art.LocalPath = filepath.Join(root, ".artifacts", "alias")
			art.LocalRoot = filepath.Join(root, "subtree")
			art.InputAlias, art.Key = true, "derived"
			aliased := make([]string, 0, 2*len(want))
			for _, file := range want {
				aliased = append(aliased, path.Join(".artifacts/alias", file), path.Join("subtree", file))
			}
			want = aliased
		case "committed":
			art.LocalPath, art.Key, art.Content = filepath.Join(
				root,
				".artifacts",
				"commit",
			), "committed", "committed"
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
		if (*testCaseInput) != "missing" {
			st.AddObject(source)
			st.SetArtifact(source.Named(), art)
		}
		if (*testCaseInput) == "working" {
			input.FS = os.DirFS(root)
			st.SetArtifact(manifest.BootstrapSourceID, art)
		}
		_, err := l.Load(t.Context(), filepath.Join(root, "manifests"), nil)
		assert.NoError(t, err)
		return l, input, want
	}
	l, input, want := newLoader()

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	id := testutil.HelmReleaseID("apps", "demo")
	record, _ := l.Index.Lookup(id)
	slices.Sort(want)
	assert.Diff(t, readFilesForFilter(l.Index, id), want)
	assert.Equal(t, len(record.Inputs), 0)
	for _, readers := range l.Index.Readers {
		assert.Diff(
			t,
			readers,
			[]manifest.FileReader{
				{
					Reader: id,
					Source: (manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "cluster", Namespace: "flux-system"}),
					Exact:  true,
				},
			},
		)
	}
	assert.Equal(t, input.opens, 0)
	assert.Equal(t, len(l.Index.Claims), 0)
	assert.Equal(t, len(l.Index.Records), 1)
	record, _ = l.Index.Lookup(id)
	assert.Equal(t, len(record.Productions), 1)
	filter := admissiontest.Filter(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"config/prod.yaml"}),
			Index:     l.Index,
			Inventory: l.Store,
			Root:      "",
		},
	)
	assert.Equal(t, filter.ShouldReconcile(id), (*testCaseInput) == "working")
	snapshot := snapshotIndexForFilter(l.Index)

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	assertFilterIndexForTest(t, *l.Index, snapshot)
	assert.Equal(t, input.opens, 0)
	fresh, freshInput, _ := newLoader()

	{
		err := fresh.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	assertFilterIndexForTest(t, *fresh.Index, snapshot)
	assert.Equal(t, freshInput.opens, 0)
}

func testFinalizeIndexSourceValuesReadersChartRef(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [chart.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"entry/chart.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: chart, namespace: apps}\nspec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: values}, valuesFiles: [config/prod.yaml, config/prod.yaml, entry/kustomization.yaml]}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"hr.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {chartRef: {kind: HelmChart, name: chart}}\n",
	)
	st := store.New()
	admitForFilter(st, root)
	source := &manifest.GitRepository{Name: "values", Namespace: "apps"}
	st.AddObject(source)
	input := &readerForFilter{
		FS: fstest.MapFS{
			"config/prod.yaml":         {Data: []byte("invalid: [")},
			"entry/kustomization.yaml": {Data: []byte("not bootstrap")},
		},
		missing: "config/prod.yaml",
	}
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{
			LocalPath:  filepath.Join(root, ".selected"),
			LocalRoot:  root,
			InputAlias: true,
			FS:         input,
		},
	)
	l := loader.New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, nil)
	assert.NoError(t, err)
	chartID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "chart",
	}
	chart := &manifest.HelmChartSource{Name: chartID.Name, Namespace: chartID.Namespace}
	chart.Chart = "charts/demo"
	chart.SourceRef.Kind, chart.SourceRef.Name = source.Named().Kind, source.Name
	chart.ValuesFiles = []string{"config/prod.yaml", "config/prod.yaml", "entry/kustomization.yaml"}
	l.RegisterBuilt(
		loader.BuiltInput{
			Root:   filepath.Join(root, "entry"),
			Source: manifest.BootstrapSourceID,
			Owner: manifest.NamedResource{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "apps",
				Name:      "entry",
			},
		},
		[]store.Child{{Obj: chart}},
	)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	consumer := testutil.HelmReleaseID("apps", "demo")
	want := []manifest.FileReader{
		{Reader: consumer, Source: source.Named(), Exact: true},
		{Reader: chartID, Source: source.Named(), Exact: true},
	}
	slices.SortFunc(want, func(a, b manifest.FileReader) int { return a.Reader.Compare(b.Reader) })
	assert.Diff(t, l.Index.Readers["config/prod.yaml"], want)
	assert.Diff(t, l.Index.Readers[".selected/config/prod.yaml"], want)
	want = []manifest.FileReader{
		{Reader: consumer, Source: source.Named(), Exact: true},
		{Reader: consumer, Source: manifest.BootstrapSourceID, Declaration: chartID},
		{Reader: chartID, Source: source.Named(), Exact: true},
	}
	slices.SortFunc(want, func(a, b manifest.FileReader) int {
		if order := a.Reader.Compare(b.Reader); order != 0 {
			return order
		}
		return a.Source.Compare(b.Source)
	})
	assert.Diff(t, l.Index.Readers["entry/kustomization.yaml"], want)
	record, _ := l.Index.Lookup(chartID)
	assert.Equal(t, slices.Contains(record.Inputs, "entry/kustomization.yaml"), true)
	assert.Equal(
		t,
		slices.Contains(
			l.Index.Readers["entry/kustomization.yaml"],
			manifest.FileReader{Reader: chartID, Source: manifest.BootstrapSourceID},
		),
		false,
	)
	assert.Equal(t, input.opens, 0)
	snapshot := snapshotIndexForFilter(l.Index)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assertFilterIndexForTest(t, *l.Index, snapshot)
	assert.Equal(t, input.opens, 0)
}

func testFinalizeIndexOwnersOfFileResourcesEscapeCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		overlay string
		mixed   bool
	},
) {
	t.Helper()

	dir := t.TempDir()
	base, err := filepath.Rel(testCaseInput.overlay, "apps/base/postgres")
	assert.NoError(t, err)
	testutil.WriteFile(
		t,
		dir, testCaseInput.
			overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(base)+"]\n",
	)
	if testCaseInput.mixed {
		testutil.WriteFile(
			t,
			dir,
			"apps/homelab/sibling.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\n",
		)
	}
	testutil.WriteFile(
		t,
		dir,
		"apps/base/postgres/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cluster.yaml\npatches: [{path: ../../../inputs/patch}]\n",
	)
	testutil.WriteFile(t, dir, "apps/base/postgres/cluster.yaml",
		"apiVersion: postgresql.cnpg.io/v1\nkind: Cluster\nmetadata:\n  name: postgres\n")

	s := store.New()
	want := make([]manifest.FileReader, 0, 2)
	for i := range 2 {
		ks := &manifest.Kustomization{
			Name:      fmt.Sprintf("apps-%d", i),
			Namespace: "flux-system",
			Path:      "./apps/homelab",
		}
		s.AddObject(ks)
		want = append(want, manifest.FileReader{Reader: ks.Named(), Source: manifest.BootstrapSourceID})
	}

	idx := rendererForFilter(t, loader.New(s), dir)

	for _, file := range []string{
		testCaseInput.overlay + "/kustomization.yaml", "apps/base/postgres/kustomization.yaml",
		"apps/base/postgres/cluster.yaml",
		"inputs/patch",
	} {
		{
			got := idx.Readers[file]
			assert.ErrorIf(t, !slices.Equal(got, want), "Readers[%s] = %v, want %v", file, got, want)
		}
	}
	filter := admissiontest.Filter(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"apps/base/postgres/cluster.yaml"}),
			Index:     idx,
			Inventory: s,
			Root:      "",
		},
	)
	{
		idValues := want
		for idIndex := range idValues {
			id := idValues[idIndex]

			assert.ErrorIf(
				t,
				!filter.ShouldReconcile(id.Reader),
				"changed shared base did not select %s",
				id.Reader,
			)
		}
	}
}
