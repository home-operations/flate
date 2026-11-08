package discovery

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveEntrySources_EvidenceAfterRemoval(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [source.yaml, child.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"entry/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps, uid: source-evidence}\nspec: {url: 'https://example.test/cluster'}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"entry/child.yaml",
		testutil.KustomizationYAML(
			"child",
			"apps",
			"  path: ./leaf\n  sourceRef: {kind: GitRepository, name: cluster}\n",
		),
	)
	st := store.New()
	l := loader.New(st)
	l.SourceRoot = root
	d := Session{
		cfg: Config{
			Path:     filepath.Join(root, "entry"),
			RepoRoot: root,
			Store:    st,
			SelfURLs: []string{"https://example.test/cluster"},
		},
		loader: l,
	}

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		_, err := l.Load(t.Context(), d.cfg.Path, manifest.NamedResource{}, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	l.Admit(root)
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "cluster",
	}

	{
		count, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil || count != 1, "initial evidence resolution = %d, %v", count, err)
	}

	before := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	child, childDoc, emission := st.GetEmission(testutil.KustomizationID("apps", "child"))
	emission.Finish()

	assert.FatalIf(t, !st.DeleteObject(id), "%v", "source was not admitted before removal")

	st.DeleteObject(testutil.KustomizationID("apps", "child"))

	assert.FatalIf(t, st.GetObject(id) != nil || st.GetArtifact(id, store.ArtifactContent) != nil ||
		st.GetObject(
			testutil.KustomizationID("apps", "child"),
		) != nil, "%v", "source removal retained published evidence")

	{
		count, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil || count != 1, "removed evidence resolution = %d, %v", count, err)
	}

	after, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t,
		!ok || !after.InputAlias || after.LocalRoot != before.LocalRoot || after.LocalPath != before.LocalPath ||
			st.GetObject(id) != nil,
		"loader evidence lost source identity: %+v",
		after,
	)

	{
		count, err := d.resolveEntrySources(
			t.Context(),
			true,
		)
		assert.FatalIf(t, err != nil || count != 0 ||
			st.GetArtifact(
				id,
				store.ArtifactContent,
			) != after, "unchanged evidence resolution = %d, %v", count, err)
	}

	st.AddObject(child, childDoc)

	{
		count, err := d.resolveEntrySources(
			t.Context(),
			false,
		)
		assert.FatalIf(t, err != nil || count != 0 ||
			st.GetArtifact(
				id,
				store.ArtifactContent,
			) != after, "source republication changed resolved evidence: %d, %v", count, err)
	}

	_, doc, attempt := st.GetEmission(id)
	attempt.Finish()

	assert.FatalIf(
		t,
		doc == nil || doc["metadata"].(map[string]any)["uid"] != "source-evidence",
		"republished source lost its document: %v",
		doc,
	)
}

func TestResolveEntrySources_IndexedRoots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, kind, role string
		want             bool
	}{
		{name: "git", kind: manifest.KindGitRepository, want: true},
		{name: "bucket", kind: manifest.KindBucket},
		{name: "helm repository", kind: manifest.KindHelmRepository},
		{name: "oci", kind: manifest.KindOCIRepository},
		{name: "covered", kind: manifest.KindGitRepository, role: "covered"},
		{name: "terminal", kind: manifest.KindGitRepository, role: "terminal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResolveEntrySourcesIndexedRootsCase(t, &tc)
		},
		)
	}
}

func TestRun_IndexedRootPreparedProjection(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "namespace: projected\nresources: [release.yaml]\n")
	testutil.WriteFile(t, root, "entry/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: orphan, namespace: authored}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source}
`)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n",
	)
	entries, err := Plan(t.Context(), Config{Path: filepath.Join(root, "entry"), RepoRoot: root})

	assert.FatalIf(t, err != nil, "%v", err)

	assert.FatalIf(
		t,
		len(entries) != 1 || len(entries[0].prepared.aliasViews) == 0,
		"plan did not prepare the indexed source: %+v",
		entries,
	)

	id := testutil.HelmReleaseID("projected", "orphan")
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "projected",
		Name:      "source",
	}
	t.Run("native", func(t *testing.T) {
		t.Parallel()
		testRunIndexedRootPreparedProjectionNative(
			t,
			testRunIndexedRootPreparedProjectionNativeInputs{root: &root, entries: &entries, id: &id, source: &source},
		)
	},
	)
	for _, prepared := range []bool{false, true} {
		t.Run(strconv.FormatBool(prepared), func(t *testing.T) {
			t.Parallel()
			testRunIndexedRootPreparedProjectionCase(
				t,
				testRunIndexedRootPreparedProjectionCaseInputs{
					entries:  &entries,
					id:       &id,
					source:   &source,
					prepared: &prepared,
				},
			)
		},
		)
	}
}

func TestResolveEntrySources_Identity(t *testing.T) {
	t.Parallel()
	t.Run("real supplied source", func(t *testing.T) {
		t.Parallel()
		const root = "/virtual/supplied-source"
		st := store.New()
		st.SetArtifact(manifest.BootstrapSourceID,
			&store.SourceArtifact{
				LocalPath: root,
				FS:        fstest.MapFS{"apps/file": {Data: []byte("input")}},
			})
		l := loader.New(st)
		src := &manifest.GitRepository{Name: "external", Namespace: "apps", URL: "https://fixture.invalid/external"}
		l.AdmitDeclarations([]store.Declaration{{Obj: src, Origin: manifest.SuppliedDeclaration}})
		for _, name := range []string{"first", "second"} {
			st.AddObject(&manifest.Kustomization{
				Name:      name,
				Namespace: "apps",
				Path:      "./apps",
				SourceRef: kustomizev1.CrossNamespaceSourceReference{
					Kind: manifest.KindGitRepository,
					Name: src.Name,
				},
			})
		}
		d := Session{cfg: Config{Path: root, Store: st}, loader: l}
		count, err := d.resolveEntrySources(t.Context(), true)
		if err != nil || count != 0 || st.GetObject(src.Named()) != src ||
			st.GetArtifact(src.Named(), store.ArtifactContent) != nil {
			t.Fatalf("real source became an alias: count=%d err=%v", count, err)
		}
	})

	for _, tc := range []struct {
		kind, group, namespace, name, path string
		want                               bool
	}{
		{manifest.KindGitRepository, manifest.SourceDomain, "flux-system", "source", "", true},
		{manifest.KindOCIRepository, manifest.SourceDomain, "other", "source", "", false},
		{manifest.KindBucket, manifest.SourceDomain, "other", "source", "", false},
		{manifest.KindHelmRepository, manifest.SourceDomain, "other", "source", "", false},
		{manifest.KindExternalArtifact, manifest.SourceDomain, "other", "source", "", false},
		{manifest.KindGitRepository, "wrong.group", "other", "source", "", false},
		{manifest.KindGitRepository, manifest.SourceDomain, "", "source", "", false},
		{manifest.KindGitRepository, manifest.SourceDomain, "other", "", "", false},
		{"", "", "other", "", "./apps", false},
		{manifest.KindOCIRepository, manifest.SourceDomain, "other", "", "./apps", false},
	} {
		t.Run(tc.kind+"/"+tc.group+"/"+tc.namespace+"/"+tc.name+"/"+tc.path, func(t *testing.T) {
			t.Parallel()
			testResolveEntrySourcesIdentityCase(t, &tc)
		},
		)
	}
	for _, tc := range []struct {
		name string
		obj  manifest.BaseManifest
		want bool
	}{
		{
			name: "chart reference without path fact",
			obj: &manifest.HelmRelease{
				Name: "release", Namespace: "apps",
				ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "source"},
			},
		},
		{
			name: "artifact reference without path fact",
			obj: &manifest.RawObject{
				APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps",
				Spec: map[string]any{"sources": []any{map[string]any{"kind": manifest.KindOCIRepository, "name": "source"}}},
			},
		},
		{
			name: "loader witness without path fact",
			obj: &manifest.RawObject{
				APIVersion: manifest.FluxKustomizeDomain + "/v1", Kind: manifest.KindKustomization, Name: "lookalike", Namespace: "apps",
				Spec: map[string]any{"path": ".", "sourceRef": map[string]any{"kind": manifest.KindOCIRepository, "name": "source"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResolveEntrySourcesIdentityCase2(t, &tc)
		},
		)
	}
	for _, scenario := range []string{"supplied", "preseeded", "disk"} {
		t.Run("input precedence/"+scenario, func(t *testing.T) {
			t.Parallel()
			testResolveEntrySourcesIdentityCase3(t, &scenario)
		},
		)
	}
	t.Run(
		"separate identities",
		func(t *testing.T) { t.Parallel(); testResolveEntrySourcesIdentitySeparateIdentities(t) },
	)
}

func testResolveEntrySourcesIndexedRootsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		kind string
		role string
		want bool
	},
) {
	t.Helper()

	root := t.TempDir()
	id := testutil.HelmReleaseID("apps", "orphan")
	declaration := fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: orphan, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: %s, name: source}
`, testCaseInput.kind)

	if testCaseInput.kind == manifest.KindOCIRepository {
		declaration = "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: orphan, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: source}}\n"
	}
	testutil.WriteFile(t, root, "entry/release.yaml", declaration)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n",
	)
	st := store.New()
	switch testCaseInput.role {
	case "covered":
		st.AddObject(&manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./entry"})
	case "terminal":
		testutil.WriteFile(
			t,
			root,
			"entry/kustomization.yaml",
			"configMapGenerator:\n- name: held\n  files: [release.yaml]\n",
		)
	}
	l := loader.New(st)
	l.SourceRoot = root
	d := Session{cfg: Config{Path: filepath.Join(root, "entry"), RepoRoot: root, Store: st}, loader: l}

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		_, err := l.Load(t.Context(), d.cfg.Path, manifest.NamedResource{}, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      testCaseInput.kind,
		Namespace: "apps",
		Name:      "source",
	}
	l.Admit(root)
	if testCaseInput.role == "terminal" {
		l.Index.Records[id] = manifest.DiscoveryRecord{
			Authored: id,
			Path:     filepath.Join(root, "entry", "release.yaml"),
			File:     "entry/release.yaml",
			Refs:     []manifest.NamedResource{source},
		}
	}

	assert.FatalIf(
		t,
		st.GetObject(id) != nil,
		"%v",
		"indexed release was published before orphan promotion",
	)

	record, ok := l.Index.Lookup(id)
	if !ok ||
		selection.Root(
			rootAtForTest(l, l.IndexedObject(id, l.Index.Records[id]).Obj, record.Path),
		).Admitted != (testCaseInput.role == "") {
		t.Fatalf(
			"indexed root admission = %t, record = %+v",
			selection.Root(rootAtForTest(l, l.IndexedObject(id, l.Index.Records[id]).Obj, record.Path)).Admitted,
			record,
		)
	}
	count, err := d.resolveEntrySources(t.Context(), true)

	assert.FatalIf(
		t,
		err != nil || (count == 1) != testCaseInput.want,
		"indexed source resolution = %d, %v; want alias %t",
		count,
		err,
		testCaseInput.want,
	)

	artifact, ok := st.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t, testCaseInput.
			want && (!ok || !artifact.InputAlias || st.GetObject(source).Named() != source) ||
			!testCaseInput.want && (ok || st.GetObject(source) != nil), "indexed alias = %+v",
		artifact,
	)

	assert.FatalIf(t, st.GetObject(id) != nil, "%v", "source census published an orphan release")

	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	if testCaseInput.kind == manifest.KindGitRepository && testCaseInput.want &&
		!slices.Contains(
			l.Index.Readers["charts/demo/templates/cm.yaml"],
			manifest.FileReader{Reader: id, Source: source, Exact: true},
		) {
		t.Fatalf("source was unavailable when chart readers froze: %+v", l.Index.Readers)
	}
}

type testRunIndexedRootPreparedProjectionNativeInputs struct {
	root    *string
	entries *[]Entry
	id      *manifest.NamedResource
	source  *manifest.NamedResource
}

func testRunIndexedRootPreparedProjectionNative(
	t *testing.T,
	testCaseInput testRunIndexedRootPreparedProjectionNativeInputs,
) {
	t.Helper()

	st := store.New()
	d := newDiscoverer(
		&Config{
			Input: (*testCaseInput.entries)[0].Input,
			Entry: &(*testCaseInput.entries)[0],
			Path:  (*testCaseInput.entries)[0].Path,
			Store: st,
		},
	)

	{
		_, err := d.resolveEntrySources(t.Context(), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	{
		err := d.loadManifests(t.Context(), (*testCaseInput.root), false)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	if st.GetArtifact((*testCaseInput.source), store.ArtifactContent) == nil ||
		st.GetObject((*testCaseInput.id)) != nil {
		t.Fatalf(
			"native admission lost projected source or published the release: source=%v release=%v",
			st.GetArtifact((*testCaseInput.source), store.ArtifactContent),
			st.GetObject((*testCaseInput.id)),
		)
	}
}

type testRunIndexedRootPreparedProjectionCaseInputs struct {
	entries  *[]Entry
	id       *manifest.NamedResource
	source   *manifest.NamedResource
	prepared *bool
}

func testRunIndexedRootPreparedProjectionCase(
	t *testing.T,
	testCaseInput testRunIndexedRootPreparedProjectionCaseInputs,
) {
	t.Helper()

	st := store.New()
	cfg := Config{
		Input: (*testCaseInput.entries)[0].Input,
		Path:  (*testCaseInput.entries)[0].Path,
		Store: st,
		Trees: testTrees(t),
	}
	if *testCaseInput.prepared {
		cfg.Entry = &(*testCaseInput.entries)[0]

		assert.FatalIf(
			t,
			newDiscoverer(&cfg).loader.Options.ParsedInputs == nil,
			"%v",
			"matching prepared input was not reused",
		)
	}
	result, err := Run(t.Context(), cfg)

	assert.FatalIf(t, err != nil, "%v", err)

	record, indexed := result.Index.Lookup((*testCaseInput.id))
	if !indexed || record.Parent != result.Binding.Producer || st.GetObject((*testCaseInput.id)) != nil ||
		st.GetArtifact((*testCaseInput.source), store.ArtifactContent) == nil ||
		st.GetObject((*testCaseInput.source)) == nil {
		t.Fatalf(
			"projected release ownership or source missing: record=%+v release=%v source=%v",
			record,
			st.GetObject((*testCaseInput.id)),
			st.GetObject((*testCaseInput.source)),
		)
	}
	if !slices.Contains(
		result.Index.Readers["charts/demo/templates/cm.yaml"],
		manifest.FileReader{Reader: (*testCaseInput.id), Source: (*testCaseInput.source), Exact: true},
	) {
		t.Fatalf(
			"projected chart reader missing after prepared=%t: %+v",
			(*testCaseInput.prepared),
			result.Index.Readers,
		)
	}
}

func testResolveEntrySourcesIdentityCase(t *testing.T, testCaseInput *struct {
	kind string

	group     string
	namespace string
	name      string
	path      string
	want      bool
},
) {
	t.Helper()

	root := filepath.Join(t.TempDir(), ".flate-base", "missing")
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{
			LocalPath: root,
			LocalRoot: root,
			FS:        fstest.MapFS{"apps/file": {Data: []byte("input")}},
		},
	)
	ref := kustomizev1.CrossNamespaceSourceReference{Kind: testCaseInput.kind, Name: testCaseInput.name}
	if testCaseInput.group != "" {
		ref.APIVersion = testCaseInput.group + "/v1"
	}
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: testCaseInput.namespace,
		Path:      testCaseInput.path,
		SourceRef: ref,
	}
	st.AddObject(ks)
	d := Session{cfg: Config{Path: root, Store: st}}
	count, err := d.resolveEntrySources(t.Context(), true)

	assert.FatalIf(t, err != nil || (count == 1) != testCaseInput.want, "resolution = %d, %v", count, err)

	obj := st.GetObject(ks.SourceID())

	assert.FatalIf(t, testCaseInput.want && (obj == nil || obj.Named() != ks.SourceID()) ||
		!testCaseInput.want &&
			(obj != nil || st.GetArtifact(ks.SourceID(), store.ArtifactContent) != nil), "identity = %v", obj)
}

func testResolveEntrySourcesIdentityCase2(t *testing.T, testCaseInput *struct {
	name string

	obj  manifest.BaseManifest
	want bool
},
) {
	t.Helper()

	root := t.TempDir()
	st := store.New()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{"file": {}}},
	)
	st.AddObject(testCaseInput.obj)
	d := Session{cfg: Config{Path: root, Store: st}, loader: loader.New(st)}
	count, err := d.resolveEntrySources(t.Context(), true)

	assert.FatalIf(
		t,
		err != nil || (count == 1) != testCaseInput.want,
		"generic reference resolution = %d, %v",
		count,
		err,
	)

	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "source",
	}
	artifact, ok := st.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(
		t, testCaseInput.
			want && (!ok || artifact.LocalRoot != root || st.GetObject(id).Named() != id) ||
			!testCaseInput.want &&
				(ok || st.GetObject(id) != nil), "generic reference identity or root changed: %+v",
		artifact,
	)
}

func testResolveEntrySourcesIdentityCase3(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	root := filepath.Join(t.TempDir(), ".flate-base", "missing")
	seedRoot := filepath.Join(t.TempDir(), "preseeded")
	st := store.New()
	supplied := &store.SourceArtifact{
		LocalPath: root,
		LocalRoot: root,
		FS:        fstest.MapFS{"file": {Data: []byte("supplied")}},
	}
	preseeded := &store.SourceArtifact{
		LocalPath: seedRoot,
		LocalRoot: seedRoot,
		FS:        fstest.MapFS{"file": {Data: []byte("preseeded")}},
	}
	cfg := Config{Store: st, SelfURLs: []string{"https://example.test/cluster"}}
	want := supplied
	switch *testCaseInput {
	case "supplied":
		cfg.Input = supplied
		st.SetArtifact(manifest.BootstrapSourceID, preseeded)
	case "preseeded":
		st.SetArtifact(manifest.BootstrapSourceID, preseeded)
		want = preseeded
	case "disk":
		cfg.Path = t.TempDir()
		testutil.WriteFile(t, cfg.Path, "file", "disk")
		want = nil
	}
	source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", URL: cfg.SelfURLs[0]}
	st.AddObject(source)
	d := Session{cfg: cfg}

	{
		_, err := d.resolveEntrySources(t.Context(), true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	got := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if d.cfg.Path != got.LocalPath {
		t.Fatalf("scan coordinate = %q, want admitted root %q", d.cfg.Path, got.LocalPath)
	}

	assert.FatalIf(t, want != nil && got != want, "bootstrap = %+v, want authoritative %+v", got, want)

	body, err := fs.ReadFile(got.FS, "file")

	assert.FatalIf(
		t,
		err != nil || string(body) != (*testCaseInput),
		"bootstrap bytes = %q, %v, want %s",
		body,
		err, (*testCaseInput),
	)

	assert.FatalIf(t, st.GetObject(source.Named()) != source, "%v", "authored source replaced")

	alias := st.GetArtifact(source.Named(), store.ArtifactContent).(*store.SourceArtifact)

	assert.FatalIf(t, alias.ContentFingerprint == nil, "%v", "alias content fingerprint is absent")

	fingerprint, err := alias.ContentFingerprint(t.Context(), alias.FS)

	assert.FatalIf(
		t,
		err != nil || fingerprint == "",
		"alias callback did not identify admitted bytes: %q, %v",
		fingerprint,
		err,
	)

	assert.FatalIf(
		t,
		want != nil && want.ContentFingerprint != nil,
		"%v",
		"SDK bootstrap must not supply the alias fingerprint",
	)

	body, err = fs.ReadFile(alias.FS, "file")
	if err != nil || string(body) != (*testCaseInput) || alias.Key != "" {
		t.Fatalf("alias bytes = %q, %v; key = %q", body, err, alias.Key)
	}
	if want != nil {
		{
			_, err := os.Stat(want.LocalPath)
			assert.FatalIf(
				t,
				!errors.Is(err, fs.ErrNotExist),
				"synthetic label unexpectedly exists: %v",
				err,
			)
		}
	}
}
