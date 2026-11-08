package loader

import (
	"encoding/json"
	"path"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/selection"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPromoteOrphans_IndexedEvidence(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"a.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x, namespace: default}\ndata: {VALUE: X}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\ndata: {VALUE: A}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling, namespace: default}\ndata: {VALUE: S}\n",
			),
		},
		"z.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared, namespace: default}\ndata: {VALUE: Z}\n",
			),
		},
		"generator.yaml": {
			Data: []byte(
				"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: default, uid: authored, annotations: {source.extensions.fluxcd.io/reconcile: disabled}}\nspec: {sources: [{alias: repo, kind: GitRepository, name: input}]}\n",
			),
		},
	}
	x := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "x"}
	shared := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "shared"}
	sibling := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "sibling"}
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "default",
		Name:      "generator",
	}
	for _, tc := range []struct {
		name      string
		order     []manifest.NamedResource
		chartPath string
	}{
		{name: "x_then_shared", order: []manifest.NamedResource{x, shared}},
		{name: "shared_then_x", order: []manifest.NamedResource{shared, x}},
		{name: "discovery"},
		{name: "root chart", chartPath: "."},
		{name: "nested chart", chartPath: "charts/demo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testPromoteOrphansIndexedEvidenceCase(
				t,
				testPromoteOrphansIndexedEvidenceCaseInputs{
					input:     &input,
					x:         &x,
					shared:    &shared,
					sibling:   &sibling,
					generator: &generator,
					tc:        &tc,
				},
			)
		},
		)
	}
}

func TestPromoteOrphans_NamespaceAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		indexed, siblings bool
	}{
		{name: "raw requested"},
		{name: "raw siblings", siblings: true},
		{name: "indexed requested", indexed: true},
		{name: "indexed siblings and cluster scope", indexed: true, siblings: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testPromoteOrphansNamespaceAdmissionCase(t, &tc)
		},
		)
	}
}

func TestPromoteOrphans_ChecksEachSiblingsProducer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(
		t,
		dir,
		"apps/bundle.yaml",
		testutil.KustomizationYAML(
			"self",
			"authored",
			"  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n",
		)+"---\n"+
			`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: child, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: source, namespace: apps}
---
apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps, uid: authored}
spec: {sources: [{alias: repo, kind: GitRepository, name: source}]}
`,
	)
	st := store.New()
	admitForTest(st, dir)
	l := New(st)
	self := testutil.KustomizationID("apps", "self")
	child := testutil.HelmReleaseID("apps", "child")
	generator := manifest.NamedResource{
		Group:     "source.extensions.fluxcd.io",
		Kind:      "ArtifactGenerator",
		Namespace: "apps",
		Name:      "generator",
	}
	for _, id := range []manifest.NamedResource{self, child, generator} {
		record := manifest.DiscoveryRecord{
			Authored: id,
			Path:     filepath.Join(dir, "apps", "bundle.yaml"),
			File:     "apps/bundle.yaml",
		}
		if id == self {
			record.Authored.Namespace = "authored"
		}
		l.Index.Records[id] = record
	}
	l.Index.Claims = map[string][]manifest.NamedResource{"apps/": {self}}
	l.PromoteOrphans(t.Context())

	assert.ErrorIf(t, st.GetObject(self) == nil, "%v", "self-excluded orphan must be admitted")

	assert.ErrorIf(t, st.GetObject(child) != nil, "%v", "orphan promotion must not admit a parent-owned sibling")

	assert.ErrorIf(t, st.GetObject(generator) != nil, "%v", "orphan promotion admitted a parent-owned generator")
}

func TestPromoteOrphans_GraphOwnedSiblingsAndSelf(t *testing.T) {
	t.Parallel()
	t.Run("omitted mixed file", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		body := testutil.KustomizationYAML(
			"disabled",
			"apps",
			"  path: ./target\n",
		) + "---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: sibling, namespace: apps}\nspec: {chartRef: {kind: OCIRepository, name: chart}}\n"
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: []\n")
		testutil.WriteFile(t, root, "apps/disabled.yaml", body)
		testutil.WriteFile(t, root, "target/kustomization.yaml", "resources: []\n")
		st := store.New()
		l := New(st)
		l.SourceRoot = root

		{
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		l.Admit(root)

		{
			err := l.FinalizeIndex(t.Context(), root, true)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		l.PromoteOrphans(t.Context())
		for _, id := range []manifest.NamedResource{testutil.KustomizationID("apps", "disabled"), testutil.HelmReleaseID("apps", "sibling")} {
			assert.FatalIf(t, st.GetObject(id) != nil, "omitted file promoted %s", id)
		}
	})

	dir := t.TempDir()
	body := testutil.KustomizationYAML(
		"self",
		"apps",
		"  {path: ./apps, sourceRef: {kind: GitRepository, name: source}}\n",
	) + "---\n" +
		`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: child, namespace: apps}
spec: {chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: source}}}}
`
	testutil.WriteFile(t, dir, "bundle.yaml", body)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [../bundle.yaml]\n")
	st := store.New()
	admitForTest(st, dir)
	docs, err := testutil.SplitDocs([]byte(body))

	assert.FatalIf(t, err != nil, "%v", err)

	result1, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
	ks := result1.Object

	assert.FatalIf(t, err != nil, "%v", err)

	st.AddObject(ks)
	source := &manifest.GitRepository{Name: "source", Namespace: ks.Named().Namespace}
	st.AddObject(source)
	st.SetArtifact(
		source.Named(),
		st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
	)
	l := New(st)

	{
		err := l.FinalizeIndex(t.Context(), dir, true)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	self := ks.Named()
	st.DeleteObject(self)
	child := testutil.HelmReleaseID("apps", "child")
	for _, id := range []manifest.NamedResource{self, child} {
		l.Index.Records[id] = manifest.DiscoveryRecord{
			Authored: id,
			Path:     filepath.Join(dir, "bundle.yaml"),
			File:     "bundle.yaml",
		}
	}
	l.PromoteOrphans(t.Context())

	assert.ErrorIf(t, st.GetObject(self) == nil, "%v", "self-owned Kustomization must be admitted")

	assert.ErrorIf(t, st.GetObject(child) != nil, "%v", "graph-owned sibling bypassed per-document admission")
}

type testPromoteOrphansIndexedEvidenceCaseInputs struct {
	input     *fstest.MapFS
	x         *manifest.NamedResource
	shared    *manifest.NamedResource
	sibling   *manifest.NamedResource
	generator *manifest.NamedResource
	tc        *struct {
		name string

		order     []manifest.NamedResource
		chartPath string
	}
}

func testPromoteOrphansIndexedEvidenceCase(t *testing.T, testCaseInput testPromoteOrphansIndexedEvidenceCaseInputs) {
	t.Helper()
	const root = "/virtual/orphan-evidence"

	st := store.New()
	view := fstest.MapFS{}
	for name, file := range *testCaseInput.input {
		view[path.Join(testCaseInput.tc.chartPath, name)] = file
	}
	var documentReads atomic.Int64
	inputFS := parsedReadFS{
		FS:    view,
		reads: map[string]*atomic.Int64{path.Join(testCaseInput.tc.chartPath, "a.yaml"): &documentReads},
	}
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: inputFS},
	)
	l := New(st)
	supplied := &manifest.Kustomization{Name: "supplied", Namespace: "default", Path: "."}
	l.AdmitDeclarations([]store.Declaration{{
		Obj:    supplied,
		Doc:    testutil.ObjectDocument(supplied),
		Origin: manifest.SuppliedDeclaration,
	}})
	l.SourceRoot = root
	if testCaseInput.tc.chartPath != "" {
		view[path.Join(testCaseInput.tc.chartPath, "Chart.yaml")] = &fstest.MapFile{
			Data: []byte("apiVersion: v2\nname: demo\nversion: 1.0.0\n"),
		}
		source := &manifest.GitRepository{Name: "source", Namespace: "default"}
		release := &manifest.HelmRelease{
			Name:      "demo",
			Namespace: "default",
			Chart:     manifest.HelmChart{Name: testCaseInput.tc.chartPath, Source: source.Named()},
		}
		release.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
			Chart: testCaseInput.tc.chartPath,
			SourceRef: &helmv2.CrossNamespaceObjectReference{
				Kind: manifest.KindGitRepository,
				Name: source.Name,
			},
		}}
		st.AddObjects([]manifest.BaseManifest{source, release})
		st.SetArtifact(
			source.Named(),
			st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
		)
		l.Index.Records[release.Named()] = manifest.DiscoveryRecord{
			File: "release.yaml",
			Refs: []manifest.NamedResource{source.Named()},
		}

		{
			err := l.FinalizeIndex(t.Context(), root, true)
			assert.FatalIf(t, err != nil, "%v", err)
		}

		readers := l.Index.Readers[path.Join(testCaseInput.tc.chartPath, "a.yaml")]

		assert.FatalIf(t, !slices.Contains(
			readers,
			manifest.FileReader{Reader: release.Named(), Source: source.Named(), Exact: true},
		), "chart did not read independent declarations: %+v", readers)
	} else {
		assertOrphanOccurrencesDistinct(t, l, root, testCaseInput.shared)
	}
	for _, id := range []manifest.NamedResource{(*testCaseInput.x), (*testCaseInput.shared), (*testCaseInput.sibling)} {
		file := "a.yaml"
		if id == (*testCaseInput.shared) {
			file = "z.yaml"
		}
		file = path.Join(testCaseInput.tc.chartPath, file)
		l.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: root + "/" + file, File: file}
	}
	file := path.Join(testCaseInput.tc.chartPath, "generator.yaml")
	l.Index.Records[(*testCaseInput.generator)] = manifest.DiscoveryRecord{
		Authored: (*testCaseInput.generator),
		Path:     root + "/" + file,
		File:     file,
	}
	if testCaseInput.tc.name == "discovery" {
		{
			err := l.FinalizeIndex(t.Context(), root, true)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	readsBefore := documentReads.Load()
	if testCaseInput.tc.order == nil {
		l.PromoteOrphans(t.Context())
	} else {
		var reads store.ReadSet
		reads.Init(st, manifest.NamedResource{}, nil)
		defer reads.Retire()
		query := rootQuery{loader: l, objects: &reads}
		for _, id := range testCaseInput.tc.order {
			record, _ := l.Index.Lookup(id)
			promoted, err := query.promote(t.Context(), id, &record, &reads)
			assert.FatalIf(t, err != nil || !promoted, "promotion failed for %s", id)
		}
		l.PromoteOrphans(t.Context())
	}

	assert.FatalIf(
		t, testCaseInput.tc.name == "discovery" && documentReads.Load() != readsBefore, "%v",
		"orphan promotion reread indexed documents",
	)

	childValues := l.Kustomizations()
	for childIndex := range childValues {
		child := childValues[childIndex]
		assert.FatalIf(t, child.Obj.Named() == supplied.Named(), "%s", "supplied evidence gained producer eligibility")
	}
	r := l.Index.Records[supplied.Named()]
	assert.FatalIf(t, r.Path != "" || r.File != "" || r.Parent != (manifest.NamedResource{}) || len(r.Productions) != 0,
		"supplied orphan: %+v", r)

	assert.Equal(t, st.HasInventory(supplied.Named()), false)
	obj, doc, attempt := st.GetEmission((*testCaseInput.generator))
	attempt.Finish()

	assert.FatalIf(t, obj == nil || doc == nil || doc["metadata"].(map[string]any)["uid"] != "authored" ||
		doc["metadata"].(map[string]any)["annotations"].(map[string]any)["source.extensions.fluxcd.io/reconcile"] != "disabled", "orphan generator lost authored document: %#v", doc)

	for _, want := range []struct {
		id    manifest.NamedResource
		value string
	}{{(*testCaseInput.x), "X"}, {(*testCaseInput.shared), "Z"}, {(*testCaseInput.sibling), "S"}} {
		obj, ok := st.GetObject(want.id).(*manifest.ConfigMap)

		assert.FatalIf(t, !ok, "missing ConfigMap %s", want.id)

		{
			got := obj.Data["VALUE"]
			assert.ErrorIf(t, got != want.value, "%s VALUE=%v, want indexed %s", want.id, got, want.value)
		}
	}
}

func testPromoteOrphansNamespaceAdmissionCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		indexed  bool
		siblings bool
	},
) {
	t.Helper()

	root := t.TempDir()
	body := testutil.KustomizationYAML("bare", "", "  path: ./workload\n")
	if testCaseInput.siblings {
		body += "---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: eligible, namespace: apps}\ndata: {value: retained}\n"
		if testCaseInput.indexed {
			body += "---\napiVersion: v1\nkind: Namespace\nmetadata: {name: apps}\n"
		}
	}
	testutil.WriteFile(t, root, "bundle.yaml", body)
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	if testCaseInput.indexed {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [bundle.yaml]\n")
		docs, localErr := testutil.SplitDocs([]byte(body))
		assert.NoError(t, localErr)
		var batch []store.Child
		if len(docs) > 0 {
			batch = make([]store.Child, 0, len(docs))
		}
		for _, doc := range docs {
			result2, parseErr := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
			obj, document := result2.Object, result2.Document
			assert.NoError(t, parseErr)
			batch = append(batch, store.Child{Obj: obj, Doc: document})
		}
		support := new(store.ReadSet)
		support.Init(st, manifest.NamedResource{}, nil)
		support.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		l.RegisterBuilt(BuiltInput{Root: root, Source: manifest.BootstrapSourceID, Support: support}, batch)
	}
	l.Admit(root)
	bare := testutil.KustomizationID("", "bare")

	assert.ErrorIf(t, st.GetObject(bare) != nil, "%v", "early admission published the namespace-less seed")

	record, ok := l.Index.Lookup(bare)
	assert.Equal(t, ok, true)
	child := l.IndexedObject(bare, record)

	assert.FatalIf(
		t,
		child.Obj == nil || child.Doc == nil,
		"%v",
		"missing indexed declaration before promotion",
	)

	target := bare
	if testCaseInput.siblings {
		target = manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "eligible"}
		st.DeleteObject(target)
		record.Declaration = child.Obj
		l.Index.Records[bare] = record
	}
	targetRecord, ok := l.Index.Lookup(target)
	assert.Equal(t, ok, true)
	if !testCaseInput.indexed {
		targetRecord.Declaration = nil
		l.Index.Records[target] = targetRecord
		record, _ = l.Index.Lookup(bare)
	}

	assert.FatalIf(t, st.GetObject(target) != nil, "%v", "promotion target already published")

	assert.Equal(t, l.PromotionDocuments(target) != nil, testCaseInput.indexed)
	before, err := json.Marshal(record)
	assert.NoError(t, err)
	document := manifest.DeepCopyMap(child.Doc)
	object := child.Obj.(*manifest.Kustomization).Clone()

	l.PromoteOrphans(t.Context())

	assert.ErrorIf(
		t,
		st.GetObject(bare) != nil,
		"%v",
		"namespace-less namespaced seed became an orphan root",
	)

	after, ok := l.Index.Lookup(bare)
	assert.Equal(t, ok, true)
	encoded, err := json.Marshal(after)
	assert.NoError(t, err)
	assert.Equal(t, string(encoded), string(before))
	assert.Equal(t, after.Declaration, record.Declaration)
	assert.Diff(t, l.IndexedObject(bare, after).Doc, document)
	assert.Equal(t, manifest.Equal(child.Obj, object), true)
	if testCaseInput.siblings {
		eligible := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "eligible"}

		assert.ErrorIf(
			t,
			st.GetObject(eligible) == nil,
			"%v",
			"explicitly namespaced sibling was not published",
		)

		assert.ErrorIf(
			t,
			testCaseInput.indexed && st.GetObject(manifest.NamedResource{Kind: "Namespace", Name: "apps"}) == nil,
			"%v",
			"cluster-scoped sibling was not published",
		)
	}
}

func assertOrphanOccurrencesDistinct(t *testing.T, l *Loader, root string, shared *manifest.NamedResource) {
	t.Helper()

	{
		_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
		assert.FatalIf(t, err != nil, "%v", err)
	}

	occurrences, err := l.Occurrences(t.Context(), root, "")

	assert.FatalIf(t, err != nil, "%v", err)

	var sharedDeclarations []selection.OccurrenceID
	{
		oValues := occurrences
		for oIndex := range oValues {
			o := oValues[oIndex]

			if o.Identity == (*shared) {
				sharedDeclarations = append(sharedDeclarations, o.Declaration)
			}
		}
	}

	assert.FatalIf(
		t,
		len(sharedDeclarations) != 2 || sharedDeclarations[0] == sharedDeclarations[1],
		"identity collapse erased distinct orphan occurrences: %+v",
		sharedDeclarations,
	)
}

func TestPromoteOrphans_UnreadableSecretWarning(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: credentials, namespace: apps}
data: {password: "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"}
sops: {version: 3.9.0}
`)
	st := store.New()
	l := New(st)
	l.SourceRoot = root
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	l.Admit(root)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	assert.NoError(t, l.PromoteOrphans(t.Context()))
	id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
	secret, ok := st.GetObject(id).(*manifest.Secret)
	assert.FatalIf(t, !ok, "orphan Secret was not promoted")
	value, err := secret.Value("password")
	assert.NoError(t, err)
	assert.Equal(t, value.Found, false)
	warnings := st.Warnings()
	assert.FatalIf(t, len(warnings) != 1, "warnings = %+v, want one SOPS warning", warnings)
	assert.Equal(t, warnings[0].Resource, id)
	assert.Equal(t, warnings[0].Category, manifest.WarnSOPSUnreadable)
	assert.Equal(t, warnings[0].Count, 1)
	assert.Diff(t, warnings[0].Detail, []string{"password"})
}
