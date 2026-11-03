package loader

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/assert"
	internalkustomize "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func builtObjectsForTest(t *testing.T, data string) []store.Child {
	t.Helper()
	docs, err := manifest.DecodeDocs(strings.NewReader(data), manifest.RequiredDocuments)
	{
		assert.NoError(t, err)
	}
	objects := make([]store.Child, 0, len(docs))
	for _, doc := range docs {
		child, outcome, _ := Prepare(doc, manifest.ParseDocOptions{WipeSecrets: true})
		{
			assert.NoError(t, outcome.Err)
		}
		if child.Obj != nil {
			objects = append(objects, child)
		} else {
			manifest.ReleaseDoc(outcome.Document)
		}
	}
	return objects
}

func TestRegisterBuilt_TransformedEvidence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for file, data := range map[string]string{
		"entry/kustomization.yaml": `resources: [bundle.yaml, selected.yaml]
namespace: apps
nameSuffix: -dev
patches:
- path: version.yaml
- target: {kind: ResourceSet, name: selected}
  patch: '[{"op":"replace","path":"/spec/dependsOn/0/apiVersion","value":"v1"}]'
configMapGenerator: [{name: generated, files: [value.txt]}]
`,
		"entry/bundle.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: podinfo, uid: built-release}
spec:
  interval: 1h
  chart: {spec: {chart: podinfo, version: 1.0.0, sourceRef: {kind: HelmRepository, name: charts}}}
  dependsOn: [{name: dependency}]
  valuesFrom: [{kind: ConfigMap, name: values}, {kind: Secret, name: credentials, optional: true}]
---
apiVersion: v1
kind: ConfigMap
metadata: {name: settings}
data: {mode: base}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: source}
spec: {url: https://example.test/source.git}
---
apiVersion: example.test/v1
kind: Widget
metadata: {name: foreign}
`,
		"entry/version.yaml": `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: podinfo}
spec: {chart: {spec: {version: 2.0.0}}}
`,
		"entry/selected.yaml": `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: selected}
spec:
  dependsOn: [{apiVersion: bad/group/version, kind: ConfigMap, name: settings-dev, namespace: apps}]
`,
		"entry/independent/invalid.yaml": `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: independent}
spec:
  dependsOn: [{apiVersion: bad/group/version, kind: ConfigMap, name: missing}]
`,
		"entry/value.txt":                      "fixture",
		"entry/independent/kustomization.yaml": "resources: [separate.yaml, invalid.yaml]\n",
		"entry/independent/separate.yaml":      "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: separate, namespace: apps}\n",
	} {
		testutil.WriteFile(t, root, file, data)
	}
	entry := filepath.Join(root, "entry")
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	for _, dir := range []string{entry, filepath.Join(entry, "independent")} {
		{
			_, err := l.Load(t.Context(), dir)
			assert.NoError(t, err)
		}
	}
	selectedFile := "entry/selected.yaml"
	independentFile := "entry/independent/invalid.yaml"
	for _, file := range []string{selectedFile, independentFile} {
		{
			err := retainedErrorsForTest(l)[file]
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "raw error for %s = %v, want ErrInput", file, err)
		}
	}
	independentError := retainedErrorsForTest(l)[independentFile]
	selected := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain,
		Kind:  manifest.KindResourceSet,
		Name:  "selected",
	}
	{
		_, exists := l.Index.Lookup(selected)
		assert.FatalIf(t, exists, "%s", "invalid selected resource produced a raw record")
	}
	rawSource := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "source"}
	st.AddObject(candidateForTest(t, l, rawSource))
	internalkustomize.BuildMutex.Lock()
	result, err := internalkustomize.Build(filesys.MakeFsOnDisk(), entry)
	internalkustomize.BuildMutex.Unlock()
	{
		assert.NoError(t, err)
	}
	data, err := result.AsYaml()
	{
		assert.NoError(t, err)
	}
	docs := builtObjectsForTest(t, string(data))
	l.RegisterBuilt(
		BuiltInput{Root: entry, Source: manifest.BootstrapSourceID, Owner: testutil.KustomizationID("apps", "entry")},
		docs,
	)
	{
		err := retainedErrorsForTest(l)[selectedFile]
		assert.FatalIf(t, err != nil, "selected raw error survived successful built registration: %v", err)
	}
	{
		assert.FatalIf(
			t,
			!errors.Is(retainedErrorsForTest(l)[independentFile], independentError),
			"%s",
			"successful built registration replaced an unselected raw error",
		)
	}
	{
		assert.FatalIf(t, st.GetObject(rawSource) != nil, "%s", "selected raw source survived built registration")
	}
	separate := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "separate"}
	{
		_, exists := l.Index.Lookup(separate)
		assert.FatalIf(t, !exists, "%s", "independently loaded evidence under the entry prefix was removed")
	}
	admit := func(obj manifest.BaseManifest) (bool, error) {
		record, ok := l.Index.Lookup(obj.Named())
		return ok && record.Parent == testutil.KustomizationID("apps", "entry"), nil
	}
	{
		promoted, err := Promote(
			t.Context(),
			st,
			docs[0].Obj.Named(),
			PromoteOptions{Documents: docs, Admit: admit},
		)
		assert.FatalIf(t, err != nil ||
			!promoted, "%s", "built promotion failed")
	}
	selected.Namespace, selected.Name = "apps", "selected-dev"
	rs, ok := st.GetObject(selected).(*manifest.ResourceSet)
	{
		assert.FatalIf(
			t,
			!ok || len(rs.DependsOn) != 1 || rs.DependsOn[0].APIVersion != "v1",
			"corrected built ResourceSet unavailable: %+v",
			rs,
		)
	}
	for range 2 {
		l.Admit(root)
	}
	extra := &manifest.GitRepository{Name: "external", Namespace: "apps"}
	st.AddObject(extra)
	view := filepath.Join(root, "source-view")
	st.SetArtifact(extra.Named(), &store.SourceArtifact{LocalPath: view, FS: fstest.MapFS{
		"extra.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\n")},
		"collision.yaml": {
			Data: []byte(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: podinfo-dev, namespace: apps}\nspec:\n  interval: 1h\n  chart: {spec: {chart: podinfo, version: 0.0.0, sourceRef: {kind: HelmRepository, name: charts}}}\n",
			),
		},
	}})
	{
		_, err := l.Load(t.Context(), view)
		assert.NoError(t, err)
	}
	{
		assert.FatalIf(
			t,
			len(l.documents.cache[filepath.Join(view, "collision.yaml")].objects) != 1,
			"%s",
			"colliding source document was not loaded",
		)
	}
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	if retainedErrorsForTest(l)[selectedFile] != nil ||
		!errors.Is(retainedErrorsForTest(l)[independentFile], independentError) {
		t.Fatalf("admission and finalization changed replacement errors: %v", retainedErrorsForTest(l))
	}
	admitForTest(st, root)
	for _, dir := range []string{entry, filepath.Join(entry, "independent")} {
		{
			_, err := l.Load(t.Context(), dir)
			assert.NoError(t, err)
		}
	}
	independentError = retainedErrorsForTest(l)[independentFile]
	{
		assert.FatalIf(
			t,
			!errors.Is(independentError, manifest.ErrInput),
			"reloaded independent raw error = %v, want ErrInput",
			independentError,
		)
	}
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	if retainedErrorsForTest(l)[selectedFile] != nil ||
		!errors.Is(retainedErrorsForTest(l)[independentFile], independentError) {
		t.Fatalf("remount and finalization changed replacement errors: %v", retainedErrorsForTest(l))
	}
	testRegisterBuiltTransformedEvidenceCompletion(t, &testRegisterBuiltTransformedEvidenceCompletionState{st: &st, l: &l, selectedFile: &selectedFile, data: &data, docs: &docs})
}

func TestRegisterBuilt_SharedInputs(t *testing.T) {
	t.Parallel()
	t.Run("typed", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"kustomization.yaml",
			"resources: [release.yaml]\nconfigMapGenerator: [{name: generated, files: [value.txt]}]\n",
		)
		release := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: retained, namespace: apps}
spec:
  chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: charts}}}
`
		testutil.WriteFile(t, root, "release.yaml", release)
		testutil.WriteFile(t, root, "value.txt", "input")
		st := store.New()
		admitForTest(st, root)
		l := New(st)
		l.SourceRoot = root
		{
			_, err := l.Load(t.Context(), root)
			assert.NoError(t, err)
		}
		docs := builtObjectsForTest(
			t,
			release+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: generated-hash, namespace: apps}\ndata: {value.txt: input}\n",
		)
		l.RegisterBuilt(
			BuiltInput{
				Root:   root,
				Source: manifest.BootstrapSourceID,
				Owner:  testutil.KustomizationID("apps", "entry"),
			},
			docs,
		)
		hr, _ := l.Index.Lookup(docs[0].Obj.Named())
		generated, _ := l.Index.Lookup(docs[1].Obj.Named())
		assert.Equal(t, hr.File, "release.yaml")
		assert.Diff(t, hr.Inputs, []string{"kustomization.yaml", "value.txt"})
		assert.Equal(t, generated.File, "kustomization.yaml")
		assert.Diff(t, generated.Inputs, []string{"kustomization.yaml", "release.yaml", "value.txt"})
		assert.Diff(
			t,
			hr.Refs,
			[]manifest.NamedResource{
				{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "apps-retained"},
				{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "charts"},
			},
		)
	})
	for _, tc := range []struct {
		name   string
		value  string
		file   string
		inputs []string
	}{
		{"raw equal content", "app", "generator.yaml", []string{"kustomization.yaml", "value.txt"}},
		{"raw changed nested content", "other", "kustomization.yaml", []string{"generator.yaml", "kustomization.yaml", "value.txt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"kustomization.yaml",
				"resources: [generator.yaml]\nconfigMapGenerator: [{name: generated, files: [value.txt]}]\n",
			)
			generator := `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps}
spec:
  artifacts: [{name: app}]
`
			testutil.WriteFile(t, root, "generator.yaml", generator)
			testutil.WriteFile(t, root, "value.txt", "input")
			st := store.New()
			admitForTest(st, root)
			l := New(st)
			l.SourceRoot = root
			{
				_, err := l.Load(t.Context(), root)
				assert.NoError(t, err)
			}
			docs := builtObjectsForTest(
				t,
				strings.Replace(generator, "artifacts: [{name: app}]", "artifacts: [{name: "+tc.value+"}]", 1),
			)
			raw := docs[0].Obj.(*manifest.RawObject)
			l.RegisterBuilt(
				BuiltInput{
					Root:   root,
					Source: manifest.BootstrapSourceID,
					Owner:  testutil.KustomizationID("apps", "entry"),
				},
				docs,
			)
			record, ok := l.Index.Lookup(raw.Named())
			assert.Equal(t, ok, true)
			assert.Equal(t, record.File, tc.file)
			assert.Diff(t, record.Inputs, tc.inputs)
		})
	}
}

func TestRegisterBuilt_RemoteGenerator(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", `configMapGenerator:
- name: remote
  envs: [https://example.test/input.env]
`)
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	{
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
	}
	docs := []store.Child{
		{Obj: &manifest.ConfigMap{Name: "remote-with-hash", Data: map[string]any{"VALUE": "fetched"}}},
	}
	l.RegisterBuilt(
		BuiltInput{Root: root, Source: manifest.BootstrapSourceID, Owner: testutil.KustomizationID("apps", "entry")},
		docs,
	)
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.FatalIf(t, err != nil, "finalization fetched an already built remote generator: %v", err)
	}
	{
		_, exists := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "remote"})
		assert.FatalIf(t, exists, "%s", "raw remote generator declaration was indexed")
	}
	record, _ := l.Index.Lookup(docs[0].Obj.Named())
	assert.Diff(t, record.Inputs, []string{"kustomization.yaml"})
	assert.Equal(t, len(l.Index.Readers), 0)
}

func TestRegisterBuilt_DeletedInvalidResource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for file, data := range map[string]string{
		"kustomization.yaml": `resources: [selected.yaml, retained.yaml]
patches:
- patch: |
    apiVersion: fluxcd.controlplane.io/v1
    kind: ResourceSet
    metadata: {name: selected}
    $patch: delete
`,
		"selected.yaml": `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: selected}
spec:
  dependsOn: [{apiVersion: bad/group/version, kind: ConfigMap, name: retained}]
`,
		"retained.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: retained}\n",
	} {
		testutil.WriteFile(t, root, file, data)
	}
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	{
		_, err := l.Load(t.Context(), root)
		assert.NoError(t, err)
	}
	selectedFile := filepath.ToSlash(filepath.Join(root, "selected.yaml"))
	{
		err := retainedErrorsForTest(l)[selectedFile]
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "absolute raw error = %v, want ErrInput", err)
	}
	selected := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain,
		Kind:  manifest.KindResourceSet,
		Name:  "selected",
	}
	{
		_, exists := l.Index.Lookup(selected)
		assert.FatalIf(t, exists, "%s", "invalid selected resource produced a raw record")
	}
	internalkustomize.BuildMutex.Lock()
	result, err := internalkustomize.Build(filesys.MakeFsOnDisk(), root)
	internalkustomize.BuildMutex.Unlock()
	{
		assert.NoError(t, err)
	}
	data, err := result.AsYaml()
	{
		assert.NoError(t, err)
	}
	docs := builtObjectsForTest(t, string(data))
	{
		assert.FatalIf(t, len(docs) != 1 ||
			docs[0].Obj.Named() != (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "retained"}), "deletion patch did not leave only the retained ConfigMap: %v", docs)
	}
	l.RegisterBuilt(
		BuiltInput{Root: root, Source: manifest.BootstrapSourceID, Owner: testutil.KustomizationID("apps", "entry")},
		docs,
	)
	{
		err := retainedErrorsForTest(l)[selectedFile]
		assert.FatalIf(t, err != nil, "deleted resource raw error survived successful built registration: %v", err)
	}
	admit := func(obj manifest.BaseManifest) (bool, error) {
		record, ok := l.Index.Lookup(obj.Named())
		return ok && record.Parent == testutil.KustomizationID("apps", "entry"), nil
	}
	{
		promoted, err := Promote(
			t.Context(),
			st,
			docs[0].Obj.Named(),
			PromoteOptions{Documents: docs, Admit: admit},
		)
		assert.FatalIf(t, err != nil ||
			!promoted, "%s", "retained built promotion failed")
	}
	l.Admit(root)
	{
		err := l.FinalizeIndex(t.Context(), root, true)
		assert.NoError(t, err)
	}
	if len(retainedErrorsForTest(l)) != 0 {
		t.Fatalf("deleted resource raw errors resurrected: %v", retainedErrorsForTest(l))
	}
	{
		_, exists := l.Index.Lookup(selected)
		assert.FatalIf(t, exists || st.GetObject(selected) != nil, "%s", "deleted raw resource resurrected")
	}
}

func TestRegisterBuilt_StandaloneRoots(t *testing.T) {
	t.Parallel()
	t.Run(
		"inactive package batch",
		func(t *testing.T) { t.Parallel(); testRegisterBuiltStandaloneRootsInactivePackageBatch(t) },
	)

	root := t.TempDir()
	for file, body := range map[string]string{
		"kustomization.yaml":                   "resources: [entry]\n",
		"entry/kustomization.yaml":             "resources: [../shared]\ngenerators: [../configuration]\n",
		"entry/independent/kustomization.yaml": "resources: []\n",
		"shared/kustomization.yaml":            "resources: [cm.yaml]\n",
		"shared/cm.yaml":                       "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n",
		"configuration/kustomization.yaml":     "resources: []\n",
		"component/kustomization.yaml":         "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n",
	} {
		testutil.WriteFile(t, root, file, body)
	}
	st := store.New()
	admitForTest(st, root)
	l := New(st)
	l.SourceRoot = root
	entry := filepath.Join(root, "entry")
	{
		_, err := l.Load(t.Context(), entry)
		assert.NoError(t, err)
	}
	for _, dir := range []string{"entry/independent", "component", "configuration"} {
		{
			_, err := l.Load(t.Context(), filepath.Join(root, dir))
			assert.NoError(t, err)
		}
	}
	l.RegisterBuilt(
		BuiltInput{Root: entry, Source: manifest.BootstrapSourceID, Owner: testutil.KustomizationID("apps", "entry")},
		nil,
	)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "shared"}
	{
		_, ok := l.Index.Lookup(id)
		assert.FatalIf(t, ok || l.PromotionDocuments(id) != nil, "%s", "empty output retained raw resource evidence")
	}
	l.RegisterBuilt(
		BuiltInput{
			Root:   filepath.Join(root, "entry/independent"),
			Source: manifest.BootstrapSourceID,
			Owner:  testutil.KustomizationID("apps", "entry"),
		},
		nil,
	)
	{
		assert.FatalIf(t, l.documents.cache[entry].built == nil ||
			l.documents.cache[filepath.Join(root, "entry/independent")].built == nil, "%s", "empty builds did not retain completion evidence")
	}
	for _, tc := range []struct {
		name, ownerPath string
		reader, scanned bool
		eligible        bool
	}{
		{name: "nested owner", ownerPath: "./entry/nested", eligible: true},
		{name: "nested reader", ownerPath: "./entry/nested", reader: true, eligible: true},
		{name: "same root owner", ownerPath: "./entry"},
		{name: "ancestor owner", ownerPath: "."},
		{name: "scanned nested owner", ownerPath: "./entry/nested", scanned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRegisterBuiltStandaloneRootsCase(t, &tc)
		},
		)
	}
}

func TestPromote_ContentOnlyBinding(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	artifact := &store.SourceArtifact{
		Key:       "source",
		Content:   "generation",
		LocalPath: "/input",
		FS: fstest.MapFS{
			"settings.yaml": {
				Data: []byte(
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {value: selected}\n",
				),
			},
		},
	}
	st.SetArtifact(source, artifact)
	var reads store.ReadSet
	reads.Init(st, testutil.KustomizationID("apps", "reader"), nil)
	defer reads.Close()
	{
		promoted, err := Promote(
			t.Context(),
			st,
			id,
			PromoteOptions{
				Record: manifest.Production{Source: source, Path: "/input/settings.yaml"},
				Reads:  &reads,
				Admit:  func(manifest.BaseManifest) (bool, error) { return true, nil },
			},
		)
		assert.FatalIf(t, err != nil ||
			!promoted, "%s", "source document was not promoted")
	}
	var consumer store.ReadSet
	consumer.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	defer consumer.Close()
	consumer.GetObject(id)
	next := *artifact
	next.Revision = "next"
	next.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
	st.SetArtifact(source, &next)
	{
		assert.FatalIf(
			t,
			!reads.Valid() || !consumer.Valid() || st.GetObject(id) == nil,
			"%s",
			"metadata invalidated the promoted content binding",
		)
	}
	replacement := next
	replacement.Content = "replacement"
	st.SetArtifact(source, &replacement)
	{
		assert.FatalIf(
			t,
			reads.Valid() || consumer.Valid() || st.GetObject(id) != nil,
			"%s",
			"replacement retained a stale promoted document",
		)
	}
}

type testRegisterBuiltTransformedEvidenceCompletionState struct {
	st           **store.Store
	l            **Loader
	selectedFile *string
	data         *[]byte
	docs         *[]store.Child
}

func testRegisterBuiltTransformedEvidenceCompletion(t *testing.T, scope *testRegisterBuiltTransformedEvidenceCompletionState) {
	t.Helper()
	var generated manifest.NamedResource
	for _, child := range *scope.docs {
		obj := child.Obj
		id := obj.Named()
		record, exists := (*scope.l).Index.Lookup(id)
		{
			assert.FatalIf(t, !exists || record.Path != "" || record.Authored != (manifest.NamedResource{}) ||
				record.File != "entry/kustomization.yaml", "built record %s lost authoritative evidence: %+v", id, record)
		}
		{
			assert.ErrorIf(t, (*scope.st).GetObject(id) != obj || candidateForTest(t, (*scope.l), id) != obj ||
				(*scope.l).AdmitsRoot(
					(*scope.l).IndexedObject(id, (*scope.l).Index.Records[id]).Obj,
					"",
				), "built identity %s did not survive admission and source mounts", id)
		}
		for _, file := range []string{"entry/kustomization.yaml", "entry/bundle.yaml", "entry/version.yaml", "entry/value.txt", (*scope.selectedFile)} {
			{
				assert.ErrorIf(t, !slices.Contains(record.Inputs, file), "%s does not retain built input %s", id, file)
			}
			{
				assert.ErrorIf(
					t,
					slices.ContainsFunc(
						(*scope.l).Index.Readers[file],
						func(r manifest.FileReader) bool { return r.Reader == id },
					),
					"package input %s made %s a render owner",
					file,
					id,
				)
			}
		}
		if strings.HasPrefix(id.Name, "generated-dev-") {
			generated = id
		}
		for _, production := range record.Productions {
			{
				assert.ErrorIf(
					t,
					production.Path != "",
					"built %s advertises a raw lazy production: %+v",
					id,
					production,
				)
			}
		}
	}
	if generated.Name == "" {
		t.Fatalf("upstream generated hash missing from built output: %s", bytes.TrimSpace((*scope.data)))
	}
	for id, record := range (*scope.l).Index.Files() {
		{
			assert.ErrorIf(
				t,
				record.File == "entry/bundle.yaml" || id.Name == "generated" || id.Name == "generated-dev",
				"raw selected identity resurrected: %s %+v",
				id,
				record,
			)
		}
	}
	hrID := testutil.HelmReleaseID("apps", "podinfo-dev")
	hr := (*scope.st).GetObject(hrID).(*manifest.HelmRelease)
	assert.Equal(t, hr.Chart.Version, "2.0.0")
	_, document, emission := (*scope.st).GetEmission(hrID)
	emission.Finish()
	assert.Equal(t, document["metadata"].(map[string]any)["uid"], any("built-release"))
	spec := document["spec"].(map[string]any)
	assert.Equal(t, spec["chart"].(map[string]any)["spec"].(map[string]any)["version"], any("2.0.0"))
	record, _ := (*scope.l).Index.Lookup(hrID)
	assert.Diff(t, record.Refs, []manifest.NamedResource{
		{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "apps", Name: "apps-podinfo-dev"},
		{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "charts"},
		{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "values"},
		{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"},
	})
	var sources []store.Child
	for source, err := range (*scope.l).Sources(t.Context()) {
		assert.NoError(t, err)
		sources = append(sources, source)
	}
	{
		assert.FatalIf(t, !slices.ContainsFunc(
			sources,
			func(child store.Child) bool { return child.Obj.Named().Name == "source-dev" && child.Doc != nil },
		), "%s", "built source is unavailable to discovery")
	}
}
