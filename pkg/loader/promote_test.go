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

func builtObjectsForTest(t *testing.T, data string) []manifest.BaseManifest {
	t.Helper()
	docs, err := manifest.DecodeDocs(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	objects := make([]manifest.BaseManifest, 0, len(docs))
	for _, doc := range docs {
		obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: true})
		if err != nil {
			t.Fatal(err)
		}
		if obj != nil {
			objects = append(objects, obj)
		}
		manifest.ReleaseDoc(doc)
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
metadata: {name: podinfo}
spec:
  interval: 1h
  chart: {spec: {chart: podinfo, version: 1.0.0, sourceRef: {kind: HelmRepository, name: charts}}}
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
		if _, err := l.Load(t.Context(), dir); err != nil {
			t.Fatal(err)
		}
	}
	selectedFile := "entry/selected.yaml"
	independentFile := "entry/independent/invalid.yaml"
	for _, file := range []string{selectedFile, independentFile} {
		if err := l.Index.Errors[file]; !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("raw error for %s = %v, want ErrInput", file, err)
		}
	}
	independentError := l.Index.Errors[independentFile]
	selected := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "selected"}
	if _, exists := l.Index.Lookup(selected); exists {
		t.Fatal("invalid selected resource produced a raw record")
	}
	rawSource := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "source"}
	st.AddObject(candidateForTest(t, l, rawSource))
	internalkustomize.BuildMutex.Lock()
	result, err := internalkustomize.Build(filesys.MakeFsOnDisk(), entry)
	internalkustomize.BuildMutex.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	data, err := result.AsYaml()
	if err != nil {
		t.Fatal(err)
	}
	docs := builtObjectsForTest(t, string(data))
	l.RegisterBuilt(entry, docs)
	if err := l.Index.Errors[selectedFile]; err != nil {
		t.Fatalf("selected raw error survived successful built registration: %v", err)
	}
	if !errors.Is(l.Index.Errors[independentFile], independentError) {
		t.Fatal("successful built registration replaced an unselected raw error")
	}
	if st.GetObject(rawSource) != nil {
		t.Fatal("selected raw source survived built registration")
	}
	separate := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "separate"}
	if _, exists := l.Index.Lookup(separate); !exists {
		t.Fatal("independently loaded evidence under the entry prefix was removed")
	}
	admit := func(obj manifest.BaseManifest) bool { return l.AdmitsRoot(obj.Named(), "") }
	if !Promote(t.Context(), st, docs[0].Named(), PromoteOptions{Documents: docs, Admit: admit}) {
		t.Fatal("built promotion failed")
	}
	selected.Namespace, selected.Name = "apps", "selected-dev"
	rs, ok := st.GetObject(selected).(*manifest.ResourceSet)
	if !ok || len(rs.DependsOn) != 1 || rs.DependsOn[0].APIVersion != "v1" {
		t.Fatalf("corrected built ResourceSet unavailable: %+v", rs)
	}
	for range 2 {
		l.Admit(root)
	}
	extra := &manifest.GitRepository{Name: "external", Namespace: "apps"}
	st.AddObject(extra)
	view := filepath.Join(root, "source-view")
	st.SetArtifact(extra.Named(), &store.SourceArtifact{LocalPath: view, FS: fstest.MapFS{
		"extra.yaml":     {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\n")},
		"collision.yaml": {Data: []byte("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: podinfo-dev, namespace: apps}\nspec:\n  interval: 1h\n  chart: {spec: {chart: podinfo, version: 0.0.0, sourceRef: {kind: HelmRepository, name: charts}}}\n")},
	}})
	if _, err := l.Load(t.Context(), view); err != nil {
		t.Fatal(err)
	}
	if len(l.documents.cache[filepath.Join(view, "collision.yaml")].objects) != 1 {
		t.Fatal("colliding source document was not loaded")
	}
	l.Admit(root)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	if l.Index.Errors[selectedFile] != nil || !errors.Is(l.Index.Errors[independentFile], independentError) {
		t.Fatalf("admission and finalization changed replacement errors: %v", l.Index.Errors)
	}
	admitForTest(st, root)
	for _, dir := range []string{entry, filepath.Join(entry, "independent")} {
		if _, err := l.Load(t.Context(), dir); err != nil {
			t.Fatal(err)
		}
	}
	independentError = l.Index.Errors[independentFile]
	if !errors.Is(independentError, manifest.ErrInput) {
		t.Fatalf("reloaded independent raw error = %v, want ErrInput", independentError)
	}
	l.Admit(root)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	if l.Index.Errors[selectedFile] != nil || !errors.Is(l.Index.Errors[independentFile], independentError) {
		t.Fatalf("remount and finalization changed replacement errors: %v", l.Index.Errors)
	}
	var generated manifest.NamedResource
	for _, obj := range docs {
		id := obj.Named()
		record, exists := l.Index.Lookup(id)
		if !exists || record.Path != "" || record.Authored != (manifest.NamedResource{}) || record.File != "entry/kustomization.yaml" {
			t.Fatalf("built record %s lost authoritative evidence: %+v", id, record)
		}
		if st.GetObject(id) != obj || candidateForTest(t, l, id) != obj || !l.AdmitsRoot(id, "") {
			t.Errorf("built identity %s did not survive admission and source mounts", id)
		}
		for _, file := range []string{"entry/kustomization.yaml", "entry/bundle.yaml", "entry/version.yaml", "entry/value.txt", selectedFile} {
			if !slices.Contains(record.Inputs, file) {
				t.Errorf("%s does not retain built input %s", id, file)
			}
			if slices.Contains(l.Index.Readers[file], id) {
				t.Errorf("package input %s made %s a render owner", file, id)
			}
		}
		if strings.HasPrefix(id.Name, "generated-dev-") {
			generated = id
		}
		for _, production := range record.Productions {
			if production.Path != "" {
				t.Errorf("built %s advertises a raw lazy production: %+v", id, production)
			}
		}
	}
	if generated.Name == "" {
		t.Fatalf("upstream generated hash missing from built output: %s", bytes.TrimSpace(data))
	}
	for id, record := range l.Index.Files() {
		if record.File == "entry/bundle.yaml" || id.Name == "generated" || id.Name == "generated-dev" {
			t.Errorf("raw selected identity resurrected: %s %+v", id, record)
		}
	}
	hrID := testutil.HelmReleaseID("apps", "podinfo-dev")
	hr := st.GetObject(hrID).(*manifest.HelmRelease)
	assert.Equal(t, hr.Chart.Version, "2.0.0")
	record, _ := l.Index.Lookup(hrID)
	assert.Diff(t, record.Refs, []manifest.NamedResource{{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "charts"}})
	sources := slices.Collect(l.Sources())
	if !slices.ContainsFunc(sources, func(obj manifest.BaseManifest) bool { return obj.Named().Name == "source-dev" }) {
		t.Fatal("built source is unavailable to discovery")
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
	if _, err := l.Load(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	docs := []manifest.BaseManifest{&manifest.ConfigMap{Name: "remote-with-hash", Data: map[string]any{"VALUE": "fetched"}}}
	l.RegisterBuilt(root, docs)
	l.Admit(root)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatalf("finalization fetched an already built remote generator: %v", err)
	}
	if _, exists := l.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "remote"}); exists {
		t.Fatal("raw remote generator declaration was indexed")
	}
	record, _ := l.Index.Lookup(docs[0].Named())
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
	if _, err := l.Load(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	selectedFile := filepath.ToSlash(filepath.Join(root, "selected.yaml"))
	if err := l.Index.Errors[selectedFile]; !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("absolute raw error = %v, want ErrInput", err)
	}
	selected := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "selected"}
	if _, exists := l.Index.Lookup(selected); exists {
		t.Fatal("invalid selected resource produced a raw record")
	}
	internalkustomize.BuildMutex.Lock()
	result, err := internalkustomize.Build(filesys.MakeFsOnDisk(), root)
	internalkustomize.BuildMutex.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	data, err := result.AsYaml()
	if err != nil {
		t.Fatal(err)
	}
	docs := builtObjectsForTest(t, string(data))
	if len(docs) != 1 || docs[0].Named() != (manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "retained"}) {
		t.Fatalf("deletion patch did not leave only the retained ConfigMap: %v", docs)
	}
	l.RegisterBuilt(root, docs)
	if err := l.Index.Errors[selectedFile]; err != nil {
		t.Fatalf("deleted resource raw error survived successful built registration: %v", err)
	}
	admit := func(obj manifest.BaseManifest) bool { return l.AdmitsRoot(obj.Named(), "") }
	if !Promote(t.Context(), st, docs[0].Named(), PromoteOptions{Documents: docs, Admit: admit}) {
		t.Fatal("retained built promotion failed")
	}
	l.Admit(root)
	if err := l.FinalizeIndex(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	if len(l.Index.Errors) != 0 {
		t.Fatalf("deleted resource raw errors resurrected: %v", l.Index.Errors)
	}
	if _, exists := l.Index.Lookup(selected); exists || st.GetObject(selected) != nil {
		t.Fatal("deleted raw resource resurrected")
	}
}

func TestRegisterBuilt_StandaloneRoots(t *testing.T) {
	t.Parallel()
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
	if _, err := l.Load(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"entry/independent", "component", "configuration"} {
		if _, err := l.Load(t.Context(), filepath.Join(root, dir)); err != nil {
			t.Fatal(err)
		}
	}
	assert.Diff(t, l.StandaloneRoots(root), []string{"entry", "entry/independent"})
	l.RegisterBuilt(entry, nil)
	assert.Diff(t, l.StandaloneRoots(root), []string{"entry/independent"})
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "shared"}
	if _, ok := l.Index.Lookup(id); ok || l.BuiltDocuments(id) != nil {
		t.Fatal("empty output retained raw resource evidence")
	}
	l.RegisterBuilt(filepath.Join(root, "entry/independent"), nil)
	if roots := l.StandaloneRoots(root); len(roots) != 0 {
		t.Fatalf("empty build was not complete: %v", roots)
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
			root := t.TempDir()
			testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [cm.yaml, nested]\n")
			testutil.WriteFile(t, root, "entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: outer}\n")
			nested := "resources: []\n"
			if tc.reader {
				nested += "configMapGenerator: [{name: read, files: [../kustomization.yaml]}]\n"
			}
			testutil.WriteFile(t, root, "entry/nested/kustomization.yaml", nested)
			st := store.New()
			admitForTest(st, root)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: tc.ownerPath}
			st.AddObject(owner)
			l := New(st)
			l.SourceRoot = root
			entry := filepath.Join(root, "entry")
			if tc.scanned {
				entry = root
			}
			if _, err := l.Load(t.Context(), entry); err != nil {
				t.Fatal(err)
			}
			var want []string
			if tc.eligible {
				want = []string{"entry"}
			}
			if roots := l.StandaloneRoots(root); !slices.Equal(roots, want) {
				t.Fatalf("standalone roots = %v, want %v", roots, want)
			}
			if err := l.FinalizeIndex(t.Context(), root); err != nil {
				t.Fatal(err)
			}
			if tc.reader {
				if !slices.Contains(l.Index.Readers["entry/kustomization.yaml"], owner.Named()) {
					t.Fatalf("nested generator lost its read edge: %v", l.Index.Readers)
				}
				for prefix, owners := range l.Index.Claims {
					if strings.HasPrefix("entry/", prefix) && slices.Contains(owners, owner.Named()) {
						t.Fatalf("nested reader acquired a parent claim: %s", prefix)
					}
				}
				assert.Diff(t, l.StandaloneRoots(root), want)
			}
		})
	}
}
