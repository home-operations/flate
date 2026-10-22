package loader

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Discovery drivers stay in the Store; deferred content stays in the file index.
func TestDiscovery_SkipsNonDiscoveryKinds(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cases := []struct {
		kind, api, spec string
		admitted        bool
	}{
		{manifest.KindKustomization, manifest.FluxKustomizeDomain + "/v1", "spec: {interval: 10m, path: ./apps, sourceRef: {kind: GitRepository, name: fixture}}", true},
		{manifest.KindResourceSet, manifest.FluxOperatorDomain + "/v1", "spec: {resources: []}", true},
		{manifest.KindResourceSetInputProvider, manifest.FluxOperatorDomain + "/v1", "spec: {type: Static, defaultValues: {key: value}}", true},
		{manifest.KindGitRepository, manifest.SourceDomain + "/v1", "spec: {url: https://example.test/repo.git}", true},
		{manifest.KindHelmRelease, manifest.HelmReleaseDomain + "/v2", "spec: {chart: {spec: {chart: podinfo, sourceRef: {kind: HelmRepository, name: podinfo}}}}", false},
		{manifest.KindConfigMap, "v1", "data: {FOO: bar}", false},
	}
	for _, tc := range cases {
		testutil.WriteFile(t, dir, tc.kind+".yaml", fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata: {name: fixture, namespace: flux-system}\n%s\n", tc.api, tc.kind, tc.spec))
	}
	st := store.New()
	l := New(st)
	l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	if _, err := l.Load(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			group, _, found := strings.Cut(tc.api, "/")
			if !found {
				group = ""
			}
			id := manifest.NamedResource{Group: group, Kind: tc.kind, Name: "fixture", Namespace: "flux-system"}
			assert.Equal(t, st.GetObject(id) != nil, tc.admitted)
			_, indexed := l.Index.Lookup(id)
			assert.Equal(t, indexed, true)
		})
	}
	cached := l.documents.cache[filepath.Join(dir, manifest.KindGitRepository+".yaml")].objects
	assert.Equal(t, slices.ContainsFunc(cached, func(obj manifest.BaseManifest) bool { return obj.Named().Kind == manifest.KindGitRepository }), true)
}

// TestDiscovery_PromoteMaterializesFromIndex covers the
// lazy-promotion contract: when depwait hits a missing dep that the
// Existence index knows about, Promote re-parses the file and
// AddObjects it into the Store so the wait can clear.
func TestDiscovery_PromoteMaterializesFromIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "cm.yaml", `
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-cm
  namespace: default
data:
  KEY: value
`)

	st := store.New()
	admitForTest(st, dir)
	l := loaderFixture(st, nil)
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "my-cm"}
	if st.GetObject(id) != nil {
		t.Fatalf("precondition: CM should not be in store yet")
	}
	if !Promote(t.Context(), l.Store, id, PromoteOptions{Record: productionForTest(l.Index.Records[id]), WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}) {
		t.Fatalf("Promote should return true on a known id")
	}
	if st.GetObject(id) == nil {
		t.Errorf("Promote did not materialize CM into Store")
	}
}

// TestPromote_UnknownIDReturnsFalse covers the path
// where depwait's missing-dep fallback asks for a CM that never
// reached file-load: existence index has no entry, so Promote bails
// out cleanly and the depwait surfaces "dependency not found" as
// intended.
func TestPromote_UnknownIDReturnsFalse(t *testing.T) {
	t.Parallel()
	idx := loaderFixture(store.New(), nil)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ghost"}
	if got := Promote(t.Context(), idx.Store, id, PromoteOptions{Record: productionForTest(idx.Index.Records[id]), WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}); got {
		t.Errorf("Promote on unknown id should return false, got true")
	}
}

// TestPromote_FileRemovedBetweenRecordAndPromote
// exercises the race where the indexed file was deleted (or
// permission-stripped) between the file walk and the lazy lookup.
// Promote should fail gracefully (return false), not panic, so the
// depwait surfaces "dependency not found" instead of crashing the
// orchestrator.
func TestPromote_FileRemovedBetweenRecordAndpromoteForTest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "cm.yaml", `
apiVersion: v1
kind: ConfigMap
metadata:
  name: ephemeral
  namespace: default
data:
  KEY: value
`)
	st := store.New()
	admitForTest(st, dir)
	l := loaderFixture(st, nil)
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Remove the file behind the index.
	if err := os.Remove(filepath.Join(dir, "cm.yaml")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ephemeral"}
	if got := Promote(t.Context(), l.Store, id, PromoteOptions{Record: productionForTest(l.Index.Records[id]), WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}); got {
		t.Errorf("Promote on a removed file should return false, got true")
	}
	if st.GetObject(id) != nil {
		t.Errorf("removed file should not have produced a Store entry")
	}
}

// TestPromote_MultiDocLoadsSiblings pins the
// "whole-file parse" contract documented on Promote: a YAML file
// packing CM + Secret + HR together promotes ALL three when ANY one
// is requested. This is what allows depwait to avoid re-opening the
// same file once per dep in the common multi-doc fixture pattern.
func TestPromote_MultiDocLoadsSiblings(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", `
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: default
data:
  KEY: value
---
apiVersion: v1
kind: Secret
metadata:
  name: app-secret
  namespace: default
type: Opaque
data:
  TOKEN: dG9rZW4=
`)
	st := store.New()
	admitForTest(st, dir)
	l := loaderFixture(st, nil)
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}
	secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "default", Name: "app-secret"}
	if !Promote(t.Context(), l.Store, cmID, PromoteOptions{Record: productionForTest(l.Index.Records[cmID]), WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}) {
		t.Fatalf("promoteForTest(t.Context(), cm) should succeed")
	}
	if st.GetObject(cmID) == nil {
		t.Errorf("CM not in store after Promote")
	}
	if st.GetObject(secretID) == nil {
		t.Errorf("sibling Secret should have been promoted alongside the CM (whole-file parse contract)")
	}
}

func TestPromote_AdmissionAndPreferExisting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		secret   bool
		existing bool
	}{
		{name: "admitted request"},
		{name: "rejected request", secret: true},
		{name: "rendered object retained", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "bundle.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: selected, namespace: apps}
data: {source: raw}
---
apiVersion: v1
kind: Secret
metadata: {name: excluded, namespace: apps}
stringData: {value: fixture}
`)
			cm := &manifest.ConfigMap{Name: "selected", Namespace: "apps", Data: map[string]any{"source": "rendered"}}
			secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "excluded"}
			st := store.New()
			if tc.existing {
				st.AddObject(cm)
			}
			admitForTest(st, dir)
			idx := loaderFixture(st, nil)
			for _, id := range []manifest.NamedResource{cm.Named(), secret} {
				idx.Index.Records[id] = manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "bundle.yaml")}
			}
			id := cm.Named()
			if tc.secret {
				id = secret
			}
			admit := func(obj manifest.BaseManifest) bool { return obj.Named() == cm.Named() }
			if got := Promote(t.Context(), idx.Store, id, PromoteOptions{Record: productionForTest(idx.Index.Records[id]), WipeSecrets: true, Admit: admit}); got == tc.secret {
				t.Errorf("Promote returned %v, want %v", got, !tc.secret)
			}
			if st.GetObject(cm.Named()) == nil || st.GetObject(secret) != nil {
				t.Error("admission must apply separately to requested and sibling objects")
			}
			if tc.existing && st.GetObject(cm.Named()) != cm {
				t.Error("existing rendered object must take precedence")
			}
		})
	}
}

// Sources must remain admitted so bootstrap aliasing can resolve their URLs
// without redirecting unrelated source references to the working tree.
func TestDiscovery_SourcesStayFileLoaded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "src.yaml", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: upstream
  namespace: flux-system
spec:
  url: https://example.test/upstream.git
---
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata:
  name: charts
  namespace: flux-system
spec:
  url: oci://example.test/charts
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: podinfo
  namespace: default
spec:
  url: https://example.test/charts
`)

	st := store.New()
	l := loaderFixture(st, nil)
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	for _, want := range []manifest.NamedResource{
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "upstream"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "charts"},
		{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmRepository, Namespace: "default", Name: "podinfo"},
	} {
		if st.GetObject(want) == nil {
			t.Errorf("source %s must stay in Store during discovery; got nil", want.String())
		}
	}
}

func TestPromote_AuthoredDocumentProjection(t *testing.T) {
	t.Parallel()
	const doc = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: podinfo, namespace: %q}
spec:
  interval: 1h
  chart: {spec: {chart: podinfo, version: %s, sourceRef: {kind: HelmRepository, name: podinfo}}}
`
	for _, tc := range []struct {
		name, authored, namespace, version string
		evidence                           bool
	}{
		{"bare projected", "", "apps", "1.0.0", true},
		{"authored wins over foreign exact identity", "authored", "other", "1.0.0", true},
		{"no evidence exact identity", "authored", "other", "2.0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "bundle.yaml", strings.Replace(fmt.Sprintf(doc, tc.authored, "9.0.0"), manifest.HelmReleaseDomain, "foreign.example", 1)+"---\n"+fmt.Sprintf(doc, "other", "2.0.0")+"---\n"+fmt.Sprintf(doc, tc.authored, "1.0.0")+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: note}\n")
			st := store.New()
			admitForTest(st, dir)
			id := testutil.HelmReleaseID(tc.namespace, "podinfo")
			record := manifest.DiscoveryRecord{Path: filepath.Join(dir, "bundle.yaml")}
			if tc.evidence {
				record.Authored = id
				record.Authored.Namespace = tc.authored
			}
			if got := Promote(t.Context(), st, id, PromoteOptions{Record: productionForTest(record), Admit: func(manifest.BaseManifest) bool { return true }}); got != (tc.version != "") {
				t.Fatalf("promoteForTest(t.Context(), %s) = %v, want %v", id, got, tc.version != "")
			}
			if got := st.GetObject(id).(*manifest.HelmRelease).Chart.Version; got != tc.version {
				t.Errorf("%s chart version = %s, want %s", id, got, tc.version)
			}
			foreign := id
			foreign.Group = "foreign.example"
			if Promote(t.Context(), st, foreign, PromoteOptions{Record: productionForTest(record), Admit: func(manifest.BaseManifest) bool { return true }}) {
				t.Fatal("foreign promotion borrowed the real HelmRelease")
			}
			sibling := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "note"}
			if st.GetObject(sibling) == nil {
				t.Error("admitted sibling must retain its authored identity")
			}
			sibling.Namespace = tc.namespace
			if st.GetObject(sibling) != nil {
				t.Errorf("sibling %s acquired the requested namespace", sibling)
			}
		})
	}
}

func TestPromote_SourceView(t *testing.T) {
	t.Parallel()
	const doc = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected, namespace: authored}\ndata: {source: %s}\n"
	for _, present := range []bool{true, false} {
		t.Run(fmt.Sprintf("present=%v", present), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "values.yaml", fmt.Sprintf(doc, "working"))
			input := fstest.MapFS{}
			if present {
				input["values.yaml"] = &fstest.MapFile{Data: []byte(fmt.Sprintf(doc, "committed"))}
			}
			st := store.New()
			source := &manifest.GitRepository{Name: "source"}
			st.AddObject(source)
			repoRoot := t.TempDir()
			st.SetArtifact(source.Named(), &store.SourceArtifact{LocalRoot: repoRoot, LocalPath: dir, Key: "pinned", FS: input})
			foreign := &manifest.RawObject{APIVersion: "foreign.example/v1", Kind: manifest.KindGitRepository, Name: source.Name}
			st.AddObject(foreign)
			st.SetArtifact(foreign.Named(), &store.SourceArtifact{LocalPath: dir, FS: fstest.MapFS{"values.yaml": &fstest.MapFile{Data: []byte(fmt.Sprintf(doc, "foreign"))}}})
			for _, version := range []string{"", manifest.SourceDomain + "/v1", "v1", "foreign.example/v1", "missing.example/v1"} {
				ks := &manifest.Kustomization{SourceRef: kustomizev1.CrossNamespaceSourceReference{APIVersion: version, Kind: manifest.KindGitRepository, Name: source.Name}}
				root, pinned, external := SourceView(st, ks.SourceID(), repoRoot)
				admitted := version == "" || version == manifest.SourceDomain+"/v1"
				wantRoot := repoRoot
				if admitted {
					wantRoot = dir
				}
				if root != wantRoot || pinned != admitted || external != (version == "foreign.example/v1") {
					t.Fatalf("SourceView(%q) = %s, %t, %t", version, root, pinned, external)
				}
				if !admitted && st.GetArtifact(ks.SourceID()) != nil && version != "foreign.example/v1" {
					t.Fatalf("missing source %s borrowed the real artifact", ks.SourceID())
				}
			}
			id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"}
			record := manifest.DiscoveryRecord{Authored: id, Path: filepath.Join(dir, "values.yaml"), Productions: []manifest.Production{{Source: source.Named()}}}
			record.Authored.Namespace = "authored"
			if got := Promote(t.Context(), st, id, PromoteOptions{Record: productionForTest(record), Admit: func(manifest.BaseManifest) bool { return true }}); got != present {
				t.Fatalf("Promote = %v, want %v", got, present)
			}
			if present && st.GetObject(id).(*manifest.ConfigMap).Data["source"] != "committed" {
				t.Fatal("promotion read the working file instead of the mounted view")
			}
		})
	}
}

type promotionBarrier struct {
	fs.FS
	opened  chan struct{}
	release chan struct{}
}

func (b promotionBarrier) Open(name string) (fs.File, error) {
	close(b.opened)
	<-b.release
	return b.FS.Open(name)
}

func TestPromote_CapturedSourceReplacement(t *testing.T) {
	t.Parallel()
	st := store.New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	root := t.TempDir()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "selected", Namespace: "apps"}
	makeFS := func(value string) fstest.MapFS {
		return fstest.MapFS{"values.yaml": &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected, namespace: apps}\ndata: {value: " + value + "}\n")}}
	}
	barrier := promotionBarrier{FS: makeFS("old"), opened: make(chan struct{}), release: make(chan struct{})}
	st.SetArtifact(source.Named(), &store.SourceArtifact{Key: "old", LocalPath: root, FS: barrier})
	var first store.ReadSet
	first.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	options := PromoteOptions{Record: manifest.Production{Source: source.Named(), Authored: id, Path: filepath.Join(root, "values.yaml")}, Reads: &first, Admit: func(manifest.BaseManifest) bool { return true }}
	var arrivals int
	st.AddListener(store.EventObjectAdded, func(changed manifest.NamedResource, _ any) {
		if changed == id {
			arrivals++
		}
	}, false)
	done := make(chan bool, 1)
	go func() { done <- Promote(t.Context(), st, id, options) }()
	<-barrier.opened
	st.SetArtifact(source.Named(), &store.SourceArtifact{Key: "new", LocalPath: root, FS: makeFS("new")})
	close(barrier.release)
	if <-done || st.GetObject(id) != nil || first.Valid() || arrivals != 0 {
		t.Fatal("changed source admitted stale raw bytes")
	}
	first.Retire()
	var second store.ReadSet
	second.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	options.Reads = &second
	if !Promote(t.Context(), st, id, options) {
		t.Fatal("current captured source did not admit")
	}
	if got, _ := second.Get[*manifest.ConfigMap](id); got == nil || got.Data["value"] != "new" || !second.Valid() {
		t.Fatalf("current promotion = %+v, valid=%t", got, second.Valid())
	}
	st.SetArtifact(source.Named(), &store.SourceArtifact{Key: "third", LocalPath: root, FS: makeFS("third")})
	if st.GetObject(id) != nil || second.Valid() {
		t.Fatal("source-bound raw admission survived changed source")
	}
	var third store.ReadSet
	third.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
	options.Reads = &third
	if !Promote(t.Context(), st, id, options) {
		t.Fatal("replacement source could not replace raw admission")
	}
	if got, _ := third.Get[*manifest.ConfigMap](id); got == nil || got.Data["value"] != "third" || !third.Valid() {
		t.Fatalf("replacement promotion = %+v, valid=%t", got, third.Valid())
	}
	second.Retire()
	third.Retire()
	for range st.Sweep() {
	}
}
