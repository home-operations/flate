package loader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestDiscoveryOnly_SkipsNonDiscoveryKinds locks the step-4 contract:
// under DiscoveryOnly, the file walker AddObjects only KS + RS + RSIP
// + source kinds. HRs, CMs, Secrets, and raw manifests stay out of
// the Store and land in Existence instead.
func TestDiscoveryOnly_SkipsNonDiscoveryKinds(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: podinfo
  namespace: default
spec:
  chart:
    spec:
      chart: podinfo
      sourceRef:
        kind: HelmRepository
        name: podinfo
        namespace: default
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  FOO: bar
`)

	st := store.New()
	l := loaderFixture(st, nil)
	l.Options.DiscoveryOnly = true
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	if st.GetObject(ksID) == nil {
		t.Errorf("Kustomization should be in Store under DiscoveryOnly")
	}

	hrID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "podinfo"}
	if st.GetObject(hrID) != nil {
		t.Errorf("HelmRelease should NOT be in Store under DiscoveryOnly; should be in Existence")
	}
	if _, ok := l.Index.Lookup(hrID); !ok {
		t.Errorf("HelmRelease must be recorded in Existence index")
	}

	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if st.GetObject(cmID) != nil {
		t.Errorf("ConfigMap should NOT be in Store under DiscoveryOnly; should be in Existence")
	}
	if _, ok := l.Index.Lookup(cmID); !ok {
		t.Errorf("ConfigMap must be recorded in Existence index")
	}
}

// TestDiscoveryOnly_PromoteMaterializesFromIndex covers the
// lazy-promotion contract: when depwait hits a missing dep that the
// Existence index knows about, Promote re-parses the file and
// AddObjects it into the Store so the wait can clear.
func TestDiscoveryOnly_PromoteMaterializesFromIndex(t *testing.T) {
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
	l := loaderFixture(st, nil)
	l.Options.DiscoveryOnly = true
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "my-cm"}
	if st.GetObject(id) != nil {
		t.Fatalf("precondition: CM should not be in store yet")
	}
	if !Promote(l.Store, id, l.Index.Records[id].Path, PromoteOptions{WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}) {
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
	idx := loaderFixture(store.New(), nil)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ghost"}
	if got := Promote(idx.Store, id, idx.Index.Records[id].Path, PromoteOptions{WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}); got {
		t.Errorf("Promote on unknown id should return false, got true")
	}
}

// TestPromote_FileRemovedBetweenRecordAndPromote
// exercises the race where the indexed file was deleted (or
// permission-stripped) between the file walk and the lazy lookup.
// Promote should fail gracefully (return false), not panic, so the
// depwait surfaces "dependency not found" instead of crashing the
// orchestrator.
func TestPromote_FileRemovedBetweenRecordAndPromote(t *testing.T) {
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
	l := loaderFixture(st, nil)
	l.Options.DiscoveryOnly = true
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Remove the file behind the index.
	if err := os.Remove(filepath.Join(dir, "cm.yaml")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ephemeral"}
	if got := Promote(l.Store, id, l.Index.Records[id].Path, PromoteOptions{WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}); got {
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
	l := loaderFixture(st, nil)
	l.Options.DiscoveryOnly = true
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}
	secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "default", Name: "app-secret"}
	if !Promote(l.Store, cmID, l.Index.Records[cmID].Path, PromoteOptions{WipeSecrets: true, Admit: func(manifest.BaseManifest) bool { return true }}) {
		t.Fatalf("Promote(cm) should succeed")
	}
	if st.GetObject(cmID) == nil {
		t.Errorf("CM not in store after Promote")
	}
	if st.GetObject(secretID) == nil {
		t.Errorf("sibling Secret should have been promoted alongside the CM (whole-file parse contract)")
	}
}

func TestPromote_AdmissionAndPreferExisting(t *testing.T) {
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
			idx := loaderFixture(st, nil)
			for _, id := range []manifest.NamedResource{cm.Named(), secret} {
				idx.Index.Records[id] = manifest.DiscoveryRecord{Path: filepath.Join(dir, "bundle.yaml")}
			}
			id := cm.Named()
			if tc.secret {
				id = secret
			}
			admit := func(obj manifest.BaseManifest) bool { return obj.Named() == cm.Named() }
			if got := Promote(idx.Store, id, idx.Index.Records[id].Path, PromoteOptions{WipeSecrets: true, Admit: admit}); got == tc.secret {
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

// TestDiscoveryOnly_SourcesStayFileLoaded pins the regression fix
// found on JJGadgets/Biohazard: sources (GitRepository,
// OCIRepository, HelmRepository) must remain in the Store under
// DiscoveryOnly, otherwise aliasBootstrapSources can't recognize
// them and aliases every source URL to the working tree (silently
// redirecting every KS render to the wrong source root).
func TestDiscoveryOnly_SourcesStayFileLoaded(t *testing.T) {
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
	l.Options.DiscoveryOnly = true
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	for _, want := range []manifest.NamedResource{
		{Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "upstream"},
		{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "charts"},
		{Kind: manifest.KindHelmRepository, Namespace: "default", Name: "podinfo"},
	} {
		if st.GetObject(want) == nil {
			t.Errorf("source %s must stay in Store under DiscoveryOnly; got nil", want.String())
		}
	}
}

func TestPromote_BareDocumentProjection(t *testing.T) {
	const doc = `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: podinfo%s}
spec:
  interval: 1h
  chart: {spec: {chart: podinfo, version: %s, sourceRef: {kind: HelmRepository, name: podinfo}}}
`
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "bundle.yaml", fmt.Sprintf(doc, "", "1.0.0")+"---\n"+fmt.Sprintf(doc, ", namespace: other", "2.0.0")+"---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: note}\n")
	for _, tc := range []struct{ namespace, version string }{{"apps", "1.0.0"}, {"other", "2.0.0"}} {
		t.Run(tc.namespace, func(t *testing.T) {
			st := store.New()
			id := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: tc.namespace, Name: "podinfo"}
			admit := func(manifest.BaseManifest) bool { return true }
			if !Promote(st, id, filepath.Join(dir, "bundle.yaml"), PromoteOptions{Admit: admit}) {
				t.Fatalf("Promote(%s) = false", id)
			}
			if got := st.GetObject(id).(*manifest.HelmRelease).Chart.Version; got != tc.version {
				t.Errorf("%s chart version = %s, want %s", id, got, tc.version)
			}
			if sibling := (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: tc.namespace, Name: "note"}); st.GetObject(sibling) != nil {
				t.Errorf("sibling %s must keep its own identity", sibling)
			}
		})
	}
}
