package loader

import (
	"context"
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
	l := New(st)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
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
	if _, ok := l.Existence.Get(hrID); !ok {
		t.Errorf("HelmRelease must be recorded in Existence index")
	}

	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cluster-settings"}
	if st.GetObject(cmID) != nil {
		t.Errorf("ConfigMap should NOT be in Store under DiscoveryOnly; should be in Existence")
	}
	if _, ok := l.Existence.Get(cmID); !ok {
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
	l := New(st)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}

	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "my-cm"}
	if st.GetObject(id) != nil {
		t.Fatalf("precondition: CM should not be in store yet")
	}
	if !l.Existence.Promote(st, id, true) {
		t.Fatalf("Promote should return true on a known id")
	}
	if st.GetObject(id) == nil {
		t.Errorf("Promote did not materialize CM into Store")
	}
}

// TestExistenceIndex_PromoteNilReceiverSafe confirms the defensive
// nil-receiver guard: a nil index promoted against any id should be
// a no-op false return, not a panic. The orchestrator's
// resolveMissing closure relies on this when DiscoveryOnly is off
// and Existence was never initialized.
func TestExistenceIndex_PromoteNilReceiverSafe(t *testing.T) {
	var idx *ExistenceIndex
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "x"}
	if got := idx.Promote(store.New(), id, true); got {
		t.Errorf("nil index Promote should return false, got true")
	}
}

// TestExistenceIndex_PromoteUnknownIDReturnsFalse covers the path
// where depwait's missing-dep fallback asks for a CM that never
// reached file-load: existence index has no entry, so Promote bails
// out cleanly and the depwait surfaces "dependency not found" as
// intended.
func TestExistenceIndex_PromoteUnknownIDReturnsFalse(t *testing.T) {
	idx := NewExistenceIndex()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ghost"}
	if got := idx.Promote(store.New(), id, true); got {
		t.Errorf("Promote on unknown id should return false, got true")
	}
}

// TestExistenceIndex_PromoteFileRemovedBetweenRecordAndPromote
// exercises the race where the indexed file was deleted (or
// permission-stripped) between the file walk and the lazy lookup.
// Promote should fail gracefully (return false), not panic, so the
// depwait surfaces "dependency not found" instead of crashing the
// orchestrator.
func TestExistenceIndex_PromoteFileRemovedBetweenRecordAndPromote(t *testing.T) {
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
	l := New(st)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Remove the file behind the index.
	if err := os.Remove(filepath.Join(dir, "cm.yaml")); err != nil {
		t.Fatalf("remove: %v", err)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "ephemeral"}
	if got := l.Existence.Promote(st, id, true); got {
		t.Errorf("Promote on a removed file should return false, got true")
	}
	if st.GetObject(id) != nil {
		t.Errorf("removed file should not have produced a Store entry")
	}
}

// TestExistenceIndex_PromoteMultiDocLoadsSiblings pins the
// "whole-file parse" contract documented on Promote: a YAML file
// packing CM + Secret + HR together promotes ALL three when ANY one
// is requested. This is what allows depwait to avoid re-opening the
// same file once per dep in the common multi-doc fixture pattern.
func TestExistenceIndex_PromoteMultiDocLoadsSiblings(t *testing.T) {
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
	l := New(st)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
	if _, err := l.Load(context.Background(), dir); err != nil {
		t.Fatalf("Load: %v", err)
	}
	cmID := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "default", Name: "app-config"}
	secretID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "default", Name: "app-secret"}
	if !l.Existence.Promote(st, cmID, true) {
		t.Fatalf("Promote(cm) should succeed")
	}
	if st.GetObject(cmID) == nil {
		t.Errorf("CM not in store after Promote")
	}
	if st.GetObject(secretID) == nil {
		t.Errorf("sibling Secret should have been promoted alongside the CM (whole-file parse contract)")
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
	l := New(st)
	l.Options.DiscoveryOnly = true
	l.Existence = NewExistenceIndex()
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
