package loader

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestDiscoveryOnly_SkipsNonDiscoveryKinds locks the step-4 contract:
// under DiscoveryOnly, the file walker AddObjects only KS + RS + RSIP
// + source kinds. HRs, CMs, Secrets, and raw manifests stay out of
// the Store and land in Existence instead.
func TestDiscoveryOnly_SkipsNonDiscoveryKinds(t *testing.T) {
	dir := t.TempDir()
	writeExistenceFile(t, dir, "ks.yaml", `
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
	writeExistenceFile(t, dir, "cm.yaml", `
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
	if !Promote(l.Existence, st, id, true) {
		t.Fatalf("Promote should return true on a known id")
	}
	if st.GetObject(id) == nil {
		t.Errorf("Promote did not materialize CM into Store")
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
	writeExistenceFile(t, dir, "src.yaml", `
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

func writeExistenceFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
