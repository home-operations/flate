package orchestrator

import (
	"context"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestDetectOrphans drives the orphan-detection logic in isolation —
// no controllers, just the store / sourceFiles wiring the orchestrator
// builds during Bootstrap. Three scenarios:
//
//  1. Truly orphaned child (file under parent path, never emitted by
//     parent's render): downgraded.
//  2. Root-level resource (no covering parent): NOT downgraded.
//  3. Child re-emitted by parent (WasRendered set): NOT downgraded.
func TestDetectOrphans(t *testing.T) {
	parent := &manifest.Kustomization{
		Name: "cluster-apps", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps"},
	}
	orphan := &manifest.Kustomization{
		Name: "orphan", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps/orphan/app"},
	}
	emittedChild := &manifest.Kustomization{
		Name: "wired", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./kubernetes/apps/wired/app"},
	}
	root := &manifest.Kustomization{
		Name: "another-root", Namespace: "flux-system",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./standalone"},
	}

	o := &Orchestrator{
		store:    store.New(),
		rendered: newRenderedSet(),
		sourceFiles: map[manifest.NamedResource]string{
			parent.Named():       "kubernetes/flux/cluster/ks.yaml",
			orphan.Named():       "kubernetes/apps/orphan/ks.yaml",
			emittedChild.Named(): "kubernetes/apps/wired/ks.yaml",
			root.Named():         "kubernetes/standalone/ks.yaml",
		},
	}
	for _, ks := range []*manifest.Kustomization{parent, orphan, emittedChild, root} {
		o.store.AddObject(ks)
	}
	// Mark emittedChild as rendered by its parent — simulates the
	// AddObject + MarkRendered call cluster-apps's render would make.
	o.rendered.MarkRendered(emittedChild.Named())

	failed := map[manifest.NamedResource]store.StatusInfo{
		orphan.Named():       {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		emittedChild.Named(): {Status: store.StatusFailed, Message: "TIMEZONE undefined"},
		root.Named():         {Status: store.StatusFailed, Message: "broken"},
	}

	orphans := o.detectOrphans(failed)

	if _, ok := orphans[orphan.Named()]; !ok {
		t.Errorf("expected orphan to be detected")
	}
	if _, ok := orphans[emittedChild.Named()]; ok {
		t.Errorf("re-emitted child is not an orphan: parent's render covered it")
	}
	if _, ok := orphans[root.Named()]; ok {
		t.Errorf("root resource with no covering parent is not an orphan")
	}
	if got := len(orphans); got != 1 {
		t.Errorf("expected exactly 1 orphan, got %d: %+v", got, orphans)
	}
}

// TestDetectOrphans_NonReconcilableKindsIgnored — ConfigMaps and
// Secrets that fail (they can't fail in practice but the failure
// map is permissive) are never reported as orphans; orphan
// detection only applies to Kustomization + HelmRelease.
func TestDetectOrphans_NonReconcilableKindsIgnored(t *testing.T) {
	parent := &manifest.Kustomization{
		Name: "p", Namespace: "ns",
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	cm := &manifest.ConfigMap{Name: "stuck", Namespace: "ns"}

	o := &Orchestrator{
		store: store.New(),
		sourceFiles: map[manifest.NamedResource]string{
			parent.Named(): "ks.yaml",
			cm.Named():     "apps/stuck/cm.yaml",
		},
	}
	o.store.AddObject(parent)
	o.store.AddObject(cm)

	failed := map[manifest.NamedResource]store.StatusInfo{
		cm.Named(): {Status: store.StatusFailed, Message: "bogus"},
	}
	orphans := o.detectOrphans(failed)
	if len(orphans) != 0 {
		t.Errorf("orphan detection should skip non-reconcilable kinds; got %+v", orphans)
	}
}

func TestOrchestrator_SimpleCluster(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := o.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := len(o.Store().ListObjects(manifest.KindKustomization)); got != 1 {
		t.Errorf("expected 1 Kustomization, got %d", got)
	}
	if got := len(o.Store().ListObjects(manifest.KindConfigMap)); got < 1 {
		t.Errorf("expected at least 1 ConfigMap after reconcile, got %d", got)
	}
}

// TestOrchestrator_Render exercises the embed-friendly Render() entry:
// one call drives Bootstrap + Run + result collection, and returns a
// structured Result keyed by NamedResource. Embedders previously had
// to scrape o.Store().GetArtifact(id) per-kind after Run.
func TestOrchestrator_Render(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: hello
data: {k: v}
`)

	o, err := New(Config{Path: dir, WipeSecrets: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if res == nil {
		t.Fatal("Render returned nil Result")
	}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	mans, ok := res.Manifests[ksID]
	if !ok {
		t.Fatalf("Result.Manifests missing %s; keys=%v", ksID, keysOf(res.Manifests))
	}
	if len(mans) == 0 {
		t.Errorf("expected rendered docs for %s; got empty", ksID)
	}
	if len(res.Failed) != 0 {
		t.Errorf("expected no failures; got %v", res.Failed)
	}
	if len(res.Orphans) != 0 {
		t.Errorf("expected no orphans; got %v", res.Orphans)
	}
}

// TestOrchestrator_Render_AppliesSkipKinds locks the iter-16
// embed-facing follow-on to #169: HelmOptions.SkipResourceKinds()
// applies uniformly to BOTH HR and KS docs in Result.Manifests, not
// just to the CLI emit paths. Without this, an embedder calling
// orchestrator.New + Render would see HR-rendered Secrets dropped
// but KS-rendered Secrets retained — the same asymmetry the CLI
// fix patched at a different layer.
func TestOrchestrator_Render_AppliesSkipKinds(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml",
		"resources:\n- cm.yaml\n- secret.yaml\n")
	testutil.WriteFile(t, dir, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: kept-cm, namespace: default}
data: {k: v}
`)
	testutil.WriteFile(t, dir, "apps/secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: dropped-secret, namespace: default}
stringData: {password: hunter2}
`)

	o, err := New(Config{
		Path:        dir,
		WipeSecrets: true,
		HelmOptions: helm.Options{SkipSecrets: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	mans := res.Manifests[ksID]
	if len(mans) == 0 {
		t.Fatalf("Result.Manifests[apps] should retain non-Secret docs; got empty")
	}
	for _, doc := range mans {
		if manifest.DocKind(doc) == manifest.KindSecret {
			t.Errorf("Result.Manifests should not include Secret docs when SkipSecrets=true; got %v", doc)
		}
	}
	// ConfigMap must survive — sanity that the filter only drops Secret.
	var sawCM bool
	for _, doc := range mans {
		if manifest.DocKind(doc) == manifest.KindConfigMap {
			sawCM = true
		}
	}
	if !sawCM {
		t.Errorf("ConfigMap should remain in Result.Manifests; mans=%v", mans)
	}
}

// TestOrchestrator_AllowMissingSecretsPropagates locks the #190 fix:
// when --allow-missing-secrets is on AND a source's auth Secret isn't
// in the offline tree, the source soft-skips (Ready+"skipped:") and
// the downstream KS that consumes it propagates the skip rather than
// failing with "source artifact not found". `flate test` then reports
// SKIPPED, not FAILED.
func TestOrchestrator_AllowMissingSecretsPropagates(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "oci.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: private-app
  namespace: default
spec:
  interval: 5m
  url: oci://example.invalid/private/app
  secretRef:
    name: ghcr-creds
`)
	testutil.WriteFile(t, dir, "ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: private-app
  namespace: default
spec:
  interval: 5m
  path: ./
  sourceRef:
    kind: OCIRepository
    name: private-app
    namespace: default
`)

	o, err := New(Config{
		Path:                dir,
		EnableOCI:           true,
		AllowMissingSecrets: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	ociID := manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "default", Name: "private-app"}
	ksID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "default", Name: "private-app"}

	if _, failed := res.Failed[ociID]; failed {
		t.Errorf("OCIRepository must not be in Failed when --allow-missing-secrets is on; got %v", res.Failed[ociID])
	}
	if _, failed := res.Failed[ksID]; failed {
		t.Errorf("dependent KS must propagate skip, not fail; got %v", res.Failed[ksID])
	}

	ociInfo, ok := o.Store().GetStatus(ociID)
	if !ok || !store.IsSkipped(ociInfo) {
		t.Errorf("OCIRepository should be Ready+skipped; got %+v", ociInfo)
	}
	ksInfo, ok := o.Store().GetStatus(ksID)
	if !ok || !store.IsSkipped(ksInfo) {
		t.Errorf("KS should be Ready+skipped; got %+v", ksInfo)
	}
}

// TestOrchestrator_AllowMissingSecretsHRSkipsBeforePull locks the
// silent-anonymous-pull regression the iter-17 edge-case review
// flagged: an HR with chartRef → OCIRepository whose auth Secret is
// missing MUST propagate the skip BEFORE helm.TemplateDocs runs.
// Without the pre-check, the source-controller's soft-skip leaves
// no on-disk artifact but the helm client's locateOCIChart would
// still try an oras-pull against the registry — succeeding silently
// against a public mirror, or failing with an opaque registry error.
// Either way the user's "the auth is missing" signal is lost.
func TestOrchestrator_AllowMissingSecretsHRSkipsBeforePull(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "oci.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: app-template
  namespace: flux-system
spec:
  interval: 5m
  url: oci://example.invalid/private/chart
  secretRef:
    name: ghcr-creds
`)
	testutil.WriteFile(t, dir, "hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: demo
  namespace: default
spec:
  interval: 5m
  chartRef:
    kind: OCIRepository
    name: app-template
    namespace: flux-system
`)

	o, err := New(Config{
		Path:                dir,
		EnableOCI:           true,
		AllowMissingSecrets: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	res, err := o.Render(context.Background())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}

	hrID := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "default", Name: "demo"}
	if _, failed := res.Failed[hrID]; failed {
		t.Errorf("HR must propagate skip before helm.TemplateDocs runs; got %v", res.Failed[hrID])
	}
	hrInfo, ok := o.Store().GetStatus(hrID)
	if !ok || !store.IsSkipped(hrInfo) {
		t.Errorf("HR should be Ready+skipped (chart source skipped); got %+v. "+
			"If status is Failed with a registry-pull error, the pre-check is missing and "+
			"helm tried an anonymous pull — exactly the silent-downgrade the pre-check exists to prevent.",
			hrInfo)
	}
}

func keysOf[V any](m map[manifest.NamedResource]V) []manifest.NamedResource {
	out := make([]manifest.NamedResource, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestOrchestrator_TypedListener verifies Store.OnStatus delivers a
// typed StatusInfo payload (no `any` type-switching needed by the
// embedder).
func TestOrchestrator_TypedListener(t *testing.T) {
	s := store.New()
	id := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "k"}

	var seen store.StatusInfo
	unsub := s.OnStatus(func(other manifest.NamedResource, info store.StatusInfo) {
		if other == id {
			seen = info
		}
	}, false)
	defer unsub()

	s.UpdateStatus(id, store.StatusFailed, "boom")
	if seen.Status != store.StatusFailed || seen.Message != "boom" {
		t.Errorf("typed listener did not receive payload: %+v", seen)
	}
}
