package cli

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// newStreamFixture builds a Bootstrap'd orchestrator over a minimal on-disk
// cluster (one KS named "apps") so the store carries real file-loaded
// objects, then returns a stream emitter attached to it. Reconcile is
// simulated by the tests via SetArtifact + UpdateStatus — no network.
func newStreamFixture(t *testing.T, kinds []string, name string) (*streamEmitter, *store.Store, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
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
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: []\n")

	o, err := orchestrator.New(orchestrator.Config{Path: dir, WipeSecrets: true, CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := o.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	var out, errOut bytes.Buffer
	se := newStreamEmitter(&out, &errOut, o, kinds, name, &commonFlags{}, &buildFlags{})
	t.Cleanup(se.attach(o.Store()))
	return se, o.Store(), &out, &errOut
}

func ksApps() manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
}

func cmDoc(name string) map[string]any {
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": name, "namespace": "default"},
	}
}

// docNames extracts the metadata.name of every doc in a multi-doc YAML blob —
// the order-insensitive identity the set-equivalence assertions compare.
func docNames(t *testing.T, yamlOut string) []string {
	t.Helper()
	var names []string
	for line := range strings.Lines(yamlOut) {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "name: "); ok {
			names = append(names, rest)
		}
	}
	sort.Strings(names)
	return names
}

// TestStreamEmitter_EmitsOnTerminal: a resource streams its docs exactly once
// when it reaches a terminal status; idempotent re-writes don't duplicate.
func TestStreamEmitter_EmitsOnTerminal(t *testing.T) {
	_, st, out, _ := newStreamFixture(t, []string{manifest.KindKustomization}, "")
	id := ksApps()

	st.UpdateStatus(id, store.StatusPending, "rendering")
	if out.Len() != 0 {
		t.Fatalf("Pending emitted docs: %q", out.String())
	}
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("b"), cmDoc("a")}, Fingerprint: "fp1",
	})
	st.UpdateStatus(id, store.StatusReady, "")
	if got, want := docNames(t, out.String()), []string{"a", "b"}; !slices.Equal(got, want) {
		t.Fatalf("streamed docs = %v, want %v", got, want)
	}
	before := out.Len()
	st.UpdateStatus(id, store.StatusReady, "noop re-write")
	if out.Len() != before {
		t.Error("idempotent terminal re-write duplicated streamed docs")
	}
}

// TestStreamEmitter_FailedWithArtifactStreams: a resource that rendered but
// ended Failed still streams — mirroring the buffered path, which emits
// artifacts of failed resources.
func TestStreamEmitter_FailedWithArtifactStreams(t *testing.T) {
	_, st, out, _ := newStreamFixture(t, []string{manifest.KindKustomization}, "")
	id := ksApps()
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("rendered")}, Fingerprint: "fp1",
	})
	st.UpdateStatus(id, store.StatusFailed, "dependency failed after render")
	if got := docNames(t, out.String()); !slices.Equal(got, []string{"rendered"}) {
		t.Fatalf("failed-with-artifact streamed %v, want [rendered]", got)
	}
}

// TestStreamEmitter_ScopeFilters: out-of-scope kinds and non-matching name
// positionals never stream.
func TestStreamEmitter_ScopeFilters(t *testing.T) {
	_, st, out, _ := newStreamFixture(t, []string{manifest.KindHelmRelease}, "other")
	id := ksApps() // KS kind, name "apps" — fails both filters
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("x")}, Fingerprint: "fp1",
	})
	st.UpdateStatus(id, store.StatusReady, "")
	if out.Len() != 0 {
		t.Fatalf("out-of-scope resource streamed: %q", out.String())
	}
}

// TestStreamEmitter_FinishCatchUpAndSetEquivalence: an id that went terminal
// while streaming plus one that never streamed; finish emits the remainder so
// the combined streamed doc set equals the buffered collectRendered set.
func TestStreamEmitter_FinishCatchUpAndSetEquivalence(t *testing.T) {
	se, st, out, _ := newStreamFixture(t, []string{manifest.KindKustomization}, "")
	id := ksApps()

	// Streamed live.
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("live")}, Fingerprint: "fp1",
	})
	st.UpdateStatus(id, store.StatusReady, "")

	// A second KS arrives mid-run (render-emitted) but its terminal status
	// never fires through the listener (simulates post-Run attribution):
	// it must be caught up by finish via Result.Manifests.
	late := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "late"}
	st.AddObject(&manifest.Kustomization{Name: "late", Namespace: "flux-system"})

	res := &orchestrator.Result{Manifests: map[manifest.NamedResource][]map[string]any{
		id:   {cmDoc("live")},
		late: {cmDoc("rs-extension")},
	}}
	if err := se.finish(res); err != nil {
		t.Fatalf("finish: %v", err)
	}
	got := docNames(t, out.String())
	// Buffered reference: collectRendered over the same store/result.
	var want []string
	for _, doc := range mustCollect(t, se, res) {
		meta, _ := doc["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		want = append(want, name)
	}
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("streamed set %v != buffered set %v", got, want)
	}
}

// TestStreamEmitter_RSExtensionTail: ResourceSet-extension docs appended to an
// ALREADY-streamed Kustomization's Result entry after the run are emitted by
// finish — once, without re-emitting the artifact docs.
func TestStreamEmitter_RSExtensionTail(t *testing.T) {
	se, st, out, _ := newStreamFixture(t, []string{manifest.KindKustomization}, "")
	id := ksApps()
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("art")}, Fingerprint: "fp1",
	})
	st.UpdateStatus(id, store.StatusReady, "")

	res := &orchestrator.Result{Manifests: map[manifest.NamedResource][]map[string]any{
		id: {cmDoc("art"), cmDoc("ext")}, // render() appends extensions after artifact docs
	}}
	if err := se.finish(res); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if got := docNames(t, out.String()); !slices.Equal(got, []string{"art", "ext"}) {
		t.Fatalf("docs after finish = %v, want exactly [art ext]", got)
	}
}

// TestStreamEmitter_StaleWarning: a post-stream re-render (changed
// fingerprint) warns on stderr and does not re-emit to stdout.
func TestStreamEmitter_StaleWarning(t *testing.T) {
	se, st, out, errOut := newStreamFixture(t, []string{manifest.KindKustomization}, "")
	id := ksApps()
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("v1")}, Fingerprint: "fp1",
	})
	st.UpdateStatus(id, store.StatusReady, "")
	// Re-render with different content after the stream.
	st.SetArtifact(id, &store.KustomizationArtifact{
		Manifests: []map[string]any{cmDoc("v2")}, Fingerprint: "fp2",
	})

	res := &orchestrator.Result{Manifests: map[manifest.NamedResource][]map[string]any{
		id: {cmDoc("v2")},
	}}
	if err := se.finish(res); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !strings.Contains(errOut.String(), "re-rendered after streaming") {
		t.Errorf("missing stale warning on stderr: %q", errOut.String())
	}
	if got := docNames(t, out.String()); !slices.Equal(got, []string{"v1"}) {
		t.Errorf("stdout docs = %v; the stale id must not re-emit", got)
	}
}

// TestStreamEmitter_NameTypoErrors mirrors the buffered path: an explicit
// name matching nothing errors instead of emitting an empty render.
func TestStreamEmitter_NameTypoErrors(t *testing.T) {
	se, _, _, _ := newStreamFixture(t, []string{manifest.KindKustomization}, "no-such-name")
	err := se.finish(&orchestrator.Result{Manifests: map[manifest.NamedResource][]map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "no-such-name") {
		t.Fatalf("finish err = %v, want name-typo error", err)
	}
}

// TestBuildStream_JSONRejected: --stream is YAML-only.
func TestBuildStream_JSONRejected(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: []\n")
	var out, errOut bytes.Buffer
	code := Run([]string{"build", "all", "--stream", "-o", "json", "--path", dir}, &out, &errOut)
	if code == 0 || !strings.Contains(errOut.String(), "--stream requires YAML") {
		t.Fatalf("code=%d stderr=%q; want rejection of --stream with JSON", code, errOut.String())
	}
}

// mustCollect runs the buffered collectRendered for the emitter's scope —
// the reference for set-equivalence.
func mustCollect(t *testing.T, se *streamEmitter, res *orchestrator.Result) []map[string]any {
	t.Helper()
	var docs []map[string]any
	for _, kind := range se.kinds {
		rendered, err := collectRendered(se.o, res, kind, se.name, se.c, se.b)
		if err != nil {
			t.Fatalf("collectRendered: %v", err)
		}
		docs = append(docs, rendered...)
	}
	return docs
}
