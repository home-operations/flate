package kustomization

import (
	"testing"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// TestKustomizationFingerprint_StableAcrossLabelStamping locks the
// dedup contract: a KS re-AddObject'd with kustomize ownership
// labels (the typical pattern when the parent KS emits a re-stamped
// child) must produce the same fingerprint as the file-loaded
// original — otherwise the dedup short-circuit can't fire and
// kustomize.RenderFlux runs twice for one logical Kustomization.
func TestKustomizationFingerprint_StableAcrossLabelStamping(t *testing.T) {
	t.Parallel()
	base := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path:            "./apps",
		TargetNamespace: "apps",
	}
	stamped := base.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent-ks",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	stamped.Annotations = map[string]string{"reconcile.fluxcd.io/requestedAt": "now"}

	if got, want := replayKey(stamped.Content("/repo")), replayKey(base.Content("/repo")); got != want {
		t.Errorf("fingerprint changed under label/annotation stamping; got %q want %q", got, want)
	}
}

// TestKustomizationFingerprint_DifferentOnSpecChange flips the
// invariant: when a parent KS injects spec mutations via patches /
// replacements (TargetNamespace, postBuild.substitute, etc.), the
// fingerprint MUST differ so the controller renders the canonical
// post-patch values.
func TestKustomizationFingerprint_DifferentOnSpecChange(t *testing.T) {
	t.Parallel()
	docs, err := manifest.SplitDocs([]byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec: {path: ./apps, targetNamespace: apps, postBuild: {substitute: {VALUE: initial}}}
`))
	if err != nil {
		t.Fatal(err)
	}
	obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	base := obj.(*manifest.Kustomization)
	key := replayKey(base.Content("/repo"))
	for _, tc := range []struct {
		name   string
		change func(*manifest.Kustomization)
	}{
		{"prefix", func(k *manifest.Kustomization) { k.NamePrefix = "typed-" }},
		{"namespace", func(k *manifest.Kustomization) { k.TargetNamespace = "production" }},
		{"substitution", func(k *manifest.Kustomization) { k.PostBuild.Substitute["VALUE"] = "changed" }},
		{"patch", func(k *manifest.Kustomization) { k.Patches = []fluxapis.Patch{{Patch: "changed"}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			patched := base.Clone()
			tc.change(patched)
			if replayKey(patched.Content("/repo")) == key {
				t.Fatal("effective spec change reused the fingerprint")
			}
			if replayKey(base.Content("/repo")) != key {
				t.Fatal("clone edit mutated the original content")
			}
		})
	}
}

// TestKustomizationFingerprint_SourceRootInputs guards that a KS
// resolving to a different on-disk root (e.g. one bootstrap-GR vs.
// a sibling GitRepository) does NOT collide with the file-loaded
// sibling at the same spec.path — the source content differs, so
// the render output differs too.
func TestKustomizationFingerprint_SourceRootInputs(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	}
	if a, b := replayKey(ks.Content("/repo-a")), replayKey(ks.Content("/repo-b")); a == b {
		t.Errorf("fingerprint must differ across distinct sourceRoots; both = %q", a)
	}
}

func TestKustomizationFingerprint_PreparedOverlay(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	ks.PostBuild = &kustomizev1.PostBuild{}
	first, err := kustomize.Prepare(ks, nil, map[string]string{"VALUE": "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := kustomize.Prepare(ks, nil, map[string]string{"VALUE": "second"})
	if err != nil {
		t.Fatal(err)
	}
	if replayKey(first.Content("/repo")) == replayKey(second.Content("/repo")) {
		t.Fatal("effective overlay missing from dedup fingerprint")
	}
	if len(ks.PostBuild.Substitute) != 0 {
		t.Fatal("fingerprinting preparation mutated canonical manifest")
	}
}

func TestKustomizationFingerprint_PreparedSources(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			ks.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: kind, Name: "external"}}}
			var fingerprints []string
			for _, value := range []string{"first", "second", "first"} {
				var source manifest.BaseManifest
				if kind == manifest.KindConfigMap {
					source = &manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": value}}
				} else {
					source = &manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": value}}
				}
				provider := values.ResolvedInputs{{NamedResource: source.Named(), Value: source.(manifest.Data)}}
				prepared, err := kustomize.Prepare(ks, provider, nil)
				if err != nil {
					t.Fatal(err)
				}
				if prepared.PostBuild.Substitute["VALUE"] != value {
					t.Fatal("supplied value missing from prepared manifest")
				}
				fingerprints = append(fingerprints, replayKey(prepared.Content("/repo")))
			}
			if fingerprints[0] == "" || fingerprints[0] == fingerprints[1] || fingerprints[0] != fingerprints[2] {
				t.Fatalf("supplied values missing from stable fingerprint: %v", fingerprints)
			}
			if len(ks.PostBuild.Substitute) != 0 {
				t.Fatal("preparation mutated canonical manifest")
			}
		})
	}
}

func TestKustomizationFingerprint_NoSubstitutionOptions(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.AddObject(&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "repository"}})
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	ks.PostBuild = &kustomizev1.PostBuild{}
	ks.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "external"}}}
	ordinary, err := kustomize.Prepare(ks, values.NewStoreProvider(st, true), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := replayKey(ordinary.Content("/repo"))
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{name: "nil"},
		{name: "empty", opts: Options{SubstituteFrom: map[manifest.NamedResource]manifest.BaseManifest{}, Substitute: map[string]string{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := New(st, task.NewBounded(2), nil, false)
			c.Configure(tc.opts, sourcecache.RetryConfig{})
			var resolved depwait.Resolution
			err := c.Require(&depwait.Waiter{Parent: ks.Named(), Declaration: ks}, manifest.ReconcileAttempt{}, &resolved)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := kustomize.Prepare(ks, resolved.Inputs, c.substitute)
			if err != nil {
				t.Fatal(err)
			}
			if got := replayKey(prepared.Content("/repo")); got != want || got == "" {
				t.Fatalf("no-option fingerprint=%q, ordinary=%q", got, want)
			}
		})
	}
}

func TestKustomizationFingerprint_ArtifactKey(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{Path: "app"}
	first := &store.SourceArtifact{LocalPath: "/label", Key: "one"}
	second := &store.SourceArtifact{LocalPath: "/label", Key: "two"}
	if replayKey(ks.Content(struct{ Path, Key, Revision, Digest string }{first.LocalPath, first.Key, first.Revision, first.Digest})) == replayKey(ks.Content(struct{ Path, Key, Revision, Digest string }{second.LocalPath, second.Key, second.Revision, second.Digest})) {
		t.Fatal("changed artifact key did not change the render fingerprint")
	}
}

func replayKey(content any) string {
	c := base.New(store.New(), nil, "test")
	fp, _, _ := c.FingerprintDedup(manifest.NamedResource{}, content)
	return fp
}
