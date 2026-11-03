package kustomization

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxapis "github.com/fluxcd/pkg/apis/kustomize"

	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
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
// kustomize.Render runs twice for one logical Kustomization.
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

	got := replayKey(t.Context(), stamped.Content("/repo", map[string]struct{}{"VALUE": {}}, true))
	want := replayKey(t.Context(), base.Content("/repo", map[string]struct{}{"VALUE": {}}, true))
	if got != want {
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
	docs, err := testutil.SplitDocs([]byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
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
	key := replayKey(t.Context(), base.Content("/repo", map[string]struct{}{"VALUE": {}}, true))
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
			if replayKey(t.Context(), patched.Content("/repo", map[string]struct{}{"VALUE": {}}, true)) == key {
				t.Fatal("effective spec change reused the fingerprint")
			}
			if replayKey(t.Context(), base.Content("/repo", map[string]struct{}{"VALUE": {}}, true)) != key {
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
	ks := &manifest.Kustomization{Name: "apps", Namespace: "flux-system", Path: "./apps"}
	a := replayKey(t.Context(), ks.Content("/repo-a", map[string]struct{}{"VALUE": {}}, true))
	b := replayKey(t.Context(), ks.Content("/repo-b", map[string]struct{}{"VALUE": {}}, true))
	if a == b {
		t.Errorf("fingerprint must differ across distinct sourceRoots; both = %q", a)
	}
}

func TestKustomizationFingerprint_PreparedInline(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "first"}},
	}
	first, err := kustomize.Prepare(ks, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := ks.Clone()
	other.PostBuild.Substitute["VALUE"] = "second"
	second, err := kustomize.Prepare(other, nil)
	if err != nil {
		t.Fatal(err)
	}
	if replayKey(t.Context(), first.Content("/repo", map[string]struct{}{"VALUE": {}}, true)) ==
		replayKey(t.Context(), second.Content("/repo", map[string]struct{}{"VALUE": {}}, true)) {
		t.Fatal("authored substitution missing from fingerprint")
	}
	if ks.PostBuild.Substitute["VALUE"] != "first" {
		t.Fatal("preparation mutated canonical manifest")
	}
}

func TestKustomizationFingerprint_PreparedSources(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			ks.PostBuild = &kustomizev1.PostBuild{
				SubstituteFrom: []manifest.SubstituteReference{{Kind: kind, Name: "external"}},
			}
			var fingerprints []string
			for _, value := range []string{"first", "second", "first"} {
				var source manifest.BaseManifest
				if kind == manifest.KindConfigMap {
					source = &manifest.ConfigMap{
						Name:      "external",
						Namespace: "apps",
						Data:      map[string]any{"VALUE": value},
					}
				} else {
					source = &manifest.Secret{
						Name:       "external",
						Namespace:  "apps",
						StringData: map[string]any{"VALUE": value},
					}
				}
				supplied, err := store.NewSupplied([]store.SuppliedObject{{Object: source}})
				if err != nil {
					t.Fatal(err)
				}
				st := store.New()
				st.SetSupplied(supplied)
				provider := values.NewStoreProvider(st, true)
				prepared, err := kustomize.Prepare(ks, provider)
				if err != nil {
					t.Fatal(err)
				}
				if prepared.PostBuild.Substitute["VALUE"] != value {
					t.Fatal("supplied value missing from prepared manifest")
				}
				fingerprints = append(
					fingerprints,
					replayKey(t.Context(), prepared.Content("/repo", map[string]struct{}{"VALUE": {}}, true)),
				)
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
	ks.PostBuild = &kustomizev1.PostBuild{
		SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "external"}},
	}
	ordinary, err := kustomize.Prepare(ks, values.NewStoreProvider(st, true))
	if err != nil {
		t.Fatal(err)
	}
	want := replayKey(t.Context(), ordinary.Content("/repo", map[string]struct{}{"VALUE": {}}, true))
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{name: "nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := New(st, task.NewBounded(2), nil, false)
			c.Configure(tc.opts, sourcecache.RetryConfig{})
			prepared, err := kustomize.Prepare(
				ks,
				values.NewStoreProvider(&c.newExecution(ks.Named()).run.Reads, c.WipeSecrets),
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := replayKey(
				t.Context(),
				prepared.Content("/repo", map[string]struct{}{"VALUE": {}}, true),
			); got != want ||
				got == "" {
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
	if replayKey(
		t.Context(),
		ks.Content(
			struct{ Path, Key, Revision, Digest string }{first.LocalPath, first.Key, first.Revision, first.Digest},
			map[string]struct{}{"VALUE": {}},
			true,
		),
	) ==
		replayKey(
			t.Context(),
			ks.Content(
				struct{ Path, Key, Revision, Digest string }{
					second.LocalPath,
					second.Key,
					second.Revision,
					second.Digest,
				},
				map[string]struct{}{"VALUE": {}},
				true,
			),
		) {
		t.Fatal("changed artifact key did not change the render fingerprint")
	}
}

func TestKustomizationFingerprint_EffectivePresenceAndActivation(t *testing.T) {
	t.Parallel()
	ks := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"OTHER": "active"}},
	}
	names := map[string]struct{}{"MISSING": {}}
	absent := replayKey(t.Context(), ks.Content("/repo", names, true))
	present := ks.Clone()
	present.PostBuild.Substitute["MISSING"] = ""
	if replayKey(t.Context(), present.Content("/repo", names, true)) == absent {
		t.Fatal("absent and present-empty effective entries collided")
	}
	empty := map[string]struct{}{}
	if replayKey(
		t.Context(),
		ks.Content("/repo", empty, true),
	) == replayKey(
		t.Context(),
		ks.Content("/repo", empty, false),
	) {
		t.Fatal("active zero-name bag lost activation")
	}

	if _, found := ks.PostBuild.Substitute["MISSING"]; found || len(ks.PostBuild.Substitute) != 1 {
		t.Fatal("projection mutated the canonical manifest")
	}
}

func replayKey(ctx context.Context, content any) string {
	c := base.New(store.New(), nil)
	fp, _, _ := controllertest.NewExecution(c, manifest.NamedResource{}).FingerprintDedup(
		ctx,
		manifest.NamedResource{},
		content,
		&store.Emission{},
	)
	return fp
}
