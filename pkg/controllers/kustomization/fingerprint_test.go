package kustomization

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
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

	if got, want := kustomizationFingerprint(stamped, "/repo"), kustomizationFingerprint(base, "/repo"); got != want {
		t.Errorf("fingerprint changed under label/annotation stamping; got %q want %q", got, want)
	}
}

// TestKustomizationFingerprint_DifferentOnSpecChange flips the
// invariant: when a parent KS injects spec mutations via patches /
// replacements (TargetNamespace, postBuild.substitute, etc.), the
// fingerprint MUST differ so the controller renders the canonical
// post-patch values.
func TestKustomizationFingerprint_DifferentOnSpecChange(t *testing.T) {
	base := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps", TargetNamespace: "apps",
	}
	patched := base.Clone()
	patched.TargetNamespace = "production"

	if got := kustomizationFingerprint(base, "/repo"); got == kustomizationFingerprint(patched, "/repo") {
		t.Errorf("fingerprint should differ when spec.targetNamespace mutates; both = %q", got)
	}
}

// TestKustomizationFingerprint_SourceRootInputs guards that a KS
// resolving to a different on-disk root (e.g. one bootstrap-GR vs.
// a sibling GitRepository) does NOT collide with the file-loaded
// sibling at the same spec.path — the source content differs, so
// the render output differs too.
func TestKustomizationFingerprint_SourceRootInputs(t *testing.T) {
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
	}
	if a, b := kustomizationFingerprint(ks, "/repo-a"), kustomizationFingerprint(ks, "/repo-b"); a == b {
		t.Errorf("fingerprint must differ across distinct sourceRoots; both = %q", a)
	}
}

func TestKustomizationFingerprint_PreparedOverlay(t *testing.T) {
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	ks.PostBuild = &kustomizev1.PostBuild{}
	first, err := kustomize.PrepareWithSubstitutions(ks, nil, map[string]string{"VALUE": "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := kustomize.PrepareWithSubstitutions(ks, nil, map[string]string{"VALUE": "second"})
	if err != nil {
		t.Fatal(err)
	}
	if kustomizationFingerprint(first, "/repo") == kustomizationFingerprint(second, "/repo") {
		t.Fatal("effective overlay missing from dedup fingerprint")
	}
	if len(ks.PostBuildSubstitute) != 0 {
		t.Fatal("fingerprinting preparation mutated canonical manifest")
	}
}

func TestKustomizationFingerprint_PreparedSources(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			ks.PostBuild = &kustomizev1.PostBuild{}
			ks.PostBuildSubstituteFrom = []manifest.SubstituteReference{{Kind: kind, Name: "external"}}
			var fingerprints []string
			for _, value := range []string{"first", "second", "first"} {
				var source manifest.BaseManifest
				if kind == manifest.KindConfigMap {
					source = &manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": value}}
				} else {
					source = &manifest.Secret{Name: "external", Namespace: "apps", StringData: map[string]any{"VALUE": value}}
				}
				provider := &substitutionProvider{
					sources:  map[manifest.NamedResource]manifest.BaseManifest{source.Named(): source},
					fallback: values.NewStoreProvider(store.New()),
				}
				prepared, err := kustomize.PrepareWithSubstitutions(ks, provider, nil)
				if err != nil {
					t.Fatal(err)
				}
				if prepared.PostBuildSubstitute["VALUE"] != value {
					t.Fatal("supplied value missing from prepared manifest")
				}
				fingerprints = append(fingerprints, kustomizationFingerprint(prepared, "/repo"))
			}
			if fingerprints[0] == "" || fingerprints[0] == fingerprints[1] || fingerprints[0] != fingerprints[2] {
				t.Fatalf("supplied values missing from stable fingerprint: %v", fingerprints)
			}
			if len(ks.PostBuildSubstitute) != 0 {
				t.Fatal("preparation mutated canonical manifest")
			}
		})
	}
}

func TestKustomizationFingerprint_NoSubstitutionOptions(t *testing.T) {
	st := store.New()
	st.AddObject(&manifest.ConfigMap{Name: "external", Namespace: "apps", Data: map[string]any{"VALUE": "repository"}})
	ks := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	ks.PostBuild = &kustomizev1.PostBuild{}
	ks.PostBuildSubstituteFrom = []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "external"}}
	ordinary, err := kustomize.Prepare(ks, values.NewStoreProvider(st))
	if err != nil {
		t.Fatal(err)
	}
	want := kustomizationFingerprint(ordinary, "/repo")
	for _, tc := range []struct {
		name string
		opts Options
	}{
		{name: "nil"},
		{name: "empty", opts: Options{SubstituteFrom: map[manifest.NamedResource]manifest.BaseManifest{}, Substitute: map[string]string{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(st, task.NewBounded(2), nil, false)
			c.Configure(tc.opts)
			prepared, err := kustomize.PrepareWithSubstitutions(ks, c.substitutionProvider, c.substitute)
			if err != nil {
				t.Fatal(err)
			}
			if got := kustomizationFingerprint(prepared, "/repo"); got != want || got == "" {
				t.Fatalf("no-option fingerprint=%q, ordinary=%q", got, want)
			}
		})
	}
}
