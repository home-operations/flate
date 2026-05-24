package kustomization

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
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
		KustomizationSpec: kustomizev1.KustomizationSpec{
			Path:            "./apps",
			TargetNamespace: "apps",
		},
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
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps", TargetNamespace: "apps"},
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
		KustomizationSpec: kustomizev1.KustomizationSpec{Path: "./apps"},
	}
	if a, b := kustomizationFingerprint(ks, "/repo-a"), kustomizationFingerprint(ks, "/repo-b"); a == b {
		t.Errorf("fingerprint must differ across distinct sourceRoots; both = %q", a)
	}
}
