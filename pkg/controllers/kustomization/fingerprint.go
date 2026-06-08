package kustomization

import (
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// kustomizationFingerprint produces a stable hash of the post-Prepare
// inputs that determine kustomize.RenderFlux's output for ks. The
// resolved sourceRoot is included so a sibling KS that points at a
// different source artifact path doesn't collide. Labels and
// annotations are excluded on purpose: kustomize-controller-emitted
// children carry stamped ownership labels that don't affect the
// rendered manifests, and re-rendering on that delta is pure waste.
// Degrades safely into a re-render when the payload can't be hashed
// (manifest.Fingerprint returns "", which never matches the dedup check).
func kustomizationFingerprint(ks *manifest.Kustomization, sourceRoot string) string {
	return manifest.Fingerprint(struct {
		Path                string
		SourceRoot          string
		Contents            map[string]any
		PostBuildSubstitute map[string]any
		Spec                kustomizev1.KustomizationSpec
	}{
		Path:                ks.Path,
		SourceRoot:          sourceRoot,
		Contents:            ks.Contents,
		PostBuildSubstitute: ks.PostBuildSubstitute,
		Spec:                ks.KustomizationSpec,
	})
}
