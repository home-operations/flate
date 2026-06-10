package resourceset

import (
	"cmp"
	"slices"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

// resourceSetFingerprint produces a stable hash of the inputs that
// determine resourceset.Render's output for rs: the RS spec PLUS the
// resolved inputsFrom provider set (each RSIP's NamespacedName and its
// ExportedInputs). Hashing the resolved inputs — not the rendered docs —
// is what makes the fingerprint usable BEFORE the render for the dedup
// short-circuit, while still distinguishing two renders that would
// produce different docs: a drain-rerun whose selector matched a new
// RSIP since the last render gets a different fingerprint and re-renders;
// one that converged hashes identically and is a no-op replay.
//
// Degrades safely into a re-render when the payload can't be hashed
// (manifest.Fingerprint returns "", which never matches the dedup check)
// and when StoreResolver errors (the empty inputs digest simply changes
// the fingerprint, forcing a render that surfaces the real error).
func resourceSetFingerprint(rs *manifest.ResourceSet, s *store.Store) string {
	return manifest.Fingerprint(struct {
		Spec   fluxopv1.ResourceSetSpec
		Inputs []inputDigest
	}{
		Spec:   rs.ResourceSetSpec,
		Inputs: resolvedInputDigests(rs, s),
	})
}

// inputDigest pairs an RSIP identity with the digest of its exported
// inputs. The slice is sorted by identity so the fingerprint is
// independent of store iteration / selector-match order.
type inputDigest struct {
	ID     string
	Inputs string
}

// resolvedInputDigests resolves rs.InputsFrom through the same
// StoreResolver resourceset.Render uses and returns one sorted digest
// entry per matching RSIP. An RSIP that fails to export inputs or a
// reference that fails to resolve contributes nothing — the fingerprint
// then differs from a successful resolution, forcing a render.
func resolvedInputDigests(rs *manifest.ResourceSet, s *store.Store) []inputDigest {
	resolve := resourceset.StoreResolver(s)
	var out []inputDigest
	for _, ref := range rs.InputsFrom {
		// InputProviderReference is same-namespace by spec — resolve in the
		// ResourceSet's own namespace, exactly as resourceset.Render does.
		providers, err := resolve(ref, rs.Namespace)
		if err != nil {
			continue
		}
		for _, p := range providers {
			inputs, err := p.ExportedInputs()
			if err != nil {
				continue
			}
			out = append(out, inputDigest{
				ID:     p.Named().String(),
				Inputs: manifest.Fingerprint(inputs),
			})
		}
	}
	slices.SortFunc(out, func(a, b inputDigest) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.Inputs, b.Inputs))
	})
	return out
}
