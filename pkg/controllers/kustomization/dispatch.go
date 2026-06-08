package kustomization

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/manifest"
)

// emitRenderedChildren parses the rendered docs and lands them in the
// store using a two-pass emission strategy:
//
//   - Pass 1 — non-leaf "data" kinds (ConfigMap, Secret, sources, etc.)
//     go into the store first. Sources go through AddObject because
//     they have their own status to track; ConfigMap/Secret have no
//     controller so AddObject's event dispatch is a no-op for them.
//     Either way they're in the store before pass 2 fires.
//
//   - Pass 2 — leaf reconcilables (Kustomization, HelmRelease). Their
//     substituteFrom / chartRef lookups now see the data from pass 1.
//
// Without the two passes, AddObject for a reconcilable kind fires its
// controller on a separate goroutine immediately, racing the parent's
// "data first" emission. Within each pass the controller renders docs
// in kustomize's emission order; passes themselves are ordered so the
// data backing a reconcile always arrives first.
//
// Parse errors on inline resources are debug-logged and skipped — they
// may be raw Kubernetes manifests flate doesn't track. SOPS-encrypted
// secrets are debug-noted; ParseSecret wipes their values to the
// PLACEHOLDER token the same way --wipe-secrets does for cleartext.
func (c *Controller) emitRenderedChildren(id manifest.NamedResource, docs []map[string]any) {
	type parsed struct {
		obj          manifest.BaseManifest
		reconcilable bool
		leaf         bool // held for pass 2 (Kustomization / HelmRelease)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	objs := make([]parsed, 0, len(docs))
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			slog.Debug("kustomization: SOPS-encrypted resource wiped to placeholder",
				"id", id.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("kustomization: skipped doc", "id", id.String(), "err", err)
			continue
		}
		if manifest.IsKustomizeBuildDirective(obj) {
			continue // build input, not a cluster resource — never store it
		}
		reconcilable := shouldDispatchAsObject(obj)
		objs = append(objs, parsed{
			obj:          obj,
			reconcilable: reconcilable,
			leaf:         reconcilable && isLeafReconcilable(obj),
		})
	}
	// Accumulate every reconcilable child's id across both passes so
	// the renderedSet write can flush through MarkRenderedBatch in a
	// single lock acquisition. N children → 1 r.mu.Lock instead of N
	// (the prior per-child markRendered loop was the contention hot
	// spot on KS-heavy fixtures).
	rendered := make([]manifest.NamedResource, 0, len(objs))
	keep := func(obj manifest.BaseManifest) {
		c.KeepEmitted(id, obj)
		rendered = append(rendered, obj.Named())
	}
	// Pass 1 — data first.
	for _, p := range objs {
		switch {
		case p.leaf:
			continue // held for pass 2
		case p.reconcilable:
			keep(p.obj)
			c.Store.AddObject(p.obj)
		default:
			c.Store.AddRendered(p.obj)
		}
	}
	// Pass 2 — leaf reconcilables.
	var leaves []manifest.BaseManifest
	for _, p := range objs {
		if p.leaf {
			keep(p.obj)
			leaves = append(leaves, p.obj)
		}
	}
	c.ReportRendered(id, rendered)
	c.Store.AddObjects(leaves)
}

// shouldDispatchAsObject reports whether a render-emitted Flux
// resource needs to fire EventObjectAdded so its own controller picks
// it up. The pattern is: parent Kustomization renders → emits a
// child Flux resource (e.g. another Kustomization with parent patches
// applied, a HelmRelease, an OCIRepository fanned out from a kustomize
// component) → that child's controller must reconcile the patched
// version, not the statically-loaded one.
func shouldDispatchAsObject(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.Kustomization,
		*manifest.HelmRelease,
		*manifest.HelmRepository,
		*manifest.OCIRepository,
		*manifest.GitRepository,
		*manifest.Bucket,
		*manifest.HelmChartSource,
		*manifest.ExternalArtifact,
		*manifest.ConfigMap,
		*manifest.Secret:
		return true
	}
	return false
}

// isLeafReconcilable reports whether an emitted object should be held
// for pass 2. Kustomization + HelmRelease have controllers that fire
// substituteFrom / chartRef lookups against the store the instant
// their AddObject event arrives; emitting them after all "data" kinds
// guarantees those lookups succeed.
func isLeafReconcilable(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.Kustomization, *manifest.HelmRelease:
		return true
	}
	return false
}
