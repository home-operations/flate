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
		objs = append(objs, parsed{obj: obj, reconcilable: shouldDispatchAsObject(obj)})
	}
	// Accumulate every reconcilable child's id across both passes so
	// the renderedSet write can flush through MarkRenderedBatch in a
	// single lock acquisition. N children → 1 r.mu.Lock instead of N
	// (the prior per-child markRendered loop was the contention hot
	// spot on KS-heavy fixtures).
	rendered := make([]manifest.NamedResource, 0, len(objs))
	// Pass 1 — data first.
	for _, p := range objs {
		if p.reconcilable && isLeafReconcilable(p.obj) {
			continue
		}
		if p.reconcilable {
			childID := p.obj.Named()
			c.keepEmitted(id, childID)
			c.Store.AddObject(p.obj)
			rendered = append(rendered, childID)
		} else {
			c.Store.AddRendered(p.obj)
		}
	}
	// Pass 2 — leaf reconcilables.
	var leaves []manifest.BaseManifest
	for _, p := range objs {
		if p.reconcilable && isLeafReconcilable(p.obj) {
			childID := p.obj.Named()
			c.keepEmitted(id, childID)
			rendered = append(rendered, childID)
			leaves = append(leaves, p.obj)
		}
	}
	c.markRenderedBatch(id, rendered)
	c.Store.AddObjects(leaves)
}

// keepEmitted extends the change filter's keep set so render-emitted
// children pass the changed-only-mode PreGate check. Without this,
// kustomize component+replacement patterns (parent KS emitting a
// per-app KS via ConfigMap-driven replacements) produce silent gaps
// in `flate diff`: the leaf KS isn't keep-set'd at filter-build time
// because it didn't exist on disk, never reconciles, and its render
// output never reaches the diff comparison. Issue #204.
//
// Routed through Filter.AddEmitted so an ancestor-only parent
// (kept for #58's patch-propagation but with no file change of its
// own) doesn't cascade unrelated file-loaded children into keep on
// every render. Primary parents — those whose own file changed or
// that share an owner with a changed file — still propagate their
// emissions normally.
//
// Called BEFORE Store.AddObject so the listener that fires
// synchronously during AddObject sees the extended keep set when it
// invokes PreGate.
func (c *Controller) keepEmitted(parent, child manifest.NamedResource) {
	if f := c.Filter(); f != nil {
		f.AddEmitted(parent, child)
	}
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
