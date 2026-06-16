// Package emit holds the render-emission helpers shared by the
// Kustomization and ResourceSet controllers: both parse a rendered doc
// set and land the children in the store through the identical two-pass
// strategy, so the logic lives here exactly once rather than being
// copied per controller.
package emit

import (
	"log/slog"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
)

// Child is one parsed render-emission: the typed object and the source doc it
// was parsed from. Children returns these in doc order so a caller can
// post-process them (e.g. the ResourceSet controller routes RawObject children
// to its output sink) without re-parsing.
type Child struct {
	Obj manifest.BaseManifest
	Doc map[string]any
}

// Children parses the rendered docs and lands them in the store using a
// two-pass emission strategy:
//
//   - Pass 1 — non-leaf "data" kinds (ConfigMap, Secret, sources,
//     ResourceSet, RSIP, etc.) go into the store first. Sources go
//     through AddObject because they have their own status to track;
//     ConfigMap/Secret/RSIP have no controller (or no schedulable node),
//     so AddObject's event dispatch only wakes waiters. Either way
//     they're in the store before pass 2 fires.
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
//
// publish gates the two event-firing store writes (AddObject for pass-1
// reconcilables, AddObjects for pass-2 leaves). The fresh-render path
// passes true. The fingerprint-dedup replay passes FALSE: the children
// were already published byte-identically by the render that set the
// cached artifact, so re-AddObject-ing them would only re-fire
// EventObjectAdded and re-submit a coalesced re-reconcile of an already-
// settled child — churn that transiently flips a Ready parent back to
// Pending and, if a transient quiescence drain lands in that window,
// makes a dependent's depwait give up ("parent ... not ready"). The
// provenance (ReportRendered) and keep-set (KeepEmitted) side-effects
// the replay exists for are idempotent and event-free, so they still
// run on both paths; only the redundant republish is skipped.
//
// Returns the parsed children in doc order (build directives and parse
// failures excluded), so a caller can post-process them without re-parsing —
// the ResourceSet controller routes the RawObject children to its output sink.
func Children(c *base.Controller, wipeSecrets bool, id manifest.NamedResource, docs []map[string]any, publish bool) []Child {
	type parsed struct {
		Child
		reconcilable bool
		leaf         bool // held for pass 2 (Kustomization / HelmRelease)
	}
	opts := manifest.ParseDocOptions{WipeSecrets: wipeSecrets}
	// Log under the invoking controller's identity plus component=emit, so a
	// "skipped doc" line names both the controller whose render produced the
	// doc and the emit helper it surfaced in.
	log := c.Logger().With(slog.String("component", "emit"))
	objs := make([]parsed, 0, len(docs))
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			log.Debug("SOPS-encrypted resource wiped to placeholder",
				"id", id.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			log.Debug("skipped doc", "id", id.String(), "err", err)
			continue
		}
		if manifest.IsKustomizeBuildDirective(obj) {
			continue // build input, not a cluster resource — never store it
		}
		reconcilable := ShouldDispatchAsObject(obj)
		objs = append(objs, parsed{
			Child:        Child{Obj: obj, Doc: doc},
			reconcilable: reconcilable,
			leaf:         reconcilable && IsLeafReconcilable(obj),
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
	// A KS whose spec.path re-includes its own definition file (the Zariel
	// self-emit, or a cross-tree base/ overlay it belongs to, #777) re-emits
	// ITSELF. Its own render is never the source of truth for its spec — that's
	// its file or its parent's substituted/patched emission — and re-storing the
	// self-copy would strip a parent-injected postBuild, flip-flopping spec.path
	// back to unsubstituted. Skip the store write; renderedSet already drops the
	// self-edge for attribution.
	selfEmit := func(obj manifest.BaseManifest) bool { return obj.Named() == id }
	// Pass 1 — data first.
	for _, p := range objs {
		switch {
		case p.leaf:
			continue // held for pass 2
		case p.reconcilable:
			keep(p.Obj)
			if publish && !selfEmit(p.Obj) {
				c.Store.AddObject(p.Obj)
			}
		default:
			c.Store.AddRendered(p.Obj)
		}
	}
	// Pass 2 — leaf reconcilables.
	var leaves []manifest.BaseManifest
	for _, p := range objs {
		if p.leaf {
			keep(p.Obj)
			if !selfEmit(p.Obj) {
				leaves = append(leaves, p.Obj)
			}
		}
	}
	c.ReportRendered(id, rendered)
	if publish {
		c.Store.AddObjects(leaves)
	}
	children := make([]Child, len(objs))
	for i, p := range objs {
		children[i] = p.Child
	}
	return children
}

// ShouldDispatchAsObject reports whether a render-emitted Flux
// resource needs to fire EventObjectAdded so its own controller picks
// it up. The pattern is: parent Kustomization renders → emits a
// child Flux resource (e.g. another Kustomization with parent patches
// applied, a HelmRelease, an OCIRepository fanned out from a kustomize
// component, a ResourceSet) → that child's controller must reconcile
// the patched version, not the statically-loaded one.
//
// ResourceSet is included so a render-emitted RS expands through the DAG
// like any other reconcilable child. ResourceSetInputProvider is
// included so a render-emitted RSIP fires EventObjectAdded to WAKE any
// RS parked on it (or selector-matching RS at the drain fixpoint) — but
// it is pure data: the scheduler's dagSchedulable excludes it, so the
// arrival event registers no runnable node, exactly like a ConfigMap.
func ShouldDispatchAsObject(obj manifest.BaseManifest) bool {
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
		*manifest.Secret,
		*manifest.ResourceSet,
		*manifest.ResourceSetInputProvider:
		return true
	}
	return false
}

// IsLeafReconcilable reports whether an emitted object should be held
// for pass 2. Kustomization + HelmRelease have controllers that fire
// substituteFrom / chartRef lookups against the store the instant
// their AddObject event arrives; emitting them after all "data" kinds
// guarantees those lookups succeed. A ResourceSet is NOT a leaf — it is
// pass-1 data, emitted before the leaves so any KS/HR it itself produces
// (re-emitted in turn through this helper) lands behind its own data.
func IsLeafReconcilable(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.Kustomization, *manifest.HelmRelease:
		return true
	}
	return false
}
