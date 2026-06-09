// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish a KustomizationArtifact. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates Kustomization reconciliation. Reconcile-
// shaping state (Filter, ParentOf) flows in via Configure exactly once
// before Start. The invariant — "config is read-only after Start" — is
// encoded in the embedded *base.Controller, not just in code review.
type Controller struct {
	*base.Controller

	// Trees is the per-run in-memory render cache: it captures each source
	// root once into an immutable byte snapshot from which every render
	// derives a private in-memory filesystem (see kustomize.RenderFlux).
	Trees *kustomize.TreeCache

	// WipeSecrets controls whether Secret cleartext is wiped when
	// parsing rendered manifests.
	WipeSecrets bool

	// selfProduces reports whether a Kustomization's own render emits a
	// given ConfigMap (see Options.SelfProduces). collectDeps consults it
	// to drop a self-produced substituteFrom CM from the dep set. Nil when
	// unset (tests / no repoRoot) → edge always added.
	selfProduces func(cm, consumer manifest.NamedResource) bool
}

// Options carries the post-bootstrap state the orchestrator wires onto
// the controller before Start. Filter narrows reconciliation to
// changed resources in changed-only mode. ParentOf maps each Flux
// Kustomization to its structural parent so reconcile waits for the
// parent's Ready before rendering (so any parent-render-time spec
// mutations — e.g. `replacements:` injecting spec.targetNamespace —
// are observable when the child renders). A nil ParentOf disables
// parent-enforcement, matching pre-#102 behavior. RenderTracker
// receives every reconcilable child this KS emits during render.
type Options struct {
	Filter        *change.Filter
	ParentOf      func(manifest.NamedResource) (manifest.NamedResource, bool)
	RenderTracker base.RenderTracker
	// Existence is the file-existence lookup the orchestrator wires
	// against the loader's ExistenceIndex. Classify uses it to lazy-
	// promote file-indexed deps and to distinguish render-only
	// deps from typo'd ones. See depwait.ExistenceLookup for the
	// decision matrix. Forwarded to every Waiter built during reconcile.
	Existence depwait.ExistenceLookup
	// PreflightFailure reports dependency-graph errors discovered by the
	// orchestrator before reconcile. When set for an id, the controller
	// marks the resource Failed and does not render it.
	PreflightFailure func(manifest.NamedResource) (string, bool)
	// SelfProduces reports whether consumer's OWN render emits cm. When
	// it does, collectDeps drops cm from the dependency set — a KS can't
	// hard-wait on a postBuild.substituteFrom ConfigMap that only its own
	// render produces (the bjw-s/onedr0p self-substitute). Available in
	// full mode (graph-aware index), unlike the changed-only producer
	// skip. Nil → the edge is always added (pre-index behavior).
	SelfProduces func(cm, consumer manifest.NamedResource) bool
}

// New constructs a Kustomization controller.
func New(s *store.Store, t *task.Service, trees *kustomize.TreeCache, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t),
		Trees:       trees,
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options) {
	c.SetFilter(opts.Filter)
	c.SetDepwait(opts.Existence)
	c.SetPreflight(opts.PreflightFailure)
	c.SetParentOf(opts.ParentOf)
	c.SetRenderTracker(opts.RenderTracker)
	c.selfProduces = opts.SelfProduces
}

// Start wires lifecycle state. The scheduler owns dispatch (via ReconcileNode)
// and the orchestrator's dagrun wires its own discovery/wake listeners, so
// Start registers no dispatch listener of its own.
func (c *Controller) Start(_ context.Context) {
	c.StartLifecycle()
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized) and whether id ended Ready. The
// orchestrator's scheduler Dispatcher calls this for Kustomization nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, drainLevel int) (blocked []manifest.NamedResource, ready bool) {
	return base.DispatchNode(ctx, c.Controller, id, drainLevel,
		func(ks *manifest.Kustomization) bool { return ks.Suspend },
		"kustomization", c.reconcile)
}

func (c *Controller) reconcile(ctx context.Context, ks *manifest.Kustomization) error {
	id := ks.Named()
	if err := c.PreflightError(id); err != nil {
		return err
	}
	// SetPendingUnlessReady (not a raw UpdateStatus) on every pre-render
	// progress write: a no-op re-reconcile of an already-Ready KS (re-emitted
	// by its structural parent / coalesced re-run) must not transiently
	// downgrade Ready→Pending, or a dependent's quiescence-bound depwait can
	// re-read it at a transient pool drain and give up ("parent Kustomization
	// not ready"). The genuine-render downgrade at "rendering" below (after the
	// fingerprint dedup) stays unconditional so a changed re-render re-gates
	// dependents. See base.SetPendingUnlessReady (#657).
	c.SetPendingUnlessReady(id, "resolving dependencies")

	deps := c.collectDeps(ks)
	if len(deps) > 0 {
		// RequireRefresh fuses the gate with the load-bearing re-read: our
		// structural parent may have re-emitted us with mutated spec (e.g.
		// `replacements:` injecting spec.targetNamespace) while we were
		// waiting. Without the refresh the first render would use the
		// stale-spec snapshot captured by RunWithStatus, producing
		// duplicate renders that linger in the store with the wrong
		// namespace. See #102.
		fresh, ok, err := base.RequireRefresh[*manifest.Kustomization](
			ctx, c.Controller, id, ks.Timeout, deps,
			"", base.DepFailed(id)) // empty pendingMsg: status set above (kept Ready on a no-op re-run)
		if err != nil {
			return err
		}
		if ok {
			ks = fresh
		}
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	c.SetPendingUnlessReady(id, "resolving source artifact")
	sourceRoot, applyIgnore, err := c.resolveSource(ks)
	if err != nil {
		return err
	}

	c.SetPendingUnlessReady(id, "expanding substitutions")
	// kustomize.Prepare clones ks and expands postBuild.substituteFrom —
	// the same pre-render dance an embedder calling RenderFlux directly
	// would perform. Keeping one canonical implementation here means
	// changes to the contract only land in one place. Mirrors helm.Prepare.
	ks, err = kustomize.Prepare(ks, values.NewStoreProvider(c.Store))
	if err != nil {
		return err
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Fingerprint dedup (kustomize.RenderFlux is the hot path): skip the
	// re-render and replay the cached artifact's side-effects when the
	// post-Prepare inputs are byte-identical to the cached artifact. The
	// shared helper publishes nothing (publish=false) — see its doc for the
	// churn/quiescence rationale (#657–#660). fp is reused for SetArtifact below.
	fp := kustomizationFingerprint(ks, sourceRoot)
	if handled, err := c.FingerprintDedup(id, fp, "kustomization", func(docs []map[string]any) {
		c.emitRenderedChildren(id, docs, false)
	}); handled {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	data, err := kustomize.RenderFlux(ctx, c.Trees, sourceRoot, applyIgnore, ks.Path, ks.Contents)
	if err != nil {
		return err
	}
	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return err
	}
	docs = manifest.FlattenLists(docs)

	// Per-resource envsubst. Flux's kustomize-controller skips
	// substitution on any resource carrying the
	// "kustomize.toolkit.fluxcd.io/substitute: disabled" label or
	// annotation — used in real repos for ConfigMaps that embed
	// shell scripts with bash array expansions (${ARR[@]}) that
	// envsubst's parser cannot handle. Mirror that behavior here:
	// substitute per-doc, skip opted-out resources, so we match Flux
	// bit-for-bit.
	if vars := values.VarsMap(ks.PostBuildSubstitute); len(vars) > 0 {
		for i, doc := range docs {
			if manifest.HasSubstituteDisabled(doc) {
				continue
			}
			substituted, sErr := substituteDoc(doc, vars)
			if sErr != nil {
				return sErr
			}
			docs[i] = substituted
		}
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.PreflightError(id); err != nil {
		return err
	}
	c.emitRenderedChildren(id, docs, true)

	c.Store.SetArtifact(id, &store.KustomizationArtifact{
		Path:        filepath.Join(sourceRoot, ks.Path),
		Manifests:   docs,
		Fingerprint: fp,
	})
	return nil
}

// collectDeps assembles the dependency refs whose readiness must
// precede this Kustomization: explicit dependsOn entries (carrying any
// CEL ReadyExpr), the source ref, the implicit structural parent (the
// enclosing Flux KS that renders us — must finish first so any
// parent-render-time spec injections land before our reconcile), and
// every non-Optional postBuild.substituteFrom ConfigMap (or, in
// changed-only mode, the unchanged producer Kustomization that renders
// that ConfigMap).
//
// substituteFrom ConfigMap/producer edges fix the race where the
// referenced CM is emitted by another KS's render: without the edge,
// KS-A would race KS-B and Prepare would silently expand with empty
// values for vars that should have come from KS-B's CM. Flux's
// eventual-consistency reconcile loop self-heals; flate is one-shot,
// so a missed substitution shows up as broken rendered output.
//
// Secret refs are deliberately NOT added as depwait edges. In real
// repos substituteFrom Secrets are almost always SOPS-encrypted or
// ExternalSecret-managed — they live in cluster state that flate
// cannot materialize offline. values.ExpandPostBuildSubstituteReference
// already gracefully degrades on missing Secrets (logs DEBUG and
// continues); adding a hard edge here would regress every offline
// render against a Flux repo that uses secret-substitute patterns.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := slices.Clone(ks.DependsOn)
	if ks.SourceKind != "" && ks.SourceName != "" {
		deps = append(deps, manifest.DependencyRef{
			NamedResource: manifest.NamedResource{
				Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
			},
		})
	}
	if parent, ok := c.LookupParent(ks.Named()); ok {
		deps = append(deps, manifest.DependencyRef{NamedResource: parent})
	}
	for _, ref := range ks.PostBuildSubstituteFrom {
		// Shared with change.transitiveDepsOf via manifest.IsHardConfigMapEdge:
		// only a non-Optional, named ConfigMap is a hard offline edge (Optional
		// is best-effort, Secrets are SOPS/ExternalSecret-managed). Keep-set and
		// reconcile ordering MUST agree on this — see the predicate's doc + #418.
		if !manifest.IsHardConfigMapEdge(ref) {
			continue
		}
		depID := manifest.NamedResource{Kind: ref.Kind, Namespace: ks.Namespace, Name: ref.Name}
		// Drop the hard CM edge ONLY when THIS KS's own render subtree
		// emits it (the bjw-s/onedr0p self-substitute: cluster-apps' bare-
		// dir render produces ConfigMap/flux-system/cluster-settings via a
		// component). Graph-aware and available in full mode, unlike the
		// changed-only ProducersFor skip below. A CM produced by a
		// DIFFERENT KS — or genuinely absent — keeps the edge and fails
		// loudly: a KS waiting on a CM only its own render can emit would
		// otherwise deadlock against itself.
		if c.selfProduces == nil || !c.selfProduces(depID, ks.Named()) {
			deps = append(deps, manifest.DependencyRef{NamedResource: depID})
		}
		// In changed-only mode, when the substituteFrom CM is rendered
		// by another Flux Kustomization (the producer), wait on that
		// producer too — the CM dep alone doesn't gate ordering when
		// the producer's reconcile is what materializes the CM. Only
		// KS producers form depwait edges (generator-produced CMs have
		// no controller to wait on); skip the self-producer case
		// (bjw-s self-substitute pattern).
		if f := c.Filter(); f != nil {
			for _, producer := range f.ProducersFor(depID) {
				if producer == ks.Named() {
					continue
				}
				if producer.Kind != manifest.KindKustomization {
					continue
				}
				deps = append(deps, manifest.DependencyRef{NamedResource: producer})
			}
		}
	}
	return deps
}

// resolveSource returns the source's on-disk root and whether
// source-controller's default file exclusions must be applied when the tree is
// materialized for the build.
//
// applyIgnore is true exactly for working-tree / self-referential sources — the
// bootstrap GitRepository alias and overrideSelfReferentialGitRepositories —
// which expose the user's raw working tree and never passed through a fetcher's
// artifact filtering. Every real fetcher (git, OCI, bucket) already ran
// source.ApplyIgnore on the LocalPath it publishes and records a Revision or
// Digest, so those are materialized as-is (applyIgnore=false); re-filtering them
// would be redundant and, for buckets, wrong (defaults vs no-defaults). The
// working-tree aliases are the only artifacts that set neither Revision nor
// Digest, which is precisely the signal.
func (c *Controller) resolveSource(ks *manifest.Kustomization) (sourceRoot string, applyIgnore bool, err error) {
	srcID := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	if ks.SourceKind == "" || ks.SourceName == "" {
		// Child Kustomizations that inherit sourceRef from a parent's
		// render patches show empty here until that parent reconciles.
		// Fall back to the seeded bootstrap GitRepository (the user's
		// working tree) so the first reconcile resolves to the repo
		// root instead of doubling ks.Path against itself (#105).
		if ks.Path == "" {
			return "", false, fmt.Errorf("%w: kustomization %s has no path and no source",
				manifest.ErrInput, ks.Named().NamespacedName())
		}
		srcID = manifest.BootstrapSourceID
	}
	art := c.Store.GetArtifact(srcID)
	if art == nil {
		// A source that soft-skipped (--allow-missing-secrets) reports
		// Ready with a "skipped:" reason but writes no artifact. Surface
		// that as a typed skip so the caller can mark the KS skipped too
		// rather than reporting a generic "artifact not found" failure.
		if info, ok := c.Store.GetStatus(srcID); ok && store.IsSkipped(info) {
			return "", false, fmt.Errorf("%w: source %s %s", manifest.ErrSourceSkipped, srcID.String(), info.Message)
		}
		return "", false, fmt.Errorf("%w: source %s artifact not found", manifest.ErrObjectNotFound, srcID.String())
	}
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa.LocalPath, sa.Digest == "" && sa.Revision == "", nil
	}
	return "", false, fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}
