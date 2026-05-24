// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish a KustomizationArtifact. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	// Staging is a process-wide cache that materializes source roots
	// into writable copies so Flux's Generator can write the merged
	// kustomization.yaml without touching the user's working tree.
	Staging *kustomize.StagingCache

	// WipeSecrets controls whether Secret cleartext is wiped when
	// parsing rendered manifests.
	WipeSecrets bool

	// Set via Configure() — see Options.
	parentOf       func(manifest.NamedResource) (manifest.NamedResource, bool)
	renderTracker  RenderTracker
	resolveMissing func(manifest.NamedResource) bool
}

// RenderTracker is the tiny seam the controller uses to report
// "this child id was emitted by THIS parent KS's render" to the
// orchestrator. Nil is OK — the controller no-ops.
//
// The parent linkage is consumed by detectOrphans, the parent
// resolver (combined with the file-loaded path-prefix index), and
// ResourceSet extension attribution — every place that needs to
// query parent provenance for render-emitted resources.
type RenderTracker interface {
	MarkRendered(parent, child manifest.NamedResource)
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
	RenderTracker RenderTracker
	// ResolveMissing, when non-nil, is forwarded to every depwait
	// Waiter built during reconcile. The orchestrator wires this
	// against the loader's ExistenceIndex so a substituteFrom CM
	// rendered by a sibling KS can be lazy-loaded on demand instead
	// of failing the parent KS with "dependency not found."
	ResolveMissing func(manifest.NamedResource) bool
}

// New constructs a Kustomization controller.
func New(s *store.Store, t *task.Service, staging *kustomize.StagingCache, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t),
		Staging:     staging,
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// Start — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options) {
	c.SetFilter(opts.Filter)
	c.parentOf = opts.ParentOf
	c.renderTracker = opts.RenderTracker
	c.resolveMissing = opts.ResolveMissing
}

// markRendered reports a parent→child render emission to the
// orchestrator's tracker if one is wired; no-op otherwise.
// Centralizes the nil-check so the reconcile body stays readable.
func (c *Controller) markRendered(parent, child manifest.NamedResource) {
	if c.renderTracker != nil {
		c.renderTracker.MarkRendered(parent, child)
	}
}

// newWaiter constructs a depwait.Waiter pre-wired with the
// controller's Store and lazy-promotion fallback, parented to id and
// budgeted from timeout (typically ks.Timeout).
func (c *Controller) newWaiter(id manifest.NamedResource, timeout *metav1.Duration) *depwait.Waiter {
	return &depwait.Waiter{
		Store:          c.Store,
		Parent:         id,
		Timeout:        depwait.TimeoutFromSpec(timeout),
		ResolveMissing: c.resolveMissing,
	}
}

// Start registers the listener that drives reconciliation.
func (c *Controller) Start(ctx context.Context) {
	c.StartLifecycle("kustomization")
	c.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx))
}

func (c *Controller) onObjectAdded(ctx context.Context) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		if id.Kind != manifest.KindKustomization {
			return
		}
		ks, ok := payload.(*manifest.Kustomization)
		if !ok {
			return
		}
		if c.PreGate(id, ks.Suspend) {
			return
		}
		c.Submit(ctx, id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "kustomization", c.reconcile)
		})
	}
}

func (c *Controller) reconcile(ctx context.Context, ks *manifest.Kustomization) error {
	id := ks.Named()
	c.Store.UpdateStatus(id, store.StatusPending, "resolving dependencies")

	deps := c.collectDeps(ks)
	if len(deps) > 0 {
		// Release our worker slot for the duration of the dep wait so
		// children we depend on can acquire one and make progress.
		// Without this, N parents on a bounded Service consume all
		// slots while waiting for children that need a slot to run.
		var sum depwait.Summary
		c.Tasks.YieldSlot(func() {
			sum = depwait.WaitAll(c.newWaiter(id, ks.Timeout).Watch(ctx, deps))
		})
		if sum.AnyFailed() {
			return &manifest.DependencyFailedError{
				Parent:  id,
				Failed:  sum.Failed,
				Reasons: sum.Messages,
			}
		}
		// Refresh the KS — our structural parent may have re-emitted
		// us with mutated spec (e.g. `replacements:` injecting
		// spec.targetNamespace) while we were waiting. Without this
		// re-read the first render would use the stale-spec snapshot
		// captured by RunWithStatus, producing duplicate renders that
		// linger in the store with the wrong namespace. See #102.
		if fresh, ok := c.Store.GetObject(id).(*manifest.Kustomization); ok {
			ks = fresh
		}
	}

	c.Store.UpdateStatus(id, store.StatusPending, "resolving source artifact")
	sourceRoot, err := c.resolveSourceRoot(ks)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "expanding substitutions")
	// kustomize.Prepare clones ks and expands postBuild.substituteFrom —
	// the same pre-render dance an embedder calling RenderFlux directly
	// would perform. Keeping one canonical implementation here means
	// changes to the contract only land in one place. Mirrors helm.Prepare.
	ks, err = kustomize.Prepare(ks, values.NewStoreProvider(c.Store))
	if err != nil {
		return err
	}

	// Fingerprint dedup: the same id may receive multiple AddObject
	// events with the same effective spec (e.g. when the structural
	// parent re-emits this KS after running its own render, stamping
	// kustomize.toolkit.fluxcd.io ownership labels). kustomize.RenderFlux
	// is the hot path; skip it and reuse the prior artifact when the
	// post-Prepare inputs are byte-identical. Same pattern as HR (#219).
	fp := kustomizationFingerprint(ks, sourceRoot)
	if existing, ok := c.Store.GetArtifact(id).(*store.KustomizationArtifact); ok && existing.Fingerprint != "" && existing.Fingerprint == fp {
		slog.Debug("kustomization: skipped re-render (fingerprint unchanged)", "id", id.String())
		return nil
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering")
	data, err := kustomize.RenderFlux(ctx, c.Staging, sourceRoot, ks.Path, ks.Contents)
	if err != nil {
		return err
	}
	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return err
	}

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
	c.emitRenderedChildren(id, docs)

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
// every non-Optional postBuild.substituteFrom ConfigMap.
//
// substituteFrom ConfigMap edges fix the race where the referenced CM
// is emitted by another KS's render: without the edge, KS-A would
// race KS-B and Prepare would silently expand with empty values for
// vars that should have come from KS-B's CM. Flux's eventual-
// consistency reconcile loop self-heals; flate is one-shot, so a
// missed substitution shows up as broken rendered output.
//
// Secret refs are deliberately NOT added as depwait edges. In real
// repos substituteFrom Secrets are almost always SOPS-encrypted or
// ExternalSecret-managed — they live in cluster state that flate
// cannot materialize offline. values.ExpandPostBuildSubstituteReference
// already gracefully degrades on missing Secrets (logs DEBUG and
// continues); adding a hard edge here would regress every offline
// render against a Flux repo that uses secret-substitute patterns.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := append([]manifest.DependencyRef(nil), ks.DependsOn...)
	if ks.SourceKind != "" && ks.SourceName != "" {
		deps = append(deps, manifest.DependencyRef{
			NamedResource: manifest.NamedResource{
				Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
			},
		})
	}
	if c.parentOf != nil {
		if parent, ok := c.parentOf(ks.Named()); ok {
			deps = append(deps, manifest.DependencyRef{NamedResource: parent})
		}
	}
	for _, ref := range ks.PostBuildSubstituteFrom {
		if ref.Optional {
			// Optional refs are best-effort — their absence shouldn't
			// gate reconcile. Matches Flux's own substituteFrom
			// semantics for Optional=true.
			continue
		}
		if ref.Kind != manifest.KindConfigMap {
			continue
		}
		if ref.Name == "" {
			continue
		}
		deps = append(deps, manifest.DependencyRef{
			NamedResource: manifest.NamedResource{
				Kind: ref.Kind, Namespace: ks.Namespace, Name: ref.Name,
			},
		})
	}
	return deps
}

// resolveSourceRoot returns the on-disk root the kustomization should
// be built from — i.e. the source artifact's local path. The Flux
// renderer then joins ks.Path onto this root.
func (c *Controller) resolveSourceRoot(ks *manifest.Kustomization) (string, error) {
	srcID := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	if ks.SourceKind == "" || ks.SourceName == "" {
		// Child Kustomizations that inherit sourceRef from a parent's
		// render patches show empty here until that parent reconciles.
		// Fall back to the seeded bootstrap GitRepository (the user's
		// working tree) so the first reconcile resolves to the repo
		// root instead of doubling ks.Path against itself (#105).
		if ks.Path == "" {
			return "", fmt.Errorf("%w: kustomization %s has no path and no source",
				manifest.ErrInput, ks.NamespacedName())
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
			return "", fmt.Errorf("%w: source %s %s", manifest.ErrSourceSkipped, srcID.String(), info.Message)
		}
		return "", fmt.Errorf("%w: source %s artifact not found", manifest.ErrObjectNotFound, srcID.String())
	}
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa.LocalPath, nil
	}
	return "", fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}
