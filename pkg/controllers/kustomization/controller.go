// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish an Emission. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"errors"
	"fmt"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates Kustomization reconciliation. Reconcile-
// shaping state (Filter, ParentOf) flows in via Configure exactly once
// before StartLifecycle. The invariant — "config is read-only after StartLifecycle" — is
// encoded in the embedded *base.Controller, not just in code review.
type Controller struct {
	*base.Controller
	retry cache.RetryConfig

	// Trees shares remote fetches and persisted renders.
	Trees *kustomize.TreeCache

	// WipeSecrets controls whether Secret cleartext is wiped when
	// parsing rendered manifests.
	WipeSecrets bool

	index               *manifest.DiscoveryIndex
	substitutionSources map[manifest.NamedResource]manifest.BaseManifest
	substitute          map[string]string
	strictSubstitutions bool
}

// Options carries the post-bootstrap state the orchestrator wires onto the
// controller before StartLifecycle. base.Options holds the common reconcile config.
type Options struct {
	base.Options
	// SubstituteFrom is the immutable bootstrap-approved postBuild source snapshot.
	SubstituteFrom map[manifest.NamedResource]manifest.BaseManifest
	// Substitute layers over referenced and inline postBuild values.
	Substitute map[string]string
	// StrictSubstitutions rejects undefined variables without defaults.
	StrictSubstitutions bool
}

// New constructs a Kustomization controller.
func New(s *store.Store, t *task.Service, trees *kustomize.TreeCache, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t, "kustomization"),
		Trees:       trees,
		WipeSecrets: wipeSecrets,
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// StartLifecycle — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options, retry cache.RetryConfig) {
	c.Controller.Configure(opts.Options)
	c.retry = retry
	c.index = opts.Index
	c.substitutionSources = opts.SubstituteFrom
	c.substitute = opts.Substitute
	c.strictSubstitutions = opts.StrictSubstitutions
}

// ReconcileNode runs one Kustomization invocation and returns any remaining typed wait.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, attempt manifest.ReconcileAttempt) manifest.ReconcileWait {
	return c.DispatchNode(ctx, id, attempt, c.reconcile)
}

func (c *Controller) reconcile(ctx context.Context, ks *manifest.Kustomization, invocation manifest.ReconcileAttempt) error {
	level := invocation.Drain
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
		// stale-spec snapshot captured by DispatchNode, producing
		// duplicate renders that linger in the store with the wrong
		// namespace. See #102.
		fresh, ok, err := c.RequireRefresh[*manifest.Kustomization](
			ctx, id, deps, level)
		if err != nil {
			ref := ks.SourceID()
			_, blocked := errors.AsType[*depwait.ErrBlocked](err)
			_, failed := errors.AsType[*manifest.DependencyFailedError](err)
			if (blocked || failed) && level >= manifest.DrainCascade && ref.Kind == manifest.KindOCIRepository && c.Store.GetObject(ref) == nil && c.Store.GetArtifact(ref) == nil {
				return fmt.Errorf("%w: source %s has an unknown OCI artifact root", manifest.ErrInput, ref)
			}
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
	var source *store.SourceArtifact
	var applyIgnore bool
	var attempt store.Emission
	for {
		var err error
		source, applyIgnore, err = c.resolveSource(ks, level)
		if err != nil {
			return err
		}
		effective, captured := c.Store.GetEmission(id)
		current, _ := effective.(*manifest.Kustomization)
		sourceID := ks.SourceID()
		if sourceID.Name == "" || sourceID.Kind == "" {
			sourceID = manifest.BootstrapSourceID
		}
		if current == ks && c.Store.GetArtifact(sourceID) == source {
			attempt = captured
			break
		}
		captured.Finish()
		if current == nil {
			return nil
		}
		ks = current
	}
	defer attempt.Finish()

	c.SetPendingUnlessReady(id, "expanding substitutions")
	var resolved depwait.Resolution
	if err := c.Require(&depwait.Waiter{Parent: id, Declaration: ks, Supplied: c.substitutionSources, WipeSecrets: c.WipeSecrets}, invocation, &resolved); err != nil {
		return err
	}
	ks, err := kustomize.Prepare(ks, resolved.Inputs, c.substitute)
	if err != nil {
		return err
	}
	// A substituteFrom Secret flate can't read offline (absent, or an
	// ExternalSecret-synthesized empty target) leaves its variables unavailable.
	// References remain unchanged when substitution is skipped; otherwise they
	// use defaults or render empty, which may cause schema/template validation
	// failures. Surface that as a non-fatal
	// advisory so the gap is explained rather than silent; substitution
	// itself stays Flux-faithful. SOPS secrets resolve to placeholders and
	// aren't reported. Render-global and keyed on the bare Secret name (not
	// ns/name): the same logical secret is referenced by the many per-
	// namespace KSes a cluster-config KS re-emits, so the store collapses
	// them to one advisory line per secret name.
	for _, name := range values.UnreadableSubstituteSecrets(ks, resolved.Inputs) {
		c.Store.AddWarning(id, manifest.Warning{
			Category: manifest.WarnUnresolvedSubstitution,
			Message: fmt.Sprintf("substituteFrom Secret %q could not be read offline; "+
				"its variables are unavailable, so their ${VAR} references may remain unsubstituted "+
				"or resolve to defaults or empty values", name),
		})
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	sourceContent := struct{ Path, Key, Revision, Digest string }{source.LocalPath, source.Key, source.Revision, source.Digest}
	fp, handled, err := c.FingerprintDedup(id, ks.Content(sourceContent))
	if handled {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "", "rendering")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	data, err := kustomize.RenderFlux(ctx, c.Trees, kustomize.RenderRequest{
		Source: source, ApplyIgnore: applyIgnore, SubPath: ks.Path, Spec: ks, HeldSlot: c.Tasks,
	}, c.retry)
	if err != nil {
		return err
	}
	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return err
	}
	docs = manifest.FlattenLists(docs)

	// Flux skips substitution on resources marked
	// "kustomize.toolkit.fluxcd.io/substitute: disabled" to preserve shell
	// expansions such as ${ARR[@]} that envsubst cannot parse.
	strategy := (kustomizev1.Kustomization{Spec: ks.KustomizationSpec}).GetSubstituteStrategy()
	if ks.PostBuild != nil && len(ks.PostBuild.Substitute) > 0 || strategy == kustomizev1.SubstituteStrategyAlways {
		var vars map[string]string
		if ks.PostBuild != nil {
			vars = manifest.VarsMap(ks.PostBuild.Substitute)
		}
		for i, doc := range docs {
			if manifest.HasSubstituteDisabled(doc) {
				continue
			}
			substituted, sErr := substituteDoc(doc, vars, c.strictSubstitutions)
			if sErr != nil {
				return fmt.Errorf("%w: Kustomization %s: %w", manifest.ErrInvalidSubstituteReference, id.NamespacedName(), sErr)
			}
			docs[i] = substituted
		}
	}

	c.Store.UpdateStatus(id, store.StatusPending, "", fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.PreflightError(id); err != nil {
		return err
	}
	attempt.Children, attempt.Manifests = base.ParseChildren(id, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets, DefaultNamespace: ks.TargetNamespace})
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, data
	c.Emit(id, &attempt)
	return nil
}

// collectDeps preserves authored ordering, declaration parents and source gates.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := make([]manifest.DependencyRef, 0, len(ks.DependsOn))
	var source manifest.DependencyRef
	for ref := range manifest.References(ks) {
		if ref.Use == "source" {
			source = ref.DependencyRef
		} else if ref.Use == "dependsOn" && c.KeepOrderingDep(ref.NamedResource) {
			deps = append(deps, ref.DependencyRef)
		}
	}
	record, _ := c.index.Lookup(ks.Named())
	start := len(deps)
	for _, p := range c.Store.Productions(ks.Named(), c.index) {
		if p.Path == "" && p.Owner != (manifest.NamedResource{}) && p.Owner != ks.Named() {
			deps = append(deps, manifest.DependencyRef{NamedResource: p.Owner})
		}
	}
	if len(deps) == start && record.Parent != (manifest.NamedResource{}) && record.Parent != ks.Named() {
		deps = append(deps, manifest.DependencyRef{NamedResource: record.Parent})
	}
	if source.Kind != "" && source.Name != "" {
		deps = append(deps, source)
	}
	return deps
}

// resolveSource retains source-controller ignore shaping for working-tree aliases.
// Acquired immutable views already carry their fetcher's policy.
func (c *Controller) resolveSource(ks *manifest.Kustomization, level manifest.DrainLevel) (*store.SourceArtifact, bool, error) {
	srcID := ks.SourceID()
	if srcID.Kind == "" || srcID.Name == "" {
		// Child Kustomizations that inherit sourceRef from a parent's
		// render patches show empty here until that parent reconciles.
		// Fall back to the seeded bootstrap GitRepository (the user's
		// working tree) so the first reconcile resolves to the repo
		// root instead of doubling ks.Path against itself (#105).
		if ks.Path == "" {
			return nil, false, fmt.Errorf("%w: kustomization %s has no path and no source",
				manifest.ErrInput, ks.Named().NamespacedName())
		}
		srcID = manifest.BootstrapSourceID
	}
	art := c.Store.GetArtifact(srcID)
	if art == nil {
		// A source that soft-skipped (--allow-missing-secrets) reports
		// Ready with ReasonSkipped but writes no artifact. Surface
		// that as a typed skip so the caller can mark the KS skipped too
		// rather than reporting a generic "artifact not found" failure.
		if err := c.SourceSkipped(srcID, "source"); err != nil {
			return nil, false, err
		}
		return nil, false, fmt.Errorf("%w: source %s artifact not found", manifest.ErrObjectNotFound, srcID.String())
	}
	if sa, ok := art.(*store.SourceArtifact); ok {
		if sa.FS == nil {
			if level == manifest.DrainNone {
				return nil, false, &depwait.ErrBlocked{Deps: []manifest.NamedResource{srcID}}
			}
			return nil, false, fmt.Errorf("%w: source %s artifact content unavailable", manifest.ErrObjectNotFound, srcID)
		}
		return sa, srcID == manifest.BootstrapSourceID || sa.Digest == "" && sa.Revision == "", nil
	}
	return nil, false, fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}
