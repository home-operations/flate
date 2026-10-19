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
	"slices"

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

	index                *manifest.DiscoveryIndex
	substitutionSources  map[manifest.NamedResource]manifest.BaseManifest
	substitutionProvider values.Provider
	substitute           map[string]string
	strictSubstitutions  bool
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
	c.substitutionProvider = values.NewStoreProvider(c.Store, c.WipeSecrets)
	if len(opts.SubstituteFrom) > 0 {
		c.substitutionProvider = &substitutionProvider{
			sources: opts.SubstituteFrom, fallback: c.substitutionProvider,
		}
	}
}

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for Kustomization nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) []manifest.NamedResource {
	return c.DispatchNode(ctx, id, level, c.reconcile)
}

func (c *Controller) reconcile(ctx context.Context, ks *manifest.Kustomization, level manifest.DrainLevel) error {
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
		// stale-spec snapshot captured by RunWithStatusOutcome, producing
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
	var err error
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
	// kustomize.Prepare clones ks and expands postBuild.substituteFrom —
	// the same pre-render dance an embedder calling RenderFlux directly
	// would perform. Keeping one canonical implementation here means
	// changes to the contract only land in one place. Mirrors helm.Prepare.
	provider := c.substitutionProvider
	if provider == nil {
		provider = values.NewStoreProvider(c.Store, c.WipeSecrets)
	}
	ks, err = kustomize.Prepare(ks, provider, c.substitute)
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
	for _, name := range values.UnreadableSubstituteSecrets(ks, provider) {
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

	c.Store.UpdateStatus(id, store.StatusPending, "rendering")
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

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	if err := c.PreflightError(id); err != nil {
		return err
	}
	attempt.Children, attempt.Manifests = base.ParseChildren(id, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets, DefaultNamespace: ks.TargetNamespace})
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, data
	c.Emit(id, &attempt)
	return nil
}

// collectDeps preserves dependsOn, parent, source, then data/producer gate order.
// Secrets have no direct hard gate: SOPS/ExternalSecret data may exist only in-cluster.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	var deps []manifest.DependencyRef
	if len(ks.DependsOn) > 0 {
		deps = make([]manifest.DependencyRef, 0, len(ks.DependsOn))
	}
	var source manifest.DependencyRef
	flushed := false
	flush := func() {
		record, _ := c.index.Lookup(ks.Named())
		start := len(deps)
		c.Store.Productions(ks.Named(), c.index)(func(p manifest.Production) bool {
			if p.Path != "" || p.Owner == (manifest.NamedResource{}) || p.Owner == ks.Named() {
				return true
			}
			deps = append(deps, manifest.DependencyRef{NamedResource: p.Owner})
			return true
		})
		if len(deps) == start && record.Parent != (manifest.NamedResource{}) && record.Parent != ks.Named() {
			deps = append(deps, manifest.DependencyRef{NamedResource: record.Parent})
		}
		if source.Kind != "" && source.Name != "" {
			deps = append(deps, source)
		}
		flushed = true
	}
	manifest.References(ks)(func(ref manifest.Reference) bool {
		switch ref.Use {
		case "source":
			source = ref.DependencyRef
		case "dependsOn":
			if c.KeepOrderingDep(ref.NamedResource) {
				deps = append(deps, ref.DependencyRef)
			}
		case "substituteFrom":
			if !flushed {
				flush()
			}
			if ref.Optional || ref.Name == "" || c.substitutionSources[ref.NamedResource] != nil {
				return true
			}
			var producers []manifest.NamedResource
			for p := range c.Store.Productions(ref.NamedResource, c.index) {
				if p.Declaration != (manifest.NamedResource{}) && !slices.Contains(producers, p.Declaration) {
					producers = append(producers, p.Declaration)
				}
				if p.Owner != (manifest.NamedResource{}) {
					if !slices.Contains(producers, p.Owner) {
						producers = append(producers, p.Owner)
					}
				}
			}
			if slices.Contains(producers, ks.Named()) {
				return true
			}
			if ref.Kind == manifest.KindConfigMap {
				deps = append(deps, ref.DependencyRef)
			}
			for _, producer := range producers {
				deps = append(deps, manifest.DependencyRef{NamedResource: producer})
			}
		}
		return true
	})
	if !flushed {
		flush()
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
		// Ready with a "skipped:" reason but writes no artifact. Surface
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
