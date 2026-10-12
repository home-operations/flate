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

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
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

	// Trees shares secure disk roots, remote fetches and persisted renders.
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
	// Index contains immutable data renderers discovered before reconciliation.
	Index *manifest.DiscoveryIndex
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
func (c *Controller) Configure(opts Options) {
	c.Controller.Configure(opts.Options)
	c.index = opts.Index
	c.substitutionSources = opts.SubstituteFrom
	c.substitute = opts.Substitute
	c.strictSubstitutions = opts.StrictSubstitutions
	c.substitutionProvider = values.NewStoreProvider(c.Store)
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
	return c.DispatchNode(ctx, id, level, base.Dispatch[*manifest.Kustomization]{
		Suspended: func(ks *manifest.Kustomization) bool { return ks.Suspend },
		Reconcile: c.reconcile,
	})
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
			ref := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
			if level >= manifest.DrainCascade && ks.SourceKind == manifest.KindOCIRepository && c.Store.GetObject(ref) == nil && c.Store.GetArtifact(ref) == nil {
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
	source, applyIgnore, err := c.resolveSource(ks)
	if err != nil {
		return err
	}

	sourceRoot := source.LocalPath
	c.SetPendingUnlessReady(id, "expanding substitutions")
	// kustomize.Prepare clones ks and expands postBuild.substituteFrom —
	// the same pre-render dance an embedder calling RenderFlux directly
	// would perform. Keeping one canonical implementation here means
	// changes to the contract only land in one place. Mirrors helm.Prepare.
	provider := c.substitutionProvider
	if provider == nil {
		provider = values.NewStoreProvider(c.Store)
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
		c.Store.AddWarning(manifest.Warning{
			Category: manifest.WarnUnresolvedSubstitution,
			Message: fmt.Sprintf("substituteFrom Secret %q could not be read offline; "+
				"its variables are unavailable, so their ${VAR} references may remain unsubstituted "+
				"or resolve to defaults or empty values", name),
		})
	}
	if err := c.PreflightError(id); err != nil {
		return err
	}

	// Fingerprint dedup (kustomize.RenderFlux is the hot path): skip the
	// re-render and replay the cached artifact's side-effects when the
	// post-Prepare inputs are byte-identical to the cached artifact. The
	// shared helper publishes nothing (publish=false) — see its doc for the
	// churn/quiescence rationale (#657–#660). fp is reused for SetArtifact below.
	fp := kustomizationFingerprint(ks, fingerprintSource(source))
	if handled, err := c.FingerprintDedup(id, fp, func(docs []map[string]any) {
		c.Emit(id, docs, c.WipeSecrets, false)
	}); handled {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering")
	if err := c.PreflightError(id); err != nil {
		return err
	}
	data, err := kustomize.RenderFlux(ctx, c.Trees, kustomize.RenderRequest{Source: source, ApplyIgnore: applyIgnore, SubPath: ks.Path, Spec: ks.Contents})
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
	strategy := (kustomizev1.Kustomization{Spec: ks.KustomizationSpec}).GetSubstituteStrategy()
	if len(ks.PostBuildSubstitute) > 0 || strategy == kustomizev1.SubstituteStrategyAlways {
		vars := manifest.VarsMap(ks.PostBuildSubstitute)
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
	_, docs = c.Emit(id, docs, c.WipeSecrets, true)

	c.Store.SetArtifact(id, &store.KustomizationArtifact{
		Path:        filepath.Join(sourceRoot, ks.Path),
		Manifests:   docs,
		Fingerprint: fp,
	})
	return nil
}

// collectDeps orders explicit dependencies, sources, structural parents and
// substitution data producers before the consumer reads their rendered data.
// Secrets are not hard data edges: SOPS/ExternalSecret inputs may exist only in cluster state.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := c.OrderingDeps(ks.DependsOn)
	if ks.SourceKind != "" && ks.SourceName != "" {
		deps = append(deps, manifest.DependencyRef{
			Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
		})
	}
	if parent, ok := c.LookupParent(ks.Named()); ok {
		deps = append(deps, manifest.DependencyRef{NamedResource: parent})
	}
	for _, ref := range ks.PostBuildSubstituteFrom {
		if !manifest.IsSubstituteDataRef(ref) {
			continue
		}
		depID := manifest.NamedResource{Kind: ref.Kind, Namespace: ks.Namespace, Name: ref.Name}
		if c.substitutionSources[depID] != nil {
			continue
		}
		producers := c.index.DataProducers(depID)
		if slices.Contains(producers, ks.Named()) {
			continue
		}
		if ref.Kind == manifest.KindConfigMap {
			deps = append(deps, manifest.DependencyRef{NamedResource: depID})
		}
		for _, producer := range producers {
			deps = append(deps, manifest.DependencyRef{NamedResource: producer})
		}
	}
	return deps
}

// resolveSource returns the source artifact and whether
// source-controller's default file exclusions must be applied when the tree is
// materialized for the build.
//
// applyIgnore is true exactly for working-tree / self-referential sources:
// the bootstrap GitRepository alias and overrideSelfReferentialGitRepositories,
// which expose the user's raw working tree and never passed through a fetcher's
// artifact filtering. Every real fetcher (git, OCI, bucket) already ran
// source.ApplyIgnore on the LocalPath it publishes and records a Revision or
// Digest, so those are materialized as-is (applyIgnore=false); re-filtering them
// would be redundant and, for buckets, wrong (defaults vs no-defaults). The
// working-tree aliases are the only artifacts that set neither Revision nor
// Digest, which is precisely the signal.
func (c *Controller) resolveSource(ks *manifest.Kustomization) (*store.SourceArtifact, bool, error) {
	srcID := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	if ks.SourceKind == "" || ks.SourceName == "" {
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
		return sa, sa.Digest == "" && sa.Revision == "", nil
	}
	return nil, false, fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}
