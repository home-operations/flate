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

type execution struct {
	*Controller
	run        base.Execution
	data       *values.StoreProvider
	unreadable []manifest.NamedResource
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.data = values.NewStoreProvider(&execution.run.Reads, c.WipeSecrets, c.substitutionSources)
	return execution
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

// ReconcileNode runs id's reconcile under the dag engine, returning the blocked
// dependency set (nil = terminalized). The
// orchestrator's scheduler Dispatcher calls this for Kustomization nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	execution := c.newExecution(id)
	execution.run.Dispatch(ctx, level, execution.reconcile)
	return &execution.run
}

func (c *execution) reconcile(ctx context.Context, ks *manifest.Kustomization, level manifest.DrainLevel) error {
	id := ks.Named()
	if err := c.run.PreflightError(id); err != nil {
		return err
	}
	c.SetPendingUnlessReady(id, "resolving dependencies")

	deps := c.collectDeps(ks)
	if len(deps) > 0 {
		err := c.run.Require(ctx, deps, level)
		if err != nil {
			ref := ks.SourceID()
			_, blocked := errors.AsType[*depwait.ErrBlocked](err)
			_, failed := errors.AsType[*manifest.DependencyFailedError](err)
			if (blocked || failed) && level >= manifest.DrainCascade && ref.Kind == manifest.KindOCIRepository && c.run.Reads.GetObject(ref) == nil && c.run.Reads.GetArtifact(ref) == nil {
				return fmt.Errorf("%w: source %s has an unknown OCI artifact root", manifest.ErrInput, ref)
			}
			return err
		}
	}
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

	c.SetPendingUnlessReady(id, "resolving source artifact")
	source, applyIgnore, err := c.resolveSource(ks, level)
	if err != nil {
		return err
	}
	c.SetPendingUnlessReady(id, "expanding substitutions")
	var inputs []depwait.DataDependency
	for ref := range manifest.References(ks) {
		if ref.Use != "substituteFrom" || ref.Name == "" || !ref.NamedResource.KindInfo().Data {
			continue
		}
		if c.substitutionSources[ref.NamedResource] == nil {
			c.run.PromoteData(ctx, ref.NamedResource)
		}
		input := c.data.Lookup(ref.NamedResource)
		inputs = append(inputs, depwait.DataDependency{DependencyRef: ref.DependencyRef, Input: input, Optional: ref.Optional, AllowMissing: ref.Kind == manifest.KindSecret})
		unreadable := input.Data == nil || !input.Data.HasKeys()
		if ref.Kind == manifest.KindSecret && !ref.Optional && unreadable {
			c.unreadable = append(c.unreadable, ref.NamedResource)
			if c.substitutionSources[ref.NamedResource] == nil {
				c.run.Unreadable = append(c.run.Unreadable, ref.NamedResource)
			}
		}
	}
	return c.run.AwaitData(ctx, base.DataWait{Inputs: inputs, Resume: func(ctx context.Context, _ manifest.DrainLevel, _ []manifest.NamedResource) error {
		return c.render(ctx, ks, source, applyIgnore)
	}}, level)
}

func (c *execution) render(ctx context.Context, ks *manifest.Kustomization, source *store.SourceArtifact, applyIgnore bool) error {
	id := ks.Named()
	ks, err := kustomize.Prepare(ks, c.data, c.substitute)
	if err != nil {
		return err
	}
	for _, ref := range c.unreadable {
		c.run.Reads.AddWarning(id, manifest.Warning{
			Category: manifest.WarnUnresolvedSubstitution,
			Message: fmt.Sprintf("substituteFrom Secret %q could not be read offline; "+
				"its variables are unavailable, so their ${VAR} references may remain unsubstituted "+
				"or resolve to defaults or empty values", ref.Name),
		})
	}
	if err := c.run.PreflightError(id); err != nil {
		return err
	}

	sourceContent := struct{ Path, Key, Revision, Digest string }{source.LocalPath, source.Key, source.Revision, source.Digest}
	fp, handled, err := c.run.FingerprintDedup(id, ks.Content(sourceContent))
	if handled {
		return err
	}

	_, attempt := c.run.Reads.GetEmission(id)
	defer attempt.Finish()
	c.Store.UpdateStatus(id, store.StatusPending, "", "rendering")
	if err := c.run.PreflightError(id); err != nil {
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
	if err := c.run.PreflightError(id); err != nil {
		return err
	}
	attempt.Children, attempt.Manifests = base.ParseChildren(id, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets, DefaultNamespace: ks.TargetNamespace})
	attempt.Fingerprint, attempt.Complete, attempt.Render = fp, true, data
	c.run.Emit(id, &attempt)
	return nil
}

func (c *execution) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	var deps []manifest.DependencyRef
	if len(ks.DependsOn) != 0 {
		deps = make([]manifest.DependencyRef, 0, len(ks.DependsOn))
	}
	var source manifest.DependencyRef
	for ref := range manifest.References(ks) {
		if ref.Use == "dependsOn" && c.run.KeepOrderingDep(ref.NamedResource) {
			deps = append(deps, ref.DependencyRef)
		}
		if ref.Use == "source" {
			source = ref.DependencyRef
		}
	}
	record, _ := c.index.Lookup(ks.Named())
	start := len(deps)
	for _, p := range c.run.Reads.Productions(ks.Named(), c.index) {
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
func (c *execution) resolveSource(ks *manifest.Kustomization, level manifest.DrainLevel) (*store.SourceArtifact, bool, error) {
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
	art := c.run.Reads.GetArtifact(srcID)
	if art == nil {
		// A source that soft-skipped (--allow-missing-secrets) reports
		// Ready with ReasonSkipped but writes no artifact. Surface
		// that as a typed skip so the caller can mark the KS skipped too
		// rather than reporting a generic "artifact not found" failure.
		if err := c.run.SourceSkipped(srcID, "source"); err != nil {
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
