// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish an Emission. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"fmt"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/controllers/base"
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

	substitute          map[string]string
	strictSubstitutions bool
}

// Options carries the post-bootstrap state the orchestrator wires onto the
// controller before StartLifecycle. base.Options holds the common reconcile config.
type Options struct {
	base.Options
	// Substitute layers over referenced and inline postBuild values.
	Substitute map[string]string
	// StrictSubstitutions rejects undefined variables without defaults.
	StrictSubstitutions bool
}

type execution struct {
	*Controller
	run  base.Execution
	data *values.StoreProvider
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.data = values.NewStoreProvider(&execution.run.Reads, c.WipeSecrets, c.SubstituteFrom)
	return execution
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
// StartLifecycle — encodes the invariant that reconcile-shaping config is
// read-only once the controller is dispatching.
func (c *Controller) Configure(opts Options, retry cache.RetryConfig) {
	c.Controller.Configure(opts.Options)
	c.retry = retry
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
	var err error
	ks, err = c.run.RequireRefresh(ctx, ks, manifest.GateInputs, level)
	if err != nil {
		return err
	}
	source, applyIgnore, err := c.resolveSource(ks)
	if err != nil {
		return err
	}
	inputs := c.run.DataInputs(ctx, ks, c.data)
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
	sourceContent := struct{ Path, Key, Revision, Digest string }{source.LocalPath, source.Key, source.Revision, source.Digest}
	fp, handled, err := c.run.FingerprintDedup(id, ks.Content(sourceContent))
	if handled {
		return err
	}

	_, attempt := c.run.Reads.GetEmission(id)
	defer attempt.Finish()
	docs, err := kustomize.Render(ctx, c.Trees, kustomize.RenderRequest{
		Source: source, ApplyIgnore: applyIgnore, SubPath: ks.Path, Spec: ks, HeldSlot: c.Tasks,
	}, c.retry)
	if err != nil {
		return err
	}
	docs = manifest.FlattenLists(docs)
	base.Stamp(ks.EmissionPolicy(), docs)

	// Flux skips substitution on resources marked
	// "kustomize.toolkit.fluxcd.io/substitute: disabled" to preserve shell
	// expansions such as ${ARR[@]} that envsubst cannot parse.
	strategy := ks.SubstituteStrategy()
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

	attempt.Children, attempt.Manifests = c.ParseChildren(ks, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	attempt.Fingerprint, attempt.Complete = fp, true
	return c.run.Emit(id, &attempt)
}

// resolveSource retains source-controller ignore shaping for working-tree aliases.
// Acquired immutable views already carry their fetcher's policy.
func (c *execution) resolveSource(ks *manifest.Kustomization) (*store.SourceArtifact, bool, error) {
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
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa, srcID == manifest.BootstrapSourceID || sa.Digest == "" && sa.Revision == "", nil
	}
	return nil, false, fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}
