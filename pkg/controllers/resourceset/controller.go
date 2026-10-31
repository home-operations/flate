// Package resourceset reconciles flux-operator ResourceSets as
// first-class DAG nodes: wait on dependsOn / inputsFrom RSIPs / the
// structural parent KS, render the RS via the resourceset package, and
// emit every child through the shared emission transaction.
package resourceset

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller orchestrates ResourceSet reconciliation. Reconcile-shaping
// state (Filter, Index, Existence) flows in via Configure exactly once before
// StartLifecycle — the same "config is read-only after StartLifecycle" invariant the KS
// and HR controllers carry, encoded in the embedded *base.Controller.
type Controller struct {
	*base.Controller

	// WipeSecrets controls whether Secret cleartext is wiped when parsing
	// emitted children (forwarded to base.Execution.Emit).
	WipeSecrets bool
	// Inputs fetches registry-backed provider content through the shared OCI owner.
	Inputs *oci.Fetcher
	// Retry controls transient failures during provider acquisition.
	Retry cache.RetryConfig
	// AllowMissingSecrets applies the source credential policy to provider inputs.
	AllowMissingSecrets bool
}

type execution struct {
	*Controller
	run    base.Execution
	inputs []resourceset.ProviderInputs
	next   int
}

func (c *Controller) newExecution(id manifest.NamedResource) *execution {
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	return execution
}

// New constructs a ResourceSet controller.
func New(s *store.Store, t *task.Service, wipeSecrets bool) *Controller {
	return &Controller{
		Controller:  base.New(s, t),
		WipeSecrets: wipeSecrets,
	}
}

// Owns reports whether id belongs to the ResourceSet controller.
func (*Controller) Owns(id manifest.NamedResource) bool {
	return id.Group == manifest.FluxOperatorDomain && id.Kind == manifest.KindResourceSet
}

// ReconcileNode captures one ResourceSet execution for the scheduler.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	ex := c.newExecution(id)
	ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
	return &ex.run
}

func (c *execution) reconcile(ctx context.Context, rs *manifest.ResourceSet, level manifest.DrainLevel) error {
	var err error
	rs, err = c.run.RequireRefresh(ctx, rs, manifest.GateInputs, level)
	if err != nil {
		return err
	}

	c.inputs, err = resourceset.CollectProviders(rs, resourceset.StoreResolver(&c.run.Reads))
	if err != nil {
		return err
	}
	return c.collect(ctx, rs, level)
}

func (c *execution) collect(ctx context.Context, rs *manifest.ResourceSet, level manifest.DrainLevel) error {
	for c.next < len(c.inputs) {
		input := &c.inputs[c.next]
		provider := input.Declaration
		plan, err := provider.InputPlan()
		if err != nil {
			return manifest.NewError(manifest.ErrInput, fmt.Errorf("ResourceSetInputProvider %s: %w", provider.Named().NamespacedName(), err), "")
		}
		input.Inputs = plan.Inputs
		switch plan.Kind {
		case manifest.ProviderInputUnsupported:
			input.State = resourceset.ProviderInputUnsupported
		case manifest.ProviderInputLiteral:
		case manifest.ProviderInputRegistry:
			if c.Inputs == nil {
				return manifest.NewError(manifest.ErrInput, nil, "registry input fetcher is not configured")
			}
			return c.run.Acquire(ctx, provider, base.Acquisition{
				AllowMissingSecrets:  c.AllowMissingSecrets,
				AllowAnonymousDenied: true,
				Fetch: func(ctx context.Context) (*store.SourceArtifact, error) {
					return c.Inputs.FetchInputs(ctx, provider, &c.run.Reads, cache.Policy{Retry: c.Retry})
				},
				Complete: func(ctx context.Context, artifact *store.SourceArtifact, unavailable error, level manifest.DrainLevel) error {
					if !c.run.Reads.Valid() {
						return nil
					}
					if unavailable != nil {
						input.State = resourceset.ProviderInputUnavailable
					} else {
						body, err := fs.ReadFile(artifact.FS, "inputs.json")
						if err != nil {
							return manifest.NewError(manifest.ErrFlux, fmt.Errorf("read provider inputs: %w", err), "")
						}
						if err := json.Unmarshal(body, &input.Inputs); err != nil {
							return manifest.NewError(manifest.ErrFlux, fmt.Errorf("decode provider inputs: %w", err), "")
						}
					}
					c.next++
					return c.collect(ctx, rs, level)
				},
			}, level)
		}
		c.next++
	}
	return c.render(ctx, rs)
}

func (c *execution) render(ctx context.Context, rs *manifest.ResourceSet) error {
	id := rs.Named()
	var attempt store.Emission
	defer attempt.Finish()
	handled := false
	docs, warnings, err := resourceset.Render(rs, c.inputs, func(content any) (bool, error) {
		fp, hit, err := c.run.FingerprintDedup(ctx, id, content, &attempt)
		handled = hit
		if hit || err != nil {
			return hit, err
		}
		_, _, attempt = c.run.Reads.GetEmission(id)
		attempt.Fingerprint = fp
		return false, nil
	})
	for _, warning := range warnings {
		c.run.Reads.AddWarning(warning)
	}
	if err != nil {
		return err
	}
	if handled {
		return nil
	}
	rs.EmissionPolicy().Stamp(docs)
	attempt.Children, attempt.Manifests, attempt.Failures = c.ParseChildren(rs, docs, manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets})
	attempt.Complete = true
	return c.run.Emit(ctx, id, &attempt, nil)
}
