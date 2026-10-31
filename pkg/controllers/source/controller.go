// Package source reconciles Flux source CRs (GitRepository,
// OCIRepository, Bucket, ExternalArtifact, HelmRepository) into on-disk
// artifacts via per-kind Fetcher implementations from pkg/source, then
// publishes the result to the Store. Mirrors
// pkg/controllers/{kustomization,helmrelease}.
//
// The controller does not know about individual source kinds — it
// dispatches via the Fetchers map keyed by id.Kind, so adding a new
// kind is a one-line registration at orchestrator-construction time.
package source

import (
	"context"
	"log/slog"
	"time"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type sourceFacts interface {
	ExistenceOnly() bool
	SourceInterval() time.Duration
}

// Controller watches the Store for source-kind objects, fetches each
// into an on-disk artifact via the matching Fetcher, and updates the
// Store with the result.
type Controller struct {
	*base.Controller
	retry cache.RetryConfig

	// Fetchers maps source kinds to their fetchers. Kinds without an entry
	// are ignored. Register fetchers before StartLifecycle.
	Fetchers map[string]src.Fetcher

	// allowMissingSecrets converts ErrMissingSecret fetch errors into a
	// skip (StatusReady with ReasonSkipped). Wired from
	// FetchOptions.AllowMissingSecrets via Configure.
	allowMissingSecrets bool
	admission           *discovery.Admission
}

// FetchOptions carries the post-bootstrap state the orchestrator wires
// onto the controller. Filter narrows fetches to sources referenced by
// changed resources in changed-only mode.
type FetchOptions struct {
	// Loader borrows the invocation's declaration admission boundary.
	Loader              *loader.Loader
	Filter              *change.Filter
	AllowMissingSecrets bool
	// Admission resolves runtime sources against immutable discovery policy.
	Admission *discovery.Admission
}

type execution struct {
	*Controller
	run      base.Execution
	acquired *store.SourceArtifact
}

// New constructs a source controller. Register fetchers on the
// returned struct's Fetchers map before StartLifecycle.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{
		Controller: base.New(s, t),
		Fetchers:   map[string]src.Fetcher{},
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// StartLifecycle.
func (c *Controller) Configure(opts FetchOptions, retry cache.RetryConfig) {
	common := base.Options{Filter: opts.Filter, Loader: opts.Loader}
	if opts.Loader != nil {
		common.Index = opts.Loader.Index
	}
	c.Controller.Configure(common)
	c.retry = retry
	c.allowMissingSecrets = opts.AllowMissingSecrets
	c.admission = opts.Admission
}

// Owns reports whether id is a node this controller reconciles — the dag
// Dispatcher uses it to route source nodes to ReconcileNode. A registered
// kind is not enough: kind names are not unique across API groups, and a
// foreign CRD reusing one (seaweed.seaweedfs.com Bucket) parses to a
// RawObject that the kind's Fetcher would reject as an unexpected payload.
// An id absent from the store still counts as ours — reporting the
// vanished object is the controller's job.
func (c *Controller) Owns(id manifest.NamedResource) bool {
	if _, ok := c.Fetchers[id.Kind]; !ok {
		return false
	}
	obj := c.Store.GetObject(id)
	return obj == nil && id.Group == manifest.SourceDomain || manifest.KindOf(obj).Runnable
}

// ReconcileNode returns the captured assessment of a source acquisition.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) *base.Execution {
	ex := &execution{Controller: c}
	ex.run.Init(c.Controller, id)
	for {
		ex.run.Dispatch(ctx, level, ex, (*execution).reconcile)
		if ex.acquired == nil || ex.run.Reads.Valid() {
			return &ex.run
		}
		next := &execution{Controller: c, acquired: ex.acquired}
		next.run.Init(c.Controller, id)
		if !next.run.Reads.Matches(&ex.run.Reads) {
			next.run.Reads.Retire()
			return &ex.run
		}
		next.run.Reads.CarryAdvisories(&ex.run.Reads)
		ex.run.Retire()
		ex = next
	}
}

func (c *execution) reconcile(ctx context.Context, obj manifest.BaseManifest, level manifest.DrainLevel) error {
	id := obj.Named()
	facts := obj.(sourceFacts)
	if facts.ExistenceOnly() {
		return nil
	}
	// Dispatcher ownership guarantees a fetcher for this kind.
	fetcher := c.Fetchers[id.Kind]
	// Available content survives idempotent re-emission. Released pins must
	// reacquire normally and publish a Ready transition to wake content consumers.
	if art, ok := c.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact); ok {
		if art.FS != nil && art.Reads.Valid() {
			c.run.Reads.CarryAdvisories(art.Reads)
			for _, warning := range art.Warnings {
				c.run.Reads.AddWarning(warning)
			}
			c.run.Reused = true
			return nil
		}
		if art.FS != nil && c.run.Reads.Matches(art.Reads) {
			c.run.Reads.CarryAdvisories(art.Reads)
			c.Store.SetArtifact(id, art.WithReads(&c.run.Reads))
			for _, warning := range art.Warnings {
				c.run.Reads.AddWarning(warning)
			}
			c.run.Reused = true
			return nil
		}
	}
	var err error
	obj, err = c.run.RequireRefresh(ctx, obj, manifest.GateInputs, level)
	if err != nil {
		return err
	}

	if c.admission != nil {
		_, artifact, err := c.admission.Resolve(ctx, obj.Named(), obj, nil)
		if err != nil {
			return err
		}
		if artifact != nil {
			return c.completeFetch(obj, artifact, nil)
		}
	}

	if level == manifest.DrainNone {
		// A producer may need this source's content; drain releases availability waits.
		if blocked := c.run.Reads.UnsettledProducers(id); len(blocked) != 0 {
			return &depwait.ErrBlocked{Deps: blocked}
		}
	}

	if c.acquired != nil {
		artifact := c.acquired
		c.acquired = nil
		return c.completeFetch(obj, artifact, nil)
	}

	c.SetPendingUnlessReady(id, "fetching")

	return c.run.Acquire(ctx, obj, base.Acquisition{
		AllowMissingSecrets: c.allowMissingSecrets,
		Fetch: func(ctx context.Context) (*store.SourceArtifact, error) {
			return fetcher.Fetch(ctx, obj, &c.run.Reads, cache.Policy{MaxAge: facts.SourceInterval(), Retry: c.retry})
		},
		Complete: func(_ context.Context, artifact *store.SourceArtifact, unavailable error, _ manifest.DrainLevel) error {
			return c.completeFetch(obj, artifact, unavailable)
		},
	}, level)
}

func (c *execution) completeFetch(obj manifest.BaseManifest, artifact *store.SourceArtifact, err error) error {
	if artifact != nil {
		for _, warning := range artifact.Warnings {
			c.run.Reads.AddWarning(warning)
		}
	}
	if err != nil {
		return manifest.NewError(manifest.ErrSourceSkipped, err, "")
	}
	if artifact != nil {
		if !c.run.Reads.Valid() {
			c.acquired = artifact
			return nil
		}
		c.Store.SetArtifact(obj.Named(), artifact.WithReads(&c.run.Reads))
	}
	slog.Debug("source: fetch complete", "id", manifest.DiagnosticFor(obj.Named(), obj).String())
	return nil
}
