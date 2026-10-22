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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
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

	// Fetchers maps source CR kind → Fetcher implementation. Source
	// kinds with no entry are ignored, which is also how a flate caller
	// disables a kind (omitting it from the map, e.g. via
	// Orchestrator.WithFetcher(kind, nil)). Exposed for Orchestrator.WithFetcher.
	Fetchers map[string]src.Fetcher

	// allowMissingSecrets converts ErrMissingSecret fetch errors into a
	// skip (StatusReady with ReasonSkipped). Wired from
	// Config.AllowMissingSecrets via Configure.
	allowMissingSecrets bool
}

// FetchOptions carries the post-bootstrap state the orchestrator wires
// onto the controller. Filter narrows fetches to sources referenced by
// changed resources in changed-only mode.
type FetchOptions struct {
	Filter              *change.Filter
	AllowMissingSecrets bool
}

type execution struct {
	*Controller
	run base.Execution
}

// New constructs a source controller. Register fetchers on the
// returned struct's Fetchers map before StartLifecycle.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{
		Controller: base.New(s, t, "source"),
		Fetchers:   map[string]src.Fetcher{},
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// StartLifecycle.
func (c *Controller) Configure(opts FetchOptions, retry cache.RetryConfig) {
	c.Controller.Configure(base.Options{Filter: opts.Filter})
	c.retry = retry
	c.allowMissingSecrets = opts.AllowMissingSecrets
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
	execution := &execution{Controller: c}
	execution.run.Init(c.Controller, id)
	execution.run.Dispatch(ctx, level, execution.reconcile)
	return &execution.run
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
	if art, ok := c.Store.GetArtifact(id).(*store.SourceArtifact); ok {
		if art.FS != nil && art.Reads.Valid() {
			for _, warning := range art.Warnings {
				c.run.Reads.AddWarning(c.run.Reads.ID(), warning)
			}
			c.run.Reused = true
			return nil
		}
		if art.FS != nil && c.run.Reads.Matches(art.Reads) {
			captured := *art
			captured.Reads = &c.run.Reads
			c.Store.SetArtifact(id, &captured)
			for _, warning := range art.Warnings {
				c.run.Reads.AddWarning(c.run.Reads.ID(), warning)
			}
			c.run.Reused = true
			return nil
		}
	}
	if level == manifest.DrainNone {
		// A producer may need this source's content; drain releases availability waits.
		if blocked := c.run.Reads.UnsettledProducers(id); len(blocked) != 0 {
			return &depwait.ErrBlocked{Deps: blocked}
		}
	}

	c.SetPendingUnlessReady(id, "fetching")

	// Release the worker slot during network I/O so other nodes can progress.
	var artifact *store.SourceArtifact
	var fetchErr error
	c.Tasks.YieldSlot(func() {
		artifact, fetchErr = fetcher.Fetch(ctx, obj, &c.run.Reads, cache.Policy{MaxAge: facts.SourceInterval(), Retry: c.retry})
	})
	// A Kustomization may still emit or patch this Secret. Wait for the
	// rendered contents; only fall back, fail or skip for a missing Secret
	// once the scheduler reaches a fixpoint. The wait MUST precede the
	// fallback so an in-repo Secret takes precedence over the global
	// registry config.
	if missing, ok := errors.AsType[*src.MissingSecretError](fetchErr); ok && level == manifest.DrainNone {
		c.Store.UpdateStatus(id, store.StatusPending, "", "waiting for source Secret")
		return &pendingSource{execution: c, object: obj, missing: missing}
	}
	return c.completeFetch(obj, artifact, fetchErr)
}

type pendingSource struct {
	execution *execution
	object    manifest.BaseManifest
	missing   *src.MissingSecretError
}

func (p *pendingSource) Error() string { return p.missing.Error() }
func (p *pendingSource) Unwrap() error {
	return &depwait.ErrBlocked{Deps: []manifest.NamedResource{p.missing.Secret}}
}
func (p *pendingSource) Settle(ctx context.Context, _ manifest.DrainLevel) error {
	c := p.execution
	var artifact *store.SourceArtifact
	var err error = p.missing
	if p.missing.RetryWithRegistryConfig != nil && !c.producerBacked(err) {
		c.Tasks.YieldSlot(func() { artifact, err = p.missing.RetryWithRegistryConfig(ctx, p.missing.RegistryConfig) })
		if err != nil && c.allowMissingSecrets {
			err = p.missing
		}
	}
	return c.completeFetch(p.object, artifact, err)
}

func (c *execution) completeFetch(obj manifest.BaseManifest, artifact *store.SourceArtifact, err error) error {
	if artifact != nil {
		for _, warning := range artifact.Warnings {
			c.run.Reads.AddWarning(c.run.Reads.ID(), warning)
		}
	}
	if err != nil {
		if errors.Is(err, manifest.ErrMissingSecret) && (c.allowMissingSecrets || c.producerBacked(err)) {
			if c.run.Reads.Valid() {
				slog.Info("skipped (missing secret)", "id", obj.Named().String(), "err", err)
			}
			return fmt.Errorf("%w: %s", manifest.ErrSourceSkipped, manifest.TrimSentinelPrefix(err.Error()))
		}
		return err
	}
	if artifact != nil {
		if !c.run.Reads.Valid() {
			return nil
		}
		current, _ := c.Store.GetArtifact(obj.Named()).(*store.SourceArtifact)
		captured := *artifact
		captured.Reads = &c.run.Reads
		c.Store.SetArtifact(obj.Named(), &captured)
		if current != nil && current.Reads != captured.Reads {
			current.Reads.Retire()
		}
	}
	slog.Debug("fetch complete", "id", obj.Named().String())
	return nil
}

// producerBacked reports whether err is a missing-auth-Secret error whose
// Secret is declared by an in-repo producer (ExternalSecret / SealedSecret) in
// the shared index — in which case the source is skipped without the global
// --allow-missing-secrets flag. The structured *src.MissingSecretError carries
// the Secret identity; a missing-secret error that isn't that type (or whose
// Secret has no producer) is not producer-backed.
func (c *execution) producerBacked(err error) bool {
	mse, ok := errors.AsType[*src.MissingSecretError](err)
	if !ok {
		return false
	}
	for _, p := range c.run.Reads.Productions(mse.Secret, nil) {
		if p.Declaration != (manifest.NamedResource{}) {
			return true
		}
	}
	return false
}
