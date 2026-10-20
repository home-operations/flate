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
	// skip (StatusReady with reason starting "skipped: "). Wired from
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

// ReconcileNode runs id's source fetch under the dag engine, returning the
// blocked Secret dependencies, or nil when the fetch terminalizes. The
// orchestrator's scheduler Dispatcher calls this for source-kind nodes.
func (c *Controller) ReconcileNode(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel) []manifest.NamedResource {
	return c.DispatchNode(ctx, id, level, c.reconcile)
}

// reconcile fetches the source artifact via the registered Fetcher and
// writes the result through base.RunWithStatusOutcome so panic recovery,
// final status writes, and ErrSourceSkipped routing match the
// KS/HR controllers' shape. Returning ErrSourceSkipped yields a
// Ready+"skipped: ..." status; ErrBlocked parks the fetch for a dependency
// retry; any other error yields Failed.
func (c *Controller) reconcile(ctx context.Context, obj manifest.BaseManifest, level manifest.DrainLevel) error {
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
		if art.FS != nil {
			return nil
		}
		c.Store.UpdateStatus(id, store.StatusPending, "acquiring source content")
	}

	c.SetPendingUnlessReady(id, "fetching")
	started := time.Now()

	// Release the worker slot during network I/O so other nodes can progress.
	var artifact *store.SourceArtifact
	var fetchErr error
	c.Tasks.YieldSlot(func() {
		artifact, fetchErr = fetcher.Fetch(ctx, obj, cache.Policy{MaxAge: facts.SourceInterval(), Retry: c.retry})
	})
	// A Kustomization may still emit or patch this Secret. Wait for the
	// rendered contents; only fall back, fail or skip for a missing Secret
	// once the scheduler reaches a fixpoint. The wait MUST precede the
	// fallback so an in-repo Secret takes precedence over the global
	// registry config.
	missing, isMissing := errors.AsType[*src.MissingSecretError](fetchErr)
	if isMissing && level == manifest.DrainNone {
		c.Store.UpdateStatus(id, store.StatusPending, "waiting for source Secret")
		return &depwait.ErrBlocked{Deps: []manifest.NamedResource{missing.Secret}}
	}
	if isMissing && missing.RetryWithRegistryConfig != nil && !c.producerBacked(fetchErr) {
		c.Tasks.YieldSlot(func() {
			artifact, fetchErr = missing.RetryWithRegistryConfig(ctx, missing.RegistryConfig)
		})
		// An optional registry retry MUST preserve --allow-missing-secrets skips.
		if fetchErr != nil && c.allowMissingSecrets {
			fetchErr = missing
		}
	}
	if fetchErr != nil {
		c.Logger().Debug("fetch failed", "id", id.String(), "duration", time.Since(started), "err", fetchErr)
		// Skip a missing auth Secret either when the user asked globally
		// (--allow-missing-secrets) or when an in-repo ExternalSecret /
		// SealedSecret declares it (producer-backed: positive evidence it
		// materializes live). A missing Secret with neither still fails loud.
		if errors.Is(fetchErr, manifest.ErrMissingSecret) && (c.allowMissingSecrets || c.producerBacked(fetchErr)) {
			c.Logger().Info("skipped (missing secret)", "id", id.String(), "err", fetchErr)
			return fmt.Errorf("%w: %s", manifest.ErrSourceSkipped, manifest.TrimSentinelPrefix(fetchErr.Error()))
		}
		return fetchErr
	}
	c.Logger().Debug("fetch complete", "id", id.String(), "duration", time.Since(started), "artifact", artifact != nil)
	if artifact != nil {
		c.Store.SetArtifact(id, artifact)
	}
	return nil
}

// producerBacked reports whether err is a missing-auth-Secret error whose
// Secret is declared by an in-repo producer (ExternalSecret / SealedSecret) in
// the shared index — in which case the source is skipped without the global
// --allow-missing-secrets flag. The structured *src.MissingSecretError carries
// the Secret identity; a missing-secret error that isn't that type (or whose
// Secret has no producer) is not producer-backed.
func (c *Controller) producerBacked(err error) bool {
	mse, ok := errors.AsType[*src.MissingSecretError](err)
	if !ok {
		return false
	}
	for p := range c.Store.Productions(mse.Secret, nil) {
		if p.Declaration != (manifest.NamedResource{}) {
			return true
		}
	}
	return false
}
