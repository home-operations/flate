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

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Controller watches the Store for source-kind objects, fetches each
// into an on-disk artifact via the matching Fetcher, and updates the
// Store with the result.
type Controller struct {
	*base.Controller

	// Fetchers maps source CR kind → Fetcher implementation. Source
	// kinds with no entry are ignored, which is also how a flate caller
	// disables a kind (e.g. EnableOCI=false simply omits OCIRepository
	// from the map). Exposed for Orchestrator.WithFetcher.
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
	Filter             *change.Filter
	AllowMissingSecrets bool
}

// New constructs a source controller. Register fetchers on the
// returned struct's Fetchers map before Start.
func New(s *store.Store, t *task.Service) *Controller {
	return &Controller{
		Controller: base.New(s, t),
		Fetchers:   map[string]src.Fetcher{},
	}
}

// Configure installs the post-bootstrap state. Panics if called after
// Start.
func (c *Controller) Configure(opts FetchOptions) {
	c.SetFilter(opts.Filter)
	c.allowMissingSecrets = opts.AllowMissingSecrets
}

// Start registers listeners on the Store. The controller runs until
// Close is called.
func (c *Controller) Start(ctx context.Context) {
	c.StartLifecycle("source")
	c.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx))
}

func (c *Controller) onObjectAdded(ctx context.Context) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		if _, registered := c.Fetchers[id.Kind]; !registered {
			return
		}
		obj, ok := payload.(manifest.BaseManifest)
		if !ok {
			return
		}
		sus, _ := obj.(src.Suspendable)
		if c.PreGate(id, sus != nil && sus.Suspended()) {
			return
		}
		// Coalesce per-id so a duplicate AddObject for the same source
		// (e.g. a parent KS re-emits a child source on re-render)
		// doesn't race two concurrent fetches into the same cache slot.
		c.Submit(ctx, id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "source", c.reconcile)
		})
	}
}

// reconcile fetches the source artifact via the registered Fetcher and
// writes the result through base.RunWithStatus so panic recovery,
// final status writes, and ErrSourceSkipped routing match the
// KS/HR controllers' shape. Returning ErrSourceSkipped yields a
// Ready+"skipped: ..." status; any other error yields Failed.
func (c *Controller) reconcile(ctx context.Context, obj manifest.BaseManifest) error {
	id := obj.Named()
	fetcher, registered := c.Fetchers[id.Kind]
	if !registered {
		return nil
	}
	// Existence short-circuit: a source we already fetched in this run
	// stays fetched. Idempotent re-AddObject (e.g. a parent KS
	// re-emitting a stamped copy) returns without touching the network.
	if c.Store.GetArtifact(id) != nil {
		return nil
	}

	c.Store.UpdateStatus(id, store.StatusPending, "fetching")
	slog.Debug("source: fetch", "id", id.String())

	// Release the worker slot during the fetch so consumers of this
	// source (KS/HR depwait watchers) can acquire one and make
	// progress instead of blocking on a fetcher that's itself blocked
	// on network I/O. Mirrors KS/HR's pattern of yielding around
	// depwait calls.
	var artifact *store.SourceArtifact
	var fetchErr error
	c.Tasks.YieldSlot(func() {
		artifact, fetchErr = fetcher.Fetch(ctx, obj)
	})
	if fetchErr != nil {
		if c.allowMissingSecrets && errors.Is(fetchErr, manifest.ErrMissingSecret) {
			slog.Info("source: skipped (missing secret)", "id", id.String(), "err", fetchErr)
			return fmt.Errorf("%w: %s", manifest.ErrSourceSkipped, manifest.TrimSentinelPrefix(fetchErr.Error()))
		}
		return fetchErr
	}
	// An ExistenceFetcher returns (nil, nil) — the kind doesn't produce
	// an on-disk artifact (HelmRepository; OCIRepository when fetching
	// is disabled). RunWithStatus will still mark Ready so dependsOn
	// watchers unblock; chart resolution falls through to whichever
	// mechanism actually serves the chart.
	if artifact != nil {
		c.Store.SetArtifact(id, artifact)
	}
	return nil
}
