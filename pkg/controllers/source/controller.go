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
		fetcher, registered := c.Fetchers[id.Kind]
		if !registered {
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
			defer base.Recover(c.Store, id, "source")
			c.runFetch(ctx, id, obj, fetcher)
		})
	}
}

// runFetch invokes the registered Fetcher and translates the result
// into Store status + artifact writes. Generic over kind: the only
// per-kind decision (which Fetcher to call) was made above.
func (c *Controller) runFetch(ctx context.Context, id manifest.NamedResource, obj manifest.BaseManifest, fetcher src.Fetcher) {
	c.Store.UpdateStatus(id, store.StatusPending, "fetching")
	if art := c.Store.GetArtifact(id); art != nil {
		c.Store.UpdateStatus(id, store.StatusReady, "")
		return
	}
	slog.Debug("source: fetch", "id", id.String())
	artifact, err := fetcher.Fetch(ctx, obj)
	if err != nil {
		if c.allowMissingSecrets && errors.Is(err, manifest.ErrMissingSecret) {
			// Soft-skip: leave the artifact slot empty so consumers see
			// "no artifact" + Ready+"skipped:" status and propagate.
			c.Store.UpdateStatus(id, store.StatusReady,
				"skipped: "+manifest.TrimSentinelPrefix(err.Error()))
			slog.Info("source: skipped (missing secret)", "id", id.String(), "err", err)
			return
		}
		c.Store.UpdateStatus(id, store.StatusFailed, err.Error())
		return
	}
	// An ExistenceFetcher returns (nil, nil) — the kind doesn't produce
	// an on-disk artifact (HelmRepository; OCIRepository when fetching
	// is disabled). Mark Ready without writing an artifact so dependsOn
	// watchers unblock and chart resolution falls through to whichever
	// mechanism actually serves the chart.
	if artifact != nil {
		c.Store.SetArtifact(id, artifact)
	}
	c.Store.UpdateStatus(id, store.StatusReady, "")
}
