// Package source reconciles Flux source CRs (GitRepository,
// OCIRepository, future: Bucket, ExternalArtifact, …) into on-disk
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
	Store *store.Store
	Tasks *task.Service
	// Filter, when non-nil and enabled, narrows fetches to only
	// sources referenced by changed resources.
	Filter *change.Filter

	// Fetchers maps source CR kind → Fetcher implementation. Source
	// kinds with no entry are ignored, which is also how a flate caller
	// disables a kind (e.g. EnableOCI=false simply omits OCIRepository
	// from the map).
	Fetchers map[string]src.Fetcher

	unsubscribers []store.Unsubscribe
}

// Start registers listeners on the Store. The controller runs until
// Close is called.
func (c *Controller) Start(ctx context.Context) {
	c.unsubscribers = append(c.unsubscribers,
		c.Store.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx), true),
	)
}

// Close removes all listeners.
func (c *Controller) Close() {
	for _, u := range c.unsubscribers {
		u()
	}
	c.unsubscribers = nil
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
		if sus, ok := obj.(src.Suspendable); ok && sus.Suspended() {
			c.Store.UpdateStatus(id, store.StatusReady, "suspended")
			return
		}
		if c.skip(id) {
			return
		}
		c.Tasks.Go(ctx, "source/"+id.String(), func(ctx context.Context) {
			defer base.Recover(c.Store, id, "source")
			c.runFetch(ctx, id, obj, fetcher)
		})
	}
}

// skip applies the change filter and marks unaffected sources Ready
// without fetching so dependsOn waits succeed instantly.
func (c *Controller) skip(id manifest.NamedResource) bool {
	if !c.Filter.Enabled() {
		return false
	}
	if c.Filter.ShouldReconcile(id) {
		return false
	}
	c.Store.UpdateStatus(id, store.StatusReady, "unchanged")
	return true
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
		c.Store.UpdateStatus(id, store.StatusFailed, err.Error())
		return
	}
	c.Store.SetArtifact(id, artifact)
	c.Store.UpdateStatus(id, store.StatusReady, "")
}
