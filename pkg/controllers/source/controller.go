// Package source reconciles Flux source CRs (GitRepository,
// OCIRepository) into on-disk artifacts via the pkg/source SDK adapter,
// then publishes the result to the Store. It mirrors
// pkg/controllers/{kustomization,helmrelease}.
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

// Controller watches the Store for GitRepository and OCIRepository
// objects, reconciles each into an on-disk artifact, and updates the
// Store with the result.
type Controller struct {
	Store          *store.Store
	Tasks          *task.Service
	Cache          *src.Cache
	RegistryConfig string
	// Filter, when non-nil and enabled, narrows fetches to only
	// sources referenced by changed resources.
	Filter *change.Filter

	// EnableOCI controls whether OCIRepository reconciliation is active.
	// The upstream Python defaults to off so behavior matches.
	EnableOCI bool

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
		switch id.Kind {
		case manifest.KindGitRepository:
			repo, ok := payload.(*manifest.GitRepository)
			if !ok {
				return
			}
			if repo.Suspend {
				c.Store.UpdateStatus(id, store.StatusReady, "suspended")
				return
			}
			if c.skip(id) {
				return
			}
			c.Tasks.Go(ctx, "source/"+id.String(), func(ctx context.Context) {
				defer base.Recover(c.Store, id, "source")
				c.reconcileGit(ctx, id, repo)
			})
		case manifest.KindOCIRepository:
			if !c.EnableOCI {
				return
			}
			repo, ok := payload.(*manifest.OCIRepository)
			if !ok {
				return
			}
			if repo.Suspend {
				c.Store.UpdateStatus(id, store.StatusReady, "suspended")
				return
			}
			if c.skip(id) {
				return
			}
			c.Tasks.Go(ctx, "source/"+id.String(), func(ctx context.Context) {
				defer base.Recover(c.Store, id, "source")
				c.reconcileOCI(ctx, id, repo)
			})
		}
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

func (c *Controller) reconcileGit(ctx context.Context, id manifest.NamedResource, repo *manifest.GitRepository) {
	c.Store.UpdateStatus(id, store.StatusPending, "fetching")
	if art := c.Store.GetArtifact(id); art != nil {
		c.Store.UpdateStatus(id, store.StatusReady, "")
		return
	}
	slog.Debug("source: git fetch", "id", id.String(), "url", repo.URL)
	artifact, err := src.FetchGit(ctx, c.Cache, repo)
	if err != nil {
		c.Store.UpdateStatus(id, store.StatusFailed, err.Error())
		return
	}
	c.Store.SetArtifact(id, artifact)
	c.Store.UpdateStatus(id, store.StatusReady, "")
}

func (c *Controller) reconcileOCI(ctx context.Context, id manifest.NamedResource, repo *manifest.OCIRepository) {
	c.Store.UpdateStatus(id, store.StatusPending, "fetching")
	if art := c.Store.GetArtifact(id); art != nil {
		c.Store.UpdateStatus(id, store.StatusReady, "")
		return
	}
	slog.Debug("source: oci fetch", "id", id.String(), "url", repo.URL)
	artifact, err := src.FetchOCI(ctx, c.Cache, repo, c.RegistryConfig)
	if err != nil {
		c.Store.UpdateStatus(id, store.StatusFailed, err.Error())
		return
	}
	c.Store.SetArtifact(id, artifact)
	c.Store.UpdateStatus(id, store.StatusReady, "")
}
