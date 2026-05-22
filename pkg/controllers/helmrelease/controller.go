// Package helmrelease implements the HelmReleaseController.
//
// It listens for new HelmRelease objects and renders them via the helm
// SDK. The controller also maintains a chart-source index by listening
// for HelmRepository, OCIRepository, and GitRepository events: when an
// upstream source becomes Ready the helm client is told about it so
// subsequent template calls can resolve charts.
package helmrelease

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates HelmRelease reconciliation.
type Controller struct {
	Store *store.Store
	Tasks *task.Service
	Helm  *helm.Client
	// Filter, when non-nil and enabled, narrows reconciliation to
	// only the HelmReleases whose source files (or whose referenced
	// sources/values) changed between two checkouts.
	Filter *change.Filter

	// Options applied to every template call.
	Options helm.Options

	// WipeSecrets controls whether secrets are wiped from rendered
	// templates.
	WipeSecrets bool

	unsub []store.Unsubscribe
	coal  *task.Coalescer[manifest.NamedResource]

	chartSourcesMu sync.RWMutex
	chartSources   map[string]*manifest.HelmChartSource
}

// Start registers the listeners. The controller runs until Close.
func (c *Controller) Start(ctx context.Context) {
	c.coal = task.NewCoalescer[manifest.NamedResource](c.Tasks)
	c.chartSources = map[string]*manifest.HelmChartSource{}
	c.unsub = append(c.unsub,
		c.Store.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx), true),
		c.Store.AddListener(store.EventArtifactUpdated, c.onArtifactUpdated, true),
	)
}

// Close removes listeners.
func (c *Controller) Close() {
	for _, u := range c.unsub {
		u()
	}
	c.unsub = nil
}

func (c *Controller) onObjectAdded(ctx context.Context) store.Listener {
	return func(id manifest.NamedResource, payload any) {
		switch id.Kind {
		case manifest.KindHelmRepository:
			if r, ok := payload.(*manifest.HelmRepository); ok {
				c.Helm.AddRepo(r)
			}
		case manifest.KindOCIRepository:
			if r, ok := payload.(*manifest.OCIRepository); ok {
				c.Helm.AddOCIRepo(r)
			}
		case manifest.KindHelmChart:
			if s, ok := payload.(*manifest.HelmChartSource); ok {
				c.chartSourcesMu.Lock()
				c.chartSources[s.ResourceFullName()] = s
				c.chartSourcesMu.Unlock()
			}
		case manifest.KindHelmRelease:
			if _, ok := payload.(*manifest.HelmRelease); !ok {
				return
			}
			if c.Filter.Enabled() && !c.Filter.ShouldReconcile(id) {
				c.Store.UpdateStatus(id, store.StatusReady, "unchanged")
				return
			}
			c.coal.Submit(ctx, "helmrelease/"+id.String(), id, func(ctx context.Context) {
				defer func() {
					if r := recover(); r != nil {
						slog.Error("helmrelease: panic during reconcile", "id", id.String(), "panic", r)
						c.Store.UpdateStatus(id, store.StatusFailed, fmt.Sprintf("panic: %v", r))
					}
				}()
				// Re-read each iteration so a coalesced re-run picks up
				// a parent KS's patches rather than the stale payload.
				hr, _ := c.Store.GetObject(id).(*manifest.HelmRelease)
				if hr == nil {
					return
				}
				if err := c.reconcile(ctx, hr); err != nil {
					c.Store.UpdateStatus(id, store.StatusFailed, err.Error())
					return
				}
				c.Store.UpdateStatus(id, store.StatusReady, "")
			})
		}
	}
}

// onArtifactUpdated registers GitRepository artifacts with the helm
// client so charts referenced via sourceRef.kind=GitRepository can be
// loaded from disk.
func (c *Controller) onArtifactUpdated(id manifest.NamedResource, payload any) {
	if id.Kind != manifest.KindGitRepository {
		return
	}
	gitArt, ok := payload.(*store.GitArtifact)
	if !ok {
		return
	}
	repo, _ := c.Store.GetObject(id).(*manifest.GitRepository)
	if repo == nil {
		return
	}
	c.Helm.AddLocalGit(helm.LocalGitRepository{Repo: repo, Artifact: gitArt})
}

func (c *Controller) reconcile(ctx context.Context, hr *manifest.HelmRelease) error {
	id := hr.Named()
	c.Store.UpdateStatus(id, store.StatusPending, "resolving chart")

	// Resolve chartRef if applicable.
	helmCharts := c.gatherHelmChartSources()
	if err := hr.ResolveChartRef(helmCharts); err != nil {
		return err
	}

	// Wait for chart source (HelmRepository / OCIRepository / GitRepository)
	// to be ready. For HelmRepository we wait by existence rather than
	// status — there's no controller updating HelmRepository status.
	srcID := manifest.NamedResource{
		Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
	}
	w := &depwait.Waiter{Store: c.Store, Parent: id}
	sum := depwait.WaitAll(w.Watch(ctx, []manifest.NamedResource{srcID}))
	if sum.AnyFailed() {
		return fmt.Errorf("chart source %s not ready: %s", srcID.String(), sum.Messages[srcID])
	}

	c.Store.UpdateStatus(id, store.StatusPending, "resolving values")
	provider := values.NewStoreProvider(c.Store)
	if err := values.ExpandValueReferences(hr, provider); err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering chart")
	docs, err := c.Helm.TemplateDocs(ctx, hr, hr.Values, c.Options)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("helmrelease: skipped doc", "id", id.String(), "err", err)
			continue
		}
		c.Store.AddRendered(obj)
	}

	c.Store.SetArtifact(id, &store.HelmReleaseArtifact{
		ChartName: hr.Chart.ChartName(),
		Manifests: docs,
		Values:    hr.Values,
	})
	return nil
}

// gatherHelmChartSources returns a snapshot of the HelmChart lookup
// map. The cache is maintained incrementally by onObjectAdded — every
// HelmChart added to the store (initial parse phase or later, e.g. via
// a Kustomization render) flows through the same listener.
func (c *Controller) gatherHelmChartSources() map[string]*manifest.HelmChartSource {
	c.chartSourcesMu.RLock()
	defer c.chartSourcesMu.RUnlock()
	out := make(map[string]*manifest.HelmChartSource, len(c.chartSources))
	maps.Copy(out, c.chartSources)
	return out
}
