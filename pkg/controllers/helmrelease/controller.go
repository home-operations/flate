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
	"strings"
	"sync"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
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
			hr, ok := payload.(*manifest.HelmRelease)
			if !ok {
				return
			}
			if hr.Suspend {
				c.Store.UpdateStatus(id, store.StatusReady, "suspended")
				return
			}
			if c.Filter.Enabled() && !c.Filter.ShouldReconcile(id) {
				c.Store.UpdateStatus(id, store.StatusReady, "unchanged")
				return
			}
			c.coal.Submit(ctx, "helmrelease/"+id.String(), id, func(ctx context.Context) {
				base.RunWithStatus(ctx, c.Store, id, "helmrelease", c.reconcile)
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
	gitArt, ok := payload.(*store.SourceArtifact)
	if !ok || gitArt.Kind != manifest.KindGitRepository {
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

	// Honor spec.dependsOn — HR-to-HR ordering. Flux gates rendering on
	// each dependency reaching Ready before this HR reconciles.
	if deps := c.collectHRDeps(hr); len(deps) > 0 {
		c.Store.UpdateStatus(id, store.StatusPending, "resolving dependencies")
		w := &depwait.Waiter{Store: c.Store, Parent: id}
		sum := depwait.WaitAll(w.Watch(ctx, deps))
		if sum.AnyFailed() {
			var msgs []string
			for _, f := range sum.Failed {
				msgs = append(msgs, f.String()+": "+sum.Messages[f])
			}
			return fmt.Errorf("dependencies failed: %s", strings.Join(msgs, "; "))
		}
	}

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
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			return fmt.Errorf(
				"SOPS-encrypted %s %s/%s in rendered chart output: flate does not implement spec.decryption — "+
					"render against pre-decrypted manifests or remove the encrypted resource",
				manifest.DocKind(doc), ns, name,
			)
		}
	}
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

// collectHRDeps converts hr.DependsOn ("namespace/name" entries) into
// NamedResources for the depwait Waiter. dependsOn on a HelmRelease
// references other HelmReleases only (per Flux spec).
func (c *Controller) collectHRDeps(hr *manifest.HelmRelease) []manifest.NamedResource {
	if len(hr.DependsOn) == 0 {
		return nil
	}
	out := make([]manifest.NamedResource, 0, len(hr.DependsOn))
	for _, dep := range hr.DependsOn {
		ns, name, ok := manifest.SplitNamespacedName(dep, hr.Namespace)
		if !ok {
			continue
		}
		out = append(out, manifest.NamedResource{
			Kind: manifest.KindHelmRelease, Namespace: ns, Name: name,
		})
	}
	return out
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
