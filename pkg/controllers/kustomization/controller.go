// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn/sourceRef, resolve postBuild substitutions, run the
// kustomize SDK, parse the result back into the Store, and publish a
// KustomizationArtifact. Failures bubble up to the orchestrator.
package kustomization

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	"github.com/home-operations/flate/pkg/values"
)

// Controller orchestrates Kustomization reconciliation.
type Controller struct {
	Store *store.Store
	Tasks *task.Service
	// Staging is a process-wide cache that materializes source roots
	// into writable copies so Flux's Generator can write the merged
	// kustomization.yaml without touching the user's working tree.
	Staging *kustomize.StagingCache
	// Filter, when non-nil and enabled, narrows reconciliation to
	// only the resources whose source files changed between two
	// checkouts.
	Filter *change.Filter

	// WipeSecrets controls whether Secret cleartext is wiped when
	// parsing rendered manifests.
	WipeSecrets bool

	unsub []store.Unsubscribe
	coal  *task.Coalescer[manifest.NamedResource]
}

// Start registers the listener that drives reconciliation.
func (c *Controller) Start(ctx context.Context) {
	c.coal = task.NewCoalescer[manifest.NamedResource](c.Tasks)
	c.unsub = append(c.unsub,
		c.Store.AddListener(store.EventObjectAdded, c.onObjectAdded(ctx), true),
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
		if id.Kind != manifest.KindKustomization {
			return
		}
		ks, ok := payload.(*manifest.Kustomization)
		if !ok {
			return
		}
		if ks.Suspend {
			c.Store.UpdateStatus(id, store.StatusReady, "suspended")
			return
		}
		if c.Filter.Enabled() && !c.Filter.ShouldReconcile(id) {
			c.Store.UpdateStatus(id, store.StatusReady, "unchanged")
			return
		}
		c.coal.Submit(ctx, "kustomization/"+id.String(), id, func(ctx context.Context) {
			base.RunWithStatus(ctx, c.Store, id, "kustomization", c.reconcile)
		})
	}
}

func (c *Controller) reconcile(ctx context.Context, ks *manifest.Kustomization) error {
	id := ks.Named()
	c.Store.UpdateStatus(id, store.StatusPending, "resolving dependencies")

	deps := c.collectDeps(ks)
	if len(deps) > 0 {
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

	c.Store.UpdateStatus(id, store.StatusPending, "resolving source artifact")
	sourceRoot, err := c.resolveSourceRoot(ks)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "expanding substitutions")
	provider := values.NewStoreProvider(c.Store)
	if err := values.ExpandPostBuildSubstituteReference(ks, provider); err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, "rendering")
	// Flux's Generator merges spec.patches / spec.images / spec.components /
	// spec.targetNamespace / spec.commonMetadata into the kustomization.yaml
	// before krusty runs — none of which the bare kustomize SDK applies.
	data, err := kustomize.RenderFlux(c.Staging, sourceRoot, ks.Path, ks.Contents)
	if err != nil {
		return err
	}
	if vars := values.VarsMap(ks.PostBuildSubstitute); len(vars) > 0 && kustomize.HasSubstitutions(data) {
		data, err = kustomize.Substitute(data, vars)
		if err != nil {
			return err
		}
	}

	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return err
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	var sopsRefs []string
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			name, ns := manifest.DocMetadata(doc)
			sopsRefs = append(sopsRefs, fmt.Sprintf("%s %s/%s", manifest.DocKind(doc), ns, name))
			continue
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			// Don't fail on parse errors of inline resources — they may
			// be raw kubernetes manifests flate doesn't track. Log and
			// continue.
			slog.Debug("kustomization: skipped doc", "id", id.String(), "err", err)
			continue
		}
		// When a parent Kustomization renders a Flux resource that has
		// its own controller (child Kustomization, HelmRelease, sources),
		// route through AddObject so the rendered version (with patches,
		// commonMetadata, targetNamespace, etc. applied) supersedes any
		// statically-loaded copy and triggers a fresh reconcile.
		// Non-reconcilable leaves keep the cheaper AddRendered path.
		if shouldDispatchAsObject(obj) {
			c.Store.AddObject(obj)
		} else {
			c.Store.AddRendered(obj)
		}
	}
	if len(sopsRefs) > 0 {
		// Surface SOPS docs as the Kustomization's failure reason but
		// keep the non-SOPS docs in the store so downstream consumers
		// can still reconcile against the parts flate could render.
		return fmt.Errorf(
			"SOPS-encrypted resource(s) in rendered output: %s — flate does not implement spec.decryption; "+
				"render against pre-decrypted manifests or remove the encrypted resource",
			strings.Join(sopsRefs, ", "),
		)
	}

	c.Store.SetArtifact(id, &store.KustomizationArtifact{
		Path:      filepath.Join(sourceRoot, ks.Path),
		Manifests: docs,
	})
	return nil
}

// shouldDispatchAsObject reports whether a render-emitted Flux
// resource needs to fire EventObjectAdded so its own controller picks
// it up. The pattern is: parent Kustomization renders → emits a
// child Flux resource (e.g. another Kustomization with parent patches
// applied, a HelmRelease, an OCIRepository fanned out from a kustomize
// component) → that child's controller must reconcile the patched
// version, not the statically-loaded one.
func shouldDispatchAsObject(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.Kustomization,
		*manifest.HelmRelease,
		*manifest.HelmRepository,
		*manifest.OCIRepository,
		*manifest.GitRepository,
		*manifest.Bucket,
		*manifest.HelmChartSource,
		*manifest.ExternalArtifact,
		*manifest.ConfigMap,
		*manifest.Secret:
		return true
	}
	return false
}

// collectDeps assembles the dependency refs whose readiness must
// precede this Kustomization: explicit dependsOn entries (carrying any
// CEL ReadyExpr) + the source ref.
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := append([]manifest.DependencyRef(nil), ks.DependsOn...)
	if ks.SourceKind != "" && ks.SourceName != "" {
		deps = append(deps, manifest.DependencyRef{
			NamedResource: manifest.NamedResource{
				Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
			},
		})
	}
	return deps
}

// resolveSourceRoot returns the on-disk root the kustomization should
// be built from — i.e. the source artifact's local path. The Flux
// renderer then joins ks.Path onto this root.
func (c *Controller) resolveSourceRoot(ks *manifest.Kustomization) (string, error) {
	if ks.SourceKind == "" || ks.SourceName == "" {
		// No source — use ks.Path directly. This handles bootstrap
		// kustomizations whose source is implicit.
		if ks.Path == "" {
			return "", errors.New("kustomization has no path and no source")
		}
		return ks.Path, nil
	}
	srcID := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	art := c.Store.GetArtifact(srcID)
	if art == nil {
		return "", fmt.Errorf("%w: source %s artifact not found", manifest.ErrObjectNotFound, srcID.String())
	}
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa.LocalPath, nil
	}
	return "", fmt.Errorf("unsupported source artifact type %T for %s", art, srcID.String())
}
