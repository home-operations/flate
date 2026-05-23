// Package kustomization reconciles Flux Kustomizations: wait on
// dependsOn / sourceRef / structural parent, resolve postBuild
// substitutions, run the kustomize SDK, parse the result back into the
// Store, and publish a KustomizationArtifact. Failures bubble up to
// the orchestrator.
package kustomization

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	yaml "go.yaml.in/yaml/v4"

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

	// ParentOf maps each Flux Kustomization to its structural parent —
	// the enclosing KS whose spec.path contains this one's source
	// file. Reconcile waits for the parent's Ready before rendering so
	// any parent-render-time spec mutations (e.g. `replacements:`
	// injecting spec.targetNamespace) are observable when the child
	// renders. Nil means "no parent enforcement," matching pre-#102
	// behavior.
	ParentOf map[manifest.NamedResource]manifest.NamedResource

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
		// Release our worker slot for the duration of the dep wait so
		// children we depend on can acquire one and make progress.
		// Without this, N parents on a bounded Service consume all
		// slots while waiting for children that need a slot to run.
		var sum depwait.Summary
		c.Tasks.YieldSlot(func() {
			w := &depwait.Waiter{Store: c.Store, Parent: id}
			sum = depwait.WaitAll(w.Watch(ctx, deps))
		})
		if sum.AnyFailed() {
			return &manifest.DependencyFailedError{
				Parent:  id,
				Failed:  sum.Failed,
				Reasons: sum.Messages,
			}
		}
		// Refresh the KS — our structural parent may have re-emitted
		// us with mutated spec (e.g. `replacements:` injecting
		// spec.targetNamespace) while we were waiting. Without this
		// re-read the first render would use the stale-spec snapshot
		// captured by RunWithStatus, producing duplicate renders that
		// linger in the store with the wrong namespace. See #102.
		if fresh, ok := c.Store.GetObject(id).(*manifest.Kustomization); ok {
			ks = fresh
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
	data, err := kustomize.RenderFlux(ctx, c.Staging, sourceRoot, ks.Path, ks.Contents)
	if err != nil {
		return err
	}
	docs, err := manifest.SplitDocs(data)
	if err != nil {
		return err
	}

	// Per-resource envsubst. Flux's kustomize-controller skips
	// substitution on any resource carrying the
	// "kustomize.toolkit.fluxcd.io/substitute: disabled" label or
	// annotation — used in real repos for ConfigMaps that embed
	// shell scripts with bash array expansions (${ARR[@]}) that
	// envsubst's parser cannot handle. Mirror that behavior here:
	// substitute per-doc, skip opted-out resources, so we match Flux
	// bit-for-bit.
	if vars := values.VarsMap(ks.PostBuildSubstitute); len(vars) > 0 {
		for i, doc := range docs {
			if manifest.HasSubstituteDisabled(doc) {
				continue
			}
			substituted, sErr := substituteDoc(doc, vars)
			if sErr != nil {
				return sErr
			}
			docs[i] = substituted
		}
	}

	c.Store.UpdateStatus(id, store.StatusPending, fmt.Sprintf("applying %d objects", len(docs)))
	opts := manifest.ParseDocOptions{WipeSecrets: c.WipeSecrets}
	// Two-pass emission. Pass 1 lands "data" kinds (ConfigMap, Secret,
	// sources) into the store so child Kustomization / HelmRelease
	// reconciles in pass 2 see a complete store — substituteFrom and
	// chart-source lookups would race otherwise, since AddObject for a
	// reconcilable kind fires its controller on a separate goroutine
	// immediately. Within each pass the controller renders the docs in
	// kustomize's emission order; passes themselves are ordered so the
	// data backing a reconcile always arrives first.
	type parsed struct {
		obj          manifest.BaseManifest
		reconcilable bool
	}
	objs := make([]parsed, 0, len(docs))
	for _, doc := range docs {
		if manifest.IsEncryptedSecret(doc) {
			// flate can't decrypt SOPS offline. ParseSecret will wipe
			// the encrypted values to PLACEHOLDER below — same as it
			// does for cleartext Secret data when --wipe-secrets is on
			// — so downstream substituteFrom lookups succeed with a
			// clearly-marked placeholder rather than failing.
			name, ns := manifest.DocMetadata(doc)
			slog.Debug("kustomization: SOPS-encrypted resource wiped to placeholder",
				"id", id.String(), "ref", manifest.DocKind(doc)+" "+ns+"/"+name)
		}
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			// Don't fail on parse errors of inline resources — they may
			// be raw kubernetes manifests flate doesn't track. Log and
			// continue.
			slog.Debug("kustomization: skipped doc", "id", id.String(), "err", err)
			continue
		}
		objs = append(objs, parsed{obj: obj, reconcilable: shouldDispatchAsObject(obj)})
	}
	// Pass 1 — data first. Sources go through AddObject because they
	// have their own status to track; ConfigMap/Secret have no
	// controller, so AddObject's event dispatch is a no-op for them.
	// Either way they're in the store before pass 2 fires.
	for _, p := range objs {
		if p.reconcilable && isLeafReconcilable(p.obj) {
			continue
		}
		if p.reconcilable {
			c.Store.AddObject(p.obj)
			c.Store.MarkRendered(p.obj.Named())
		} else {
			c.Store.AddRendered(p.obj)
		}
	}
	// Pass 2 — leaf reconcilables (Kustomization, HelmRelease). Their
	// substituteFrom / chartRef lookups now see the data from pass 1.
	for _, p := range objs {
		if p.reconcilable && isLeafReconcilable(p.obj) {
			c.Store.AddObject(p.obj)
			c.Store.MarkRendered(p.obj.Named())
		}
	}

	c.Store.SetArtifact(id, &store.KustomizationArtifact{
		Path:      filepath.Join(sourceRoot, ks.Path),
		Manifests: docs,
	})
	return nil
}

// substituteDoc marshals a single manifest doc, runs envsubst over it,
// and unmarshals the result back. Per-doc substitution (rather than
// substitute-the-whole-blob) lets us honor Flux's
// "kustomize.toolkit.fluxcd.io/substitute: disabled" opt-out, which is
// scoped to individual resources.
func substituteDoc(doc map[string]any, vars map[string]string) (map[string]any, error) {
	raw, err := yaml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("substitute: marshal doc: %w", err)
	}
	if !kustomize.HasSubstitutions(raw) {
		return doc, nil
	}
	out, err := kustomize.Substitute(raw, vars)
	if err != nil {
		return nil, err
	}
	var next map[string]any
	if err := yaml.Unmarshal(out, &next); err != nil {
		return nil, fmt.Errorf("substitute: unmarshal doc: %w", err)
	}
	return next, nil
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

// isLeafReconcilable reports whether an emitted object should be held
// for pass 2. Kustomization + HelmRelease have controllers that fire
// substituteFrom / chartRef lookups against the store the instant
// their AddObject event arrives; emitting them after all "data" kinds
// guarantees those lookups succeed.
func isLeafReconcilable(obj manifest.BaseManifest) bool {
	switch obj.(type) {
	case *manifest.Kustomization, *manifest.HelmRelease:
		return true
	}
	return false
}

// collectDeps assembles the dependency refs whose readiness must
// precede this Kustomization: explicit dependsOn entries (carrying any
// CEL ReadyExpr), the source ref, and the implicit structural parent
// (the enclosing Flux KS that renders us — must finish first so any
// parent-render-time spec injections land before our reconcile).
func (c *Controller) collectDeps(ks *manifest.Kustomization) []manifest.DependencyRef {
	deps := append([]manifest.DependencyRef(nil), ks.DependsOn...)
	if ks.SourceKind != "" && ks.SourceName != "" {
		deps = append(deps, manifest.DependencyRef{
			NamedResource: manifest.NamedResource{
				Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
			},
		})
	}
	if parent, ok := c.ParentOf[ks.Named()]; ok {
		deps = append(deps, manifest.DependencyRef{NamedResource: parent})
	}
	return deps
}

// bootstrapSourceID is the synthetic GitRepository the orchestrator
// seeds for the user's working tree. Children that inherit sourceRef
// only from a parent's render patches (a common onedr0p-style pattern)
// look empty until the parent reconciles, so resolveSourceRoot uses
// this as the fallback rather than mis-joining ks.Path against itself.
var bootstrapSourceID = manifest.NamedResource{
	Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "flux-system",
}

// resolveSourceRoot returns the on-disk root the kustomization should
// be built from — i.e. the source artifact's local path. The Flux
// renderer then joins ks.Path onto this root.
func (c *Controller) resolveSourceRoot(ks *manifest.Kustomization) (string, error) {
	srcID := manifest.NamedResource{Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName}
	if ks.SourceKind == "" || ks.SourceName == "" {
		// Child Kustomizations that inherit sourceRef from a parent's
		// render patches show empty here until that parent reconciles.
		// Fall back to the seeded bootstrap GitRepository (the user's
		// working tree) so the first reconcile resolves to the repo
		// root instead of doubling ks.Path against itself (#105).
		if ks.Path == "" {
			return "", fmt.Errorf("%w: kustomization %s has no path and no source",
				manifest.ErrInput, ks.NamespacedName())
		}
		srcID = bootstrapSourceID
	}
	art := c.Store.GetArtifact(srcID)
	if art == nil {
		return "", fmt.Errorf("%w: source %s artifact not found", manifest.ErrObjectNotFound, srcID.String())
	}
	if sa, ok := art.(*store.SourceArtifact); ok {
		return sa.LocalPath, nil
	}
	return "", fmt.Errorf("%w: unsupported source artifact type %T for %s",
		manifest.ErrFlux, art, srcID.String())
}
