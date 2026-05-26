package orchestrator

import (
	"cmp"
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"golang.org/x/sync/errgroup"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

// expandResourceSetsPostRun re-renders every ResourceSet using the
// post-Run store state and attributes any non-Flux child docs to the
// owning structural-parent Kustomization. Fires after Run so it sees
// RSIPs the KS controller emitted from kustomize substitution (the
// `dragonfly-${APP}` -> `dragonfly-renovate-operator-jobs` etc.
// pattern in tholinka/home-ops, which discovery's pre-Bootstrap RS
// pass cannot see because the substitution hasn't happened yet).
//
// Flux-kind children (Kustomization, HelmRelease, …) are intentionally
// NOT re-emitted here — they would have failed reconcile anyway since
// it's too late in the pipeline to add reconcilable objects. Discovery
// is the canonical seeding point for Flux-kind RS children; this pass
// only handles the visibility gap for non-Flux output.
func (o *Orchestrator) expandResourceSetsPostRun(ctx context.Context) error {
	rsList := store.ListAs[*manifest.ResourceSet](o.store, manifest.KindResourceSet)
	if len(rsList) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Owner index keyed by deepest spec.path prefix wins, mirroring
	// loader.BuildParentIndex. The RS's source-file path lives below
	// some KS's spec.path — that KS becomes its visibility parent.
	type owner struct {
		prefix string
		id     manifest.NamedResource
	}
	ksList := store.ListAs[*manifest.Kustomization](o.store, manifest.KindKustomization)
	owners := make([]owner, 0, len(ksList))
	for _, ks := range ksList {
		if ks.Path == "" {
			continue
		}
		owners = append(owners, owner{prefix: loader.NormalizePrefix(ks.Path), id: ks.Named()})
	}
	slices.SortFunc(owners, func(a, b owner) int {
		return cmp.Compare(len(b.prefix), len(a.prefix))
	})

	// A RS that arrived through file discovery has a sourceFile; a RS
	// that arrived through KS-controller emission (kustomize bakes the
	// parent's targetNamespace into a duplicate copy) does not. Build
	// a name-keyed sourceFile fallback so we can attribute the
	// namespace-resolved variant — which is the one with RSIPs visible
	// to its selectors — through its file-loaded sibling.
	sourceByName := map[string]string{}
	for id, f := range o.sourceFiles {
		if id.Kind != manifest.KindResourceSet || f == "" {
			continue
		}
		if _, exists := sourceByName[id.Name]; !exists {
			sourceByName[id.Name] = f
		}
	}

	// Render each RS in parallel — they only read shared state (the
	// store + owners index) which is safe under concurrent reads. The
	// dedup + accumulation step runs under a mutex so the cross-RS
	// "first-wins" invariant is preserved (a name-grouped RS may
	// legitimately render the same child from each namespace variant
	// and we don't want to double-emit it under the parent KS).
	//
	// Concurrency limit matches the bucket fetcher (8) — render is
	// CPU-bound, so the cap exists more to prevent goroutine
	// explosion on huge RS-counts than as a rate limiter.
	var (
		mu   sync.Mutex
		seen = map[string]struct{}{}
		out  = map[manifest.NamedResource][]map[string]any{}
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, rs := range rsList {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			docs, err := resourceset.Render(rs, o.resolveInputProvider)
			if err != nil {
				o.store.UpdateStatus(rs.Named(), store.StatusFailed, err.Error())
				return err
			}
			if len(docs) == 0 {
				return nil
			}
			// Resolve parent KS in priority order:
			//   1. renderedSet.ParentOf — direct lookup.
			//   2. sourceFiles + path-prefix match — file-loaded.
			//   3. Name-keyed sourceFile fallback — KS-substituted
			//      variant whose namespace shifted at emit time.
			var parentKS manifest.NamedResource
			if parent, ok := o.rendered.ParentOf(rs.Named()); ok {
				parentKS = parent
			} else {
				file := cmp.Or(o.sourceFiles[rs.Named()], sourceByName[rs.Name])
				if file == "" {
					return nil
				}
				slashFile := filepath.ToSlash(file)
				i := slices.IndexFunc(owners, func(w owner) bool {
					return strings.HasPrefix(slashFile, w.prefix)
				})
				if i < 0 {
					return nil
				}
				parentKS = owners[i].id
			}
			// Filter docs to RawObjects + collect dedup keys
			// outside the mutex; the mutex only holds for the
			// commit-to-shared-state step.
			type emit struct {
				key string
				doc map[string]any
			}
			pending := make([]emit, 0, len(docs))
			for _, doc := range docs {
				parsed, perr := manifest.ParseDoc(doc, manifest.ParseDocOptions{WipeSecrets: o.cfg.WipeSecrets})
				if perr != nil {
					continue
				}
				if _, raw := parsed.(*manifest.RawObject); !raw {
					continue
				}
				key := resourceset.DedupKey(doc)
				if key == "" {
					continue
				}
				pending = append(pending, emit{key, doc})
			}
			if len(pending) == 0 {
				return nil
			}
			mu.Lock()
			defer mu.Unlock()
			for _, e := range pending {
				if _, dup := seen[e.key]; dup {
					continue
				}
				seen[e.key] = struct{}{}
				out[parentKS] = append(out[parentKS], e.doc)
			}
			return nil
		})
	}
	err := g.Wait()
	o.rsExtensions = out
	if err != nil {
		failed := map[manifest.NamedResource]store.StatusInfo{}
		for _, rs := range rsList {
			id := rs.Named()
			if info, ok := o.store.GetStatus(id); ok && info.Status == store.StatusFailed {
				failed[id] = info
			}
		}
		if len(failed) > 0 {
			return o.aggregateFailures(failed)
		}
		return err
	}
	return nil
}

// resolveInputProvider mirrors discovery.resolveInputProvider but
// against the post-Run store, which now includes RSIPs emitted by
// the KS controller from kustomize substitution. Same semantics:
// name-only refs hit the exact id; selector refs walk the requested
// namespace's RSIPs and filter by metadata.labels.
func (o *Orchestrator) resolveInputProvider(ref fluxopv1.InputProviderReference, namespace string) ([]*manifest.ResourceSetInputProvider, error) {
	if ref.Name != "" {
		id := manifest.NamedResource{
			Kind:      manifest.KindResourceSetInputProvider,
			Namespace: namespace,
			Name:      ref.Name,
		}
		obj, ok := store.Get[*manifest.ResourceSetInputProvider](o.store, id)
		if !ok {
			return nil, nil
		}
		return []*manifest.ResourceSetInputProvider{obj}, nil
	}
	if ref.Selector == nil {
		return nil, nil
	}
	var out []*manifest.ResourceSetInputProvider
	for _, p := range store.ListAs[*manifest.ResourceSetInputProvider](o.store, manifest.KindResourceSetInputProvider) {
		if p.Namespace != namespace {
			continue
		}
		match, err := resourceset.MatchSelector(ref.Selector, p.Labels)
		if err != nil {
			return nil, err
		}
		if match {
			out = append(out, p)
		}
	}
	return out, nil
}
