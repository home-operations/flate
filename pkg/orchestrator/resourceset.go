package orchestrator

import (
	"cmp"
	"context"

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
	// Parent attribution: a RS's source-file path lives below some KS's
	// claimed prefix — that KS is its visibility parent. Use the canonical
	// claim index (spec.path + spec.components + on-disk components:, via
	// the shared ComponentCache) and LongestParent — the SAME pair
	// discovery's orphan promotion and the orchestrator's detectOrphans
	// already use, so a RS inside a parent's component subtree attributes
	// to the deeper component-owning KS instead of being dropped or pinned
	// to a shallower one. (The previous inline index was a degraded third
	// copy keyed on spec.path only.)
	prefixes := loader.KSPathPrefixesWithCache(o.store, o.repoRoot, o.componentCache)

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
	// Concurrency cap respects Config.Concurrency when set so operators
	// who request serial/deterministic runs (Concurrency: 1) also get
	// that here; default is rsExpansionDefaultConcurrency (8).
	const rsExpansionDefaultConcurrency = 8
	type emit struct {
		key string
		doc map[string]any
	}
	type rsResult struct {
		parentKS manifest.NamedResource
		pending  []emit
	}
	results := make([]rsResult, len(rsList))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(cmp.Or(o.cfg.Concurrency, rsExpansionDefaultConcurrency))
	for i, rs := range rsList {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			docs, err := resourceset.Render(rs, resourceset.StoreResolver(o.store))
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
				// RS is never a KS, so LongestParent's self-exclusion is a no-op.
				parent, ok := loader.LongestParent(prefixes, file, rs.Named())
				if !ok {
					return nil
				}
				parentKS = parent
			}
			// Filter docs to RawObjects + collect dedup keys into this
			// RS's own slot; the deterministic commit happens below.
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
			// Each goroutine writes only its own results[i] slot (distinct
			// indices — no shared mutable state, no mutex needed).
			results[i] = rsResult{parentKS: parentKS, pending: pending}
			return nil
		})
	}
	err := g.Wait()
	// Commit serially in rsList order (sorted by namespace,name via
	// store.ListAs) AFTER g.Wait establishes happens-before: the first RS
	// to claim a DedupKey wins, deterministically, regardless of which
	// goroutine finished first. A name-grouped RS rendering the same
	// child from each namespace variant still collapses to one doc.
	seen := map[string]struct{}{}
	out := map[manifest.NamedResource][]map[string]any{}
	for _, r := range results {
		for _, e := range r.pending {
			if _, dup := seen[e.key]; dup {
				continue
			}
			seen[e.key] = struct{}{}
			out[r.parentKS] = append(out[r.parentKS], e.doc)
		}
	}
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
