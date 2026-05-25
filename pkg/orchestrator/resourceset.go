package orchestrator

import (
	"cmp"
	"path/filepath"
	"slices"
	"strings"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

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
func (o *Orchestrator) expandResourceSetsPostRun() {
	rsList := store.ListAs[*manifest.ResourceSet](o.store, manifest.KindResourceSet)
	if len(rsList) == 0 {
		return
	}
	// Owner index keyed by deepest spec.path prefix wins, mirroring
	// loader.BuildParentIndex. The RS's source-file path lives below
	// some KS's spec.path — that KS becomes its visibility parent.
	type owner struct {
		prefix string
		id     manifest.NamedResource
	}
	var owners []owner
	for _, ks := range store.ListAs[*manifest.Kustomization](o.store, manifest.KindKustomization) {
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

	// Dedupe by (apiVersion, kind, ns, name) across the union of every
	// RS's render — a name-grouped RS may legitimately render the same
	// child from each namespace variant, and we don't want to double-
	// emit it under the parent KS.
	seen := map[string]struct{}{}
	out := map[manifest.NamedResource][]map[string]any{}
	for _, rs := range rsList {
		docs, err := resourceset.Render(rs, o.resolveInputProvider)
		if err != nil || len(docs) == 0 {
			continue
		}
		// Resolve parent KS in priority order:
		//
		//   1. renderedSet.ParentOf — most direct; set when the RS
		//      arrived via emitRenderedChildren. No prefix matching
		//      needed.
		//   2. sourceFiles + path-prefix match — file-loaded RSes.
		//   3. Name-keyed sourceFile fallback — covers a KS-
		//      substituted variant whose namespace shifted at emit
		//      time, identified by sharing a name with a file-loaded
		//      sibling.
		var parentKS manifest.NamedResource
		var matched bool
		if parent, ok := o.rendered.ParentOf(rs.Named()); ok {
			parentKS, matched = parent, true
		} else {
			file := cmp.Or(o.sourceFiles[rs.Named()], sourceByName[rs.Name])
			if file == "" {
				continue
			}
			slashFile := filepath.ToSlash(file)
			for _, w := range owners {
				if strings.HasPrefix(slashFile, w.prefix) {
					parentKS = w.id
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
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
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			out[parentKS] = append(out[parentKS], doc)
		}
	}
	o.rsExtensions = out
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
