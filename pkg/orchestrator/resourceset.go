package orchestrator

import (
	"cmp"
	"path/filepath"
	"slices"
	"strings"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
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
	rsList := o.store.ListObjects(manifest.KindResourceSet)
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
	for _, obj := range o.store.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.Path == "" {
			continue
		}
		p := filepath.ToSlash(ks.Path)
		p = strings.TrimPrefix(p, "./")
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		owners = append(owners, owner{prefix: p, id: ks.Named()})
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
	for _, obj := range rsList {
		rs, ok := obj.(*manifest.ResourceSet)
		if !ok {
			continue
		}
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
			file := o.sourceFiles[rs.Named()]
			if file == "" {
				file = sourceByName[rs.Name]
			}
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
			key := dedupKeyForDoc(doc)
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

// dedupKeyForDoc keys a rendered doc by (apiVersion, kind, namespace,
// name). Empty string when any required component is missing —
// signals "drop this doc" rather than collide with other emptys.
func dedupKeyForDoc(doc map[string]any) string {
	apiVersion, _ := doc["apiVersion"].(string)
	kind, _ := doc["kind"].(string)
	md, _ := doc["metadata"].(map[string]any)
	name, _ := md["name"].(string)
	ns, _ := md["namespace"].(string)
	if kind == "" || name == "" {
		return ""
	}
	return apiVersion + "|" + kind + "|" + ns + "|" + name
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
		obj, _ := o.store.GetObject(id).(*manifest.ResourceSetInputProvider)
		if obj == nil {
			return nil, nil
		}
		return []*manifest.ResourceSetInputProvider{obj}, nil
	}
	if ref.Selector == nil {
		return nil, nil
	}
	var out []*manifest.ResourceSetInputProvider
	for _, obj := range o.store.ListObjects(manifest.KindResourceSetInputProvider) {
		p, ok := obj.(*manifest.ResourceSetInputProvider)
		if !ok || p.Namespace != namespace {
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
