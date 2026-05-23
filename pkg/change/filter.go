package change

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// Filter answers "should I reconcile this resource?" by checking
// against a keep-set resolved from a file-level diff. Construct via
// NewFilter — the zero value is the "no filtering" sentinel and
// returns true from ShouldReconcile for every id.
type Filter struct {
	changes     *Set
	sourceFiles map[manifest.NamedResource]string
	repoRoot    string
	keep        map[manifest.NamedResource]struct{}

	// keepByName: (Kind, Name) presence set used as an O(1) fallback
	// when either side of a lookup has an empty namespace.
	keepByName map[nameKey]struct{}
}

type nameKey struct{ kind, name string }

// NewFilter constructs a fully-resolved Filter in one shot. It walks
// the file-level Changes set, attributes each change to the most
// specific Flux Kustomization that owns it, then expands transitive
// dependencies (chart sources, sourceRef, valuesFrom). Pass a nil
// changes argument to construct a disabled filter (ShouldReconcile
// returns true for everything).
//
//  1. Every resource whose source file changed is kept.
//  2. For each changed file, the most-specific Flux Kustomization that
//     owns it (longest matching spec.path, including spec.components)
//     is kept — along with every resource whose source file shares
//     the same owner.
//  3. Ancestor Kustomizations (shorter-prefix spec.path matches) are
//     also kept so parent-injected patches / postBuild.substituteFrom
//     land before the leaf renders. See #58.
//  4. BFS over chart sources, sourceRef, and valuesFrom to pull in
//     upstream dependencies. dependsOn is intentionally excluded.
func NewFilter(changes *Set, sourceFiles map[manifest.NamedResource]string, repoRoot string, objs ObjectLister) *Filter {
	f := &Filter{
		changes:     changes,
		sourceFiles: sourceFiles,
		repoRoot:    repoRoot,
	}
	if changes == nil {
		return f
	}
	f.resolve(objs)
	return f
}

// Enabled reports whether change-based filtering is active.
func (f *Filter) Enabled() bool { return f != nil && f.changes != nil }

// ShouldReconcile reports whether the controller for id should do work
// (true when filtering is disabled). Lookups tolerate an empty
// namespace on either side because parent-Kustomization targetNamespace
// inheritance is applied lazily.
func (f *Filter) ShouldReconcile(id manifest.NamedResource) bool {
	if !f.Enabled() {
		return true
	}
	if _, ok := f.keep[id]; ok {
		return true
	}
	if id.Namespace != "" {
		if _, ok := f.keepByName[nameKey{id.Kind, id.Name}]; ok {
			return true
		}
	}
	return false
}

func (f *Filter) resolve(objs ObjectLister) {
	keep := make(map[manifest.NamedResource]struct{})
	var queue []manifest.NamedResource
	enqueue := func(id manifest.NamedResource) {
		if _, seen := keep[id]; seen {
			return
		}
		keep[id] = struct{}{}
		queue = append(queue, id)
	}

	owners := buildOwnership(objs, f.repoRoot)
	ownersHit := make(map[manifest.NamedResource]struct{})

	for _, file := range f.changes.Paths() {
		for _, owner := range owners.ownersOf(file) {
			ownersHit[owner] = struct{}{}
			enqueue(owner)
		}
		// Also include ancestor/meta Kustomizations whose render
		// mutates the leaf owner's spec — parent-injected spec.patches
		// and postBuild.substituteFrom land at parent-render time, so
		// in changed-only mode the parent has to run too. Ancestors
		// are NOT added to ownersHit, so the sibling-pull-in below
		// doesn't drag in everything else they own. See #58.
		for _, ancestor := range owners.ancestorsOf(file) {
			enqueue(ancestor)
		}
	}
	for id, src := range f.sourceFiles {
		if f.changes.Contains(src) {
			enqueue(id)
			continue
		}
		// Pull in every sibling resource that shares an affected owner.
		for _, owner := range owners.ownersOf(src) {
			if _, hit := ownersHit[owner]; hit {
				enqueue(id)
				break
			}
		}
	}

	for head := 0; head < len(queue); head++ {
		for _, d := range transitiveDeps(objs, queue[head]) {
			enqueue(d)
		}
		// Also walk the structural-parent chain of any Flux
		// Kustomization in the keep set. A leaf change pulls in its
		// owner KS (above); that KS's own source file might live under
		// a parent KS's spec.path (the home-ops cross-tree pattern —
		// see #103). Without the parent reconciling, namespace-scoped
		// sources it emits (e.g. components/namespace producing one
		// OCIRepository per tenant ns) never land in the store, and
		// the leaf can't resolve its chart ref.
		if queue[head].Kind != manifest.KindKustomization {
			continue
		}
		src, ok := f.sourceFiles[queue[head]]
		if !ok {
			continue
		}
		// Pull in whichever KS owns this KS's *source file* — i.e. the
		// structural parent in the home-ops cross-tree pattern where a
		// leaf KS in apps/base/ is registered by a parent KS rendering
		// apps/main/. Use ownersOf so the parent (longest-prefix match
		// for the source file) gets included; also append ancestorsOf
		// so deeper chains of meta-Kustomizations get pulled in too.
		// queue[head] itself owns its OWN spec.path, not its source
		// file, so the parent never collides with the KS we're walking.
		for _, owner := range owners.ownersOf(src) {
			if owner == queue[head] {
				continue
			}
			enqueue(owner)
		}
		for _, ancestor := range owners.ancestorsOf(src) {
			enqueue(ancestor)
		}
	}
	f.keep = keep

	f.keepByName = make(map[nameKey]struct{}, len(keep))
	for id := range keep {
		if id.Namespace == "" {
			f.keepByName[nameKey{id.Kind, id.Name}] = struct{}{}
		}
	}
}

// Size returns the number of resources in the resolved keep set.
func (f *Filter) Size() int {
	if f == nil {
		return 0
	}
	return len(f.keep)
}

// KeepNames returns the resolved keep-set as sorted strings for logs.
func (f *Filter) KeepNames() []string {
	if f == nil || f.keep == nil {
		return nil
	}
	out := make([]string, 0, len(f.keep))
	for id := range f.keep {
		out = append(out, id.String())
	}
	slices.Sort(out)
	return out
}

// KeepNamespaces returns the namespaces represented in the keep-set,
// or nil when no scope can be derived (disabled, empty, or
// cluster-scoped only).
func (f *Filter) KeepNamespaces() map[string]struct{} {
	if f == nil || f.keep == nil {
		return nil
	}
	out := make(map[string]struct{})
	for id := range f.keep {
		if id.Namespace != "" {
			out[id.Namespace] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ObjectLister is the Store surface filter resolution needs.
type ObjectLister interface {
	GetObject(manifest.NamedResource) manifest.BaseManifest
	ListObjects(kind string) []manifest.BaseManifest
}

// transitiveDeps returns the references id needs to render — chart
// sources, KS sourceRef, valuesFrom. dependsOn is intentionally
// excluded: it's a reconcile-ordering signal in real Flux, not a
// content dependency, so it adds nothing to an offline render.
// Skipped resources still get marked Ready by their controllers, so
// downstream depwait completes naturally.
func transitiveDeps(objs ObjectLister, id manifest.NamedResource) []manifest.NamedResource {
	switch id.Kind {
	case manifest.KindHelmRelease:
		hr, _ := objs.GetObject(id).(*manifest.HelmRelease)
		if hr == nil {
			return nil
		}
		out := []manifest.NamedResource{{
			Kind: hr.Chart.RepoKind, Namespace: hr.Chart.RepoNamespace, Name: hr.Chart.RepoName,
		}}
		for _, ref := range hr.ValuesFrom {
			out = append(out, manifest.NamedResource{
				Kind: ref.Kind, Namespace: hr.Namespace, Name: ref.Name,
			})
		}
		return out

	case manifest.KindKustomization:
		ks, _ := objs.GetObject(id).(*manifest.Kustomization)
		if ks == nil {
			return nil
		}
		if ks.SourceKind == "" || ks.SourceName == "" {
			return nil
		}
		return []manifest.NamedResource{{
			Kind: ks.SourceKind, Namespace: ks.SourceNamespace, Name: ks.SourceName,
		}}
	}
	return nil
}
