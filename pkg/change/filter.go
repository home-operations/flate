package change

import (
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// Filter answers "should I reconcile this resource?" by checking
// against a keep-set resolved from a file-level diff. Construct via
// NewFilter — the zero value is the "no filtering" sentinel and
// returns true from ShouldReconcile for every id.
//
// The keep set is built once at construction and then extended at
// runtime by Add: a parent KS already in the keep set may render and
// emit child KS / HR resources that the file-walk discovery phase
// couldn't see (kustomize component + replacement patterns generate
// Flux Kustomizations on the fly). Those children inherit the
// parent's in-scope-ness because the parent only reconciled by
// passing the filter in the first place. See issue #204.
type Filter struct {
	changes     *Set
	sourceFiles map[manifest.NamedResource]string
	repoRoot    string

	// objs is captured from NewFilter so runtime Add() can walk
	// transitiveDeps without the caller re-supplying it. Set once
	// at construction; never mutated.
	objs ObjectLister

	// OnAdd, when non-nil, fires for every id newly added to the
	// keep set by Add (including transitive-dep recursion). The
	// orchestrator wires this to refire EventObjectAdded for source-
	// kind ids whose listener already short-circuited via PreGate
	// before the consuming KS joined keep. Issue #260.
	//
	// Set BEFORE controllers start. The Filter calls OnAdd outside
	// its internal lock so callbacks are free to take other locks.
	OnAdd func(manifest.NamedResource)

	// mu guards keep + keepByName + primary for runtime Add();
	// resolve() runs once during construction before the controllers
	// start, so it doesn't need to hold the lock.
	mu   sync.RWMutex
	keep map[manifest.NamedResource]struct{}

	// primary is the subset of keep whose render output likely differs
	// from baseline — file-change owners, their siblings, and runtime
	// adds emitted by another primary entry. Ancestor-only entries
	// (kept so parent patches apply per #58) are explicitly NOT
	// marked primary so their render emissions don't cascade-include
	// every file-loaded sibling via AddEmitted.
	primary map[manifest.NamedResource]struct{}

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
		objs:        objs,
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
// (true when filtering is disabled). The (Kind, Name) fallback below
// bridges parent-Kustomization targetNamespace inheritance: a
// resource loaded from disk with no namespace (entry kept with
// Namespace="") and queried later with the inherited namespace
// (lookup with Namespace=X) refers to the same logical resource.
//
// The fallback ONLY indexes keep-entries whose Namespace is empty —
// so a fully-namespaced lookup never matches an unrelated fully-
// namespaced entry that happens to share (Kind, Name). Without this
// asymmetry the keep set would silently expand across namespaces
// (e.g. a kept `Kustomization/cluster-infra/external-secrets`
// dragging an unrelated `Kustomization/database/external-secrets`
// into scope on name match alone).
func (f *Filter) ShouldReconcile(id manifest.NamedResource) bool {
	if !f.Enabled() {
		return true
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if _, ok := f.keep[id]; ok {
		return true
	}
	if _, ok := f.keepByName[nameKey{id.Kind, id.Name}]; ok {
		return true
	}
	return false
}

// AddEmitted extends the keep set with child when emitter is a
// "primary" keep entry — i.e. one whose own render output differs
// from baseline (its source file changed, it's a sibling of a
// changed file under a shared owner KS, or it was itself emitted
// by another primary parent at runtime). Ancestor-only emitters
// (kept so their patches/substituteFrom apply to descendants per
// #58) DON'T propagate keep to file-loaded children: their render
// output for unrelated siblings is identical to baseline, and
// cascading those siblings through the keep set turns a one-file
// change into a full-tree reconcile.
//
// child inherits the emitter's primacy: AddEmitted walks
// transitiveDeps recursively for sourceRef / chartRef / valuesFrom
// (issue #260) and marks every newly-added entry primary so their
// own future emissions cascade correctly.
//
// Used by the KS controller when a parent KS in the keep set
// renders and emits id as a child. Covers the kustomize
// component+replacement pattern (parent emits render-only per-app
// Kustomization from a CM-driven replacement, see #204) AND the
// patch-propagation chain (primary parent emits patched file-loaded
// child whose render-with-new-patches differs from baseline).
//
// Newly-added ids are passed to OnAdd (when configured) so the
// orchestrator can refire dependent listeners (e.g. retrigger the
// source controller's fetch for a source whose PreGate-skip happened
// before its consumer joined keep).
//
// Ordering contract for embedders: call AddEmitted(parent, child)
// BEFORE the emitting Store.AddObject(child). Store events fire
// synchronously on the calling goroutine, so the controller's
// listener invokes PreGate (and thus ShouldReconcile) inside that
// AddObject — if AddEmitted ran after, the listener sees the old
// keep set and short-circuits to Ready/"unchanged".
//
// No-op when the filter is disabled. Safe for concurrent use.
func (f *Filter) AddEmitted(emitter, child manifest.NamedResource) {
	if !f.Enabled() {
		return
	}
	f.mu.RLock()
	_, primaryEmitter := f.primary[emitter]
	f.mu.RUnlock()
	if !primaryEmitter {
		return
	}
	f.Add(child)
}

// Add unconditionally extends the keep set with id (and its
// transitive sourceRef/chartRef/valuesFrom deps) at runtime, marking
// every newly-inserted entry primary. Callers that need the
// "skip when emitter is ancestor-only" gating should use AddEmitted
// instead.
//
// No-op when the filter is disabled. Safe for concurrent use.
func (f *Filter) Add(id manifest.NamedResource) {
	if !f.Enabled() {
		return
	}
	added := f.addRecursive(id)
	if f.OnAdd == nil || len(added) == 0 {
		return
	}
	// Call OnAdd outside the lock — callbacks may take other locks
	// (e.g. the Store's mu via Refire) and we don't want to invert
	// lock order with concurrent ShouldReconcile readers.
	for _, newID := range added {
		f.OnAdd(newID)
	}
}

// addRecursive adds id (and transitive deps) to keep AND primary,
// returning the list of ids that were newly added (so the caller
// can dispatch OnAdd notifications outside the lock). Holds mu for
// the full graph walk so the recursion sees a coherent keep
// snapshot.
//
// Every entry inserted by this path is primary: runtime adds happen
// when a primary parent emits a child, so the child inherits primacy
// and any future emissions IT produces also propagate. Ancestor-only
// entries are inserted directly into f.keep (NOT f.primary) by
// resolve()'s own walks, never via addRecursive.
func (f *Filter) addRecursive(id manifest.NamedResource) []manifest.NamedResource {
	f.mu.Lock()
	defer f.mu.Unlock()

	var added []manifest.NamedResource
	stack := []manifest.NamedResource{id}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		_, alreadyKeep := f.keep[cur]
		_, alreadyPrimary := f.primary[cur]
		if alreadyKeep && alreadyPrimary {
			continue
		}
		if !alreadyKeep {
			f.keep[cur] = struct{}{}
			if cur.Namespace == "" {
				f.keepByName[nameKey{cur.Kind, cur.Name}] = struct{}{}
			}
			added = append(added, cur)
		}
		// Promote ancestor-only entries to primary when a runtime
		// add reaches them: an ancestor that ALSO becomes the emit-
		// chain target of a primary parent is itself rendering a
		// changed graph, so its further emissions should cascade.
		f.primary[cur] = struct{}{}
		// Transitive walk: a runtime-added KS / HR pulls its
		// sourceRef / chartRef / valuesFrom into keep with it.
		// objs may be nil for tests that construct a Filter without
		// an ObjectLister; in that case the walk is a no-op (the
		// resolve() pre-build covered the initial graph already).
		if f.objs == nil {
			continue
		}
		for _, dep := range transitiveDeps(f.objs, cur) {
			if _, ok := f.primary[dep]; !ok {
				stack = append(stack, dep)
			}
		}
	}
	return added
}

func (f *Filter) resolve(objs ObjectLister) {
	keep := make(map[manifest.NamedResource]struct{})
	primary := make(map[manifest.NamedResource]struct{})
	var queue []manifest.NamedResource
	enqueuePrimary := func(id manifest.NamedResource) {
		if _, isPrimary := primary[id]; isPrimary {
			return
		}
		primary[id] = struct{}{}
		if _, seen := keep[id]; !seen {
			keep[id] = struct{}{}
			queue = append(queue, id)
		}
	}
	// enqueueAncestor adds id to keep without marking it primary.
	// Ancestors render so their patches/substituteFrom apply to
	// descendants (#58), but their render output for unrelated
	// sibling children is identical to baseline — so AddEmitted
	// must NOT keep-add those siblings on the cascade path. The
	// primary/non-primary distinction is the gate.
	enqueueAncestor := func(id manifest.NamedResource) {
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
			enqueuePrimary(owner)
		}
		// Also include ancestor/meta Kustomizations whose render
		// mutates the leaf owner's spec — parent-injected spec.patches
		// and postBuild.substituteFrom land at parent-render time, so
		// in changed-only mode the parent has to run too. Ancestors
		// are NOT added to ownersHit, so the sibling-pull-in below
		// doesn't drag in everything else they own. See #58. They're
		// also NOT marked primary so AddEmitted skips their unrelated
		// emitted children — preventing the keep cascade where a
		// one-file change pulls in the entire cluster.
		for _, ancestor := range owners.ancestorsOf(file) {
			enqueueAncestor(ancestor)
		}
	}
	for id, src := range f.sourceFiles {
		if f.changes.Contains(src) {
			enqueuePrimary(id)
			continue
		}
		// Pull in every sibling resource that shares an affected owner.
		for _, owner := range owners.ownersOf(src) {
			if _, hit := ownersHit[owner]; hit {
				enqueuePrimary(id)
				break
			}
		}
	}

	for head := 0; head < len(queue); head++ {
		_, headPrimary := primary[queue[head]]
		for _, d := range transitiveDeps(objs, queue[head]) {
			if headPrimary {
				enqueuePrimary(d)
			} else {
				enqueueAncestor(d)
			}
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
		// Structural parents are ancestor-only (their unrelated children
		// shouldn't cascade into keep — same rationale as ancestorsOf).
		for _, owner := range owners.ownersOf(src) {
			if owner == queue[head] {
				continue
			}
			enqueueAncestor(owner)
		}
		for _, ancestor := range owners.ancestorsOf(src) {
			enqueueAncestor(ancestor)
		}
	}
	f.keep = keep
	f.primary = primary

	// Index ONLY empty-namespace keep entries by (Kind, Name) — see
	// ShouldReconcile's doc for the asymmetry rationale. Indexing
	// every keep entry (regardless of namespace) would let one kept
	// resource silently scope-in every same-(Kind, Name) resource in
	// other namespaces.
	f.keepByName = make(map[nameKey]struct{})
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
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.keep)
}

// KeepNames returns the resolved keep-set as sorted strings for logs.
func (f *Filter) KeepNames() []string {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.keep == nil {
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
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.keep == nil {
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
