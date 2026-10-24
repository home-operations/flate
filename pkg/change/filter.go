package change

import (
	"log/slog"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Filter answers "should I reconcile this resource?" by checking
// against a keep-set resolved from a file-level diff. Construct via
// NewFilter — the zero value is the "no filtering" sentinel and
// returns true from ShouldReconcile for every id.
//
// The keep set has two tiers:
//
//   - "keep" entries reconcile. resolve() seeds this from file
//     changes + ancestors of changed files (#58) + structural
//     parents of owner KSes (#103).
//   - "primary" is the subset whose render output likely differs
//     from baseline: file-change owners, their siblings under the
//     same owner, transitive deps walked from a primary entry, and
//     runtime entries inserted by AddEmitted from a primary
//     emitter. Ancestor-only entries are explicitly NOT primary.
//
// The keep set extends at runtime via AddEmitted when a primary
// parent KS renders and emits a child the file-walk couldn't see
// (kustomize component + replacement patterns generate Flux
// Kustomizations on the fly — see #204). Ancestor-only emitters
// don't propagate keep to file-loaded children, which prevents a
// one-file change from cascading the entire tree into keep.
type Filter struct {
	changes *Set
	index   *manifest.DiscoveryIndex
	objs    ObjectLister

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

	// keepByName: (Group, Kind, Name) presence set used as an O(1) fallback
	// when either side of a lookup has an empty namespace.
	keepByName map[nameKey]struct{}
}

type nameKey struct{ group, kind, name string }

// keyOf is the namespace-agnostic key for the keepByName fallback index.
func keyOf(id manifest.NamedResource) nameKey {
	return nameKey{id.Group, id.Kind, id.Name}
}

// NewFilter resolves changed files through an immutable discovery index, then
// expands content dependencies. Nil changes disables filtering.
func NewFilter(changes *Set, index *manifest.DiscoveryIndex, objs ObjectLister) *Filter {
	f := &Filter{changes: changes, index: index, objs: objs}
	if changes != nil {
		f.resolve()
	}
	return f
}

// Enabled reports whether change-based filtering is active.
func (f *Filter) Enabled() bool { return f != nil && f.changes != nil }

// ShouldReconcile reports whether the controller for id should do work
// (true when filtering is disabled). The (Group, Kind, Name) fallback below
// bridges parent-Kustomization targetNamespace inheritance: a
// resource loaded from disk with no namespace (entry kept with
// Namespace="") and queried later with the inherited namespace
// (lookup with Namespace=X) refers to the same logical resource.
//
// The fallback ONLY indexes keep-entries whose Namespace is empty —
// so a fully-namespaced lookup never matches an unrelated fully-
// namespaced entry that happens to share (Group, Kind, Name). Without this
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
	_, ok := f.keepByName[keyOf(id)]
	return ok
}

func (f *Filter) producersFor(id manifest.NamedResource, objs ObjectLister) []manifest.NamedResource {
	if f == nil {
		return nil
	}
	var out []manifest.NamedResource
	if objs == nil {
		return nil
	}
	for _, p := range objs.Productions(id, f.index) {
		if id.KindInfo().Discovery && p.Declaration == (manifest.NamedResource{}) {
			continue
		}
		if p.Owner != (manifest.NamedResource{}) && !slices.Contains(out, p.Owner) {
			out = append(out, p.Owner)
		}
	}
	return out
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
// child inherits the emitter's primacy. Its content references come from
// the incoming manifest because admission precedes Store.AddObject, which
// may still hold a previous child with the same identity. Stored transitive
// dependencies are traversed recursively; their required producers remain
// dependency-only.
//
// Used by the KS controller when a parent KS in the keep set
// renders and emits id as a child. Covers the kustomize
// component+replacement pattern (parent emits render-only per-app
// Kustomization from a CM-driven replacement, see #204) AND the
// patch-propagation chain (primary parent emits patched file-loaded
// child whose render-with-new-patches differs from baseline).
//
// Ordering contract for embedders: call AddEmitted(parent, child)
// BEFORE the emitting Store.AddObject(child). Store events fire
// synchronously on the calling goroutine, so the controller's
// listener invokes PreGate (and thus ShouldReconcile) inside that
// AddObject — if AddEmitted ran after, the listener sees the old
// keep set and short-circuits to Ready/"unchanged".
//
// Returns newly-kept ids after releasing the filter lock so callers can publish admission.
func (f *Filter) AddEmitted(emitter manifest.NamedResource, child manifest.BaseManifest, objs ObjectLister) []manifest.NamedResource {
	if !f.Enabled() {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, primaryEmitter := f.primary[emitter]; !primaryEmitter {
		return nil
	}
	id := child.Named()
	var added []manifest.NamedResource
	if _, kept := f.keep[id]; !kept {
		f.markKept(id)
		added = append(added, id)
	}
	f.primary[id] = struct{}{}
	for _, accepted := range acceptedChildren(id, objs) {
		added = append(added, f.addRecursiveLocked(accepted.Obj.Named(), objs)...)
	}
	var refs [8]manifest.NamedResource
	for _, dep := range appendReferences(refs[:0], child, false) {
		added = append(added, f.addRecursiveLocked(dep, objs)...)
		added = append(added, f.addDependencyOnlyRecursiveLocked(objs, f.producersFor(dep, objs)...)...)
	}
	return added
}

// markKept records id in the keep set, mirroring it into keepByName
// only for empty-namespace ids (see ShouldReconcile for the asymmetry
// rationale). Caller MUST hold f.mu.Lock().
func (f *Filter) markKept(id manifest.NamedResource) {
	f.keep[id] = struct{}{}
	if id.Namespace == "" {
		f.keepByName[keyOf(id)] = struct{}{}
	}
}

// addRecursiveLocked is the lock-free body of addRecursive — the
// caller MUST hold f.mu.Lock(). Pulled out so AddEmitted can do its
// gate-and-recurse atomically under a single lock acquisition.
func (f *Filter) addRecursiveLocked(id manifest.NamedResource, objs ObjectLister) []manifest.NamedResource {
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
			f.markKept(cur)
			added = append(added, cur)
		}
		// Promote ancestor-only entries to primary when a runtime
		// add reaches them: the primary parent's render contains
		// this entry's manifest, so the entry's spec-as-rendered
		// differs from baseline and its own emissions must cascade
		// too. This is symmetric with resolve()'s enqueuePrimary
		// upgrade path.
		f.primary[cur] = struct{}{}
		// Transitive walk: a runtime-added KS / HR pulls its
		// sourceRef / chartRef / valuesFrom into keep with it.
		// objs may be nil for tests that construct a Filter without
		// an ObjectLister; in that case the walk is a no-op (the
		// resolve() pre-build covered the initial graph already).
		if objs == nil {
			continue
		}
		for _, child := range acceptedChildren(cur, objs) {
			stack = append(stack, child.Obj.Named())
		}
		var refs [8]manifest.NamedResource
		for _, dep := range appendReferences(refs[:0], objs.GetObject(cur), false) {
			if _, ok := f.primary[dep]; !ok {
				stack = append(stack, dep)
			}
			// Producers of a runtime-discovered substituteFrom CM
			// must reconcile so the CM materializes, but they stay
			// dependency-only — promoting them to primary would
			// re-introduce the #204 keep cascade (an unchanged
			// producer's render output for unrelated siblings is
			// identical to baseline). See #418.
			added = append(added, f.addDependencyOnlyRecursiveLocked(objs, f.producersFor(dep, objs)...)...)
		}
	}
	return added
}

// addDependencyOnlyRecursiveLocked extends the keep set with ids
// (and their transitive sourceRef/chartRef/valuesFrom deps AND any
// producers of those deps) at runtime WITHOUT marking the entries
// primary. The caller MUST hold f.mu.Lock().
//
// Used for runtime substituteFrom-producer promotion: a primary
// consumer reaches a CM whose producing KS is unchanged. The
// producer must reconcile so the CM materializes, but its render
// output for unrelated siblings matches baseline — promoting it
// to primary would re-introduce the #204 keep cascade.
//
// Chained producers are walked: a producer that itself has a
// substituteFrom CM produced by another KS pulls that KS in too,
// again ancestor-only. See #418.
func (f *Filter) addDependencyOnlyRecursiveLocked(objs ObjectLister, ids ...manifest.NamedResource) []manifest.NamedResource {
	var added []manifest.NamedResource
	stack := slices.Clone(ids)
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, alreadyKeep := f.keep[cur]; alreadyKeep {
			continue
		}
		f.markKept(cur)
		added = append(added, cur)
		if objs == nil {
			continue
		}
		var refs [8]manifest.NamedResource
		for _, dep := range appendReferences(refs[:0], objs.GetObject(cur), false) {
			stack = append(stack, dep)
			for _, producer := range f.producersFor(dep, objs) {
				if producer == cur {
					continue
				}
				stack = append(stack, producer)
			}
		}
	}
	return added
}

func (f *Filter) resolve() {
	objs := f.objs
	// Capacity hint: the final keep set is roughly proportional to the
	// discovery record map; prime both maps and the BFS queue at that size
	// to avoid repeated rehash/realloc on the initial resolve scan.
	var records map[manifest.NamedResource]manifest.DiscoveryRecord
	if f.index != nil {
		records = f.index.Records
	}
	hint := len(records)
	keep := make(map[manifest.NamedResource]struct{}, hint)
	primary := make(map[manifest.NamedResource]struct{}, hint)
	queue := make([]manifest.NamedResource, 0, hint)
	enqueuePrimary := func(id manifest.NamedResource) {
		if _, isPrimary := primary[id]; isPrimary {
			return
		}
		primary[id] = struct{}{}
		keep[id] = struct{}{}
		// Always re-queue when promoting an ancestor-only entry
		// to primary: the BFS body uses the head's primacy at
		// dequeue time to decide whether dependencies propagate
		// as primary or ancestor. Without the re-queue, an entry
		// walked first as ancestor would never have its deps re-
		// walked as primary, so chained transitive deps (a future
		// chart-source-of-source or similar) silently stay ancestor-
		// only. Symmetric with addRecursive's stack push.
		queue = append(queue, id)
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

	type fileOwnership struct{ owners, ancestors []manifest.NamedResource }
	ownership := map[string]fileOwnership{}
	ownersOf := func(file string) fileOwnership {
		if cached, ok := ownership[file]; ok {
			return cached
		}
		owners, ancestors := f.index.OwnersOfFile(file)
		result := fileOwnership{owners, ancestors}
		ownership[file] = result
		return result
	}
	// enqueueAncestorChain marks the structural parent that owns file
	// (longest-prefix spec.path match) plus its meta-Kustomization
	// ancestors as ancestor-only — they must render to inject patches /
	// emit namespace-scoped sources, but their unrelated children must
	// not cascade into keep (#58/#103). self is skipped so a KS walking
	// its own source file doesn't re-enqueue itself; pass the zero id
	// when there is nothing to exclude.
	enqueueAncestorChain := func(file string, self manifest.NamedResource) {
		for _, owner := range ownersOf(file).owners {
			if owner != self {
				enqueueAncestor(owner)
			}
		}
		for _, ancestor := range ownersOf(file).ancestors {
			enqueueAncestor(ancestor)
		}
	}
	ownersHit := make(map[manifest.NamedResource]struct{})
	changed := func(record manifest.DiscoveryRecord) bool {
		return f.changes.Contains(record.File) || slices.ContainsFunc(record.Inputs, f.changes.Contains)
	}

	for _, file := range f.changes.Paths() {
		for _, owner := range ownersOf(file).owners {
			if record, _ := f.index.Lookup(owner); record.Pinned {
				continue
			}
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
		for _, ancestor := range ownersOf(file).ancestors {
			if record, _ := f.index.Lookup(ancestor); record.Pinned {
				continue
			}
			enqueueAncestor(ancestor)
		}
	}
	changedInputs := len(f.changes.inputs) != 0
	for id, record := range f.index.Files() {
		src := record.File
		if changed(record) {
			changedInputs = changedInputs || id.KindInfo().Data
			enqueuePrimary(id)
			continue
		}
		// Pull in every sibling resource that shares an affected owner.
		for _, owner := range ownersOf(src).owners {
			if _, hit := ownersHit[owner]; hit {
				enqueuePrimary(id)
				break
			}
		}
	}

	for consumer, record := range records {
		var refs [8]manifest.NamedResource
		sources := append(refs[:0], record.Refs...)
		if changedInputs && objs != nil {
			sources = appendReferences(sources, objs.GetObject(consumer), true)
		}
		for _, src := range sources {
			sr, ok := f.index.Lookup(src)
			_, oneSided := f.changes.inputs[src]
			if changedInputs && !oneSided {
				src.Namespace = ""
				_, oneSided = f.changes.inputs[src]
			}
			if (!ok || !changed(sr)) && !oneSided {
				continue
			}
			enqueuePrimary(consumer)
			enqueueAncestorChain(record.File, manifest.NamedResource{})
			break
		}
	}

	// NOTE: queue grows inside the loop via enqueuePrimary/enqueueAncestor.
	// Re-evaluate len(queue) each iteration; do NOT convert to `range queue`
	// (intrange linter must not auto-fix this — see //nolint below).
	for head := 0; head < len(queue); head++ { //nolint:intrange // queue grows during iteration
		_, headPrimary := primary[queue[head]]
		if headPrimary {
			for _, child := range acceptedChildren(queue[head], objs) {
				enqueuePrimary(child.Obj.Named())
			}
		}
		var refs [8]manifest.NamedResource
		record, _ := f.index.Lookup(queue[head])
		deps := append(refs[:0], record.Refs...)
		if objs != nil {
			deps = appendReferences(deps, objs.GetObject(queue[head]), false)
		}
		for _, d := range deps {
			if headPrimary {
				enqueuePrimary(d)
			} else {
				enqueueAncestor(d)
			}
			// If a data dependency is rendered by another Flux
			// Kustomization, that producer must run so the data
			// object materializes. Producers are ancestor-only —
			// their output is needed, but an unchanged producer
			// should NOT become a primary emitter that cascades
			// unrelated children into changed-only scope (the #204
			// cascade rationale applies). Skip the self-producer
			// case (a KS that produces its own substituteFrom CM —
			// bjw-s self-substitute pattern). See #418.
			//
			// Read discovery data owners via the unlocked producersFor:
			// resolve() runs pre-publication (f.keep / f.primary not yet set),
			// before the keep-set lock is used by concurrent readers.
			for _, producer := range f.producersFor(d, objs) {
				if producer == queue[head] {
					continue
				}
				enqueueAncestor(producer)
			}
		}
		src := record.File
		if src == "" || !queue[head].KindInfo().Parent {
			continue
		}
		enqueueAncestorChain(src, queue[head])
	}
	f.keep = keep
	f.primary = primary

	// Index ONLY empty-namespace keep entries by (Group, Kind, Name) — see
	// ShouldReconcile's doc for the asymmetry rationale. Indexing
	// every keep entry (regardless of namespace) would let one kept
	// resource silently scope-in every same-(Group, Kind, Name) resource in
	// other namespaces.
	f.keepByName = make(map[nameKey]struct{})
	for id := range keep {
		if id.Namespace == "" {
			f.keepByName[keyOf(id)] = struct{}{}
		}
	}
}

// LogValue snapshots the keep set only when the log handler needs it.
func (f *Filter) LogValue() slog.Value {
	items := f.KeepNames()
	return slog.GroupValue(slog.Int("size", len(items)), slog.Any("items", items))
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

func acceptedChildren(id manifest.NamedResource, objs ObjectLister) []store.Child {
	if objs != nil {
		if current, ok := objs.GetArtifact(id).(*store.Emission); ok {
			return current.Children
		}
	}
	return nil
}

// ObjectLister is the Store surface filter resolution needs.
type ObjectLister interface {
	GetArtifact(manifest.NamedResource) store.Artifact
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	GetObject(manifest.NamedResource) manifest.BaseManifest
	ListObjects(group, kind string) []manifest.BaseManifest
}

// Reverse admission retains discovery's scope; only named substitution inputs
// extend it. Forward traversal also follows content dependencies.
func appendReferences(dst []manifest.NamedResource, obj manifest.BaseManifest, reverse bool) []manifest.NamedResource {
	include := func(ref manifest.Reference) bool {
		return (!reverse && ref.Content) || (ref.Use == "substituteFrom" && ref.Name != "")
	}
	refs := manifest.References(obj, manifest.ReferenceRelations)
	prefixLen := len(dst)
	for ref := range refs {
		if include(ref) {
			if len(dst) == cap(dst) {
				count := prefixLen
				for next := range refs {
					if include(next) {
						count++
					}
				}
				dst = slices.Grow(dst, count-len(dst))
			}
			dst = append(dst, ref.NamedResource)
		}
	}
	return dst
}
