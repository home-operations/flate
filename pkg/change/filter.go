package change

import (
	"log/slog"
	"path/filepath"
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

	// Runtime uses membership; construction also tracks admission strength.
	primary map[manifest.NamedResource]uint8

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
	f.primary[id] = 1
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
		f.primary[cur] = 1
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
	const (
		primaryAdmission uint8 = 1 << iota
		changeAdmission
		ownerAdmission
	)
	objs := f.objs
	var records map[manifest.NamedResource]manifest.DiscoveryRecord
	if f.index != nil {
		records = f.index.Records
	}
	hint := len(records)
	keep := make(map[manifest.NamedResource]struct{}, hint)
	primary := make(map[manifest.NamedResource]uint8, hint)
	queue := make([]manifest.NamedResource, 0, hint)
	enqueuePrimary := func(id manifest.NamedResource, changed bool) {
		state := primary[id] | primaryAdmission
		if changed {
			state |= changeAdmission
		}
		if state == primary[id] {
			return
		}
		primary[id] = state
		keep[id] = struct{}{}
		queue = append(queue, id)
	}
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
	changed := func(record manifest.DiscoveryRecord) bool {
		return f.changes.Contains(record.File) || slices.ContainsFunc(record.Inputs, f.changes.Contains)
	}

	for _, file := range f.changes.Paths() {
		for _, owner := range ownersOf(file).owners {
			if record, _ := f.index.Lookup(owner); record.Pinned {
				continue
			}
			enqueuePrimary(owner, true)
			primary[owner] |= ownerAdmission
		}
		for _, ancestor := range ownersOf(file).ancestors {
			if record, _ := f.index.Lookup(ancestor); record.Pinned {
				continue
			}
			enqueueAncestor(ancestor)
		}
	}
	changedInputs := len(f.changes.inputs) != 0
	for id, record := range f.index.Files() {
		if changed(record) {
			changedInputs = changedInputs || id.KindInfo().Data
			enqueuePrimary(id, true)
			continue
		}
		for _, owner := range ownersOf(record.File).owners {
			if primary[owner]&ownerAdmission != 0 {
				enqueuePrimary(id, true)
				break
			}
		}
	}

	type reverseEdge struct{ source, consumer manifest.NamedResource }
	var edgeBuffer [32]reverseEdge
	edges := edgeBuffer[:0]
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
				unnamespaced := src
				unnamespaced.Namespace = ""
				_, oneSided = f.changes.inputs[unnamespaced]
			}
			if ok && changed(sr) || oneSided {
				enqueuePrimary(src, true)
				edges = append(edges, reverseEdge{src, consumer})
			}
			for _, production := range sr.Productions {
				if production.Declaration == (manifest.NamedResource{}) || !filepath.IsLocal(production.File) {
					continue
				}
				if f.changes.Contains(production.File) {
					enqueuePrimary(production.Declaration, true)
				}
				edges = append(edges, reverseEdge{production.Declaration, consumer})
			}
		}
	}
	slices.SortFunc(edges, func(a, b reverseEdge) int { return a.source.Compare(b.source) })

	// The queue grows when an admission gains primary or change-affecting strength.
	for head := 0; head < len(queue); head++ { //nolint:intrange // queue grows during iteration
		id := queue[head]
		headPrimary := primary[id]&primaryAdmission != 0
		headChanged := primary[id]&changeAdmission != 0
		if headChanged {
			start, _ := slices.BinarySearchFunc(edges, id, func(edge reverseEdge, id manifest.NamedResource) int {
				return edge.source.Compare(id)
			})
			for _, edge := range edges[start:] {
				if edge.source != id {
					break
				}
				enqueuePrimary(edge.consumer, true)
				record, _ := f.index.Lookup(edge.consumer)
				enqueueAncestorChain(record.File, manifest.NamedResource{})
			}
		}
		if headPrimary {
			for _, child := range acceptedChildren(id, objs) {
				enqueuePrimary(child.Obj.Named(), headChanged)
			}
		}
		var refs [8]manifest.NamedResource
		record, _ := f.index.Lookup(id)
		deps := append(refs[:0], record.Refs...)
		if objs != nil {
			deps = appendReferences(deps, objs.GetObject(id), false)
		}
		for _, dep := range deps {
			if headPrimary {
				enqueuePrimary(dep, false)
			} else {
				enqueueAncestor(dep)
			}
			for _, producer := range f.producersFor(dep, objs) {
				if producer != id {
					enqueueAncestor(producer)
				}
			}
		}
		if record.File != "" && id.KindInfo().Parent {
			enqueueAncestorChain(record.File, id)
		}
	}
	f.keep = keep
	f.primary = primary
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
