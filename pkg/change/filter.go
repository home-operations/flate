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
	changes     *Set
	sourceFiles map[manifest.NamedResource]string
	repoRoot    string

	// consumerRefs maps each consumer (HelmRelease / Kustomization) to
	// the source resources it references — supplied by discovery because
	// HelmReleases are absent from the Store at resolve() time under
	// render-driven discovery. resolve() inverts it (source -> consumers)
	// to drive the reverse edge. Nil disables reverse propagation.
	consumerRefs map[manifest.NamedResource][]manifest.NamedResource

	// objs is captured from NewFilter so runtime AddEmitted can
	// walk transitiveDeps without the caller re-supplying it. The
	// pointer is set once at construction and never reassigned —
	// but the underlying *store.Store IS mutated post-Bootstrap by
	// controllers, so reads through objs.GetObject /
	// objs.ListObjects must take the Store's own RLock (which
	// transitiveDeps does internally).
	objs ObjectLister

	// componentCache, when non-nil, memoizes
	// manifest.ReadKustomizeComponents reads across Filter resolution
	// and other Bootstrap consumers (loader.KSPathPrefixes, discovery's
	// parent-index passes). The orchestrator wires one cache per
	// Bootstrap so the kustomization.yaml at each spec.path is read
	// once. Supplied via the cache argument of NewFilterWithCache; the
	// resolve()-time buildOwnership reads it once and stops using it
	// after. nil is fine — buildOwnership falls back to a per-call
	// cache. See manifest.ComponentCache.
	componentCache *manifest.ComponentCache

	// OnAdd, when non-nil, fires for every id newly added to the
	// keep set by AddEmitted / Add (including transitive-dep
	// recursion). The orchestrator wires this to refire
	// EventObjectAdded for source-kind ids whose listener already
	// short-circuited via PreGate before the consuming KS joined
	// keep. Issue #260.
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

	// producersByID and producersByName map file-backed data resources
	// (ConfigMap/Secret) to the Flux Kustomizations whose spec.path
	// renders them. Two maps mirror the keepByName empty-namespace
	// bridge: DiscoveryOnly indexes the raw on-disk ConfigMap before
	// kustomize's namespace directive has rendered, but consumers wait
	// on the namespaced form. Built once during resolve() and read-only
	// after — so the BFS-local lookup in resolve() and the lock-held
	// runtime lookups can both share the same maps without copying.
	// See #418.
	producersByID   map[manifest.NamedResource][]manifest.NamedResource
	producersByName map[nameKey][]manifest.NamedResource
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
//  5. Reverse edge: when a changed file IS itself a source resource
//     (OCIRepository / HelmRepository / GitRepository / Bucket /
//     ExternalArtifact / HelmChart), every HelmRelease and
//     Kustomization that references it is kept primary so its render
//     re-runs against the new source spec — e.g. an OCIRepository
//     spec.ref.tag bump pulls a different chart version. Only fires
//     for a source whose OWN file changed (not one merely pulled in as
//     a forward dep of step 4), so a single HR edit can't reverse-
//     cascade into every sibling sharing its source.
func NewFilter(changes *Set, sourceFiles map[manifest.NamedResource]string, repoRoot string, objs ObjectLister) *Filter {
	return NewFilterWithCache(changes, sourceFiles, repoRoot, objs, nil, nil)
}

// NewFilterWithCache is NewFilter with a shared
// *manifest.ComponentCache threaded into resolve()'s buildOwnership
// call. The orchestrator instantiates one cache per Bootstrap and
// passes the same pointer here and to the loader so a single
// kustomization.yaml's `components:` field is read once across the
// entire Bootstrap (vs. once per consumer: loader's parent index,
// discovery's orphan promotion, and the Filter's ownership index all
// previously re-read disk). Pass nil cache to fall back to a per-resolve
// local cache.
//
// consumerRefs maps each consumer (HelmRelease / Kustomization) to the
// source resources it references; discovery supplies it because
// HelmReleases are absent from the Store at resolve() time. It drives
// the reverse edge (step 5 above). Pass nil to disable reverse
// propagation (the default for tests that don't exercise it).
func NewFilterWithCache(changes *Set, sourceFiles map[manifest.NamedResource]string, repoRoot string, objs ObjectLister, cache *manifest.ComponentCache, consumerRefs map[manifest.NamedResource][]manifest.NamedResource) *Filter {
	f := &Filter{
		changes:        changes,
		sourceFiles:    sourceFiles,
		repoRoot:       repoRoot,
		objs:           objs,
		componentCache: cache,
		consumerRefs:   consumerRefs,
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

// ProducersFor returns Flux Kustomizations that render the file-backed
// data resource id. Populated only for Enabled filters; used by
// controllers to add ordering edges for data deps that arrive via
// another KS's render — e.g. a postBuild.substituteFrom ConfigMap
// rendered by an unchanged producer KS in changed-only mode. See
// #418.
//
// Returns a copy — callers append-without-aliasing the filter's
// internal state.
func (f *Filter) ProducersFor(id manifest.NamedResource) []manifest.NamedResource {
	if !f.Enabled() {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return slices.Clone(f.producersFor(id))
}

// producersFor returns Flux Kustomizations that render the file-backed
// data resource id. Read-only after resolve(); caller must already
// hold f.mu (when called from runtime paths) or be inside resolve()
// (read-only construction).
//
// The id-namespace asymmetry mirrors ShouldReconcile / keepByName:
// the byName fallback fires only when the lookup id IS namespaced,
// so a fully-namespaced producer entry can't silently match an
// unrelated fully-namespaced consumer that happens to share
// (Kind, Name).
func (f *Filter) producersFor(id manifest.NamedResource) []manifest.NamedResource {
	if f == nil {
		return nil
	}
	out := f.producersByID[id]
	if id.Namespace != "" {
		out = appendUniqueProducers(out, f.producersByName[nameKey{id.Kind, id.Name}])
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
// child inherits the emitter's primacy: AddEmitted walks the child's
// sourceRef / chartRef / valuesFrom edges recursively (issue #260) and
// marks every newly-added entry primary so their own future emissions
// cascade correctly. Those edges are read straight off the child
// manifest (transitiveDepsOf), NOT via a Store lookup — a render-emitted
// child is kept here BEFORE its Store.AddObject (the ordering contract
// below), so it isn't yet visible to the Store, and its namespace-
// stamped chart source (e.g. a shared OCIRepository) would otherwise go
// unkept and unfetched.
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
func (f *Filter) AddEmitted(emitter manifest.NamedResource, child manifest.BaseManifest) {
	if !f.Enabled() {
		return
	}
	// Gate the primacy check and the keep-set mutation under a
	// single lock acquisition so a concurrent Add or AddEmitted
	// that promotes `emitter` to primary between our gate read and
	// the recurse can't cause `child` to be silently dropped.
	// addEmittedLocked reads primary and (when gated through) does
	// the addRecursive walk under the same WLock.
	added := f.addEmittedLocked(emitter, child)
	if f.OnAdd == nil || len(added) == 0 {
		return
	}
	for _, newID := range added {
		f.OnAdd(newID)
	}
}

// addEmittedLocked is the WLock-held body of AddEmitted: it reads
// primary[emitter] and the recurse for child without dropping the
// lock between them. Returns the slice of newly-keep'd ids so the
// caller can fire OnAdd outside the lock.
func (f *Filter) addEmittedLocked(emitter manifest.NamedResource, child manifest.BaseManifest) []manifest.NamedResource {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, primaryEmitter := f.primary[emitter]; !primaryEmitter {
		return nil
	}
	added := f.addRecursiveLocked(child.Named())
	// addRecursiveLocked resolves deps via the Store, but a render-emitted
	// child is kept here BEFORE its Store.AddObject (the ordering contract
	// above), so the Store can't see it yet. Walk the child's own
	// chartRef / sourceRef / valuesFrom straight off the manifest so a
	// shared, namespace-stamped source (e.g. an OCIRepository every
	// HelmRelease chartRefs) still joins keep — and OnAdd refires the
	// fetch its listener PreGate-skipped before the consumer joined keep.
	for _, dep := range transitiveDepsOf(child) {
		added = append(added, f.addRecursiveLocked(dep)...)
	}
	return added
}

// addUngated unconditionally extends the keep set with id (and its
// transitive sourceRef/chartRef/valuesFrom deps) at runtime, marking
// every newly-inserted entry primary.
//
// Internal-only: production code MUST use AddEmitted so the
// primary-emitter gate prevents the ancestor-cascade failure mode.
// Test scaffolding that needs to seed an entry without simulating a
// render emission can call this directly from within the package.
//
// No-op when the filter is disabled. Safe for concurrent use.
func (f *Filter) addUngated(id manifest.NamedResource) {
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
	return f.addRecursiveLocked(id)
}

// markKept records id in the keep set, mirroring it into keepByName
// only for empty-namespace ids (see ShouldReconcile for the asymmetry
// rationale). Caller MUST hold f.mu.Lock().
func (f *Filter) markKept(id manifest.NamedResource) {
	f.keep[id] = struct{}{}
	if id.Namespace == "" {
		f.keepByName[nameKey{id.Kind, id.Name}] = struct{}{}
	}
}

// addRecursiveLocked is the lock-free body of addRecursive — the
// caller MUST hold f.mu.Lock(). Pulled out so AddEmitted can do its
// gate-and-recurse atomically under a single lock acquisition.
func (f *Filter) addRecursiveLocked(id manifest.NamedResource) []manifest.NamedResource {
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
		if f.objs == nil {
			continue
		}
		for _, dep := range transitiveDeps(f.objs, cur) {
			if _, ok := f.primary[dep]; !ok {
				stack = append(stack, dep)
			}
			// Producers of a runtime-discovered substituteFrom CM
			// must reconcile so the CM materializes, but they stay
			// dependency-only — promoting them to primary would
			// re-introduce the #204 keep cascade (an unchanged
			// producer's render output for unrelated siblings is
			// identical to baseline). See #418.
			added = append(added, f.addDependencyOnlyRecursiveLocked(f.producersFor(dep)...)...)
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
func (f *Filter) addDependencyOnlyRecursiveLocked(ids ...manifest.NamedResource) []manifest.NamedResource {
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
		if f.objs == nil {
			continue
		}
		for _, dep := range transitiveDeps(f.objs, cur) {
			stack = append(stack, dep)
			for _, producer := range f.producersFor(dep) {
				if producer == cur {
					continue
				}
				stack = append(stack, producer)
			}
		}
	}
	return added
}

func (f *Filter) resolve(objs ObjectLister) {
	// Capacity hint: the final keep set is roughly proportional to the
	// source-files map; prime both maps and the BFS queue at that size
	// to avoid repeated rehash/realloc on the initial resolve scan.
	hint := len(f.sourceFiles)
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
		// dequeue time to decide whether transitiveDeps propagate
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

	owners := buildOwnership(objs, f.repoRoot, f.componentCache)
	f.producersByID, f.producersByName = buildProducerIndex(f.sourceFiles, owners)
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

	// Reverse edge: when a changed file IS the source a HelmRelease or
	// Kustomization references, that consumer must re-render — the
	// forward transitiveDeps walk only goes consumer→source, so a
	// centralized source (an OCIRepository in its own Kustomization tree,
	// separate from the HelmReleases that chartRef it) would otherwise
	// leave its consumers out of the keep set entirely. consumerRefs is
	// the only view of these edges at resolve() time: HelmReleases are
	// absent from the Store under render-driven discovery. Iterating it
	// and testing the referenced source's file directly also gives the
	// guard for free — a consumer is pulled only when a source it
	// references actually changed on disk, never when its own edit
	// merely drags the source in as a forward dep.
	//
	// The consumer is primary: its render (an HR's helm template against
	// the new chart version) differs from baseline, and its namespace
	// must enter KeepNamespaces for diff scoping. Its owner Kustomization
	// is ancestor-only — it must render to emit the HR into the Store,
	// but its own kustomize output is unchanged (the HR manifest is
	// identical; only the downstream helm render moved), so it must not
	// cascade unrelated siblings into keep (the #58/#204 rationale).
	for consumer, sources := range f.consumerRefs {
		for _, src := range sources {
			sf, ok := f.sourceFiles[src]
			if !ok || !f.changes.Contains(sf) {
				continue
			}
			enqueuePrimary(consumer)
			if cf, ok := f.sourceFiles[consumer]; ok {
				for _, owner := range owners.ownersOf(cf) {
					enqueueAncestor(owner)
				}
				for _, ancestor := range owners.ancestorsOf(cf) {
					enqueueAncestor(ancestor)
				}
			}
			break
		}
	}

	// NOTE: queue grows inside the loop via enqueuePrimary/enqueueAncestor.
	// Re-evaluate len(queue) each iteration; do NOT convert to `range queue`
	// (intrange linter must not auto-fix this — see //nolint above).
	for head := 0; head < len(queue); head++ { //nolint:intrange // queue grows during iteration
		_, headPrimary := primary[queue[head]]
		// transitiveDeps reads the consumer from the Store; that covers
		// Kustomizations but NOT HelmReleases, which are render-driven and
		// absent from the Store at resolve() time (see the reverse-edge note
		// above). consumerRefs is the discovery-supplied view of those
		// consumer→source edges (chartRef / chart.spec.sourceRef / valuesFrom),
		// keyed identically to the keep seed. Union it so a changed HelmRelease
		// still pulls its chart source into keep; without it a one-file HR edit
		// would leave its OCIRepository/HelmRepository unfetched.
		deps := transitiveDeps(objs, queue[head])
		deps = append(deps, f.consumerRefs[queue[head]]...)
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
			// Read producersByID/producersByName directly via the
			// unlocked producersFor: resolve() runs pre-publication
			// (f.keep / f.primary not yet set), so the public
			// ProducersFor's RLock contract is not established here.
			for _, producer := range f.producersFor(d) {
				if producer == queue[head] {
					continue
				}
				enqueueAncestor(producer)
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

// buildProducerIndex maps file-backed data resources to the
// Kustomization(s) that render their files. Exact id matches cover
// namespaced resources and finalized generators; the name map covers
// the common kustomize-namespace case where DiscoveryOnly indexed a
// raw ConfigMap/Secret with namespace="" but the consuming KS waits
// on the rendered namespace.
//
// Indexing by name only for empty-namespace ids mirrors keepByName's
// asymmetric rule (see ShouldReconcile / keepByName comments) —
// without it, a producer of ConfigMap/cluster-infra/shared would
// silently match a ConfigMap/database/shared consumer lookup. See
// #418.
func buildProducerIndex(sourceFiles map[manifest.NamedResource]string, owners ownershipIndex) (map[manifest.NamedResource][]manifest.NamedResource, map[nameKey][]manifest.NamedResource) {
	byID := make(map[manifest.NamedResource][]manifest.NamedResource)
	byName := make(map[nameKey][]manifest.NamedResource)
	for id, file := range sourceFiles {
		if id.Kind != manifest.KindConfigMap && id.Kind != manifest.KindSecret {
			continue
		}
		producers := owners.ownersOf(file)
		if len(producers) == 0 {
			continue
		}
		byID[id] = appendUniqueProducers(byID[id], producers)
		if id.Namespace == "" {
			key := nameKey{id.Kind, id.Name}
			byName[key] = appendUniqueProducers(byName[key], producers)
		}
	}
	return byID, byName
}

// appendUniqueProducers extends dst with the producer ids in src,
// deduping against dst. Used during producer-index construction and
// at producersFor merge time; the slices are short (typically a
// single owner KS per data file) so the linear scan is cheaper than
// a map intermediary.
func appendUniqueProducers(dst []manifest.NamedResource, src []manifest.NamedResource) []manifest.NamedResource {
	for _, id := range src {
		if !slices.Contains(dst, id) {
			dst = append(dst, id)
		}
	}
	return dst
}

// transitiveDeps returns the references the resource id needs to render,
// resolved from the Store. Thin wrapper over transitiveDepsOf — see it
// for the edge set. Returns nil when id isn't in the Store (a render-
// emitted child not yet AddObject'd); callers that hold the manifest
// should use transitiveDepsOf directly so those deps aren't missed.
func transitiveDeps(objs ObjectLister, id manifest.NamedResource) []manifest.NamedResource {
	return transitiveDepsOf(objs.GetObject(id))
}

// transitiveDepsOf returns the references obj needs to render — chart
// sources, KS sourceRef, valuesFrom, and non-Optional
// postBuild.substituteFrom ConfigMaps. Reading straight off the manifest
// (rather than a Store id lookup) lets the runtime keep-extension resolve
// a render-emitted child's deps BEFORE it lands in the Store, where the
// child's namespace-stamped chart source is the authority. dependsOn is
// intentionally excluded: it's a reconcile-ordering signal in real Flux,
// not a content dependency, so it adds nothing to an offline render.
// Skipped resources still get marked Ready by their controllers, so
// downstream depwait completes naturally.
func transitiveDepsOf(obj manifest.BaseManifest) []manifest.NamedResource {
	switch o := obj.(type) {
	case *manifest.HelmRelease:
		out := []manifest.NamedResource{{
			Kind: o.Chart.RepoKind, Namespace: o.Chart.RepoNamespace, Name: o.Chart.RepoName,
		}}
		for _, ref := range o.ValuesFrom {
			out = append(out, manifest.NamedResource{
				Kind: ref.Kind, Namespace: o.Namespace, Name: ref.Name,
			})
		}
		return out

	case *manifest.Kustomization:
		var out []manifest.NamedResource
		if o.SourceKind != "" && o.SourceName != "" {
			out = append(out, manifest.NamedResource{
				Kind: o.SourceKind, Namespace: o.SourceNamespace, Name: o.SourceName,
			})
		}
		for _, ref := range o.PostBuildSubstituteFrom {
			// Shared with collectDeps in pkg/controllers/kustomization via
			// manifest.IsHardConfigMapEdge — keep-set membership and reconcile
			// ordering MUST agree on which substituteFrom refs are hard edges
			// (see the predicate's doc + #418).
			if !manifest.IsHardConfigMapEdge(ref) {
				continue
			}
			out = append(out, manifest.NamedResource{
				Kind: ref.Kind, Namespace: o.Namespace, Name: ref.Name,
			})
		}
		return out

	case *manifest.HelmChartSource:
		// A HelmRelease chartRef pointing at a HelmChart CRD lands the
		// HelmChart in the BFS via the HR's RepoKind=KindHelmChart edge
		// above. The chart's actual bytes come from the HelmChart's own
		// sourceRef (OCIRepository/HelmRepository/GitRepository/Bucket)
		// — follow that edge so changed-only mode keeps the backing
		// source alive. Without this, the HelmChart sits in keep but
		// the source artifact is PreGate-skipped and render fails
		// "artifact not found."
		if o.SourceRef.Name == "" {
			return nil
		}
		return []manifest.NamedResource{{
			Kind: o.SourceRef.Kind, Namespace: o.Namespace, Name: o.SourceRef.Name,
		}}
	}
	return nil
}
