package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"sync"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/source/git/mirror"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// Config carries everything the orchestrator needs.
type Config struct {
	// StageCacheBytes caps the persistent kustomize stage cache. 0
	// disables eviction (unbounded growth — the GC subcommand still
	// handles age-based cleanup). The flag's expected unit is mebibytes
	// at the CLI layer; this field is bytes.
	StageCacheBytes int64

	// Path is the directory to scan for Flux objects (the scan entry
	// point — a cluster's Flux entry, which may be a subdir of RepoRoot).
	Path string
	// RepoRoot is the source root that Kustomization spec.path values
	// resolve against. Supplied explicitly by SDK consumers rendering
	// extracted trees that have no .git; the CLI defaults it to the .git
	// ancestor of Path. Empty ⇒ the .git walk (FindRepoRoot) runs,
	// preserving local behavior.
	RepoRoot string
	// SelfURLs are the remote URL(s) this tree represents, for
	// self-referential GitRepository aliasing (a cluster pulling itself).
	// Supplied explicitly by SDK consumers rendering extracted trees with
	// no .git/config; empty ⇒ the working tree's .git remotes are read.
	SelfURLs []string
	// PathOrig, when non-empty, switches every command into
	// changed-only mode: only resources whose source files differ
	// (plus the sources they reference) get reconciled.
	PathOrig string

	// HelmOptions tunes templating (skip CRDs/secrets/tests, kube
	// version, etc.).
	HelmOptions helm.Options
	// WipeSecrets controls Secret cleartext placeholders.
	WipeSecrets bool
	// AllowMissingSecrets converts source auth-secret-not-found errors
	// into skips and omits HelmRelease valuesFrom Secret/ConfigMap refs
	// that cannot materialize in the offline tree. Skipped source
	// resources mark Ready with a "skipped:" reason; omitted valuesFrom
	// refs let HelmReleases render with the remaining values.
	AllowMissingSecrets bool

	// RegistryConfig is the docker config.json used for OCI auth.
	RegistryConfig string

	// CacheDir overrides the default on-disk cache root. The default
	// follows os.UserCacheDir (XDG_CACHE_HOME on Linux, Library/Caches
	// on macOS, %LocalAppData% on Windows) with a "flate" subdir, so
	// the cache survives reboots and OS tmpfs cleanups. Falls back to
	// os.TempDir()/flate-cache only when UserCacheDir errors.
	CacheDir string
	// SourceCache, when non-nil, is shared across orchestrators. The
	// `flate diff` flow constructs two orchestrators that point at the
	// same on-disk source-cache root; they MUST share one *Cache so the
	// internal mutex serializes concurrent slot allocation. When nil a
	// per-orchestrator cache is constructed (fine for single-orchestrator
	// commands like `build` / `get`).
	SourceCache *source.Cache
	// ExternalChanges, when non-nil, supplies the file-level diff so
	// the orchestrator skips its built-in change.Detect step. The
	// filter is still built from this set + the loaded SourceFiles
	// during Bootstrap.
	ExternalChanges *change.Set

	// Concurrency caps the number of active reconcile bodies running
	// in parallel. <= 0 means unbounded (every Kustomization / HR
	// reconciles on its own goroutine). Background watch loops are
	// unaffected. Sensible default for I/O-bound work is
	// runtime.NumCPU() * 4.
	Concurrency int

	// SourceRetry tunes the bounded, classified retry applied to every
	// source fetch (Git/OCI/Bucket) on transient network failures —
	// connection resets, refused connections, dial/IO timeouts. Permanent
	// errors (bad path, missing secret, not-found) fail fast. Attempts
	// <= 1 disables retry; the CLI defaults it to 3. See
	// source.RetryConfig / source.WithRetry.
	SourceRetry source.RetryConfig

	// GitDepth caps the shallow-clone history depth for GitRepository
	// sources (both the bare mirror and the legacy clone). 0 clones full
	// history; the CLI defaults it to 1 (opt-out via --git-depth=0).
	// Commit-pinned refs always full-clone regardless. See
	// git.Fetcher.Depth.
	GitDepth int

	// HelmTemplateCacheBytes caps the in-memory helm template-output
	// cache. Repeat HRs with identical effective inputs (chart
	// fingerprint, resolved values, render options) hit the cache and
	// skip action.Install.RunWithContext — the single largest CPU +
	// allocation consumer in the codebase. <= 0 disables the cache.
	// The CLI flag `--helm-template-cache-mb` exposes this in MB
	// units; embedders pass bytes directly.
	HelmTemplateCacheBytes int64

	// HelmRenderCacheBytes caps the persistent on-disk helm template-
	// output cache (Phase 3.4a). Cross-process reuse: repeat `flate
	// build` / `flate diff` invocations against the same checkout
	// short-circuit the helm render entirely when the chart
	// fingerprint + resolved values + opts tuple hits the disk layer.
	// <= 0 disables disk caching; the in-memory layer (sized by
	// HelmTemplateCacheBytes) continues to operate independently.
	// The CLI flag `--helm-render-cache-mb` exposes this in MB
	// units; embedders pass bytes directly.
	HelmRenderCacheBytes int64
}

// Orchestrator wires controllers and drives reconciliation.
type Orchestrator struct {
	cfg       Config
	store     *store.Store
	tasks     *task.Service
	src       *sourcectrl.Controller
	ksc       *kustomization.Controller
	hrc       *helmrelease.Controller
	helm      *helm.Client
	treeCache *kustomize.TreeCache
	filter    *change.Filter

	// gitFetcher holds the typed *git.Fetcher constructed in New so
	// Run can drive Prewarm against every discovered GitRepository in
	// parallel with controller startup. The source controller already
	// holds the same fetcher wrapped behind source.Wrap; we keep a
	// direct reference because Prewarm is a typed call (one URL per
	// GitRepository) the wrapper doesn't expose. nil when the orchestrator
	// is built without a git fetcher (tests that strip the default via
	// WithFetcher(KindGitRepository, nil)) — Run's pre-warm pass skips.
	gitFetcher *git.Fetcher

	// repoRoot is the resolved .git ancestor of cfg.Path (or
	// cfg.Path when no .git exists). Populated during Bootstrap from
	// discovery.Result.RepoRoot. Consumed by finalize.detectOrphans
	// to feed loader.KSPathPrefixes the correct filesystem root for
	// `components:` lookups (cfg.Path is the user-supplied --path,
	// which may be a subdir of the actual repo root and produce
	// wrong component prefixes — same bug fixed in pkg/discovery by
	// PR #358).
	repoRoot string

	// sourceFiles tracks which file produced each loaded resource. It
	// is populated during loadManifests and consumed once by Bootstrap
	// to construct the immutable change.Filter.
	sourceFiles map[manifest.NamedResource]string

	// sourceRefs maps each loaded consumer (HelmRelease / Kustomization)
	// to the source resources it references. Populated during discovery
	// and consumed once by Bootstrap to give the change.Filter its
	// reverse edge (changed source -> consuming HelmReleases).
	sourceRefs map[manifest.NamedResource][]manifest.NamedResource

	// parentOf is the structural-parent index Bootstrap computes after
	// loadManifests + namespace inheritance — keyed by every
	// reconcilable id that uses a parent gate (KS and HR). KS lookups
	// only see KS entries and HR lookups only see HR entries because
	// NamedResource includes Kind; the single map keeps controller
	// wiring symmetric.
	parentOf map[manifest.NamedResource]manifest.NamedResource

	// selfProduce attributes each ConfigMap to the Kustomization(s) whose
	// own render emits it (resolved across the bare-dir/component graph).
	// The KS controller consults it to drop a self-produced
	// postBuild.substituteFrom ConfigMap from the dependency set.
	selfProduce *loader.SelfProduceIndex

	// rendered tracks IDs the KS controller emitted from a parent
	// render (vs. only loaded by the file walker). detectOrphans reads
	// it to demote stale-on-disk resources to "orphan" rather than
	// failing the run. Lives here, not on Store — iter-15 carved this
	// orchestrator-internal bookkeeping out of the data substrate.
	rendered *renderedSet

	// existence is the file-indexed bookkeeping the DiscoveryOnly
	// loader populated. Consumed by the depwait ResolveMissing
	// closure to lazy-promote substituteFrom CMs/Secrets and any
	// other file-indexed dep on demand.
	existence *loader.ExistenceIndex

	// componentCache memoizes manifest.ReadKustomizeComponents reads
	// across every Bootstrap consumer that walks the KS list: the
	// loader's FinalizeGenerators (KSPathPrefixes), discovery's
	// parent-index builds and orphan-promotion pass, the change
	// filter's buildOwnership, and finalize.detectOrphans. Without
	// the shared cache each consumer re-reads every kustomization.yaml
	// once — N consumers × K KSes file opens per Bootstrap; with it,
	// each (repoRoot, base) pair is read exactly once. Live for the
	// life of the orchestrator (cleared via GC when the Orchestrator
	// is collected), instantiated fresh per New() so test harnesses
	// that reuse an orchestrator across re-Bootstrap cycles still pick
	// up on-disk edits.
	componentCache *manifest.ComponentCache

	// orphans records resources Run demoted from Failed → Ready because
	// they aren't referenced by any parent KS. Populated during Run,
	// surfaced via Render() so embedders can distinguish orphan-skips
	// from genuine successes. Keyed by id, value is the original
	// failure message.
	orphans map[manifest.NamedResource]string

	// preflightFailures records dependency cycles found before a
	// controller renders the resource. Controllers consult it from
	// their object listeners and mark the resource Failed instead of
	// waiting on a graph that cannot converge.
	// preflightMu guards the full detect-replace-refire unit in
	// failDependsOnCycles and serializes reads in preflightFailure.
	preflightMu       sync.RWMutex
	preflightFailures map[manifest.NamedResource]string

	// depGraph maintains an incremental copy of the same-kind
	// dependsOn graph so cycle detection on EventObjectAdded touches
	// only the edges that changed instead of rerunning the full tri-
	// color DFS over every Kustomization / HelmRelease. Lives for the
	// life of the orchestrator; populated lazily as objects are added.
	depGraph *dependencyGraph

	// rsExtensions holds non-Flux docs produced by ResourceSet renders,
	// keyed by the owning structural-parent Kustomization. Populated
	// by expandResourceSetsPostRun (called from Render) after Run
	// completes, so it sees RSIPs emitted via KS-controller kustomize
	// substitution; merged into Result.Manifests at Render time so
	// `flate build` surfaces what the RS would create in-cluster.
	rsExtensions map[manifest.NamedResource][]map[string]any

	// bootstrapped flips true once Bootstrap returns. Read by
	// WithFetcher to refuse late fetcher swaps that would silently
	// miss any source CR discovery already reconciled.
	bootstrapped bool

	// renderOnce serializes the Run + Result-collection phase so a
	// second Render returns the cached Result/err pair instead of
	// re-driving the controllers. The underlying controllers' Configure
	// hooks panic if invoked after Start (the "reconcile-shaping config
	// is frozen once dispatch begins" invariant), so a naive re-Run
	// would panic even with Bootstrap idempotent. Caching at the Render
	// boundary is the embed-friendly answer: embedders that retry
	// Render get back the original artifacts without paying the
	// controller-restart cost.
	renderOnce   sync.Once
	renderResult *Result
	renderErr    error

	// stopOnce guards Stop so a New+Bootstrap-then-abort caller's
	// manual Stop and Run's deferred Stop don't double-close the
	// staging cache / controller unsubs.
	stopOnce sync.Once
}

// Result is the structured output of Orchestrator.Render: the rendered
// manifests keyed by the originating Kustomization / HelmRelease, the
// set of resources that failed reconcile, and the orphans flate
// detected (sources sitting under a parent KS's spec.path but never
// emitted by that parent's render — real Flux would not reconcile
// them either).
//
// Manifests is empty for an HR that had nothing to render or a KS
// whose render produced zero docs; Failed/Orphans are empty when
// everything reconciled cleanly.
type Result struct {
	Manifests map[manifest.NamedResource][]map[string]any
	Failed    map[manifest.NamedResource]store.StatusInfo
	Orphans   map[manifest.NamedResource]string
}

// New constructs an Orchestrator. It allocates the Store and TaskService
// but does not yet start any reconciliation — call Bootstrap then Run.
func New(cfg Config) (*Orchestrator, error) {
	if cfg.Path == "" {
		return nil, errors.New("orchestrator: path is required")
	}

	layout := cacheroot.New(cmp.Or(cfg.CacheDir, cacheroot.Default()))
	helmClientOpts := helm.ClientOptions{
		TemplateCacheBytes: cfg.HelmTemplateCacheBytes,
		RenderCacheBytes:   cfg.HelmRenderCacheBytes,
	}
	// Only point the disk layer at a concrete path when the persistent
	// cache is enabled (HelmRenderCacheBytes > 0). Leaving the root
	// blank when disabled lets the disk-cache constructor short-circuit
	// without performing any directory layout pre-checks.
	if cfg.HelmRenderCacheBytes > 0 {
		helmClientOpts.RenderCacheRoot = layout.RenderHelmCache()
	}
	helmClient, err := helm.NewClientWithOptions(layout, helmClientOpts)
	if err != nil {
		return nil, err
	}
	// The kustomize render cache is now purely in-memory: each source tree is
	// captured once per run into an immutable byte snapshot, from which every
	// render derives a private in-memory filesystem (no disk staging). Nothing
	// to clean up on Stop.
	treeCache := kustomize.NewTreeCache()

	st := store.New()
	ts := task.NewBounded(cfg.Concurrency)
	cache := cmp.Or(cfg.SourceCache, source.NewCache(layout))
	secretGet := func(ns, name string) *manifest.Secret {
		s, _ := store.GetByName[*manifest.Secret](st, manifest.KindSecret, ns, name)
		return s
	}
	// Route helm.Client's source-CR lookups straight through the canonical
	// Store rather than maintaining a duplicate registry the HR controller
	// would otherwise have to keep in sync via Add* push-API calls.
	resolver := helm.NewStoreSourceResolver(st)
	helmClient.SetSourceResolver(resolver)
	srcCtrl := sourcectrl.New(st, ts)
	// Every kind-specific fetcher is wired through source.Wrap so the
	// concrete Fetch signature stays typed (no per-impl type
	// assertion). The wrap label is the kind string; a mismatched
	// payload at dispatch surfaces "<kind> fetcher: unexpected
	// payload <T>" from the single adapter site rather than from
	// four nearly-identical type assertions.
	gitFetcher := &git.Fetcher{
		Cache:   cache,
		Secrets: secretGet,
		Mirrors: mirror.New(layout),
		Depth:   cfg.GitDepth,
	}
	// Resolve kustomize remote git bases (resources: URLs carrying ?ref= or a
	// git marker) through the same clone/mirror/ref-resolution machinery as
	// Flux GitRepository sources. The seam is a function value because
	// pkg/kustomize cannot import pkg/source/git (import cycle via pkg/source).
	treeCache.SetGitBaseFetcher(func(ctx context.Context, repoURL, ref string) (string, string, error) {
		art, err := gitFetcher.FetchRemoteBase(ctx, repoURL, ref)
		if err != nil {
			return "", "", err
		}
		return art.LocalPath, art.Revision, nil
	})
	srcCtrl.Fetchers[manifest.KindGitRepository] = source.Wrap(
		manifest.KindGitRepository, gitFetcher)
	srcCtrl.Fetchers[manifest.KindExternalArtifact] = source.Wrap(
		manifest.KindExternalArtifact, &external.Fetcher{})
	srcCtrl.Fetchers[manifest.KindBucket] = source.Wrap(
		manifest.KindBucket, &bucket.Fetcher{Cache: cache, Secrets: secretGet})
	// HelmRepository: existence-only — the controller just needs the
	// resource in Ready so HelmRelease deps unblock. The chart itself is
	// fetched per (chart, version) through a synthesized HelmChart (see
	// materializeHelmChartSource), so every chart kind routes through the
	// source controller uniformly.
	srcCtrl.Fetchers[manifest.KindHelmRepository] = source.ExistenceFetcher{}
	// Bare OCI fetcher, used two ways: as the standalone OCIRepository
	// fetcher, and as the HelmChart fetcher's OCI-branch delegate (so
	// OCI-backed HelmRepository charts pull with the same auth/TLS/verify).
	// Every OCIRepository real-fetches through the source controller, like
	// every other source kind — there is no existence-only OCI path.
	// Embedders that want one can still WithFetcher a source.ExistenceFetcher.
	// Not separately retry-wrapped: each consuming fetcher's own WithRetry
	// wrapper owns retries.
	ociFetcher := &oci.Fetcher{Cache: cache, RegistryConfig: cfg.RegistryConfig, Secrets: secretGet}
	srcCtrl.Fetchers[manifest.KindOCIRepository] = source.Wrap(
		manifest.KindOCIRepository, ociFetcher)
	// HelmChart: the single authoritative chart fetcher. The HR controller
	// synthesizes a HelmChart per (chart, version, repo) for every
	// HelmRepository-sourced chart; this fetcher pulls it — HTTP repos via
	// helm's getter, OCI repos via the OCI fetcher above.
	hcFetcher, err := helmchart.New(secretGet, resolver.HelmRepository, ociFetcher, cache, layout)
	if err != nil {
		return nil, err
	}
	srcCtrl.Fetchers[manifest.KindHelmChart] = source.Wrap(
		manifest.KindHelmChart, hcFetcher)
	// Wrap every registered fetcher in the classified retry decorator so
	// transient network failures (connection resets, refused connections,
	// timeouts) get a bounded retry the same way across all source kinds,
	// while permanent errors (bad path, auth, not-found) still fail fast.
	// A no-op when retry is disabled (cfg.SourceRetry.Attempts <= 1), and
	// any future kind picks this up automatically. Reassigning existing
	// map values during range is well-defined (no keys added/removed).
	for kind, f := range srcCtrl.Fetchers {
		srcCtrl.Fetchers[kind] = source.WithRetry(f, cfg.SourceRetry)
	}
	o := &Orchestrator{
		cfg:            cfg,
		store:          st,
		tasks:          ts,
		src:            srcCtrl,
		ksc:            kustomization.New(st, ts, treeCache, cfg.WipeSecrets),
		hrc:            helmrelease.New(st, ts, helmClient, cfg.HelmOptions, cfg.WipeSecrets),
		rendered:       newRenderedSet(),
		helm:           helmClient,
		treeCache:      treeCache,
		componentCache: manifest.NewComponentCache(),
		depGraph:       newDependencyGraph(),
		gitFetcher:     gitFetcher,
	}
	return o, nil
}

// Store returns the underlying object store.
func (o *Orchestrator) Store() *store.Store { return o.store }

// WithFetcher installs (or replaces) a per-kind source.Fetcher on the
// internal source controller. Call BEFORE Bootstrap. Returns the
// orchestrator for chaining. Use this to embed flate as a library with
// a custom fetcher (in-memory test fixtures, additional source kinds,
// alternate verification logic) without forking the New() construction.
//
// Passing a nil fetcher unregisters the kind — useful for stripping a
// default registration in tests.
//
// Panics if called after Bootstrap: by then discovery has already run
// and any source CR discovered between New() and Bootstrap was
// reconciled by whichever fetcher was registered at that moment. A
// later swap silently misses those reconciles and the embedder gets
// inconsistent behavior across source CRs of the same kind. Bootstrap
// is the natural commit point for the controller wiring.
func (o *Orchestrator) WithFetcher(kind string, f source.Fetcher) *Orchestrator {
	if o.bootstrapped {
		panic("orchestrator: WithFetcher called after Bootstrap; install fetchers BEFORE Bootstrap")
	}
	if f == nil {
		delete(o.src.Fetchers, kind)
		return o
	}
	o.src.Fetchers[kind] = f
	return o
}

// Filter returns the change filter (may be nil-but-non-active).
func (o *Orchestrator) Filter() *change.Filter { return o.filter }

// Bootstrap discovers manifests, applies namespace inheritance, primes
// existence-only sources Ready, and prepares the change filter.
// Delegates the load / expand / alias phase to pkg/discovery; the
// remainder is dependency validation + change-filter construction.
//
// Idempotent: a second call returns nil without re-running discovery.
// Bootstrap mutates orchestrator state (sourceFiles, parentOf,
// existence, depGraph, componentCache, filter); replaying it would
// rebuild the change.Filter from scratch, dropping any OnAdd hook
// already wired into the store's listener set. Centralizing the
// guard here means Render, embedders, and test harnesses all get
// the invariant for free. A partial-failure path that returns before
// flipping bootstrapped=true leaves the orchestrator eligible for a
// clean retry on the next call.
func (o *Orchestrator) Bootstrap(ctx context.Context) error {
	if o.bootstrapped {
		return nil
	}
	res, err := discovery.Run(ctx, discovery.Config{
		Path: o.cfg.Path, RepoRoot: o.cfg.RepoRoot, SelfURLs: o.cfg.SelfURLs,
		Store: o.store, WipeSecrets: o.cfg.WipeSecrets,
		ComponentCache: o.componentCache,
	})
	if err != nil {
		return err
	}
	o.repoRoot = res.RepoRoot
	o.sourceFiles = res.SourceFiles
	o.sourceRefs = res.SourceRefs
	o.parentOf = res.ParentOf
	o.selfProduce = res.SelfProduce
	o.existence = res.Existence

	o.failDependsOnCycles()
	if err := o.buildChangeFilter(res.RepoRoot); err != nil {
		return err
	}
	o.bootstrapped = true
	return nil
}

// replacePreflightFailures replaces the current preflight-failure map
// with failures and returns the IDs that were previously failing but
// are no longer present in the new set (cleared entries). Must be
// called while holding preflightMu for writing.
func (o *Orchestrator) replacePreflightFailures(failures map[manifest.NamedResource]string) []manifest.NamedResource {
	cleared := make([]manifest.NamedResource, 0, len(o.preflightFailures))
	for id := range o.preflightFailures {
		if _, stillFailed := failures[id]; !stillFailed {
			cleared = append(cleared, id)
		}
	}
	if len(failures) == 0 {
		o.preflightFailures = nil
		return cleared
	}
	o.preflightFailures = maps.Clone(failures)
	return cleared
}

func (o *Orchestrator) preflightFailure(id manifest.NamedResource) (string, bool) {
	o.preflightMu.RLock()
	defer o.preflightMu.RUnlock()
	msg, ok := o.preflightFailures[id]
	return msg, ok
}
