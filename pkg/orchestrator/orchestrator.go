package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
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

	// Path is the directory to scan for Flux objects.
	Path string
	// PathOrig, when non-empty, switches every command into
	// changed-only mode: only resources whose source files differ
	// (plus the sources they reference) get reconciled.
	PathOrig string

	// HelmOptions tunes templating (skip CRDs/secrets/tests, kube
	// version, etc.).
	HelmOptions helm.Options
	// WipeSecrets controls Secret cleartext placeholders.
	WipeSecrets bool
	// EnableOCI turns on OCIRepository reconciliation.
	EnableOCI bool
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
	cfg     Config
	store   *store.Store
	tasks   *task.Service
	src     *sourcectrl.Controller
	ksc     *kustomization.Controller
	hrc     *helmrelease.Controller
	helm    *helm.Client
	staging *kustomize.StagingCache
	filter  *change.Filter

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

	// parentOf is the structural-parent index Bootstrap computes after
	// loadManifests + namespace inheritance — keyed by every
	// reconcilable id that uses a parent gate (KS and HR). KS lookups
	// only see KS entries and HR lookups only see HR entries because
	// NamedResource includes Kind; the single map keeps controller
	// wiring symmetric.
	parentOf map[manifest.NamedResource]manifest.NamedResource

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
	staging, err := kustomize.NewStagingCacheFromLayout(layout, cfg.StageCacheBytes)
	if err != nil {
		// helmClient already created tmpDir + cacheDir under the
		// cache root. Leaving them would leak a temp directory per
		// failed orchestrator construction (e.g. retry-on-bad-config
		// loops in a test harness). The helm client itself has no
		// Close; the best-effort cleanup is to drop the dirs we own.
		_ = os.RemoveAll(layout.HelmTmp())
		_ = os.RemoveAll(layout.HelmCache())
		return nil, err
	}

	st := store.New()
	ts := task.NewBounded(cfg.Concurrency)
	cache := cmp.Or(cfg.SourceCache, source.NewCache(layout))
	secretGet := func(ns, name string) *manifest.Secret {
		s, _ := store.GetByName[*manifest.Secret](st, manifest.KindSecret, ns, name)
		return s
	}
	helmClient.SetSecretGetter(secretGet)
	// Route helm.Client's source-CR lookups straight through the canonical
	// Store rather than maintaining a duplicate registry the HR controller
	// would otherwise have to keep in sync via Add* push-API calls.
	helmClient.SetSourceResolver(helm.NewStoreSourceResolver(st))
	// Yield the worker-pool slot during OCI pulls so concurrent helm
	// renders don't starve. Passes task.Service.YieldSlot through as
	// a callback so pkg/helm doesn't have to import pkg/task.
	helmClient.SetTaskYield(ts.YieldSlot)
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
	}
	srcCtrl.Fetchers[manifest.KindGitRepository] = source.Wrap(
		manifest.KindGitRepository, gitFetcher)
	srcCtrl.Fetchers[manifest.KindExternalArtifact] = source.Wrap(
		manifest.KindExternalArtifact, &external.Fetcher{})
	srcCtrl.Fetchers[manifest.KindBucket] = source.Wrap(
		manifest.KindBucket, &bucket.Fetcher{Cache: cache, Secrets: secretGet})
	// HelmRepository: existence-only — flate resolves charts via the
	// Helm client's registry/repo machinery directly, the controller
	// just needs the resource to land in Ready so HelmRelease deps
	// unblock.
	srcCtrl.Fetchers[manifest.KindHelmRepository] = source.ExistenceFetcher{}
	if cfg.EnableOCI {
		ociFetcher := &oci.Fetcher{Cache: cache, RegistryConfig: cfg.RegistryConfig, Secrets: secretGet}
		srcCtrl.Fetchers[manifest.KindOCIRepository] = source.Wrap(
			manifest.KindOCIRepository, ociFetcher)
		// Share the same fetcher with helm.Client so HelmRepository
		// (type=oci) and OCIRepository chart resolution both route
		// through one OCI pull path — spec.verify / certSecretRef /
		// proxySecretRef / insecure / layerSelector / ignore apply
		// uniformly. Without this, type=oci would silently drop
		// those fields (helm-side fallback used the registry client
		// with no auth/TLS surface).
		helmClient.SetOCIPuller(ociFetcher)
	} else {
		// --enable-oci=false: skip the real fetch but still mark each
		// OCIRepository Ready so HRs that dependsOn one don't time out.
		// helm.Client's OCIPuller stays nil, falling back to the
		// registry-client pull (matches prior EnableOCI=false behavior).
		srcCtrl.Fetchers[manifest.KindOCIRepository] = source.ExistenceFetcher{}
	}
	o := &Orchestrator{
		cfg:            cfg,
		store:          st,
		tasks:          ts,
		src:            srcCtrl,
		ksc:            kustomization.New(st, ts, staging, cfg.WipeSecrets),
		hrc:            helmrelease.New(st, ts, helmClient, cfg.HelmOptions, cfg.WipeSecrets),
		rendered:       newRenderedSet(),
		helm:           helmClient,
		staging:        staging,
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
		Path: o.cfg.Path, Store: o.store, WipeSecrets: o.cfg.WipeSecrets,
		ComponentCache: o.componentCache,
	})
	if err != nil {
		return err
	}
	o.repoRoot = res.RepoRoot
	o.sourceFiles = res.SourceFiles
	o.parentOf = res.ParentOf
	o.existence = res.Existence

	o.failDependsOnCycles()
	o.warnOnDisabledOCIFeatures()
	o.warnOnKSOCISourceRefWithoutOCI()
	if err := o.buildChangeFilter(res.RepoRoot); err != nil {
		return err
	}
	o.bootstrapped = true
	return nil
}

// warnOnDisabledOCIFeatures surfaces the EnableOCI=false footgun: with
// the source.ExistenceFetcher wired for OCIRepository, spec.verify,
// spec.layerSelector, spec.certSecretRef, spec.proxySecretRef,
// spec.insecure, and spec.ignore are all parsed from the manifests but
// silently discarded — the source is marked Ready without a real
// fetch. A user who configured spec.verify deserves to SEE that
// flate skipped verification rather than discover it via "passed in
// flate, failed in cluster".
func (o *Orchestrator) warnOnDisabledOCIFeatures() {
	if o.cfg.EnableOCI {
		return
	}
	for _, repo := range store.ListAs[*manifest.OCIRepository](o.store, manifest.KindOCIRepository) {
		var fields []string
		if repo.Verify != nil {
			fields = append(fields, "spec.verify")
		}
		if repo.LayerSelector != nil {
			fields = append(fields, "spec.layerSelector")
		}
		if repo.CertSecretRef != nil {
			fields = append(fields, "spec.certSecretRef")
		}
		if repo.ProxySecretRef != nil {
			fields = append(fields, "spec.proxySecretRef")
		}
		if repo.Insecure {
			fields = append(fields, "spec.insecure")
		}
		if repo.Ignore != nil {
			fields = append(fields, "spec.ignore")
		}
		if len(fields) == 0 {
			continue
		}
		slog.Warn("OCIRepository spec fields ignored — EnableOCI=false wires ExistenceFetcher",
			"oci_repository", repo.Namespace+"/"+repo.Name,
			"ignored_fields", fields)
	}
}

// warnOnKSOCISourceRefWithoutOCI surfaces the EnableOCI=false +
// KS-sourceRef=OCIRepository combo at bootstrap. Without OCI
// reconciliation, source/ExistenceFetcher gives the OCIRepository a
// Ready status but NO SourceArtifact — a Kustomization that needs
// the artifact for spec.path resolution then dies at reconcile with
// the cryptic "artifact not found", far from where the actual
// configuration error lives. Warn up front so the operator either
// enables OCI (--enable-oci=true / default) or restructures the KS
// to use a GitRepository source.
func (o *Orchestrator) warnOnKSOCISourceRefWithoutOCI() {
	if o.cfg.EnableOCI {
		return
	}
	for _, ks := range store.ListAs[*manifest.Kustomization](o.store, manifest.KindKustomization) {
		if ks.SourceKind != manifest.KindOCIRepository {
			continue
		}
		slog.Warn("Kustomization sourceRef points at OCIRepository but --enable-oci=false; the synthesized existence-only artifact has no LocalPath and spec.path resolution will fail at render time",
			"kustomization", ks.Namespace+"/"+ks.Name,
			"source_ref", ks.SourceNamespace+"/"+ks.SourceName)
	}
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
