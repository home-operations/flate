package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
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
		return nil, fmt.Errorf("orchestrator: path is required")
	}

	layout := cacheroot.New(cmp.Or(cfg.CacheDir, cacheroot.Default()))
	helmClient, err := helm.NewClient(layout)
	if err != nil {
		return nil, err
	}
	staging, err := kustomize.NewStagingCache(layout.Stage())
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
	srcCtrl.Fetchers[manifest.KindGitRepository] = source.Wrap(
		manifest.KindGitRepository, &git.Fetcher{
			Cache:   cache,
			Secrets: secretGet,
			Mirrors: mirror.New(layout),
		})
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
		cfg:      cfg,
		store:    st,
		tasks:    ts,
		src:      srcCtrl,
		ksc:      kustomization.New(st, ts, staging, cfg.WipeSecrets),
		hrc:      helmrelease.New(st, ts, helmClient, cfg.HelmOptions, cfg.WipeSecrets),
		rendered: newRenderedSet(),
		helm:     helmClient,
		staging:  staging,
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
func (o *Orchestrator) Bootstrap(ctx context.Context) error {
	res, err := discovery.Run(ctx, discovery.Config{
		Path: o.cfg.Path, Store: o.store, WipeSecrets: o.cfg.WipeSecrets,
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

// buildChangeFilter computes the file-level change set (if changed-only
// mode is requested) and constructs the immutable change.Filter from
// (changes, sourceFiles, repoRoot, store), then wires it onto every
// controller. When changed-only mode is off the filter remains nil and
// controllers reconcile everything.
func (o *Orchestrator) buildChangeFilter(repoRoot string) error {
	changes := o.cfg.ExternalChanges
	if changes == nil && o.cfg.PathOrig != "" {
		origAbs, err := discovery.ResolveScanPath(o.cfg.PathOrig)
		if err != nil {
			return fmt.Errorf("--path-orig: %w", err)
		}
		currAbs, err := discovery.ResolveScanPath(o.cfg.Path)
		if err != nil {
			return fmt.Errorf("--path: %w", err)
		}
		// Both paths resolved to the same directory (e.g. a symlink and
		// its target, or literally the same arg twice). Changed-only mode
		// would diff a tree against itself producing an empty change set.
		// Skip filter build so the user's `--path-orig` typo doesn't
		// silently render zero output.
		if origAbs == currAbs {
			slog.Warn("--path and --path-orig resolve to the same directory; ignoring --path-orig",
				"resolved_path", currAbs)
			return nil
		}
		// Widen the diff scope to each side's .git root when the user
		// pointed at sibling subdirs of separate checkouts — the
		// canonical PR-vs-base-branch flow `flate diff ks --path
		// pr/cluster --path-orig base/cluster` where the actual edits
		// live in `apps/`, outside the cluster subdir. Diffing the
		// literal subdirs would report zero changes even though the
		// rendered output differs. We only widen when both sides resolve
		// to .git roots that are (a) not the same root (one checkout,
		// two subdirs is the deliberate subdir-vs-subdir case) and
		// (b) actually distinct from the path the user passed (no .git
		// ancestor → FindRepoRoot returns the path unchanged).
		diffOrig, diffCurr := origAbs, currAbs
		origRoot := discovery.FindRepoRoot(origAbs)
		currRoot := discovery.FindRepoRoot(currAbs)
		widened := origRoot != origAbs && currRoot != currAbs && origRoot != currRoot
		if widened {
			diffOrig, diffCurr = origRoot, currRoot
		}
		cs, err := change.Detect(diffOrig, diffCurr)
		if err != nil {
			return fmt.Errorf("change detect: %w", err)
		}
		// Detect emits paths relative to diffCurr. When we widened, that
		// already equals repoRoot and SourceFiles keys line up. When we
		// didn't, lift the subdir-relative paths into repoRoot's
		// coordinate system so they match SourceFiles keys.
		if !widened {
			if rel, err := filepath.Rel(repoRoot, currAbs); err == nil && rel != "." {
				cs = cs.Reroot(rel)
			}
		}
		slog.Debug("changed-only mode",
			"baseline", diffOrig, "current", diffCurr, "changed_files", cs.Len(), "widened_to_repo_root", widened)
		if cs.Len() == 0 {
			slog.Warn("no changes detected between --path and --path-orig — output will be empty; verify both paths reference distinct snapshots")
		}
		changes = cs
	}
	if changes == nil {
		return nil
	}
	f := change.NewFilter(changes, o.sourceFiles, repoRoot, o.store)
	// Wire OnAdd so a runtime keep-set extension (KS controller's
	// emitRenderedChildren → keepEmitted) refires any source whose
	// listener already short-circuited via PreGate before the
	// consuming KS joined keep. Without this hook, the source stays
	// Ready/"unchanged" with no artifact and the KS's
	// resolveSourceRoot surfaces "artifact not found" downstream.
	// Issue #260. Refire owns the status reset that closes the
	// depwait race — see Store.Refire.
	//
	// Kinds limited to the source-controller-managed set (those wired
	// with a Fetcher in the constructor above). HelmChart resources
	// are read directly by helm.storeResolver without a Fetcher, so
	// Refire on a HelmChart id would write a Pending status that no
	// controller transitions back to Ready.
	f.OnAdd = func(id manifest.NamedResource) {
		switch id.Kind {
		case manifest.KindGitRepository,
			manifest.KindOCIRepository,
			manifest.KindHelmRepository,
			manifest.KindBucket,
			manifest.KindExternalArtifact:
			o.store.Refire(id)
		}
	}
	o.filter = f
	slog.Debug("changed-only keep set", "size", o.filter.Size(), "items", o.filter.KeepNames())
	return nil
}

// orchestratorExistence adapts the orchestrator's
// loader.ExistenceIndex + Store + WipeSecrets config into the
// depwait.ExistenceLookup interface that flows through controller
// Options into every Waiter. Bundled so controllers don't each
// reach into the orchestrator's private fields, and so future
// existence-related signals land on one type instead of plumbing
// new closures through three call layers.
type orchestratorExistence struct {
	idx         *loader.ExistenceIndex
	store       *store.Store
	wipeSecrets bool
}

func (e *orchestratorExistence) Promote(id manifest.NamedResource) bool {
	return e.idx.Promote(e.store, id, e.wipeSecrets)
}

func (e *orchestratorExistence) IsFileIndexed(id manifest.NamedResource) bool {
	_, ok := e.idx.Get(id)
	return ok
}

// orchestratorRenderInflight adapts task.Service.ActiveCount into
// depwait.RenderInflight. OtherActive returns true when any task
// is doing productive work — the caller itself is parked in
// task.Service.YieldQuiescent (see base.Controller.Await), so its
// own slot is excluded from the count and a non-zero active reading
// is by definition "other work in flight".
type orchestratorRenderInflight struct{ tasks *task.Service }

func (r *orchestratorRenderInflight) OtherActive() bool {
	return r.tasks.ActiveCount() > 0
}

// QuiescenceCh delegates to task.Service.QuiescenceCh(0). The
// threshold is 0 because depwait callers reach this method while
// inside YieldQuiescent, which has already decremented the caller's
// own active slot — drain-to-zero means no productive task remains
// in the pool. Without YieldQuiescent's hop, two reconciles each
// blocked in depwait would pin the count at 2 and miss this signal
// entirely.
func (r *orchestratorRenderInflight) QuiescenceCh() <-chan struct{} {
	return r.tasks.QuiescenceCh(0)
}

// Run starts every controller, blocks until the task service drains,
// then aggregates and returns any failures. The post-drain reporting
// + error-string assembly lives in finalize so Run reads as a clean
// start → drain → finalize sequence.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.src.Configure(sourcectrl.FetchOptions{
		Filter:              o.filter,
		AllowMissingSecrets: o.cfg.AllowMissingSecrets,
	})
	// parentResolver unifies the two sources of structural-parent
	// info: (1) the pre-built file-path prefix index for file-loaded
	// resources, and (2) the renderedSet for resources that arrive
	// via a KS render's emitRenderedChildren (populated at Run-time,
	// not at Bootstrap). Controllers query through this single seam.
	parentResolver := func(id manifest.NamedResource) (manifest.NamedResource, bool) {
		if parent, ok := o.parentOf[id]; ok {
			return parent, true
		}
		return o.rendered.ParentOf(id)
	}
	// existence bundles the file-existence lookups depwait needs:
	// Promote lazy-loads a file-indexed dep into the Store the
	// moment a depwait edge asks for it (covering the bjw-s parent-
	// patches pattern where a KS's substituteFrom CM lives inside
	// the same KS's spec.path); IsFileIndexed distinguishes
	// "render-only dep still in flight" (no file record → wait on
	// the per-dep ctx) from "file-indexed but promote failed" (file
	// record → fail fast). The orchestrator owns both signals; the
	// controllers and Waiter consume them via the single
	// depwait.ExistenceLookup interface.
	existence := &orchestratorExistence{
		idx:         o.existence,
		store:       o.store,
		wipeSecrets: o.cfg.WipeSecrets,
	}
	renders := &orchestratorRenderInflight{tasks: o.tasks}
	o.ksc.Configure(kustomization.Options{
		Filter:           o.filter,
		ParentOf:         parentResolver,
		RenderTracker:    o.rendered,
		Existence:        existence,
		Renders:          renders,
		PreflightFailure: o.preflightFailure,
	})
	o.hrc.Configure(helmrelease.ReconcileOptions{
		Filter:              o.filter,
		ParentOf:            parentResolver,
		Existence:           existence,
		Renders:             renders,
		PreflightFailure:    o.preflightFailure,
		AllowMissingSecrets: o.cfg.AllowMissingSecrets,
	})
	// Re-detect dependsOn cycles when render-emitted children land.
	// Bootstrap's one-shot pass only sees file-loaded resources; a
	// parent KS render that emits a child with a dependsOn pointing at
	// another freshly-emitted (or pre-existing) peer can introduce a
	// cycle invisible to the Bootstrap pass. failDependsOnCycles
	// short-circuits when no cycle is present (O(N+E) DFS, then
	// early-return), so re-running on every KS/HR add is cheap even on
	// render-heavy passes.
	//
	// REGISTERED BEFORE the controllers: listeners fire in registration
	// order, so this records preflight failures synchronously BEFORE the
	// controllers' listeners spawn their reconcile goroutines. Without
	// the ordering the controller goroutine could start waiting on a peer
	// that's also waiting on it.
	unsubCycles := o.store.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind != manifest.KindKustomization && id.Kind != manifest.KindHelmRelease {
			return
		}
		o.failDependsOnCycles()
	}, false)
	defer unsubCycles()

	// Keep depwait's render-quiescence signal open while listener
	// flushes submit the bootstrap objects. Without this marker, a
	// Kustomization can start waiting on a render-emitted dependency
	// before the producer's replay event has been submitted and
	// incorrectly conclude that the render pool is drained.
	startupDone := make(chan struct{})
	o.tasks.Go(ctx, "orchestrator/startup", func(ctx context.Context) {
		select {
		case <-startupDone:
		case <-ctx.Done():
		}
	})
	o.src.Start(ctx)
	o.ksc.Start(ctx)
	o.hrc.Start(ctx)
	close(startupDone)
	defer o.Stop()

	// Race ctx.Done() against task drain so a Ctrl-C during a stuck
	// fetch / render is observable within one task's natural
	// cancellation latency rather than blocking forever on
	// wgActive.Wait. Reconcile bodies still need to honor their own
	// ctx to actually return promptly, but at minimum Run no longer
	// hides the cancellation signal from finalize.
	done := make(chan struct{})
	go func() {
		o.tasks.BlockTillDone()
		close(done)
	}()
	select {
	case <-done:
		return errors.Join(o.finalize(), ctx.Err())
	case <-ctx.Done():
		// Wait for in-flight tasks to wind down before finalizing so
		// the store snapshot is internally consistent, then preserve
		// the cancellation in the returned error. Queued bounded tasks
		// may never acquire a worker slot once ctx is canceled; a clean
		// partial snapshot must not look like a successful full run.
		<-done
		return errors.Join(o.finalize(), ctx.Err())
	}
}

// Render is the structured embed-friendly entry point: Bootstrap +
// Run + collect everything an external caller needs to consume the
// reconcile result. CLI / Run() ergonomics remain unchanged; callers
// who want a single function that returns a typed Result use this.
//
// The returned Result is non-nil even when err is non-nil — failures
// during reconcile populate Result.Failed without aborting collection,
// so the caller sees both the partial output and the failure list.
// An error from Bootstrap (the load phase) is fatal and returns
// (nil, err); errors from Run yield (result, err).
func (o *Orchestrator) Render(ctx context.Context) (*Result, error) {
	if err := o.Bootstrap(ctx); err != nil {
		return nil, err
	}
	runErr := o.Run(ctx)
	// Post-Run RS expansion: re-render every ResourceSet now that
	// KS-controller-emitted RSIPs (kustomize-substituted dynamic
	// names like dragonfly-${APP}) are in the store. Discovery's
	// pre-Bootstrap renders see only literal-file RSIPs and miss
	// these. The output attributes non-Flux children to the owning
	// parent KS so `flate build` surfaces them.
	rsErr := o.expandResourceSetsPostRun(ctx)
	res := &Result{
		Manifests: map[manifest.NamedResource][]map[string]any{},
		Failed:    map[manifest.NamedResource]store.StatusInfo{},
		Orphans:   map[manifest.NamedResource]string{},
	}
	// Apply --skip-secrets / --skip-crds / --skip-kinds uniformly here
	// so embedders calling Render see consistent Result.Manifests
	// regardless of producing controller. helm.TemplateDocs pre-filters
	// HR output upstream, but Kustomization-rendered docs reach the
	// artifact unfiltered (the Store must hold the full set so
	// downstream valuesFrom / substituteFrom resolution finds Secret /
	// ConfigMap objects). The drop happens here, on the slice that
	// crosses the embed boundary — Store stays whole. Iter-15 #169
	// patched the CLI emit paths; this closes the same gap one layer
	// down for SDK consumers.
	skip := o.cfg.HelmOptions.SkipResourceKinds()
	for _, kind := range []string{manifest.KindKustomization, manifest.KindHelmRelease} {
		for _, obj := range o.store.ListObjects(kind) {
			id := obj.Named()
			var mans []map[string]any
			if art, ok := o.store.GetArtifact(id).(store.RenderedArtifact); ok {
				mans = art.RenderedManifests()
			}
			// Append any ResourceSet-rendered non-Flux docs whose
			// owning KS is this one. The RS doc itself stays in the
			// parent's render output (kustomize emitted it); these
			// are its synthetic children that flate evaluates offline.
			if kind == manifest.KindKustomization {
				if ext := o.rsExtensions[id]; len(ext) > 0 {
					mans = append(mans, ext...)
				}
			}
			mans = manifest.DropKinds(mans, skip)
			if len(mans) > 0 {
				res.Manifests[id] = mans
			}
		}
	}
	// Same projection logRecourceFailures + aggregateFailures use —
	// see sanitizeFailed for the contract. Centralizing in one
	// helper keeps the three readers in sync if the strip rule
	// changes.
	maps.Copy(res.Failed, sanitizeFailed(o.store.FailedResources()))
	maps.Copy(res.Orphans, o.orphans)
	return res, errors.Join(runErr, rsErr)
}

// Stop shuts the controllers down in reverse-construction order and
// releases the staging cache. Safe to call multiple times: each
// controller's Close is idempotent (drains the unsub slice once and
// nils it), and StagingCache.Close zeroes its stages map after the
// first cleanup. Wrapped in sync.Once so the bookkeeping reads
// cleanly even if a caller's defer runs after Run's defer.
//
// Embedders who call only New + Bootstrap (without Run) MUST call
// Stop themselves — the staging cache holds a tempdir that would
// otherwise leak until process exit.
func (o *Orchestrator) Stop() {
	o.stopOnce.Do(func() {
		o.hrc.Close()
		o.ksc.Close()
		o.src.Close()
		if o.staging != nil {
			_ = o.staging.Close()
		}
	})
}
