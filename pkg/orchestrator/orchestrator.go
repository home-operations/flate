package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strings"

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
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/source/git"
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
	// AllowMissingSecrets converts auth-secret-not-found errors during
	// source fetch into a skip rather than a failure. Use when secrets
	// are materialized on the live cluster (ExternalSecret, etc.) and
	// can't appear in the offline tree. Skipped sources mark Ready with
	// a "skipped:" reason and produce no artifact; downstream KS / HR
	// consumers propagate the skip so the dependency chain doesn't
	// surface as a cascade of failures in `flate test`.
	AllowMissingSecrets bool

	// RegistryConfig is the docker config.json used for OCI auth.
	RegistryConfig string

	// CacheDir overrides the default on-disk cache root
	// (os.TempDir()/flate-cache).
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

	// rsExtensions holds non-Flux docs produced by ResourceSet renders,
	// keyed by the owning structural-parent Kustomization. Populated
	// during Bootstrap (from discovery.Result.RSExtensions); merged
	// into Result.Manifests at Render time so `flate build` surfaces
	// what the RS would create in-cluster.
	rsExtensions map[manifest.NamedResource][]map[string]any

	// bootstrapped flips true once Bootstrap returns. Read by
	// WithFetcher to refuse late fetcher swaps that would silently
	// miss any source CR discovery already reconciled.
	bootstrapped bool
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

	cacheRoot := cmp.Or(cfg.CacheDir, filepath.Join(os.TempDir(), "flate-cache"))
	helmClient, err := helm.NewClient(
		filepath.Join(cacheRoot, "helm-tmp"),
		filepath.Join(cacheRoot, "helm-cache"),
	)
	if err != nil {
		return nil, err
	}
	staging, err := kustomize.NewStagingCache(filepath.Join(cacheRoot, "stage"))
	if err != nil {
		// helmClient already created tmpDir + cacheDir under cacheRoot.
		// Leaving them would leak a temp directory per failed
		// orchestrator construction (e.g. retry-on-bad-config loops in
		// a test harness). The helm client itself has no Close; the
		// best-effort cleanup is to drop the cacheRoot we own.
		_ = os.RemoveAll(filepath.Join(cacheRoot, "helm-tmp"))
		_ = os.RemoveAll(filepath.Join(cacheRoot, "helm-cache"))
		return nil, err
	}

	st := store.New()
	ts := task.NewBounded(cfg.Concurrency)
	cache := cmp.Or(cfg.SourceCache, source.NewCache(filepath.Join(cacheRoot, "sources")))
	secretGet := func(ns, name string) *manifest.Secret {
		s, _ := store.GetByName[*manifest.Secret](st, manifest.KindSecret, ns, name)
		return s
	}
	helmClient.SetSecretGetter(secretGet)
	// Route helm.Client's source-CR lookups straight through the canonical
	// Store rather than maintaining a duplicate registry the HR controller
	// would otherwise have to keep in sync via Add* push-API calls.
	helmClient.SetSourceResolver(helm.NewStoreSourceResolver(st))
	srcCtrl := sourcectrl.New(st, ts)
	srcCtrl.Fetchers[manifest.KindGitRepository] = &git.Fetcher{Cache: cache, Secrets: secretGet}
	srcCtrl.Fetchers[manifest.KindExternalArtifact] = &external.Fetcher{}
	srcCtrl.Fetchers[manifest.KindBucket] = &bucket.Fetcher{Cache: cache, Secrets: secretGet}
	// HelmRepository: existence-only — flate resolves charts via the
	// Helm client's registry/repo machinery directly, the controller
	// just needs the resource to land in Ready so HelmRelease deps
	// unblock.
	srcCtrl.Fetchers[manifest.KindHelmRepository] = source.ExistenceFetcher{}
	if cfg.EnableOCI {
		srcCtrl.Fetchers[manifest.KindOCIRepository] = &oci.Fetcher{
			Cache: cache, RegistryConfig: cfg.RegistryConfig, Secrets: secretGet,
		}
	} else {
		// --enable-oci=false: skip the real fetch but still mark each
		// OCIRepository Ready so HRs that dependsOn one don't time out.
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
	o.sourceFiles = res.SourceFiles
	o.parentOf = res.ParentOf
	o.existence = res.Existence

	o.validateDependsOn()
	o.breakDependsOnCycles()
	o.warnOnDisabledOCIFeatures()
	o.warnOnUnsupportedHelmRepoSecretRef()
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
			"ociRepository", repo.Namespace+"/"+repo.Name,
			"ignoredFields", fields)
	}
}

// warnOnUnsupportedHelmRepoSecretRef surfaces the OCI-HelmRepository
// + SecretRef gap at bootstrap. The helm-side
// locateHelmRepoChart hard-errors at render time on this combo
// (see pkg/helm/repo.go) — by then the user is one helm template
// failure deep into a reconcile pass with a non-obvious "not yet
// implemented" message. Warn up front so the operator can switch
// the manifests to OCIRepository CRs (the recommended workaround)
// before iterating.
//
// Triggered for any HelmRepository whose spec.type is "oci" OR
// whose URL starts with `oci://` AND has a SecretRef — matches
// the render-time check exactly.
func (o *Orchestrator) warnOnUnsupportedHelmRepoSecretRef() {
	for _, repo := range store.ListAs[*manifest.HelmRepository](o.store, manifest.KindHelmRepository) {
		if repo.SecretRef == nil {
			continue
		}
		if repo.Type != manifest.RepoTypeOCI && !strings.HasPrefix(repo.URL, "oci://") {
			continue
		}
		slog.Warn("HelmRepository: SecretRef on OCI HelmRepositories is not yet implemented and will fail at render time; reference the chart via a sibling OCIRepository CR instead",
			"helmRepository", repo.Namespace+"/"+repo.Name,
			"url", repo.URL)
	}
}

// validateDependsOn drops dangling dependsOn references on both
// Kustomizations and HelmReleases so the dependency-wait phase fails
// fast on typos instead of stalling out the full per-dep budget.
func (o *Orchestrator) validateDependsOn() {
	known := map[string]map[string]struct{}{
		manifest.KindKustomization: {},
		manifest.KindHelmRelease:   {},
	}
	ksList := store.ListAs[*manifest.Kustomization](o.store, manifest.KindKustomization)
	for _, ks := range ksList {
		known[manifest.KindKustomization][ks.Named().NamespacedName()] = struct{}{}
	}
	hrList := store.ListAs[*manifest.HelmRelease](o.store, manifest.KindHelmRelease)
	for _, hr := range hrList {
		known[manifest.KindHelmRelease][hr.Named().NamespacedName()] = struct{}{}
	}
	// Mutate via the Store helper — encodes the clone-then-AddObject
	// contract so callers don't have to remember it.
	for _, ks := range ksList {
		kept, dropped := manifest.FilterDependsOn(ks.DependsOn, known[manifest.KindKustomization])
		if dropped == 0 {
			continue
		}
		store.Mutate(o.store, ks.Named(), func(k *manifest.Kustomization) { k.DependsOn = kept })
	}
	for _, hr := range hrList {
		kept, dropped := manifest.FilterDependsOn(hr.DependsOn, known[manifest.KindHelmRelease])
		if dropped == 0 {
			continue
		}
		store.Mutate(o.store, hr.Named(), func(h *manifest.HelmRelease) { h.DependsOn = kept })
	}
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
				"resolvedPath", currAbs)
			return nil
		}
		// Diff the literal user-supplied paths so subdir-vs-subdir
		// comparisons inside one repo work. Walking up to .git would
		// collapse both endpoints to the same root.
		cs, err := change.Detect(origAbs, currAbs)
		if err != nil {
			return fmt.Errorf("change detect: %w", err)
		}
		// Detect emits paths relative to currAbs; re-root them under
		// repoRoot so they line up with SourceFiles keys.
		if rel, err := filepath.Rel(repoRoot, currAbs); err == nil && rel != "." {
			cs = cs.Reroot(rel)
		}
		slog.Info("changed-only mode",
			"baseline", origAbs, "current", currAbs, "changedFiles", cs.Len())
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
	o.attachFilter(f)
	slog.Debug("changed-only keep set", "size", o.filter.Size(), "items", o.filter.KeepNames())
	return nil
}

// attachFilter records the resolved Filter; controllers consume it
// via Configure() at Run time.
func (o *Orchestrator) attachFilter(f *change.Filter) {
	o.filter = f
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
// depwait.RenderInflight. OtherActive returns true when the pool
// has more than one task running — the caller's own goroutine is
// always one of them since depwait runs inside a Submit'd reconcile
// body, so a count of 1 means "just me, no future emissions can
// produce my missing dep".
type orchestratorRenderInflight struct{ tasks *task.Service }

func (r *orchestratorRenderInflight) OtherActive() bool {
	return r.tasks.ActiveCount() > 1
}

// QuiescenceCh delegates to task.Service.QuiescenceCh(1). The
// threshold matches OtherActive's "> 1" check: the caller's own
// goroutine is one slot, so a drain to <= 1 means no other reconcile
// is running. depwait's waitRenderEmission selects on this channel
// instead of polling OtherActive.
func (r *orchestratorRenderInflight) QuiescenceCh() <-chan struct{} {
	return r.tasks.QuiescenceCh(1)
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
		Filter:        o.filter,
		ParentOf:      parentResolver,
		RenderTracker: o.rendered,
		Existence:     existence,
		Renders:       renders,
	})
	o.hrc.Configure(helmrelease.ReconcileOptions{
		Filter:    o.filter,
		ParentOf:  parentResolver,
		Existence: existence,
		Renders:   renders,
	})
	o.src.Start(ctx)
	o.ksc.Start(ctx)
	o.hrc.Start(ctx)
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
		return o.finalize()
	case <-ctx.Done():
		// Wait for in-flight tasks to wind down before finalizing so
		// the store snapshot is internally consistent — but if they
		// take longer than the parent ctx's grace window, the worst
		// case is the orchestrator hangs at the same point it would
		// have before this fix. Net: Ctrl-C is at least observable
		// in logs now; eventual exit semantics unchanged.
		<-done
		return o.finalize()
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
	o.expandResourceSetsPostRun()
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
	for id, info := range sanitizeFailed(o.store.FailedResources()) {
		res.Failed[id] = info
	}
	maps.Copy(res.Orphans, o.orphans)
	return res, runErr
}

// Stop shuts the controllers down in reverse-construction order and
// releases the staging cache.
func (o *Orchestrator) Stop() {
	o.hrc.Close()
	o.ksc.Close()
	o.src.Close()
	if o.staging != nil {
		_ = o.staging.Close()
	}
}
