package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
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
	// loadManifests + namespace inheritance. Configured onto the
	// kustomization controller at Run-time, never mutated thereafter.
	parentOf map[manifest.NamedResource]manifest.NamedResource

	// rendered tracks IDs the KS controller emitted from a parent
	// render (vs. only loaded by the file walker). detectOrphans reads
	// it to demote stale-on-disk resources to "orphan" rather than
	// failing the run. Lives here, not on Store — iter-15 carved this
	// orchestrator-internal bookkeeping out of the data substrate.
	rendered *renderedSet

	// orphans records resources Run demoted from Failed → Ready because
	// they aren't referenced by any parent KS. Populated during Run,
	// surfaced via Render() so embedders can distinguish orphan-skips
	// from genuine successes. Keyed by id, value is the original
	// failure message.
	orphans map[manifest.NamedResource]string
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

	cacheRoot := cfg.CacheDir
	if cacheRoot == "" {
		cacheRoot = filepath.Join(os.TempDir(), "flate-cache")
	}
	helmClient, err := helm.NewClient(
		filepath.Join(cacheRoot, "helm-tmp"),
		filepath.Join(cacheRoot, "helm-cache"),
	)
	if err != nil {
		return nil, err
	}
	staging, err := kustomize.NewStagingCache(filepath.Join(cacheRoot, "stage"))
	if err != nil {
		return nil, err
	}

	st := store.New()
	ts := task.NewBounded(cfg.Concurrency)
	cache := cmp.Or(cfg.SourceCache, source.NewCache(filepath.Join(cacheRoot, "sources")))
	secretGet := func(ns, name string) *manifest.Secret {
		obj := st.GetByName(manifest.KindSecret, ns, name)
		s, _ := obj.(*manifest.Secret)
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
		helm:    helmClient,
		staging: staging,
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
func (o *Orchestrator) WithFetcher(kind string, f source.Fetcher) *Orchestrator {
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

	o.validateDependsOn()
	o.breakDependsOnCycles()
	return o.buildChangeFilter(res.RepoRoot)
}

// validateDependsOn drops dangling dependsOn references on both
// Kustomizations and HelmReleases so the dependency-wait phase fails
// fast on typos instead of stalling out the full per-dep budget.
func (o *Orchestrator) validateDependsOn() {
	known := map[string]map[string]struct{}{
		manifest.KindKustomization: {},
		manifest.KindHelmRelease:   {},
	}
	ksList := o.store.ListObjects(manifest.KindKustomization)
	for _, obj := range ksList {
		if ks, ok := obj.(*manifest.Kustomization); ok {
			known[manifest.KindKustomization][ks.NamespacedName()] = struct{}{}
		}
	}
	hrList := o.store.ListObjects(manifest.KindHelmRelease)
	for _, obj := range hrList {
		if hr, ok := obj.(*manifest.HelmRelease); ok {
			known[manifest.KindHelmRelease][hr.NamespacedName()] = struct{}{}
		}
	}
	// Mutate via the Store helper — encodes the clone-then-AddObject
	// contract so callers don't have to remember it.
	for _, obj := range ksList {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok {
			continue
		}
		kept, dropped := manifest.FilterDependsOn(ks.DependsOn, known[manifest.KindKustomization])
		if dropped == 0 {
			continue
		}
		store.Mutate(o.store, ks.Named(), func(k *manifest.Kustomization) { k.DependsOn = kept })
	}
	for _, obj := range hrList {
		hr, ok := obj.(*manifest.HelmRelease)
		if !ok {
			continue
		}
		kept, dropped := manifest.FilterDependsOn(hr.DependsOn, known[manifest.KindHelmRelease])
		if dropped == 0 {
			continue
		}
		store.Mutate(o.store, hr.Named(), func(h *manifest.HelmRelease) { h.DependsOn = kept })
	}
}

// detectOrphans returns the subset of failed resources that are
// "orphans" — Kustomizations/HelmReleases whose source files sit
// under another Kustomization's spec.path but were never emitted by
// that parent's render output. Such files exist on disk but Flux
// would never see them, so flate downgrades the failure to a
// warning rather than gating the test on stale local files.
func (o *Orchestrator) detectOrphans(failed map[manifest.NamedResource]store.StatusInfo) map[manifest.NamedResource]struct{} {
	out := make(map[manifest.NamedResource]struct{})
	// Collect every loaded Kustomization's spec.path so we can ask
	// "does any parent's path cover this file?" cheaply.
	type parentPath struct {
		id   manifest.NamedResource
		path string
	}
	var parents []parentPath
	for _, obj := range o.store.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.Path == "" {
			continue
		}
		parents = append(parents, parentPath{
			id:   ks.Named(),
			path: filepath.ToSlash(strings.TrimPrefix(strings.TrimSuffix(ks.Path, "/"), "./")) + "/",
		})
	}
	for id := range failed {
		if id.Kind != manifest.KindKustomization && id.Kind != manifest.KindHelmRelease {
			continue
		}
		// A resource that any parent's render also emitted is by
		// definition not orphaned — kustomize-controller saw it.
		if o.rendered.has(id) {
			continue
		}
		file, ok := o.sourceFiles[id]
		if !ok {
			continue
		}
		slashFile := filepath.ToSlash(file)
		var covered bool
		for _, p := range parents {
			if p.id == id {
				continue
			}
			if strings.HasPrefix(slashFile, p.path) {
				covered = true
				break
			}
		}
		if covered {
			out[id] = struct{}{}
		}
	}
	return out
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
	o.attachFilter(change.NewFilter(changes, o.sourceFiles, repoRoot, o.store))
	slog.Debug("changed-only keep set", "size", o.filter.Size(), "items", o.filter.KeepNames())
	return nil
}

// attachFilter records the resolved Filter; controllers consume it
// via Configure() at Run time.
func (o *Orchestrator) attachFilter(f *change.Filter) {
	o.filter = f
}

// Run starts every controller, blocks until the task service drains,
// then aggregates and returns any failures. The post-drain reporting
// + error-string assembly lives in finalize so Run reads as a clean
// start → drain → finalize sequence.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.src.Configure(sourcectrl.FetchOptions{Filter: o.filter})
	o.ksc.Configure(kustomization.Options{
		Filter:        o.filter,
		ParentOf:      o.parentOf,
		RenderTracker: o.rendered,
	})
	o.hrc.Configure(helmrelease.ReconcileOptions{Filter: o.filter})
	o.src.Start(ctx)
	o.ksc.Start(ctx)
	o.hrc.Start(ctx)
	defer o.Stop()

	o.tasks.BlockTillDone()
	return o.finalize()
}

// finalize is the post-BlockTillDone reporting phase: demote orphans
// from Failed → Ready, log per-resource warnings, and assemble the
// aggregated error string. Pulled out of Run so the lifecycle entry
// point reads as start → drain → finalize.
func (o *Orchestrator) finalize() error {
	failed := o.store.FailedResources()
	o.demoteOrphans(failed)
	o.logSummary(failed)
	o.logResourceFailures(failed)

	if len(failed) == 0 {
		// Controllers attribute panics by marking the resource StatusFailed
		// (see kustomization/helmrelease/source controllers). This catches
		// any panic that escaped attribution — e.g. inside a future task
		// dispatched outside the per-resource recover.
		if n := o.tasks.Failures(); n > 0 {
			return fmt.Errorf("%d task(s) panicked without per-resource attribution; check logs", n)
		}
		return nil
	}
	return o.aggregateFailures(failed)
}

// demoteOrphans filters out resources whose source files sit under
// another Kustomization's spec.path but were never emitted by that
// parent's render. Real Flux wouldn't reconcile them either — the
// file walker only loaded them because flate scans the whole tree.
// Surface as warnings instead of failures so the test isn't gated on
// stale on-disk files the user has not wired into their kustomize
// tree. Mutates the failed map in place; the demoted ids land in
// o.orphans for Render() to surface.
func (o *Orchestrator) demoteOrphans(failed map[manifest.NamedResource]store.StatusInfo) {
	o.orphans = map[manifest.NamedResource]string{}
	for id := range o.detectOrphans(failed) {
		info := failed[id]
		o.orphans[id] = info.Message
		o.store.UpdateStatus(id, store.StatusReady, "orphaned (not referenced by any parent kustomization.yaml)")
		slog.Warn("resource orphaned", "id", id.String(),
			"file", o.sourceFiles[id],
			"reason", info.Message)
		delete(failed, id)
	}
}

func (o *Orchestrator) logSummary(failed map[manifest.NamedResource]store.StatusInfo) {
	ksCount := len(o.store.ListObjects(manifest.KindKustomization))
	hrCount := len(o.store.ListObjects(manifest.KindHelmRelease))
	slog.Info("reconcile complete",
		"kustomizations", ksCount,
		"helmReleases", hrCount,
		"failed", len(failed))
	// Surface a clear warning when the scan turned up nothing — covers
	// the "typo'd --path that happens to be an empty directory" case
	// where flate would otherwise look like a silent success.
	if ksCount == 0 && hrCount == 0 {
		slog.Warn("no Flux Kustomization or HelmRelease objects found under --path; check the path is correct")
	}
}

func (o *Orchestrator) logResourceFailures(failed map[manifest.NamedResource]store.StatusInfo) {
	for id, info := range failed {
		// Include the originating source file when known so the user can
		// jump straight to the offending YAML — `flux error: input error:`
		// chains alone don't reveal which spec.path declared a missing
		// directory.
		args := []any{"id", id.String(), "reason", manifest.TrimSentinelPrefix(info.Message)}
		if f := o.sourceFiles[id]; f != "" {
			args = append(args, "file", f)
		}
		slog.Warn("resource failed", args...)
	}
}

func (o *Orchestrator) aggregateFailures(failed map[manifest.NamedResource]store.StatusInfo) error {
	msgs := make([]string, 0, len(failed))
	for id, info := range failed {
		// Strip the `flux error: …: ` chain from user-facing messages —
		// it's three layers of noise before the actual cause. The
		// sentinels are still wired up for errors.Is callers (e.g.
		// embedders branching on ErrObjectNotFound); this only affects
		// the formatted text the CLI ultimately prints.
		msg := manifest.TrimSentinelPrefix(info.Message)
		if f := o.sourceFiles[id]; f != "" {
			msgs = append(msgs, fmt.Sprintf("%s (%s): %s", id.String(), f, msg))
		} else {
			msgs = append(msgs, fmt.Sprintf("%s: %s", id.String(), msg))
		}
	}
	slices.Sort(msgs) // deterministic ordering across runs
	return fmt.Errorf("reconcile completed with %d failure(s):\n  %s",
		len(msgs), strings.Join(msgs, "\n  "))
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
			if art, ok := o.store.GetArtifact(id).(store.RenderedArtifact); ok {
				mans := manifest.DropKinds(art.RenderedManifests(), skip)
				if len(mans) > 0 {
					res.Manifests[id] = mans
				}
			}
		}
	}
	for id, info := range o.store.FailedResources() {
		// Strip the `flux error: <subcategory>:` sentinel chain from the
		// rendered message so embedders get the actual cause first.
		// The Status field and the Store entry stay untouched — only
		// the user-visible string is reshaped, matching the treatment
		// the aggregated Run error and the WARN log already give.
		info.Message = manifest.TrimSentinelPrefix(info.Message)
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

