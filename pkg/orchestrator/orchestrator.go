package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
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
	helmClient.SetSecretGetter(helm.SecretGetter(secretGet))
	fetchers := map[string]source.Fetcher{
		manifest.KindGitRepository:    &git.Fetcher{Cache: cache, Secrets: secretGet},
		manifest.KindExternalArtifact: &external.Fetcher{},
		manifest.KindBucket:           &bucket.Fetcher{Cache: cache, Secrets: secretGet},
		// HelmRepository: existence-only — flate resolves charts via the
		// Helm client's registry/repo machinery directly, the controller
		// just needs the resource to land in Ready so HelmRelease deps
		// unblock.
		manifest.KindHelmRepository: source.ExistenceFetcher{},
	}
	if cfg.EnableOCI {
		fetchers[manifest.KindOCIRepository] = &oci.Fetcher{
			Cache: cache, RegistryConfig: cfg.RegistryConfig, Secrets: secretGet,
		}
	} else {
		// --enable-oci=false: skip the real fetch but still mark each
		// OCIRepository Ready so HRs that dependsOn one don't time out.
		fetchers[manifest.KindOCIRepository] = source.ExistenceFetcher{}
	}
	o := &Orchestrator{
		cfg:   cfg,
		store: st,
		tasks: ts,
		src: &sourcectrl.Controller{
			Store:    st,
			Tasks:    ts,
			Fetchers: fetchers,
		},
		ksc:     &kustomization.Controller{Store: st, Tasks: ts, Staging: staging, WipeSecrets: cfg.WipeSecrets},
		hrc:     &helmrelease.Controller{Store: st, Tasks: ts, Helm: helmClient, Options: cfg.HelmOptions, WipeSecrets: cfg.WipeSecrets},
		helm:    helmClient,
		staging: staging,
	}
	return o, nil
}

// Store returns the underlying object store.
func (o *Orchestrator) Store() *store.Store { return o.store }

// Filter returns the change filter (may be nil-but-non-active).
func (o *Orchestrator) Filter() *change.Filter { return o.filter }

// Bootstrap discovers manifests, applies namespace inheritance, primes
// existence-only sources Ready, and prepares the change filter.
func (o *Orchestrator) Bootstrap(ctx context.Context) error {
	_ = ctx // bootstrap is filesystem-only; ctx kept for future async work

	repoRoot, err := o.seedBootstrapSource()
	if err != nil {
		return err
	}
	if err := o.loadManifests(ctx, repoRoot); err != nil {
		return err
	}
	o.validateDependsOn()
	return o.buildChangeFilter(repoRoot)
}

// seedBootstrapSource publishes a synthetic GitRepository pointing at
// the working tree's repo root, the anchor for spec.path resolution.
func (o *Orchestrator) seedBootstrapSource() (string, error) {
	abs, err := filepath.Abs(o.cfg.Path)
	if err != nil {
		return "", err
	}
	root := findRepoRoot(abs)

	repo := &manifest.GitRepository{
		Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace,
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
	}
	id := repo.Named()
	o.store.AddObject(repo)
	o.store.SetArtifact(id, &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: root,
	})
	o.store.UpdateStatus(id, store.StatusReady, "bootstrap")
	return root, nil
}

// loadManifests scans cfg.Path, then iteratively follows each loaded
// Flux KS's spec.path so a narrow entry (e.g. ./kubernetes/flux/cluster)
// still discovers the apps/ tree it references — without dragging in
// unrelated siblings of the user-supplied path.
func (o *Orchestrator) loadManifests(ctx context.Context, repoRoot string) error {
	o.sourceFiles = map[manifest.NamedResource]string{}

	l := loader.New(o.store)
	l.Options.WipeSecrets = o.cfg.WipeSecrets
	l.SourceRoot = repoRoot
	l.SourceFiles = o.sourceFiles

	scanRoot := repoRoot
	if o.cfg.Path != "" {
		if abs, err := filepath.Abs(o.cfg.Path); err == nil {
			scanRoot = abs
		}
	}
	if info, err := os.Stat(scanRoot); err != nil {
		return fmt.Errorf("--path %q: %w", o.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("--path %q is not a directory", o.cfg.Path)
	}
	scanned := map[string]struct{}{}
	total := 0
	if err := o.loadAt(ctx, l, scanRoot, scanned, &total); err != nil {
		return err
	}
	// Iteratively follow each loaded Flux KS's spec.path so a narrow
	// entry (e.g. ./kubernetes/flux/cluster) still discovers the
	// apps/ tree it references, AND render any unrendered ResourceSet
	// CRs so their generated children become visible to further
	// passes (a ResourceSet may emit Flux Kustomizations whose
	// spec.path then needs expansion). Frontier indexes track what's
	// been handled so we don't rescan or rerender on every pass.
	// PreferExisting protects the initial scan's data from being
	// overwritten if a discovered path aliases a different snapshot.
	l.PreferExisting = true
	ksExpanded := make(map[manifest.NamedResource]struct{})
	rsExpanded := make(map[manifest.NamedResource]struct{})
	for {
		var added int
		for _, obj := range o.store.ListObjects(manifest.KindKustomization) {
			ks, ok := obj.(*manifest.Kustomization)
			if !ok || ks.Path == "" {
				continue
			}
			id := ks.Named()
			if _, ok := ksExpanded[id]; ok {
				continue
			}
			ksExpanded[id] = struct{}{}
			target := filepath.Join(repoRoot, filepath.FromSlash(strings.TrimPrefix(ks.Path, "./")))
			if _, seen := scanned[target]; seen {
				continue
			}
			if !strings.HasPrefix(target+string(filepath.Separator), repoRoot+string(filepath.Separator)) {
				continue
			}
			if err := o.loadAt(ctx, l, target, scanned, &total); err != nil {
				return err
			}
			added++
		}
		for _, obj := range o.store.ListObjects(manifest.KindResourceSet) {
			rs, ok := obj.(*manifest.ResourceSet)
			if !ok {
				continue
			}
			id := rs.Named()
			if _, ok := rsExpanded[id]; ok {
				continue
			}
			rsExpanded[id] = struct{}{}
			n, err := o.renderResourceSet(rs)
			if err != nil {
				return err
			}
			if n > 0 {
				added++
				total += n
			}
		}
		if added == 0 {
			break
		}
	}
	l.PreferExisting = false
	slog.Debug("orchestrator: loaded objects", "count", total, "scanRoot", scanRoot, "sourceRoot", repoRoot)

	loader.ApplyNamespaceInheritance(o.store, o.sourceFiles, repoRoot)
	// Build the parent index after namespace inheritance so the
	// recorded child→parent ids reflect the canonical (post-rewrite)
	// NamedResources the controller will see.
	o.ksc.ParentOf = loader.BuildParentIndex(o.store, o.sourceFiles)
	return nil
}

// loadAt scans dir if not already scanned, marks it, and accumulates
// the loaded object count.
func (o *Orchestrator) loadAt(ctx context.Context, l *loader.Loader, dir string, scanned map[string]struct{}, total *int) error {
	if _, seen := scanned[dir]; seen {
		return nil
	}
	scanned[dir] = struct{}{}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil
	}
	n, err := l.Load(ctx, dir)
	if err != nil {
		return err
	}
	*total += n
	return nil
}

// renderResourceSet evaluates rs.Spec across its inputs and AddObjects
// every resulting recognized Flux resource into the store. The rendered
// children are attributed to the ResourceSet's own source file so the
// change filter treats them as siblings of the ResourceSet definition
// (a ResourceSet change reruns its children's reconciles). Returns
// the count of new objects added so the caller can detect a fixed
// point in the expansion loop.
func (o *Orchestrator) renderResourceSet(rs *manifest.ResourceSet) (int, error) {
	docs, err := resourceset.Render(rs, o.resolveInputProvider)
	if err != nil {
		return 0, err
	}
	srcFile := o.sourceFiles[rs.Named()]
	opts := manifest.ParseDocOptions{WipeSecrets: o.cfg.WipeSecrets}
	added := 0
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("resourceset: skipped doc", "rs", rs.NamespacedName(), "err", err)
			continue
		}
		if _, ok := obj.(*manifest.RawObject); ok {
			// Generic / unrecognized kinds: not something flate
			// reconciles further. Skip them rather than polluting the
			// store with opaque entries.
			continue
		}
		id := obj.Named()
		if o.store.GetObject(id) != nil {
			continue
		}
		o.store.AddObject(obj)
		if srcFile != "" {
			o.sourceFiles[id] = srcFile
		}
		added++
	}
	return added, nil
}

// resolveInputProvider satisfies resourceset.ProviderResolver against
// the orchestrator's object store. Returns the RSIPs matching a single
// spec.inputsFrom reference within the given namespace. Name lookups
// hit a single id; selector matches walk the store's RSIPs in nsScope
// and filter by metadata.labels.
func (o *Orchestrator) resolveInputProvider(ref fluxopv1.InputProviderReference, namespace string) ([]*manifest.ResourceSetInputProvider, error) {
	if ref.Name != "" {
		id := manifest.NamedResource{
			Kind:      manifest.KindResourceSetInputProvider,
			Namespace: namespace,
			Name:      ref.Name,
		}
		obj, _ := o.store.GetObject(id).(*manifest.ResourceSetInputProvider)
		if obj == nil {
			return nil, nil
		}
		return []*manifest.ResourceSetInputProvider{obj}, nil
	}
	if ref.Selector == nil {
		return nil, nil
	}
	var out []*manifest.ResourceSetInputProvider
	for _, obj := range o.store.ListObjects(manifest.KindResourceSetInputProvider) {
		p, ok := obj.(*manifest.ResourceSetInputProvider)
		if !ok || p.Namespace != namespace {
			continue
		}
		match, err := resourceset.MatchSelector(ref.Selector, p.Labels)
		if err != nil {
			return nil, err
		}
		if match {
			out = append(out, p)
		}
	}
	return out, nil
}

// validateDependsOn drops dangling dependsOn references on both
// Kustomizations and HelmReleases so the dependency-wait phase fails
// fast on typos instead of stalling out the full per-dep budget.
// Pre-2026-05-23 only Kustomizations were pruned, so a dangling
// HR→HR dependsOn would hit the 30-second timeout.
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
	// Store immutability contract: clone the struct, set the new slice
	// on the clone, re-AddObject so listeners see the transition cleanly
	// and any in-flight reader's pointer stays valid.
	for _, obj := range ksList {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok {
			continue
		}
		kept, dropped := manifest.FilterDependsOn(ks.DependsOn, known[manifest.KindKustomization])
		if dropped == 0 {
			continue
		}
		clone := *ks
		clone.DependsOn = kept
		o.store.AddObject(&clone)
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
		clone := *hr
		clone.DependsOn = kept
		o.store.AddObject(&clone)
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
		if o.store.WasRendered(id) {
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
		origAbs, err := filepath.Abs(o.cfg.PathOrig)
		if err != nil {
			return fmt.Errorf("--path-orig: %w", err)
		}
		currAbs, err := filepath.Abs(o.cfg.Path)
		if err != nil {
			return fmt.Errorf("--path: %w", err)
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
			"baseline", origAbs, "current", currAbs, "changed_files", cs.Len())
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

// attachFilter wires the same filter into every controller.
func (o *Orchestrator) attachFilter(f *change.Filter) {
	o.filter = f
	o.src.Filter = f
	o.ksc.Filter = f
	o.hrc.Filter = f
}

// Run starts every controller, blocks until the task service drains,
// then aggregates and returns any failures.
func (o *Orchestrator) Run(ctx context.Context) error {
	o.src.Start(ctx)
	o.ksc.Start(ctx)
	o.hrc.Start(ctx)
	defer o.Stop()

	o.tasks.BlockTillDone()

	failed := o.store.FailedResources()
	// Filter out orphans: Kustomizations / HelmReleases whose source
	// files sit under another Kustomization's spec.path but were never
	// emitted by that parent's render. Real Flux would not reconcile
	// them either — the file walker only loaded them because flate
	// scans the whole tree. Surface as warnings instead of failures so
	// the test isn't gated on stale on-disk files the user has not
	// wired into their kustomize tree.
	for id := range o.detectOrphans(failed) {
		info := failed[id]
		o.store.UpdateStatus(id, store.StatusReady, "orphaned (not referenced by any parent kustomization.yaml)")
		slog.Warn("resource orphaned", "id", id.String(),
			"file", o.sourceFiles[id],
			"reason", info.Message)
		delete(failed, id)
	}
	slog.Info("reconcile complete",
		"kustomizations", len(o.store.ListObjects(manifest.KindKustomization)),
		"helmReleases", len(o.store.ListObjects(manifest.KindHelmRelease)),
		"failed", len(failed))

	for id, info := range failed {
		slog.Warn("resource failed", "id", id.String(), "reason", info.Message)
	}

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
	msgs := make([]string, 0, len(failed))
	for id, info := range failed {
		msgs = append(msgs, fmt.Sprintf("%s: %s", id.String(), info.Message))
	}
	return fmt.Errorf("reconcile completed with %d failure(s):\n  %s",
		len(msgs), strings.Join(msgs, "\n  "))
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

// findRepoRoot walks upward from p looking for a .git directory; falls
// back to p itself when there isn't one.
func findRepoRoot(p string) string {
	for cur := p; ; {
		if _, err := os.Stat(filepath.Join(cur, ".git")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return p
		}
		cur = parent
	}
}
