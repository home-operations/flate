package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/helmrelease"
	"github.com/home-operations/flate/pkg/controllers/kustomization"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
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
	ts := task.New()
	cache := cmp.Or(cfg.SourceCache, source.NewCache(filepath.Join(cacheRoot, "sources")))
	secretGet := func(ns, name string) *manifest.Secret {
		obj := st.GetByName(manifest.KindSecret, ns, name)
		s, _ := obj.(*manifest.Secret)
		return s
	}
	fetchers := map[string]source.Fetcher{
		manifest.KindGitRepository:    &source.GitFetcher{Cache: cache, Secrets: secretGet},
		manifest.KindExternalArtifact: &source.ExternalArtifactFetcher{},
		manifest.KindBucket:           &source.BucketFetcher{Cache: cache, Secrets: secretGet},
	}
	if cfg.EnableOCI {
		fetchers[manifest.KindOCIRepository] = &source.OCIFetcher{
			Cache: cache, RegistryConfig: cfg.RegistryConfig,
		}
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

// Tasks returns the task scheduler.
func (o *Orchestrator) Tasks() *task.Service { return o.tasks }

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
	if err := o.loadManifests(repoRoot); err != nil {
		return err
	}
	o.validateDependsOn()
	o.markExistenceOnlyReady()
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
		Name: "flux-system", Namespace: "flux-system",
		URL: "file://" + root,
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
func (o *Orchestrator) loadManifests(repoRoot string) error {
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
	if err := o.loadAt(l, scanRoot, scanned, &total); err != nil {
		return err
	}
	// Iteratively follow each loaded Flux KS's spec.path so a narrow
	// entry (e.g. ./kubernetes/flux/cluster) still discovers the
	// apps/ tree it references. A frontier index tracks which KSes
	// have been expanded so we don't rescan the store on every pass.
	// PreferExisting protects the initial scan's data from being
	// overwritten if a discovered path aliases a different snapshot.
	l.PreferExisting = true
	expanded := make(map[manifest.NamedResource]struct{})
	for {
		var added int
		for _, obj := range o.store.ListObjects(manifest.KindKustomization) {
			ks, ok := obj.(*manifest.Kustomization)
			if !ok || ks.Path == "" {
				continue
			}
			id := ks.Named()
			if _, ok := expanded[id]; ok {
				continue
			}
			expanded[id] = struct{}{}
			target := filepath.Join(repoRoot, filepath.FromSlash(strings.TrimPrefix(ks.Path, "./")))
			if _, seen := scanned[target]; seen {
				continue
			}
			if !strings.HasPrefix(target+string(filepath.Separator), repoRoot+string(filepath.Separator)) {
				continue
			}
			if err := o.loadAt(l, target, scanned, &total); err != nil {
				return err
			}
			added++
		}
		if added == 0 {
			break
		}
	}
	l.PreferExisting = false
	slog.Debug("orchestrator: loaded objects", "count", total, "scan", scanRoot, "source-root", repoRoot)

	loader.ApplyNamespaceInheritance(o.store, o.sourceFiles, repoRoot)
	return nil
}

// loadAt scans dir if not already scanned, marks it, and accumulates
// the loaded object count.
func (o *Orchestrator) loadAt(l *loader.Loader, dir string, scanned map[string]struct{}, total *int) error {
	if _, seen := scanned[dir]; seen {
		return nil
	}
	scanned[dir] = struct{}{}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil
	}
	n, err := l.Load(dir)
	if err != nil {
		return err
	}
	*total += n
	return nil
}

// validateDependsOn drops dangling dependsOn references so the
// dependency-wait phase never blocks on a resource that will never
// appear.
func (o *Orchestrator) validateDependsOn() {
	known := make(map[string]struct{})
	ksList := o.store.ListObjects(manifest.KindKustomization)
	for _, obj := range ksList {
		if ks, ok := obj.(*manifest.Kustomization); ok {
			known[ks.NamespacedName()] = struct{}{}
		}
	}
	for _, obj := range ksList {
		if ks, ok := obj.(*manifest.Kustomization); ok {
			ks.ValidateDependsOn(known)
		}
	}
}

// markExistenceOnlyReady marks HelmRepository (always) and
// OCIRepository (when source-controller is disabled) as Ready so
// HelmRelease waits can resolve without a real fetch.
func (o *Orchestrator) markExistenceOnlyReady() {
	for _, obj := range o.store.ListObjects(manifest.KindHelmRepository) {
		o.store.UpdateStatus(obj.Named(), store.StatusReady, "")
	}
	if o.cfg.EnableOCI {
		return
	}
	for _, obj := range o.store.ListObjects(manifest.KindOCIRepository) {
		o.store.UpdateStatus(obj.Named(), store.StatusReady, "")
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
