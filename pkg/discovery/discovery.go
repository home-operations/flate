// Package discovery owns flate's filesystem-to-store hydration phase:
// walking the user's working tree, expanding spec.path references,
// aliasing in-cluster-bootstrapped sources, rendering ResourceSets, and
// computing the structural-parent index. The output is everything the
// reconcile phase needs to start firing controllers — repo root,
// per-object source files, and the parent index.
//
// Splitting this out of the orchestrator turns a 750-line god-object
// into two ~350-line files with one clean interface between them. The
// load phase is independently testable (no controller wiring or
// task service required) and the orchestrator now reads as pure
// reconcile orchestration.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

// Result summarizes what discovery hydrated into the store.
type Result struct {
	// RepoRoot is the resolved working-tree anchor (with .git ancestor
	// walk + symlink resolution applied).
	RepoRoot string
	// SourceFiles maps each loaded resource to the repo-relative path
	// it was parsed from. Consumed by the change filter.
	SourceFiles map[manifest.NamedResource]string
	// ParentOf maps each reconcilable resource (Kustomization or
	// HelmRelease) to its structural-parent Kustomization — the KS
	// whose spec.path is the deepest strict ancestor of the child's
	// source file. KS children honor it as a depwait dep so any
	// parent-render-time spec mutations (replacements: injecting
	// targetNamespace) are visible before the child renders;
	// HR children honor it so the first render reads the post-patch
	// spec (driftDetection / upgrade strategy / CRD policy overrides
	// applied at the cluster-KS level) instead of the pre-patch
	// file-loaded copy. Keyed by NamedResource so KS and HR entries
	// never collide. Empty when no parent enforcement applies.
	ParentOf map[manifest.NamedResource]manifest.NamedResource
}

// Config is the input contract for Run. Store is mandatory.
type Config struct {
	Path        string
	Store       *store.Store
	WipeSecrets bool
}

// Run performs the full discovery phase against cfg and writes results
// into cfg.Store. Returns the structural metadata the orchestrator
// needs for change-filter construction and controller wiring.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	l := loader.New(cfg.Store)
	l.Options.WipeSecrets = cfg.WipeSecrets
	d := &discoverer{
		cfg:         cfg,
		loader:      l,
		sourceFiles: map[manifest.NamedResource]string{},
	}
	repoRoot, err := d.seedBootstrapSource()
	if err != nil {
		return nil, err
	}
	if err := d.loadManifests(ctx, repoRoot); err != nil {
		return nil, err
	}
	d.aliasBootstrapSources(repoRoot)
	loader.ApplyNamespaceInheritance(d.cfg.Store, d.sourceFiles, repoRoot)
	// Unified parent index over every reconcilable kind that uses a
	// parent gate. KS and HR keys never collide because NamedResource
	// includes Kind; downstream controllers look up by their own id
	// and naturally filter to their own kind.
	parentOf := mergeParents(
		loader.BuildParentIndex(d.cfg.Store, d.sourceFiles),
		loader.BuildParentIndexForKind(d.cfg.Store, d.sourceFiles, manifest.KindHelmRelease),
	)
	return &Result{
		RepoRoot:    repoRoot,
		SourceFiles: d.sourceFiles,
		ParentOf:    parentOf,
	}, nil
}

// mergeParents combines per-kind parent maps into one. Earlier
// arguments win on collision (which can't happen in practice — keys
// are NamedResource with distinct Kind components — but the rule is
// explicit so future callers don't accidentally clobber a KS parent
// with an HR-built rebuild).
func mergeParents(maps ...map[manifest.NamedResource]manifest.NamedResource) map[manifest.NamedResource]manifest.NamedResource {
	out := map[manifest.NamedResource]manifest.NamedResource{}
	for _, m := range maps {
		for k, v := range m {
			if _, exists := out[k]; exists {
				continue
			}
			out[k] = v
		}
	}
	return out
}

type discoverer struct {
	cfg         Config
	loader      *loader.Loader
	sourceFiles map[manifest.NamedResource]string
}

// seedBootstrapSource publishes a synthetic GitRepository pointing at
// the working tree's repo root — the anchor for spec.path resolution
// when a Kustomization carries no explicit sourceRef.
func (d *discoverer) seedBootstrapSource() (string, error) {
	abs, err := ResolveScanPath(d.cfg.Path)
	if err != nil {
		return "", err
	}
	root := FindRepoRoot(abs)

	repo := &manifest.GitRepository{
		Name: manifest.BootstrapSourceID.Name, Namespace: manifest.BootstrapSourceID.Namespace,
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + root},
	}
	id := repo.Named()
	d.cfg.Store.AddObject(repo)
	d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: root,
	})
	d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap")
	return root, nil
}

// loadManifests scans cfg.Path, then iteratively follows each loaded
// Flux Kustomization's spec.path until a fixed point is reached.
// Interleaved with KS expansion is ResourceSet rendering: a parent
// KS may emit a ResourceSet which itself emits child Kustomizations
// referencing new spec.paths the file walker hasn't visited yet.
func (d *discoverer) loadManifests(ctx context.Context, repoRoot string) error {
	l := d.loader
	l.SourceRoot = repoRoot
	l.SourceFiles = d.sourceFiles

	scanRoot := repoRoot
	if d.cfg.Path != "" {
		if abs, err := ResolveScanPath(d.cfg.Path); err == nil {
			scanRoot = abs
		}
	}
	if info, err := os.Stat(scanRoot); err != nil {
		return fmt.Errorf("--path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("--path %q is not a directory", d.cfg.Path)
	}
	scanned := map[string]struct{}{}
	total := 0
	if err := d.loadAt(ctx, l, scanRoot, scanned, &total); err != nil {
		return err
	}

	// Fixed-point expansion: each pass renders Kustomizations the prior
	// pass discovered, plus ResourceSets that may emit further KSes.
	// PreferExisting lets repeated AddObject re-emission be a no-op so
	// the loop terminates on convergence (no new objects added).
	//
	// ResourceSets are re-rendered every iteration rather than memoized,
	// because a RS's inputsFrom selector may match RSIPs that only
	// arrive after a downstream Kustomization chain expands. Without
	// the retry, a RS whose RSIPs are produced by a child KS renders
	// to zero docs on first pass and never recovers — observed in
	// tholinka/home-ops where `dragonfly-acls` (a Permute RS) selects
	// RSIPs created by an unrelated `dragonfly/manual` component
	// applied through `renovate-operator-jobs-jobs`. Re-rendering is
	// safe: renderResourceSet skips already-present objects in the
	// store, so a steady-state RS contributes 0 new docs and the loop
	// converges via the `added == 0` exit.
	l.PreferExisting = true
	ksExpanded := map[manifest.NamedResource]struct{}{}
	for {
		added := 0
		for _, obj := range d.cfg.Store.ListObjects(manifest.KindKustomization) {
			ks, ok := obj.(*manifest.Kustomization)
			if !ok {
				continue
			}
			id := ks.Named()
			if _, seen := ksExpanded[id]; seen {
				continue
			}
			if ks.Path == "" {
				ksExpanded[id] = struct{}{}
				continue
			}
			ksExpanded[id] = struct{}{}
			target := filepath.Join(repoRoot, filepath.FromSlash(stripDotSlash(ks.Path)))
			if _, seen := scanned[target]; seen {
				continue
			}
			if !pathUnderRoot(target, repoRoot) {
				continue
			}
			if err := d.loadAt(ctx, l, target, scanned, &total); err != nil {
				return err
			}
			added++
		}
		for _, obj := range d.cfg.Store.ListObjects(manifest.KindResourceSet) {
			rs, ok := obj.(*manifest.ResourceSet)
			if !ok {
				continue
			}
			n, err := d.renderResourceSet(rs)
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
	slog.Debug("discovery: loaded objects", "count", total, "scanRoot", scanRoot, "sourceRoot", repoRoot)
	return nil
}

// loadAt scans dir if not already scanned, marks it, and accumulates
// the loaded object count.
func (d *discoverer) loadAt(ctx context.Context, l *loader.Loader, dir string, scanned map[string]struct{}, total *int) error {
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
func (d *discoverer) renderResourceSet(rs *manifest.ResourceSet) (int, error) {
	docs, err := resourceset.Render(rs, d.resolveInputProvider)
	if err != nil {
		return 0, err
	}
	srcFile := d.sourceFiles[rs.Named()]
	opts := manifest.ParseDocOptions{WipeSecrets: d.cfg.WipeSecrets}
	added := 0
	for _, doc := range docs {
		obj, err := manifest.ParseDoc(doc, opts)
		if err != nil {
			slog.Debug("resourceset: skipped doc", "rs", rs.NamespacedName(), "err", err)
			continue
		}
		if _, ok := obj.(*manifest.RawObject); ok {
			// Generic / unrecognized kinds: not something flate
			// reconciles further. Skipped here; the orchestrator's
			// post-Run RS expansion pass picks them up and attributes
			// them to the owning KS for `flate build` visibility.
			// That late pass sees RSIPs emitted from KS reconcile
			// (kustomize-substituted dragonfly-${APP} style) which
			// this discovery pass would miss.
			continue
		}
		id := obj.Named()
		if d.cfg.Store.GetObject(id) != nil {
			continue
		}
		d.cfg.Store.AddObject(obj)
		if srcFile != "" {
			d.sourceFiles[id] = srcFile
		}
		added++
	}
	return added, nil
}

// resolveInputProvider satisfies resourceset.ProviderResolver against
// the discoverer's object store. Name lookups hit a single id;
// selector matches walk the store's RSIPs in the requested namespace
// and filter by metadata.labels.
func (d *discoverer) resolveInputProvider(ref fluxopv1.InputProviderReference, namespace string) ([]*manifest.ResourceSetInputProvider, error) {
	if ref.Name != "" {
		id := manifest.NamedResource{
			Kind:      manifest.KindResourceSetInputProvider,
			Namespace: namespace,
			Name:      ref.Name,
		}
		obj, _ := d.cfg.Store.GetObject(id).(*manifest.ResourceSetInputProvider)
		if obj == nil {
			return nil, nil
		}
		return []*manifest.ResourceSetInputProvider{obj}, nil
	}
	if ref.Selector == nil {
		return nil, nil
	}
	var out []*manifest.ResourceSetInputProvider
	for _, obj := range d.cfg.Store.ListObjects(manifest.KindResourceSetInputProvider) {
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

// aliasBootstrapSources seeds a working-tree SourceArtifact for every
// GitRepository referenced by a loaded Kustomization whose definition
// isn't in the repo itself. Targets `flux bootstrap` / flux-operator
// FluxInstance patterns: the cluster's root GitRepository is created
// out-of-band (the source that delivers the rest of the manifests
// cannot, by construction, be one of the manifests it delivers), so
// no static manifest exists in the tree to discover. Without aliasing,
// every Kustomization referencing it via `sourceRef` fails depwait
// with "dependency not found" (issue #199).
//
// All namespaces are aliased, not just `flux-system` — the convention
// of running Flux in a non-default namespace (e.g. `gitops-system`)
// is widespread, and the bootstrap-source-points-at-the-local-tree
// property is identical regardless of where the user happens to deploy
// Flux. A typo'd sourceRef name will silently render against the
// working tree instead of failing fast — same trade-off the
// `flux-system` path already accepted.
func (d *discoverer) aliasBootstrapSources(repoRoot string) {
	known := make(map[manifest.NamedResource]struct{})
	for _, obj := range d.cfg.Store.ListObjects(manifest.KindGitRepository) {
		known[obj.Named()] = struct{}{}
	}
	seen := make(map[manifest.NamedResource]struct{})
	var aliased []manifest.NamedResource
	for _, obj := range d.cfg.Store.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.SourceKind != manifest.KindGitRepository {
			continue
		}
		id := manifest.NamedResource{Kind: manifest.KindGitRepository, Namespace: ks.SourceNamespace, Name: ks.SourceName}
		if _, ok := known[id]; ok {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		alias := &manifest.GitRepository{
			Name: id.Name, Namespace: id.Namespace,
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + repoRoot},
		}
		d.cfg.Store.AddObject(alias)
		d.cfg.Store.SetArtifact(id, &store.SourceArtifact{
			Kind: manifest.KindGitRepository,
			URL:  alias.URL, LocalPath: repoRoot,
		})
		d.cfg.Store.UpdateStatus(id, store.StatusReady, "bootstrap alias")
		slog.Debug("discovery: aliased bootstrap GitRepository",
			"id", id.String(), "localPath", repoRoot)
		aliased = append(aliased, id)
	}
	// Multiple unresolved GitRepositories is the cross-repo footgun:
	// each gets aliased to the SAME working tree, so a real upstream
	// shared-infra GitRepository would render against the wrong files
	// without any user-visible signal. Warn so an operator can spot
	// the divergence; the single-source case stays Debug because
	// that's the intended flux-bootstrap shape.
	if len(aliased) > 1 {
		names := make([]string, len(aliased))
		for i, id := range aliased {
			names[i] = id.String()
		}
		slog.Warn("discovery: aliased multiple GitRepositories to the working tree; cross-repo refs render against the wrong tree",
			"count", len(aliased), "ids", names, "localPath", repoRoot)
	}
}

// ResolveScanPath normalizes a user-supplied --path / --path-orig:
// absolute, with symlinks resolved. Without symlink resolution
// filepath.WalkDir doesn't follow root-level symlinks, producing an
// empty manifest set without any error indication — a footgun.
func ResolveScanPath(p string) (string, error) {
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return abs, nil
		}
		return "", fmt.Errorf("resolve --path %q: %w", p, err)
	}
	return resolved, nil
}

// FindRepoRoot walks upward from p looking for a .git directory; falls
// back to p itself when there isn't one.
func FindRepoRoot(p string) string {
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

func stripDotSlash(p string) string {
	for len(p) > 0 && (p[0] == '.' || p[0] == '/') {
		if p[0] == '.' && (len(p) == 1 || p[1] == '/') {
			p = p[1:]
			continue
		}
		if p[0] == '/' {
			p = p[1:]
			continue
		}
		break
	}
	return p
}

func pathUnderRoot(target, root string) bool {
	t := filepath.Clean(target) + string(filepath.Separator)
	r := filepath.Clean(root) + string(filepath.Separator)
	return len(t) >= len(r) && t[:len(r)] == r
}
