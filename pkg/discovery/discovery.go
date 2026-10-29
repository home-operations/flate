// Package discovery hydrates the store, resolves local source identities, and
// publishes immutable discovery records, ownership claims and resource-graph readers.
// Source aliasing uses authored URL/ref values before parent rendering.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Result summarizes what discovery hydrated into the store.
type Result struct {
	pending *discoverer

	// Binding identifies the entry's actual producer and admitted input.
	Binding Binding
	// Admission applies the same immutable bootstrap policy to runtime source identities.
	Admission *Admission
	// RepoRoot is the bootstrap input's ownership coordinate. Admitted
	// inputs may use a synthetic label with no corresponding directory.
	RepoRoot string
	// Index contains the immutable static discovery facts consumed by rendering.
	Index *manifest.DiscoveryIndex
	// WipeSecrets reflects the loader's WipeSecrets setting. The
	// orchestrator forwards it to lazy-promotion so SOPS Secrets
	// stay wiped on demand the same way they were at file-load.
	WipeSecrets bool
	// DependencySubstitutions is frozen after complete discovery and before rendering.
	DependencySubstitutions values.DependencySubstitutions
}

// Config is the input contract for Run. Store is mandatory.
type Config struct {
	// Input is authoritative over a preseeded bootstrap artifact or disk admission.
	Input *store.SourceArtifact
	// Entry supplies immutable parsed input prepared by Plan for this admission.
	Entry *Entry
	// Path is the scan entry point, defaulting to the admitted input's LocalPath.
	// Relative paths resolve against the working directory.
	Path string
	// RepoRoot is the source root that Kustomization spec.path values
	// resolve against (the GitRepository artifact root). Supplied
	// explicitly by SDK consumers rendering extracted trees that have no
	// .git. Without an admitted bootstrap input, an empty value selects
	// the repository containing Path or Path itself. A preseeded
	// input retains its own coordinate. Path must sit at or under RepoRoot.
	RepoRoot string
	// SelfURLs are the remote URL(s) this tree represents. A user-authored
	// GitRepository matching one is the cluster pulling itself: no ref or
	// HEAD uses the working tree, another local ref uses a cached committed
	// artifact, and unavailable refs or non-HEAD sparse/submodule options
	// warn and fetch normally. SDK consumers supply these for extracted
	// trees with no .git/config; empty uses the working tree's git remotes.
	SelfURLs []string
	// KRMIgnoreFile, when non-empty, is read in place of <Path>/.krmignore
	// for the initial scan; spec.path targets followed afterwards keep
	// their own per-directory .krmignore. Lets one checkout carry several
	// scan scopes (e.g. .krmignore.staging / .krmignore.production) and
	// pick one per run.
	KRMIgnoreFile string
	// Trees is required when discovered packages need a bare Kustomize build.
	Trees               *kustomize.TreeCache
	Store               *store.Store
	WipeSecrets         bool
	RemoteInputs        *source.HTTPInputs
	RemoteRetry         cache.RetryConfig
	StrictSubstitutions bool
	SkipKinds           []string
}

// Run performs the full discovery phase against cfg and writes results
// into cfg.Store. Returns the structural metadata the orchestrator
// needs for change-filter construction and controller wiring.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	d := newDiscoverer(cfg)
	l := d.loader
	_, err := d.resolveEntrySources(ctx, false)
	if err != nil {
		return nil, err
	}
	repoRoot := cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).LocalPath
	if err := d.loadManifests(ctx, repoRoot, true); err != nil {
		return nil, err
	}
	for _, child := range d.loader.Kustomizations() {
		ks := child.Obj.(*manifest.Kustomization)
		if resolved := manifest.MapDependsOn(ks, d.substitutions.Resolve, d.substitutions.Resolve); resolved != ks && cfg.Store.GetObject(ks.Named()) != nil {
			_, doc, emission := cfg.Store.GetEmission(ks.Named())
			emission.Finish()
			cfg.Store.AddObject(resolved, doc)
		}
	}
	var standaloneSecrets []manifest.NamedResource
	if cfg.WipeSecrets {
		for id := range l.Index.Files() {
			if id.Group == "" && id.Kind == manifest.KindSecret && d.cfg.Store.GetObject(id) == nil {
				standaloneSecrets = append(standaloneSecrets, id)
			}
		}
	}
	if err := l.EntryFailure(ctx, repoRoot); err != nil {
		return nil, err
	}
	if err := l.FinalizeIndex(ctx, repoRoot); err != nil {
		return nil, err
	}
	d.promoteOrphans(ctx, standaloneSecrets)
	if err := d.acceptEntry(ctx); err != nil {
		return nil, err
	}
	admission := d.admission()
	if d.binding == (Binding{}) && d.selection == nil {
		admission.declarations = d.applications()
	}
	return &Result{
		pending:                 d,
		Binding:                 d.binding,
		Admission:               admission,
		DependencySubstitutions: d.substitutions,
		RepoRoot:                repoRoot,
		Index:                   l.Index,
		WipeSecrets:             cfg.WipeSecrets,
	}, nil
}

// PrepareEntry completes discovery preparation after validated receipt evidence is admitted.
// It MUST run before reconciliation publishes the discovery index to concurrent readers.
func (r *Result) PrepareEntry(ctx context.Context) error {
	d := r.pending
	r.pending = nil
	if d == nil || r.Binding != (Binding{}) {
		return nil
	}
	root, binding, err := r.Admission.BindEntry(ctx, d.cfg.Store)
	if err != nil || root == nil {
		return err
	}
	index := *r.Index
	index.Records = maps.Clone(index.Records)
	for id, record := range index.Records {
		record.Inputs, record.Refs = slices.Clone(record.Inputs), slices.Clone(record.Refs)
		record.Productions, record.ReferenceData = slices.Clone(record.Productions), slices.Clone(record.ReferenceData)
		record.ContentRoots = maps.Clone(record.ContentRoots)
		for source, paths := range record.ContentRoots {
			record.ContentRoots[source] = slices.Clone(paths)
		}
		index.Records[id] = record
	}
	index.Claims, index.Readers = maps.Clone(index.Claims), maps.Clone(index.Readers)
	for path, ids := range index.Claims {
		index.Claims[path] = slices.Clone(ids)
	}
	for path, readers := range index.Readers {
		index.Readers[path] = slices.Clone(readers)
	}
	d.loader.Index, d.binding = &index, binding
	if d.cfg.Store.GetObject(root.Named()) == nil {
		var doc map[string]any
		if r.Admission.selected != nil {
			doc = r.Admission.selected.document
		}
		d.cfg.Store.AddObject(root, doc)
	}
	source, _, _ := kustomize.SourceInput(d.cfg.Store, root)
	if _, err := d.prepareEntry(ctx, root, source); err != nil {
		d.binding.Failure = err
	}
	if err := d.loader.FinalizeIndex(ctx, r.RepoRoot); err != nil {
		return err
	}
	if err := d.acceptEntry(ctx); err != nil {
		return err
	}
	r.Index, r.Binding = d.loader.Index, d.binding
	return nil
}

type discoverer struct {
	binding         Binding
	selection       *applicationEvidence
	prepared        map[*manifest.Kustomization]entryPreparation
	substitutions   values.DependencySubstitutions
	cfg             Config
	loader          *loader.Loader
	resolvedSources map[manifest.NamedResource]entryResolution
	remotes         map[string]struct{}
	aliasViews      *map[string]aliasView
	hasPins         bool
}

type entryPreparation struct {
	source     *store.SourceArtifact
	children   []store.Child
	receipt    *manifest.RenderReceipt
	registered bool
}

func newDiscoverer(cfg Config) *discoverer {
	l := loader.New(cfg.Store)
	l.Options = loader.Options{WipeSecrets: cfg.WipeSecrets, RemoteInputs: cfg.RemoteInputs, RemoteRetry: cfg.RemoteRetry}
	d := &discoverer{cfg: cfg, loader: l}
	if cfg.Entry == nil || cfg.Entry.prepared == nil {
		return d
	}
	prepared := cfg.Entry.prepared
	input := cfg.Input
	if input == nil {
		input, _ = cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	if input == nil || cfg.WipeSecrets != prepared.wipeSecrets {
		return d
	}
	if !prepared.parsed.Matches(input) || input.LocalPath != prepared.input.LocalPath || input.LocalRoot != prepared.input.LocalRoot ||
		input.Key != prepared.input.Key || input.Content != prepared.input.Content || input.Revision != prepared.input.Revision || input.Digest != prepared.input.Digest {
		return d
	}
	l.Options.ParsedInputs = prepared.parsed
	d.aliasViews = prepared.aliasViews
	return d
}

func (d *discoverer) loadManifests(ctx context.Context, repoRoot string, build bool) error {
	l := d.loader
	l.SourceRoot = repoRoot

	input := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	name, err := filepath.Rel(input.LocalPath, d.cfg.Path)
	if err != nil {
		return err
	}
	if info, err := fs.Stat(input.FS, filepath.ToSlash(name)); err != nil {
		return fmt.Errorf("--path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("--path %q is not a directory", d.cfg.Path)
	}
	total := 0
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if err := d.loadAt(ctx, d.cfg.Path, &total); err != nil {
		return err
	}
	l.IgnoreFile = ""
	// Projection may introduce a new effective source reference. Resolve it
	// before Load admits its spec.path edge to scope construction.
	l.PreferExisting = true
	type expansion struct {
		ks     *manifest.Kustomization
		source *store.SourceArtifact
		object manifest.BaseManifest
	}
	ksExpanded := map[expansion]struct{}{}
	var pending []expansion
	rounds := 0
	for {
		rounds++
		l.Admit(repoRoot)
		_, err := d.resolveEntrySources(ctx, false)
		if err != nil {
			return err
		}
		if d.hasPins {
			d.discardPinnedWorkingTreeFiles(repoRoot)
		}
		kustomizations := d.loader.Kustomizations()
		for _, child := range kustomizations {
			ks := child.Obj.(*manifest.Kustomization)
			art, _ := d.cfg.Store.GetArtifact(ks.SourceID(), store.ArtifactContent).(*store.SourceArtifact)
			key := expansion{ks: ks, source: art, object: d.cfg.Store.GetObject(ks.SourceID())}
			if _, seen := ksExpanded[key]; seen {
				continue
			}
			ksExpanded[key] = struct{}{}
			pending = append(pending, key)
		}
		added := 0
		next := pending[:0]
		for _, key := range pending {
			ks := key.ks
			if ks.IsFragment() {
				continue
			}
			sourceRoot, _, external := loader.SourceView(d.cfg.Store, ks.SourceID(), repoRoot)
			if external && d.cfg.Store.GetObject(ks.SourceID()) == nil {
				next = append(next, key)
				continue
			}
			target := filepath.Join(sourceRoot, manifest.NormalizeClaimBase(ks.Path))
			if !safepath.Contains(sourceRoot, target) {
				continue
			}
			loaded, err := d.loadSourcePath(ctx, target, key.source, &total)
			if err != nil {
				return err
			}
			if !loaded {
				continue
			}
			added++
		}
		pending = next
		if added == 0 {
			missing, err := d.resolveEntrySources(ctx, true)
			if err != nil {
				return err
			}
			if missing != 0 {
				continue
			}
			if !build {
				break
			}
			var discovered []*manifest.Kustomization
			for _, child := range kustomizations {
				discovered = append(discovered, child.Obj.(*manifest.Kustomization))
			}
			d.substitutions = values.NewDependencySubstitutions(discovered)
			ks, err := d.bindEntry(ctx)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			l.Admit(repoRoot)
			if ks == nil {
				break
			}
			source, _, _ := kustomize.SourceInput(d.cfg.Store, ks)
			prepared, err := d.prepareEntry(ctx, ks, source)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				d.binding.Failure = err
				slog.Debug("discovery: entry preparation deferred", "err", err)
				break
			}
			if !prepared {
				break
			}
		}
	}
	l.PreferExisting = false
	d.warnBootstrapAliases(repoRoot)
	slog.Debug("discovery: loaded objects", "count", total, "rounds", rounds, "scan_root", d.cfg.Path, "source_root", repoRoot)
	return nil
}

// Binding keeps actual ownership separate from the paired entry comparison coordinate.
type Binding struct {
	Producer, Source, Key manifest.NamedResource
	Path                  string
	Failure               error
}

func (d *discoverer) bindEntry(ctx context.Context) (*manifest.Kustomization, error) {
	admission := d.admission()
	if d.selection == nil {
		admission.declarations = d.applications()
	}
	root, binding, err := admission.BindEntry(ctx, d.cfg.Store)
	d.selection = admission.selected
	if err != nil || root == nil {
		return nil, err
	}
	if d.cfg.Store.GetObject(root.Named()) == nil {
		var doc map[string]any
		if d.selection != nil {
			doc = d.selection.document
		}
		d.cfg.Store.AddObject(root, doc)
	}
	for owner, prepared := range d.prepared {
		if prepared.registered && owner != root {
			delete(d.prepared, owner)
		}
	}
	d.binding = binding
	return root, nil
}

func (d *discoverer) prepareDocuments(ctx context.Context, ks *manifest.Kustomization, input *store.SourceArtifact) (entryPreparation, error) {
	if previous, ok := d.prepared[ks]; ok && previous.source == input {
		return previous, nil
	}
	prepared, err := kustomize.Prepare(ks, nil)
	if err != nil {
		return entryPreparation{}, err
	}
	docs, receipt, err := kustomize.Render(ctx, d.cfg.Trees, kustomize.RenderRequest{
		Source: input, ApplyIgnore: ks.SourceID() == manifest.BootstrapSourceID || input.InputAlias || input.Content == "", SubPath: prepared.Path, Spec: prepared,
		WipeSecrets: d.cfg.WipeSecrets, StrictSubstitutions: d.cfg.StrictSubstitutions, SkipKinds: d.cfg.SkipKinds, DependencySubstitutions: d.substitutions,
	}, d.cfg.RemoteRetry)
	if err != nil {
		return entryPreparation{}, err
	}
	if err := kustomize.Shape(prepared, docs, d.cfg.StrictSubstitutions); err != nil {
		return entryPreparation{}, err
	}
	built, _, failures := loader.ParseChildren(prepared, docs, manifest.ParseDocOptions{WipeSecrets: d.cfg.WipeSecrets}, d.cfg.SkipKinds)
	if err := manifest.DocumentFailure(failures); err != nil {
		return entryPreparation{}, err
	}
	if d.prepared == nil {
		d.prepared = map[*manifest.Kustomization]entryPreparation{}
	}
	result := entryPreparation{source: input, children: built, receipt: receipt}
	d.prepared[ks] = result
	return result, nil
}

func (d *discoverer) prepareEntry(ctx context.Context, ks *manifest.Kustomization, input *store.SourceArtifact) (bool, error) {
	if d.cfg.Trees == nil || input == nil || ks.PostBuild != nil && len(ks.PostBuild.SubstituteFrom) != 0 {
		return false, nil
	}
	if previous, ok := d.prepared[ks]; ok && previous.registered && previous.source == input {
		return false, nil
	}
	prepared, err := d.prepareDocuments(ctx, ks, input)
	if err != nil {
		return false, err
	}
	d.loader.RegisterBuilt(loader.BuiltInput{Root: filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(ks.Path)), Owner: ks.Named(), Source: d.binding.Source, Reads: kustomize.RenderInputs(prepared.receipt)}, prepared.children)
	prepared.registered = true
	d.prepared[ks] = prepared
	return true, nil
}

func (d *discoverer) acceptEntry(ctx context.Context) error {
	for ks, prepared := range d.prepared {
		if !prepared.registered || ks.Named() != d.binding.Producer {
			continue
		}
		children := prepared.children
		cloned := false
		for i, child := range children {
			if obj := manifest.MapDependsOn(child.Obj, d.substitutions.Resolve, nil); obj != child.Obj {
				if !cloned {
					children = slices.Clone(children)
					cloned = true
				}
				children[i].Obj = obj
			}
		}
		if err := d.cfg.Trees.RecordRender(ctx, kustomize.RenderAcceptance{
			Owner: d.binding.Producer, Receipt: prepared.receipt,
			Records: d.cfg.Store.ChildFootprint(d.binding.Producer, children, d.loader.Index), DependencySubstitutions: d.substitutions,
		}); err != nil {
			return fmt.Errorf("discovery entry %q publish: %w", ks.Path, err)
		}
	}
	return nil
}

// A broad --path scan can overlap a pinned subtree. Its working-tree objects
// must be discarded before the committed subtree supplies discovery metadata.
// Resolved sources must retain their objects and artifacts so a pinned root
// claim cannot replace its own source with a working-tree alias.
func (d *discoverer) discardPinnedWorkingTreeFiles(repoRoot string) {
	d.loader.Admit(repoRoot)
	children := d.loader.Kustomizations()
	occurrences := make([]loader.Occurrence, len(children))
	for i, child := range children {
		occurrences[i] = loader.Occurrence{Identity: child.Obj.Named(), Object: child.Obj}
	}
	prefixes, pinnedSources := d.pinnedWorkingTreeClaims(repoRoot, occurrences)
	if len(prefixes) == 0 {
		return
	}
	for id, record := range d.loader.Index.Files() {
		if _, resolved := d.resolvedSources[id]; resolved {
			continue
		}
		if _, pinned := pinnedSources[id]; pinned {
			continue
		}
		if !d.pinnedWorkingTreeFile(repoRoot, prefixes, id, record) {
			continue
		}
		d.cfg.Store.DeleteObject(id)
		delete(d.loader.Index.Records, id)
	}
}

// scratch MUST be caller-owned; package-owned occurrence slices are immutable.
func (d *discoverer) pinnedWorkingTreeClaims(repoRoot string, scratch []loader.Occurrence) ([]manifest.KSClaim, map[manifest.NamedResource]struct{}) {
	pinned := scratch[:0]
	sources := map[manifest.NamedResource]struct{}{}
	for _, o := range scratch {
		obj := o.Object
		if obj.Named().Namespace != o.Identity.Namespace {
			obj = obj.WithNamespace(o.Identity.Namespace)
		}
		ref, _, hasPath := obj.SourcePath()
		if !hasPath {
			continue
		}
		if _, pin, _ := loader.SourceView(d.cfg.Store, ref, repoRoot); pin {
			pinned = append(pinned, o)
			sources[ref] = struct{}{}
		}
	}
	return d.loader.OccurrenceClaims(repoRoot, pinned), sources
}

func (d *discoverer) pinnedWorkingTreeFile(repoRoot string, prefixes []manifest.KSClaim, id manifest.NamedResource, record manifest.DiscoveryRecord) bool {
	if len(prefixes) == 0 {
		return false
	}
	file := record.File
	if !safepath.Contains(repoRoot, filepath.Join(repoRoot, filepath.FromSlash(file))) {
		for source := range d.resolvedSources {
			root, ok := d.cfg.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			if ok && root.InputAlias && safepath.Contains(root.LocalPath, record.Path) {
				rel, _ := filepath.Rel(root.LocalPath, record.Path)
				file, _ = filepath.Rel(repoRoot, filepath.Join(root.LocalRoot, rel))
				file = filepath.ToSlash(file)
				break
			}
		}
		if !safepath.Contains(repoRoot, filepath.Join(repoRoot, filepath.FromSlash(file))) {
			return false
		}
	}
	_, owned := manifest.StrictParent(prefixes, file, id, nil)
	return owned
}

// The Loader deduplicates unchanged views; unavailable followed paths fail during reconciliation.
func (d *discoverer) loadAt(ctx context.Context, dir string, total *int) error {
	n, err := d.loader.Load(ctx, dir)
	if d.loader.IgnoreFile == "" && errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	*total += n
	return nil
}

func (d *discoverer) loadSourcePath(ctx context.Context, dir string, input *store.SourceArtifact, total *int) (bool, error) {
	if err := d.loadAt(ctx, dir, total); err != nil {
		if errors.Is(err, fs.ErrPermission) && input != nil && input.FS != nil {
			name, _ := filepath.Rel(input.LocalPath, dir)
			if _, statErr := fs.Stat(input.FS, filepath.ToSlash(name)); errors.Is(statErr, fs.ErrPermission) {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}
