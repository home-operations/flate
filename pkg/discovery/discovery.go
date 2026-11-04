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

	"github.com/home-operations/flate/pkg/change"
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
	pending *Session

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
	// Projections shares source-free facts across concurrent Plan calls.
	Projections *manifest.ProducerProjections
	// Selection controls restricted declaration construction.
	Selection manifest.Selection
	// Loader borrows the invocation's sole declaration loader.
	Loader  *loader.Loader
	session *Session
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
//
//nolint:gocritic // Public discovery preserves its configuration value snapshot.
func Run(ctx context.Context, cfg Config) (*Result, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	if cfg.session == nil && (cfg.Selection.Changed || len(cfg.Selection.Roots) != 0) {
		return runSelected(ctx, &cfg)
	}
	d := cfg.session
	if d == nil {
		d = newDiscoverer(cfg)
	}
	l := d.loader
	if cfg.session == nil {
		if _, err := d.resolveEntrySources(ctx, false); err != nil {
			return nil, err
		}
	}
	repoRoot := d.cfg.Input.LocalPath
	if cfg.session == nil {
		if err := d.loadManifests(ctx, repoRoot, true); err != nil {
			return nil, err
		}
	}
	return d.finishDiscovery(ctx, &cfg, repoRoot, l)
}

func runSelected(ctx context.Context, cfg *Config) (*Result, error) {
	if cfg.Loader == nil {
		cfg.Loader = loader.New(cfg.Store)
	}
	if cfg.Loader.Options.Filter == nil {
		cfg.Loader.Options.Filter = change.NewFilter()
	}
	session, err := Begin(ctx, *cfg)
	if err != nil {
		return nil, err
	}
	for {
		proposal, err := cfg.Loader.Prepare(ctx, store.Child{}, nil, nil)
		if err != nil {
			return nil, err
		}
		advanced, err := session.Advance(ctx, proposal)
		if err != nil {
			return nil, err
		}
		if !advanced {
			added, retry := cfg.Loader.Options.Filter.AddEmitted(
				proposal.Needed,
				proposal.Selected,
				proposal.Dirty,
				proposal.Revision,
			)
			if retry {
				return nil, manifest.NewError(
					manifest.ErrInput,
					nil,
					"discovery membership changed during construction",
				)
			}
			for _, id := range added {
				cfg.Store.SetIncluded(id, true)
			}
			return session.Freeze(ctx)
		}
	}
}

func (d *Session) finishDiscovery(
	ctx context.Context,
	cfg *Config,
	repoRoot string,
	l *loader.Loader,
) (*Result, error) {
	for _, child := range d.loader.Kustomizations() {
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		if resolved := manifest.MapDependsOn(
			ks,
			d.substitutions.Resolve,
			d.substitutions.Resolve,
		); resolved != ks &&
			cfg.Store.GetObject(ks.Named()) != nil {
			_, doc, emission := cfg.Store.GetEmission(ks.Named())
			emission.Finish()
			cfg.Store.AddObject(resolved, doc)
		}
	}
	standaloneSecrets := d.standaloneSecretIDs(l, cfg.WipeSecrets)
	if d.binding.Failure == nil {
		if err := l.EntryFailure(ctx, repoRoot); err != nil {
			return nil, err
		}
	}
	if err := l.FinalizeIndex(ctx, repoRoot, true); err != nil {
		return nil, err
	}
	if !cfg.Selection.Changed && len(cfg.Selection.Roots) == 0 {
		if err := d.promoteOrphans(ctx, standaloneSecrets); err != nil {
			return nil, err
		}
	}
	if err := d.acceptPrepared(ctx); err != nil {
		return nil, err
	}
	if !cfg.Selection.Changed && len(cfg.Selection.Roots) == 0 && (d.binding != (Binding{}) || cfg.Trees == nil) {
		l.Retire()
	}
	return &Result{
		pending:                 d,
		Binding:                 d.binding,
		Admission:               d.admission(),
		DependencySubstitutions: d.substitutions,
		RepoRoot:                repoRoot,
		Index:                   l.Index,
		WipeSecrets:             cfg.WipeSecrets,
	}, nil
}

func (d *Session) standaloneSecretIDs(l *loader.Loader, wipeSecrets bool) []manifest.NamedResource {
	var standaloneSecrets []manifest.NamedResource
	if wipeSecrets {
		for id := range l.Index.Files() {
			if id.Group == "" && id.Kind == manifest.KindSecret && d.cfg.Store.GetObject(id) == nil {
				standaloneSecrets = append(standaloneSecrets, id)
			}
		}
	}
	return standaloneSecrets
}

// PrepareEntry completes discovery preparation after validated receipt evidence is admitted.
// It MUST run before reconciliation publishes the discovery index to concurrent readers.
func (r *Result) PrepareEntry(ctx context.Context, changes *change.Set) error {
	d := r.pending
	r.pending = nil

	if d == nil || r.Binding != (Binding{}) {
		return nil
	}
	if d.cfg.Trees != nil && d.needsEntry(changes) {
		root, _, err := r.Admission.entry(ctx, d.cfg.Store, d.cfg.Store.ListObjects("", ""))
		if err != nil {
			return err
		}
		if root == nil {
			if err := d.qualifyEntry(ctx, r.Admission); err != nil {
				return err
			}
		}
	}
	root, binding, err := r.Admission.BindEntry(ctx, d.cfg.Store)
	if err != nil || root == nil {
		return err
	}
	d.loader.Index, d.binding = r.cloneIndex(), binding
	d.cfg.Store.AddObject(root)
	source, _, _ := kustomize.SourceInput(d.cfg.Store, root)
	if source != nil && d.cfg.Trees != nil {
		if err := d.prepareProducer(ctx, root, source); err != nil {
			d.binding.Failure = err
		}
	}
	if err := d.loader.FinalizeIndex(ctx, r.RepoRoot, true); err != nil {
		return err
	}
	if err := d.acceptPrepared(ctx); err != nil {
		return err
	}
	r.Index, r.Binding = d.loader.Index, d.binding
	return nil
}

func (r *Result) cloneIndex() *manifest.DiscoveryIndex {
	index := *r.Index
	index.Records = maps.Clone(index.Records)
	for id, record := range index.Records { //nolint:gocritic // Map iteration preserves a value snapshot without an extra lookup.
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
	return &index
}

// Session retains invocation-local declaration construction until Freeze.
type Session struct {
	binding         Binding
	entryState      entryQualification
	entryApplier    manifest.NamedResource
	prepared        map[*manifest.Kustomization]declarationPreparation
	substitutions   values.DependencySubstitutions
	cfg             Config
	loader          *loader.Loader
	resolvedSources map[manifest.NamedResource]entryResolution
	remotes         map[string]struct{}
	aliasViews      map[string]*store.SourceArtifact
	hasPins         bool
	result          *Result
}

type declarationPreparation struct {
	source     *store.SourceArtifact
	children   []store.Child
	receipt    *manifest.RenderReceipt
	content    any
	registered bool
}

func newDiscoverer(cfg Config) *Session {
	l := cfg.Loader
	if l == nil {
		l = loader.New(cfg.Store)
	}
	l.Options.WipeSecrets, l.Options.RemoteInputs, l.Options.RemoteRetry = cfg.WipeSecrets, cfg.RemoteInputs, cfg.RemoteRetry
	l.Options.Selection = cfg.Selection
	if cfg.Projections != nil {
		l.Options.Projections = cfg.Projections
	}
	d := &Session{cfg: cfg, loader: l, aliasViews: map[string]*store.SourceArtifact{}}
	d.cfg.Entry = nil
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
	if !prepared.parsed.Matches(input) || input.LocalPath != prepared.input.LocalPath ||
		input.LocalRoot != prepared.input.LocalRoot ||
		input.Key != prepared.input.Key ||
		input.Content != prepared.input.Content ||
		input.Revision != prepared.input.Revision ||
		input.Digest != prepared.input.Digest {
		return d
	}
	l.Options.ParsedInputs = prepared.parsed
	d.aliasViews = maps.Clone(prepared.aliasViews)
	return d
}

func (d *Session) loadManifests(ctx context.Context, repoRoot string, build bool) error {
	l := d.loader
	l.SourceRoot = repoRoot

	input := d.cfg.Input
	if input == nil {
		input, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		d.cfg.Input = input
	}
	name, err := filepath.Rel(input.LocalPath, d.cfg.Path)
	if err != nil {
		return fmt.Errorf("rel: %w", err)
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
			ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
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
			var discovered []*manifest.Kustomization
			for _, child := range kustomizations {
				discovered = append(
					discovered,
					child.Obj.(*manifest.Kustomization), //nolint:errcheck // This branch handles an admitted Kustomization.
				)
			}
			d.substitutions = values.NewDependencySubstitutions(discovered)
			if !build {
				break
			}
			ks, err := d.bindEntry(ctx, repoRoot)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("context: %w", err)
			}
			l.Admit(repoRoot)
			if ks == nil {
				break
			}
			source, _, _ := kustomize.SourceInput(d.cfg.Store, ks)
			if d.cfg.Trees == nil || source == nil || ks.PostBuild != nil && len(ks.PostBuild.SubstituteFrom) != 0 {
				break
			}
			if previous, ok := d.prepared[ks]; ok && previous.registered && previous.source == source {
				break
			}
			if err := d.prepareProducer(ctx, ks, source); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				d.binding.Failure = err
				slog.Debug("discovery: declaration preparation deferred", "err", err)
				break
			}
		}
	}
	l.PreferExisting = false
	d.warnBootstrapAliases(repoRoot)
	slog.Debug(
		"discovery: loaded objects",
		"count",
		total,
		"rounds",
		rounds,
		"scan_root",
		d.cfg.Path,
		"source_root",
		repoRoot,
	)
	return nil
}

// Binding keeps actual ownership separate from the paired entry comparison coordinate.
type Binding struct {
	Producer, Source, Key manifest.NamedResource
	Path                  string
	Failure               error
	Entry                 *kustomize.EntryEvidence
}

func (d *Session) bindEntry(ctx context.Context, repoRoot string) (*manifest.Kustomization, error) {
	input := d.cfg.Input
	if input == nil {
		input, _ = d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	internal, err := d.entryRoot(ctx, input)
	if err != nil || internal == nil {
		return nil, err
	}
	var matches []store.Child
	for _, child := range d.loader.Kustomizations() {
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		if d.matchesEntryPath(ks, input, repoRoot) {
			matches = append(matches, child)
		}
	}
	slices.SortFunc(matches, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	ks := internal
	for _, child := range matches {
		candidate := child.Obj.(*manifest.Kustomization) //nolint:errcheck // Matches retain admitted Kustomization values.
		if !d.entryCandidateReady(ctx, candidate) {
			continue
		}
		ks = candidate
		d.entryApplier = candidate.Named()
		d.cfg.Store.AddObject(ks, child.Doc)
		if previous := d.binding.Producer; previous.ReportHidden() && previous != ks.Named() {
			d.cfg.Store.DeleteObject(previous)
		}
		break
	}
	if ks == internal {
		if previous, ok := d.cfg.Store.GetObject(internal.Named()).(*manifest.Kustomization); ok {
			ks = previous
		} else {
			d.cfg.Store.AddObject(ks)
		}
	}
	source := ks.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	failure := d.binding.Failure
	if d.binding.Producer != ks.Named() {
		failure = nil
	}
	d.binding = Binding{
		Producer: ks.Named(),
		Source:   source,
		Path:     manifest.NormalizeClaimBase(ks.Path),
		Key:      internal.Named(),
		Failure:  failure,
	}
	return ks, nil
}

func (d *Session) entryRoot(ctx context.Context, input *store.SourceArtifact) (*manifest.Kustomization, error) {
	var internal *manifest.Kustomization
	if d.binding == (Binding{}) {
		var err error
		internal, _, err = d.admission().entry(ctx, d.cfg.Store, d.applications())
		if err != nil || internal == nil {
			return nil, err
		}
		d.entryApplier = manifest.DiagnosticFor(internal.Named(), internal).Resource
	} else {
		subpath, _ := filepath.Rel(input.LocalPath, d.cfg.Path)
		internal = manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(subpath), d.entryApplier)
	}
	return internal, nil
}

func (d *Session) matchesEntryPath(
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
	repoRoot string,
) bool {
	if ks.IsFragment() || ks.Named().ReportHidden() {
		return false
	}
	if record, indexed := d.loader.Index.Lookup(ks.Named()); indexed &&
		(record.Builder == "" && record.Authored != ks.Named() || record.Builder != "" && record.File == record.Builder) {
		return false
	}
	if source, ok := d.cfg.Store.GetObject(ks.SourceID()).(entrySource); ok {
		_, descriptor := source.BootstrapAlias(input.LocalPath)
		if descriptor != nil &&
			(descriptor.Ignore != nil && *descriptor.Ignore != "" || len(descriptor.SparseCheckout) != 0) {
			return false
		}
	}
	art, _, err := kustomize.SourceInput(d.cfg.Store, ks)
	if err != nil {
		return false
	}
	root, pinned, external := loader.SourceView(d.cfg.Store, ks.SourceID(), repoRoot)
	if pinned || external {
		return false
	}
	target := art.OriginPath(filepath.Join(root, manifest.NormalizeClaimBase(ks.Path)))
	return target == d.cfg.Path
}

func (d *Session) entryCandidateReady(ctx context.Context, candidate *manifest.Kustomization) bool {
	if len(candidate.DependsOn) == 0 {
		return true
	}
	if d.cfg.Selection.Changed || len(d.cfg.Selection.Roots) != 0 {
		return false
	}
	if d.cfg.Trees == nil || candidate.PostBuild != nil && len(candidate.PostBuild.SubstituteFrom) != 0 {
		return false
	}
	source, _, _ := kustomize.SourceInput(d.cfg.Store, candidate)
	prepared, err := d.prepareDocuments(ctx, candidate, source)
	if err != nil {
		slog.Debug("discovery: entry candidate preparation deferred", "err", err)
		return false
	}
	return d.entryEligible(candidate, prepared.children)
}

func (d *Session) entryEligible(owner *manifest.Kustomization, children []store.Child) bool {
	e := entryPrerequisites{d: d, owner: owner, children: children}
	e.batch = make(map[manifest.NamedResource]manifest.BaseManifest, len(children))
	for _, child := range children {
		e.batch[child.Obj.Named()] = child.Obj
	}
	e.seen = map[manifest.NamedResource]bool{}

	e.source = owner.SourceID()
	if e.source.Name == "" || e.source.Kind == "" {
		e.source = manifest.BootstrapSourceID
	}
	return !e.prerequisites(owner)
}

type entryPrerequisites struct {
	d        *Session
	owner    *manifest.Kustomization
	children []store.Child
	batch    map[manifest.NamedResource]manifest.BaseManifest
	seen     map[manifest.NamedResource]bool
	source   manifest.NamedResource
}

func (e *entryPrerequisites) prerequisites(obj manifest.BaseManifest) bool {
	obj = manifest.MapDependsOn(obj, e.d.substitutions.Resolve, e.d.substitutions.Resolve)
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if (ref.Ordering || ref.ArtifactStages != 0 && e.d.cfg.Store.GetArtifact(ref.NamedResource, store.ArtifactContent) == nil) &&
			e.captured(ref.NamedResource) {
			return true
		}
	}
	return false
}

func (e *entryPrerequisites) captured(id manifest.NamedResource) bool {
	if id.Name == "" || id.Kind == "" || id == e.owner.Named() || e.seen[id] {
		return false
	}
	e.seen[id] = true
	if e.batch[id] != nil {
		return true
	}
	for _, child := range e.children {
		for _, target := range manifest.ProducerTargets(child.Obj, id) {
			if target.NamedResource == id {
				return true
			}
		}
	}
	obj := e.d.cfg.Store.GetObject(id)
	if obj == nil {
		obj = e.d.loader.Index.Records[id].Declaration
	}
	if obj != nil && e.prerequisites(obj) {
		return true
	}
	{
		productionValues := e.d.cfg.Store.Productions(id, e.d.loader.Index)
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]

			if production.Source != (manifest.NamedResource{}) && production.Source != e.source {
				continue
			}
			if production.Owner == e.owner.Named() || e.captured(production.Materializer(id)) ||
				e.captured(production.Declaration) {
				return true
			}
		}
	}
	return false
}

func (d *Session) prepareDocuments(
	ctx context.Context,
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
) (declarationPreparation, error) {
	if previous, ok := d.prepared[ks]; ok && previous.source == input {
		return previous, nil
	}
	prepared, err := kustomize.Prepare(ks, nil)
	if err != nil {
		return declarationPreparation{}, err
	}
	request := kustomize.RenderRequest{
		Source:                  input,
		ApplyIgnore:             ks.SourceID() == manifest.BootstrapSourceID || input.InputAlias || input.Content == "",
		SubPath:                 prepared.Path,
		Spec:                    prepared,
		WipeSecrets:             d.cfg.WipeSecrets,
		StrictSubstitutions:     d.cfg.StrictSubstitutions,
		SkipKinds:               d.cfg.SkipKinds,
		DependencySubstitutions: d.substitutions,
	}
	docs, receipt, found := d.cfg.Trees.PreviousRender(ctx, request)
	if !found {
		var err error
		docs, receipt, err = kustomize.Render(ctx, d.cfg.Trees, request, d.cfg.RemoteRetry)
		if err != nil {
			return declarationPreparation{}, err
		}
	}
	content := request.Content(docs)
	if err := kustomize.Shape(prepared, docs, d.cfg.StrictSubstitutions); err != nil {
		return declarationPreparation{}, err
	}
	built, _, failures := loader.ParseChildren(
		prepared,
		docs,
		manifest.ParseDocOptions{WipeSecrets: d.cfg.WipeSecrets},
		d.cfg.SkipKinds,
	)
	if err := manifest.DocumentFailure(failures); err != nil {
		return declarationPreparation{}, err
	}
	if d.prepared == nil {
		d.prepared = map[*manifest.Kustomization]declarationPreparation{}
	}
	result := declarationPreparation{source: input, children: built, receipt: receipt, content: content}
	d.prepared[ks] = result
	return result, nil
}

func (d *Session) prepareProducer(ctx context.Context, ks *manifest.Kustomization, input *store.SourceArtifact) error {
	prepared, err := d.prepareDocuments(ctx, ks, input)
	if err != nil {
		return err
	}
	source := ks.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	owner := manifest.NamedResource{}
	if d.binding.Producer == ks.Named() || !ks.Named().ReportHidden() {
		owner = ks.Named()
	}
	d.loader.RegisterBuilt(
		loader.BuiltInput{
			Root:   filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(ks.Path)),
			Owner:  owner,
			Source: source,
			Reads:  kustomize.RenderInputs(prepared.receipt),
		},
		prepared.children,
	)
	prepared.registered = true
	d.prepared[ks] = prepared
	if d.binding.Producer == ks.Named() {
		d.binding.Failure = nil
	}
	return nil
}

func (d *Session) acceptPrepared(ctx context.Context) error {
	for ks, prepared := range d.prepared {
		if !prepared.registered {
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
		source := ks.SourceID()
		if source.Name == "" || source.Kind == "" {
			source = manifest.BootstrapSourceID
		}
		input := prepared.source
		if err := d.cfg.Trees.RecordRender(ctx, kustomize.RenderAcceptance{
			Owner: ks.Named(), Receipt: prepared.receipt, Content: prepared.content, Entry: ks.EntryReceipt,
			Source:                  input.HistoryIdentity(source),
			Records:                 d.cfg.Store.ChildFootprint(ks.Named(), children, d.loader.Index),
			Sources:                 d.cfg.Store.ChildSourcePaths(ks.Named(), children, d.loader.Index),
			DependencySubstitutions: d.substitutions,
		}); err != nil {
			return fmt.Errorf("discovery entry %q publish: %w", ks.Path, err)
		}
	}
	return nil
}
