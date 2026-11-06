// Package discovery hydrates the store, resolves local source identities, and
// publishes immutable discovery records, ownership claims and resource-graph readers.
// Source aliasing uses authored URL/ref values before parent rendering.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
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
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Result summarizes what discovery hydrated into the store.
type Result struct {
	prepared   map[manifest.NamedResource]*declarationPreparation
	loader     *loader.Loader
	ownsLoader bool       // Binding identifies the entry's actual producer and admitted input.
	Binding    Binding    // Admission applies the same immutable bootstrap policy to runtime source identities.
	Admission  *Admission // RepoRoot is the bootstrap input's ownership coordinate. Admitted
	// inputs may use a synthetic label with no corresponding directory.
	RepoRoot                string // Index contains the immutable static discovery facts consumed by rendering.
	Index                   *manifest.DiscoveryIndex
	DependencySubstitutions values.DependencySubstitutions
}

// Config is the input contract for Run. Store is mandatory.
type Config struct {
	Work manifest.WorkScope
	// Projections shares source-free facts across concurrent Plan calls.
	Projections *manifest.ProducerProjections
	// Declarations contains immutable invocation references admitted before discovery.
	Declarations []store.Declaration
	// Selection controls restricted declaration construction.
	Selection manifest.Selection
	// Loader borrows the invocation's sole declaration loader.
	Loader     *loader.Loader
	session    *Session
	ownsLoader bool
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
	if cfg.Loader == nil {
		cfg.Loader = loader.New(cfg.Store)
		cfg.ownsLoader = true
	}
	if cfg.session == nil && (cfg.Selection.Changed || len(cfg.Selection.Roots) != 0) {
		return runSelected(ctx, &cfg)
	}
	d := cfg.session
	if d == nil {
		d = newDiscoverer(&cfg)
	}
	complete := false
	defer func() {
		if !complete {
			d.Close()
			return
		}
		for _, frame := range d.prepared {
			if !frame.registered {
				frame.retireSupport(d.cfg.Store)
			}
		}
	}()
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
	result, err := d.finishDiscovery(ctx, &cfg, repoRoot, l)
	complete = err == nil
	return result, err
}

func runSelected(ctx context.Context, cfg *Config) (*Result, error) {
	if cfg.Loader == nil {
		cfg.Loader = loader.New(cfg.Store)
		cfg.ownsLoader = true
	}
	if cfg.Loader.Options.Filter == nil {
		cfg.Loader.Options.Filter = change.NewFilter()
	}
	session, err := Begin(ctx, *cfg)
	if err != nil {
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			session.Close()
		}
	}()
	var preparation *loader.Preparation
	defer func() { preparation.Retire() }()
	for {
		proposal, err := cfg.Loader.Prepare(
			ctx,
			session.prepareRequest(&loader.PrepareRequest{Invocation: true, Preparation: &preparation}),
		)
		if err != nil {
			return nil, err
		}
		advanced, err := session.Advance(ctx, proposal)
		if err != nil {
			return nil, err
		}
		if advanced {
			continue
		}
		final, err := cfg.Loader.Prepare(
			ctx,
			session.prepareRequest(&loader.PrepareRequest{Invocation: true, Preparation: &preparation}),
		)
		if err != nil {
			return nil, err
		}
		committed, err := cfg.Loader.Commit(ctx, final, loader.Execution)
		if err != nil {
			return nil, err
		}
		if !committed {
			continue
		}
		result, err := session.Freeze(ctx)
		complete = err == nil
		return result, err
	}
}

// Session retains invocation-local declaration construction until Freeze.
type Session struct {
	request             loader.PrepareRequest
	constructionStarted bool
	binding             Binding
	entryState          entryQualification
	entryAdmission      *Admission
	entryDemand         bool
	entryApplier        manifest.NamedResource
	prepared            map[manifest.NamedResource]*declarationPreparation
	substitutions       values.DependencySubstitutions
	cfg                 Config
	loader              *loader.Loader
	resolvedSources     map[manifest.NamedResource]entryResolution
	remotes             map[string]struct{}
	aliasViews          map[string]*store.SourceArtifact
	hasPins             bool
	result              *Result
	ownsLoader          bool
}

func (d *Session) finishDiscovery(
	ctx context.Context,
	cfg *Config,
	repoRoot string,
	l *loader.Loader,
) (*Result, error) {
	if d.binding.Failure == nil {
		if err := l.EntryFailure(ctx, repoRoot); err != nil {
			return nil, err
		}
	}
	if !cfg.Selection.Changed && len(cfg.Selection.Roots) == 0 && d.binding.Producer == (manifest.NamedResource{}) {
		proposal, err := l.Prepare(ctx, d.prepareRequest(&loader.PrepareRequest{Invocation: true}))
		if err != nil {
			return nil, err
		}
		if _, err := d.admitRequiredObjects(ctx, proposal); err != nil {
			return nil, err
		}
		d.constructionStarted = true
		if err := d.refreshDeclarations(ctx, repoRoot); err != nil {
			return nil, err
		}
	}
	d.applyDependencySubstitutions()
	if err := l.FinalizeIndex(ctx, repoRoot, true); err != nil {
		return nil, err
	}
	if !cfg.Selection.Changed && len(cfg.Selection.Roots) == 0 {
		if err := l.PromoteOrphans(ctx); err != nil {
			return nil, err
		}
	}
	if err := d.acceptPrepared(ctx); err != nil {
		return nil, err
	}
	admission := d.finishAdmission(ctx)
	result := &Result{
		prepared: d.prepared, loader: l, ownsLoader: d.ownsLoader,
		Binding:                 d.binding,
		Admission:               admission,
		DependencySubstitutions: d.substitutions,
		RepoRoot:                repoRoot,
		Index:                   l.Index,
	}
	d.result = result
	return result, nil
}

func (d *Session) applyDependencySubstitutions() {
	kustomizations := d.loader.Kustomizations()
	for i := range kustomizations {
		child := &kustomizations[i]
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		if resolved := manifest.MapDependsOn(
			ks,
			d.substitutions.Resolve,
			d.substitutions.Resolve,
		); resolved != ks &&
			d.cfg.Store.GetObject(ks.Named()) != nil {
			_, doc, emission := d.cfg.Store.GetEmission(ks.Named())
			emission.Finish()
			d.cfg.Store.AddObject(resolved, doc)
		}
	}
}

func (d *Session) finishAdmission(ctx context.Context) *Admission {
	admission := d.admission()
	admission.session = nil
	if admission.qualification != nil {
		admission.qualification.session = nil
	}
	if admission.entryBound.Load() || d.cfg.Trees == nil {
		d.loader.Options.HistoryOwners = nil
	}
	if d.cfg.Trees == nil {
		if admission.qualification != nil {
			admission.qualification.close()
		}
		admission.qualification = nil
	}
	if admission.qualification != nil && d.cfg.Trees != nil && d.cfg.Selection.Changed && d.binding == (Binding{}) {
		d.entryReceipt(ctx, admission, admission.input)
		if d.entryHistoryDemand() {
			admission.entryDemand = true
			admission.qualification.closed = false
		}
	}
	return admission
}

type declarationPreparation struct {
	preparation   kustomize.Preparation
	complete      bool
	err           error
	work          *manifest.RenderWork
	owner         *manifest.Kustomization
	source        *store.SourceArtifact
	children      []store.Child
	receipt       *manifest.RenderReceipt
	content       any
	registered    bool
	support       store.ReadSet
	warnings      []manifest.Warning
	substitutions values.DependencySubstitutions
}

func (p *declarationPreparation) matches(
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
	substitutions values.DependencySubstitutions,
	guard *store.ReadSet,
) bool {
	return p.complete && p.source == input && manifest.Equal(p.owner, ks) && p.support.Valid() &&
		p.substitutions.Equal(substitutions) &&
		(guard == nil || guard.Valid() && guard.Matches(&p.support) && p.support.Valid())
}

func newDiscoverer(config *Config) *Session {
	cfg := *config
	l := cfg.Loader
	if l == nil {
		l = loader.New(cfg.Store)
	}
	l.AdmitDeclarations(cfg.Declarations)
	l.Options.RemoteInputs, l.Options.RemoteRetry = cfg.RemoteInputs, cfg.RemoteRetry
	l.Options.Selection = cfg.Selection
	if cfg.Projections != nil {
		l.Options.Projections = cfg.Projections
	}
	l.Options.Work = cfg.Work
	d := &Session{
		cfg: cfg, loader: l, aliasViews: map[string]*store.SourceArtifact{},
		ownsLoader: cfg.ownsLoader || cfg.Loader == nil,
	}
	d.cfg.Entry = nil
	if cfg.Entry == nil || cfg.Entry.prepared == nil {
		return d
	}
	prepared := cfg.Entry.prepared
	input := cfg.Input
	if input == nil {
		input, _ = cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	}
	if input == nil {
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

	total := 0
	if err := d.loadEntryInput(ctx, l, &total); err != nil {
		return err
	}
	// Projection may introduce a new effective source reference. Resolve it
	// before Load admits its spec.path edge to scope construction.
	l.PreferExisting = true
	var pending []sourceExpansion
	rounds := 0
	for prepared := true; prepared; {
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
		pending = d.enqueueExpansions(kustomizations, pending[:0])
		next, added, loadErr := d.loadExpansions(ctx, repoRoot, pending, &total)
		if loadErr != nil {
			return loadErr
		}
		pending = next
		if added != 0 {
			continue
		}
		l.Admit(repoRoot)
		missing, err := d.resolveEntrySources(ctx, true)
		if err != nil {
			return err
		}
		if missing != 0 || !slices.EqualFunc(kustomizations, l.Kustomizations(), func(a, b store.Child) bool {
			return manifest.Equal(a.Obj, b.Obj)
		}) {
			continue
		}
		d.setDependencySubstitutions(kustomizations)
		if !build {
			break
		}
		prepared, err = d.prepareStandaloneEntry(ctx, repoRoot, l)
		if err != nil {
			return err
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

func (d *Session) setDependencySubstitutions(kustomizations []store.Child) {
	var discovered []*manifest.Kustomization
	if len(kustomizations) != 0 {
		discovered = make([]*manifest.Kustomization, 0, len(kustomizations))
	}
	childValues2 := kustomizations
	for childIndex2 := range childValues2 {
		child := childValues2[childIndex2]
		discovered = append(
			discovered,
			child.Obj.(*manifest.Kustomization), //nolint:errcheck // This branch handles an admitted Kustomization.
		)
	}
	d.substitutions = values.NewDependencySubstitutions(discovered)
}

func (d *Session) prepareStandaloneEntry(ctx context.Context, repoRoot string, l *loader.Loader) (bool, error) {
	ks, err := d.bindEntry(ctx, repoRoot)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	l.Admit(repoRoot)
	if ks == nil {
		return d.entryState.changed, nil
	}
	sourceID, _, _ := kustomize.SourceInput(d.cfg.Store, ks)
	if d.cfg.Trees == nil || sourceID == nil || ks.PostBuild != nil && len(ks.PostBuild.SubstituteFrom) != 0 {
		return false, nil
	}
	if previous, ok := d.prepared[ks.Named()]; ok && previous.registered &&
		previous.matches(ks, sourceID, d.substitutions, nil) {
		return d.entryState.changed, nil
	}
	if err := d.prepareProducer(ctx, ks, sourceID, nil); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err() //nolint:wrapcheck // Preserve cancellation identity after producer preparation.
		}
		d.binding.Failure = err
		slog.Debug("discovery: declaration preparation deferred", "err", err)
		return false, nil
	}
	return true, nil
}

func (d *Session) requiresEntry() bool {
	if d.entryDemand || d.entryHistoryDemand() {
		return true
	}
	if !d.cfg.Selection.Changed && len(d.cfg.Selection.Roots) == 0 {
		return d.hasEntryBuilder()
	}
	changes := d.loader.Options.Changes
	if changes == nil {
		return false
	}
	delta, _, _ := changes.ForSource(manifest.BootstrapSourceID)
	if d.changedEntryFiles(delta) {
		// Native builder demand MUST survive replacement of its file-indexed record.
		d.entryDemand = d.hasEntryBuilder()
		return true
	}
	for id := range d.loader.Index.Files() {
		record, _ := d.loader.Index.Lookup(id)
		if !d.cfg.Selection.Matches(id) ||
			!delta.Contains(record.File) &&
				(record.Builder == "" || !slices.ContainsFunc(record.Inputs, delta.Contains)) {
			continue
		}
		if d.entryFileDemand(record.File) {
			return true
		}
		application, valid := manifest.ApplicationFor(d.loader.CatalogObject(id))
		if valid && loader.AppliesToEntry(d.cfg.Store, &application, d.cfg.Input, d.cfg.Path) {
			return true
		}
	}
	return false
}

func (d *Session) entryHistoryDemand() bool {
	if d.loader == nil || d.cfg.Input == nil || len(d.loader.Options.HistoryOwners) == 0 {
		return false
	}
	relative, _ := filepath.Rel(d.cfg.Input.LocalPath, d.cfg.Path)
	native := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID, filepath.ToSlash(relative), manifest.NamedResource{},
	).Named()
	return len(d.loader.Options.HistoryOwners[native]) != 0 ||
		len(d.loader.Options.HistoryOwners[d.entryState.previous.Producer]) != 0
}

func (d *Session) hasEntryBuilder() bool {
	input, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if input == nil {
		input = d.cfg.Input
	}
	if input == nil {
		return false
	}
	relative, _ := filepath.Rel(input.LocalPath, d.cfg.Path)
	for _, filename := range manifest.KustomizeBuilderFilenames {
		_, err := fs.Stat(input.FS, filepath.ToSlash(filepath.Join(relative, filename)))
		if !errors.Is(err, fs.ErrNotExist) {
			return true
		}
	}
	return false
}

// Binding keeps actual ownership separate from the paired entry comparison coordinate.
type Binding struct {
	Producer, Source, Key manifest.NamedResource
	Path                  string
	Failure               error
	Entry                 *kustomize.EntryEvidence
	Reads                 *store.ReadSet
}

func (d *Session) bindEntry(ctx context.Context, _ string) (*manifest.Kustomization, error) {
	progressed := false
	defer func() { d.entryState.changed = progressed }()
	admission := d.admission()
	if q := admission.qualification; q != nil && q.previous.Receipt == nil && d.cfg.Trees != nil &&
		(!d.hasEntryBuilder() || d.entryHistoryDemand()) {
		d.entryReceipt(ctx, admission, admission.input)
	}
	for {
		root, binding, err := admission.BindEntry(ctx, d.cfg.Store)
		progressed = progressed || admission.qualification != nil && admission.qualification.changed
		if err != nil {
			return nil, err
		}
		if binding.Reads == nil {
			if admission.qualification != nil && admission.qualification.changed {
				continue
			}
			previous, _ := d.cfg.Store.GetObject(d.binding.Producer).(*manifest.Kustomization)
			return previous, nil
		}
		accepted, err := d.commitBinding(ctx, root, &binding)
		if err != nil {
			return nil, err
		}
		if accepted == (Binding{}) {
			continue
		}
		d.binding = accepted
		if root != nil {
			d.entryApplier = manifest.DiagnosticFor(root.Named(), root).Resource
		}
		return root, nil
	}
}

func (d *Session) prepareRequest(request *loader.PrepareRequest) loader.PrepareRequest {
	previous := d.request
	if request.Preparation == nil {
		request.Preparation = previous.Preparation
	}
	if len(request.History) != 0 || len(previous.History) == 0 || !previous.Reads.Valid() {
		return *request
	}
	if request.Reads == nil {
		request.Reads = previous.Reads
	}
	if previous.Reads != nil && (!request.Reads.Matches(previous.Reads) || !previous.Reads.Valid()) {
		return *request
	}
	request.History = previous.History
	return *request
}

func (d *Session) commitBinding(ctx context.Context, root *manifest.Kustomization, binding *Binding) (Binding, error) {
	defer d.cfg.Store.Retire(binding.Reads)
	defer binding.Reads.ReleaseCapture()
	if root != nil && root == d.cfg.Store.GetDeclaration(root.Named()) {
		return d.admission().AcceptEntry(ctx, *binding)
	}
	incoming := store.Child{}
	if root != nil {
		application := manifest.EntryApplication{Source: root.SourceID(), Path: root.Path}
		incoming = store.Child{Obj: root, Entry: &application}
	}
	binding.Reads.SetWork(d.cfg.Work.Recorder(&manifest.WorkKey{
		Tree: d.cfg.Work.Identity().Tree, Owner: binding.Producer, Source: binding.Source,
	}))
	request := d.prepareRequest(&loader.PrepareRequest{Generation: []store.Child{incoming}, Reads: binding.Reads})
	if entry := binding.Entry; entry != nil {
		history := map[manifest.NamedResource]struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{}
		maps.Copy(history, request.History)
		history[entry.Owner] = struct {
			Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			Complete bool
		}{Records: entry.Records, Complete: true}
		request.History = history
	}
	proposal, err := d.loader.Prepare(ctx, request)
	if err != nil {
		return Binding{}, err
	}
	committed, err := d.loader.Commit(ctx, proposal, loader.Construction)
	if err != nil {
		return Binding{}, err
	}
	if !committed || !binding.Reads.Valid() {
		return Binding{}, nil
	}
	return d.admission().AcceptEntry(ctx, *binding)
}

func (d *Session) entryCandidateReady(
	ctx context.Context,
	reads *store.ReadSet,
	candidate *manifest.Kustomization,
) bool {
	if candidate.PostBuild != nil && len(candidate.PostBuild.SubstituteFrom) != 0 {
		return len(candidate.DependsOn) == 0
	}
	if d.cfg.Trees == nil {
		return len(candidate.DependsOn) == 0
	}
	sourceID, _, _ := kustomize.SourceInput(d.cfg.Store, candidate)
	prepared, err := d.prepareDocuments(ctx, candidate, sourceID, nil)
	if err != nil {
		slog.Debug("discovery: entry candidate preparation deferred", "err", err)
		return len(candidate.DependsOn) == 0 &&
			sourceID == reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	}
	eligibility := entryPrerequisites{loader: d.loader, reads: reads, substitutions: d.substitutions}
	return eligibility.eligible(candidate, prepared.children)
}

func (d *Session) prepareDocuments(
	ctx context.Context,
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
	guard *store.ReadSet,
) (*declarationPreparation, error) {
	previous := d.prepared[ks.Named()]
	if previous != nil && previous.matches(ks, input, d.substitutions, guard) {
		return previous, previous.err
	}
	frame := &declarationPreparation{owner: ks, source: input, substitutions: d.substitutions}
	if previous != nil {
		defer previous.retire(d.cfg.Store)
	}
	if d.prepared == nil {
		d.prepared = map[manifest.NamedResource]*declarationPreparation{}
	}
	d.prepared[ks.Named()] = frame
	sourceID := ks.SourceID()
	if sourceID.Kind == "" || sourceID.Name == "" {
		sourceID = manifest.BootstrapSourceID
	}
	frame.captureSupport(d.cfg.Store, sourceID)
	if guard != nil && (!guard.Valid() || !frame.support.Matches(guard)) {
		frame.retire(d.cfg.Store)
		return nil, manifest.NewError(manifest.ErrInput, nil, "construction support changed")
	}
	prepared, err := kustomize.Prepare(ks, nil)
	if err != nil {
		frame.err, frame.complete = err, ctx.Err() == nil
		return frame, err
	}
	work := d.preparationWork(ks, sourceID)
	request := kustomize.RenderRequest{
		Work:                    work.RenderWork(),
		Source:                  input,
		ApplyIgnore:             ks.SourceID() == manifest.BootstrapSourceID || input.InputAlias || input.Content == "",
		SubPath:                 prepared.Path,
		Spec:                    prepared,
		StrictSubstitutions:     d.cfg.StrictSubstitutions,
		SkipKinds:               d.cfg.SkipKinds,
		DependencySubstitutions: d.substitutions,
	}
	request.Preparation = &frame.preparation
	if previous != nil {
		frame.preparation.Take(&previous.preparation, d.cfg.Trees, &request)
	}
	docs, receipt, found := d.cfg.Trees.PreviousRender(ctx, request)
	if !found {
		var err error
		docs, receipt, err = kustomize.Render(ctx, d.cfg.Trees, request, d.cfg.RemoteRetry)
		if err != nil {
			frame.err, frame.complete = err, ctx.Err() == nil
			return frame, err
		}
	}
	content := request.Content(docs)
	if err := kustomize.Shape(
		ctx,
		prepared,
		docs,
		kustomize.ShapeOptions{
			Strict: d.cfg.StrictSubstitutions,
			Work:   request.Work.Substitution(),
		},
	); err != nil {
		frame.err, frame.complete = err, ctx.Err() == nil
		return frame, err
	}
	parseChildrenResult2 := loader.ParseChildren(
		prepared,
		docs,
		manifest.ParseDocOptions{},
		d.cfg.SkipKinds,
	)
	built, _, failures := parseChildrenResult2.Children, parseChildrenResult2.Visible, parseChildrenResult2.Failures
	if err := manifest.DocumentFailure(failures); err != nil {
		frame.err, frame.complete = err, ctx.Err() == nil
		return frame, err
	}
	frame.work, frame.children, frame.receipt, frame.content = request.Work, built, receipt, content
	frame.complete = true
	for _, doc := range docs {
		frame.warnings = append(frame.warnings, doc.Warnings...)
	}
	return frame, nil
}

func (d *Session) prepareProducer(
	ctx context.Context,
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
	guard *store.ReadSet,
) error {
	prepared, err := d.prepareDocuments(ctx, ks, input, guard)
	if err != nil {
		return err
	}
	sourceID := ks.SourceID()
	if sourceID.Name == "" || sourceID.Kind == "" {
		sourceID = manifest.BootstrapSourceID
	}
	owner := ks.Named()
	d.loader.RegisterBuilt(
		loader.BuiltInput{
			Root:    filepath.Join(input.LocalPath, manifest.NormalizeClaimBase(ks.Path)),
			Owner:   owner,
			Source:  sourceID,
			Reads:   kustomize.RenderInputs(prepared.receipt),
			Support: &prepared.support,
		},
		prepared.children,
	)
	prepared.registered = true
	d.prepared[ks.Named()] = prepared
	if d.binding.Producer == ks.Named() {
		d.binding.Failure = nil
	}
	return nil
}

func (d *Session) acceptPrepared(ctx context.Context) error {
	for _, prepared := range d.prepared {
		ks := prepared.owner
		if !prepared.registered {
			continue
		}
		children := prepared.children
		cloned := false
		childValues7 := children
		for i := range childValues7 {
			child := childValues7[i]
			if obj := manifest.MapDependsOn(child.Obj, d.substitutions.Resolve, nil); obj != child.Obj {
				if !cloned {
					children = slices.Clone(children)
					cloned = true
				}
				children[i].Obj = obj
			}
		}
		sourceID := ks.SourceID()
		if sourceID.Name == "" || sourceID.Kind == "" {
			sourceID = manifest.BootstrapSourceID
		}
		input := prepared.source
		if err := d.cfg.Trees.RecordRender(ctx, kustomize.RenderAcceptance{
			Work:  prepared.work,
			Owner: ks.Named(), Receipt: prepared.receipt, Content: prepared.content,
			Source:                  input.HistoryIdentity(sourceID),
			Records:                 d.cfg.Store.ChildFootprint(ks.Named(), children, d.loader.Index),
			Sources:                 d.cfg.Store.ChildSourcePaths(ks.Named(), children, d.loader.Index),
			DependencySubstitutions: prepared.substitutions,
		}); err != nil {
			return fmt.Errorf("discovery entry %q publish: %w", ks.Path, err)
		}
		for warningIndex := range prepared.warnings {
			warning := prepared.warnings[warningIndex]
			d.cfg.Store.AddWarning(warning)
		}
	}
	return nil
}

func (d *Session) preparationWork(ks *manifest.Kustomization, sourceID manifest.NamedResource) manifest.WorkScope {
	work := d.cfg.Work.Source(sourceID)
	knownEntry := ks.Named().ReportHidden() && (len(d.loader.Options.HistoryOwners[ks.Named()]) != 0 ||
		d.entryAdmission != nil && d.entryAdmission.qualification != nil && d.entryAdmission.qualification.previous.Receipt != nil)
	if knownEntry {
		delta, _, _ := d.loader.Options.Changes.ForSource(manifest.BootstrapSourceID)
		knownEntry = !d.changedEntryFiles(delta)
	}
	if !ks.Named().ReportHidden() || knownEntry {
		work = work.Recorder(&manifest.WorkKey{Tree: work.Identity().Tree, Owner: ks.Named(), Source: sourceID}).Scope()
	}
	return work
}
