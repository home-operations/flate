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

	"github.com/home-operations/flate/pkg/selection"

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
	ownsLoader bool
	Binding    Binding
	Admission  *Admission
	// Admitted inputs may use a synthetic label with no corresponding directory.
	RepoRoot string
	Index    *manifest.DiscoveryIndex
}

// Config is the input contract for Begin. Store is mandatory.
type Config struct {
	Cache *cache.Cache
	Work  manifest.WorkScope
	// Projections shares source-free facts across concurrent Plan calls.
	Projections *manifest.ProducerProjections
	// Declarations contains immutable invocation references admitted before discovery.
	Declarations []store.Declaration
	// Selection controls restricted declaration construction.
	Selection selection.Request
	Changes   *change.Set
	// Loader borrows the invocation's sole declaration loader.
	Loader *loader.Loader
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

// Session retains invocation-local declaration construction until Freeze.
type Session struct {
	entryBuilderInput   *store.SourceArtifact
	entryBuilderPath    string
	entryBuilder        bool
	planner             *entryPlanner
	planLoaded          bool
	pendingSources      []manifest.NamedResource
	sourceGroups        map[manifest.NamedResource]*entrySourceGroup
	loaded              bool
	built               bool
	finalized           bool
	loading             bool
	request             selection.PrepareRequest
	preparation         *selection.Preparation
	constructionStarted bool
	binding             Binding
	entryState          entryQualification
	entryAdmission      *Admission
	entryDemand         bool
	entryReceiptRequest *manifest.EntryApplication
	entryApplier        manifest.NamedResource
	prepared            map[manifest.NamedResource]*declarationPreparation
	cfg                 Config
	loader              *loader.Loader
	resolvedSources     *entrySourcePublisher
	remotes             map[string]struct{}
	aliasViews          map[string]*store.SourceArtifact
	hasPins             bool
	result              *Result
	ownsLoader          bool
}

// Run requires the caller to reconcile PendingSources whenever it returns no result.
func (d *Session) Run(ctx context.Context) (*Result, error) {
	if !d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{}) {
		return d.runSelected(ctx)
	}
	if d.built {
		return d.Freeze(ctx)
	}
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		if errors.Is(err, errPendingSources) {
			return nil, nil //nolint:nilnil // The caller MUST reconcile PendingSources before resuming.
		}
		return nil, err
	}
	if err := d.loadManifests(ctx, d.cfg.Input.LocalPath, true); err != nil {
		if errors.Is(err, errPendingSources) {
			return nil, nil //nolint:nilnil // The caller MUST reconcile PendingSources before resuming.
		}
		return nil, err
	}
	d.loaded, d.built = true, true
	return d.Freeze(ctx)
}

func (d *Session) runSelected(ctx context.Context) (*Result, error) {
	if ready, err := d.Load(ctx); err != nil || !ready {
		return nil, err
	}

	var preparation *selection.Preparation
	defer func() { preparation.Retire() }()
	for {
		request := d.prepareRequest(&selection.PrepareRequest{Invocation: true, Preparation: &preparation})
		admitted, committed, err := d.loader.Options.Session.Admit(ctx, selection.AdmissionOptions{
			Request: request, Stage: selection.Construction,
		})
		if err != nil {
			return nil, err
		}
		if !committed {
			continue
		}
		advanced, err := d.Advance(ctx, &admitted)
		if err != nil {
			return nil, err
		}
		if advanced {
			if len(d.pendingSources) != 0 {
				return nil, nil //nolint:nilnil // The caller MUST reconcile PendingSources before resuming.
			}
			continue
		}
		request = d.prepareRequest(&selection.PrepareRequest{Invocation: true, Preparation: &preparation})
		_, committed, err = d.loader.Options.Session.Admit(ctx, selection.AdmissionOptions{
			Request: request, Stage: selection.Execution,
		})
		if err != nil {
			return nil, err
		}
		if !committed {
			continue
		}
		result, err := d.Freeze(ctx)
		return result, err
	}
}

func (d *Session) finishDiscovery(
	ctx context.Context,
	repoRoot string,
	l *loader.Loader,
) (*Result, error) {
	if !d.finalized && d.binding.Failure == nil {
		if err := l.EntryFailure(ctx, repoRoot); err != nil {
			return nil, err
		}
	}
	if !d.finalized {
		if err := l.FinalizeIndex(ctx, repoRoot, true); err != nil {
			return nil, err
		}
		d.finalized = true
		if _, err := d.resolveEntrySources(ctx, true); err != nil {
			return nil, err
		}
	}
	if l.Options.Session.Snapshot().Output(manifest.NamedResource{}) {
		if err := l.PromoteOrphans(ctx); err != nil {
			return nil, err
		}
	}
	if err := d.acceptPrepared(ctx); err != nil {
		return nil, err
	}
	if len(d.PendingSources()) != 0 {
		return nil, errPendingSources
	}
	admission := d.finishAdmission(ctx)
	result := &Result{
		prepared: d.prepared, loader: l, ownsLoader: d.ownsLoader,
		Binding:   d.binding,
		Admission: admission,
		RepoRoot:  repoRoot,
		Index:     l.Index,
	}
	d.result = result
	return result, nil
}

func (d *Session) finishAdmission(ctx context.Context) *Admission {
	admission := d.admission()
	admission.session = nil
	if admission.qualification != nil {
		admission.qualification.session = nil
	}
	if d.cfg.Trees == nil {
		if admission.qualification != nil {
			admission.qualification.close()
		}
		admission.qualification = nil
	}
	if admission.qualification != nil && d.cfg.Trees != nil && d.cfg.Selection.Changed && d.binding == (Binding{}) {
		d.entryReceipt(ctx, admission, admission.input)
		if d.entryReceiptRequest != nil {
			admission.entryDemand = true
			admission.qualification.closed = false
		}
	}
	d.resolvedSources.freeze()
	return admission
}

type declarationPreparation struct {
	preparation kustomize.Preparation
	complete    bool
	err         error
	work        *manifest.RenderWork
	owner       *manifest.Kustomization
	source      *store.SourceArtifact
	children    []store.Child
	receipt     *manifest.RenderReceipt
	content     any
	registered  bool
	support     store.ReadSet
	warnings    []manifest.Warning
}

func (p *declarationPreparation) matches(
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
	guard *store.ReadSet,
) bool {
	return p.complete && p.source == input && manifest.Equal(p.owner, ks) && p.support.Valid() &&
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
	if cfg.Projections != nil {
		l.Options.Projections = cfg.Projections
	}
	l.Options.Work = cfg.Work
	l.Options.Cache = cfg.Cache
	if cfg.Changes != nil {
		l.Options.Changes = cfg.Changes
	}
	d := &Session{
		cfg: cfg, loader: l, aliasViews: map[string]*store.SourceArtifact{},
		ownsLoader: cfg.Loader == nil,
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
	if !cfg.Entry.matches(input, cfg.Path) {
		return d
	}
	l.Options.ParsedInputs = prepared.parsed
	d.aliasViews = maps.Clone(prepared.aliasViews)
	d.cfg.Entry = cfg.Entry
	return d
}

func (d *Session) loadManifests(ctx context.Context, repoRoot string, build bool) error {
	l := d.loader
	defer func() { l.PreferExisting = false }()
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
		if !build {
			break
		}
		prepared, err = d.prepareStandaloneEntry(ctx, repoRoot, l)
		if err != nil {
			return err
		}
	}
	d.loading = false
	l.PreferExisting = false
	if warning := d.resolvedSources.aliasWarning(); warning != nil {
		d.cfg.Store.AddWarning(*warning)
	}
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
	if d.cfg.Trees == nil || sourceID == nil || !d.canPrepare(ks) {
		return false, nil
	}
	if previous, ok := d.prepared[ks.Named()]; ok && previous.registered &&
		previous.matches(ks, sourceID, nil) {
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
	if d.loader == nil || d.loader.Options.Session.Snapshot().Output(manifest.NamedResource{}) {
		return d.hasEntryBuilder()
	}
	return d.entryDemand || d.entryReceiptRequest != nil
}

func (d *Session) hasEntryBuilder() bool {
	input, _ := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	if input == nil {
		input = d.cfg.Input
	}
	if input == nil {
		return false
	}
	if input.SameMaterial(d.entryBuilderInput) && d.entryBuilderPath == d.cfg.Path {
		return d.entryBuilder
	}
	d.entryBuilderInput, d.entryBuilderPath, d.entryBuilder = input, d.cfg.Path, false
	relative, _ := filepath.Rel(input.LocalPath, d.cfg.Path)
	for _, filename := range manifest.KustomizeBuilderFilenames {
		_, err := fs.Stat(input.FS, filepath.ToSlash(filepath.Join(relative, filename)))
		if !errors.Is(err, fs.ErrNotExist) {
			d.entryBuilder = true
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
	support               *store.ReadSet
}

func (d *Session) bindEntry(ctx context.Context, _ string) (*manifest.Kustomization, error) {
	progressed := false
	defer func() { d.entryState.changed = progressed }()
	admission := d.admission()
	if q := admission.qualification; q != nil && q.previous.Receipt == nil && d.cfg.Trees != nil &&
		(!d.hasEntryBuilder() || d.entryReceiptRequest != nil) {
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

func (d *Session) prepareRequest(request *selection.PrepareRequest) selection.PrepareRequest {
	previous := d.request
	request.Changes = d.loader.Options.Changes
	request.Missing = d.loader.Options.Missing
	if request.ReadHistory == nil {
		request.ReadHistory = previous.ReadHistory
	}
	if request.HistoryOwners == nil {
		request.HistoryOwners = previous.HistoryOwners
	}
	if len(request.Acquired) == 0 {
		request.Acquired = previous.Acquired
	}
	if request.Preparation == nil {
		request.Preparation = previous.Preparation
		if request.Preparation == nil {
			request.Preparation = &d.preparation
		}
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
		application := manifest.EntryApplication{Source: root.EffectiveSourceID(), Path: root.Path}
		incoming = store.Child{Obj: root, Entry: &application}
	}
	binding.Reads.SetWork(d.cfg.Work.Recorder(&manifest.WorkKey{
		Tree: d.cfg.Work.Identity().Tree, Owner: binding.Producer, Source: binding.Source,
	}))
	request := d.prepareRequest(&selection.PrepareRequest{Generation: []store.Child{incoming}, Reads: binding.Reads})
	if d.loader.Options.Session == nil {
		if incoming.Obj != nil {
			binding.Reads.Admit(incoming, binding.Source, manifest.NamedResource{})
		}
		if !binding.Reads.Valid() {
			return Binding{}, nil
		}
		return d.admission().AcceptEntry(ctx, *binding)
	}
	options := selection.AdmissionOptions{Request: request, Stage: selection.Construction}
	if entry := binding.Entry; entry != nil {
		options.EntryHistory.Owner, options.EntryHistory.Records = entry.Owner, entry.Records
	}
	_, committed, err := d.loader.Options.Session.Admit(ctx, options)
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
	if !d.canPrepare(candidate) {
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
	eligibility := entryPrerequisites{loader: d.loader, reads: reads}
	return eligibility.eligible(candidate, prepared.children)
}

func (d *Session) prepareDocuments(
	ctx context.Context,
	ks *manifest.Kustomization,
	input *store.SourceArtifact,
	guard *store.ReadSet,
) (*declarationPreparation, error) {
	previous := d.prepared[ks.Named()]
	if previous != nil && previous.matches(ks, input, guard) {
		return previous, previous.err
	}
	frame := &declarationPreparation{owner: ks, source: input}
	if previous != nil {
		defer previous.retire(d.cfg.Store)
	}
	if d.prepared == nil {
		d.prepared = map[manifest.NamedResource]*declarationPreparation{}
	}
	d.prepared[ks.Named()] = frame
	sourceID := ks.EffectiveSourceID()
	frame.captureSupport(d.cfg.Store, sourceID)
	if guard != nil && (!guard.Valid() || !frame.support.Matches(guard)) {
		frame.retire(d.cfg.Store)
		return nil, manifest.NewError(manifest.ErrInput, nil, "construction support changed")
	}
	var provider values.Provider
	if ks.PostBuild != nil && len(ks.PostBuild.SubstituteFrom) != 0 {
		provider = values.NewStoreProvider(&frame.support)
	}
	prepared, err := kustomize.Prepare(ks, provider, d.loader.Options.Missing)
	if err != nil {
		frame.err, frame.complete = err, ctx.Err() == nil
		return frame, err
	}
	work := d.preparationWork(ks, sourceID)
	request := kustomize.RenderRequest{
		Work:                work.RenderWork(),
		Source:              input,
		ApplyIgnore:         kustomize.ApplySourceIgnore(prepared, input),
		SubPath:             prepared.Path,
		Spec:                prepared,
		StrictSubstitutions: d.cfg.StrictSubstitutions,
		SkipKinds:           d.cfg.SkipKinds,
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
	shaping := kustomize.PrepareShape(&request, docs)
	content := shaping.Content()
	if err := shaping.Apply(
		ctx,
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
	d.registerProducer(prepared)
	return nil
}

func (d *Session) registerProducer(prepared *declarationPreparation) {
	sourceID := prepared.owner.EffectiveSourceID()
	owner := prepared.owner.Named()
	d.loader.RegisterBuilt(
		loader.BuiltInput{
			Root:    filepath.Join(prepared.source.LocalPath, manifest.NormalizeClaimBase(prepared.owner.Path)),
			Owner:   owner,
			Source:  sourceID,
			Reads:   kustomize.RenderInputs(prepared.receipt),
			Support: &prepared.support,
		},
		prepared.children,
	)
	prepared.registered = true
	d.prepared[owner] = prepared
	if d.binding.Producer == owner {
		d.binding.Failure = nil
	}
}

func (d *Session) acceptPrepared(ctx context.Context) error {
	for _, prepared := range d.prepared {
		ks := prepared.owner
		if !prepared.registered {
			continue
		}
		sourceID := ks.EffectiveSourceID()
		input := prepared.source
		if err := d.cfg.Trees.RecordRender(ctx, kustomize.RenderAcceptance{
			Work:  prepared.work,
			Owner: ks.Named(), Receipt: prepared.receipt, Content: prepared.content,
			Source:  input.HistoryIdentity(sourceID),
			Records: d.cfg.Store.ChildFootprint(ks.Named(), prepared.children, d.loader.Index),
			Sources: d.cfg.Store.ChildSourcePaths(ks.Named(), prepared.children, d.loader.Index),
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
	knownEntry := ks.Named().ReportHidden() && (d.entryReceiptRequest != nil ||
		d.entryAdmission != nil && d.entryAdmission.qualification != nil && d.entryAdmission.qualification.previous.Receipt != nil)
	if knownEntry {
		knownEntry = !d.entryDemand
	}
	if !ks.Named().ReportHidden() || knownEntry {
		work = work.Recorder(&manifest.WorkKey{Tree: work.Identity().Tree, Owner: ks.Named(), Source: sourceID}).Scope()
	}
	return work
}
