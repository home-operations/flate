// Package discovery hydrates the store, resolves local source identities, and
// publishes immutable discovery records, ownership claims and resource-graph readers.
// Source aliasing uses authored URL/ref values before parent rendering.
package discovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/pkg/manifest"
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
	Trees *kustomize.TreeCache
	// PrepareDocuments applies producer metadata before fingerprints and substitutions.
	PrepareDocuments    func(manifest.BaseManifest, []manifest.DocOccurrence)
	Store               *store.Store
	RemoteInputs        *source.HTTPInputs
	RemoteRetry         cache.RetryConfig
	StrictSubstitutions bool
	SkipKinds           []string
}

// Session retains invocation-local declaration construction until Freeze.
type Session struct {
	bootstrapInput         *store.SourceArtifact
	entryBuilderInput      *store.SourceArtifact
	entryBuilderPath       string
	entryBuilder           bool
	aggregate              bool
	aggregateRequiresBuild bool
	entryPackagesToQualify [][]selection.OccurrenceFacts
	generatorPackages      []string
	publicOperands         []manifest.NamedResource
	entryPackages          []*plannedPackage
	planner                *entryPlanner
	packages               []loader.PackageEvidence
	planLoaded             bool
	pendingSources         []manifest.NamedResource
	pendingMaterializers   []manifest.NamedResource
	sourceGroups           map[manifest.NamedResource]*entrySourceGroup
	loaded                 bool
	built                  bool
	finalized              bool
	loading                bool
	request                selection.PrepareRequest
	preparation            *selection.Preparation
	constructionStarted    bool
	binding                Binding
	entryState             entryQualification
	entryAdmission         *Admission
	entryDemand            bool
	entryReceiptRequest    *manifest.EntryApplication
	entryApplier           manifest.NamedResource
	prepared               map[manifest.NamedResource]*declarationPreparation
	cfg                    Config
	loader                 *loader.Loader
	resolvedSources        *entrySourcePublisher
	remotes                map[string]struct{}
	aliasViews             map[string]*store.SourceArtifact
	hasPins                bool
	result                 *Result
	ownsLoader             bool
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
	if err := d.finishEntryOperands(ctx, repoRoot); err != nil {
		return nil, err
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
		admission.qualification.prepared = d.prepared
	}
	if d.cfg.Trees == nil {
		if admission.qualification != nil {
			admission.qualification.close()
		}
		admission.qualification = nil
	}
	if admission.qualification != nil && d.cfg.Trees != nil && d.cfg.Selection.Changed && d.binding == (Binding{}) {
		d.entryReceipt(ctx, admission, admission.bootstrapArtifact())
		if d.entryReceiptRequest != nil {
			admission.entryDemand = true
			admission.qualification.closed = false
		}
	}
	d.resolvedSources.freeze()
	return admission
}

type declarationRender struct {
	preparation     kustomize.Preparation
	owner           *manifest.Kustomization
	source          *store.SourceArtifact
	children        []store.Child
	failedDocuments []manifest.DocOccurrence
	receipt         *manifest.RenderReceipt
	content         any
	warnings        []manifest.Warning
}

type declarationPreparation struct {
	declarationRender

	complete   bool
	err        error
	work       *manifest.RenderWork
	registered bool
	support    store.ReadSet
}

func (p *declarationPreparation) canSupply(execution manifest.BaseManifest, reads *store.ReadSet) bool {
	if p == nil || !p.complete || !p.registered {
		return false
	}
	return p.support.Valid() && reads.Matches(&p.support) && manifest.Equal(p.owner, execution)
}

type plannedPackage struct {
	render   declarationRender
	register bool
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
	matched := cfg.Entry.matches(input, cfg.Path)
	if !matched && (input != cfg.Entry.Input || cfg.Path != cfg.Entry.Path) {
		return d
	}
	if matched {
		l.Options.ParsedInputs = prepared.parsed
		d.entryPackages = prepared.packages
	} else if cfg.Projections == nil {
		l.Options.Projections = prepared.projections
	}
	d.aliasViews = maps.Clone(prepared.aliasViews)
	d.entryPackagesToQualify = prepared.packagesToQualify
	d.generatorPackages = prepared.generatorPackages
	d.cfg.Entry = cfg.Entry
	d.cfg.Path = cmp.Or(cfg.Entry.target, cfg.Path)
	d.aggregate, d.aggregateRequiresBuild = cfg.Entry.aggregate, prepared.unknown
	return d
}

func (d *Session) loadManifests(ctx context.Context, repoRoot string, build bool) error {
	l := d.loader
	defer func() { l.PreferExisting = false }()
	l.SourceRoot = repoRoot

	total, rounds := 0, 0
	if err := d.loadEntryInput(ctx, l, &total); err != nil {
		return err
	}
	registeredPackage, err := d.preparePlannedPackages(ctx)
	if err != nil {
		return err
	}
	// Projection can change a source reference, which MUST resolve before its path enters scope construction.
	l.PreferExisting = true
	var pending []sourceExpansion
	indexedForQualification := false
	for prepared := true; prepared; {
		if err := ctx.Err(); err != nil {
			return err
		}
		rounds++
		if err := d.admitEntrySources(ctx, repoRoot, registeredPackage); err != nil {
			return err
		}
		kustomizations := d.loader.Kustomizations()
		pending = d.enqueueExpansions(kustomizations, pending[:0])
		next, added, loadErr := d.loadExpansions(ctx, repoRoot, pending, &total)
		pending = next
		switch {
		case loadErr != nil:
			return loadErr
		case added != 0:
			continue
		}
		if err := l.Admit(ctx, repoRoot); err != nil {
			return err
		}
		missing, err := d.resolveEntrySources(ctx, true)
		switch {
		case err != nil:
			return err
		case missing != 0, !slices.EqualFunc(kustomizations, l.Kustomizations(), func(a, b store.Child) bool {
			return manifest.Equal(a.Obj, b.Obj)
		}):
			continue
		case !build:
			prepared = false
			continue
		}
		prepared, err = d.prepareStandaloneEntry(ctx, repoRoot, l, !indexedForQualification)
		indexedForQualification = true
		if err != nil {
			return err
		}
	}
	d.loading, l.PreferExisting = false, false
	d.entryPackages = nil
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

func (d *Session) admitEntrySources(ctx context.Context, repoRoot string, registeredPackage bool) error {
	if err := d.loader.Admit(ctx, repoRoot); err != nil {
		return err
	}
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return err
	}
	if d.hasPins {
		if err := d.discardPinnedWorkingTreeFiles(ctx, repoRoot); err != nil {
			return err
		}
	}
	if registeredPackage {
		_, err := d.loader.Packages(ctx)
		return err
	}
	return nil
}

func (d *Session) prepareStandaloneEntry(
	ctx context.Context, repoRoot string, l *loader.Loader, indexForQualification bool,
) (bool, error) {
	if indexForQualification && d.cfg.Trees != nil {
		if err := l.FinalizeIndex(ctx, repoRoot, false); err != nil {
			return false, err
		}
	}
	ks, err := d.bindEntry(ctx, repoRoot)
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	if err := l.Admit(ctx, repoRoot); err != nil {
		return false, err
	}
	if ks == nil {
		return d.entryState.changed, nil
	}
	sourceID, _ := kustomize.SourceInput(d.cfg.Store, ks)
	if d.cfg.Trees == nil || sourceID == nil || !d.canPrepare(ks) {
		return false, nil
	}
	if previous, ok := d.prepared[ks.Named()]; ok && previous.registered && previous.err == nil &&
		previous.matches(ks, sourceID, nil) {
		return d.entryState.changed, nil
	}
	if err := d.prepareProducer(ctx, ks, sourceID, nil); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if d.aggregate {
			err = manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("requested path %q (select an input path for one entry): %w", d.cfg.Path, err),
				"",
			)
		}
		d.binding.Failure = err
		slog.Debug("discovery: declaration preparation deferred", "err", err)
		return false, nil
	}
	return true, nil
}

// Binding keeps actual ownership separate from the paired entry comparison coordinate.
type Binding struct {
	Producer, Source, Key manifest.NamedResource
	SupportOwner          manifest.NamedResource
	Path                  string
	Descendant            string
	documents             *entryDocuments
	Failure               error
	Entry                 *kustomize.EntryEvidence
	Reads                 *store.ReadSet
	support               *store.ReadSet
}

// Projection borrows the immutable inventory for selection.
func (b *Binding) Projection() selection.EntryIdentity {
	entry := selection.EntryIdentity{
		Producer: b.Producer, Key: b.Key, Source: b.Source, Descendant: b.Descendant,
	}
	if b.documents != nil {
		entry.Documents = *b.documents
	}
	return entry
}

func (d *Session) bindEntry(ctx context.Context, _ string) (*manifest.Kustomization, error) {
	progressed := false
	defer func() { d.entryState.changed = progressed }()
	admission := d.admission()
	if q := admission.qualification; q != nil && q.previous.Receipt == nil && d.cfg.Trees != nil {
		needsHistory := d.hasEntryOperands() || d.entryReceiptRequest != nil || !d.hasEntryBuilder()
		if needsHistory {
			d.entryReceipt(ctx, admission, admission.bootstrapArtifact())
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			admission.cancelQualification()
			return nil, err
		}
		root, binding, err := admission.BindEntry(ctx, d.cfg.Store)
		changed := admission.qualification != nil && admission.qualification.changed
		progressed = progressed || changed
		switch {
		case err != nil:
			return nil, err
		case binding.Reads == nil && changed:
			continue
		case binding.Reads == nil:
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
	artifact, err := kustomize.SourceInput(reads, candidate)
	if err != nil {
		return len(candidate.DependsOn) == 0
	}
	selected := d.resolvedSources.snapshot()[candidate.EffectiveSourceID()]
	workingInput := selected.usesUnfilteredInput(d.cfg.Input, artifact) ||
		selected.usesUnfilteredInput(d.bootstrapInput, artifact)
	// An independent bootstrap entry stays eligible even when its preparation fails.
	if workingInput && !hasEntryPrerequisites(candidate) {
		return true
	}
	prepared, err := d.prepareDocuments(ctx, candidate, artifact, nil)
	if err != nil {
		slog.Debug("discovery: entry candidate preparation deferred", "err", err)
		return workingInput && len(candidate.DependsOn) == 0
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	err := d.renderPreparation(ctx, frame, previous)
	return frame, err
}

func (d *Session) renderPreparation(
	ctx context.Context,
	frame, previous *declarationPreparation,
) error {
	if previous != nil {
		frame.preparation, previous.preparation = previous.preparation, kustomize.Preparation{}
	}
	ks := frame.owner
	sourceID := ks.EffectiveSourceID()
	provider := values.NewStoreProvider(&frame.support)
	var inputs []values.Result
	for ref := range manifest.References(ks, manifest.ReferenceRelations) {
		if keyed, data := ref.DataInput(); data && !keyed {
			inputs = append(
				inputs,
				values.Resolve(
					&values.ResolveRequest{
						Reference:  ref,
						Facts:      provider.Lookup(ref.NamedResource),
						Permission: d.loader.Options.Missing,
					},
				),
			)
		}
	}
	for i := range inputs {
		if err := inputs[i].MaterialError(); err != nil {
			frame.err, frame.complete = err, true
			return err
		}
	}
	for i := range inputs {
		if !inputs[i].CanPrepare() {
			return nil
		}
	}
	policy := d.cfg.packagePreparation()
	work := d.preparationWork(ks, sourceID).RenderWork()
	err := policy.inventory(ctx, &frame.declarationRender, inputs, work)
	if err == nil {
		frame.work = work
		err = manifest.DocumentFailure(frame.failedDocuments)
	}
	frame.err, frame.complete = err, ctx.Err() == nil
	return err
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
	return d.registerProducer(ctx, prepared)
}

func (d *Session) preparePackageInventory(
	ctx context.Context, ks *manifest.Kustomization, input *store.SourceArtifact, guard *store.ReadSet,
) (*declarationPreparation, error) {
	prepared, err := d.prepareDocuments(ctx, ks, input, guard)
	if prepared != nil && prepared.complete {
		err = prepared.inventoryError()
	}
	return prepared, err
}

func (d *Session) prepareEntryPackage(
	ctx context.Context, ks *manifest.Kustomization, input *store.SourceArtifact, guard *store.ReadSet,
) error {
	if !ks.Named().ReportHidden() {
		return d.prepareProducer(ctx, ks, input, guard)
	}
	prepared, err := d.preparePackageInventory(ctx, ks, input, guard)
	if err != nil {
		return err
	}
	return d.registerProducer(ctx, prepared)
}

func (d *Session) registerProducer(ctx context.Context, prepared *declarationPreparation) error {
	if !prepared.complete {
		return nil
	}
	sourceID := prepared.owner.EffectiveSourceID()
	owner := prepared.owner.Named()
	root := filepath.Join(prepared.source.LocalPath, manifest.NormalizeClaimBase(prepared.owner.Path))
	if err := d.loader.RegisterBuilt(
		ctx,
		loader.BuiltInput{
			Root:             root,
			Owner:            owner,
			Source:           sourceID,
			Reads:            kustomize.RenderInputs(prepared.receipt),
			Support:          &prepared.support,
			PrepareDocuments: d.cfg.PrepareDocuments,
		},
		prepared.children,
	); err != nil {
		return err
	}
	prepared.registered = true
	d.prepared[owner] = prepared
	if prepared.err == nil && d.binding.Producer == owner {
		d.binding.Failure = nil
	}
	return nil
}

func (d *Session) acceptPrepared(ctx context.Context) error {
	for _, prepared := range d.prepared {
		ks := prepared.owner
		if !prepared.registered || prepared.err != nil {
			continue
		}
		sourceID := ks.EffectiveSourceID()
		input := prepared.source
		if err := d.cfg.Cache.RecordRender(ctx, cache.RenderAcceptance{
			Reads: &prepared.support,
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
