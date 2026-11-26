package discovery

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// A broad --path scan can overlap a pinned subtree. Its working-tree objects
// must be discarded before the committed subtree supplies discovery metadata.
// Resolved sources must retain their objects and artifacts so a pinned root
// claim cannot replace its own source with a working-tree alias.
func (d *Session) discardPinnedWorkingTreeFiles(ctx context.Context, repoRoot string) error {
	resolvedSources := d.resolvedSources.snapshot()
	var scratch [8]manifest.NamedResource
	admitted := slices.AppendSeq(scratch[:0], maps.Keys(resolvedSources))
	if err := d.loader.Admit(ctx, repoRoot, admitted...); err != nil {
		return err
	}
	occurrences := d.loader.PathObjects()
	pinnedWorkingTreeClaimsResult1 := d.pinnedWorkingTreeClaims(repoRoot, occurrences)
	index, pinnedSources := pinnedWorkingTreeClaimsResult1.index, pinnedWorkingTreeClaimsResult1.sources
	if len(index.Claims) == 0 {
		return nil
	}
	for id, record := range d.loader.Index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if _, resolved := resolvedSources[id]; resolved {
			continue
		}
		if _, pinned := pinnedSources[id]; pinned {
			continue
		}
		if !d.pinnedWorkingTreeFile(repoRoot, &index, id, &record) {
			continue
		}
		d.loader.DiscardOccurrence(id)
	}
	return nil
}

type pinnedClaims struct {
	index   manifest.DiscoveryIndex
	sources map[manifest.NamedResource]struct{}
}

// scratch MUST be caller-owned; package-owned occurrence slices are immutable.
func (d *Session) pinnedWorkingTreeClaims(
	repoRoot string,
	scratch []selection.OccurrenceFacts,
) pinnedClaims {
	pinned := scratch[:0]
	var sources map[manifest.NamedResource]struct{}
	for i := range scratch {
		o := &scratch[i]
		ref, _, hasPath := o.SourcePath()
		if !hasPath || !loader.SourceView(d.cfg.Store, ref, repoRoot).Pinned {
			continue
		}
		if sources == nil {
			sources = map[manifest.NamedResource]struct{}{}
		}
		pinned = append(pinned, *o)
		sources[ref] = struct{}{}
	}
	if len(pinned) == 0 {
		return pinnedClaims{}
	}
	index := manifest.DiscoveryIndex{Claims: map[string][]manifest.NamedResource{}}
	for _, claim := range d.loader.OccurrenceClaims(repoRoot, pinned) {
		index.Claims[claim.Prefix] = append(index.Claims[claim.Prefix], claim.ID)
	}
	return pinnedClaims{index: index, sources: sources}
}

func (d *Session) pinnedWorkingTreeFile(
	repoRoot string,
	index *manifest.DiscoveryIndex,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) bool {
	if index == nil || len(index.Claims) == 0 {
		return false
	}
	file := record.File
	if !safepath.Contains(repoRoot, filepath.Join(repoRoot, filepath.FromSlash(file))) {
		projected := d.loader.WorkingTreePath(record.Path)
		if !safepath.Contains(repoRoot, projected) {
			return false
		}
		relative, err := filepath.Rel(repoRoot, projected)
		if err != nil {
			return false
		}
		file = filepath.ToSlash(relative)
	}
	for range index.ClaimOwners(file, manifest.NamedResource{}, id, nil) {
		return true
	}
	return false
}

// The Loader deduplicates unchanged views; unavailable followed paths fail during reconciliation.
func (d *Session) loadAt(
	ctx context.Context,
	dir string,
	total *int,
	input *sourceLoad,
) (int, error) {
	n, err := d.loader.Load(ctx, dir, input.owner, input.work)
	if d.loader.IgnoreFile == "" && errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		if errors.Is(err, fs.ErrPermission) && input.artifact != nil && input.artifact.FS != nil {
			name, _ := filepath.Rel(input.artifact.LocalPath, dir)
			if _, statErr := fs.Stat(input.artifact.FS, filepath.ToSlash(name)); errors.Is(statErr, fs.ErrPermission) {
				return 0, nil
			}
		}
		return 0, err
	}
	*total += n
	return n, nil
}

func (d *Session) loadSourcePath(
	ctx context.Context,
	dir string,
	expansion *sourceExpansion,
	total *int,
) (int, error) {
	owner := expansion.ks.Named()
	if !d.constructionStarted {
		owner = manifest.NamedResource{}
	}
	return d.loadAt(ctx, dir, total, &sourceLoad{
		owner:    owner,
		work:     d.cfg.Work.Source(expansion.ks.EffectiveSourceID()).Record(manifest.WorkPreparation, dir),
		artifact: expansion.source,
	})
}

// Begin admits the bootstrap declaration before any material-dependent construction.
//
//nolint:gocritic // Public session construction preserves its configuration value snapshot.
func Begin(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	d := newDiscoverer(&cfg)
	if d.loader.Options.Session == nil {
		var err error
		d.loader.Options.Session, err = selection.New(cfg.Store, d.loader, cfg.Selection)
		if err != nil {
			return nil, err
		}
	}
	complete := false
	defer func() {
		if !complete {
			d.loader.BindProvisionalInput(nil)
			d.Close()
		}
	}()
	bootstrap, err := d.resolveBootstrapInput(ctx)
	if err != nil {
		return nil, err
	}
	admission := d.admission()
	admission.input = bootstrap
	group, err := d.bootstrapDeclaration(ctx, bootstrap)
	if err != nil {
		return nil, err
	}
	if _, err := d.resolveEntrySource(
		ctx,
		admission,
		manifest.BootstrapSourceID,
		group,
	); err != nil &&
		!errors.Is(err, errPendingSources) {
		return nil, err
	}
	d.loader.BindProvisionalInput(d.admission().Input())

	complete = true
	return d, nil
}

// Load resumes construction after the caller reconciles PendingSources.
func (d *Session) Load(ctx context.Context) (bool, error) {
	if d.loaded {
		return true, nil
	}
	d.pendingSources = d.pendingSources[:0]
	if err := d.load(ctx); err != nil {
		if errors.Is(err, errPendingSources) {
			return false, nil
		}
		return false, err
	}
	d.loaded = true
	return true, nil
}

func (d *Session) load(ctx context.Context) error {
	if err := d.refreshDeclarations(ctx, d.cfg.Input.LocalPath, d.aggregateRequiresBuild); err != nil {
		return err
	}
	admission := d.admission()
	if d.cfg.Selection.Changed && d.cfg.Trees != nil && !d.hasEntryBuilder() {
		d.entryReceipt(ctx, admission, admission.Input())
	}
	return nil
}

// Advance MUST consume only accepted observations after admission publication.
func (d *Session) Advance(ctx context.Context, admission *selection.AdmissionResult) (bool, error) {
	d.pendingSources = d.pendingSources[:0]
	d.pendingMaterializers = d.pendingMaterializers[:0]
	if admission != nil {
		for i := range admission.Demands {
			demand := &admission.Demands[i]
			if demand.Materialize && d.cfg.Store.GetObject(demand.Identity) != nil {
				d.pendingMaterializers = append(d.pendingMaterializers, demand.Identity)
			}
		}
		slices.SortFunc(d.pendingMaterializers, manifest.NamedResource.Compare)
	}
	changed, err := d.advance(ctx, admission)
	if errors.Is(err, errPendingSources) {
		return true, nil
	}
	return changed, err
}

func (d *Session) advance(ctx context.Context, admission *selection.AdmissionResult) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	if d.result != nil || admission == nil {
		return false, nil
	}
	d.constructionStarted = true
	previousReceipt := d.entryReceiptRequest
	d.entryReceiptRequest = admission.EntryReceipt
	d.request = admission.Request
	d.request.Generation = nil
	observed, err := d.observeRequiredObjects(ctx, admission)
	changed, refresh := observed.changed, observed.refresh
	if err != nil {
		return false, err
	}
	changed = d.refreshEntryReceipt(ctx, previousReceipt) || changed
	root := d.cfg.Input.LocalPath
	lookupChanged, err := d.refreshEnclosingLookups(ctx, root)
	if err != nil {
		return false, err
	}
	changed, refresh = changed || lookupChanged, refresh || lookupChanged
	prepared, err := d.prepareDemandedProducers(ctx, admission.Demands, root)
	if err != nil {
		return false, err
	}
	changed, refresh = changed || prepared, refresh || prepared
	for d.request.Preparation != nil {
		frontier, err := d.loader.Options.Session.RefreshProducers(ctx, admission, d.request)
		if err != nil {
			return false, err
		}
		prepared, err = d.prepareDemandedProducers(ctx, slices.Collect(frontier.Demands()), root)
		frontier.Retire()
		if err != nil {
			return false, err
		}
		if !prepared {
			break
		}
		changed, refresh = true, true
	}
	if !refresh {
		return changed, nil
	}
	if err := d.refreshDeclarations(ctx, root, false); err != nil {
		return false, err
	}
	return true, nil
}

func (d *Session) prepareDemandedProducers(ctx context.Context, demands []selection.Demand, root string) (bool, error) {
	d.entryState.changed = false
	pending, err := d.pendingProducers(ctx, demands, root)
	if err != nil {
		return false, err
	}
	changed := d.entryState.changed
	for _, id := range slices.SortedFunc(maps.Keys(pending), manifest.NamedResource.Compare) {
		demand := pending[id]
		ks, _ := demand.Witness.(*manifest.Kustomization)
		if demand.Root == "" && !demand.Reads.Valid() {
			continue
		}
		var guard *store.ReadSet
		if demand.Root == "" {
			guard = demand.Reads
		}
		prepared, err := d.preparePendingProducer(ctx, ks, guard)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if d.binding.Producer == ks.Named() {
				d.binding.Failure = err
			}
			slog.Debug("discovery: declaration preparation deferred", "err", err)
			return false, nil
		}
		changed = changed || prepared
	}
	return changed, nil
}

func (d *Session) refreshEntryReceipt(ctx context.Context, previous *manifest.EntryApplication) bool {
	if d.entryReceiptRequest == nil || d.entryReceiptRequest == previous || d.cfg.Trees == nil {
		return false
	}
	current := d.admission()
	if current.qualification == nil || current.qualification.receiptRead {
		return false
	}
	d.entryReceipt(ctx, current, d.cfg.Input)
	return true
}

type requiredAdmission struct {
	changed bool
	refresh bool
}

func (d *Session) observeRequiredObjects(
	ctx context.Context,
	admission *selection.AdmissionResult,
) (requiredAdmission, error) {
	changed, refresh := false, false
	absent := 0
	for i := range admission.Objects {
		child := &admission.Objects[i]
		if err := ctx.Err(); err != nil {
			return requiredAdmission{changed: false, refresh: false}, fmt.Errorf("context: %w", err)
		}
		if child.Obj != nil {
			for warningIndex := range child.Warnings {
				warning := child.Warnings[warningIndex]
				warning.Resource = child.Obj.Named()
				d.cfg.Store.AddWarning(warning)
			}
		}
		if child.Obj != nil && absent < len(admission.Absent) && admission.Absent[absent] == child.Obj.Named() {
			absent++
			_, _, hasPath := child.Obj.SourcePath()
			source := child.Obj.Named().KindInfo().IsSource()
			changed = changed || hasPath || source || child.Entry != nil
			refresh = refresh || hasPath || source
		}
	}
	return requiredAdmission{changed: changed, refresh: refresh}, nil
}

func (d *Session) pendingProducers(
	ctx context.Context,
	demands []selection.Demand,
	root string,
) (map[manifest.NamedResource]selection.Demand, error) {
	var selected []*selection.Demand
	for i := range demands {
		if demands[i].Application != (selection.OccurrenceID{}) {
			selected = append(selected, &demands[i])
		}
	}
	if len(selected) != 0 {
		if err := d.qualifySelectedEntry(ctx, selected, root); err != nil {
			return nil, err
		}
	}
	pending := map[manifest.NamedResource]selection.Demand{}
	for i := range demands {
		demand := &demands[i]
		if demand.History || demand.Materialize || demand.Occurrence.Identity != (manifest.NamedResource{}) ||
			demand.Application != (selection.OccurrenceID{}) {
			continue
		}
		ks, err := d.pendingProducer(ctx, demand, root)
		if err != nil {
			return nil, err
		}
		d.request.Acquired = append(
			d.request.Acquired,
			selection.Acquisition{Demand: *demand, Reads: demand.Reads, Available: ks != nil},
		)
		if ks != nil && !ks.IsFragment() {
			current := *demand
			current.Witness = ks
			pending[ks.Named()] = current
		}
	}
	return pending, nil
}

func (d *Session) pendingProducer(
	ctx context.Context,
	demand *selection.Demand,
	root string,
) (*manifest.Kustomization, error) {
	if demand.Root == "" {
		ks, _ := demand.Witness.(*manifest.Kustomization)
		if ks != nil {
			ks, _ = manifest.CatalogObject(ks, demand.Identity).(*manifest.Kustomization)
		}
		return ks, nil
	}
	if filepath.Join(root, demand.Root) == d.cfg.Path {
		d.entryDemand = true
		a := d.admission()
		if len(a.entryTargets) != 0 {
			previous, _ := d.cfg.Store.GetObject(d.binding.Producer).(*manifest.Kustomization)
			return previous, nil
		}
		if a.qualification != nil && !a.entryBound.Load() {
			a.entryDemand, a.qualification.closed = true, false
		}
		return d.bindEntry(ctx, root)
	}
	return manifest.NewEntryKustomization(demand.Source, demand.Root, manifest.NamedResource{}), nil
}

func (d *Session) qualifySelectedEntry(ctx context.Context, demands []*selection.Demand, root string) error {
	a := d.admission()
	if a.entryBound.Load() {
		return nil
	}
	coverEntry := d.cfg.Selection.Matches(manifest.NamedResource{})
	updated := false
	for _, demand := range demands {
		query := demand.Application
		if slices.ContainsFunc(a.entryTargets, func(target entryTarget) bool { return target.occurrence == query }) {
			continue
		}
		file := demand.Artifact.OriginPath(filepath.Join(query.Source, query.File))
		origin := a.input.OriginPath(a.input.LocalPath)
		if !safepath.Contains(origin, file) {
			continue
		}
		a.entryTargets = append(a.entryTargets, entryTarget{occurrence: query, origin: file, coverEntry: coverEntry})
		updated = true
	}
	if len(a.entryTargets) == 0 {
		return nil
	}
	if updated {
		slices.SortFunc(a.entryTargets, func(left, right entryTarget) int {
			l, r := left.occurrence, right.occurrence
			return cmp.Or(
				cmp.Compare(left.origin, right.origin),
				cmp.Compare(l.Document, r.Document),
				cmp.Compare(l.Item, r.Item),
			)
		})
		origin := a.input.OriginPath(a.input.LocalPath)
		a.entryOccurrences = make([]kustomize.EntryOccurrence, len(a.entryTargets))
		for i, target := range a.entryTargets {
			relative, err := filepath.Rel(origin, target.origin)
			if err != nil {
				return fmt.Errorf("resolve selected application coordinate: %w", err)
			}
			a.entryOccurrences[i] = kustomize.EntryOccurrence{
				File: filepath.ToSlash(relative), Document: target.occurrence.Document, Item: target.occurrence.Item,
			}
		}
		d.entryDemand, a.entryDemand = true, true
		a.qualification.closed, a.qualification.receiptRead = false, false
		a.qualification.previous, a.qualification.refresh = kustomize.EntryEvidence{}, nil
		a.publishEntry(nil, manifest.NamedResource{})
	}
	_, err := d.bindEntry(ctx, root)
	d.entryState.changed = d.entryState.changed || a.entryBound.Load()
	return err
}

func (d *Session) preparePendingProducer(
	ctx context.Context,
	ks *manifest.Kustomization,
	guard *store.ReadSet,
) (bool, error) {
	var objects interface {
		GetObject(manifest.NamedResource) manifest.BaseManifest
		GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	} = d.cfg.Store
	if guard != nil {
		objects = guard
	}
	input, err := kustomize.SourceInput(objects, ks)
	if err != nil || !guard.Valid() || d.cfg.Trees == nil {
		return false, nil //nolint:nilerr // Unavailable inputs defer preparation to reconciliation.
	}
	if d.loader.EntryScope(ks.Named()) != nil {
		return d.prepareLookupPackage(ctx, ks, input, nil)
	}
	if !d.canPrepare(ks) {
		return false, nil
	}
	prepared, err := d.prepareDocuments(ctx, ks, input, nil)
	if err != nil {
		return false, err
	}
	if !guard.Valid() || prepared.publication == loader.BuiltRegistered {
		return false, nil
	}
	return d.registerProducer(ctx, prepared)
}

func (d *Session) refreshDeclarations(ctx context.Context, root string, build bool) error {
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return err
	}
	if err := d.loadManifests(ctx, root, build); err != nil {
		return err
	}
	if err := d.loader.FinalizeIndex(ctx, root, false); err != nil {
		return err
	}
	named := slices.ContainsFunc(d.cfg.Selection.Roots, func(id manifest.NamedResource) bool { return id.Name != "" })
	if named || d.cfg.Selection.Changed {
		a := d.admission()
		projected := a.entryProjected
		if !a.entryBound.Load() && a.qualification != nil && len(a.entryTargets) == 0 {
			projected = a.projectedEntry(named)
		}
		patches := !named && a.projectedEntryPatches()
		if projected != a.entryProjected || patches != a.entryProjectedPatches {
			a.entryProjected, a.entryProjectedPatches = projected, patches
			a.publishEntry(nil, manifest.NamedResource{})
		}
	}
	return nil
}

// Freeze requires the caller to reconcile PendingSources whenever it returns no result.
func (d *Session) Freeze(ctx context.Context) (*Result, error) {
	if d.result != nil {
		return d.result, nil
	}
	d.pendingSources = d.pendingSources[:0]
	if !d.built && !d.finalized {
		if err := d.loadManifests(ctx, d.cfg.Input.LocalPath, d.requiresEntry()); err != nil {
			if errors.Is(err, errPendingSources) {
				return nil, nil //nolint:nilnil // The caller MUST reconcile PendingSources before resuming.
			}
			return nil, err
		}
	}
	result, err := d.finishDiscovery(ctx, d.cfg.Input.LocalPath, d.loader)
	if errors.Is(err, errPendingSources) {
		return nil, nil //nolint:nilnil // The caller MUST reconcile PendingSources before resuming.
	}
	if err != nil {
		return nil, err
	}
	for _, frame := range d.prepared {
		frame.retireSupport(d.cfg.Store)
	}

	result.Admission.session = nil
	if result.Admission.qualification != nil {
		result.Admission.qualification.session = nil
	}
	d.preparation.Retire()
	d.preparation = nil
	d.request = selection.PrepareRequest{}
	d.result = result
	return result, nil
}

type sourceLoad struct {
	owner    manifest.NamedResource
	work     *manifest.WorkRecord
	artifact *store.SourceArtifact
}
