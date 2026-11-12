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

	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// A broad --path scan can overlap a pinned subtree. Its working-tree objects
// must be discarded before the committed subtree supplies discovery metadata.
// Resolved sources must retain their objects and artifacts so a pinned root
// claim cannot replace its own source with a working-tree alias.
func (d *Session) discardPinnedWorkingTreeFiles(repoRoot string) {
	d.loader.Admit(repoRoot)
	occurrences := d.loader.PathObjects()
	pinnedWorkingTreeClaimsResult1 := d.pinnedWorkingTreeClaims(repoRoot, occurrences)
	prefixes, pinnedSources := pinnedWorkingTreeClaimsResult1.claims, pinnedWorkingTreeClaimsResult1.sources
	if len(prefixes) == 0 {
		return
	}
	for id, record := range d.loader.Index.Files() { //nolint:gocritic // The iterator supplies independent value snapshots.
		if _, resolved := d.resolvedSources.snapshot()[id]; resolved {
			continue
		}
		if _, pinned := pinnedSources[id]; pinned {
			continue
		}
		if !d.pinnedWorkingTreeFile(repoRoot, prefixes, id, &record) {
			continue
		}
		d.cfg.Store.DeleteObject(id)
		delete(d.loader.Index.Records, id)
	}
}

type pinnedClaims struct {
	claims  []manifest.KSClaim
	sources map[manifest.NamedResource]struct{}
}

// scratch MUST be caller-owned; package-owned occurrence slices are immutable.
func (d *Session) pinnedWorkingTreeClaims(
	repoRoot string,
	scratch []selection.OccurrenceFacts,
) pinnedClaims {
	pinned := scratch[:0]
	sources := map[manifest.NamedResource]struct{}{}
	{
		oValues := scratch
		for oIndex := range oValues {
			o := oValues[oIndex]

			ref, _, hasPath := o.SourcePath()
			if !hasPath {
				continue
			}
			sourceViewResult1 := loader.SourceView(d.cfg.Store, ref, repoRoot)
			if _, pin, _ := sourceViewResult1.Path, sourceViewResult1.Pinned, sourceViewResult1.External; pin {
				pinned = append(pinned, o)
				sources[ref] = struct{}{}
			}
		}
	}
	return pinnedClaims{claims: d.loader.OccurrenceClaims(repoRoot, pinned), sources: sources}
}

func (d *Session) pinnedWorkingTreeFile(
	repoRoot string,
	prefixes []manifest.KSClaim,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
) bool {
	if len(prefixes) == 0 {
		return false
	}
	file := record.File
	if !safepath.Contains(repoRoot, filepath.Join(repoRoot, filepath.FromSlash(file))) {
		for source := range d.resolvedSources.snapshot() {
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
			d.Close()
		}
	}()
	bootstrap, err := d.resolveBootstrapInput(ctx)
	if err != nil {
		return nil, err
	}
	d.admission().input = bootstrap
	if _, err := d.resolveEntrySource(
		ctx,
		d.admission(),
		manifest.BootstrapSourceID,
		&entrySourceGroup{required: true},
	); err != nil &&
		!errors.Is(err, errPendingSources) {
		return nil, err
	}

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
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return err
	}
	root := d.cfg.Input.LocalPath
	if err := d.loadManifests(ctx, root, false); err != nil {
		return err
	}
	if err := d.loader.FinalizeIndex(ctx, root, false); err != nil {
		return err
	}
	admission := d.admission()
	if d.cfg.Selection.Changed && d.cfg.Trees != nil && !d.hasEntryBuilder() {
		d.entryReceipt(ctx, admission, admission.input)
	}
	return nil
}

// Advance MUST consume only accepted observations after admission publication.
func (d *Session) Advance(ctx context.Context, admission *selection.AdmissionResult) (bool, error) {
	d.pendingSources = d.pendingSources[:0]
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
	if d.refreshEntryReceipt(ctx, previousReceipt) {
		changed, refresh = true, true
	}
	root := d.cfg.Input.LocalPath
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
	if err := d.refreshDeclarations(ctx, root); err != nil {
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
				return false, ctx.Err() //nolint:wrapcheck // Preserve cancellation identity after producer preparation.
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
	if current.qualification == nil {
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
			_, source := child.Obj.(entrySource)
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
	pending := map[manifest.NamedResource]selection.Demand{}
	for i := range demands {
		demand := &demands[i]
		if demand.History || demand.Occurrence.Identity != (manifest.NamedResource{}) {
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
		return d.bindEntry(ctx, root)
	}
	return manifest.NewEntryKustomization(demand.Source, demand.Root, manifest.NamedResource{}), nil
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
	input, _, err := kustomize.SourceInput(objects, ks)
	if err != nil || !guard.Valid() || d.cfg.Trees == nil ||
		ks.PostBuild != nil && len(ks.PostBuild.SubstituteFrom) != 0 {
		return false, nil //nolint:nilerr // Unavailable inputs defer preparation to reconciliation.
	}
	if previous, ok := d.prepared[ks.Named()]; ok && previous.registered &&
		previous.matches(ks, input, nil) {
		return false, nil
	}
	prepared, err := d.prepareDocuments(ctx, ks, input, nil)
	if err != nil {
		return false, err
	}
	if !guard.Valid() {
		return false, nil
	}
	d.registerProducer(prepared)
	return true, nil
}

func (d *Session) refreshDeclarations(ctx context.Context, root string) error {
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return err
	}
	if err := d.loadManifests(ctx, root, false); err != nil {
		return err
	}
	if err := d.loader.FinalizeIndex(ctx, root, false); err != nil {
		return err
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
