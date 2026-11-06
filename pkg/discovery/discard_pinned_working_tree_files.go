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
		if _, resolved := d.resolvedSources[id]; resolved {
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
	scratch []loader.Occurrence,
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
		work:     d.cfg.Work.Source(expansion.ks.SourceID()).Record(manifest.WorkPreparation, dir),
		artifact: expansion.source,
	})
}

// Begin binds sources and collects declarations without evaluating standalone packages.
//
//nolint:gocritic // Public session construction preserves its configuration value snapshot.
func Begin(ctx context.Context, cfg Config) (*Session, error) {
	if cfg.Store == nil {
		return nil, errors.New("discovery: Store is required")
	}
	d := newDiscoverer(&cfg)
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return nil, err
	}
	root := d.cfg.Input.LocalPath
	if err := d.loadManifests(ctx, root, false); err != nil {
		return nil, err
	}
	if err := d.loader.FinalizeIndex(ctx, root, false); err != nil {
		return nil, err
	}
	return d, nil
}

// Advance prepares source-qualified declarations and packages required by admission.
func (d *Session) Advance(ctx context.Context, admission *loader.Admission) (bool, error) {
	defer admission.Retire()
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context: %w", err)
	}
	if d.result != nil || admission == nil {
		return false, nil
	}
	d.constructionStarted = true
	d.request = admission.Request
	d.request.Incoming = store.Child{}
	// Admission can retire provisional relations, so construction MUST capture their owners first.
	owners := d.neededMaterializers(admission.Needed)
	admitRequiredObjectsResult2, admitRequiredObjectsResult2Err := d.admitRequiredObjects(ctx, admission)
	changed, refresh, err := admitRequiredObjectsResult2.changed, admitRequiredObjectsResult2.refresh, admitRequiredObjectsResult2Err
	if err != nil {
		return false, err
	}
	root := d.cfg.Input.LocalPath
	d.entryState.changed = false
	pending, err := d.pendingProducers(ctx, admission, root, owners)
	if err != nil {
		return false, err
	}
	if d.entryState.changed {
		changed, refresh = true, true
	}
	for _, id := range slices.SortedFunc(maps.Keys(pending), manifest.NamedResource.Compare) {
		ks := pending[id]
		prepared, err := d.preparePendingProducer(ctx, ks)
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
		if prepared {
			changed, refresh = true, true
		}
	}
	if !changed || !refresh {
		return changed, nil
	}
	if err := d.refreshDeclarations(ctx, root); err != nil {
		return false, err
	}
	return true, nil
}

type requiredAdmission struct {
	changed bool
	refresh bool
}

func (d *Session) admitRequiredObjects(ctx context.Context, proposal *loader.Admission) (requiredAdmission, error) {
	changed, refresh := false, false
	for i := range proposal.Objects {
		child := &proposal.Objects[i]
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
		if child.Obj != nil && d.cfg.Store.GetObject(child.Obj.Named()) == nil {
			changed = true
			_, _, hasPath := child.Obj.SourcePath()
			_, source := child.Obj.(entrySource)
			refresh = refresh || hasPath || source
		}
	}
	committed, err := d.loader.Commit(ctx, proposal, loader.Construction)
	if err != nil || !committed {
		return requiredAdmission{}, err
	}
	return requiredAdmission{changed: changed, refresh: refresh}, nil
}

func (d *Session) neededMaterializers(needed []manifest.NamedResource) map[manifest.NamedResource]bool {
	owners := map[manifest.NamedResource]bool{}
	for _, id := range needed {
		if !id.KindInfo().Runnable {
			continue
		}
		{
			productionValues := d.cfg.Store.Productions(id, d.loader.Index)
			for productionIndex := range productionValues {
				production := productionValues[productionIndex]

				if owner := production.Materializer(id); owner != id {
					owners[owner] = true
				}
			}
		}
	}
	return owners
}

func (d *Session) pendingProducers(
	ctx context.Context,
	admission *loader.Admission,
	root string,
	owners map[manifest.NamedResource]bool,
) (map[manifest.NamedResource]*manifest.Kustomization, error) {
	pending := map[manifest.NamedResource]*manifest.Kustomization{}
	for _, packageRoot := range d.loader.StandaloneRoots(root) {
		if !slices.Contains(admission.Blocked, packageRoot) {
			continue
		}
		ks := manifest.NewEntryKustomization(manifest.BootstrapSourceID, packageRoot, manifest.NamedResource{})
		if filepath.Join(root, packageRoot) == d.cfg.Path {
			d.entryDemand = true
			var err error
			ks, err = d.bindEntry(ctx, root)
			if err != nil {
				return nil, err
			}
			if ks == nil {
				continue
			}
		}
		pending[ks.Named()] = ks
	}
	d.loader.Admit(root)
	kustomizations := d.loader.Kustomizations()
	for i := range kustomizations {
		child := &kustomizations[i]
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		if !owners[ks.Named()] || ks.IsFragment() {
			continue
		}
		record, _ := d.loader.Index.Lookup(ks.Named())
		if d.cfg.Store.GetDeclaration(ks.Named()) != nil || record.Builder != "" {
			pending[ks.Named()] = ks
		}
	}
	return pending, nil
}

func (d *Session) preparePendingProducer(ctx context.Context, ks *manifest.Kustomization) (bool, error) {
	input, _, err := kustomize.SourceInput(d.cfg.Store, ks)
	if err != nil || d.cfg.Trees == nil || ks.PostBuild != nil && len(ks.PostBuild.SubstituteFrom) != 0 {
		return false, nil //nolint:nilerr // Unavailable inputs defer preparation to reconciliation.
	}
	if previous, ok := d.prepared[ks.Named()]; ok && previous.registered &&
		previous.matches(ks, input, d.substitutions, nil) {
		return false, nil
	}
	if err := d.prepareProducer(ctx, ks, input, nil); err != nil {
		return false, err
	}
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

// Freeze publishes the final index once and releases construction ownership.
func (d *Session) Freeze(ctx context.Context) (*Result, error) {
	if d.result != nil {
		return d.result, nil
	}
	cfg := d.cfg
	cfg.session = d
	if err := d.loadManifests(ctx, d.cfg.Input.LocalPath, d.requiresEntry()); err != nil {
		return nil, err
	}
	result, err := Run(ctx, cfg)
	if err != nil {
		return nil, err
	}
	result.Admission.session = nil
	if result.Admission.qualification != nil {
		result.Admission.qualification.session = nil
	}
	d.request = loader.PrepareRequest{}
	d.result = result
	return result, nil
}

type sourceLoad struct {
	owner    manifest.NamedResource
	work     *manifest.WorkRecord
	artifact *store.SourceArtifact
}
