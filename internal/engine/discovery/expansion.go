package discovery

import (
	"cmp"
	"context"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type sourceExpansion struct {
	ks     *manifest.Kustomization
	source *store.SourceArtifact
}

func (d *Session) loadEntryInput(ctx context.Context, l *loader.Loader, total *int) error {
	if d.loading {
		return nil
	}
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
		return fmt.Errorf("input path %q: %w", d.cfg.Path, err)
	} else if !info.IsDir() {
		return fmt.Errorf("input path %q is not a directory", d.cfg.Path)
	}
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if _, err := d.loadAt(
		ctx,
		d.cfg.Path,
		total,
		&sourceLoad{work: d.cfg.Work.Record(manifest.WorkPreparation, d.cfg.Path)},
	); err != nil {
		return err
	}
	l.IgnoreFile = ""
	if err := d.prepareEnclosingPackages(ctx, input, filepath.ToSlash(name)); err != nil {
		return err
	}
	d.loading = true
	return nil
}

func (d *Session) prepareEnclosingPackages(ctx context.Context, input *store.SourceArtifact, entry string) error {
	if d.cfg.Trees == nil || entry == "." || !d.cfg.Selection.RestrictedPackageLookup() ||
		d.partitionedEntry {
		return nil
	}
	owned, err := d.prepareEnclosingApplications(ctx, input, entry)
	if err != nil {
		return err
	}
	var packages []string
	for _, target := range d.cfg.Selection.Roots {
		if target.Name == "" && target.Namespace == "" {
			continue
		}
		enclosing, err := d.loader.EnclosingPackages(ctx, input, entry, target)
		if err != nil {
			return err
		}
		for _, path := range enclosing {
			origin := input.OriginPath(filepath.Join(input.LocalPath, path))
			if !owned[origin] && !slices.Contains(packages, path) {
				packages = append(packages, path)
			}
		}
	}
	for _, path := range packages {
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, path, manifest.NamedResource{})
		if err := d.prepareLookupPackage(ctx, owner, input, nil); err != nil {
			return err
		}
	}
	return nil
}

func (d *Session) prepareEnclosingApplications(
	ctx context.Context, input *store.SourceArtifact, entry string,
) (map[string]bool, error) {
	applications, recipes, err := loader.ApplicationPackages(ctx, inputprojection.Authored(input), ".", entry)
	if err != nil {
		return nil, err
	}
	admission := d.admission()
	entryOrigin := input.OriginPath(d.cfg.Path)
	var appInputs []string
	if len(recipes) != 0 {
		appInputs, err = d.lookupApplicationInputs(ctx, input, entry)
		if err != nil {
			return nil, err
		}
	}
	d.lookupSources = nil
	owned := map[string]bool{}
	if err := d.qualifyLookupOwners(ctx, applications, false, owned); err != nil {
		return nil, err
	}
	var reads store.ReadSet
	reads.Init(d.cfg.Store, manifest.BootstrapSourceID, nil)
	defer d.cfg.Store.Retire(&reads)
	defer reads.ReleaseCapture()
	named := d.cfg.Selection.Roots[0].Name != ""
	for i := range recipes {
		pkg := &recipes[i]
		pkgOrigin := input.OriginPath(filepath.Join(input.LocalPath, pkg.Path))
		if slices.ContainsFunc(appInputs, func(origin string) bool { return safepath.Contains(origin, pkgOrigin) }) {
			continue
		}
		mode := requiredLookup
		if !pkg.RequiresBuild {
			switch reached, err := admission.authoredOwnersReachEntry(ctx, &reads, pkg.Owners, entryOrigin); {
			case err != nil:
				return nil, err
			case !reached:
				continue
			}
		} else if named {
			mode = exploratoryLookup
		}
		owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, pkg.Path, manifest.NamedResource{})
		frame, err := d.lookupInventory(ctx, owner, input, mode)
		if err != nil {
			return nil, err
		}
		if !frame.inventoryReady() {
			continue
		}
		reached := d.loader.EntryPackageReached(input, pkg.Path, owner, entryOrigin)
		if err := d.qualifyLookupOwners(ctx, frame.children, reached, owned); err != nil {
			return nil, err
		}
	}
	slices.SortFunc(d.lookupSources, func(a, b selection.LookupSource) int {
		return cmp.Or(a.Source.Compare(b.Source), a.Materializer.Compare(b.Materializer))
	})
	d.lookupSources = slices.Compact(d.lookupSources)
	return owned, nil
}

func (a *Admission) authoredOwnersReachEntry(
	ctx context.Context, reads *store.ReadSet, owners []store.Child, entry string,
) (bool, error) {
	for i := range owners {
		evidence := applicationEvidence{object: owners[i].Obj, visibleGeneration: true}
		if _, err := a.readApplication(ctx, a.store, reads, &evidence); err != nil {
			return false, err
		}
		material := evidence.source.artifact
		origin := material.ApplicationOrigin(evidence.application.Path)
		unknown := material == nil || material.Provider == store.ProviderGenerator
		overlaps := safepath.Contains(origin, entry) || safepath.Contains(entry, origin)
		if origin == "" && unknown || overlaps {
			return true, nil
		}
	}
	return false, nil
}

func (d *Session) lookupApplicationInputs(
	ctx context.Context, input *store.SourceArtifact, entry string,
) ([]string, error) {
	if !d.hasEntryBuilder() {
		return nil, nil
	}
	_, recipes, err := loader.ApplicationPackages(ctx, inputprojection.Authored(input), entry, "")
	if err != nil || len(recipes) == 0 {
		return nil, err
	}
	recipe := &recipes[0]
	owners := recipe.Owners
	owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, entry, manifest.NamedResource{})
	if recipe.RequiresBuild {
		frame, err := d.preparePackageInventory(ctx, owner, input, nil)
		if frame == nil || ctx.Err() != nil {
			return nil, err
		}
		if !frame.inventoryReady() {
			return nil, nil
		}
		owners = frame.children
	}
	entryOrigin := input.OriginPath(d.cfg.Path)
	admission := d.admission()
	var inputs []string
	var reads store.ReadSet
	reads.Init(d.cfg.Store, owner.Named(), nil)
	defer reads.ReleaseCapture()
	defer reads.Retire()
	for i := range owners {
		child := &owners[i]
		if _, applies := manifest.ApplicationFor(child.Obj); !applies || child.Obj.Suspended() {
			continue
		}
		evidence := applicationEvidence{object: child.Obj, visibleGeneration: true}
		_, err := admission.readApplication(ctx, d.cfg.Store, &reads, &evidence)
		if err != nil {
			return nil, err
		}
		origin := evidence.source.artifact.ApplicationOrigin(evidence.application.Path)
		if origin != "" && !safepath.Contains(origin, entryOrigin) && !safepath.Contains(entryOrigin, origin) {
			inputs = append(inputs, origin)
		}
	}
	return inputs, nil
}

func (d *Session) qualifyLookupOwners(
	ctx context.Context, inventory []store.Child, reached bool, owned map[string]bool,
) error {
	admission := d.admission()
	for i := range inventory {
		child := &inventory[i]
		ks, ok := child.Obj.(*manifest.Kustomization)
		if !ok || ks.Suspended() {
			continue
		}
		evidence := applicationEvidence{
			object: ks, document: child.Doc, visibleGeneration: true, lookup: true, lookupReach: reached,
		}
		origin, err := admission.qualifyLookupOwner(ctx, ks, &evidence, inventory)
		if err != nil {
			return err
		}
		if origin != "" {
			owned[origin] = true
		}
	}
	return nil
}

func (a *Admission) qualifyLookupOwner(
	ctx context.Context,
	ks *manifest.Kustomization,
	evidence *applicationEvidence,
	inventory []store.Child,
) (string, error) {
	d := a.session
	var reads store.ReadSet
	reads.Init(a.store, ks.Named(), nil)
	defer reads.ReleaseCapture()
	defer reads.Retire()
	if index := slices.IndexFunc(inventory, func(member store.Child) bool {
		return member.Obj.Named() == ks.EffectiveSourceID()
	}); index != -1 {
		evidence.generatedSource = inventory[index].Obj
	}
	matches, err := a.readApplication(ctx, a.store, &reads, evidence)
	if err != nil {
		return "", err
	}
	application, material := evidence.application, evidence.source.artifact
	var origin string
	if matches {
		origin = material.ApplicationOrigin(application.Path)
		if origin == "" {
			origin = a.input.OriginPath(a.path)
		}
	}
	policy := ks.EmissionPolicy()
	if !slices.ContainsFunc(d.cfg.Selection.Roots, func(target manifest.NamedResource) bool {
		return selection.LookupPackageMatches(
			target, &policy, application.Source, matches || evidence.lookupReach && material == nil,
		)
	}) {
		return origin, nil
	}
	if !material.Available() {
		if d.loader.EntryScope(ks.Named()) == nil {
			d.loader.SetEntryScope(ks.Named(), &selection.EntryScope{})
		}
		d.awaitLookupSource(a, inventory, application.Source)
		return origin, nil
	}
	if material.Provider == store.ProviderGenerator {
		return origin, d.prepareLookupPackage(ctx, ks, material, evidence.document)
	}
	if _, err := d.resolveEntrySource(ctx, a, application.Source, &entrySourceGroup{
		reference: sourceReference(ks, application.Source),
		object:    evidence.generatedSource,
		projected: evidence.generatedSource != nil,
		entry:     true,
	}); err != nil {
		return "", err
	}
	published, _ := d.cfg.Store.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	evidence.source.artifact = published
	if !published.Available() || !a.applicationReach(evidence) {
		return origin, nil
	}
	return origin, d.prepareLookupPackage(ctx, ks, published, evidence.document)
}

func (d *Session) awaitLookupSource(admission *Admission, inventory []store.Child, source manifest.NamedResource) {
	for i := range inventory {
		child := &inventory[i]
		if slices.ContainsFunc(manifest.ProducerTargets(child.Obj, source), func(target manifest.ProducerTarget) bool {
			return target.NamedResource == source
		}) {
			d.loader.AdmitDeclarations([]store.Declaration{{Obj: child.Obj, Doc: child.Doc}})
			d.lookupSources = append(
				d.lookupSources,
				selection.LookupSource{Source: source, Materializer: child.Obj.Named()},
			)
		}
	}
	productions := d.cfg.Store.Productions(source, d.loader.Index)
	for i := range productions {
		if owner := productions[i].Materializer(source); owner != (manifest.NamedResource{}) && owner != source {
			d.lookupSources = append(d.lookupSources, selection.LookupSource{Source: source, Materializer: owner})
		}
	}
	if provider, _ := admission.sourceProduction(
		source,
		&sourceRequest{},
		d.loader.Index,
	); provider == store.ProviderGenerator {
		d.lookupSources = append(d.lookupSources, selection.LookupSource{Source: source})
	}
}

// LookupSources borrows generated sources that can complete a restricted package lookup.
func (d *Session) LookupSources() []selection.LookupSource {
	if d == nil {
		return nil
	}
	return d.lookupSources
}

func (d *Session) prepareLookupPackage(
	ctx context.Context, owner *manifest.Kustomization, input *store.SourceArtifact, document map[string]any,
) error {
	id := owner.Named()
	entry := d.cfg.Input.OriginPath(d.cfg.Path)
	restricted := input.LocalPath != "" && input.WorkingTree() &&
		!safepath.Contains(entry, input.ApplicationOrigin(owner.Path))
	switch {
	case restricted && !d.loader.EntryScope(id).Current():
		d.loader.SetEntryScope(id, &selection.EntryScope{})
	case !restricted:
		d.loader.SetEntryScope(id, nil)
	}
	frame, err := d.lookupInventory(ctx, owner, input, requiredLookup)
	if err != nil {
		return err
	}
	if !frame.inventoryReady() {
		if restricted && !frame.complete {
			d.loader.AdmitDeclarations([]store.Declaration{{Obj: owner, Doc: document}})
		}
		return nil
	}
	matches := d.cfg.Selection.Matches
	match := matches(id) || slices.ContainsFunc(frame.children, func(child store.Child) bool {
		return matches(child.Obj.Named())
	})
	var scope *selection.EntryScope
	if restricted {
		scope = &selection.EntryScope{Preparation: &frame.support, Source: input, SourceID: owner.EffectiveSourceID()}
		if match {
			scope.Children, frame.lookupErr = d.entryMembership(ctx, frame, entry)
		}
		matchesInside := func(child store.Child) bool {
			childID := child.Obj.Named()
			return matches(childID) && scope.Children[childID]
		}
		match = frame.lookupErr == nil && (matches(id) || slices.ContainsFunc(frame.children, matchesInside))
	}
	if match && ctx.Err() == nil {
		d.loader.AdmitDeclarations([]store.Declaration{{Obj: owner, Doc: document}})
	}
	if scope != nil {
		d.loader.SetEntryScope(id, scope)
	}
	if !match || ctx.Err() != nil {
		return ctx.Err()
	}
	return d.registerProducer(ctx, frame)
}

type lookupMode uint8

const (
	noLookup lookupMode = iota
	exploratoryLookup
	requiredLookup
)

func (d *Session) finishLookup(result *Result) {
	for id, frame := range d.prepared {
		err := cmp.Or(frame.lookupErr, frame.inventoryError())
		if frame.lookup == noLookup || !frame.complete || !frame.support.Valid() || err == nil {
			continue
		}
		if frame.lookup == exploratoryLookup {
			result.LookupEvidence = append(result.LookupEvidence, selection.LookupFailure{
				Package: manifest.DiagnosticFor(id, frame.owner), Path: d.cfg.Path, Cause: err,
			})
			continue
		}
		d.cfg.Store.AddDeclaration(store.Declaration{Obj: frame.owner})
		d.cfg.Store.SetPreflight(id, store.PreflightFailure{Message: err.Error()})
		d.cfg.Store.UpdateFailure(id, err)
		result.LookupFailures = append(result.LookupFailures, id)
	}
	for i := range d.lookupSources {
		owner := d.lookupSources[i].Materializer
		if status, _ := d.cfg.Store.GetStatus(owner); status.Outcome() == store.OutcomeFailed {
			result.LookupFailures = append(result.LookupFailures, owner)
		}
	}
	slices.SortFunc(result.LookupFailures, manifest.NamedResource.Compare)
	result.LookupFailures = slices.Compact(result.LookupFailures)
	slices.SortFunc(result.LookupEvidence, func(a, b selection.LookupFailure) int {
		return a.Package.ID.Compare(b.Package.ID)
	})
}

func (d *Session) lookupInventory(
	ctx context.Context, owner *manifest.Kustomization, input *store.SourceArtifact, mode lookupMode,
) (*declarationPreparation, error) {
	frame, err := d.preparePackageInventory(ctx, owner, input, nil)
	if frame == nil {
		return nil, err
	}
	if canceled := ctx.Err(); canceled != nil {
		if err == nil {
			err = canceled
		}
		return nil, err
	}
	frame.lookup = max(frame.lookup, mode)
	return frame, nil
}

func (d *Session) enqueueExpansions(
	kustomizations []store.Child,
	pending []sourceExpansion,
) []sourceExpansion {
	for i := range kustomizations {
		child := &kustomizations[i]
		ks := child.Obj.(*manifest.Kustomization) //nolint:errcheck // This branch handles an admitted Kustomization.
		art, _ := d.cfg.Store.GetArtifact(ks.EffectiveSourceID(), store.ArtifactContent).(*store.SourceArtifact)
		key := sourceExpansion{ks: ks, source: art}
		pending = append(pending, key)
	}
	return pending
}

func (d *Session) loadExpansions(
	ctx context.Context,
	repoRoot string,
	pending []sourceExpansion,
	total *int,
) ([]sourceExpansion, int, error) {
	added := 0
	next := pending[:0]
	for _, key := range pending {
		ks := key.ks
		if ks.IsFragment() {
			continue
		}
		sourceRoot := loader.SourceView(d.cfg.Store, ks.EffectiveSourceID(), repoRoot).Path
		if sourceRoot == "" {
			next = append(next, key)
			continue
		}
		target := filepath.Join(sourceRoot, manifest.NormalizeClaimBase(ks.Path))
		if !safepath.Contains(sourceRoot, target) {
			continue
		}
		loaded, loadErr := d.loadSourcePath(ctx, target, &key, total)
		if loadErr != nil {
			return next, added, loadErr
		}
		added += loaded
	}
	return next, added, nil
}

func (d *Session) refreshEnclosingLookups(ctx context.Context, root string) (bool, error) {
	changed := slices.ContainsFunc(d.lookupSources, func(source selection.LookupSource) bool {
		material, _ := d.cfg.Store.GetArtifact(source.Source, store.ArtifactContent).(*store.SourceArtifact)
		return material.Available()
	})
	if changed {
		relative, _ := filepath.Rel(root, d.cfg.Path)
		if err := d.prepareEnclosingPackages(ctx, d.cfg.Input, filepath.ToSlash(relative)); err != nil {
			return false, err
		}
	}
	for _, id := range slices.SortedFunc(maps.Keys(d.prepared), manifest.NamedResource.Compare) {
		frame := d.prepared[id]
		scope := d.loader.EntryScope(id)
		if frame.lookup == noLookup || scope == nil || scope.Current() || frame.support.Valid() {
			continue
		}
		owner, _ := d.cfg.Store.GetDeclaration(id).(*manifest.Kustomization)
		prepared, err := d.preparePendingProducer(ctx, cmp.Or(owner, frame.owner), nil)
		if err != nil {
			return false, err
		}
		changed = changed || prepared
	}
	return changed, nil
}
