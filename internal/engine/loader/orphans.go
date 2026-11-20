package loader

import (
	"cmp"
	"context"
	"log/slog"
	"path/filepath"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// PromoteOrphans MUST run after index finalization.
func (l *Loader) PromoteOrphans(ctx context.Context) error {
	query := rootQuery{loader: l}
	//nolint:gocritic // The iterator supplies independent immutable record snapshots.
	for id, record := range l.Index.Files() {
		var reads store.ReadSet
		reads.Init(l.Store, manifest.NamedResource{}, nil)
		query.objects = &reads
		promoted, err := query.promote(ctx, id, &record, &reads)
		reads.Retire()
		if err != nil {
			return err
		}
		if !promoted {
			slog.Debug("loader: orphan promotion failed", "id", id.String())
		}
	}
	return nil
}

func (q *rootQuery) promote(
	ctx context.Context,
	id manifest.NamedResource,
	record *manifest.DiscoveryRecord,
	reads *store.ReadSet,
) (bool, error) {
	l := q.loader
	if object, policy := reads.Lookup(id); object != nil && policy.Root {
		return true, nil
	}
	if record.Declaration != nil && !q.admitsRoot(record.Declaration, record.Path) {
		return false, nil
	}
	if owned, err := q.ownedOrphan(ctx, record, reads); err != nil || owned {
		return false, err
	}
	children, err := Promote(ctx, l.Store, id, PromoteOptions{
		Record: manifest.Production{
			Authored: record.Authored,
			Path:     record.Path,
			File:     record.File,
			Source:   manifest.BootstrapSourceID,
		},
		Reads:     reads,
		Documents: l.PromotionDocuments(id),
	})
	if err != nil {
		return false, err
	}
	for i := range children {
		if ctx.Err() != nil || !reads.Valid() {
			break
		}
		if err := q.promoteChild(ctx, &children[i], record, reads); err != nil {
			return false, err
		}
	}
	return reads.Valid() && l.Store.GetObject(id) != nil, nil
}

func (q *rootQuery) promoteChild(
	ctx context.Context,
	child *store.Child,
	record *manifest.DiscoveryRecord,
	reads *store.ReadSet,
) error {
	l := q.loader
	id := child.Obj.Named()
	sibling, known := l.Index.Lookup(id)
	if !known || cmp.Or(sibling.Builder, sibling.File) != cmp.Or(record.Builder, record.File) ||
		!q.admitsRoot(child.Obj, record.Path) {
		return nil
	}
	for i := range child.Warnings {
		warning := child.Warnings[i]
		warning.Resource = id
		l.Store.AddWarning(warning)
	}
	if owned, err := q.ownedOrphan(ctx, &sibling, reads); err != nil || owned {
		return err
	}
	source := child.SourceSupport
	if sibling.Builder == "" && !selection.Root(l.roots.get(id).view()).Independent {
		child.SourceSupport = manifest.NamedResource{}
	}
	reads.Admit(*child, source, manifest.NamedResource{})
	return nil
}

func (q *rootQuery) ownedOrphan(
	ctx context.Context,
	record *manifest.DiscoveryRecord,
	reads *store.ReadSet,
) (bool, error) {
	if record.Parent == (manifest.NamedResource{}) || record.File == "" {
		return false, nil
	}
	parent := reads.GetDeclaration(record.Parent)
	application, present := manifest.ApplicationFor(parent)
	if !present || parent.Suspended() {
		return false, nil
	}
	source, _ := reads.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	if source == nil || source.FS == nil {
		return false, nil
	}
	l := q.loader
	location := SourceView(reads, application.Source, l.SourceRoot)
	if location.Pinned || location.External {
		return false, nil
	}
	file := filepath.Join(l.SourceRoot, record.File)
	visits := l.packageResources(source, application.Path, nil)
	for i := range visits {
		visit := &visits[i]
		if !visit.throughComponent && visit.reaches(source, file) {
			return true, ctx.Err()
		}
	}
	return false, ctx.Err()
}
