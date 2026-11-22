package discovery

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

var errPendingSources = errors.New("discovery: local sources require reconciliation")

// PendingSources borrows the admitted identities that block material-dependent construction.
// The caller MUST reconcile them before resuming Load, Plan, Advance, or Freeze.
func (d *Session) PendingSources() []manifest.NamedResource {
	d.pendingSources = d.pendingSources[:0]
	sources := d.resolvedSources.snapshot()
	for id := range sources {
		if sources[id].artifact != nil {
			_ = d.requireSource(id)
		}
	}
	slices.SortFunc(d.pendingSources, manifest.NamedResource.Compare)
	return d.pendingSources
}

// PendingMaterializers borrows runnable demands from the most recent Advance.
func (d *Session) PendingMaterializers() []manifest.NamedResource {
	return d.pendingMaterializers
}

func (d *Session) requireSource(id manifest.NamedResource) error {
	if d.sourceMaterial(id, d.resolvedSources.snapshot()[id].artifact) != nil {
		return nil
	}
	d.pendingSources = append(d.pendingSources, id)
	return errPendingSources
}

func (d *Session) sourceMaterial(id manifest.NamedResource, selected *store.SourceArtifact) *store.SourceArtifact {
	artifact, _ := d.cfg.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	if artifact == nil || artifact.FS == nil || !artifact.Reads.Valid() {
		return nil
	}
	if selected == nil {
		if candidate, known := d.resolvedSources.snapshot()[id]; known && artifact.Provider != candidate.provider {
			return nil
		}
		return artifact
	}
	if object := d.cfg.Store.GetObject(id); object != nil && len(object.Composition().Includes) != 0 &&
		artifact.ComposedFrom.SameMaterial(
			selected,
			store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin,
		) {
		return artifact
	}
	status, _ := d.cfg.Store.GetStatus(id)
	if status.Status == store.StatusReady &&
		artifact.SameMaterial(selected, store.ArtifactContent|store.ArtifactRevision|store.ArtifactOrigin) {
		return artifact
	}
	return nil
}
