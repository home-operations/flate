package discovery

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var errPendingSources = errors.New("discovery: local sources require reconciliation")

// PendingSources borrows the admitted identities that block material-dependent construction.
// The caller MUST reconcile them before resuming Load, Plan, Advance, or Freeze.
func (d *Session) PendingSources() []manifest.NamedResource {
	d.pendingSources = d.pendingSources[:0]
	for id, resolution := range d.resolvedSources.snapshot() {
		if resolution.artifact != nil {
			_ = d.requireSource(id)
		}
	}
	slices.SortFunc(d.pendingSources, manifest.NamedResource.Compare)
	return d.pendingSources
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
		return artifact
	}
	if facts, ok := d.cfg.Store.GetObject(id).(interface {
		Composition() manifest.ArtifactComposition
	}); ok && len(facts.Composition().Includes) != 0 {
		return artifact
	}
	status, _ := d.cfg.Store.GetStatus(id)
	if status.Status == store.StatusReady &&
		(artifact.SameMaterial(selected) || artifact.Reads == nil && id != manifest.BootstrapSourceID) {
		return artifact
	}
	return nil
}
