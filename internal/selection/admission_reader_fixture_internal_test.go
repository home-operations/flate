package selection

import (
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type admissionArtifactReads struct {
	admissionReader

	calls       int
	lists       int
	wideLists   int
	productions []manifest.NamedResource
	bySource    map[manifest.NamedResource]int
}

//nolint:ireturn // The reader contract returns heterogeneous artifact implementations.
func (r *admissionArtifactReads) GetArtifact(id manifest.NamedResource, read store.ArtifactRead) store.Artifact {
	r.calls++
	if r.bySource != nil {
		r.bySource[id]++
	}
	return r.admissionReader.GetArtifact(id, read)
}

func (r *admissionArtifactReads) ListObjects(group, kind string) []manifest.BaseManifest {
	r.lists++
	if group == "" && kind == "" {
		r.wideLists++
	}
	return r.admissionReader.ListObjects(group, kind)
}

func (r *admissionArtifactReads) ListProductionCandidates(
	group, kind string,
	index *manifest.DiscoveryIndex,
	prospective map[manifest.NamedResource]bool,
) []manifest.BaseManifest {
	r.lists++
	return r.admissionReader.ListProductionCandidates(group, kind, index, prospective)
}

func (r *admissionArtifactReads) Productions(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) []manifest.Production {
	r.productions = append(r.productions, id)
	return r.admissionReader.Productions(id, index)
}
