package loader

import (
	"cmp"
	"context"
	"iter"
	"slices"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/pkg/manifest"
)

// References MUST consume immutable declaration evidence synchronously.
func (l *Loader) References(id manifest.NamedResource) iter.Seq[manifest.Reference] {
	return func(yield func(manifest.Reference) bool) {
		refs, defaultNamespace, object := l.referenceInputs(id)
		if object != nil {
			manifest.References(object, manifest.ReferenceRelations)(yield)
			return
		}
		for i := range refs {
			ref := refs[i]
			if ref.NamespaceDefault {
				ref.Namespace = defaultNamespace
			}
			if !yield(ref) {
				return
			}
		}
	}
}

func (l *Loader) referenceInputs(id manifest.NamedResource) ([]manifest.Reference, string, manifest.BaseManifest) {
	record, _ := l.Index.Lookup(id)
	object := l.Store.GetDeclaration(id)
	if record.Builder != "" && record.Declaration != nil {
		object = record.Declaration
	}
	if occurrence, ok := l.catalogOccurrence(id); ok && occurrence.facts != nil {
		current := object == nil || object == occurrence.object || object == occurrence.facts.catalog ||
			object == l.candidates[id].Obj
		authored := record.Builder == "" && !l.documents.eager
		projected := current && object != nil && object == occurrence.object && object == record.Declaration
		if authored || projected {
			refs := occurrence.facts.relations()
			if !current && occurrence.facts.derivedInputs != 0 {
				refs = refs[:len(refs)-occurrence.facts.derivedInputs]
			}
			return refs, occurrence.Identity.Namespace, nil
		}
	}
	if object == nil {
		object = l.IndexedObject(id, record).Obj
	}
	return nil, "", object
}

func (rf *readerFinalizer) collectPendingPackageReads(
	ctx context.Context, occurrence *authoredOccurrence, record *manifest.DiscoveryRecord,
) error {
	id := occurrence.Identity
	var visited []manifest.NamedResource
	for owner := range selection.PendingPackageProductions(id, record) {
		if slices.Contains(visited, owner) {
			continue
		}
		visited = append(visited, owner)
		clear(rf.visited)
		clear(rf.templateWalks)
		if err := rf.projectOccurrence(ctx, occurrence, owner); err != nil {
			return err
		}
		namespace := id.Namespace
		if object := rf.object(owner); object != nil {
			namespace = cmp.Or(object.EmissionPolicy().Namespace, namespace)
		}
		for ref := range rf.loader.References(id) {
			if ref.NamespaceDefault && ref.NamedResource.KindInfo().Namespaced() {
				if namespace == "" {
					continue
				}
				ref.Namespace = namespace
			}
			if err := rf.observe(ctx, &ref, owner, false); err != nil {
				return err
			}
		}
	}
	return nil
}
