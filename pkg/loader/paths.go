package loader

import (
	"context"
	"slices"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// ReferenceFacts resolves data against the current discovery index before publication.
func (l *Loader) ReferenceFacts(id manifest.NamedResource, index *manifest.DiscoveryIndex) store.ReferenceFacts {
	if index == nil {
		index = l.Index
	}
	return l.Store.ReferenceFacts(id, index)
}

func (l *Loader) resolvePaths(ctx context.Context) error {
	var provider *values.StoreProvider
	var hydrated map[manifest.NamedResource]bool
	var catalogs map[manifest.NamedResource]*manifest.Kustomization
	for id, record := range l.Index.Records {
		if !id.KindInfo().Structural {
			continue
		}
		child, _ := record.Declaration.(*manifest.Kustomization)
		if child == nil {
			if occurrence, known := l.occurrence(id); known {
				child = occurrence.facts.structural
			} else {
				child, _ = l.IndexedObject(id, record).Obj.(*manifest.Kustomization)
			}
		}
		if child == nil || !manifest.HasEnvsubstReference(child.Path) {
			continue
		}
		if child.Labels[manifest.SubstituteAnnotationKey] == kustomizev1.DisabledValue || child.Annotations[manifest.SubstituteAnnotationKey] == kustomizev1.DisabledValue {
			continue
		}
		owner := record.Parent
		if owner == (manifest.NamedResource{}) || owner == id {
			continue
		}
		parent := catalogs[owner]
		if parent == nil {
			object, ok := l.CatalogObject(owner).(*manifest.Kustomization)
			if !ok {
				continue
			}
			parent = manifest.CatalogObject(object, owner).(*manifest.Kustomization)
			if catalogs == nil {
				catalogs = map[manifest.NamedResource]*manifest.Kustomization{}
			}
			catalogs[owner] = parent
		}
		if parent.PostBuild != nil {
			for _, ref := range parent.PostBuild.SubstituteFrom {
				inputID := manifest.NamedResource{Kind: ref.Kind, Namespace: parent.Namespace, Name: ref.Name}
				if hydrated[inputID] {
					continue
				}
				if hydrated == nil {
					hydrated = map[manifest.NamedResource]bool{}
				}
				hydrated[inputID] = true
				inputRecord, indexed := l.Index.Lookup(inputID)
				occurrence, located := l.occurrence(inputID)
				if !indexed || !located || inputRecord.Path == "" {
					continue
				}
				child, err := l.exactObject(ctx, occurrence, nil)
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if err != nil {
					continue
				}
				data, known := child.Obj.(manifest.Data)
				if !known {
					continue
				}
				for _, production := range inputRecord.Productions {
					inputRecord.ReferenceData = append(inputRecord.ReferenceData, manifest.ReferenceData{Production: production, OfflineData: data})
				}
				l.Index.Records[inputID] = inputRecord
			}
		}
		if provider == nil {
			provider = values.NewStoreProvider(l, l.Options.WipeSecrets)
		}
		path, known := values.ResolveStaticPath(child.Path, parent, provider)
		source := child.SourceID()
		if source.Name == "" || source.Kind == "" {
			source = manifest.BootstrapSourceID
		}
		if known {
			if path == child.Path {
				continue
			}
			projected := *child
			projected.Path = path
			record.ContentRoots = map[manifest.NamedResource][]string{source: slices.Collect(manifest.RenderRoots(&projected))}
		} else {
			source = parent.SourceID()
			if source.Name == "" || source.Kind == "" {
				source = manifest.BootstrapSourceID
			}
			record.ContentRoots = map[manifest.NamedResource][]string{source: {""}}
			producer := l.Index.Records[owner]
			producer.StaticOutput = false
			producer.ContentRoots = map[manifest.NamedResource][]string{source: {""}}
			l.Index.Records[owner] = producer
		}
		l.Index.Records[id] = record
	}
	return nil
}
