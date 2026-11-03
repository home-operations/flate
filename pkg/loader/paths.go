package loader

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/internal/inputprojection"
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
	var projections map[manifest.NamedResource]inputprojection.Projection
	var provider *values.StoreProvider
	var hydrated map[manifest.NamedResource]bool
	var catalogs map[manifest.NamedResource]*manifest.Kustomization
	hydrate := func(owner *manifest.Kustomization) bool {
		if owner.PostBuild != nil {
			for _, ref := range owner.PostBuild.SubstituteFrom {
				inputID := manifest.NamedResource{Kind: ref.Kind, Namespace: owner.Namespace, Name: ref.Name}
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
					return false
				}
				if err != nil {
					continue
				}
				data, known := child.Obj.(manifest.Data)
				if !known {
					continue
				}
				for _, production := range inputRecord.Productions {
					inputRecord.ReferenceData = append(
						inputRecord.ReferenceData,
						manifest.ReferenceData{Production: production, OfflineData: data},
					)
				}
				l.Index.Records[inputID] = inputRecord
			}
		}
		return ctx.Err() == nil
	}
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
		if child == nil {
			continue
		}
		if child.PostBuild != nil && len(child.PostBuild.SubstituteFrom) != 0 {
			if !hydrate(child) {
				return ctx.Err()
			}
			if provider == nil {
				provider = values.NewStoreProvider(l, l.Options.WipeSecrets)
			}
			known := true
			bindings := map[manifest.NamedResource]manifest.Data{}
			for _, ref := range child.PostBuild.SubstituteFrom {
				inputID := manifest.NamedResource{Kind: ref.Kind, Name: ref.Name, Namespace: child.Namespace}
				input := provider.Lookup(inputID)
				known = known && input.Data != nil && len(input.Materializers) == 0 && l.ReferenceFacts(inputID, nil).Supplied == nil
				if input.Data != nil {
					bindings[inputID] = input.Data
				}
			}
			prepared := child.Clone()
			if err := values.ExpandPostBuildSubstituteReference(prepared, provider); err != nil {
				known = false
			}
			roots := record.ContentRoots
			if occurrence, ok := l.occurrence(id); roots == nil && ok {
				source, _, _ := occurrence.SourcePath()
				if source.Kind == "" || source.Name == "" {
					source = manifest.BootstrapSourceID
				}
				roots = map[manifest.NamedResource][]string{source: occurrence.facts.roots}
			}
			var documents []manifest.DocOccurrence
			declared, err := child.RenderDocument()
			if err != nil || child.NamePrefix != "" || child.NameSuffix != "" || len(child.Images) != 0 {
				known = false
			}
			declared, decoded := projectionPatches(declared)
			known = known && decoded
			documents = append(documents, manifest.DocOccurrence{Document: map[string]any{"input": declared}})
			for source, files := range roots {
				known = known && len(files) != 0
				input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
				if input == nil || input.FS == nil || !input.Reads.Valid() {
					known = false
					break
				}
				for _, file := range files {
					if err := ctx.Err(); err != nil {
						return err
					}
					location := filepath.Join(input.LocalPath, filepath.FromSlash(file))
					packageDoc := l.documents.cache[filepath.Dir(location)]
					body := packageDoc.body
					if packageDoc.file == location && body != nil {
						hasVars := len(body.Vars) != 0 //nolint:staticcheck // Kustomize vars can synthesize expansion syntax.
						unproved := len(body.Replacements) != 0 || hasVars || body.NamePrefix != "" || body.NameSuffix != ""
						if unproved {
							known = false
						}
						if len(body.Generators) != 0 || len(body.Transformers) != 0 || len(body.Images) != 0 {
							known = false
						}
						for _, generator := range body.ConfigMapGenerator {
							known = known && len(generator.FileSources) == 0 && len(generator.EnvSources) == 0
						}
						for _, generator := range body.SecretGenerator {
							known = known && len(generator.FileSources) == 0 && len(generator.EnvSources) == 0
						}
					}
					raw, err := fs.ReadFile(input.FS, file)
					if err != nil {
						known = false
						break
					}
					docs, decoded := projectionDocuments(raw)
					known = known && decoded
					documents = append(documents, docs...)
				}
			}
			known = known && len(roots) != 0
			for _, value := range prepared.PostBuild.Substitute {
				known = known && !manifest.HasValuePlaceholder(value)
			}
			active := len(prepared.PostBuild.Substitute) != 0 || prepared.SubstituteStrategy() == kustomizev1.SubstituteStrategyAlways
			names := values.SubstitutionNames(documents, active)
			if known && names != nil {
				if projections == nil {
					projections = map[manifest.NamedResource]inputprojection.Projection{}
				}
				projections[id] = inputprojection.Projection{Content: prepared.Content(nil, names, active), Inputs: bindings}
			}
		}
		l.Index.Records[id] = record
		if !manifest.HasEnvsubstReference(child.Path) {
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
		if !hydrate(parent) {
			return ctx.Err()
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
	l.InputContent = projections
	return nil
}

func projectionDocuments(raw []byte) ([]manifest.DocOccurrence, bool) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var documents []manifest.DocOccurrence
	for {
		var input any
		err := decoder.Decode(&input)
		if errors.Is(err, io.EOF) {
			return documents, true
		}
		if err != nil {
			return nil, false
		}
		switch value := input.(type) {
		case map[string]any:
			var known bool
			input, known = projectionPatches(value)
			if !known {
				return nil, false
			}
		case []any:
		case nil:
			continue
		default:
			return nil, false
		}
		documents = append(documents, manifest.DocOccurrence{Document: map[string]any{"input": input}})
	}
}

func projectionPatches(doc map[string]any) (map[string]any, bool) {
	doc = maps.Clone(doc)
	if spec, ok := doc["spec"].(map[string]any); ok {
		var known bool
		doc["spec"], known = projectionPatches(spec)
		if !known {
			return nil, false
		}
	}
	for _, field := range []string{"patches", "patchesJson6902", "patchesStrategicMerge"} {
		patches, _ := doc[field].([]any)
		if len(patches) == 0 {
			continue
		}
		patches = slices.Clone(patches)
		for i, patch := range patches {
			entry, _ := patch.(map[string]any)
			raw, _ := entry["patch"].(string)
			if field == "patchesStrategicMerge" {
				raw, _ = patch.(string)
			}
			if raw == "" {
				continue
			}
			documents, known := projectionDocuments([]byte(raw))
			if !known {
				return nil, false
			}
			inputs := make([]any, len(documents))
			for j, document := range documents {
				inputs[j] = document.Document["input"]
			}
			if entry != nil {
				entry = maps.Clone(entry)
				entry["patch"] = inputs
				patches[i] = entry
			} else {
				patches[i] = inputs
			}
		}
		doc[field] = patches
	}
	return doc, true
}
