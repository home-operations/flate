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
func (l *Loader) ReferenceFacts(id manifest.NamedResource,
	index *manifest.DiscoveryIndex,

	consumer manifest.NamedResource,
) store.ReferenceFacts {
	if index == nil {
		index = l.Index
	}
	return l.Store.ReferenceFacts(id, index, consumer)
}

func (l *Loader) resolvePaths(ctx context.Context) error {
	p := substitutionResolution{loader: l}

	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for id, record := range l.Index.Records {
		if err := p.resolveRecord(ctx, id, new(record)); err != nil {
			return err
		}
	}
	l.InputContent = p.projections
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
			if !projectionPatch(patches, i, patch, field) {
				return nil, false
			}
		}
		doc[field] = patches
	}
	return doc, true
}

func (l *Loader) hydratePathInput(ctx context.Context, inputID manifest.NamedResource) bool {
	inputRecord, indexed := l.Index.Lookup(inputID)
	occurrence, located := l.occurrence(inputID)
	if !indexed || !located || inputRecord.Path == "" {
		return true
	}
	file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
	cached := l.documents.cache[file]
	data := cached.pathInputs[inputID]
	if data == nil {
		child, err := l.OccurrenceObject(ctx, new(occurrenceView(&occurrence)), nil)
		if ctx.Err() != nil {
			return false
		}
		if err != nil {
			return true
		}
		var known bool
		data, known = child.Obj.(manifest.Data)
		if !known {
			return true
		}
		if cached.pathInputs == nil {
			cached.pathInputs = map[manifest.NamedResource]manifest.Data{}
		}
		cached.pathInputs[inputID] = data
		l.documents.cache[file] = cached
	}
	{
		productionValues := inputRecord.Productions
		for productionIndex := range productionValues {
			production := productionValues[productionIndex]
			inputRecord.ReferenceData = append(
				inputRecord.ReferenceData,
				manifest.ReferenceData{Production: production, OfflineData: data},
			)
		}
	}
	l.Index.Records[inputID] = inputRecord
	return true
}

func projectionPatch(patches []any, i int, patch any, field string) bool {
	entry, _ := patch.(map[string]any)
	raw, _ := entry["patch"].(string)
	if field == "patchesStrategicMerge" {
		raw, _ = patch.(string)
	}
	if raw == "" {
		return true
	}
	documents, known := projectionDocuments([]byte(raw))
	if !known {
		return false
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
	return true
}

func (l *Loader) pathDeclaration(
	id manifest.NamedResource,
	recordValue *manifest.DiscoveryRecord,
) *manifest.Kustomization {
	record := *recordValue
	child, _ := record.Declaration.(*manifest.Kustomization)
	if child == nil {
		if occurrence, known := l.occurrence(id); known {
			child, _ = occurrence.facts.catalog.(*manifest.Kustomization)
		} else {
			child, _ = l.IndexedObject(id, record).Obj.(*manifest.Kustomization)
		}
	}
	return child
}

func (l *Loader) packageProjectionKnown(location string, known bool) bool {
	packageDoc := l.documents.cache[filepath.Dir(location)]
	body := packageDoc.body
	if packageDoc.file == location && body != nil {
		hasVars := len(
			//nolint:staticcheck // Kustomize still accepts deprecated variable inputs.
			body.Vars,
		) != 0
		unproved := len(body.Replacements) != 0 || hasVars || body.NamePrefix != "" ||
			body.NameSuffix != ""
		if unproved {
			known = false
		}
		if len(body.Generators) != 0 || len(body.Transformers) != 0 || len(body.Images) != 0 {
			known = false
		}
		{
			generatorValues := body.ConfigMapGenerator
			for generatorIndex := range generatorValues {
				generator := generatorValues[generatorIndex]
				known = known && len(generator.FileSources) == 0 && len(generator.EnvSources) == 0
			}
		}
		{
			generatorValues2 := body.SecretGenerator
			for generatorIndex2 := range generatorValues2 {
				generator := generatorValues2[generatorIndex2]
				known = known && len(generator.FileSources) == 0 && len(generator.EnvSources) == 0
			}
		}
	}
	return known
}

type projectionInputs struct {
	payload    *inputPayload
	Names      map[string]struct{}
	Known      bool
	NamesKnown bool
}

type projectionRead struct {
	inputs      *projectionInputs
	known, stop bool
}

func (l *Loader) projectionFile(
	ctx context.Context,
	input *store.SourceArtifact,
	file string,
	active bool,
) (projectionRead, error) {
	if err := ctx.Err(); err != nil {
		return projectionRead{}, err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	location := filepath.Join(input.LocalPath, filepath.FromSlash(file))
	known := l.packageProjectionKnown(location, true)
	mounted := l.documents.artifacts[input.LocalPath]
	shared := mounted != nil && parsedInputIdentity(mounted) == parsedInputIdentity(input) &&
		mounted.Input(
			store.ArtifactContent|store.ArtifactRevision,
		) == input.Input(
			store.ArtifactContent|store.ArtifactRevision,
		)
	var retained *projectionInputs
	if shared {
		l.documents.restore(location)
		retained = l.documents.cache[location].projection
		if retained != nil && (!active || retained.NamesKnown) {
			return projectionRead{inputs: retained, known: known && retained.Known}, nil
		}
	}
	projection, err := retained.readProjection(ctx, input, file, active, shared && l.documents.recordInputs)
	if err != nil || projection.stop {
		return projection, err
	}
	inputs := projection.inputs

	if shared {
		doc := l.documents.cache[location]
		doc.projection = inputs
		l.documents.cache[location] = doc
	}
	return projectionRead{inputs: inputs, known: known && inputs.Known}, nil
}

type projectionNames struct {
	names         map[string]struct{}
	active, known bool
}

func (l *Loader) projectionNames(
	ctx context.Context,
	child, prepared *manifest.Kustomization,
	roots map[manifest.NamedResource][]string,
) (projectionNames, error) {
	active := values.SubstitutionActive(prepared)
	declared, err := child.RenderDocument()
	known := err == nil && child.NamePrefix == "" && child.NameSuffix == "" && len(child.Images) == 0
	declared, decoded := projectionPatches(declared)
	known = known && decoded
	names := projectionSubstitutionNames(
		[]manifest.DocOccurrence{{Document: map[string]any{"input": declared}}},
		active,
	)
	for source, files := range roots {
		known = known && len(files) != 0
		input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		if input == nil || input.FS == nil || !input.Reads.Valid() {
			known = false
			break
		}
		for _, file := range files {
			projection, err := l.projectionFile(ctx, input, file, active)
			if err != nil {
				return projectionNames{}, err
			}
			known = known && projection.known
			if projection.stop {
				break
			}
			names = mergeProjectionNames(names, projection.inputs, active)
		}
	}
	known = known && len(roots) != 0
	return projectionNames{names: names, active: active, known: known}, nil
}

//nolint:gocritic // Lazy map initialization MUST remain visible to the caller across path resolutions.
func (l *Loader) hydratePathReferences(
	ctx context.Context,
	owner *manifest.Kustomization,
	hydrated *map[manifest.NamedResource]bool,
) bool {
	if owner.PostBuild != nil {
		for _, ref := range owner.PostBuild.SubstituteFrom {
			inputID := manifest.NamedResource{Kind: ref.Kind, Namespace: owner.Namespace, Name: ref.Name}
			if (*hydrated)[inputID] {
				continue
			}
			if *hydrated == nil {
				*hydrated = map[manifest.NamedResource]bool{}
			}
			(*hydrated)[inputID] = true
			if !l.hydratePathInput(ctx, inputID) {
				return false
			}
		}
	}
	return ctx.Err() == nil
}

type pathBindings struct {
	known  bool
	inputs map[manifest.NamedResource]manifest.Data
}

func (l *Loader) pathBindings(
	child *manifest.Kustomization,
	provider *values.StoreProvider,
) pathBindings {
	known := true
	bindings := map[manifest.NamedResource]manifest.Data{}
	for _, ref := range child.PostBuild.SubstituteFrom {
		inputID := manifest.NamedResource{Kind: ref.Kind, Name: ref.Name, Namespace: child.Namespace}
		input := provider.Lookup(inputID)
		known = known && input.Data != nil && len(input.Materializers) == 0 &&
			l.Store.Policy(inputID).Root
		if input.Data != nil {
			bindings[inputID] = input.Data
		}
	}
	return pathBindings{known: known, inputs: bindings}
}

//nolint:gocritic // Lazy map initialization MUST remain visible to the caller across catalog lookups.
func (l *Loader) pathCatalog(
	ctx context.Context,
	owner manifest.NamedResource,
	catalogs *map[manifest.NamedResource]*manifest.Kustomization,
) (*manifest.Kustomization, bool) {
	parent := (*catalogs)[owner]
	if parent != nil {
		return parent, true
	}
	object := l.CatalogObject(owner)
	if occurrence, found := l.occurrence(owner); found {
		file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
		if l.documents.cache[file].coverage == manifest.ApplicationDocuments {
			child, err := l.OccurrenceObject(ctx, new(occurrenceView(&occurrence)), nil)
			if err != nil {
				return nil, false
			}
			object = child.Obj
		}
	}
	declaration, ok := object.(*manifest.Kustomization)
	if !ok {
		return nil, false
	}
	//nolint:errcheck // Catalog projection retains the checked Kustomization type.
	parent = manifest.CatalogObject(declaration, owner).(*manifest.Kustomization)
	if *catalogs == nil {
		*catalogs = map[manifest.NamedResource]*manifest.Kustomization{}
	}
	(*catalogs)[owner] = parent
	return parent, true
}

type scalarContext struct {
	owner manifest.NamedResource
	value string
}

type substitutionResolution struct {
	applicationInputs map[manifest.NamedResource]bool
	full, needsInputs bool
	visited           map[visitContext]bool
	resolved          map[scalarContext]string
	contextual        map[occurrenceContext]authoredOccurrence
	contextualChanged bool
	loader            *Loader
	projections       map[manifest.NamedResource]inputprojection.Projection
	provider          *values.StoreProvider
	hydrated          map[manifest.NamedResource]bool
	catalogs          map[manifest.NamedResource]*manifest.Kustomization
	resolving         map[manifest.NamedResource]bool
}

func (p *substitutionResolution) resolveIdentity(
	ctx context.Context,
	owner manifest.NamedResource,
	id *manifest.NamedResource,
) bool {
	resolvedID := *id
	for _, field := range []*string{&resolvedID.Name, &resolvedID.Namespace} {
		if !manifest.HasEnvsubstReference(*field) {
			continue
		}
		resolved, known := p.resolveValue(ctx, owner, *field)
		if !known || manifest.HasEnvsubstReference(resolved) {
			return false
		}
		*field = resolved
	}
	if resolvedID.Name == "" {
		return false
	}
	*id = resolvedID
	return true
}

func (p *substitutionResolution) resolveValue(
	ctx context.Context,
	owner manifest.NamedResource,
	value string,
) (string, bool) {
	if p.resolving[owner] {
		return value, false
	}
	if ctx.Err() != nil {
		return value, false
	}
	key := scalarContext{owner, value}
	if resolved, found := p.resolved[key]; found {
		return resolved, true
	}
	l := p.loader
	parent, known := l.pathCatalog(ctx, owner, &p.catalogs)
	if !known {
		return value, false
	}
	if p.resolving == nil {
		p.resolving = map[manifest.NamedResource]bool{}
	}
	p.resolving[owner] = true
	defer delete(p.resolving, owner)
	parent, known = p.inheritValues(ctx, owner, parent, value)
	if !known || !l.hydratePathReferences(ctx, parent, &p.hydrated) {
		return value, false
	}
	if p.provider == nil {
		p.provider = values.NewStoreProvider(l)
	}
	resolved, known := values.ResolveStaticValue(value, parent, p.provider, l.Options.Missing)
	if known {
		if p.resolved == nil {
			p.resolved = map[scalarContext]string{}
		}
		p.resolved[key] = resolved
	}
	if !known && parent.PostBuild != nil {
		for _, ref := range parent.PostBuild.SubstituteFrom {
			id := manifest.NamedResource{Kind: ref.Kind, Namespace: parent.Namespace, Name: ref.Name}
			result, err := values.Classify(new(manifest.Reference{NamedResource: id}), new(p.provider.Lookup(id)))
			p.needsInputs = p.needsInputs || err == nil && result.State() == values.Absent
		}
	}
	return resolved, known
}

func (p *substitutionResolution) inheritValues(
	ctx context.Context,
	owner manifest.NamedResource,
	parent *manifest.Kustomization,
	value string,
) (*manifest.Kustomization, bool) {
	record, _ := p.loader.Index.Lookup(owner)
	if parent.PostBuild == nil || record.Parent == (manifest.NamedResource{}) || record.Parent == owner ||
		parent.Labels[manifest.SubstituteAnnotationKey] == kustomizev1.DisabledValue ||
		parent.Annotations[manifest.SubstituteAnnotationKey] == kustomizev1.DisabledValue {
		return parent, true
	}
	var projected *manifest.Kustomization
	resolve := func(value string) (string, bool) {
		resolved, complete := p.resolveValue(ctx, record.Parent, value)
		if complete && projected == nil {
			projected = new(*parent)
			projected.PostBuild = parent.PostBuild.DeepCopy()
		}
		return resolved, complete
	}
	for i, ref := range parent.PostBuild.SubstituteFrom {
		if !manifest.HasEnvsubstReference(ref.Name) {
			continue
		}
		resolved, complete := resolve(ref.Name)
		if !complete {
			return parent, false
		}
		projected.PostBuild.SubstituteFrom[i].Name = resolved
	}
	var names map[string]struct{}
	for key, inline := range parent.PostBuild.Substitute {
		if !manifest.HasEnvsubstReference(inline) {
			continue
		}
		if names == nil {
			names = values.SubstitutionNames([]byte(value))
		}
		if _, used := names[key]; !used {
			continue
		}
		resolved, complete := resolve(inline)
		if !complete {
			return parent, false
		}
		projected.PostBuild.Substitute[key] = resolved
	}
	if projected != nil {
		p.catalogs[owner] = projected
		return projected, true
	}
	return parent, true
}

func (p *substitutionResolution) projectSubstitutions(
	ctx context.Context,
	id manifest.NamedResource,
	child *manifest.Kustomization,
	recordValue *manifest.DiscoveryRecord,
) error {
	l := p.loader

	record := *recordValue
	if !l.hydratePathReferences(ctx, child, &p.hydrated) {
		return ctx.Err() //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	if p.provider == nil {
		p.provider = values.NewStoreProvider(l)
	}
	bindings := l.pathBindings(child, p.provider)
	if !bindings.known {
		return nil
	}
	prepared := new(*child)
	prepared.PostBuild = new(*child.PostBuild)
	if err := values.ExpandPostBuildSubstituteReference(prepared, p.provider, l.Options.Missing); err != nil {
		return nil //nolint:nilerr // Reconciliation MUST report substitution errors; incomplete bindings cannot certify content.
	}
	roots := record.ContentRoots
	if occurrence, ok := l.catalogOccurrence(id); roots == nil && ok {
		source, _, _ := occurrence.SourcePath()
		if source.Kind == "" || source.Name == "" {
			source = manifest.BootstrapSourceID
		}
		roots = map[manifest.NamedResource][]string{source: occurrence.facts.roots}
	}
	projection, err := l.projectionNames(ctx, child, prepared, roots)
	if err != nil {
		return err
	}
	if projection.known && projection.names != nil {
		if p.projections == nil {
			p.projections = map[manifest.NamedResource]inputprojection.Projection{}
		}
		p.projections[id] = inputprojection.Projection{
			Content: prepared.Content(nil, projection.names, projection.active),
			Inputs:  bindings.inputs,
		}
	}
	return nil
}

func (p *substitutionResolution) resolveInheritedPath(
	ctx context.Context,
	id manifest.NamedResource,
	child *manifest.Kustomization,
	recordValue *manifest.DiscoveryRecord,
) error {
	l := p.loader

	record := *recordValue
	if !manifest.HasEnvsubstReference(child.Path) {
		return nil
	}
	if child.Labels[manifest.SubstituteAnnotationKey] == kustomizev1.DisabledValue ||
		child.Annotations[manifest.SubstituteAnnotationKey] == kustomizev1.DisabledValue {
		return nil
	}
	owner := record.Parent
	if owner == (manifest.NamedResource{}) || owner == id {
		return nil
	}
	var path string
	var known bool
	source := child.EffectiveSourceID()
	var roots []string
	if occurrence, located := l.catalogOccurrence(id); located && occurrence.facts.path != child.Path {
		source, path, _ = occurrence.SourcePath()
		if source.Kind == "" || source.Name == "" {
			source = manifest.BootstrapSourceID
		}
		known, roots = true, slices.Collect(manifest.RenderRoots(occurrence.facts.structural))
	} else {
		path, known = p.resolveValue(ctx, owner, child.Path)
	}
	if err := ctx.Err(); err != nil {
		return err //nolint:wrapcheck // Cancellation preserves the caller context error identity.
	}
	parent, found := l.pathCatalog(ctx, owner, &p.catalogs)
	if !found {
		return nil
	}
	if known {
		if path == child.Path {
			return nil
		}
		if roots == nil {
			projected := *child
			projected.Path = path
			roots = slices.Collect(manifest.RenderRoots(&projected))
		}
		record.ContentRoots = map[manifest.NamedResource][]string{source: roots}
	} else {
		source = parent.EffectiveSourceID()
		record.ContentRoots = map[manifest.NamedResource][]string{source: {""}}
		producer := l.Index.Records[owner]
		producer.StaticOutput = false
		producer.ContentRoots = map[manifest.NamedResource][]string{source: {""}}
		l.Index.Records[owner] = producer
	}
	l.Index.Records[id] = record
	return nil
}

func (p *substitutionResolution) resolveRecord(
	ctx context.Context,
	id manifest.NamedResource,
	recordValue *manifest.DiscoveryRecord,
) error {
	l := p.loader

	record := *recordValue
	if !id.KindInfo().Structural {
		return nil
	}
	child := l.pathDeclaration(id, new(record))
	if child == nil {
		return nil
	}
	if child.PostBuild != nil && len(child.PostBuild.SubstituteFrom) != 0 {
		if err := p.projectSubstitutions(ctx, id, child, new(record)); err != nil {
			return err
		}
	}
	l.Index.Records[id] = record
	if err := p.resolveInheritedPath(ctx, id, child, new(record)); err != nil {
		return err
	}
	return nil
}

//nolint:revive // Context and independent projection policies remain explicit without a one-caller options type.
func (p *projectionInputs) readProjection(
	ctx context.Context,
	input *store.SourceArtifact,
	file string,
	active, record bool,
) (projectionRead, error) {
	if p != nil && p.payload != nil {
		data, err := p.payload.read(ctx)
		if err != nil {
			return projectionRead{}, err
		}
		docs, err := manifest.DecodeRender(data)
		if err != nil {
			return projectionRead{}, err
		}
		inputs := &projectionInputs{
			payload:    p.payload,
			Names:      projectionSubstitutionNames(docs, active),
			Known:      p.Known,
			NamesKnown: active,
		}
		return projectionRead{inputs: inputs, known: p.Known}, nil
	}
	raw, err := fs.ReadFile(input.FS, file)
	if err != nil {
		//nolint:nilerr // Read failures invalidate projection proof and stop only the current source.
		return projectionRead{stop: true}, nil
	}
	docs, decoded := projectionDocuments(raw)
	var payload *inputPayload
	if record {
		data, encodeErr := manifest.EncodeRender(docs)
		if encodeErr != nil {
			return projectionRead{}, encodeErr
		}
		payload = &inputPayload{data: data}
	}
	inputs := &projectionInputs{
		payload:    payload,
		Names:      projectionSubstitutionNames(docs, active),
		Known:      decoded,
		NamesKnown: active,
	}
	return projectionRead{inputs: inputs, known: decoded}, nil
}

func mergeProjectionNames(names map[string]struct{}, inputs *projectionInputs, active bool) map[string]struct{} {
	if !active {
		return names
	}
	input := inputs.Names
	if input == nil {
		return nil
	}
	if names != nil {
		maps.Copy(names, input)
	}
	return names
}

func projectionSubstitutionNames(docs []manifest.DocOccurrence, active bool) map[string]struct{} {
	names := map[string]struct{}{}
	for i := range docs {
		data, err := values.SubstitutionDocument(docs[i], active)
		if err != nil {
			return nil
		}
		if data == nil {
			continue
		}
		current := values.SubstitutionNames(data)
		if current == nil {
			return nil
		}
		maps.Copy(names, current)
	}
	return names
}
