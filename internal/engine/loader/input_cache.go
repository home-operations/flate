package loader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"oras.land/oras-go/v2/errdef"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type inputCache struct {
	lookup  cache.BlobKey
	input   *store.SourceArtifact
	digests map[string]string
	types   map[string]fs.FileMode
	files   map[inputCoordinate]inputEvidence
	loads   map[loadContext]manifest.DocumentRole
	walks   map[physicalContext]struct{}
}

type inputCoordinate struct {
	name    string
	builder bool
}

type cachedInput struct {
	declarations      *inputPayload
	DeclarationOffset int64                 `json:"DeclarationOffset,omitempty"`
	DeclarationSize   int                   `json:"DeclarationSize,omitempty"`
	Projection        *cachedProjection     `json:"Projection,omitempty"`
	Parsed            bool                  `json:"Parsed,omitempty"`
	Name              string                `json:"Name,omitempty"`
	Digest            string                `json:"Digest,omitempty"`
	Builder           string                `json:"Builder,omitempty"`
	Body              *types.Kustomization  `json:"Body,omitzero"`
	Coverage          manifest.DocumentRole `json:"Coverage,omitempty"`
	Possible          bool                  `json:"Possible,omitempty"`
	Objects           []cachedOccurrence    `json:"Objects,omitempty"`
}

type cachedFileDeclarations struct {
	Limited  bool                `json:"Limited,omitempty"`
	Present  bool                `json:"Present"`
	Complete bool                `json:"Complete"`
	Objects  []cachedDeclaration `json:"Objects"`
}

type cachedDeclaration struct {
	Identity    manifest.NamedResource `json:"Identity"`
	Fingerprint string                 `json:"Fingerprint"`
}

type cachedProjection struct {
	Known   bool  `json:"Known,omitempty"`
	Offset  int64 `json:"Offset,omitempty"`
	Size    int   `json:"Size,omitempty"`
	payload *inputPayload
}

type cachedOccurrence struct {
	Offset  int64 `json:"Offset,omitempty"`
	Size    int   `json:"Size,omitempty"`
	payload *inputPayload

	Ordinal       int                    `json:"Ordinal,omitempty"`
	Item          int                    `json:"Item,omitempty"`
	RequiresBuild bool                   `json:"RequiresBuild,omitempty"`
	Version       string                 `json:"Version,omitempty"`
	Identity      manifest.NamedResource `json:"Identity"`
	Authored      manifest.NamedResource `json:"Authored"`
	Catalog       json.RawMessage        `json:"Catalog,omitempty"`
	Facts         cachedOccurrenceFacts  `json:"Facts"`
}

type cachedOccurrenceFacts struct {
	Source             manifest.NamedResource    `json:"Source"`
	Path               string                    `json:"Path,omitempty"`
	HasPath            bool                      `json:"HasPath,omitempty"`
	Namespaced         bool                      `json:"Namespaced,omitempty"`
	Uncertain          bool                      `json:"Uncertain,omitempty"`
	Retained           bool                      `json:"Retained,omitempty"`
	Collection         bool                      `json:"Collection,omitempty"`
	Reverse            []manifest.NamedResource  `json:"Reverse,omitempty"`
	Refs               []manifest.Reference      `json:"Refs,omitempty"`
	TemplateInputs     []manifest.TemplateInput  `json:"TemplateInputs,omitempty"`
	Roots              []string                  `json:"Roots,omitempty"`
	Targets            []manifest.ProducerTarget `json:"Targets,omitempty"`
	AuthoredTargets    []manifest.ProducerTarget `json:"AuthoredTargets,omitempty"`
	NeedsCandidates    bool                      `json:"NeedsCandidates,omitempty"`
	SubstituteDisabled bool                      `json:"SubstituteDisabled,omitempty"`
	Policy             *manifest.EmissionPolicy  `json:"Policy,omitempty"`
	Labels             map[string]string         `json:"Labels,omitempty"`
	Labeled            bool                      `json:"Labeled,omitempty"`
}

func (l *Loader) restoreCachedInputs(ctx context.Context) error {
	if !l.documents.compact || l.documents.eager || l.Options.Cache == nil || !l.Options.Cache.CanPersistRenders() {
		return nil
	}
	maps.DeleteFunc(l.inputCaches, func(root string, state *inputCache) bool {
		input := l.documents.artifacts[root]
		return input == nil || parsedInputIdentity(input) != parsedInputIdentity(state.input) ||
			input.Input(
				store.ArtifactContent|store.ArtifactRevision,
			) != state.input.Input(
				store.ArtifactContent|store.ArtifactRevision,
			)
	})
	for _, root := range slices.Sorted(maps.Keys(l.documents.artifacts)) {
		input := l.documents.artifacts[root]
		view, ok := input.FS.(*gittree.FS)
		if !ok || l.inputCaches[root] != nil {
			continue
		}
		if prepared := l.Options.ParsedInputs; prepared != nil && prepared.Matches(input) {
			if state := prepared.sources[root].cache; state != nil {
				if l.inputCaches == nil {
					l.inputCaches = map[string]*inputCache{}
				}
				l.inputCaches[root] = new(*state)
			}
			continue
		}
		if err := l.restoreSourceInputs(ctx, input, view); err != nil {
			return err
		}
	}
	return nil
}

//nolint:gocritic // Readers share the map while deferred records remain immutable.
func (r documentReader) restoreCachedFile(absolute string) {
	doc := r.cache[absolute]
	file := doc.deferred
	if file == nil {
		return
	}
	doc.deferred, doc.parsed, doc.declarations = nil, nil, nil
	doc.declarationsData = nil
	if file.declarations != nil {
		doc.declarationsData = file.declarations.data
		doc.declarations = file.declarations.observation
	}
	defer func() { r.cache[absolute] = doc }()
	origin := new(r).localPath(absolute)
	source := new(r).mountedRoot(origin)
	coordinate, _ := filepath.Rel(source, origin)
	var objects []parsedObject
	if file.Parsed {
		objects = make([]parsedObject, 0, len(file.Objects))
	}
	for i := range file.Objects {
		saved := &file.Objects[i]
		catalog, err := manifest.DecodeCatalog(saved.Catalog)
		if err != nil {
			doc.decodeErr = err
			return
		}
		f := &saved.Facts
		facts := &occurrenceFacts{
			source: f.Source, path: f.Path, hasPath: f.HasPath,
			namespaced: f.Namespaced, uncertain: f.Uncertain, retained: f.Retained,
			catalog: catalog, reverse: f.Reverse, refs: f.Refs, templateInputs: f.TemplateInputs,
			roots: f.Roots, targets: f.Targets, authoredTargets: f.AuthoredTargets,
			needsCandidates: f.NeedsCandidates, policy: f.Policy, labels: f.Labels, labeled: f.Labeled,
			substituteDisabled: f.SubstituteDisabled, collection: f.Collection,
		}
		facts.census = facts
		facts.structural, _ = catalog.(*manifest.Kustomization)
		facts.substituted = facts.indexed() && facts.hasSubstitutions(saved.Authored)
		occurrence := &authoredOccurrence{
			Declaration: occurrenceID{
				Source:   source,
				File:     filepath.ToSlash(coordinate),
				Document: saved.Ordinal,
				item:     saved.Item,
			},
			Identity: saved.Identity, requiresBuild: saved.RequiresBuild,
			Authored: saved.Authored,
			facts:    facts,
		}
		var structural manifest.BaseManifest
		if facts.structural != nil {
			structural = facts.structural
		}
		objects = append(objects, parsedObject{
			Obj:     structural,
			outcome: manifest.DocOccurrence{Ordinal: saved.Ordinal},
			item:    saved.Item, fact: occurrence, version: saved.Version, payload: saved.payload,
		})
	}
	doc.objects, doc.coverage, doc.possible = objects, file.Coverage, file.Possible
	doc.parsed = nil
}

// Snapshot publication MUST materialize inventory maps before independent readers share them.
func (d *document) restoreDeclarations() error {
	if d.declarationsData == nil {
		return nil
	}
	var saved *cachedFileDeclarations
	if err := json.Unmarshal(d.declarationsData, &saved); err != nil {
		return manifest.NewError(manifest.ErrFlux, fmt.Errorf("decode declaration inventory: %w", err), "")
	}
	data := d.declarationsData
	d.declarationsData = nil
	if saved == nil {
		return nil
	}
	d.declarations = &fileObservation{Present: saved.Present, Complete: saved.Complete, limited: saved.Limited}
	d.declarations.payload.data = data
	if len(saved.Objects) != 0 {
		d.declarations.Objects = make(map[manifest.NamedResource]string, len(saved.Objects))
		for _, object := range saved.Objects {
			d.declarations.Objects[object.Identity] = object.Fingerprint
		}
	}
	return nil
}

// PersistInputs publishes only content-certified authored facts through the cache owner.
func (l *Loader) PersistInputs(ctx context.Context) error {
	states := slices.SortedFunc(maps.Values(l.inputCaches), func(a, b *inputCache) int {
		return strings.Compare(a.input.LocalPath, b.input.LocalPath)
	})
	for _, state := range states {
		if err := l.persistSourceInputs(ctx, state); err != nil {
			return err
		}
	}
	return nil
}

func (l *Loader) persistSourceInputs(ctx context.Context, state *inputCache) error {
	input := state.input
	mounted := l.documents.artifacts[input.LocalPath]
	if mounted == nil || parsedInputIdentity(input) != parsedInputIdentity(mounted) {
		return nil
	}
	frame, err := l.cacheSourceInputs(state)
	if err != nil {
		return err
	}

	if frame == nil {
		return nil
	}
	if certifyErr := state.certify(ctx, frame); certifyErr != nil {
		return certifyErr
	}

	content, err := frame.prepare(ctx)
	if err != nil {
		return err
	}
	if content == nil {
		return nil
	}
	_, contentKey := cache.InputFrameIdentity(input.OriginPath(input.LocalPath), input.URL, content)
	publication, err := content.PreparePublication(ctx, cache.FramePublicationInput{
		Owner: manifest.BootstrapSourceID,
		Content: inputFrameContent{
			Origin: input.OriginPath(input.LocalPath), URL: input.URL, Content: contentKey,
		},
		Complete: true,
		Records:  maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	})
	if err != nil {
		return err
	}
	if publication == nil {
		return nil
	}
	//nolint:errcheck // InputFrameIdentity returns a string lookup identity.
	lookupKey := state.lookup.Content.(string)
	err = cache.RecordInventory(ctx, l.Options.Cache, cache.AcceptedInventory[*cache.FramePublication]{
		Key: lookupKey, Acceptance: publication.Identity(), Content: publication,
	})
	if err != nil {
		return err
	}
	state.walks = map[physicalContext]struct{}{}
	for walkIndex := range frame.Walks {
		walk := &frame.Walks[walkIndex]
		state.walks[walk.key(input.LocalPath)] = struct{}{}
	}
	state.loads = map[loadContext]manifest.DocumentRole{}
	for loadIndex := range frame.Loads {
		load := &frame.Loads[loadIndex]
		state.loads[load.context(input.LocalPath)] = load.Coverage
	}
	state.files = map[inputCoordinate]inputEvidence{}
	for i := range frame.Files {
		file := &frame.Files[i]
		state.files[inputCoordinate{name: file.Name, builder: file.Builder != ""}] = inputEvidence{
			digest: file.Digest, coverage: file.Coverage, projection: file.projectionPayload(),
		}
	}
	return nil
}

func cacheInput(name, digest string, doc *document) (cachedInput, bool, error) {
	if doc.deferred != nil {
		saved := *doc.deferred
		saved.Objects = slices.Clone(saved.Objects)
		if saved.Projection != nil {
			saved.Projection = new(*saved.Projection)
		}
		return saved, true, nil
	}
	saved := cachedInput{
		Name:     name,
		Digest:   digest,
		Body:     doc.body,
		Coverage: doc.coverage,
		Possible: doc.possible,
		Parsed:   doc.objects != nil,
	}
	if doc.declarations != nil {
		saved.declarations = &doc.declarations.payload
	} else if doc.declarationsData != nil {
		saved.declarations = &inputPayload{data: doc.declarationsData}
	}
	if doc.projection != nil && doc.projection.payload != nil {
		saved.Projection = &cachedProjection{Known: doc.projection.Known, payload: doc.projection.payload}
	}
	switch {
	case doc.body != nil:
		saved.Builder = filepath.Base(name)
		return saved, true, nil
	case doc.decodeErr != nil, doc.objects == nil && doc.projection == nil &&
		doc.declarations == nil && doc.declarationsData == nil:
		return cachedInput{}, false, nil //nolint:nilerr // Incomplete or failed facts remain ordinary construction work.
	case doc.unprepared:
		saved.Parsed = false
		return saved, true, nil
	}
	for i := range doc.objects {
		object := &doc.objects[i]
		if object.fact == nil || object.outcome.Err != nil || len(object.Warnings) != 0 ||
			len(object.fact.facts.templateObjects) != 0 {
			return cachedInput{}, false, nil //nolint:nilerr // Incomplete or failed facts remain ordinary construction work.
		}
		occurrence, facts := object.fact, object.fact.facts
		catalog, err := manifest.EncodeCatalog(facts.catalog)
		if err != nil {
			return cachedInput{}, false, err
		}
		saved.Objects = append(saved.Objects, cachedOccurrence{
			Ordinal: object.outcome.Ordinal, Item: object.item, Version: object.version,
			Identity: occurrence.Identity, Authored: occurrence.Authored, RequiresBuild: occurrence.requiresBuild,
			Catalog: catalog, payload: object.payload,
			Facts: cachedOccurrenceFacts{
				Source:             facts.source,
				Path:               facts.path,
				HasPath:            facts.hasPath,
				Namespaced:         facts.namespaced,
				Uncertain:          facts.uncertain,
				Retained:           facts.retained,
				Reverse:            facts.reverse,
				Refs:               facts.refs,
				TemplateInputs:     facts.templateInputs,
				Roots:              facts.roots,
				Targets:            facts.targets,
				AuthoredTargets:    facts.authoredTargets,
				NeedsCandidates:    facts.needsCandidates,
				SubstituteDisabled: facts.substituteDisabled, Collection: facts.collection,
				Policy:  facts.policy,
				Labels:  facts.labels,
				Labeled: facts.labeled,
			},
		})
	}
	return saved, true, nil
}

func (l *Loader) restoreSourceInputs(ctx context.Context, input *store.SourceArtifact, view *gittree.FS) error {
	observation, err := view.FreshInputs(ctx, nil, nil)
	if err != nil {
		return err
	}
	lookup, _ := cache.InputFrameIdentity(input.OriginPath(input.LocalPath), input.URL, nil)
	state := &inputCache{
		lookup:  lookup,
		input:   input,
		digests: observation.Files,
		types:   observation.Types,
		files:   map[inputCoordinate]inputEvidence{},
		loads:   map[loadContext]manifest.DocumentRole{},
		walks:   map[physicalContext]struct{}{},
	}
	if l.inputCaches == nil {
		l.inputCaches = map[string]*inputCache{}
	}
	l.inputCaches[input.LocalPath] = state
	generation, _, _, err := l.Options.Cache.Blob(
		ctx,
		state.lookup,
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	if errors.Is(err, errdef.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	frame, err := readInputFrame(ctx, generation)
	if err != nil {
		return err
	}
	_, contentKey := cache.InputFrameIdentity(input.OriginPath(input.LocalPath), input.URL, frame.frame)
	if _, complete := frame.frame.History(manifest.BootstrapSourceID, inputFrameContent{
		Origin: input.OriginPath(input.LocalPath), URL: input.URL, Content: contentKey,
	}); !complete {
		return nil
	}
	if err := l.restoreFrameFiles(state, frame); err != nil {
		return err
	}
	return l.restoreFrameTraversal(state, frame)
}

func (l *Loader) restoreFrameFiles(state *inputCache, frame *inputFrame) error {
	for i := range frame.Files {
		file := &frame.Files[i]
		if !fs.ValidPath(file.Name) || file.Name == "." || file.Digest == "" {
			return manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid file coordinate")
		}
		if file.Digest != state.digests[file.Name] {
			continue
		}
		mode, present := state.types[file.Name]
		if !present || !mode.IsRegular() {
			continue
		}
		absolute := filepath.Join(state.input.LocalPath, filepath.FromSlash(file.Name))
		if new(l.documents).mountedRoot(absolute) != state.input.LocalPath {
			continue
		}
		if file.Builder != "" {
			if err := l.restoreCachedBuilder(state.input.LocalPath, state, file, absolute); err != nil {
				return err
			}
		} else {
			doc := l.documents.cache[absolute]
			// A deferred file MUST NOT retain consumed records through the frame's backing array.
			doc.deferred, doc.sourceRoot = new(*file), state.input.LocalPath
			doc.coverage, doc.possible = file.Coverage, file.Possible
			if projection := file.Projection; projection != nil {
				doc.projection = &projectionInputs{Known: projection.Known, payload: projection.payload}
			}
			l.documents.cache[absolute] = doc
		}
		state.files[inputCoordinate{name: file.Name, builder: file.Builder != ""}] = inputEvidence{
			digest:     file.Digest,
			coverage:   file.Coverage,
			projection: file.projectionPayload(),
		}
	}
	return nil
}

func (l *Loader) restoreFrameTraversal(state *inputCache, frame *inputFrame) error {
	facts := l.sourceFacts(state.input.LocalPath)
	changed := frame.changes(state.digests, state.types)
	for root := range l.documents.views {
		name, nested := inputRelative(state.input.LocalPath, root)
		if nested && name != "." {
			changed.paths[name] = true
			changed.directories[path.Dir(name)] = true
		}
	}
	for name := range state.types {
		absolute := filepath.Join(state.input.LocalPath, filepath.FromSlash(name))
		if new(l.documents).mountedRoot(absolute) != state.input.LocalPath {
			changed.paths[name] = true
			changed.directories[path.Dir(name)] = true
		}
	}
	for i := range frame.Loads {
		load := &frame.Loads[i]
		if !load.valid() {
			return manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid traversal coordinate")
		}
		if load.matches(changed) {
			key := load.context(state.input.LocalPath)
			restored := loadFacts{
				cached: load, cachedRoot: state.input.LocalPath,
				complete: true, imported: true, coverage: load.Coverage,
			}
			l.restoreLoadDirectories(state, load)
			facts.loads.values[key] = restored
			state.loads[key] = load.Coverage
		}
	}
	for i := range frame.Walks {
		walk := &frame.Walks[i]
		if !walk.valid() {
			return manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid walk coordinate")
		}
		if walk.matches(changed) {
			key := walk.key(state.input.LocalPath)
			facts.walks.values[key] = physicalWalk{cached: walk, imported: true}
			state.walks[key] = struct{}{}
		}
	}
	return nil
}

//nolint:nilnil // A nil frame means that no authored facts require publication.
func (l *Loader) cacheSourceInputs(state *inputCache) (*inputFrame, error) {
	updated, err := l.changedInputFiles(state)
	if err != nil {
		return nil, err
	}

	loads, changed := l.cacheLoads(state, len(updated) != 0)
	walks, walksChanged := l.cacheWalks(state, len(updated) != 0 || changed)
	if len(updated) == 0 && !changed && !walksChanged {
		return nil, nil
	}
	if walksChanged && len(loads) == 0 {
		loads, _ = l.cacheLoads(state, true)
	}
	if err := l.retainInputFiles(state, updated); err != nil {
		return nil, err
	}

	files := slices.SortedFunc(maps.Values(updated), func(a, b cachedInput) int {
		if a.Name != b.Name {
			return strings.Compare(a.Name, b.Name)
		}
		return strings.Compare(a.Builder, b.Builder)
	})
	frame := &inputFrame{
		Files:   files,
		Loads:   loads,
		Walks:   walks,
		Digests: map[string]string{},
		Types:   map[string]fs.FileMode{},
	}
	frame.captureEvidence(state)
	return frame, nil
}

func (l *Loader) restoreCachedBuilder(root string, state *inputCache, file *cachedInput, absolute string) error {
	if file.Builder != filepath.Base(file.Name) || file.Body == nil {
		return manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid builder")
	}
	dir := filepath.Dir(file.Name)
	for _, name := range manifest.KustomizeBuilderFilenames {
		candidate := filepath.ToSlash(filepath.Join(dir, name))
		if _, present := state.types[candidate]; present {
			if candidate != file.Name {
				return nil
			}
			break
		}
	}
	key := filepath.Dir(absolute)
	doc := l.documents.cache[key]
	doc.body, doc.file, doc.searched, doc.pathState = file.Body, absolute, true, pathDirectory
	doc.sourceRoot = root
	l.documents.cache[key] = doc
	return nil
}

func (l *Loader) restoreLoadDirectories(state *inputCache, load *cachedLoad) {
	for _, directory := range load.Directories {
		if !state.types[directory.Path].IsDir() {
			continue
		}
		absolute := filepath.Join(state.input.LocalPath, filepath.FromSlash(directory.Path))
		doc := l.documents.cache[absolute]
		doc.pathState, doc.sourceRoot = pathDirectory, state.input.LocalPath
		builder := false
		for _, name := range manifest.KustomizeBuilderFilenames {
			_, present := state.types[filepath.ToSlash(filepath.Join(directory.Path, name))]
			builder = builder || present
		}
		doc.searched = doc.searched || !builder
		l.documents.cache[absolute] = doc
	}
}

func (l *Loader) changedInputFiles(state *inputCache) (map[inputCoordinate]cachedInput, error) {
	input := state.input
	updated := map[inputCoordinate]cachedInput{}
	for _, absolute := range slices.Sorted(maps.Keys(l.documents.cache)) {
		if new(l.documents).mountedRoot(absolute) != input.LocalPath {
			continue
		}
		doc := l.documents.cache[absolute]
		file := absolute
		if doc.body != nil {
			file = doc.file
		}
		relative, err := filepath.Rel(input.LocalPath, file)
		if err != nil {
			continue
		}
		relative = filepath.ToSlash(relative)
		coordinate := inputCoordinate{name: relative, builder: doc.body != nil}
		digest := state.digests[relative]
		evidence := state.files[coordinate]
		if digest == "" || evidence.matches(&doc, digest) {
			continue
		}
		mode, observed := state.types[relative]
		if !observed || !mode.IsRegular() {
			continue
		}
		saved, known, err := cacheInput(relative, digest, &doc)
		if err != nil {
			return nil, err
		}
		if known {
			updated[coordinate] = saved
		}
	}
	return updated, nil
}

func (l *Loader) retainInputFiles(state *inputCache, updated map[inputCoordinate]cachedInput) error {
	input := state.input
	for coordinate, evidence := range state.files {
		if _, changed := updated[coordinate]; changed {
			continue
		}
		absolute := filepath.Join(input.LocalPath, filepath.FromSlash(coordinate.name))
		if coordinate.builder {
			absolute = filepath.Dir(absolute)
		}
		doc := l.documents.cache[absolute]
		saved, known, err := cacheInput(coordinate.name, evidence.digest, &doc)
		if err != nil {
			return err
		}
		if known {
			updated[coordinate] = saved
		}
	}
	return nil
}

func (f *inputFrame) captureEvidence(state *inputCache) {
	for loadIndex := range f.Loads {
		load := &f.Loads[loadIndex]
		for _, name := range load.Inputs {
			f.Digests[name] = state.digests[name]
		}
		for _, name := range load.Types {
			f.Types[name] = state.types[name]
		}
	}
	listed := map[string]bool{}
	for walkIndex := range f.Walks {
		walk := &f.Walks[walkIndex]
		for _, name := range walk.Inputs {
			f.Digests[name] = state.digests[name]
			if mode, present := state.types[name]; present {
				f.Types[name] = mode
			}
		}
		for _, name := range walk.Listed {
			listed[name] = true
		}
	}
	for name, mode := range state.types {
		if listed[path.Dir(name)] {
			f.Types[name] = mode
			f.Digests[name] = state.digests[name]
		}
	}
}
