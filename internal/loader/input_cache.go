package loader

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"oras.land/oras-go/v2/errdef"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type inputCache struct {
	key     string
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
	Projection *cachedProjection     `json:"Projection,omitempty"`
	Parsed     bool                  `json:"Parsed,omitempty"`
	Name       string                `json:"Name,omitempty"`
	Digest     string                `json:"Digest,omitempty"`
	Builder    string                `json:"Builder,omitempty"`
	Body       *types.Kustomization  `json:"Body,omitzero"`
	Coverage   manifest.DocumentRole `json:"Coverage,omitempty"`
	Possible   bool                  `json:"Possible,omitempty"`
	Objects    []cachedOccurrence    `json:"Objects,omitempty"`
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

	Ordinal  int                    `json:"Ordinal,omitempty"`
	Item     int                    `json:"Item,omitempty"`
	Version  string                 `json:"Version,omitempty"`
	Identity manifest.NamedResource `json:"Identity"`
	Authored manifest.NamedResource `json:"Authored"`
	Catalog  json.RawMessage        `json:"Catalog,omitempty"`
	Facts    cachedOccurrenceFacts  `json:"Facts"`
}

type cachedOccurrenceFacts struct {
	Declared        string                    `json:"Declared,omitempty"`
	Source          manifest.NamedResource    `json:"Source"`
	Path            string                    `json:"Path,omitempty"`
	HasPath         bool                      `json:"HasPath,omitempty"`
	Namespaced      bool                      `json:"Namespaced,omitempty"`
	Uncertain       bool                      `json:"Uncertain,omitempty"`
	Retained        bool                      `json:"Retained,omitempty"`
	Reverse         []manifest.NamedResource  `json:"Reverse,omitempty"`
	Refs            []manifest.Reference      `json:"Refs,omitempty"`
	TemplateInputs  []manifest.TemplateInput  `json:"TemplateInputs,omitempty"`
	Roots           []string                  `json:"Roots,omitempty"`
	Targets         []manifest.ProducerTarget `json:"Targets,omitempty"`
	AuthoredTargets []manifest.ProducerTarget `json:"AuthoredTargets,omitempty"`
	NeedsCandidates bool                      `json:"NeedsCandidates,omitempty"`
	Policy          *manifest.EmissionPolicy  `json:"Policy,omitempty"`
	Labels          map[string]string         `json:"Labels,omitempty"`
	Labeled         bool                      `json:"Labeled,omitempty"`
}

type inputFrame struct {
	Visits  []cachedVisit `json:"Visits,omitempty"`
	archive *inputArchive
	Schema  int                    `json:"Schema,omitempty"`
	Files   []cachedInput          `json:"Files,omitempty"`
	Walks   []cachedWalk           `json:"Walks,omitempty"`
	Loads   []cachedLoad           `json:"Loads,omitempty"`
	Digests map[string]string      `json:"Digests,omitempty"`
	Types   map[string]fs.FileMode `json:"Types,omitempty"`
}

func (l *Loader) restoreCachedInputs(ctx context.Context) error {
	if !l.documents.compact || l.Options.Cache == nil || !l.Options.Cache.CanPersistRenders() {
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
	doc.deferred, doc.parsed = nil, nil
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
			declared: f.Declared, source: f.Source, path: f.Path, hasPath: f.HasPath,
			namespaced: f.Namespaced, uncertain: f.Uncertain, retained: f.Retained,
			catalog: catalog, reverse: f.Reverse, refs: f.Refs, templateInputs: f.TemplateInputs,
			roots: f.Roots, targets: f.Targets, authoredTargets: f.AuthoredTargets,
			needsCandidates: f.NeedsCandidates, policy: f.Policy, labels: f.Labels, labeled: f.Labeled,
		}
		facts.structural, _ = catalog.(*manifest.Kustomization)
		occurrence := &authoredOccurrence{
			Declaration: occurrenceID{
				Source:   source,
				File:     filepath.ToSlash(coordinate),
				Document: saved.Ordinal,
				item:     saved.Item,
			},
			Identity: saved.Identity,
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

// PersistInputs publishes only content-certified authored facts through the cache owner.
func (l *Loader) PersistInputs(ctx context.Context) error {
	states := slices.SortedFunc(maps.Values(l.inputCaches), func(a, b *inputCache) int {
		return strings.Compare(a.key, b.key)
	})
	for _, state := range states {
		input := state.input
		mounted := l.documents.artifacts[input.LocalPath]
		if mounted == nil || parsedInputIdentity(input) != parsedInputIdentity(mounted) {
			continue
		}
		frame, err := l.cacheSourceInputs(state)
		if err != nil {
			return err
		}

		if frame == nil {
			continue
		}
		if certifyErr := state.certify(ctx, frame); certifyErr != nil {
			return certifyErr
		}

		var encoded bytes.Buffer
		if writeErr := frame.write(&encoded); writeErr != nil {
			return writeErr
		}
		_, _, _, err = l.Options.Cache.Blob(
			ctx,
			cache.BlobKey{Content: state.key, Render: cache.RenderAccepted},
			&cache.Policy{},
			func(_ context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
				_, writeErr := out.Write(encoded.Bytes())
				return cache.ProducerResult{}, writeErr //nolint:wrapcheck // Cache publication preserves the writer error identity.
			},
		)
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
		for _, file := range frame.Files {
			state.files[inputCoordinate{name: file.Name, builder: file.Builder != ""}] = inputEvidence{
				digest: file.Digest, coverage: file.Coverage, projection: file.projectionPayload(),
			}
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
	if doc.projection != nil && doc.projection.payload != nil {
		saved.Projection = &cachedProjection{Known: doc.projection.Known, payload: doc.projection.payload}
	}
	if doc.body != nil {
		saved.Builder = filepath.Base(name)
		return saved, true, nil
	}
	if doc.objects == nil && doc.projection == nil || doc.decodeErr != nil {
		return cachedInput{}, false, nil //nolint:nilerr // Incomplete or failed facts remain ordinary construction work.
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
			Identity: occurrence.Identity, Authored: occurrence.Authored,
			Catalog: catalog, payload: object.payload,
			Facts: cachedOccurrenceFacts{
				Declared:        facts.declared,
				Source:          facts.source,
				Path:            facts.path,
				HasPath:         facts.hasPath,
				Namespaced:      facts.namespaced,
				Uncertain:       facts.uncertain,
				Retained:        facts.retained,
				Reverse:         facts.reverse,
				Refs:            facts.refs,
				TemplateInputs:  facts.templateInputs,
				Roots:           facts.roots,
				Targets:         facts.targets,
				AuthoredTargets: facts.authoredTargets,
				NeedsCandidates: facts.needsCandidates,
				Policy:          facts.policy,
				Labels:          facts.labels,
				Labeled:         facts.labeled,
			},
		})
	}
	return saved, true, nil
}

func (l *Loader) restoreSourceInputs(ctx context.Context, input *store.SourceArtifact, view *gittree.FS) error {
	observation, err := view.FreshInputs(ctx, nil)
	if err != nil {
		return err
	}
	state := &inputCache{
		key: manifest.Fingerprint(
			struct{ Purpose, Origin, URL string }{"discovery-inputs", input.OriginPath(input.LocalPath), input.URL},
		),
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
		cache.BlobKey{Content: state.key, Render: cache.RenderAccepted},
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	if errors.Is(err, errdef.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	frame, err := readInputFrame(generation)
	if err != nil {
		return err
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
		Schema:  1,
		Files:   files,
		Loads:   loads,
		Walks:   walks,
		Digests: map[string]string{},
		Types:   map[string]fs.FileMode{},
	}
	frame.captureEvidence(state)
	return frame, nil
}

// Payloads MUST remain separate from the compact index and hydrate only on demand.
type inputPayload struct {
	archive *inputArchive
	offset  int64
	size    int
	data    []byte
}

func (p *inputPayload) read() ([]byte, error) {
	if p.data != nil {
		return p.data, nil
	}
	if p.offset < 0 || p.size < 0 || p.offset > p.archive.size-int64(p.size) {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "discovery payload exceeds its generation")
	}
	input, err := p.archive.generation.Open(cache.FileName(p.archive.generation))
	if err != nil {
		return nil, err //nolint:wrapcheck // The cache owner retains filesystem error identity.
	}
	defer func() { _ = input.Close() }()
	positioned, ok := input.(io.ReaderAt)
	if !ok {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "discovery inputs require positioned reads")
	}
	data := make([]byte, p.size)
	if _, err := positioned.ReadAt(data, p.archive.offset+p.offset); err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read discovery payload: %w", err), "")
	}
	return data, nil
}

func readInputFrame(generation fs.FS) (*inputFrame, error) {
	input, err := generation.Open(cache.FileName(generation))
	if err != nil {
		return nil, err //nolint:wrapcheck // The cache owner retains filesystem error identity.
	}
	defer func() { _ = input.Close() }()
	info, err := input.Stat()
	if err != nil {
		return nil, fmt.Errorf("read discovery index: %w", err)
	}
	var header [8]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read discovery index: %w", err), "")
	}
	encodedSize := binary.BigEndian.Uint64(header[:])
	//nolint:gosec // The short-circuit guard makes the subtraction nonnegative.
	if info.Size() < int64(len(header)) || encodedSize > uint64(info.Size()-int64(len(header))) {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "discovery index exceeds its generation")
	}
	size := int64(encodedSize) //nolint:gosec // The file-size bound keeps encodedSize within int64.
	data := make([]byte, size)
	if _, err := io.ReadFull(input, data); err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("read discovery index: %w", err), "")
	}
	var frame inputFrame
	if err := jsonv2.Unmarshal(data, &frame, json.DefaultOptionsV1()); err != nil {
		return nil, manifest.NewError(manifest.ErrFlux, fmt.Errorf("decode discovery inputs: %w", err), "")
	}
	if frame.Schema != 1 {
		return nil, manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: unsupported schema")
	}
	for i := range frame.Walks {
		walk := &frame.Walks[i]
		walk.dictionary = frame.Visits
		if slices.ContainsFunc(walk.VisitIDs, func(id int) bool { return id < 0 || id >= len(frame.Visits) }) {
			return nil, manifest.NewError(manifest.ErrFlux, nil, "decode discovery inputs: invalid visit reference")
		}
	}
	frame.archive = &inputArchive{
		generation: generation,
		offset:     int64(len(header)) + size,
		size:       info.Size() - int64(len(header)) - size,
	}
	for i := range frame.Files {
		file := &frame.Files[i]
		for j := range file.Objects {
			saved := &file.Objects[j]
			if saved.Size != 0 {
				saved.payload = &inputPayload{archive: frame.archive, offset: saved.Offset, size: saved.Size}
			}
		}
		if projection := file.Projection; projection != nil {
			projection.payload = &inputPayload{
				archive: frame.archive, offset: projection.Offset, size: projection.Size,
			}
		}
	}
	return &frame, nil
}

func (f *inputFrame) write(out io.Writer) error {
	f.Visits = nil
	visits := map[cachedVisit]int{}
	for i := range f.Walks {
		walk := &f.Walks[i]
		var ids []int
		for visit := range walk.visits() {
			id, known := visits[*visit]
			if !known {
				id = len(f.Visits)
				visits[*visit] = id
				f.Visits = append(f.Visits, *visit)
			}
			ids = append(ids, id)
		}
		walk.VisitIDs = ids
		walk.dictionary = nil
	}
	for i := range f.Walks {
		f.Walks[i].dictionary = f.Visits
		f.Walks[i].Visits = nil
	}
	var offset int64
	for reference := range f.payloads() {
		*reference.offset, *reference.size = offset, reference.payload.size
		if reference.payload.data != nil {
			*reference.size = len(reference.payload.data)
		}
		offset += int64(*reference.size)
	}

	data, err := jsonv2.Marshal(f, json.DefaultOptionsV1(), jsonv2.OmitZeroStructFields(true))
	if err != nil {
		return manifest.NewError(manifest.ErrFlux, fmt.Errorf("encode discovery inputs: %w", err), "")
	}
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], uint64(len(data)))
	if _, err := out.Write(header[:]); err != nil {
		return manifest.NewError(manifest.ErrFlux, fmt.Errorf("write discovery inputs: %w", err), "")
	}
	if _, err := out.Write(data); err != nil {
		return manifest.NewError(manifest.ErrFlux, fmt.Errorf("write discovery inputs: %w", err), "")
	}
	for reference := range f.payloads() {
		data, err := reference.payload.read()
		if err != nil {
			return err
		}
		if _, err := out.Write(data); err != nil {
			return manifest.NewError(manifest.ErrFlux, fmt.Errorf("write discovery inputs: %w", err), "")
		}
	}
	return nil
}

// The acquired generation owns payload lifetime across independent loader snapshots.
type inputArchive struct {
	generation fs.FS
	offset     int64
	size       int64
}

func (f *cachedInput) projectionPayload() *inputPayload {
	if f.Projection != nil {
		return f.Projection.payload
	}
	return nil
}

type inputPayloadReference struct {
	payload *inputPayload
	offset  *int64
	size    *int
}

func (f *inputFrame) payloads() iter.Seq[inputPayloadReference] {
	return func(yield func(inputPayloadReference) bool) {
		for i := range f.Files {
			file := &f.Files[i]
			if projection := file.Projection; projection != nil && projection.payload != nil {
				if !yield(
					inputPayloadReference{
						payload: projection.payload,
						offset:  &projection.Offset,
						size:    &projection.Size,
					},
				) {
					return
				}
			}
			for j := range file.Objects {
				object := &file.Objects[j]
				if object.payload != nil &&
					!yield(inputPayloadReference{payload: object.payload, offset: &object.Offset, size: &object.Size}) {
					return
				}
			}
		}
	}
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
