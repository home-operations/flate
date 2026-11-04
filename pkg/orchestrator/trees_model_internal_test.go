package orchestrator

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"go.yaml.in/yaml/v3"
)

type modelSource struct {
	binding  string
	revision string
	root     string
}

type modelID struct {
	source    modelSource
	group     string
	kind      string
	namespace string
	name      string
}

func (id modelID) String() string {
	return fmt.Sprintf("%s@%s:%s/%s/%s/%s/%s", id.source.binding, id.source.revision,
		id.source.root, id.group, id.kind, id.namespace, id.name)
}

//nolint:gocritic // slices.SortFunc requires a by-value comparison callback.
func compareModelID(a, b modelID) int {
	return cmp.Or(cmp.Compare(a.source.binding, b.source.binding),
		cmp.Compare(a.source.revision, b.source.revision), cmp.Compare(a.source.root, b.source.root),
		cmp.Compare(a.group, b.group), cmp.Compare(a.kind, b.kind),
		cmp.Compare(a.namespace, b.namespace), cmp.Compare(a.name, b.name))
}

type modelSet map[modelID]bool

func modelProducerIDs(w modelWorld) modelSet {
	ids := make(modelSet)
	for id := range w.producers {
		ids[id] = true
	}
	return ids
}

func modelIDs(set modelSet) []modelID {
	ids := slices.Collect(maps.Keys(set))
	slices.SortFunc(ids, compareModelID)
	return ids
}

type modelPath struct {
	source modelSource
	path   string
}

func compareModelPath(a, b modelPath) int {
	return cmp.Or(
		cmp.Compare(a.source.binding, b.source.binding),
		cmp.Compare(a.source.revision, b.source.revision),
		cmp.Compare(a.source.root, b.source.root),
		cmp.Compare(a.path, b.path),
	)
}

type modelFile struct {
	body      []byte
	included  bool
	directory bool
}

type modelRead struct {
	file     modelPath
	optional bool
	listing  bool
}

type modelObjectID struct {
	modelID

	version          string
	namespacePresent bool
}

type modelDocument struct {
	id  modelObjectID
	raw []byte
	sdk any
}

type modelProducer struct {
	declaration []byte
	reads       []modelRead
	content     []modelID
	ordering    []modelID
	sources     []modelID
	emitter     *modelID
	documents   []modelDocument
	suspended   bool
	alias       bool
}

type modelWorld struct {
	files       map[modelPath]modelFile
	producers   map[modelID]modelProducer
	uncertainty map[modelID]string
	static      []modelDocument
}

type modelReadValue struct {
	read    modelRead
	present bool
	names   []string
	body    []byte
}

func modelReadInput(w modelWorld, read modelRead) modelReadValue {
	value := modelReadValue{read: read}
	if !read.listing {
		file, ok := w.files[read.file]
		value.present = ok && file.included && !file.directory
		if value.present {
			value.body = file.body
		}
		return value
	}
	prefix := strings.TrimSuffix(read.file.path, "/") + "/"
	if read.file.path == "." {
		prefix = ""
	}
	directory, exists := w.files[read.file]
	value.present = exists && directory.included && directory.directory
	for key, file := range w.files {
		if key.source != read.file.source || !file.included || !strings.HasPrefix(key.path, prefix) {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimPrefix(key.path, prefix), "/")
		value.names = append(value.names, name)
	}
	slices.Sort(value.names)
	value.names = slices.Compact(value.names)
	value.present = value.present || len(value.names) > 0
	return value
}

type modelRecord struct {
	definition modelProducer
	reads      []modelReadValue
	content    []*modelRecord
}

type modelStatus struct {
	terminal bool
	failure  string
	blockers modelSet
}

type modelEvaluation struct {
	records map[modelID]*modelRecord
	status  map[modelID]modelStatus
}

func evaluateModel(w modelWorld) modelEvaluation {
	result := modelEvaluation{records: make(map[modelID]*modelRecord), status: make(map[modelID]modelStatus)}
	pending, referenced := make(modelSet), make(modelSet)
	for _, p := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		for _, id := range slices.Concat(p.content, p.ordering, p.sources) {
			referenced[id] = true
		}
		if p.emitter != nil {
			referenced[*p.emitter] = true
		}
	}
	for id, p := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		if !p.alias || referenced[id] {
			pending[id] = true
		}
	}
	evaluateModelSegment(&w, &result, &pending)
	return result
}

type modelSelection struct {
	affected    modelSet
	support     modelSet
	unavailable modelSet
	deleted     modelSet
	allowed     map[modelID]string
}

type modelComparison struct {
	before modelSelection
	after  modelSelection
}

func selectModel(w modelWorld, evaluation modelEvaluation, affected, deleted modelSet) modelSelection {
	selection := modelSelection{
		affected: make(modelSet), support: make(modelSet),
		unavailable: make(modelSet), deleted: maps.Clone(deleted), allowed: maps.Clone(w.uncertainty),
	}
	for id, state := range evaluation.status {
		if state.failure != "" {
			selection.unavailable[id] = true
		}
		if affected[id] {
			selection.affected[id] = true
		}
	}
	pending := modelIDs(selection.affected)
	for len(pending) > 0 {
		id := pending[0]
		pending = pending[1:]
		producer := w.producers[id]
		providers := slices.Concat(producer.content, producer.sources)
		if producer.emitter != nil {
			providers = append(providers, *producer.emitter)
		}
		for _, provider := range providers {
			if _, ok := w.producers[provider]; !ok || selection.affected[provider] || selection.support[provider] {
				continue
			}
			selection.support[provider] = true
			pending = append(pending, provider)
		}
	}
	return selection
}

func compareModelWorlds(before, after modelWorld) modelComparison {
	old, current := evaluateModel(before), evaluateModel(after)
	affected, deleted := make(modelSet), make(modelSet)
	for id, record := range old.records {
		if !reflect.DeepEqual(record, current.records[id]) {
			affected[id] = true
		}
		if _, ok := current.records[id]; !ok {
			deleted[id] = true
		}
	}
	for id, record := range current.records {
		if !reflect.DeepEqual(record, old.records[id]) {
			affected[id] = true
		}
	}
	oldObjects, newObjects := make(modelSet), make(modelSet)
	collectModelObjects(before, old, oldObjects)
	collectModelObjects(after, current, newObjects)
	for id := range oldObjects {
		if !newObjects[id] {
			deleted[id] = true
		}
	}
	return modelComparison{
		before: selectModel(before, old, affected, deleted),
		after:  selectModel(after, current, affected, deleted),
	}
}

func compareModelSelection(want modelSelection, got modelSet) error {
	missing, extra := make(modelSet), make(modelSet)
	for id := range want.affected {
		if !got[id] {
			missing[id] = true
		}
	}
	for id := range want.support {
		if !got[id] {
			missing[id] = true
		}
	}
	for id := range got {
		if !want.affected[id] && !want.support[id] && want.allowed[id] == "" {
			extra[id] = true
		}
	}
	if len(missing)+len(extra) > 0 {
		return fmt.Errorf("producer admission: missing=%v unexpected=%v", modelIDs(missing), modelIDs(extra))
	}
	return nil
}

type modelObservation struct {
	producers  map[modelID]modelStatus
	documents  []modelDocument
	deleted    map[modelObjectID]int
	errorClass string
}

func compareModelObservation(want, got modelObservation) error {
	missing, extra := make(modelSet), make(modelSet)
	for id := range want.producers {
		if _, ok := got.producers[id]; !ok {
			missing[id] = true
		}
	}
	for id := range got.producers {
		if _, ok := want.producers[id]; !ok {
			extra[id] = true
		}
	}
	if len(missing)+len(extra) > 0 {
		return fmt.Errorf("producer membership: missing=%v unexpected=%v", modelIDs(missing), modelIDs(extra))
	}
	index := func(documents []modelDocument) map[modelObjectID][]modelDocument {
		out := make(map[modelObjectID][]modelDocument)
		{
			documentValues := documents
			for documentIndex := range documentValues {
				document := documentValues[documentIndex]

				out[document.id] = append(out[document.id], document)
			}
		}
		return out
	}
	oldDocs, newDocs := index(want.documents), index(got.documents)
	if len(oldDocs) != len(newDocs) {
		return fmt.Errorf("object membership: want %d identities, got %d", len(oldDocs), len(newDocs))
	}
	keys := slices.Collect(maps.Keys(oldDocs))
	slices.SortFunc(keys, func(a, b modelObjectID) int {
		return cmp.Or(compareModelID(a.modelID, b.modelID), cmp.Compare(a.version, b.version),
			cmp.Compare(strconv.FormatBool(a.namespacePresent), strconv.FormatBool(b.namespacePresent)))
	})
	{
		idValues := keys
		for idIndex := range idValues {
			id := idValues[idIndex]

			if len(oldDocs[id]) != len(newDocs[id]) {
				return fmt.Errorf(
					"object membership/multiplicity %s: want %d, got %d",
					id.modelID,
					len(oldDocs[id]),
					len(newDocs[id]),
				)
			}
		}
	}
	if want.errorClass != got.errorClass {
		return fmt.Errorf("error classification: want %q, got %q", want.errorClass, got.errorClass)
	}
	if !maps.Equal(want.deleted, got.deleted) {
		return fmt.Errorf("deleted identities differ: want %v, got %v", want.deleted, got.deleted)
	}
	producerIDs := slices.Collect(maps.Keys(want.producers))
	slices.SortFunc(producerIDs, compareModelID)
	for _, id := range producerIDs {
		a, b := want.producers[id], got.producers[id]
		if a.terminal != b.terminal || a.failure != b.failure || !maps.Equal(a.blockers, b.blockers) {
			return fmt.Errorf("producer state %s: want %+v, got %+v", id, a, b)
		}
	}
	return compareModelDocuments(keys, oldDocs, newDocs)
}

func equalModelDocument(a, b *modelDocument) (bool, error) {
	if (a.raw == nil) != (b.raw == nil) {
		return false, nil
	}
	if a.raw == nil {
		return reflect.DeepEqual(a.sdk, b.sdk), nil
	}
	decode := func(raw []byte) (*yaml.Node, error) {
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		var node, trailing yaml.Node
		if err := decoder.Decode(&node); err != nil {
			return nil, fmt.Errorf("decode YAML: %w", err)
		}
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			return nil, errors.Join(errors.New("expected one YAML document"), err)
		}
		return &node, nil
	}
	left, err := decode(a.raw)
	if err != nil {
		return false, err
	}
	right, err := decode(b.raw)
	if err != nil {
		return false, err
	}
	return equalModelYAML(left, right), nil
}

func equalModelYAML(a, b *yaml.Node) bool {
	if a.Kind != b.Kind || a.Tag != b.Tag || a.Value != b.Value || len(a.Content) != len(b.Content) {
		return false
	}
	if a.Kind == yaml.MappingNode {
		used := make([]bool, len(b.Content)/2)
		for i := 0; i < len(a.Content); i += 2 {
			match := -1
			for j := 0; j < len(b.Content); j += 2 {
				if !used[j/2] && equalModelYAML(a.Content[i], b.Content[j]) &&
					equalModelYAML(a.Content[i+1], b.Content[j+1]) {
					match = j / 2
					break
				}
			}
			if match < 0 {
				return false
			}
			used[match] = true
		}
		return true
	}
	for i := range a.Content {
		if !equalModelYAML(a.Content[i], b.Content[i]) {
			return false
		}
	}
	return true
}

type modelOperation struct {
	kind       string
	file       modelPath
	body       []byte
	included   bool
	producer   modelID
	definition modelProducer
	directory  bool
}

type modelCase struct {
	before      modelWorld
	after       modelWorld
	operations  []modelOperation
	unsupported map[string]int
}

func cloneModelWorld(w modelWorld) modelWorld {
	clone := modelWorld{
		files: make(map[modelPath]modelFile), producers: make(map[modelID]modelProducer),
		uncertainty: maps.Clone(w.uncertainty), static: slices.Clone(w.static),
	}
	for key, file := range w.files {
		file.body = slices.Clone(file.body)
		clone.files[key] = file
	}
	for id, producer := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		producer.declaration = slices.Clone(producer.declaration)
		producer.reads = slices.Clone(producer.reads)
		producer.content = slices.Clone(producer.content)
		producer.ordering = slices.Clone(producer.ordering)
		producer.sources = slices.Clone(producer.sources)
		producer.documents = slices.Clone(producer.documents)
		for i := range producer.documents {
			producer.documents[i].raw = slices.Clone(producer.documents[i].raw)
		}
		clone.producers[id] = producer
	}
	return clone
}

type appliedModel struct {
	world       modelWorld
	unsupported map[string]int
}

func applyModelOperations(before modelWorld, operations []modelOperation) appliedModel {
	after := cloneModelWorld(before)
	unsupported := make(map[string]int)
	{
		operationValues := operations
		for operationIndex := range operationValues {
			operation := operationValues[operationIndex]

			switch operation.kind {
			case "write":
				if _, exists := after.files[operation.file]; !exists && len(after.files) >= 48 {
					unsupported["file-limit"]++
					continue
				}
				if len(operation.body) > 4096 {
					unsupported["body-limit"]++
					continue
				}
				after.files[operation.file] = modelFile{
					body:      slices.Clone(operation.body),
					included:  operation.included,
					directory: operation.directory,
				}
			case "producer":
				if _, exists := after.producers[operation.producer]; !exists && len(after.producers) >= 12 {
					unsupported["producer-limit"]++
					continue
				}
				after.producers[operation.producer] = operation.definition
			case "include":
				if file, exists := after.files[operation.file]; exists {
					file.included = operation.included
					after.files[operation.file] = file
				}
			case "delete-file":
				delete(after.files, operation.file)
			case "delete-producer":
				delete(after.producers, operation.producer)
			default:
				unsupported[operation.kind]++
			}
		}
	}
	after.static = renderModelStaticRoots(after, before)
	return appliedModel{world: after, unsupported: unsupported}
}

type modelBytes struct {
	data []byte
	at   int
}

func (r *modelBytes) next() byte {
	if r.at == len(r.data) {
		return 0
	}
	value := r.data[r.at]
	r.at++
	return value
}

func modelWorldOperations(before, after modelWorld) []modelOperation {
	var operations []modelOperation
	files := map[modelPath]bool{}
	for file := range before.files {
		files[file] = true
	}
	for file := range after.files {
		files[file] = true
	}
	keys := slices.Collect(maps.Keys(files))
	slices.SortFunc(keys, compareModelPath)
	for _, file := range keys {
		if _, exists := after.files[file]; !exists {
			operations = append(operations, modelOperation{kind: "delete-file", file: file})
		}
	}
	for _, file := range keys {
		content, exists := after.files[file]
		old, had := before.files[file]
		if exists && (!had || !reflect.DeepEqual(content, old)) {
			operations = append(
				operations,
				modelOperation{
					kind:      "write",
					file:      file,
					body:      slices.Clone(content.body),
					included:  content.included,
					directory: content.directory,
				},
			)
		}
	}
	ids := modelProducerIDs(before)
	for id := range after.producers {
		ids[id] = true
	}
	for _, id := range modelIDs(ids) {
		if _, exists := after.producers[id]; !exists {
			operations = append(operations, modelOperation{kind: "delete-producer", producer: id})
		}
	}
	for _, id := range modelIDs(ids) {
		p, exists := after.producers[id]
		old, had := before.producers[id]
		if exists && (!had || !reflect.DeepEqual(p, old)) {
			operations = append(operations, modelOperation{kind: "producer", producer: id, definition: p})
		}
	}
	return operations
}

func modelDeletedDocuments(before, after []modelDocument) map[modelObjectID]int {
	counts := make(map[modelObjectID]int)
	{
		docValues3 := before
		for docIndex3 := range docValues3 {
			doc := docValues3[docIndex3]

			counts[doc.id]++
		}
	}
	{
		docValues4 := after
		for docIndex4 := range docValues4 {
			doc := docValues4[docIndex4]

			counts[doc.id]--
		}
	}
	for id, count := range counts {
		if count <= 0 {
			delete(counts, id)
		}
	}
	return counts
}

func checkModelBounds(c *modelCase) error {
	if len(c.operations) > 24 {
		return fmt.Errorf("operation bound: %d", len(c.operations))
	}
	if returned, result0 := checkModelBoundsSegment(c); returned {
		return result0
	}
	return nil
}

func TestIndependentModel_RenderGrammar(t *testing.T) {
	t.Parallel()
	for _, input := range renderModelInputs() {
		t.Run(input.name, func(t *testing.T) {
			t.Parallel()
			for _, operation := range []byte{0, 1, 2, 3, 4, 5, 6, 7, 8} {
				encoded := slices.Clone(input.input)
				for i := range 24 {
					encoded = append(encoded, operation, byte(i), byte(i))
				}
				fixture := decodeRenderModelCase(encoded)
				{
					err := checkModelBounds(&fixture.modelCase)
					assert.FatalIf(t, err != nil, "input=%x: %v", encoded, err)
				}
				applyModelOperationsResult1 := applyModelOperations(fixture.before, fixture.operations)
				replayed, unsupported := applyModelOperationsResult1.world, applyModelOperationsResult1.unsupported
				assert.FatalIf(
					t,
					!reflect.DeepEqual(replayed, fixture.after) || len(unsupported) != 0,
					"operation replay differs: input=%x unsupported=%v",
					encoded,
					unsupported,
				)
				assert.FatalIf(
					t,
					!reflect.DeepEqual(fixture, decodeRenderModelCase(encoded)),
					"%v",
					"composed decoder is nondeterministic",
				)
			}
		})
	}
}

func checkModelBoundsSegment(c *modelCase) (bool, error) {
	for _, w := range []modelWorld{c.before, c.after} {
		if len(w.producers) > 12 || len(w.files) > 48 {
			return true, fmt.Errorf("collection bounds: producers=%d files=%d", len(w.producers), len(w.files))
		}
		sources, namespaces := map[string]bool{}, map[string]bool{}
		documents := 0
		if err := w.checkFileBounds(sources); err != nil {
			return true, err
		}
		for id, producer := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
			sources[id.source.binding] = true
			if id.namespace != "" {
				namespaces[id.namespace] = true
			}
			documents += len(producer.documents)
			if w.dependencyDepth(id, make(modelSet)) > 4 {
				return true, fmt.Errorf("dependency depth bound: %s", id)
			}
		}
		if len(sources) > 2 || len(namespaces) > 3 || documents > 48 {
			return true, fmt.Errorf(
				"expansion bounds: sources=%d namespaces=%d documents=%d",
				len(sources),
				len(namespaces),
				documents,
			)
		}
	}
	return false, error(nil)
}

func evaluateModelSegment(w *modelWorld, result *modelEvaluation, pending *modelSet) {
	for len((*pending)) > 0 {
		progress := false
		for _, id := range modelIDs((*pending)) {
			producer := w.producers[id]
			dependencies := slices.Concat(producer.content, producer.ordering, producer.sources)
			if producer.emitter != nil {
				dependencies = append(dependencies, *producer.emitter)
			}
			if producer.suspended {
				dependencies = nil
			}
			if slices.ContainsFunc(dependencies, func(dep modelID) bool { return (*pending)[dep] }) {
				continue
			}
			record := &modelRecord{definition: producer}
			status := modelStatus{terminal: true, blockers: make(modelSet)}
			record.readModelInputs(w, &producer, &status)
			for _, dep := range producer.content {
				record.content = append(record.content, result.records[dep])
			}
			status.blockDependencies(dependencies, result)
			result.records[id], result.status[id] = record, status
			delete((*pending), id)
			progress = true
		}
		if progress {
			continue
		}
		for _, id := range modelIDs((*pending)) {
			producer := w.producers[id]
			result.records[id] = &modelRecord{definition: producer}
			result.status[id] = modelStatus{terminal: true, failure: "cycle", blockers: maps.Clone((*pending))}
			delete(result.status[id].blockers, id)
		}
		break
	}
}

func collectModelObjects(w modelWorld, evaluation modelEvaluation, objects modelSet) {
	for id, producer := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		if evaluation.status[id].failure != "" {
			continue
		}
		documents := producer.documents
		for i := range documents {
			document := documents[i]
			objects[document.id.modelID] = true
		}
	}
}

func compareModelDocuments(keys []modelObjectID, oldDocs, newDocs map[modelObjectID][]modelDocument) error {
	for keyIndex := range keys {
		id := keys[keyIndex]
		used := make([]bool, len(newDocs[id]))
		documents := oldDocs[id]
		for documentIndex := range documents {
			document := documents[documentIndex]
			match, err := findModelDocument(&document, newDocs[id], used)
			if err != nil {
				return fmt.Errorf("document %s: %w", id.modelID, err)
			}
			if match < 0 {
				return fmt.Errorf("document value %s differs", id.modelID)
			}
			used[match] = true
		}
	}
	return nil
}

func findModelDocument(document *modelDocument, candidates []modelDocument, used []bool) (int, error) {
	match := -1
	for i := range candidates {
		candidate := candidates[i]
		if used[i] {
			continue
		}
		equal, err := equalModelDocument(document, &candidate)
		if err != nil {
			return match, err
		}
		if equal {
			match = i
			break
		}
	}
	return match, nil
}

func (w modelWorld) checkFileBounds(sources map[string]bool) error {
	for file, content := range w.files {
		sources[file.source.binding] = true
		if len(content.body) > 4096 || len(strings.Split(file.path, "/")) > 4 {
			return fmt.Errorf("file bounds: %s bytes=%d", file.path, len(content.body))
		}
	}
	return nil
}

//nolint:gocritic // Value IDs preserve stack allocation in recursive traversal.
func (w modelWorld) dependencyDepth(id modelID, path modelSet) int {
	if path[id] {
		return 5
	}
	p, exists := w.producers[id]
	if !exists {
		return 0
	}
	path = maps.Clone(path)
	path[id] = true
	d := 1
	refs := slices.Concat(p.content, p.ordering, p.sources)
	if p.emitter != nil {
		refs = append(refs, *p.emitter)
	}
	for _, dep := range refs {
		d = max(d, 1+w.dependencyDepth(dep, path))
	}
	return d
}

func (r *modelRecord) readModelInputs(w *modelWorld, producer *modelProducer, status *modelStatus) {
	if producer.suspended {
		status.failure = "suspended"
	}
	for _, read := range producer.reads {
		if producer.suspended {
			break
		}
		value := modelReadInput((*w), read)
		r.reads = append(r.reads, value)
		if !value.present && !read.optional {
			status.failure = "missing-input"
		}
	}
}

func (s *modelStatus) blockDependencies(dependencies []modelID, result *modelEvaluation) {
	for _, dep := range dependencies {
		state, ok := result.status[dep]
		if !ok || state.failure != "" {
			s.blockers[dep] = true
			s.failure = "blocked"
		}
	}
}
