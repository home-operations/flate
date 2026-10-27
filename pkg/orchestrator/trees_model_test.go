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
	"strings"
	"testing"
	"time"

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
	return cmp.Or(cmp.Compare(a.source.binding, b.source.binding), cmp.Compare(a.source.revision, b.source.revision), cmp.Compare(a.source.root, b.source.root), cmp.Compare(a.path, b.path))
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
	for _, p := range w.producers {
		for _, id := range slices.Concat(p.content, p.ordering, p.sources) {
			referenced[id] = true
		}
		if p.emitter != nil {
			referenced[*p.emitter] = true
		}
	}
	for id, p := range w.producers {
		if !p.alias || referenced[id] {
			pending[id] = true
		}
	}
	for len(pending) > 0 {
		progress := false
		for _, id := range modelIDs(pending) {
			producer := w.producers[id]
			dependencies := slices.Concat(producer.content, producer.ordering, producer.sources)
			if producer.emitter != nil {
				dependencies = append(dependencies, *producer.emitter)
			}
			if producer.suspended {
				dependencies = nil
			}
			if slices.ContainsFunc(dependencies, func(dep modelID) bool { return pending[dep] }) {
				continue
			}
			record := &modelRecord{definition: producer}
			status := modelStatus{terminal: true, blockers: make(modelSet)}
			if producer.suspended {
				status.failure = "suspended"
			}
			for _, read := range producer.reads {
				if producer.suspended {
					break
				}
				value := modelReadInput(w, read)
				record.reads = append(record.reads, value)
				if !value.present && !read.optional {
					status.failure = "missing-input"
				}
			}
			for _, dep := range producer.content {
				record.content = append(record.content, result.records[dep])
			}
			for _, dep := range dependencies {
				state, ok := result.status[dep]
				if !ok || state.failure != "" {
					status.blockers[dep] = true
					status.failure = "blocked"
				}
			}
			result.records[id], result.status[id] = record, status
			delete(pending, id)
			progress = true
		}
		if progress {
			continue
		}
		for _, id := range modelIDs(pending) {
			producer := w.producers[id]
			result.records[id] = &modelRecord{definition: producer}
			result.status[id] = modelStatus{terminal: true, failure: "cycle", blockers: maps.Clone(pending)}
			delete(result.status[id].blockers, id)
		}
		break
	}
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
	selection := modelSelection{affected: make(modelSet), support: make(modelSet),
		unavailable: make(modelSet), deleted: maps.Clone(deleted), allowed: maps.Clone(w.uncertainty)}
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
	for id, producer := range before.producers {
		if old.status[id].failure != "" {
			continue
		}
		for _, doc := range producer.documents {
			oldObjects[doc.id.modelID] = true
		}
	}
	for id, producer := range after.producers {
		if current.status[id].failure != "" {
			continue
		}
		for _, doc := range producer.documents {
			newObjects[doc.id.modelID] = true
		}
	}
	for id := range oldObjects {
		if !newObjects[id] {
			deleted[id] = true
		}
	}
	return modelComparison{before: selectModel(before, old, affected, deleted), after: selectModel(after, current, affected, deleted)}
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
		for _, document := range documents {
			out[document.id] = append(out[document.id], document)
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
			cmp.Compare(fmt.Sprint(a.namespacePresent), fmt.Sprint(b.namespacePresent)))
	})
	for _, id := range keys {
		if len(oldDocs[id]) != len(newDocs[id]) {
			return fmt.Errorf("object membership/multiplicity %s: want %d, got %d", id.modelID, len(oldDocs[id]), len(newDocs[id]))
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
	for _, id := range keys {
		used := make([]bool, len(newDocs[id]))
		for _, document := range oldDocs[id] {
			match := -1
			for i, candidate := range newDocs[id] {
				if used[i] {
					continue
				}
				equal, err := equalModelDocument(document, candidate)
				if err != nil {
					return fmt.Errorf("document %s: %w", id.modelID, err)
				}
				if equal {
					match = i
					break
				}
			}
			if match < 0 {
				return fmt.Errorf("document value %s differs", id.modelID)
			}
			used[match] = true
		}
	}
	return nil
}

func equalModelDocument(a, b modelDocument) (bool, error) {
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
				if !used[j/2] && equalModelYAML(a.Content[i], b.Content[j]) && equalModelYAML(a.Content[i+1], b.Content[j+1]) {
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
	clone := modelWorld{files: make(map[modelPath]modelFile), producers: make(map[modelID]modelProducer),
		uncertainty: maps.Clone(w.uncertainty), static: slices.Clone(w.static)}
	for key, file := range w.files {
		file.body = slices.Clone(file.body)
		clone.files[key] = file
	}
	for id, producer := range w.producers {
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

func applyModelOperations(before modelWorld, operations []modelOperation) (modelWorld, map[string]int) {
	after := cloneModelWorld(before)
	unsupported := make(map[string]int)
	for _, operation := range operations {
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
			after.files[operation.file] = modelFile{body: slices.Clone(operation.body), included: operation.included, directory: operation.directory}
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
	after.static = renderModelStaticRoots(after, before)
	return after, unsupported
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
			operations = append(operations, modelOperation{kind: "write", file: file, body: slices.Clone(content.body), included: content.included, directory: content.directory})
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
	for _, doc := range before {
		counts[doc.id]++
	}
	for _, doc := range after {
		counts[doc.id]--
	}
	for id, count := range counts {
		if count <= 0 {
			delete(counts, id)
		}
	}
	return counts
}

func checkModelBounds(c modelCase) error {
	if len(c.operations) > 24 {
		return fmt.Errorf("operation bound: %d", len(c.operations))
	}
	for _, w := range []modelWorld{c.before, c.after} {
		if len(w.producers) > 12 || len(w.files) > 48 {
			return fmt.Errorf("collection bounds: producers=%d files=%d", len(w.producers), len(w.files))
		}
		sources, namespaces := map[string]bool{}, map[string]bool{}
		documents := 0
		for file, content := range w.files {
			sources[file.source.binding] = true
			if len(content.body) > 4096 || len(strings.Split(file.path, "/")) > 4 {
				return fmt.Errorf("file bounds: %s bytes=%d", file.path, len(content.body))
			}
		}
		var depth func(modelID, modelSet) int
		depth = func(id modelID, path modelSet) int {
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
				d = max(d, 1+depth(dep, path))
			}
			return d
		}
		for id, producer := range w.producers {
			sources[id.source.binding] = true
			if id.namespace != "" {
				namespaces[id.namespace] = true
			}
			documents += len(producer.documents)
			if depth(id, make(modelSet)) > 4 {
				return fmt.Errorf("dependency depth bound: %s", id)
			}
		}
		if len(sources) > 2 || len(namespaces) > 3 || documents > 48 {
			return fmt.Errorf("expansion bounds: sources=%d namespaces=%d documents=%d", len(sources), len(namespaces), documents)
		}
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
				if err := checkModelBounds(fixture.modelCase); err != nil {
					t.Fatalf("input=%x: %v", encoded, err)
				}
				replayed, unsupported := applyModelOperations(fixture.before, fixture.operations)
				if !reflect.DeepEqual(replayed, fixture.after) || len(unsupported) != 0 {
					t.Fatalf("operation replay differs: input=%x unsupported=%v", encoded, unsupported)
				}
				if !reflect.DeepEqual(fixture, decodeRenderModelCase(encoded)) {
					t.Fatal("composed decoder is nondeterministic")
				}
			}
		})
	}
}

func TestIndependentModel_Witnesses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		input     []byte
		kind      string
		committed bool
	}{
		{"0300", []byte{3, 0}, "Kustomization", false},
		{"0301", []byte{3, 1}, "Kustomization", true},
		{"0105", []byte{1, 5}, "HelmRelease", true},
		{"0185", []byte{1, 133}, "HelmRelease", false},
		{"0106", []byte{1, 6}, "HelmRelease", true},
		{"0109", []byte{1, 9}, "HelmRelease", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := decodeRenderModelCase(tc.input)
			if fixture.committed != tc.committed {
				t.Fatal("witness baseline route changed")
			}
			source := renderModelSource(fixture.before)
			group := "kustomize.toolkit.fluxcd.io"
			if tc.kind == "HelmRelease" {
				group = "helm.toolkit.fluxcd.io"
			}
			app := modelID{source: source, group: group, kind: tc.kind, namespace: "flux-system", name: "app"}
			wantAffected := modelSet{app: true}
			wantSupport := modelSet{renderModelSourceID(source): true}
			selection := compareModelWorlds(fixture.before, fixture.after)
			for _, side := range []modelSelection{selection.before, selection.after} {
				if !maps.Equal(side.affected, wantAffected) || !maps.Equal(side.support, wantSupport) || len(side.unavailable) != 0 || len(side.deleted) != 0 {
					t.Fatalf("witness membership changed: affected=%v support=%v unavailable=%v deleted=%v", modelIDs(side.affected), modelIDs(side.support), modelIDs(side.unavailable), modelIDs(side.deleted))
				}
			}
			if tc.kind == "Kustomization" {
				file := modelPath{source: source, path: "flux/optional.yaml"}
				if _, exists := fixture.before.files[file]; exists {
					t.Fatal("optional input no longer absent before")
				}
				if !bytes.Contains(fixture.after.files[file].body, []byte("VALUE: new")) {
					t.Fatal("optional input appearance changed")
				}
			}
		})
	}
}

func TestIndependentModel(t *testing.T) {
	t.Parallel()
	source := modelSource{binding: "cluster", revision: "working", root: "repo"}
	id := func(name string) modelID {
		return modelID{source: source, group: "kustomize.toolkit.fluxcd.io", kind: "Kustomization", namespace: "apps", name: name}
	}
	provider, consumer, parent, sibling, sourceProvider := id("provider"), id("consumer"), id("parent"), id("sibling"), id("source")
	file := modelPath{source: source, path: "inputs/data"}
	world := modelWorld{files: map[modelPath]modelFile{file: {body: []byte("old"), included: true}},
		producers: map[modelID]modelProducer{
			provider:       {reads: []modelRead{{file: file}}},
			consumer:       {content: []modelID{provider}, sources: []modelID{sourceProvider}, emitter: &parent},
			parent:         {},
			sourceProvider: {},
			sibling:        {ordering: []modelID{provider}, emitter: &parent},
		}}
	tests := []struct {
		name  string
		check func(*testing.T)
	}{
		{"content-and-support-without-siblings", func(t *testing.T) {
			t.Helper()
			after, _ := applyModelOperations(world, []modelOperation{{kind: "write", file: file, body: []byte("new"), included: true}})
			result := compareModelWorlds(world, after)
			for _, side := range []modelSelection{result.before, result.after} {
				if !maps.Equal(side.affected, modelSet{provider: true, consumer: true}) ||
					!maps.Equal(side.support, modelSet{parent: true, sourceProvider: true}) {
					t.Fatalf("affected=%v support=%v", modelIDs(side.affected), modelIDs(side.support))
				}
				if side.affected[sibling] || side.support[sibling] {
					t.Fatal("support admitted unchanged sibling")
				}
			}
			if string(world.files[file].body) != "old" {
				t.Fatal("mutation changed the before world")
			}
		}},
		{"omitted-consumer", func(t *testing.T) {
			t.Helper()
			want := modelSelection{affected: modelSet{provider: true, consumer: true}}
			if err := compareModelSelection(want, modelSet{provider: true}); err == nil || !strings.Contains(err.Error(), "missing=") {
				t.Fatalf("omitted consumer accepted: %v", err)
			}
		}},
		{"extra-consumer", func(t *testing.T) {
			t.Helper()
			want := modelSelection{affected: modelSet{provider: true}}
			if err := compareModelSelection(want, modelSet{provider: true, sibling: true}); err == nil || !strings.Contains(err.Error(), "unexpected=") {
				t.Fatalf("extra consumer accepted: %v", err)
			}
		}},
		{"individually-declared-uncertainty", func(t *testing.T) {
			t.Helper()
			want := modelSelection{affected: modelSet{provider: true}, allowed: map[modelID]string{sibling: "shared unclassified input"}}
			if err := compareModelSelection(want, modelSet{provider: true, sibling: true}); err != nil {
				t.Fatal(err)
			}
			if err := compareModelSelection(want, modelSet{provider: true, parent: true}); err == nil {
				t.Fatal("uncertainty admitted an undeclared producer")
			}
			want.allowed[sibling] = ""
			if err := compareModelSelection(want, modelSet{provider: true, sibling: true}); err == nil {
				t.Fatal("uncertainty without a reason accepted")
			}
		}},
		{"source-and-group-collision", func(t *testing.T) {
			t.Helper()
			artifact := source
			artifact.revision = "pinned"
			otherRoot := source
			otherRoot.root = "other"
			otherBinding := source
			otherBinding.binding = "other"
			before := modelWorld{files: make(map[modelPath]modelFile), producers: make(map[modelID]modelProducer)}
			identities := []modelID{provider, provider, provider, provider, provider}
			identities[1].source, identities[2].source, identities[3].source = artifact, otherRoot, otherBinding
			identities[4].group = "other.example"
			for _, key := range identities {
				input := modelPath{source: key.source, path: "same/path"}
				if key.group == "other.example" {
					input.path = "group/path"
				}
				before.files[input] = modelFile{body: []byte("old"), included: true}
				before.producers[key] = modelProducer{reads: []modelRead{{file: input}}}
			}
			after := cloneModelWorld(before)
			after.files[modelPath{source: source, path: "same/path"}] = modelFile{body: []byte("new"), included: true}
			result := compareModelWorlds(before, after)
			if !maps.Equal(result.before.affected, modelSet{provider: true}) || !maps.Equal(result.after.affected, modelSet{provider: true}) {
				t.Fatalf("coordinates collapsed: before=%v after=%v", modelIDs(result.before.affected), modelIDs(result.after.affected))
			}
		}},
		{"lost-old-side", func(t *testing.T) {
			t.Helper()
			before := modelWorld{producers: map[modelID]modelProducer{provider: {}}}
			after, _ := applyModelOperations(before, []modelOperation{{kind: "delete-producer", producer: provider}, {kind: "producer", producer: consumer, definition: before.producers[provider]}})
			result := compareModelWorlds(before, after)
			if !maps.Equal(result.before.affected, modelSet{provider: true}) || !maps.Equal(result.after.affected, modelSet{consumer: true}) ||
				!result.before.deleted[provider] || !result.after.deleted[provider] {
				t.Fatalf("lost old side/tombstone: %+v", result)
			}
		}},
		{"negative-read", func(t *testing.T) {
			t.Helper()
			before := cloneModelWorld(world)
			delete(before.files, file)
			p := before.producers[provider]
			p.reads[0].optional = true
			before.producers[provider] = p
			after, _ := applyModelOperations(before, []modelOperation{{kind: "write", file: file, included: true}})
			result := compareModelWorlds(before, after)
			if !maps.Equal(result.before.affected, modelSet{provider: true, consumer: true}) || len(result.before.unavailable) != 0 {
				t.Fatalf("optional absence/appearance: %+v", result.before)
			}
		}},
		{"positive-and-negative-listings", func(t *testing.T) {
			t.Helper()
			before := modelWorld{files: make(map[modelPath]modelFile), producers: map[modelID]modelProducer{
				provider: {reads: []modelRead{{file: modelPath{source: source, path: "inputs"}, listing: true, optional: true}}},
			}}
			after, _ := applyModelOperations(before, []modelOperation{{kind: "write", file: file, body: []byte("a"), included: true}})
			if result := compareModelWorlds(before, after); !result.after.affected[provider] || len(result.before.unavailable) != 0 {
				t.Fatalf("negative listing ignored: %+v", result)
			}
			changed, _ := applyModelOperations(after, []modelOperation{{kind: "write", file: file, body: []byte("b"), included: true}})
			if result := compareModelWorlds(after, changed); len(result.after.affected) != 0 {
				t.Fatalf("listing consumed body bytes it did not read: %+v", result)
			}
		}},
		{"empty-directory-listing", func(t *testing.T) {
			t.Helper()
			directory := modelPath{source: source, path: "empty"}
			before := modelWorld{files: map[modelPath]modelFile{directory: {included: true, directory: true}},
				producers: map[modelID]modelProducer{provider: {reads: []modelRead{{file: directory, listing: true, optional: true}}}}}
			after := cloneModelWorld(before)
			delete(after.files, directory)
			result := compareModelWorlds(before, after)
			if !maps.Equal(result.before.affected, modelSet{provider: true}) || !maps.Equal(result.after.affected, modelSet{provider: true}) {
				t.Fatal("empty directory disappearance lost")
			}
			if len(result.before.unavailable)+len(result.after.unavailable) != 0 {
				t.Fatal("optional directory absence treated as unavailable")
			}
		}},
		{"inclusion-and-same-size-bytes", func(t *testing.T) {
			t.Helper()
			ignored := cloneModelWorld(world)
			ignored.files[file] = modelFile{body: []byte("old"), included: false}
			dirty, _ := applyModelOperations(ignored, []modelOperation{{kind: "write", file: file, body: []byte("new"), included: false}})
			if result := compareModelWorlds(ignored, dirty); len(result.after.affected) != 0 {
				t.Fatalf("ignored bytes selected consumers: %+v", result)
			}
			included, _ := applyModelOperations(dirty, []modelOperation{{kind: "include", file: file, included: true}})
			if result := compareModelWorlds(dirty, included); !result.after.affected[consumer] {
				t.Fatal("inclusion change omitted consumer")
			}
		}},
		{"missing-producer-and-required-input", func(t *testing.T) {
			t.Helper()
			before := cloneModelWorld(world)
			delete(before.producers, sourceProvider)
			delete(before.files, file)
			result := compareModelWorlds(before, before)
			if !maps.Equal(result.before.unavailable, modelSet{provider: true, consumer: true, sibling: true}) {
				t.Fatalf("unavailable=%v", modelIDs(result.before.unavailable))
			}
			state := evaluateModel(before).status[consumer]
			if !state.blockers[sourceProvider] || !state.blockers[provider] {
				t.Fatalf("missing blockers: %v", modelIDs(state.blockers))
			}
		}},
		{"deleted-object-identity", func(t *testing.T) {
			t.Helper()
			object := modelID{source: source, kind: "ConfigMap", namespace: "apps", name: "removed"}
			before := modelWorld{producers: map[modelID]modelProducer{provider: {documents: []modelDocument{{id: modelObjectID{modelID: object, version: "v1"}}}}}}
			after := modelWorld{producers: map[modelID]modelProducer{provider: {}}}
			if result := compareModelWorlds(before, after); !result.before.deleted[object] || !result.after.deleted[object] {
				t.Fatalf("object tombstone lost: %+v", result)
			}
		}},
		{"bounded-decoder", func(t *testing.T) {
			t.Helper()
			data := []byte{1, 0}
			for i := range 24 {
				data = append(data, 7, byte(i), byte(i))
			}
			for _, encoded := range [][]byte{nil, {1}, {1, 5, 2}, data} {
				decoded := decodeRenderModelCase(encoded)
				if err := checkModelBounds(decoded.modelCase); err != nil {
					t.Fatalf("input=%x: %v", encoded, err)
				}
				if !reflect.DeepEqual(decoded, decodeRenderModelCase(encoded)) {
					t.Fatal("truncated decoder is nondeterministic")
				}
				if decoded.unsupported["ResourceSet-frozen-semantics"] != 1 {
					t.Fatal("unsupported category disappeared")
				}
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.check(t)
		})
	}
}

func TestModelComparator_DeletedDocuments(t *testing.T) {
	t.Parallel()
	id := modelObjectID{kind: "ConfigMap", name: "same", version: "v1"}
	doc := modelDocument{id: id}
	for _, tc := range []struct {
		name          string
		before, after []modelDocument
		want          map[modelObjectID]int
	}{
		{"duplicate-removed", []modelDocument{doc, doc}, nil, map[modelObjectID]int{id: 2}},
		{"one-duplicate-retained", []modelDocument{doc, doc}, []modelDocument{doc}, map[modelObjectID]int{id: 1}},
		{"addition", []modelDocument{doc}, []modelDocument{doc, doc}, map[modelObjectID]int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := modelDeletedDocuments(tc.before, tc.after); !maps.Equal(got, tc.want) {
				t.Fatalf("deletion multiset: want=%v got=%v", tc.want, got)
			}
		})
	}
}

func TestModelComparator(t *testing.T) {
	t.Parallel()
	producer := modelID{source: modelSource{binding: "cluster", revision: "pinned", root: "repo"}, group: "kustomize.toolkit.fluxcd.io", kind: "Kustomization", namespace: "apps", name: "app"}
	object := modelObjectID{source: producer.source, kind: "ConfigMap", namespace: "apps", name: "data", version: "v1", namespacePresent: true}
	observation := func() modelObservation {
		return modelObservation{producers: map[modelID]modelStatus{producer: {terminal: true}},
			documents: []modelDocument{{id: object, raw: []byte("data: {value: 1, list: [a, b], date: '2024-01-01'}\n")}}}
	}
	tests := []struct {
		name string
		edit func(*modelObservation, *modelObservation)
		want string
	}{
		{"map-order", func(_, got *modelObservation) {
			got.documents[0].raw = []byte("data: {date: '2024-01-01', list: [a, b], value: 1}\n")
		}, ""},
		{"missing-producer", func(want, got *modelObservation) {
			want.producers[producer] = modelStatus{}
			delete(got.producers, producer)
		}, "producer membership"},
		{"extra-producer", func(_, got *modelObservation) {
			id := producer
			id.name = "extra"
			got.producers[id] = modelStatus{terminal: true}
		}, "producer membership"},
		{"missing-object", func(_, got *modelObservation) { got.documents = nil }, "object membership"},
		{"extra-object", func(_, got *modelObservation) {
			doc := got.documents[0]
			doc.id.name = "extra"
			got.documents = append(got.documents, doc)
		}, "object membership"},
		{"duplicate-deletion", func(want, _ *modelObservation) { want.documents = append(want.documents, want.documents[0]) }, "multiplicity"},
		{"duplicate-addition", func(_, got *modelObservation) { got.documents = append(got.documents, got.documents[0]) }, "multiplicity"},
		{"duplicate-substitution", func(want, got *modelObservation) {
			want.documents = append(want.documents, want.documents[0])
			doc := got.documents[0]
			doc.raw = []byte("data: {different: true}\n")
			got.documents = append(got.documents, doc)
		}, "document value"},
		{"source-coordinate", func(_, got *modelObservation) { doc := &got.documents[0]; doc.id.source.revision = "working" }, "membership"},
		{"group-coordinate", func(_, got *modelObservation) { got.documents[0].id.group = "other.example" }, "membership"},
		{"version", func(_, got *modelObservation) { got.documents[0].id.version = "v2" }, "membership"},
		{"namespace-presence", func(_, got *modelObservation) { got.documents[0].id.namespacePresent = false }, "membership"},
		{"scalar-type", func(_, got *modelObservation) {
			got.documents[0].raw = []byte("data: {value: '1', list: [a, b], date: '2024-01-01'}\n")
		}, "document value"},
		{"yaml-tag", func(want, got *modelObservation) {
			want.documents[0].raw = []byte("value: !!str 1\n")
			got.documents[0].raw = []byte("value: !!int 1\n")
		}, "document value"},
		{"sequence-order", func(_, got *modelObservation) {
			got.documents[0].raw = []byte("data: {value: 1, list: [b, a], date: '2024-01-01'}\n")
		}, "document value"},
		{"date-string-text", func(_, got *modelObservation) {
			got.documents[0].raw = []byte("data: {value: 1, list: [a, b], date: '2024-01-02'}\n")
		}, "document value"},
		{"date-string-tag", func(want, got *modelObservation) {
			want.documents[0].raw = []byte("date: '2024-01-01'\n")
			got.documents[0].raw = []byte("date: 2024-01-01\n")
		}, "document value"},
		{"typed-sdk-number", func(want, got *modelObservation) {
			want.documents[0] = modelDocument{id: object, sdk: map[string]any{"number": int64(1)}}
			got.documents[0] = modelDocument{id: object, sdk: map[string]any{"number": float64(1)}}
		}, "document value"},
		{"typed-sdk-date", func(want, got *modelObservation) {
			want.documents[0] = modelDocument{id: object, sdk: map[string]any{"date": "2024-01-01"}}
			got.documents[0] = modelDocument{id: object, sdk: map[string]any{"date": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}}
		}, "document value"},
		{"errors-with-partial-output", func(want, _ *modelObservation) { want.errorClass = "input" }, "error classification"},
		{"failure", func(want, _ *modelObservation) {
			want.producers[producer] = modelStatus{terminal: true, failure: "failed"}
		}, "producer state"},
		{"blocker", func(want, got *modelObservation) {
			want.producers[producer] = modelStatus{terminal: true, failure: "blocked", blockers: modelSet{object.modelID: true}}
			got.producers[producer] = modelStatus{terminal: true, failure: "blocked"}
		}, "producer state"},
		{"empty-producer-unfinished", func(want, got *modelObservation) {
			want.documents, got.documents = nil, nil
			got.producers[producer] = modelStatus{}
		}, "producer state"},
		{"deletions", func(want, _ *modelObservation) { want.deleted = map[modelObjectID]int{object: 1} }, "deleted identities"},
		{"malformed-YAML", func(_, got *modelObservation) { got.documents[0].raw = []byte("value: [") }, "decode YAML"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want, got := observation(), observation()
			tc.edit(&want, &got)
			err := compareModelObservation(want, got)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want diagnostic %q, got %v", tc.want, err)
			}
		})
	}
}
