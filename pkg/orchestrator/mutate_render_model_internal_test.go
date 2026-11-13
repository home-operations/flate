package orchestrator

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/home-operations/flate/internal/assert"

	gogit "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func mutateRenderModel(f *renderModelCase, opcode, target, value byte) {
	ids := modelIDs(modelProducerIDs(f.after))
	ids = slices.DeleteFunc(ids, func(id modelID) bool {
		p := f.after.producers[id]
		kind := opcode % 9
		chartEdit := id.kind == "HelmRelease" && (kind == 0 || kind == 1 || kind == 2 || kind == 7 || kind == 8)
		if id.kind != "Kustomization" && !chartEdit || id.source != renderModelSource(f.after) || p.emitter != nil ||
			len(p.reads) == 0 {
			return true
		}
		for _, consumer := range f.after.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
			if slices.Contains(consumer.content, id) || slices.Contains(consumer.ordering, id) ||
				consumer.emitter != nil && *consumer.emitter == id {
				return true
			}
		}
		return false
	})
	if len(ids) == 0 {
		return
	}
	id := ids[int(target)%len(ids)]
	p := f.after.producers[id]
	declarationFile := modelPath{source: id.source, path: "flux/" + id.name + "-" + id.namespace + ".yaml"}
	input := p.reads[0].file
	if returned := mutateRenderModelSegment(
		&mutateRenderModelSegmentInputs{
			f:               &f,
			opcode:          &opcode,
			value:           &value,
			id:              &id,
			p:               &p,
			declarationFile: &declarationFile,
			input:           &input,
		},
	); returned {
		return
	}
}

func renderModelStaticRoots(w, other modelWorld) []modelDocument {
	packages := make(map[modelPath][]modelDocument)
	owned := make(map[modelPath]bool)
	for id, p := range other.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		if id.kind == "Kustomization" && len(p.reads) > 0 {
			packages[p.reads[0].file] = p.documents
		}
	}
	for id, p := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		if id.kind == "Kustomization" && len(p.reads) > 0 {
			owned[p.reads[0].file] = true
		}
	}
	var documents []modelDocument
	for _, input := range slices.SortedFunc(maps.Keys(packages), compareModelPath) {
		if !owned[input] && w.files[input].included {
			documents = append(documents, packages[input]...)
		}
	}
	return documents
}

func writeRenderModelWorld(t *testing.T, root string, w modelWorld) {
	t.Helper()
	keys := slices.Collect(maps.Keys(w.files))
	slices.SortFunc(keys, compareModelPath)
	for _, file := range keys {
		if file.source != renderModelSource(w) {
			continue
		}
		content := w.files[file]
		if content.directory {
			{
				err := os.MkdirAll(filepath.Join(root, file.path), 0o750)
				assert.FatalIf(t, err != nil, "%v", err)
			}
			if !slices.ContainsFunc(keys, func(candidate modelPath) bool {
				return candidate.source == file.source && strings.HasPrefix(candidate.path, file.path+"/")
			}) {
				testutil.WriteFile(t, root, file.path+"/.keep", "")
			}
			continue
		}
		testutil.WriteFile(t, root, file.path, string(content.body))
	}
}

func namedRenderModelID(id *modelID) manifest.NamedResource {
	return manifest.NamedResource{Group: id.group, Kind: id.kind, Namespace: id.namespace, Name: id.name}
}

func actualRenderModelID(w modelWorld, side Rendered, id manifest.NamedResource) modelID {
	source := renderModelSource(w)
	binding := ""
	if obj := side.store.GetObject(id); obj != nil {
		if consumer, ok := obj.(interface{ SourceID() manifest.NamedResource }); ok {
			binding = consumer.SourceID().Name
		} else if id.Kind == "GitRepository" {
			binding = id.Name
		}
	}
	if binding != "" {
		source.binding = binding
		for candidate := range w.files {
			if candidate.source.binding == binding {
				source = candidate.source
				break
			}
		}
	}
	return modelID{source: source, group: id.Group, kind: id.Kind, namespace: id.Namespace, name: id.Name}
}

func renderModelDocument(source modelSource, doc map[string]any) modelDocument {
	metadata, _ := doc["metadata"].(map[string]any)
	name, _ := metadata["name"].(string)
	namespace, nsPresent := metadata["namespace"].(string)
	api, _ := doc["apiVersion"].(string)
	group, version, qualified := strings.Cut(api, "/")
	if !qualified {
		group, version = "", api
	}
	kind, _ := doc["kind"].(string)
	return modelDocument{
		id: modelObjectID{
			modelID:          modelID{source: source, group: group, kind: kind, namespace: namespace, name: name},
			version:          version,
			namespacePresent: nsPresent,
		},
		sdk: doc,
	}
}

func renderModelUniverse(w modelWorld) (modelSet, error) {
	ids := modelProducerIDs(w)
	{
		docValues := w.static
		for docIndex := range docValues {
			doc := docValues[docIndex]

			ids[doc.id.modelID] = true
		}
	}
	for _, p := range w.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		{
			docValues2 := p.documents
			for docIndex2 := range docValues2 {
				doc := docValues2[docIndex2]

				ids[doc.id.modelID] = true
			}
		}
	}
	if returned, result0, result1 := renderModelUniverseSegment(&w, &ids); returned {
		return result0, result1
	}
	return ids, nil
}

func observeRenderModelEntry(w modelWorld, side Rendered, selected bool) error {
	id := side.entryBinding.Producer
	if !id.ReportHidden() {
		return nil
	}
	want, got := modelObservation{}, modelObservation{}
	for _, batch := range side.Result.Manifests {
		if batch.Parent != nil && batch.Parent.Named() == id {
			for _, doc := range batch.Documents {
				got.documents = append(got.documents, renderModelDocument(renderModelSource(w), doc))
			}
		}
	}
	if selected && !side.selection.Snapshot().Reconcile(id) {
		if side.store.GetArtifact(id, store.ArtifactContent) != nil || len(got.documents) != 0 {
			return errors.New("unselected entry published output")
		}
		return nil
	}
	status, ok := side.store.GetStatus(id)
	emission, complete := side.store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	if !ok || status.Outcome() != store.OutcomePassed || !complete || !emission.Complete {
		return fmt.Errorf(
			"entry lacks successful complete emission: status=%+v artifact=%T",
			status,
			side.store.GetArtifact(id, store.ArtifactContent),
		)
	}
	prefix := strings.TrimSuffix(side.entryBinding.Path, "/") + "/"
	if returned, result0 := observeRenderModelEntrySegment(&w, &want, &prefix); returned {
		return result0
	}
	for i := range want.documents {
		want.documents[i].sdk = nil
	}
	for i := range got.documents {
		got.documents[i].sdk = nil
	}
	if err := compareModelObservation(want, got); err != nil {
		return fmt.Errorf("literal entry declaration output: %w", err)
	}
	return nil
}

func observeRenderModel(w modelWorld, side Rendered, selected bool) (modelObservation, modelSet, error) {
	out := modelObservation{producers: make(map[modelID]modelStatus)}
	admitted := make(modelSet)
	if side.Orchestrator == nil || side.Result == nil {
		return out, admitted, errors.Join(errors.New("missing render side"), side.Err)
	}
	side.tasks.BlockTillDone()
	if failures := side.tasks.Failures(); failures != 0 {
		return out, admitted, fmt.Errorf("task panics: %d", failures)
	}
	if side.Err != nil {
		if !errors.Is(side.Err, manifest.ErrFlux) {
			return out, admitted, fmt.Errorf("unclassified side error: %w", side.Err)
		}
		out.errorClass = "flux"
	}
	if err := observeRenderModelEntry(w, side, selected); err != nil {
		return out, admitted, err
	}
	universe, err := renderModelUniverse(w)
	if err != nil {
		return out, admitted, err
	}
	if returned, result0, result1, result2 := observeRenderModelSegment(
		&observeRenderModelSegmentInputs{
			w:        &w,
			side:     &side,
			selected: &selected,
			out:      &out,
			admitted: &admitted,
			universe: &universe,
		},
	); returned {
		return result0, result1, result2
	}
	for _, batch := range side.Result.Manifests {
		if batch.Parent != nil && batch.Parent.Named() == side.entryBinding.Producer &&
			batch.Parent.Named().ReportHidden() {
			continue
		}
		source := renderModelSource(w)
		if batch.Parent != nil {
			key := actualRenderModelID(w, side, batch.Parent.Named())
			if _, ok := out.producers[key]; !ok {
				return out, admitted, fmt.Errorf("output from unselected or unknown producer: %s", key)
			}
			source = key.source
		}
		for _, doc := range batch.Documents {
			out.documents = append(out.documents, renderModelDocument(source, doc))
		}
	}
	return out, admitted, nil
}

func expectedRenderModel(w modelWorld, selection *modelSelection) modelObservation {
	evaluation := evaluateModel(w)
	out := modelObservation{producers: make(map[modelID]modelStatus)}
	ids := slices.Collect(maps.Keys(evaluation.status))
	slices.SortFunc(ids, compareModelID)
	for _, id := range ids {
		if selection != nil && !selection.affected[id] && !selection.support[id] {
			continue
		}
		state := evaluation.status[id]
		out.producers[id] = state
		if state.failure != "" && state.failure != "suspended" {
			out.errorClass = "flux"
		}
		if state.failure == "" {
			out.documents = append(out.documents, w.producers[id].documents...)
		}
	}
	return out
}

func checkRenderModelMembership(want, got modelObservation) error {
	for i := range got.documents {
		if !slices.ContainsFunc(
			want.documents,
			func(d modelDocument) bool { return d.id == got.documents[i].id && d.sdk != nil },
		) {
			got.documents[i].sdk = nil
		}
	}
	return compareModelObservation(want, got)
}

func projectRenderModel(full modelObservation, selection modelSelection, w modelWorld) modelObservation {
	out := modelObservation{producers: make(map[modelID]modelStatus)}
	for id, state := range full.producers {
		if selection.affected[id] || selection.support[id] {
			out.producers[id] = state
			if state.failure != "" && state.failure != "suspended" {
				out.errorClass = "flux"
			}
		}
	}
	{
		docValues3 := full.documents
		for docIndex3 := range docValues3 {
			doc := docValues3[docIndex3]

			for id := range out.producers {
				if slices.ContainsFunc(
					w.producers[id].documents,
					func(d modelDocument) bool { return d.id == doc.id },
				) {
					out.documents = append(out.documents, doc)
					break
				}
			}
		}
	}
	return out
}

type modelSources struct {
	before modelWorld
	after  modelWorld
}

func materializeRenderModelSources(t *testing.T, fixture *renderModelCase) modelSources {
	t.Helper()
	before, after := cloneModelWorld(fixture.before), cloneModelWorld(fixture.after)
	primary := renderModelSource(before)
	sources := map[modelSource]bool{}
	for _, world := range []modelWorld{before, after} {
		for file := range world.files {
			if file.source != primary {
				sources[file.source] = true
			}
		}
	}
	materializeRenderModelSourcesSegment(
		t,
		&materializeRenderModelSourcesSegmentInputs{
			before:  &before,
			after:   &after,
			primary: &primary,
			sources: &sources,
		},
	)
	return modelSources{before: before, after: after}
}

type materializeRenderModelSourcesSegmentInputs struct {
	before  *modelWorld
	after   *modelWorld
	primary *modelSource
	sources *map[modelSource]bool
}

func materializeRenderModelSourcesSegment(t *testing.T, statementInput *materializeRenderModelSourcesSegmentInputs) {
	t.Helper()
	for coordinate := range *statementInput.sources {
		if coordinate.revision == "working" {
			continue
		}
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		var previous map[string]modelFile
		var revision string
		for _, world := range []modelWorld{(*statementInput.before), (*statementInput.after)} {
			files := sourceModelFiles(world, coordinate)
			if !reflect.DeepEqual(previous, files) {
				wt, err := repo.Worktree()
				assert.FatalIf(t, err != nil, "%v", err)
				removeAbsentSourceModelFiles(t, wt, previous, files)
				for name, file := range files {
					testutil.WriteFile(t, root, name, string(file.body))
				}
				revision = testutil.GitCommitAll(t, repo, "source snapshot", &gogit.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}}).
					String()
				previous = files
			}
			file := modelPath{source: (*statementInput.primary), path: "flux/source-" + coordinate.binding + ".yaml"}
			declaration := world.files[file]
			declaration.body = []byte(
				fmt.Sprintf(
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: %s, namespace: flux-system}\nspec: {url: 'file://%s', ref: {commit: %s}}\n",
					coordinate.binding,
					filepath.ToSlash(root),
					revision,
				),
			)
			world.files[file] = declaration
		}
		testutil.WriteFile(
			t,
			root,
			"apps/app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: dirty-decoy}\ndata: {value: dirty}\n",
		)
	}
}

type observeRenderModelSegmentInputs struct {
	w        *modelWorld
	side     *Rendered
	selected *bool
	out      *modelObservation
	admitted *modelSet
	universe *modelSet
}

func observeRenderModelSegment(
	statementInput *observeRenderModelSegmentInputs,
) (bool, modelObservation, modelSet, error) {
	for _, obj := range statementInput.side.store.ListObjects("", "") {
		id := obj.Named()
		if id == manifest.BootstrapSourceID || id == statementInput.side.entryBinding.Producer && id.ReportHidden() {
			continue
		}
		key := actualRenderModelID((*statementInput.w), (*statementInput.side), id)
		known := statementInput.universe.containsNamed(id)
		if !known {
			return true, (*statementInput.out), (*statementInput.admitted), fmt.Errorf(
				"unexpected store identity: %s",
				key,
			)
		}
		if statementInput.side.store.ReportHidden(id) || !id.KindInfo().Runnable {
			continue
		}
		if (*statementInput.selected) && !statementInput.side.selection.Snapshot().Reconcile(id) {
			if err := checkUnselectedModelArtifact(statementInput.side, id, &key); err != nil {
				return true, (*statementInput.out), (*statementInput.admitted), err
			}
			continue
		}
		(*statementInput.admitted)[key] = true
		state, err := observeModelProducerStatus(statementInput, obj, id, &key)
		if err != nil {
			return true, (*statementInput.out), (*statementInput.admitted), err
		}
		statementInput.out.producers[key] = state
	}
	return false, modelObservation{}, modelSet(nil), error(nil)
}

func observeRenderModelEntrySegment(w *modelWorld, want *modelObservation, prefix *string) (bool, error) {
	for file, content := range w.files {
		if file.source != renderModelSource((*w)) || !content.included || content.directory ||
			!strings.HasPrefix(file.path, (*prefix)) ||
			!strings.HasSuffix(file.path, ".yaml") {
			continue
		}
		decoder := yaml.NewDecoder(bytes.NewReader(content.body))
		for {
			var doc map[string]any
			if err := decoder.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return true, fmt.Errorf("model entry input %s: %w", file.path, err)
			}
			if doc["kind"] != nil {
				want.documents = append(want.documents, renderModelDocument(file.source, doc))
			}
		}
	}
	return false, error(nil)
}

func renderModelUniverseSegment(w *modelWorld, ids *modelSet) (bool, modelSet, error) {
	for file, content := range w.files {
		if !content.included || !strings.HasPrefix(file.path, "flux/") || !strings.HasSuffix(file.path, ".yaml") {
			continue
		}
		decoder := yaml.NewDecoder(bytes.NewReader(content.body))
		for {
			var doc map[string]any
			if err := decoder.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return true, nil, fmt.Errorf("model declaration %s: %w", file.path, err)
			}
			if doc["kind"] != nil {
				id := renderModelDocument(file.source, doc).id.modelID
				(*ids)[id] = true
				includeInlineModelChart(&id, doc, (*ids))
			}
		}
	}
	return false, modelSet(nil), error(nil)
}

type mutateRenderModelSegmentInputs struct {
	f               **renderModelCase
	opcode          *byte
	value           *byte
	id              *modelID
	p               *modelProducer
	declarationFile *modelPath
	input           *modelPath
}

func mutateRenderModelSegment(statementInput *mutateRenderModelSegmentInputs) bool {
	switch (*statementInput.opcode) % 9 {
	case 0:
		for _, read := range statementInput.p.reads {
			file, ok := (*statementInput.f).after.files[read.file]
			if !ok || !utf8.Valid(file.body) ||
				!bytes.Contains(file.body, []byte("old")) && !bytes.Contains(file.body, []byte("new")) {
				continue
			}
			body := bytes.ReplaceAll(file.body, []byte("old"), fmt.Appendf(nil, "v%02x", (*statementInput.value)))
			body = bytes.ReplaceAll(body, []byte("new"), fmt.Appendf(nil, "v%02x", (*statementInput.value)))
			writeRenderModelFile((*statementInput.f), read.file, body)
			break
		}
	case 1:
		body := append(
			slices.Clone((*statementInput.f).after.files[(*statementInput.input)].body),
			fmt.Appendf(nil, "# input-%02x\n", (*statementInput.value))...)
		if len(body) <= 4096 {
			writeRenderModelFile((*statementInput.f), (*statementInput.input), body)
		}
	case 2:
		for i, read := range statementInput.p.reads[1:] {
			file, ok := (*statementInput.f).after.files[read.file]
			if !ok || path.Base(read.file.path) != "cm.yaml" {
				continue
			}
			next := read.file
			next.path = path.Join(path.Dir(next.path), fmt.Sprintf("renamed-%02x.yaml", (*statementInput.value)))
			delete((*statementInput.f).after.files, read.file)
			file.included = renderModelFileIncluded((*statementInput.f).after, next)
			(*statementInput.f).after.files[next] = file
			statementInput.p.reads[i+1].file = next
			(*statementInput.f).after.producers[(*statementInput.id)] = (*statementInput.p)
			writeRenderModelFile(
				(*statementInput.f),
				(*statementInput.input),
				bytes.ReplaceAll(
					(*statementInput.f).after.files[(*statementInput.input)].body,
					[]byte("cm.yaml"),
					[]byte(path.Base(next.path)),
				),
			)
			break
		}
	case 3, 4:
		return statementInput.renameProducer()
	case 5:
		return statementInput.moveInputDirectory()
	case 6:
		statementInput.p.suspended = true
		if !bytes.Contains(statementInput.p.declaration, []byte("suspend: true")) {
			statementInput.p.declaration = bytes.Replace(
				statementInput.p.declaration,
				[]byte("  path:"),
				[]byte("  suspend: true\n  path:"),
				1,
			)
		}
		(*statementInput.f).after.producers[(*statementInput.id)] = (*statementInput.p)
		writeRenderModelFile(
			(*statementInput.f),
			(*statementInput.declarationFile),
			slices.Clone(statementInput.p.declaration),
		)
	case 7:
		file := modelPath{
			source: statementInput.id.source,
			path:   fmt.Sprintf("ignored-%02x.txt", (*statementInput.value)),
		}
		if len((*statementInput.f).after.files) < 48 {
			writeRenderModelFile((*statementInput.f), file, []byte("unconsumed\n"))
		}
	case 8:
		delete((*statementInput.f).after.files, (*statementInput.declarationFile))
		delete((*statementInput.f).after.producers, (*statementInput.id))
	}
	return false
}

func sourceModelFiles(world modelWorld, coordinate modelSource) map[string]modelFile {
	files := map[string]modelFile{}
	for file, content := range world.files {
		if file.source == coordinate {
			files[file.path] = content
		}
	}
	return files
}

func removeAbsentSourceModelFiles(t *testing.T, wt *gogit.Worktree, previous, files map[string]modelFile) {
	t.Helper()
	for name := range previous {
		if _, exists := files[name]; !exists {
			{
				_, err := wt.Remove(name)
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
	}
}

func includeInlineModelChart(id *modelID, doc map[string]any, ids modelSet) {
	if id.group == "helm.toolkit.fluxcd.io" && id.kind == "HelmRelease" {
		spec, _ := doc["spec"].(map[string]any)
		chart, _ := spec["chart"].(map[string]any)
		template, _ := chart["spec"].(map[string]any)
		ref, _ := template["sourceRef"].(map[string]any)
		namespace, _ := ref["namespace"].(string)
		if namespace == "" {
			namespace = id.namespace
		}
		ids[modelID{source: id.source, group: "source.toolkit.fluxcd.io", kind: "HelmChart", namespace: namespace, name: id.namespace + "-" + id.name}] = true
	}
}

func (s modelSet) containsNamed(id manifest.NamedResource) bool {
	for expected := range s {
		if namedRenderModelID(&expected) == id {
			return true
		}
	}
	return false
}

func checkUnselectedModelArtifact(side *Rendered, id manifest.NamedResource, key *modelID) error {
	if artifact := side.store.GetArtifact(id, store.ArtifactContent); artifact != nil {
		if _, source := artifact.(*store.SourceArtifact); !source {
			return fmt.Errorf(
				"unselected producer has artifact: %s",
				*key,
			)
		}
	}
	return nil
}

func observeModelProducerStatus(
	statementInput *observeRenderModelSegmentInputs,
	obj manifest.BaseManifest,
	id manifest.NamedResource,
	key *modelID,
) (modelStatus, error) {
	info, ok := statementInput.side.store.GetStatus(id)
	state := modelStatus{terminal: ok && info.Status != store.StatusPending, blockers: make(modelSet)}
	switch info.Outcome() {
	case store.OutcomePending, store.OutcomePassed, store.OutcomeSkipped, store.OutcomeUnchanged:
	case store.OutcomeFailed:
		state.failure = "missing-input"
	case store.OutcomeBlocked:
		state.failure = "blocked"
	case store.OutcomeSuspended:
		state.failure = "suspended"
	}
	for _, blocker := range statementInput.side.Result.Blocked[id] {
		state.blockers[actualRenderModelID((*statementInput.w), (*statementInput.side), blocker)] = true
	}
	if state.failure == "" && !obj.Suspended() {
		artifact := statementInput.side.store.GetArtifact(id, store.ArtifactContent)
		if artifact == nil || info.Outcome() != store.OutcomePassed {
			return modelStatus{}, fmt.Errorf(
				"admitted producer lacks terminal artifact: %s status=%+v artifact=%T",
				*key,
				info,
				artifact,
			)
		}
		if emission, ok := artifact.(*store.Emission); ok && !emission.Complete {
			return modelStatus{}, fmt.Errorf(
				"producer inventory incomplete: %s",
				*key,
			)
		}
	}
	return state, nil
}

func (s *mutateRenderModelSegmentInputs) renameProducer() bool {
	next := (*s.id)
	if (*s.opcode)%9 == 3 {
		next.name = fmt.Sprintf("renamed-%02x", (*s.value))
	} else {
		next.namespace = []string{"flux-system", "ns-1", "ns-2"}[(*s.value)%3]
	}
	if next == (*s.id) || (*s.f).after.producers[next].declaration != nil {
		return true
	}
	for _, dep := range s.p.ordering {
		s.p.declaration = bytes.ReplaceAll(
			s.p.declaration,
			[]byte("[{name: "+dep.name+"}]"),
			[]byte("[{name: "+dep.name+", namespace: "+dep.namespace+"}]"),
		)
	}
	s.p.declaration = bytes.Replace(
		s.p.declaration,
		[]byte("  name: "+s.id.name+"\n"),
		[]byte("  name: "+next.name+"\n"),
		1,
	)
	s.p.declaration = bytes.Replace(
		s.p.declaration,
		[]byte("  namespace: "+s.id.namespace+"\n"),
		[]byte("  namespace: "+next.namespace+"\n"),
		1,
	)
	for i, dep := range s.p.content {
		if dep.namespace == s.id.namespace && (dep.kind == "ConfigMap" || dep.kind == "Secret") {
			s.p.content[i].namespace = next.namespace
		}
	}
	delete((*s.f).after.files, (*s.declarationFile))
	(*s.f).after.producers[(*s.id)] = (*s.p)
	rekeyRenderModel((*s.f).after, (*s.id), next)
	writeRenderModelFile(
		(*s.f),
		modelPath{source: s.id.source, path: "flux/" + next.name + "-" + next.namespace + ".yaml"},
		slices.Clone(s.p.declaration),
	)
	for other, dependency := range (*s.f).after.producers { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
		if !slices.Contains(dependency.ordering, next) {
			continue
		}
		dependency.declaration = bytes.ReplaceAll(
			dependency.declaration,
			[]byte("[{name: "+s.id.name+"}]"),
			[]byte("[{name: "+next.name+", namespace: "+next.namespace+"}]"),
		)
		(*s.f).after.producers[other] = dependency
		writeRenderModelFile(
			(*s.f),
			modelPath{source: other.source, path: "flux/" + other.name + "-" + other.namespace + ".yaml"},
			slices.Clone(dependency.declaration),
		)
	}
	return false
}

func (s *mutateRenderModelSegmentInputs) moveInputDirectory() bool {
	if len(s.p.reads) != 2 || path.Base(s.p.reads[1].file.path) != "cm.yaml" {
		(*s.f).unsupported["path-move-with-external-reads"]++
		return true
	}
	directory := path.Dir(s.input.path)
	nextDirectory := fmt.Sprintf("moved-%02x", (*s.value))
	for file, content := range maps.Clone((*s.f).after.files) {
		if file.source != s.id.source || !strings.HasPrefix(file.path, directory+"/") {
			continue
		}
		next := file
		next.path = nextDirectory + strings.TrimPrefix(file.path, directory)
		delete((*s.f).after.files, file)
		content.included = renderModelFileIncluded((*s.f).after, next)
		(*s.f).after.files[next] = content
		for i := range s.p.reads {
			if s.p.reads[i].file == file {
				s.p.reads[i].file = next
			}
		}
	}
	s.p.declaration = bytes.Replace(
		s.p.declaration,
		[]byte("path: ./"+directory),
		[]byte("path: ./"+nextDirectory),
		1,
	)
	(*s.f).after.producers[(*s.id)] = (*s.p)
	writeRenderModelFile(
		(*s.f),
		(*s.declarationFile),
		slices.Clone(s.p.declaration),
	)
	return false
}
