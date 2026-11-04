package orchestrator

import (
	"bytes"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

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
			testIndependentModelWitnessesCase(t, &tc)
		},
		)
	}
}

func TestIndependentModel(t *testing.T) {
	t.Parallel()
	source := modelSource{binding: "cluster", revision: "working", root: "repo"}
	id := func(name string) modelID {
		return modelID{
			source:    source,
			group:     "kustomize.toolkit.fluxcd.io",
			kind:      "Kustomization",
			namespace: "apps",
			name:      name,
		}
	}
	provider, consumer, parent, sibling, sourceProvider := id(
		"provider",
	), id(
		"consumer",
	), id(
		"parent",
	), id(
		"sibling",
	), id(
		"source",
	)
	file := modelPath{source: source, path: "inputs/data"}
	world := modelWorld{
		files: map[modelPath]modelFile{file: {body: []byte("old"), included: true}},
		producers: map[modelID]modelProducer{
			provider:       {reads: []modelRead{{file: file}}},
			consumer:       {content: []modelID{provider}, sources: []modelID{sourceProvider}, emitter: &parent},
			parent:         {},
			sourceProvider: {},
			sibling:        {ordering: []modelID{provider}, emitter: &parent},
		},
	}
	tests := []struct {
		name  string
		check func(*testing.T)
	}{
		{"content-and-support-without-siblings", func(t *testing.T) {
			t.Helper()
			after, _ := applyModelOperations(
				world,
				[]modelOperation{{kind: "write", file: file, body: []byte("new"), included: true}},
			)
			result := compareModelWorlds(world, after)
			for _, side := range []modelSelection{result.before, result.after} {
				if !maps.Equal(side.affected, modelSet{provider: true, consumer: true}) ||
					!maps.Equal(side.support, modelSet{parent: true, sourceProvider: true}) {
					t.Fatalf("affected=%v support=%v", modelIDs(side.affected), modelIDs(side.support))
				}
				{
					assert.FatalIf(
						t,
						side.affected[sibling] || side.support[sibling],
						"%v",
						"support admitted unchanged sibling",
					)
				}
			}
			{
				assert.FatalIf(t, string(world.files[file].body) != "old", "%v", "mutation changed the before world")
			}
		}},
		{"omitted-consumer", func(t *testing.T) {
			t.Helper()
			want := modelSelection{affected: modelSet{provider: true, consumer: true}}
			{
				{
					err := compareModelSelection(
						want,
						modelSet{provider: true},
					)
					assert.FatalIf(t, err == nil ||
						!strings.Contains(err.Error(), "missing="), "omitted consumer accepted: %v", err)
				}
			}
		}},
		{"extra-consumer", func(t *testing.T) {
			t.Helper()
			want := modelSelection{affected: modelSet{provider: true}}
			{
				{
					err := compareModelSelection(
						want,
						modelSet{provider: true, sibling: true},
					)
					assert.FatalIf(t, err == nil ||
						!strings.Contains(err.Error(), "unexpected="), "extra consumer accepted: %v", err)
				}
			}
		}},
		{"individually-declared-uncertainty", func(t *testing.T) {
			t.Helper()
			want := modelSelection{
				affected: modelSet{provider: true},
				allowed:  map[modelID]string{sibling: "shared unclassified input"},
			}
			{
				{
					err := compareModelSelection(want, modelSet{provider: true, sibling: true})
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
			{
				{
					err := compareModelSelection(want, modelSet{provider: true, parent: true})
					assert.FatalIf(t, err == nil, "%v", "uncertainty admitted an undeclared producer")
				}
			}
			want.allowed[sibling] = ""
			{
				{
					err := compareModelSelection(want, modelSet{provider: true, sibling: true})
					assert.FatalIf(t, err == nil, "%v", "uncertainty without a reason accepted")
				}
			}
		}},
		{"source-and-group-collision", func(t *testing.T) {
			t.Helper()
			testIndependentModelCoordinates(t, source, new(provider))
		}},
		{"lost-old-side", func(t *testing.T) {
			t.Helper()
			before := modelWorld{producers: map[modelID]modelProducer{provider: {}}}
			after, _ := applyModelOperations(
				before,
				[]modelOperation{
					{kind: "delete-producer", producer: provider},
					{kind: "producer", producer: consumer, definition: before.producers[provider]},
				},
			)
			result := compareModelWorlds(before, after)
			{
				assert.FatalIf(t, !maps.Equal(result.before.affected, modelSet{provider: true}) ||
					!maps.Equal(result.after.affected, modelSet{consumer: true}) ||
					!result.before.deleted[provider] ||
					!result.after.deleted[provider], "lost old side/tombstone: %+v", result)
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
			{
				assert.FatalIf(t, !maps.Equal(result.before.affected, modelSet{provider: true, consumer: true}) ||
					len(result.before.unavailable) != 0, "optional absence/appearance: %+v", result.before)
			}
		}},
		{"positive-and-negative-listings", func(t *testing.T) {
			t.Helper()
			before := modelWorld{files: make(map[modelPath]modelFile), producers: map[modelID]modelProducer{
				provider: {
					reads: []modelRead{
						{file: modelPath{source: source, path: "inputs"}, listing: true, optional: true},
					},
				},
			}}
			after, _ := applyModelOperations(
				before,
				[]modelOperation{{kind: "write", file: file, body: []byte("a"), included: true}},
			)
			{
				{
					result := compareModelWorlds(
						before,
						after,
					)
					assert.FatalIf(t, !result.after.affected[provider] ||
						len(result.before.unavailable) != 0, "negative listing ignored: %+v", result)
				}
			}
			changed, _ := applyModelOperations(
				after,
				[]modelOperation{{kind: "write", file: file, body: []byte("b"), included: true}},
			)
			{
				{
					result := compareModelWorlds(after, changed)
					assert.FatalIf(
						t,
						len(result.after.affected) != 0,
						"listing consumed body bytes it did not read: %+v",
						result,
					)
				}
			}
		}},
		{"empty-directory-listing", func(t *testing.T) {
			t.Helper()
			directory := modelPath{source: source, path: "empty"}
			before := modelWorld{
				files: map[modelPath]modelFile{directory: {included: true, directory: true}},
				producers: map[modelID]modelProducer{
					provider: {reads: []modelRead{{file: directory, listing: true, optional: true}}},
				},
			}
			after := cloneModelWorld(before)
			delete(after.files, directory)
			result := compareModelWorlds(before, after)
			{
				assert.FatalIf(t, !maps.Equal(result.before.affected, modelSet{provider: true}) ||
					!maps.Equal(
						result.after.affected,
						modelSet{provider: true},
					), "%v", "empty directory disappearance lost")
			}
			{
				assert.FatalIf(
					t,
					len(result.before.unavailable)+len(result.after.unavailable) != 0,
					"%v",
					"optional directory absence treated as unavailable",
				)
			}
		}},
		{"inclusion-and-same-size-bytes", func(t *testing.T) {
			t.Helper()
			ignored := cloneModelWorld(world)
			ignored.files[file] = modelFile{body: []byte("old"), included: false}
			dirty, _ := applyModelOperations(
				ignored,
				[]modelOperation{{kind: "write", file: file, body: []byte("new"), included: false}},
			)
			{
				{
					result := compareModelWorlds(ignored, dirty)
					assert.FatalIf(t, len(result.after.affected) != 0, "ignored bytes selected consumers: %+v", result)
				}
			}
			included, _ := applyModelOperations(dirty, []modelOperation{{kind: "include", file: file, included: true}})
			{
				{
					result := compareModelWorlds(dirty, included)
					assert.FatalIf(t, !result.after.affected[consumer], "%v", "inclusion change omitted consumer")
				}
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
			before := modelWorld{
				producers: map[modelID]modelProducer{
					provider: {documents: []modelDocument{{id: modelObjectID{modelID: object, version: "v1"}}}},
				},
			}
			after := modelWorld{producers: map[modelID]modelProducer{provider: {}}}
			{
				{
					result := compareModelWorlds(
						before,
						after,
					)
					assert.FatalIf(t, !result.before.deleted[object] ||
						!result.after.deleted[object], "object tombstone lost: %+v", result)
				}
			}
		}},
		{"bounded-decoder", func(t *testing.T) {
			t.Helper()
			testIndependentModelDecoderBounds(t)
		}},
	}
	for _, tc := range tests {
		t.Run(
			tc.name,
			func(t *testing.T) {
				t.Parallel()
				testIndependentModelCase2(t, testIndependentModelCase2Inputs{tc: &tc})
			},
		)
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
			{
				got := modelDeletedDocuments(tc.before, tc.after)
				assert.FatalIf(t, !maps.Equal(got, tc.want), "deletion multiset: want=%v got=%v", tc.want, got)
			}
		})
	}
}

func TestModelComparator(t *testing.T) {
	t.Parallel()
	producer := modelID{
		source:    modelSource{binding: "cluster", revision: "pinned", root: "repo"},
		group:     "kustomize.toolkit.fluxcd.io",
		kind:      "Kustomization",
		namespace: "apps",
		name:      "app",
	}
	object := modelObjectID{
		source:           producer.source,
		kind:             "ConfigMap",
		namespace:        "apps",
		name:             "data",
		version:          "v1",
		namespacePresent: true,
	}
	observation := func() modelObservation {
		return modelObservation{
			producers: map[modelID]modelStatus{producer: {terminal: true}},
			documents: []modelDocument{
				{id: object, raw: []byte("data: {value: 1, list: [a, b], date: '2024-01-01'}\n")},
			},
		}
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
		{
			"duplicate-deletion",
			func(want, _ *modelObservation) { want.documents = append(want.documents, want.documents[0]) },
			"multiplicity",
		},
		{
			"duplicate-addition",
			func(_, got *modelObservation) { got.documents = append(got.documents, got.documents[0]) },
			"multiplicity",
		},
		{"duplicate-substitution", func(want, got *modelObservation) {
			want.documents = append(want.documents, want.documents[0])
			doc := got.documents[0]
			doc.raw = []byte("data: {different: true}\n")
			got.documents = append(got.documents, doc)
		}, "document value"},
		{
			"source-coordinate",
			func(_, got *modelObservation) { doc := &got.documents[0]; doc.id.source.revision = "working" },
			"membership",
		},
		{
			"group-coordinate",
			func(_, got *modelObservation) { got.documents[0].id.group = "other.example" },
			"membership",
		},
		{"version", func(_, got *modelObservation) { got.documents[0].id.version = "v2" }, "membership"},
		{
			"namespace-presence",
			func(_, got *modelObservation) { got.documents[0].id.namespacePresent = false },
			"membership",
		},
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
			got.documents[0] = modelDocument{
				id:  object,
				sdk: map[string]any{"date": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			}
		}, "document value"},
		{
			"errors-with-partial-output",
			func(want, _ *modelObservation) { want.errorClass = "input" },
			"error classification",
		},
		{"failure", func(want, _ *modelObservation) {
			want.producers[producer] = modelStatus{terminal: true, failure: "failed"}
		}, "producer state"},
		{"blocker", func(want, got *modelObservation) {
			want.producers[producer] = modelStatus{
				terminal: true,
				failure:  "blocked",
				blockers: modelSet{object.modelID: true},
			}
			got.producers[producer] = modelStatus{terminal: true, failure: "blocked"}
		}, "producer state"},
		{"empty-producer-unfinished", func(want, got *modelObservation) {
			want.documents, got.documents = nil, nil
			got.producers[producer] = modelStatus{}
		}, "producer state"},
		{
			"deletions",
			func(want, _ *modelObservation) { want.deleted = map[modelObjectID]int{object: 1} },
			"deleted identities",
		},
		{"malformed-YAML", func(_, got *modelObservation) { got.documents[0].raw = []byte("value: [") }, "decode YAML"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want, got := observation(), observation()
			tc.edit(&want, &got)
			err := compareModelObservation(want, got)
			if tc.want == "" {
				{
					assert.FatalIf(t, err != nil, "%v", err)
				}
			} else {
				assert.FatalIf(
					t,
					err == nil || !strings.Contains(err.Error(), tc.want),
					"want diagnostic %q, got %v",
					tc.want,
					err,
				)
			}
		})
	}
}

func testIndependentModelWitnessesCase(t *testing.T, testCaseInput *struct {
	name string

	input     []byte
	kind      string
	committed bool
},
) {
	t.Helper()

	fixture := decodeRenderModelCase(testCaseInput.input)
	{
		assert.FatalIf(t, fixture.committed != testCaseInput.committed, "%v", "witness baseline route changed")
	}
	source := renderModelSource(fixture.before)
	group := "kustomize.toolkit.fluxcd.io"
	if testCaseInput.kind == "HelmRelease" {
		group = "helm.toolkit.fluxcd.io"
	}
	app := modelID{source: source, group: group, kind: testCaseInput.kind, namespace: "flux-system", name: "app"}
	wantAffected := modelSet{app: true}
	wantSupport := modelSet{renderModelSourceID(source): true}
	selection := compareModelWorlds(fixture.before, fixture.after)
	for _, side := range []modelSelection{selection.before, selection.after} {
		if !maps.Equal(side.affected, wantAffected) || !maps.Equal(side.support, wantSupport) ||
			len(side.unavailable) != 0 ||
			len(side.deleted) != 0 {
			t.Fatalf(
				"witness membership changed: affected=%v support=%v unavailable=%v deleted=%v",
				modelIDs(side.affected),
				modelIDs(side.support),
				modelIDs(side.unavailable),
				modelIDs(side.deleted),
			)
		}
	}
	if testCaseInput.kind == "Kustomization" {
		file := modelPath{source: source, path: "flux/optional.yaml"}
		{
			{
				_, exists := fixture.before.files[file]
				assert.FatalIf(t, exists, "%v", "optional input no longer absent before")
			}
		}
		{
			assert.FatalIf(
				t,
				!bytes.Contains(fixture.after.files[file].body, []byte("VALUE: new")),
				"%v",
				"optional input appearance changed",
			)
		}
	}
}

func testIndependentModelCase(t *testing.T, testCaseInput *struct {
	name string

	check func(*testing.
		T)
},
) {
	t.Helper()
	testCaseInput.check(t)
}

type testIndependentModelCase2Inputs struct {
	tc *struct {
		name  string
		check func(*testing.T)
	}
}

func testIndependentModelCase2(t *testing.T, testCaseInput testIndependentModelCase2Inputs) {
	t.Helper()
	testIndependentModelCase(t, testCaseInput.tc)
}

func testIndependentModelCoordinates(t *testing.T, source modelSource, provider *modelID) {
	t.Helper()
	artifact := source
	artifact.revision = "pinned"
	otherRoot := source
	otherRoot.root = "other"
	otherBinding := source
	otherBinding.binding = "other"
	before := modelWorld{files: make(map[modelPath]modelFile), producers: make(map[modelID]modelProducer)}
	identities := []modelID{(*provider), (*provider), (*provider), (*provider), (*provider)}
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
	if !maps.Equal(result.before.affected, modelSet{(*provider): true}) ||
		!maps.Equal(result.after.affected, modelSet{(*provider): true}) {
		t.Fatalf(
			"coordinates collapsed: before=%v after=%v",
			modelIDs(result.before.affected),
			modelIDs(result.after.affected),
		)
	}
}

func testIndependentModelDecoderBounds(t *testing.T) {
	t.Helper()
	data := slices.Grow([]byte{1, 0}, 74-2)
	for i := range 24 {
		data = append(data, 7, byte(i), byte(i))
	}
	for _, encoded := range [][]byte{nil, {1}, {1, 5, 2}, data} {
		decoded := decodeRenderModelCase(encoded)
		{
			{
				err := checkModelBounds(&decoded.modelCase)
				assert.FatalIf(t, err != nil, "input=%x: %v", encoded, err)
			}
		}
		{
			assert.FatalIf(
				t,
				!reflect.DeepEqual(decoded, decodeRenderModelCase(encoded)),
				"%v",
				"truncated decoder is nondeterministic",
			)
		}
		{
			assert.FatalIf(
				t,
				decoded.unsupported["ResourceSet-frozen-semantics"] != 1,
				"%v",
				"unsupported category disappeared",
			)
		}
	}
}
