package store

import (
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_MatchesMutableArtifact(t *testing.T) {
	t.Parallel()
	st := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	artifact := &SourceArtifact{LocalPath: "input", FS: fstest.MapFS{}}
	st.SetArtifact(source.Named(), artifact)
	var before, current ReadSet
	before.Init(st, ownerID("consumer"), nil)
	before.GetArtifact(source.Named(), ArtifactContent)
	current.Init(st, ownerID("consumer"), nil)
	assert.FatalIf(
		t,
		!current.Matches(&before),
		"%s",
		"the same mutable artifact was treated as different material",
	)
	st.SetArtifact(source.Named(), artifact.WithReads(&ReadSet{}))
	assert.FatalIf(
		t,
		!before.Valid() || !current.Matches(&before),
		"%s",
		"refreshing registrations replaced the acquired mutable material",
	)
	st.SetArtifact(source.Named(), &SourceArtifact{LocalPath: artifact.LocalPath, FS: artifact.FS})
	var replacement ReadSet
	replacement.Init(st, ownerID("consumer"), nil)
	assert.FatalIf(
		t,
		replacement.Matches(&before),
		"%s",
		"a new mutable artifact was inferred equal from its location",
	)
}

func TestReadSet_MatchesRawCollection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		value   string
		matches bool
	}{
		{"equal distinct content", "same", true},
		{"changed nested content", "changed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := &manifest.RawObject{
				Kind:       "Widget",
				APIVersion: "example.test/v1",
				Name:       "app",
				Spec:       map[string]any{"items": []any{map[string]any{"value": "same"}}},
			}
			second := first.Clone()
			second.Spec["items"].([]any)[0].(map[string]any)["value"] = tc.value
			beforeStore, currentStore := New(), New()
			beforeStore.AddObject(first)
			currentStore.AddObject(second)
			var before, current ReadSet
			before.Init(beforeStore, ownerID("reader"), nil)
			current.Init(currentStore, ownerID("reader"), nil)
			defer before.Retire()
			defer current.Retire()
			id := first.Named()
			objects := before.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], manifest.BaseManifest(first))
			assert.Equal(t, current.Matches(&before), tc.matches)
			objects = current.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], manifest.BaseManifest(second))
			assert.Equal(t, objects[0] == first, false)
			assert.Diff(t, first.Spec, map[string]any{"items": []any{map[string]any{"value": "same"}}})
		})
	}
}

func TestReadSet_UnsettledProducerSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		known bool
	}{{"empty", false}, {"known", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
			second := &manifest.Kustomization{Name: "second", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, first, second})
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}}, nil)
			}
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Retire()
			before := reads.UnsettledProducers(source.Named())
			st.RecordEmitted(second.Named(), &Emission{Children: []Child{{Obj: source}}}, nil)
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}, Complete: true}, nil)
			}
			{
				after := reads.UnsettledProducers(source.Named())
				assert.FatalIf(
					t,
					!slices.Equal(before, after),
					"availability query changed within one captured execution: before=%v after=%v",
					before,
					after,
				)
			}
		})
	}
}

func TestCapture_SupplierQueryIsolation(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			t.Parallel()
			st := New()
			sourceOwner, queryOwner := ownerID("source-owner"), ownerID("query-owner")
			st.AddObjects(
				[]manifest.BaseManifest{
					&manifest.Kustomization{Name: sourceOwner.Name, Namespace: sourceOwner.Namespace},
					&manifest.Kustomization{Name: queryOwner.Name, Namespace: queryOwner.Namespace},
				},
			)
			source, query := emissionChild("source", "same"), emissionChild("query", "same")
			payload := &SourceArtifact{Key: "family", Content: "generation", FS: fstest.MapFS{}}
			st.RecordEmitted(
				sourceOwner,
				&Emission{Children: []Child{source}, Complete: complete},
				map[manifest.NamedResource]*SourceArtifact{source.Obj.Named(): payload},
			)
			st.RecordEmitted(queryOwner, &Emission{Children: []Child{query}, Complete: complete}, nil)
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Close()
			reads.GetArtifact(source.Obj.Named(), ArtifactContent)
			reads.UnsettledProducers(query.Obj.Named())
			reads.Productions(query.Obj.Named(), nil)
			next := *payload
			next.Metadata = map[string]string{"ignored": "next"}
			st.RecordEmitted(
				sourceOwner,
				&Emission{Children: []Child{source, emissionChild("unrelated", "new")}, Complete: complete},
				map[manifest.NamedResource]*SourceArtifact{source.Obj.Named(): &next},
			)
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
			st.RecordEmitted(
				queryOwner,
				&Emission{
					Children:    []Child{query, emissionChild("query-sibling", "new")},
					Complete:    complete,
					Fingerprint: "query-publication",
				},
				nil,
			)
			assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{reads.ID()})
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestCapture_PartialSupplierInputStamp(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	sibling := emissionChild("sibling", "same")
	child := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling}}, nil)
	var reads ReadSet
	reads.Init(st, child.Named(), nil)
	defer reads.Close()
	reads.GetObject(sibling.Obj.Named())
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling, {Obj: child}}}, nil)
	_, _, captured := reads.GetEmission(child.Named())
	defer captured.Finish()
	_, _, current := st.GetEmission(child.Named())
	defer current.Finish()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, captured.input, manifest.BaseManifest(child))
	assert.Equal(t, captured.inputHead, current.inputHead)
	st.RecordEmitted(
		producer,
		&Emission{Children: []Child{sibling, {Obj: child}, emissionChild("unrelated", "new")}},
		nil,
	)
	_, _, repeated := reads.GetEmission(child.Named())
	defer repeated.Finish()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, repeated.inputHead, captured.inputHead)
	assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
}

func TestReadSet_ProducedArtifact(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		t.Run(strconv.FormatBool(consumed), func(t *testing.T) {
			t.Parallel()
			s := New()
			parent := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(parent)
			child := emissionChild("output", "first")
			id := child.Obj.Named()
			s.AddObject(child.Obj, child.Doc)
			previous := &SourceArtifact{Content: "first", Revision: "first", FS: fstest.MapFS{}}
			s.SetArtifact(id, previous)
			var reads ReadSet
			reads.Init(s, parent.Named(), nil)
			if consumed {
				reads.GetArtifact(id, ArtifactContent)
			}
			_, _, next := reads.GetEmission(parent.Named())
			defer next.Finish()
			next.Children, next.Complete = []Child{child}, true
			reads.Bind(&next)
			{
				got := reads.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
				assert.FatalIf(t, got != previous, "prospective output lost the complete descriptor: %v", got)
			}
			s.RecordEmitted(
				parent.Named(),
				&next,
				map[manifest.NamedResource]*SourceArtifact{
					id: {Content: "second", Revision: "second", FS: fstest.MapFS{}},
				},
			)
			if reads.Valid() == consumed {
				t.Fatalf("self-publication validity=%v with prior consumption=%v", reads.Valid(), consumed)
			}
		})
	}
}

func TestCapture_DirectAndNamedReads(t *testing.T) {
	t.Parallel()
	st := New()
	first, other := emissionChild("first", "first"), emissionChild("other", "other")
	st.AddObject(first.Obj)
	st.AddObject(other.Obj)
	var reads ReadSet
	reads.Init(st, ownerID("reader"), nil)
	defer reads.Close()
	input := Input{Ref: manifest.DependencyRef{NamedResource: first.Obj.Named()}}
	assert.Equal(t, reads.Capture(&input), first.Obj)
	assert.Equal(t, reads.last == nil, true)
	assert.Equal(t, reads.GetObject(first.Obj.Named()), first.Obj)
	assert.Equal(t, reads.last == &input, true)
	second := Input{Ref: manifest.DependencyRef{NamedResource: other.Obj.Named()}}
	assert.Equal(t, reads.Capture(&second), other.Obj)
	assert.Equal(t, reads.last == &input, true)
	assert.Equal(t, reads.GetObject(other.Obj.Named()), other.Obj)
	assert.Equal(t, reads.last == &second, true)
	replacement := Input{Ref: manifest.DependencyRef{NamedResource: other.Obj.Named()}}
	assert.Equal(t, reads.Capture(&replacement), other.Obj)
	assert.Equal(t, reads.last == &replacement, true)
	st.AddObject(emissionChild("first", "changed").Obj)
	assert.Equal(t, reads.Valid(), false)
}

func TestReadSet_MatchesContentFacets(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"object", "absent object", "artifact", "absent artifact", "productions", "empty productions", "objects", "empty objects"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := New()
			child := emissionChild("input", "same")
			id := child.Obj.Named()
			if name != "absent object" && name != "empty objects" {
				st.AddObject(child.Obj, child.Doc)
			}
			if name == "artifact" {
				st.SetArtifact(id, &SourceArtifact{Content: "same", Revision: "revision"})
			}
			if name == "productions" {
				owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
				st.AddObject(owner)
				st.RecordEmitted(owner.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
			}
			capture := func(reads *ReadSet) {
				switch name {
				case "object", "absent object":
					reads.GetObject(id)
				case "artifact", "absent artifact":
					reads.GetArtifact(id, ArtifactContent|ArtifactRevision)
				case "productions", "empty productions":
					reads.Productions(id, nil)
				case "objects", "empty objects":
					reads.ListObjects(id.Group, id.Kind)
				}
			}
			var previous, current, incomplete ReadSet
			previous.Init(st, ownerID("before"), nil)
			current.Init(st, ownerID("current"), nil)
			incomplete.Init(st, ownerID("incomplete"), nil)
			t.Cleanup(previous.Retire)
			t.Cleanup(current.Retire)
			t.Cleanup(incomplete.Retire)
			capture(&previous)
			capture(&current)
			if name == "objects" || name == "empty objects" {
				id.Name, id.Namespace = "", ""
			}
			incomplete.input(id)
			assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
			assert.Equal(t, compareCapturedContent(t, &current, &incomplete, id), false)
			assert.Equal(t, compareCapturedContent(t, &incomplete, &previous, id), false)
			previous.GetStatus(child.Obj.Named())
			assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
		})
	}
}

func TestReadSet_MatchesContentChanges(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"object", "document", "artifact content", "artifact revision", "artifact origin", "productions", "objects", "collection value"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := New()
			child := emissionChild("input", "before")
			id := child.Obj.Named()
			st.AddObject(child.Obj, child.Doc)
			artifact := &SourceArtifact{
				Content: "before", Revision: "before",
				Metadata: map[string]string{"org.opencontainers.image.revision": "before"},
			}
			st.SetArtifact(id, artifact)
			var previous, current ReadSet
			previous.Init(st, ownerID("before"), nil)
			current.Init(st, ownerID("current"), nil)
			t.Cleanup(previous.Retire)
			t.Cleanup(current.Retire)
			switch name {
			case "object", "document":
				previous.GetObject(id)
				replacement := emissionChild("input", "after")
				if name == "document" {
					replacement.Obj = child.Obj
				}
				st.AddObject(replacement.Obj, replacement.Doc)
				current.GetObject(id)
			case "artifact content", "artifact revision", "artifact origin":
				previous.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
				replacement := *artifact
				switch name {
				case "artifact content":
					replacement.Content = "after"
				case "artifact revision":
					replacement.Revision = "after"
				case "artifact origin":
					replacement.Metadata = map[string]string{"org.opencontainers.image.revision": "after"}
				}
				st.SetArtifact(id, &replacement)
				current.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
			case "productions":
				previous.Productions(id, nil)
				owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
				st.AddObject(owner)
				st.RecordEmitted(owner.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
				current.Productions(id, nil)
			case "objects", "collection value":
				previous.ListObjects(id.Group, id.Kind)
				member := "added"
				if name == "collection value" {
					member = "input"
				}
				st.AddObject(emissionChild(member, "after").Obj)
				current.ListObjects(id.Group, id.Kind)
				id.Name, id.Namespace = "", ""
			}
			assert.Equal(t, compareCapturedContent(t, &current, &previous, id), false)
			assert.Equal(t, compareCapturedContent(t, &previous, &previous, id), true)
		})
	}
}

func TestReadSet_MatchesContentArtifactCoverage(t *testing.T) {
	t.Parallel()
	for _, bit := range []ArtifactRead{ArtifactContent, ArtifactRevision, ArtifactOrigin} {
		t.Run(strconv.Itoa(int(bit)), func(t *testing.T) {
			t.Parallel()
			st := New()
			id := manifest.BootstrapSourceID
			st.SetArtifact(id, &SourceArtifact{Content: "content", Revision: "revision"})
			var previous, current ReadSet
			previous.Init(st, ownerID("before"), nil)
			current.Init(st, ownerID("current"), nil)
			t.Cleanup(previous.Retire)
			t.Cleanup(current.Retire)
			all := ArtifactContent | ArtifactRevision | ArtifactOrigin
			previous.GetArtifact(id, all&^bit)
			current.GetArtifact(id, all)
			assert.Equal(t, compareCapturedContent(t, &current, &previous, id), false)
			assert.Equal(t, compareCapturedContent(t, &previous, &current, id), true)
		})
	}
}

func TestReadSet_MatchesContentProductionIndex(t *testing.T) {
	t.Parallel()
	st := New()
	child := emissionChild("input", "same")
	st.AddObject(child.Obj, child.Doc)
	first, second := new(manifest.DiscoveryIndex), new(manifest.DiscoveryIndex)
	var previous, current, changed ReadSet
	previous.Init(st, ownerID("before"), nil)
	current.Init(st, ownerID("current"), nil)
	changed.Init(st, ownerID("changed"), nil)
	t.Cleanup(previous.Retire)
	t.Cleanup(current.Retire)
	t.Cleanup(changed.Retire)
	id := child.Obj.Named()
	previous.Productions(id, first)
	current.Productions(id, first)
	changed.Productions(id, second)
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
	assert.Equal(t, compareCapturedContent(t, &changed, &previous, id), false)
	var unsettled ReadSet
	unsettled.Init(st, ownerID("unsettled"), nil)
	t.Cleanup(unsettled.Retire)
	unsettled.UnsettledProducers(id)
	var unindexed ReadSet
	unindexed.Init(st, ownerID("unindexed"), nil)
	t.Cleanup(unindexed.Retire)
	unindexed.Productions(id, nil)
	assert.Equal(t, compareCapturedContent(t, &unindexed, &unsettled, id), false)
}

func TestReadSet_MatchesContentLookup(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		t.Run(strconv.FormatBool(indexed), func(t *testing.T) {
			t.Parallel()
			st := New()
			id, other := ownerID("input"), ownerID("other")
			var previous, current, foreign ReadSet
			previous.Init(st, ownerID("before"), nil)
			current.Init(st, ownerID("current"), nil)
			foreign.Init(New(), ownerID("foreign"), nil)
			t.Cleanup(previous.Retire)
			t.Cleanup(current.Retire)
			t.Cleanup(foreign.Retire)
			for _, reads := range []*ReadSet{&previous, &current, &foreign} {
				reads.GetObject(id)
				reads.GetObject(other)
				if indexed {
					reads.indexInputs(0)
				}
			}
			assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
			assert.Equal(t, compareCapturedContent(t, &current, &foreign, id), false)
			assert.Equal(t, compareCapturedContent(t, &current, &previous, ownerID("absent")), false)
			assert.Equal(t, compareCapturedContent(t, &current, nil, id), false)
			assert.Equal(t, compareCapturedContent(t, nil, &previous, id), false)
		})
	}
}

type contentInputState struct {
	input, next   *Input
	details       *inputDetails
	derived       uint8
	declaration   bool
	required      bool
	gate          bool
	materializer  bool
	hasStatus     bool
	settled       bool
	watchProducer bool
	producer      uint64
	read          ArtifactRead
	heads         [bucketFacet]*registrationHead
	generations   [bucketFacet]uint64
}

func compareCapturedContent(t *testing.T, current, previous *ReadSet, id manifest.NamedResource) bool {
	t.Helper()
	states := func(reads *ReadSet) []contentInputState {
		var result []contentInputState
		if reads == nil {
			return result
		}
		for input := reads.inputs; input != nil; input = input.next {
			state := contentInputState{
				input: input, next: input.next, details: input.details.Load(), derived: input.derived,
				declaration: input.declaration, required: input.required, gate: input.gate,
				materializer: input.materializer, hasStatus: input.hasStatus, settled: input.settled,
				watchProducer: input.watchProducer, producer: input.producer,
			}
			if state.details != nil {
				state.read = state.details.artifactRead
			}
			for kind := objectFacet; kind <= bucketFacet; kind++ {
				if cell := input.cell(kind); cell != nil {
					state.heads[kind-1] = cell.head
					state.generations[kind-1] = cell.generation.Load()
				}
			}
			result = append(result, state)
		}
		return result
	}
	before, after := states(previous), states(current)
	var previousLast, currentLast *Input
	if previous != nil {
		previousLast = previous.last
	}
	if current != nil {
		currentLast = current.last
	}
	got := current.MatchesContent(previous, id)
	assert.Equal(t, slices.Equal(before, states(previous)), true)
	assert.Equal(t, slices.Equal(after, states(current)), true)
	if previous != nil {
		assert.Equal(t, previous.last, previousLast)
	}
	if current != nil {
		assert.Equal(t, current.last, currentLast)
	}
	return got
}

func TestReadSet_MatchesContentRequiredFacets(t *testing.T) {
	t.Parallel()
	st := New()
	id := manifest.BootstrapSourceID
	st.SetArtifact(id, &SourceArtifact{Content: "before"})
	var previous, current ReadSet
	previous.Init(st, ownerID("before"), nil)
	current.Init(st, ownerID("current"), nil)
	t.Cleanup(previous.Retire)
	t.Cleanup(current.Retire)
	previous.GetArtifact(id, ArtifactContent)
	st.SetArtifact(id, &SourceArtifact{Content: "after"})
	current.GetObject(id)
	assert.Equal(t, compareCapturedContent(t, &current, &previous, id), true)
	assert.Equal(t, compareCapturedContent(t, &previous, &current, id), false)
}
