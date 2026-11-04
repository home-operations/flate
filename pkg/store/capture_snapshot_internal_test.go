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
