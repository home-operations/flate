package selection

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestMaterializerCandidates_IndexedKeys(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"equal", "declaration", "identity", "witness", "nil-witness"} {
		first := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "a"}
		second := first
		second.Name = "b"
		original := OccurrenceFacts{
			Declaration: OccurrenceID{Source: "source", File: "a.yaml"},
			Identity:    first, Authored: first, Witness: new(1), CatalogKnown: true, NeedsCandidates: true,
		}
		if key == "nil-witness" {
			original.Witness = nil
		}
		other := original
		other.Authored = second
		switch key {
		case "declaration":
			other.Declaration.File = "b.yaml"
		case "identity":
			other.Identity = second
		case "witness":
			other.Witness = new(2)
		}
		want := [2][]*OccurrenceFacts{{&original}, nil}
		if key != "equal" && key != "nil-witness" {
			want[0] = append(want[0], &other)
		}
		for _, context := range []struct {
			side manifest.TreeSide
			mode int
		}{
			{side: manifest.TreeBase, mode: 0},
			{side: manifest.TreeBase, mode: 1},
			{side: manifest.TreeHead, mode: 0},
			{side: manifest.TreeHead, mode: 1},
		} {
			t.Run(fmt.Sprintf("%s/side-%d/ordering-%t", key, context.side, context.mode == 1), func(t *testing.T) {
				t.Parallel()
				facts := &solverFacts{
					index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						first: {}, second: {},
					}},
					occurrences: map[manifest.NamedResource]OccurrenceFacts{first: original, second: other},
				}
				for range 2 {
					c := solverClosure(t, store.New(), facts)
					c.context = SourceContext{SideKnown: true, Side: context.side}
					c.candidates[context.mode][0] = []*OccurrenceFacts{&original}
					assertMaterializerCandidates(t, c.materializerCandidates(context.mode == 1), want)
				}
			})
		}
	}
}

func TestCollectPreparedMaterializers_ObjectMembership(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"duplicate", "seeded", "distinct-pointer", "rejected-retry", "projected-repeat", "projected-next"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testCollectMaterializerMembership(t, "prepared", name)
		})
	}
}

func TestCollectObjectMaterializers_ObjectMembership(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"duplicate", "seeded", "distinct-pointer", "rejected-retry", "projected-repeat", "projected-next"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testCollectMaterializerMembership(t, "object", name)
		})
	}
}

func testCollectMaterializerMembership(t *testing.T, route, name string) {
	t.Helper()
	object := &manifest.Kustomization{Name: "a", Path: "apps"}
	other := &manifest.Kustomization{Name: "a", Path: "other"}
	if name == "projected-next" {
		other.Name = "b"
	}
	original := OccurrenceFacts{Identity: object.Named(), Authored: object.Named(), Object: object}
	projected := OccurrenceFacts{Identity: other.Named(), Authored: other.Named(), Object: other}
	st := store.New()
	st.AddObject(object)
	facts := &solverFacts{
		changes:    change.NewSet(nil),
		index:      manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		candidates: map[manifest.NamedResource]store.Child{},
	}
	c := solverClosure(t, st, facts)
	var candidates [2][]*OccurrenceFacts
	destination := 1
	objects := []manifest.BaseManifest{object, object}
	if route == "prepared" {
		destination = 0
		alias := object.Named()
		alias.Name = "alias"
		c.catalog = &admissionCatalog{materializers: []manifest.NamedResource{object.Named(), alias}}
		for _, id := range c.catalog.materializers {
			facts.index.Records[id] = manifest.DiscoveryRecord{Builder: "builder"}
			facts.candidates[id] = store.Child{Obj: object}
		}
		if name == "distinct-pointer" || name == "projected-next" {
			facts.candidates[alias] = store.Child{Obj: other}
		}
	}
	want := []*OccurrenceFacts{&original}
	switch name {
	case "seeded":
		candidates[destination] = []*OccurrenceFacts{&original}
	case "distinct-pointer":
		if route == "prepared" {
			want = append(want, &projected)
		} else {
			candidates[1] = []*OccurrenceFacts{&projected}
			objects = []manifest.BaseManifest{other}
			want = []*OccurrenceFacts{&projected, &original}
		}
	case "projected-repeat":
		want = []*OccurrenceFacts{&projected, &projected}
	case "projected-next":
		st.AddObject(other)
		objects = []manifest.BaseManifest{object, other}
		want = []*OccurrenceFacts{&projected}
	}
	calls := 0
	project := func(input manifest.BaseManifest) OccurrenceFacts {
		calls++
		if name == "rejected-retry" && calls == 1 {
			return OccurrenceFacts{Object: input, HasPath: true, Policy: new(manifest.EmissionPolicy)}
		}
		if name == "projected-repeat" || name == "projected-next" || input == other {
			return projected
		}
		return original
	}
	switch route {
	case "prepared":
		c.collectPreparedMaterializers(&candidates, project)
	default:
		c.collectObjectMaterializers(&candidates, objects, false, project)
	}
	expected := [2][]*OccurrenceFacts{}
	expected[destination] = want
	assertMaterializerCandidates(t, candidates, expected)
	wantCalls := len(want)
	switch {
	case name == "seeded":
		wantCalls = 0
	case name == "distinct-pointer" && route == "object":
		wantCalls = 1
	case name == "rejected-retry":
		wantCalls = 2
	}
	assert.Equal(t, calls, wantCalls)
}

func TestMaterializerCandidates_Retention(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"indexed", "prepared", "object"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			object := &manifest.Kustomization{Name: "producer", Path: "apps"}
			replacement := &manifest.Kustomization{Name: "producer", Path: "replacement"}
			id := object.Named()
			original := OccurrenceFacts{
				Identity:        id,
				Authored:        id,
				Object:          object,
				CatalogKnown:    true,
				NeedsCandidates: true,
			}
			updated := original
			updated.Object = replacement
			facts := &solverFacts{
				changes:     change.NewSet(nil),
				index:       manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
				candidates:  map[manifest.NamedResource]store.Child{},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{},
				producers: map[manifest.BaseManifest]ProducerFacts{
					object:      {Occurrence: original},
					replacement: {Occurrence: updated},
				},
			}
			st := store.New()
			destination := 0
			switch route {
			case "indexed":
				original.Object, updated.Object = nil, nil
				original.Witness, updated.Witness = object, replacement
				facts.index.Records[id] = manifest.DiscoveryRecord{File: "producer.yaml"}
				facts.occurrences[id] = original
			case "prepared":
				facts.index.Records[id] = manifest.DiscoveryRecord{Builder: "builder", Declaration: object}
				facts.candidates[id] = store.Child{Obj: object}
			default:
				destination = 1
				st.AddObject(object)
			}
			c := solverClosure(t, st, facts)
			want := [2][]*OccurrenceFacts{}
			want[destination] = []*OccurrenceFacts{&original}
			assertMaterializerCandidates(t, c.materializerCandidates(false), want)
			switch route {
			case "indexed":
				facts.occurrences[id] = updated
			case "prepared":
				facts.index.Records[id] = manifest.DiscoveryRecord{Builder: "builder", Declaration: replacement}
				facts.candidates[id] = store.Child{Obj: replacement}
			default:
				st.AddObject(replacement)
			}
			c.candidatesKnown = [2]bool{}
			want[destination] = []*OccurrenceFacts{&updated}
			assertMaterializerCandidates(t, c.materializerCandidates(false), want)
			delete(facts.index.Records, id)
			delete(facts.occurrences, id)
			delete(facts.candidates, id)
			st.DeleteObject(id)
			c.candidatesKnown = [2]bool{}
			want[destination] = []*OccurrenceFacts{}
			assertMaterializerCandidates(t, c.materializerCandidates(false), want)
		})
	}
}

func TestMaterializerCandidates_CatalogAndOrdering(t *testing.T) {
	t.Parallel()
	for _, prepared := range []string{"none", "duplicate", "new"} {
		t.Run(prepared, func(t *testing.T) {
			t.Parallel()
			indexed := &manifest.Kustomization{Name: "a-indexed", Path: "indexed"}
			last := &manifest.Kustomization{Name: "z-indexed", Path: "last"}
			built := &manifest.Kustomization{Name: "b-prepared", Path: "prepared"}
			path := &manifest.Kustomization{Name: "c-path", Path: "path"}
			plain := &manifest.HelmRelease{Name: "d-plain"}
			facts := &solverFacts{
				changes: change.NewSet(nil),
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					indexed.Named(): {File: "indexed.yaml", Declaration: indexed},
					last.Named():    {File: "last.yaml", Declaration: last},
				}},
				candidates:  map[manifest.NamedResource]store.Child{},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{},
			}
			occurrence := applicationOccurrence(indexed, "source")
			occurrence.Object = indexed
			lastOccurrence := applicationOccurrence(last, "source")
			facts.occurrences[indexed.Named()] = occurrence
			facts.occurrences[last.Named()] = lastOccurrence
			st := store.New()
			st.AddObjects([]manifest.BaseManifest{indexed, last, path, plain})
			if prepared != "none" {
				facts.index.Records[built.Named()] = manifest.DiscoveryRecord{Builder: "builder", Declaration: built}
				facts.candidates[built.Named()] = store.Child{Obj: built}
				if prepared == "duplicate" {
					facts.candidates[built.Named()] = store.Child{Obj: indexed}
				} else {
					st.AddObject(built)
				}
			}
			for range 2 {
				c := solverClosure(t, st, facts)
				c.context.Frozen = true
				c.catalog = c.session.catalogSnapshot(nil)
				catalog := append([]*OccurrenceFacts(nil), c.catalog.candidates...)
				want := [2][]*OccurrenceFacts{{&occurrence}, {new(facts.ProducerFacts(path).Occurrence)}}
				if prepared == "new" {
					want[0] = append(want[0], new(facts.ProducerFacts(built).Occurrence))
				}
				want[0] = append(want[0], &lastOccurrence)
				assertMaterializerCandidates(t, c.materializerCandidates(false), want)
				want[1] = []*OccurrenceFacts{
					new(facts.ProducerFacts(plain).Occurrence),
					new(facts.ProducerFacts(indexed).Occurrence),
				}
				if prepared == "new" {
					want[1] = append(want[1], new(facts.ProducerFacts(built).Occurrence))
				}
				want[1] = append(
					want[1],
					new(facts.ProducerFacts(path).Occurrence),
					new(facts.ProducerFacts(last).Occurrence),
				)
				assertMaterializerCandidates(t, c.materializerCandidates(true), want)
				assertMaterializerCandidates(
					t,
					[2][]*OccurrenceFacts{c.catalog.candidates, nil},
					[2][]*OccurrenceFacts{catalog, nil},
				)
				want[1] = []*OccurrenceFacts{new(facts.ProducerFacts(path).Occurrence)}
				assertMaterializerCandidates(t, c.materializerCandidates(false), want)
			}
		})
	}
}

func TestMaterializerCandidates_Filtered(t *testing.T) {
	t.Parallel()
	for _, filtered := range []bool{false, true} {
		t.Run(fmt.Sprintf("filtered-%t", filtered), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			if filtered {
				object := &manifest.HelmRelease{Name: "nonpath"}
				st.AddObject(object)
				unsupported := object.Named()
				unsupported.Name = "unsupported"
				facts.index.Records[object.Named()] = manifest.DiscoveryRecord{File: "nonproducer.yaml"}
				facts.index.Records[unsupported] = manifest.DiscoveryRecord{File: "unsupported.yaml"}
				facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{
					object.Named(): {Identity: object.Named(), Authored: object.Named(), CatalogKnown: true},
					unsupported: {
						Identity: unsupported, Authored: unsupported, CatalogKnown: true,
						HasPath: true, Policy: new(manifest.EmissionPolicy),
					},
				}
				facts.candidates = map[manifest.NamedResource]store.Child{object.Named(): {Obj: object}}
			}
			for range 2 {
				c := solverClosure(t, st, facts)
				assertMaterializerCandidates(t, c.materializerCandidates(false), [2][]*OccurrenceFacts{})
			}
		})
	}
}

func assertMaterializerCandidates(t *testing.T, got, want [2][]*OccurrenceFacts) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmp.Comparer(func(a, b manifest.BaseManifest) bool { return a == b })); diff != "" {
		t.Errorf("materializer candidates (-want +got):\n%s", diff)
	}
}
