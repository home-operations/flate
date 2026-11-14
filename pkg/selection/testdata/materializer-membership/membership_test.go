package selection

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The overlay records membership operations in package globals.
func TestMaterializerMembership_Count(t *testing.T) {
	for _, route := range []string{"indexed", "prepared", "object", "mixed", "catalog"} {
		for _, n := range []int{100, 200, 400} {
			t.Run(fmt.Sprintf("%s/%d", route, n), func(t *testing.T) {
				fixture := newMembershipFixture(t, route, n)
				var candidates [2][]*OccurrenceFacts
				for _, call := range []string{"first", "repeated"} {
					t.Run(call, func(t *testing.T) {
						membershipCounts = [3]membershipCounter{}
						fixture.collect(&candidates)
						fixture.assertCandidates(t, candidates)
						bounds := [3][3]int{}
						switch route {
						case "indexed":
							bounds[0] = [3]int{0, n, n}
						case "prepared":
							bounds[1] = [3]int{0, n, n}
						case "object":
							bounds[2] = [3]int{0, n, n}
						case "mixed":
							bounds[0], bounds[1] = [3]int{0, n, n}, [3]int{n, 2 * n, n}
						case "catalog":
							bounds[1] = [3]int{n, 2 * n, n}
						}
						if call == "repeated" && route != "catalog" {
							for i := range bounds {
								if bounds[i][1] != 0 {
									bounds[i][0], bounds[i][2] = n, 0
									if route == "mixed" {
										bounds[i][0] = 2 * n
									}
								}
							}
						}
						assertMembershipCounts(t, bounds)
						total, producers := 0, n
						for _, counter := range membershipCounts {
							total += counter.operations()
						}
						if route == "mixed" || route == "catalog" {
							producers = 2 * n
						}
						if total > 4*producers {
							t.Errorf("membership operations = %d, linear bound = %d", total, 4*producers)
						}
					})
				}
			})
		}
	}
	for _, route := range []string{"indexed", "prepared", "object"} {
		for _, control := range []string{"empty", "filtered"} {
			t.Run(route+"/"+control, func(t *testing.T) {
				n := 100
				if control == "empty" {
					n = 0
				}
				fixture := newMembershipFixture(t, route, n)
				for id, record := range fixture.facts.index.Records {
					if route == "indexed" {
						occurrence := fixture.facts.occurrences[id]
						occurrence.NeedsCandidates = false
						fixture.facts.occurrences[id] = occurrence
					} else {
						record.Builder = ""
						fixture.facts.index.Records[id] = record
					}
				}
				if route == "object" {
					fixture.objects = nil
					for i := range n {
						object := &manifest.HelmRelease{Name: fmt.Sprintf("filtered-%04d", i)}
						fixture.objects = append(fixture.objects, object)
						fixture.closure.session.store.AddObject(object)
					}
				}
				var candidates [2][]*OccurrenceFacts
				membershipCounts = [3]membershipCounter{}
				fixture.collect(&candidates)
				if len(candidates[0])+len(candidates[1]) != 0 {
					t.Errorf("control returned candidates: %v", candidates)
				}
				assertMembershipCounts(t, [3][3]int{})
			})
		}
	}
}

type membershipFixture struct {
	closure admissionClosure
	facts   *solverFacts
	route   string
	objects []manifest.BaseManifest
	want    [2][]*OccurrenceFacts
	project map[manifest.BaseManifest]OccurrenceFacts
	catalog []*OccurrenceFacts
}

func newMembershipFixture(t *testing.T, route string, n int) *membershipFixture {
	t.Helper()
	facts := &solverFacts{
		changes: change.NewSet(nil),
		index: manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{},
		},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{},
		candidates:  map[manifest.NamedResource]store.Child{},
	}
	f := &membershipFixture{
		facts: facts, route: route, project: map[manifest.BaseManifest]OccurrenceFacts{},
		closure: solverClosure(t, store.New(), facts),
	}
	groups := []string{route}
	if route == "mixed" || route == "catalog" {
		groups = []string{"indexed", "prepared"}
	}
	for _, group := range groups {
		for i := range n {
			object := &manifest.Kustomization{Name: fmt.Sprintf("%s-%04d", group, i), Path: "apps"}
			id := object.Named()
			occurrence := OccurrenceFacts{Identity: id, Authored: id, Object: object, HasPath: true}
			destination := 0
			switch group {
			case "indexed":
				occurrence.Declaration.File = id.Name + ".yaml"
				occurrence.CatalogKnown, occurrence.NeedsCandidates = true, true
				occurrence.Witness = object
				facts.occurrences[id] = occurrence
				facts.index.Records[id] = manifest.DiscoveryRecord{Builder: "builder", Declaration: object}
				if route == "mixed" || route == "catalog" {
					alias := id
					alias.Name = "alias-" + id.Name
					facts.index.Records[alias] = manifest.DiscoveryRecord{Builder: "builder", Declaration: object}
					facts.candidates[alias] = store.Child{Obj: object}
				}
			case "prepared":
				facts.index.Records[id] = manifest.DiscoveryRecord{Builder: "builder", Declaration: object}
				facts.candidates[id] = store.Child{Obj: object}
			case "object":
				destination = 1
				f.objects = append(f.objects, object)
				f.closure.session.store.AddObject(object)
			}
			f.project[object] = occurrence
			f.want[destination] = append(f.want[destination], &occurrence)
		}
	}
	if route == "catalog" {
		f.closure.context.Frozen = true
		f.closure.catalog = f.closure.session.catalogSnapshot(nil)
		f.catalog = slices.Clone(f.closure.catalog.candidates)
	}
	return f
}

func (f *membershipFixture) collect(candidates *[2][]*OccurrenceFacts) {
	project := func(object manifest.BaseManifest) OccurrenceFacts { return f.project[object] }
	switch f.route {
	case "prepared":
		f.closure.collectPreparedMaterializers(candidates, project)
	case "object":
		f.closure.collectObjectMaterializers(candidates, f.objects, false, project)
	default:
		f.closure.collectIndexedMaterializers(candidates, project)
	}
}

func (f *membershipFixture) assertCandidates(t *testing.T, candidates [2][]*OccurrenceFacts) {
	t.Helper()
	for i := range candidates {
		got := slices.Clone(candidates[i])
		slices.SortFunc(got, func(a, b *OccurrenceFacts) int { return a.Authored.Compare(b.Authored) })
		if diff := cmp.Diff(f.want[i], got, cmp.Comparer(func(a, b manifest.BaseManifest) bool { return a == b })); diff != "" {
			t.Errorf("candidates[%d] (-want +got):\n%s", i, diff)
		}
	}
	if f.closure.catalog != nil {
		if diff := cmp.Diff(f.catalog, f.closure.catalog.candidates, cmp.Comparer(func(a, b manifest.BaseManifest) bool { return a == b })); diff != "" {
			t.Errorf("catalog changed (-want +got):\n%s", diff)
		}
	}
}

func assertMembershipCounts(t *testing.T, bounds [3][3]int) {
	t.Helper()
	combined, limit := 0, 0
	for i, collector := range []string{"indexed", "prepared", "object"} {
		counter, bound := membershipCounts[i], bounds[i]
		row := struct {
			Collector string
			Counts    membershipCounter
			R, P, A   int
		}{collector, counter, bound[0], bound[1], bound[2]}
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("membership-count %s", encoded)
		maximum := bound[0] + bound[1] + bound[2]
		if counter.operations() > maximum {
			t.Errorf("%s operations = %d, R+P+A bound = %d", collector, counter.operations(), maximum)
		}
		if bound[1] > 0 && counter.Probes+counter.Comparisons == 0 {
			t.Errorf("%s positive witness did not execute membership", collector)
		}
		if bound[1] == 0 && counter != (membershipCounter{}) {
			t.Errorf("%s zero control performed membership work: %+v", collector, counter)
		}
		combined += counter.operations()
		limit += maximum
	}
	if combined > limit {
		t.Errorf("combined operations = %d, combined R+P+A bound = %d", combined, limit)
	}
}
