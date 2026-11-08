package store

import (
	"fmt"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_ListObjects_ByKindIndex(t *testing.T) {
	t.Parallel()
	for _, core := range []bool{false, true} {
		for _, insertForeignFirst := range []bool{false, true} {
			for _, withdrawForeignFirst := range []bool{false, true} {
				t.Run(
					fmt.Sprintf(
						"core=%t/insertForeignFirst=%t/withdrawForeignFirst=%t",
						core,
						insertForeignFirst,
						withdrawForeignFirst,
					),
					func(t *testing.T) {
						t.Parallel()
						testStoreListObjectsByKindIndexCase(
							t,
							&testStoreListObjectsByKindIndexCaseInputs{
								core:                 &core,
								insertForeignFirst:   &insertForeignFirst,
								withdrawForeignFirst: &withdrawForeignFirst,
							},
						)
					},
				)
			}
		}
	}
}

// TestStore_ListObjects_DeterministicOrder pins the sort contract:
// two ListObjects calls with concurrent concurrent adds must return
// objects in the same (Kind, Group, Namespace, Name) order. Without the
// sort, Go map iteration is randomized and ownership tie-breaking in
// change/ownership.go can attribute the same file to different KS
// owners on different runs.
func TestStore_ListObjects_DeterministicOrder(t *testing.T) {
	t.Parallel()
	s := New()
	// Add objects in non-alphabetical order so insertion order can't
	// accidentally hide a missing sort.
	objects := []*manifest.ConfigMap{
		{Name: "z", Namespace: "b"},
		{Name: "a", Namespace: "z"},
		{Name: "m", Namespace: "a"},
		{Name: "a", Namespace: "a"},
		{Name: "z", Namespace: "a"},
	}
	for _, obj := range objects {
		s.AddObject(obj)
	}

	first := s.ListObjects("", manifest.KindConfigMap)
	second := s.ListObjects("", manifest.KindConfigMap)

	assert.FatalIf(t, len(first) != len(second), "lengths differ: %d vs %d", len(first), len(second))
	for i := range first {
		a, b := first[i].Named(), second[i].Named()
		assert.ErrorIf(t, a != b, "position %d differs between calls: %v vs %v", i, a, b)
	}
	// Verify the order is actually sorted by (namespace, name) within
	// the same kind.
	for i := 1; i < len(first); i++ {
		prev, cur := first[i-1].Named(), first[i].Named()
		assert.ErrorIf(t, prev.Compare(cur) > 0, "unsorted at [%d]: %v > %v", i, prev, cur)
	}
}

type testStoreListObjectsByKindIndexCaseInputs struct {
	core                 *bool
	insertForeignFirst   *bool
	withdrawForeignFirst *bool
}

func testStoreListObjectsByKindIndexCase(t *testing.T, testCaseInput *testStoreListObjectsByKindIndexCaseInputs) {
	t.Helper()

	s := New()
	var typed manifest.BaseManifest = &manifest.Kustomization{Name: "a", Namespace: "ns1"}
	if *testCaseInput.core {
		typed = newCM("a", "ns1")
	}
	id := typed.Named()
	foreign := &manifest.RawObject{
		APIVersion: "example.test/v1",
		Kind:       id.Kind,
		Namespace:  id.Namespace,
		Name:       id.Name,
	}
	pair := []manifest.BaseManifest{typed, foreign}
	if *testCaseInput.insertForeignFirst {
		pair[0], pair[1] = pair[1], pair[0]
	}
	s.AddObjects(pair)
	other := &manifest.Secret{Name: "s", Namespace: "ns1"}
	s.AddObject(other)
	for _, tt := range []struct {
		name, group, kind string
		want              []manifest.BaseManifest
	}{
		{"real", id.Group, id.Kind, []manifest.BaseManifest{typed}},
		{"foreign", "example.test", id.Kind, []manifest.BaseManifest{foreign}},
		{"other kind", "", manifest.KindSecret, []manifest.BaseManifest{other}},
		{"group without kind", "example.test", "", []manifest.BaseManifest{}},
	} {
		got := s.ListObjects(tt.group, tt.kind)
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			{
				diff := cmp.Diff(
					tt.want,
					got,
					cmp.AllowUnexported(manifest.Kustomization{}),
					cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
				)
				assert.FatalIf(t, diff != "", "listed objects changed (-want +got): %s", diff)
			}
		})
	}
	inventory := []manifest.BaseManifest{typed, foreign, other}
	slices.SortFunc(
		inventory,
		func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) },
	)
	{
		diff := cmp.Diff(
			s.ListObjects("", ""),
			inventory,
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
	}
	pair = []manifest.BaseManifest{typed, foreign}
	if *testCaseInput.withdrawForeignFirst {
		pair[0], pair[1] = pair[1], pair[0]
	}
	for i, obj := range pair {
		assert.Equal(t, s.GetObject(obj.Named()), obj)
		assert.Equal(t, s.DeleteObject(obj.Named()), true)
		assert.Equal(t, s.GetObject(obj.Named()), nil)
		assert.Equal(t, len(s.ListObjects(obj.Named().Group, id.Kind)), 0)
		if i == 0 {
			remaining := pair[1]
			assert.Equal(t, s.GetObject(remaining.Named()), remaining)
			{
				diff := cmp.Diff(
					s.ListObjects(remaining.Named().Group, id.Kind),
					[]manifest.BaseManifest{remaining},
					cmp.AllowUnexported(manifest.Kustomization{}),
					cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
				)
				assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
			}
		}
	}
	{
		diff := cmp.Diff(
			s.ListObjects("", ""),
			[]manifest.BaseManifest{other},
			cmp.AllowUnexported(manifest.Kustomization{}),
			cmpopts.IgnoreFields(manifest.Kustomization{}, "projection"),
		)
		assert.FatalIf(t, diff != "", "stored objects changed (-want +got): %s", diff)
	}
}
