package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestInvocation_SelectOwnedSides(t *testing.T) {
	t.Parallel()
	for _, wrapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrapped=%t", wrapped), func(t *testing.T) {
			t.Parallel()
			testInvocationSelectOwnedSides(t, wrapped)
		})
	}
}

func testInvocationSelectOwnedSides(t *testing.T, wrapped bool) {
	t.Helper()
	owner := testutil.KustomizationID("apps", "owner")
	resourceErr := &selection.FailuresError{Message: "unprojected resource failure"}
	operationErr := errors.New("selected operation")
	neighborErr := errors.New("neighbor operation")
	sharedErr := fmt.Errorf("shared wrapper: %w", context.Canceled)
	sharedSibling := errors.New("shared sibling")
	r := &Invocation{byID: map[manifest.TreeID]int{}}
	for _, cluster := range []manifest.ClusterID{"a", "b", "c"} {
		for _, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
			run := selectedRun(t, &Result{})
			run.ID = manifest.TreeID{Cluster: cluster, Side: side}
			if cluster == "a" && side == manifest.TreeHead {
				run.store.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
				run.Result.Failed = map[manifest.NamedResource]store.StatusInfo{
					owner: {Status: store.StatusFailed, Message: "selected failure"},
				}
				run.Err = &treeError{id: run.ID, err: errors.Join(resourceErr, operationErr), attributed: true}
			}
			if cluster == "b" {
				run.Err = &treeError{id: run.ID, err: neighborErr, attributed: true}
			}
			r.byID[run.ID] = len(r.results)
			r.results = append(r.results, run)
			r.err = errors.Join(r.err, run.Err)
		}
	}
	r.err = errors.Join(sharedErr, r.err, sharedSibling)
	if wrapped {
		r.err = fmt.Errorf("outer invocation: %w", r.err)
	}
	ids := []manifest.TreeID{{Cluster: "a", Side: manifest.TreeBase}, {Cluster: "a", Side: manifest.TreeHead}}
	chosen, projection := r.Select(ids, []manifest.NamedResource{owner}, nil)
	assert.Equal(t, len(chosen), 2)
	assert.Equal(t, len(chosen[0].Failed), 0)
	assert.Equal(t, len(chosen[1].Failed), 1)
	for _, err := range []error{projection.Selected, projection.Remaining} {
		assert.Equal(t, errors.Is(err, operationErr), true)
		assert.Equal(t, errors.Is(err, sharedErr), true)
		assert.Equal(t, errors.Is(err, sharedSibling), true)
		assert.Equal(t, errors.Is(err, context.Canceled), true)
		assert.Equal(t, errors.Is(err, neighborErr), false)
		assert.Equal(t, errors.Is(err, resourceErr), false)
	}
	_, selectedFailure := errors.AsType[*selection.FailuresError](projection.Selected)
	_, remainingFailure := errors.AsType[*selection.FailuresError](projection.Remaining)
	assert.Equal(t, selectedFailure, true)
	assert.Equal(t, remainingFailure, false)
	for _, cluster := range []manifest.ClusterID{"b", "c"} {
		chosen, projection = r.Select(
			[]manifest.TreeID{{Cluster: cluster, Side: manifest.TreeHead}},
			[]manifest.NamedResource{owner},
			nil,
		)
		assert.Equal(t, len(chosen), 1)
		assert.Equal(t, errors.Is(projection.Selected, operationErr), false)
		assert.Equal(t, errors.Is(projection.Selected, neighborErr), cluster == "b")
		assert.Equal(t, errors.Is(projection.Selected, sharedErr), true)
	}
}

func TestInvocation_SelectInvalidIdentities(t *testing.T) {
	t.Parallel()
	base, head := manifest.TreeID{
		Cluster: "a",
		Side:    manifest.TreeBase,
	}, manifest.TreeID{
		Cluster: "a",
		Side:    manifest.TreeHead,
	}
	otherBase, otherHead := manifest.TreeID{
		Cluster: "b",
		Side:    manifest.TreeBase,
	}, manifest.TreeID{
		Cluster: "b",
		Side:    manifest.TreeHead,
	}
	r := &Invocation{
		results: []Rendered{{ID: base}, {ID: head}, {ID: otherBase}, {ID: otherHead}},
		byID:    map[manifest.TreeID]int{base: 0, head: 1, otherBase: 2, otherHead: 3},
	}
	for _, tc := range []struct {
		name string
		ids  []manifest.TreeID
	}{
		{"empty", nil},
		{"empty_identity", []manifest.TreeID{{}}},
		{"unknown", []manifest.TreeID{{Cluster: "missing", Side: manifest.TreeHead}}},
		{"duplicate_head", []manifest.TreeID{head, head}},
		{"duplicate_base", []manifest.TreeID{base, base}},
		{"cross_cluster", []manifest.TreeID{base, otherHead}},
		{"reversed", []manifest.TreeID{head, base}},
		{"base_only", []manifest.TreeID{base}},
		{"oversized", []manifest.TreeID{base, head, otherHead}},
		{"heads", []manifest.TreeID{head, otherHead}},
		{"bases", []manifest.TreeID{base, otherBase}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owned, err := r.ownedSides(tc.ids)
			assert.Equal(t, owned == nil, true)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			got, projection := r.Select(tc.ids, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
			assert.Equal(t, got == nil, true)
			assert.Equal(t, errors.Is(projection.Selected, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(projection.Selected, manifest.ErrFlux), true)
		})
	}
}

func TestFailedTrees_StructuralAttribution(t *testing.T) {
	t.Parallel()
	ids := []manifest.TreeID{{Cluster: "a", Side: manifest.TreeHead}, {Cluster: "b", Side: manifest.TreeHead}}
	first, second, shared := errors.New("first tree"), errors.New("second tree"), errors.New("shared failure")
	combined := fmt.Errorf("invocation: %w", errors.Join(
		fmt.Errorf("first wrapper: %w", &treeError{id: ids[0], err: first}),
		errors.Join(shared, fmt.Errorf("second wrapper: %w", &treeError{id: ids[1], err: second})),
	))
	results := failedTrees([]Tree{{ID: ids[0]}, {ID: ids[1]}}, combined)
	for i, result := range results {
		assert.Equal(t, errors.Is(result.Err, first), i == 0)
		assert.Equal(t, errors.Is(result.Err, second), i == 1)
		assert.Equal(t, errors.Is(result.Err, shared), true)
		attributed, ok := errors.AsType[*treeError](result.Err)
		assert.FatalIf(t, !ok, "missing attributed tree: %v", result.Err)
		assert.Equal(t, attributed.id, ids[i])
		assert.Equal(t, attributed.attributed, true)
	}
}
