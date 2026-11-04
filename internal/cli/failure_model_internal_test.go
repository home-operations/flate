package cli

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestFailureModel(t *testing.T) {
	t.Parallel()
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("flux-system", name)
	}
	failed := func(ids ...manifest.NamedResource) map[manifest.NamedResource]store.StatusInfo {
		m := make(map[manifest.NamedResource]store.StatusInfo, len(ids))
		for _, id := range ids {
			m[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " boom"}
		}
		return m
	}
	root, mid, leaf := id("root"), id("mid"), id("leaf")
	missing, dependent, dual, peer := id("missing"), id("dependent"), id("dual"), id("peer")
	for _, tc := range []struct {
		name   string
		result selection.Selection
		want   output.Model
		raw    *orchestrator.Result
	}{
		{
			name: "GroupsCascadeUnderRoots",
			result: selection.Selection{
				Failed:  failed(root, mid, leaf, dependent),
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{mid: {root}, leaf: {mid}, dependent: {missing}},
				Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {leaf, mid}, missing: {dependent}},
			},
			want: output.Model{
				Primary: []output.Primary{{ID: root, Msg: "root boom", Blocks: []manifest.NamedResource{leaf, mid}}},
				Missing: []output.Missing{{ID: missing, RequiredBy: []manifest.NamedResource{dependent}}},
				Blocked: 3,
			},
		},
		{
			name:   "PrimaryOnly",
			result: selection.Selection{Failed: failed(root), Causes: map[manifest.NamedResource][]manifest.NamedResource{root: nil}},
			want:   output.Model{Primary: []output.Primary{{ID: root, Msg: "root boom"}}},
		},
		{name: "Empty"},
		{
			name: "BlockedCountsDistinctResources",
			result: selection.Selection{
				Failed:  failed(root, dual),
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{dual: {root, missing}},
				Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {dual}, missing: {dual}},
			},
			want: output.Model{
				Primary: []output.Primary{{ID: root, Msg: "root boom", Blocks: []manifest.NamedResource{dual}}},
				Missing: []output.Missing{{ID: missing, RequiredBy: []manifest.NamedResource{dual}}},
				Blocked: 1,
			},
		},
		{
			name: "PreparedCycleRoot",
			raw: &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					root: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + peer.String()},
					peer: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + root.String()},
					leaf: {Status: store.StatusFailed, Message: "dependencies failed"},
				},
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{root: {peer}, peer: {root}, leaf: {root}},
			},
			want: output.Model{
				Primary: []output.Primary{
					{ID: peer, Msg: "dependency cycle: blocked by " + root.String()},
					{ID: root, Msg: "dependency cycle: blocked by " + peer.String(), Blocks: []manifest.NamedResource{leaf}},
				},
				Blocked: 1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := &tc.result
			if tc.raw != nil {
				failed := maps.Clone(tc.raw.Failed)
				blocked := maps.Clone(tc.raw.Blocked)
				for id, deps := range blocked {
					blocked[id] = slices.Clone(deps)
				}
				o, err := completedOrchestrator(t)
				assert.NoError(t, err)
				selected, err, _ := orchestrator.Select(
					[]orchestrator.Rendered{{Orchestrator: o, Result: tc.raw}},
					&orchestrator.FailuresError{
						Message: "raw failures",
					},
					[]manifest.NamedResource{testutil.KustomizationID("", "")},
					nil,
				)
				assert.FatalIf(t, len(selected) != 1 || err == nil ||
					!strings.Contains(
						err.Error(),
						fmt.Sprintf("(+%d blocked", tc.want.Blocked),
					), "selected error and report counts disagree: %v", err)
				result = selected[0]
				assert.Equal(t, maps.Equal(tc.raw.Failed, failed), true)
				assert.Diff(t, tc.raw.Blocked, blocked)
			}
			got := failureModel(result)
			assert.Diff(t, got, tc.want)
			assert.Equal(t, len(got.Primary)+len(got.Missing) == 0, len(tc.want.Primary)+len(tc.want.Missing) == 0)
		})
	}
}
