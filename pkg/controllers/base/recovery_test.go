package base_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestDispatchNode_ReplacesDerivedFailureProvenance(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"recovered", "vanished", "suspended", "filtered", "preflight", "primary error", "primary status", "blocked", "new dependency failure", "prior primary failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			obj := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", Suspend: mode == "suspended"}
			id := obj.Named()
			old := testutil.HelmReleaseID("apps", "old")
			fresh := testutil.HelmReleaseID("apps", "fresh")
			if mode != "vanished" {
				st.AddObject(obj)
			}
			st.UpdateStatus(id, store.StatusFailed, "", "original failure")
			if mode != "prior primary failure" {
				st.UpdateFailure(id, &manifest.DependencyFailedError{Failed: []manifest.NamedResource{old}})
			}
			c := base.New(st, nil, "test")
			if mode == "filtered" {
				c.Configure(base.Options{Filter: change.NewFilter(change.NewSet([]string{"other.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "consumer.yaml"}}}, st)})
			}
			if mode == "preflight" {
				c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle", true }})
			}
			ran := false
			blocked := c.DispatchNode(t.Context(), id, manifest.DrainNone, func(_ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				ran = true
				info, _ := st.GetStatus(id)
				if got := info.BlockedBy(); len(got) != 0 {
					t.Errorf("reconcile inherited old blockers: %v", got)
				}
				switch mode {
				case "primary error":
					return errors.New("render failed")
				case "primary status":
					st.UpdateStatus(id, store.StatusFailed, "", "render failed")
				case "blocked":
					return &depwait.ErrBlocked{Deps: []manifest.NamedResource{fresh}}
				case "new dependency failure":
					return &manifest.DependencyFailedError{Failed: []manifest.NamedResource{fresh}}
				}
				return nil
			})
			wantStatus := store.StatusReady
			wantRan := true
			switch mode {
			case "vanished", "suspended", "filtered":
				wantRan = false
			case "preflight":
				wantRan = false
				wantStatus = store.StatusFailed
			case "primary error", "primary status", "new dependency failure", "prior primary failure":
				wantStatus = store.StatusFailed
			case "blocked":
				wantStatus = store.StatusPending
			}
			info, _ := st.GetStatus(id)
			if ran != wantRan || info.Status != wantStatus {
				t.Fatalf("ran=%v status=%+v, want ran=%v status=%s", ran, info, wantRan, wantStatus)
			}
			wantBlockers := []manifest.NamedResource(nil)
			if mode == "new dependency failure" {
				wantBlockers = []manifest.NamedResource{fresh}
			}
			if got := info.BlockedBy(); !slices.Equal(got, wantBlockers) {
				t.Fatalf("blockers=%v, want %v", got, wantBlockers)
			}
			if mode == "blocked" && !slices.Equal(blocked, []manifest.NamedResource{fresh}) {
				t.Fatalf("parked dependencies=%v", blocked)
			}
			if mode != "blocked" && len(blocked) != 0 {
				t.Fatalf("terminal result had parked dependencies: %v", blocked)
			}
			if wantStatus == store.StatusReady {
				if failed := st.FailedResources(); len(failed) != 0 {
					t.Fatalf("recovered failure still reported: %v", failed)
				}
			}
			if mode == "prior primary failure" && info.Message != "original failure" {
				t.Fatalf("prior primary failure was reset: %+v", info)
			}
		})
	}
}
