package base_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestExecution_ReplacesDerivedFailureProvenance(t *testing.T) {
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
			st.UpdateStatus(id, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "original failure"})
			if mode != "prior primary failure" {
				st.UpdateFailure(id, &manifest.DependencyFailedError{Failed: []manifest.NamedResource{old}})
			}
			c := base.New(st, nil)
			if mode == "filtered" {
				c.Configure(base.Options{Filter: change.NewFilter(change.NewSet([]string{"other.yaml"}), &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "consumer.yaml"}}}, st)})
			}
			if mode == "preflight" {
				c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { return "cycle", true }})
			}
			ran := false
			execution := controllertest.NewExecution(c, id)
			execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(_ *base.Execution, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
				ran = true

				switch mode {
				case "primary error":
					return errors.New("render failed")
				case "primary status":
					st.UpdateStatus(id, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "render failed"})
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
				wantStatus = store.StatusFailed
			}
			info, _ := st.GetStatus(id)
			if ran != wantRan || info.Status != wantStatus {
				t.Fatalf("ran=%v status=%+v, want ran=%v status=%s", ran, info, wantRan, wantStatus)
			}
			wantBlockers := []manifest.NamedResource(nil)
			if mode == "new dependency failure" {
				wantBlockers = []manifest.NamedResource{fresh}
			}
			if mode == "blocked" {
				wantBlockers = []manifest.NamedResource{old}
			}
			if got := info.BlockedBy(); !slices.Equal(got, wantBlockers) {
				t.Fatalf("blockers=%v, want %v", got, wantBlockers)
			}
			if mode == "blocked" && !slices.Equal(execution.Blocked, []manifest.NamedResource{fresh}) {
				t.Fatalf("parked dependencies=%v", execution.Blocked)
			}
			if mode != "blocked" && len(execution.Blocked) != 0 {
				t.Fatalf("terminal result had parked dependencies: %v", execution.Blocked)
			}
			if wantStatus == store.StatusReady {
				if failed := st.FailedResources(); len(failed) != 0 {
					t.Fatalf("recovered failure still reported: %v", failed)
				}
			}
			if mode == "prior primary failure" && info.String() != "original failure" {
				t.Fatalf("prior primary failure was reset: %+v", info)
			}
		})
	}
}

func TestDispatchNode_CanonicalRead(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"preflight", "retry"} {
		for _, replacedType := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replaced-type=%t", stage, replacedType), func(t *testing.T) {
				t.Parallel()
				s := store.New()
				hr := &manifest.HelmRelease{Name: "release", Namespace: "apps"}
				fresh := hr.Clone()
				fresh.Values = map[string]any{"current": true}
				s.AddObject(hr)
				id := hr.Named()
				replace := func() {
					if replacedType {
						s.AddObject(&manifest.RawObject{APIVersion: manifest.HelmReleaseDomain + "/v2", Kind: manifest.KindHelmRelease, Name: id.Name, Namespace: id.Namespace})
					} else {
						s.AddObject(fresh)
					}
				}
				c := base.New(s, task.NewBounded(2))
				if stage == "preflight" {
					s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending})
					c.Configure(base.Options{PreflightFailure: func(manifest.NamedResource) (string, bool) { replace(); return "", false }})
				} else {
					s.UpdateFailure(id, &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{testutil.HelmReleaseID("apps", "dependency")}})
					s.AddListener(store.EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
						if payload.(store.StatusInfo).Status == store.StatusPending {
							replace()
						}
					}, false)
				}
				called := false
				execution := controllertest.NewExecution(c, id)
				execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(execution *base.Execution, _ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
					if stage == "retry" {
						s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending})
						if execution.Reads.Valid() {
							t.Fatal("replacement did not invalidate original attempt")
						}
						return nil
					}
					called = true
					if obj != fresh {
						t.Fatal("reconciliation retained the replaced canonical object")
					}
					return nil
				})
				if stage == "retry" {
					execution.Retire()
					execution = controllertest.NewExecution(c, id)
					execution.Dispatch(t.Context(), manifest.DrainNone, execution, func(_ *base.Execution, _ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
						called = true
						if obj != fresh {
							t.Fatal("fresh attempt retained replaced declaration")
						}
						return nil
					})
				}
				defer execution.Close()
				if called == replacedType {
					t.Fatalf("reconciled=%t, replaced type=%t", called, replacedType)
				}
				info, _ := s.GetStatus(id)
				if replacedType && info.Outcome() != store.OutcomeSkipped {
					t.Fatalf("missing typed snapshot status=%v", info)
				}
			})
		}
	}
}
