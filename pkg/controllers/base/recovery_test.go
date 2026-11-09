package base_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/admissiontest"
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
			testExecutionReplacesDerivedFailureProvenanceCase(
				t,
				&mode,
			)
		},
		)
	}
}

func TestDispatchNode_CanonicalRead(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"preflight", "retry"} {
		for _, replacedType := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/replaced-type=%t", stage, replacedType), func(t *testing.T) {
				t.Parallel()
				testDispatchNodeCanonicalReadCase(
					t,
					testDispatchNodeCanonicalReadCaseInputs{stage: &stage, replacedType: &replacedType},
				)
			},
			)
		}
	}
}

func testExecutionReplacesDerivedFailureProvenanceCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := store.New()
	obj := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "consumer",
		Namespace: "apps",
		Suspend:   (*testCaseInput) == "suspended",
	}
	id := obj.Named()
	old := testutil.HelmReleaseID("apps", "old")
	fresh := testutil.HelmReleaseID("apps", "fresh")
	if (*testCaseInput) != "vanished" {
		st.AddObject(obj)
	}
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "original failure"})
	if (*testCaseInput) != "prior primary failure" {
		st.UpdateFailure(id, &manifest.DependencyFailedError{Failed: []manifest.NamedResource{old}})
	}
	c := base.New(st, nil)
	if (*testCaseInput) == "filtered" {
		admissionLoader1, _ := admissiontest.New(
			t,
			admissiontest.Config{
				Changes: change.NewSet([]string{"other.yaml"}),
				Index: &manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: "consumer.yaml"}},
				},
				Inventory: st,
				Root:      "",
			},
		)
		admissiontest.Commit(t, admissionLoader1.Options.Session, nil)
		c.Configure(base.Options{Session: admissionLoader1.Options.Session, Loader: admissionLoader1})
	}
	if (*testCaseInput) == "preflight" {
		st.SetPreflight(id, "cycle")
	}
	ran := false
	execution := controllertest.NewExecution(c, id)
	execution.Dispatch(
		t.Context(),
		manifest.DrainNone,
		execution,
		func(_ *base.Execution, _ context.Context, _ *manifest.HelmRelease, _ manifest.DrainLevel) error {
			ran = true

			switch *testCaseInput {
			case "primary error":
				return errors.New("render failed")
			case "primary status":
				st.UpdateStatus(
					id,
					store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "render failed"},
				)
			case "blocked":
				return &depwait.ErrBlocked{Deps: []manifest.NamedResource{fresh}}
			case "new dependency failure":
				return &manifest.DependencyFailedError{Failed: []manifest.NamedResource{fresh}}
			}
			return nil
		},
	)
	wantStatus := store.StatusReady
	wantRan := true
	switch *testCaseInput {
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
	assert.FatalIf(
		t,
		ran != wantRan || info.Status != wantStatus,
		"ran=%v status=%+v, want ran=%v status=%s",
		ran,
		info,
		wantRan,
		wantStatus,
	)
	wantBlockers := []manifest.NamedResource(nil)
	if (*testCaseInput) == "new dependency failure" {
		wantBlockers = []manifest.NamedResource{fresh}
	}
	if (*testCaseInput) == "blocked" {
		wantBlockers = []manifest.NamedResource{old}
	}
	{
		got := info.BlockedBy()
		assert.FatalIf(t, !slices.Equal(got, wantBlockers), "blockers=%v, want %v", got, wantBlockers)
	}
	if (*testCaseInput) == "blocked" && !slices.Equal(execution.Blocked, []manifest.NamedResource{fresh}) {
		t.Fatalf("parked dependencies=%v", execution.Blocked)
	}
	if (*testCaseInput) != "blocked" && len(execution.Blocked) != 0 {
		t.Fatalf("terminal result had parked dependencies: %v", execution.Blocked)
	}
	if wantStatus == store.StatusReady {
		{
			failed := st.FailedResources()
			assert.FatalIf(t, len(failed) != 0, "recovered failure still reported: %v", failed)
		}
	}
	assert.FatalIf(
		t, (*testCaseInput) ==
			"prior primary failure" && info.String() != "original failure", "prior primary failure was reset: %+v",
		info,
	)
}

type testDispatchNodeCanonicalReadCaseInputs struct {
	stage        *string
	replacedType *bool
}

func testDispatchNodeCanonicalReadCase(t *testing.T, testCaseInput testDispatchNodeCanonicalReadCaseInputs) {
	t.Helper()

	s := store.New()
	hr := &manifest.HelmRelease{
		ChartRef:  &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: "chart"},
		Name:      "release",
		Namespace: "apps",
	}
	fresh := hr.Clone()
	fresh.Values = map[string]any{"current": true}
	s.AddObject(hr)
	id := hr.Named()
	replace := func() {
		if *testCaseInput.replacedType {
			s.AddObject(
				&manifest.RawObject{
					APIVersion: manifest.HelmReleaseDomain + "/v2",
					Kind:       manifest.KindHelmRelease,
					Name:       id.Name,
					Namespace:  id.Namespace,
				},
			)
		} else {
			s.AddObject(fresh)
		}
	}
	c := base.New(s, task.NewBounded(2))
	if (*testCaseInput.stage) == "preflight" {
		s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending})
		replace()
	} else {
		s.UpdateFailure(
			id,
			&manifest.DependencyFailedError{
				Parent: id,
				Failed: []manifest.NamedResource{testutil.HelmReleaseID("apps", "dependency")},
			},
		)
		s.AddListener(store.EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
			if payload.(store.StatusInfo).Status == store.StatusPending {
				replace()
			}
		}, false)
	}
	called := false
	execution := controllertest.NewExecution(c, id)
	execution.Dispatch(
		t.Context(),
		manifest.DrainNone,
		execution,
		func(execution *base.Execution, _ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
			if (*testCaseInput.stage) == "retry" {
				s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending})
				assert.FatalIf(
					t,
					execution.Reads.Valid(),
					"%v",
					"replacement did not invalidate original attempt",
				)
				return nil
			}
			called = true
			assert.FatalIf(
				t,
				obj != fresh,
				"%v",
				"reconciliation retained the replaced canonical object",
			)
			return nil
		},
	)
	if (*testCaseInput.stage) == "retry" {
		execution.Retire()
		execution = controllertest.NewExecution(c, id)
		execution.Dispatch(
			t.Context(),
			manifest.DrainNone,
			execution,
			func(_ *base.Execution, _ context.Context, obj *manifest.HelmRelease, _ manifest.DrainLevel) error {
				called = true
				assert.FatalIf(t, obj != fresh, "%v", "fresh attempt retained replaced declaration")
				return nil
			},
		)
	}
	defer execution.Close()
	assert.FatalIf(
		t,
		called == (*testCaseInput.replacedType),
		"reconciled=%t, replaced type=%t",
		called,
		(*testCaseInput.replacedType),
	)
	info, _ := s.GetStatus(id)
	assert.FatalIf(
		t, (*testCaseInput.replacedType) &&
			info.Outcome() != store.OutcomeSkipped, "missing typed snapshot status=%v",
		info,
	)
}
