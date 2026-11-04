package depwait

import (
	"context"
	"fmt"
	"strings"
	"testing"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

type crdPromotion struct {
	store       *store.Store
	materialize bool
	calls       []manifest.NamedResource
}

func (e *crdPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }
func (e *crdPromotion) Promote(_ context.Context, _ *store.ReadSet, id manifest.NamedResource) (bool, error) {
	e.calls = append(e.calls, id)
	if !e.materialize {
		return false, nil
	}
	e.store.AddObject(
		&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Namespace: id.Namespace, Name: id.Name},
	)
	return true, nil
}

func TestClassify_MissingCRDs(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		for _, drain := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
			for _, state := range []string{"absent", "explicit namespace", "object", "ready status", "pending status", "failed status", "promoted", "promotion miss", "ConfigMap", "Secret", "Widget", "Kustomization", "HelmRelease", "CustomResourceDefinitionList", "valid expression", "invalid expression", "present invalid expression"} {
				t.Run(fmt.Sprintf("enabled_%t/drain_%d/%s", enabled, drain, state), func(t *testing.T) {
					t.Parallel()
					testClassifyMissingCRDsCase(
						t,
						testClassifyMissingCRDsCaseInputs{enabled: &enabled, drain: &drain, state: &state},
					)
				},
				)
			}
		}
	}
}

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestClassify_MissingCRDs_NoAllocations(t *testing.T) {
	s := store.New()
	ref := manifest.DependencyRef{
		Group: apix.GroupName,
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "widgets.example.com",
	}
	for _, enabled := range []bool{false, true} {
		w := &Waiter{Store: s, AllowMissingCRDs: enabled}
		{
			{
				got := testing.AllocsPerRun(100, func() { w.Classify(t.Context(), ref, manifest.DrainCascade) })
				assert.FatalIf(t, got != 0, "enabled=%t allocations=%v", enabled, got)
			}
		}
	}
}

type testClassifyMissingCRDsCaseInputs struct {
	enabled *bool
	drain   *manifest.DrainLevel
	state   *string
}

func testClassifyMissingCRDsCase(t *testing.T, testCaseInput testClassifyMissingCRDsCaseInputs) {
	t.Helper()

	s := store.New()
	ref := manifest.DependencyRef{
		Group: apix.GroupName,
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "widgets.example.com",
	}
	want := schedule.Blocked
	msg := ""
	if (*testCaseInput.drain) >= manifest.DrainCascade {
		want = schedule.DependencyFailed
		msg = "dependency not found"
		if *testCaseInput.enabled {
			want = schedule.Terminal
			msg = ""
		}
	}
	var e *crdPromotion
	switch *testCaseInput.state {
	case "valid expression":
		ref.ReadyExpr = new("false")
	case "explicit namespace":
		ref.Namespace = "explicit"
	case "object", "present invalid expression":
		s.AddObject(&manifest.RawObject{APIVersion: ref.Group + "/v1", Kind: ref.Kind, Name: ref.Name})
		want = schedule.Terminal
		msg = ""
		if (*testCaseInput.state) == "present invalid expression" {
			ref.ReadyExpr = new("dep.((")
		}
	case "ready status", "pending status", "failed status":
		status := store.StatusReady
		if (*testCaseInput.state) == "pending status" {
			status = store.StatusPending
		}
		if (*testCaseInput.state) == "failed status" {
			status = store.StatusFailed
		}
		s.UpdateStatus(
			ref.NamedResource,
			store.StatusInfo{Status: status, Reason: "", Message: "state"},
		)
		want = schedule.Terminal
		msg = ""
	case "promoted", "promotion miss":
		e = &crdPromotion{store: s, materialize: (*testCaseInput.state) == "promoted"}
		if e.materialize {
			want = schedule.Terminal
			msg = ""
		}
	case "ConfigMap",
		"Secret",
		"Widget",
		"Kustomization",
		"HelmRelease",
		"CustomResourceDefinitionList":
		ref.Kind = (*testCaseInput.state)
		ref.Namespace = "apps"
		want = schedule.Blocked
		msg = ""
		if (*testCaseInput.drain) >= manifest.DrainCascade {
			want = schedule.DependencyFailed
			msg = "dependency not found"
		}
	case "invalid expression":
		ref.ReadyExpr = new("dep.((")
		want = schedule.DependencyFailed
	}
	w := &Waiter{Store: s, AllowMissingCRDs: (*testCaseInput.enabled)}
	if e != nil {
		w.Existence = e
	}
	got := w.Classify(t.Context(), ref, (*testCaseInput.drain))
	assert.Equal(t, got.Kind, want)
	if (*testCaseInput.state) == "invalid expression" {
		{
			assert.FatalIf(
				t,
				!strings.HasPrefix(got.Message, "readyExpr:"),
				"invalid expression must fail: %+v",
				got,
			)
		}
	} else {
		assert.Equal(t, got.Message, msg)
	}
	if e != nil {
		assert.Diff(t, e.calls, []manifest.NamedResource{ref.NamedResource})
		assert.Equal(t, s.GetObject(ref.NamedResource) != nil, e.materialize)
	}
	if (*testCaseInput.state) == "absent" || (*testCaseInput.state) == "explicit namespace" {
		assert.Equal(t, s.GetObject(ref.NamedResource) == nil, true)
		_, ok := s.GetStatus(ref.NamedResource)
		assert.Equal(t, ok, false)
		assert.Equal(t, s.GetArtifact(ref.NamedResource, store.ArtifactContent) == nil, true)
	}
}
