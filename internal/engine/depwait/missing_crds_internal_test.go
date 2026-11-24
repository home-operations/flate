package depwait

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/selection"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/internal/engine/schedule"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type crdPromotion struct {
	store       *store.Store
	materialize bool
	calls       []manifest.NamedResource
}

func (e *crdPromotion) IsFileIndexed(manifest.NamedResource) bool { return true }

//nolint:gocritic // The admission contract requires an independent request snapshot.
func (e *crdPromotion) Promote(
	_ context.Context,
	request selection.PrepareRequest,
	id manifest.NamedResource,
) (bool, error) {
	e.calls = append(e.calls, id)
	if !e.materialize {
		return false, nil
	}
	e.store.AddObject(
		&manifest.RawObject{APIVersion: id.Group + "/v1", Kind: id.Kind, Namespace: id.Namespace, Name: id.Name},
	)
	parent := request.Reads.ID()
	request.Reads.Retire()
	request.Reads.Init(e.store, parent, nil)
	return true, nil
}

func TestClassify_MissingCRDs(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		for _, drain := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
			for _, state := range []string{"absent", "explicit namespace", "foreign group", "object", "supplied", "ready status", "pending status", "failed status", "promoted", "promotion miss", "ConfigMap", "Secret", "Widget", "Kustomization", "HelmRelease", "CustomResourceDefinitionList", "valid expression", "invalid expression", "present invalid expression"} {
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
	for _, tc := range []struct {
		name          string
		level         manifest.DrainLevel
		content       bool
		want          schedule.Kind
		wantErr       error
		wantTolerated bool
	}{
		{name: "missing artifact before cascade", want: schedule.Blocked},
		{name: "missing artifact at cascade", level: manifest.DrainCascade, want: schedule.Terminal, wantErr: manifest.ErrObjectNotFound},
		{name: "required declaration stale content before cascade", content: true, want: schedule.Blocked},
		{name: "required declaration stale content at cascade", content: true, level: manifest.DrainCascade, want: schedule.Terminal, wantTolerated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			ref := manifest.DependencyRef{
				Group: apix.GroupName, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com",
				Artifact: true, RequiresDeclaration: tc.content,
			}
			if tc.content {
				s.SetArtifact(ref.NamedResource, &store.SourceArtifact{FS: fstest.MapFS{}})
			}
			reads := captureState(t, s)
			promotion := &crdPromotion{store: s}
			w := Waiter{
				Store: reads, Admission: selection.PrepareRequest{Reads: reads},
				Missing: crdMissingPolicy(true), Existence: promotion,
			}
			got := w.Classify(t.Context(), ref, tc.level)
			assert.Equal(t, got.Kind, tc.want)
			assert.Equal(t, got.ToleratedCRD, tc.wantTolerated)
			assert.Equal(t, got.Message, "")
			if tc.wantErr != nil {
				assert.Equal(t, errors.Is(got.Err, tc.wantErr), true)
				assert.Equal(t, errors.Is(got.Err, manifest.ErrFlux), true)
				assert.FatalIf(t, got.Err == nil, "missing artifact error: %+v", got)
				assert.Equal(t, got.Err.Error(),
					"source CustomResourceDefinition.apiextensions.k8s.io/widgets.example.com artifact not found",
				)
			} else {
				assert.NoError(t, got.Err)
			}
			assert.Diff(t, promotion.calls, []manifest.NamedResource{ref.NamedResource})
			assert.Equal(t, s.GetObject(ref.NamedResource) == nil, true)
			_, status := s.GetStatus(ref.NamedResource)
			assert.Equal(t, status, false)
			assert.Equal(t, s.GetArtifact(ref.NamedResource, store.ArtifactContent) != nil, tc.content)
		})
	}
}

func TestAssess_ToleratedCRDs(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{
		Group: apix.GroupName, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.com",
	}
	for _, tc := range []struct {
		name         string
		materializer bool
		want         []manifest.NamedResource
	}{
		{name: "different authored gates", want: []manifest.NamedResource{id}},
		{name: "materializer gate", materializer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			parent := manifest.NamedResource{Kind: manifest.KindResourceSet, Name: "app"}
			s.SetArtifact(id, &store.SourceArtifact{FS: fstest.MapFS{}})
			var reads store.ReadSet
			var reuse [1]store.Input
			reads.Init(s, parent, reuse[:])
			reads.GetObject(parent)
			if tc.materializer {
				reads.RequireMaterializer(id)
			} else {
				reads.Require(manifest.DependencyRef{NamedResource: id})
				reads.Require(manifest.DependencyRef{NamedResource: id, Artifact: true})
			}
			waiter := Waiter{Store: &reads, Parent: parent, Missing: crdMissingPolicy(true)}
			assert.NoError(t, waiter.Assess(t.Context(), &reads, manifest.DrainCascade, AssessGates))
			assert.Diff(t, slices.Collect(reads.ToleratedCRDs()), tc.want)

			err := waiter.Assess(t.Context(), &reads, manifest.DrainNone, AssessGates)
			_, blocked := errors.AsType[*BlockedError](err)
			assert.Equal(t, blocked, true)
			assert.Equal(t, len(slices.Collect(reads.ToleratedCRDs())), 0)

			assert.NoError(t, waiter.Assess(t.Context(), &reads, manifest.DrainCascade, AssessGates))
			assert.Diff(t, slices.Collect(reads.ToleratedCRDs()), tc.want)
			reads.Retire()
			for range s.Sweep() {
			}
			reads.ReleaseCapture()
			reads.Init(s, parent, reuse[:])
			defer reads.Retire()
			reads.GetObject(parent)
			reads.Require(manifest.DependencyRef{NamedResource: id})
			assert.Equal(t, len(slices.Collect(reads.ToleratedCRDs())), 0)
		})
	}
}

//nolint:paralleltest // testing.AllocsPerRun changes process-wide GOMAXPROCS.
//nolint:paralleltest // Allocation measurements require serial execution while testing.AllocsPerRun sets GOMAXPROCS.
func TestClassify_MissingCRDs_NoAllocations(t *testing.T) {
	s := store.New()
	ref := manifest.DependencyRef{
		Group: apix.GroupName,
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "widgets.example.com",
	}
	for _, enabled := range []bool{false, true} {
		w := &Waiter{Store: captureState(t, s), Missing: crdMissingPolicy(enabled)}
		{
			got := testing.AllocsPerRun(100, func() { w.Classify(t.Context(), ref, manifest.DrainCascade) })
			assert.FatalIf(t, got != 0, "enabled=%t allocations=%v", enabled, got)
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
	want := schedule.Terminal
	msg := ""
	switch {
	case (*testCaseInput.drain) < manifest.DrainCascade:
		want = schedule.Blocked
	case !*testCaseInput.enabled:
		want = schedule.DependencyFailed
		msg = "dependency not found"
	}
	var e *crdPromotion
	switch *testCaseInput.state {
	case "foreign group":
		ref.Group = "example.test"
		if (*testCaseInput.drain) >= manifest.DrainCascade {
			want = schedule.DependencyFailed
			msg = "dependency not found"
		}
	case "valid expression":
		ref.ReadyExpr = new("false")
	case "explicit namespace":
		ref.Namespace = "explicit"
	case "object", "supplied", "present invalid expression":
		obj := &manifest.RawObject{APIVersion: ref.Group + "/v1", Kind: ref.Kind, Name: ref.Name}
		role := map[string]store.DeclarationRole{"supplied": store.SuppliedDeclaration}[*testCaseInput.state]
		s.AddDeclaration(store.Declaration{Obj: obj, Role: role})
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
	reads := captureState(t, s)
	w := &Waiter{
		Store:     reads,
		Admission: selection.PrepareRequest{Reads: reads},
		Missing:   crdMissingPolicy(*testCaseInput.enabled),
	}
	if e != nil {
		w.Existence = e
	}
	got := w.Classify(t.Context(), ref, (*testCaseInput.drain))
	assert.Equal(t, got.Kind, want)
	assert.NoError(t, got.Err)
	if (*testCaseInput.state) == "invalid expression" {
		assert.FatalIf(
			t,
			!strings.HasPrefix(got.Message, "readyExpr:"),
			"invalid expression must fail: %+v",
			got,
		)
	} else {
		assert.Equal(t, got.Message, msg)
	}
	if e != nil {
		assert.Diff(t, e.calls, []manifest.NamedResource{ref.NamedResource})
		assert.Equal(t, s.GetObject(ref.NamedResource) != nil, e.materialize)
	}
	if (*testCaseInput.state) == "absent" || (*testCaseInput.state) == "explicit namespace" ||
		(*testCaseInput.state) == "foreign group" {
		assert.Equal(t, s.GetObject(ref.NamedResource) == nil, true)
		_, ok := s.GetStatus(ref.NamedResource)
		assert.Equal(t, ok, false)
		assert.Equal(t, s.GetArtifact(ref.NamedResource, store.ArtifactContent) == nil, true)
	}
}
