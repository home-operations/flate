package store

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/testutil"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func newCM(name, ns string) *manifest.ConfigMap {
	return &manifest.ConfigMap{Name: name, Namespace: ns}
}

func TestStore_Snapshot(t *testing.T) {
	t.Parallel()
	id := testutil.HelmReleaseID("apps", "reader")
	resourceSet := &manifest.ResourceSet{Name: id.Name, Namespace: id.Namespace}
	for _, tc := range []struct {
		name   string
		obj    manifest.BaseManifest
		id     manifest.NamedResource
		status Status
	}{
		{"typed", &manifest.HelmRelease{Name: id.Name, Namespace: id.Namespace}, id, StatusReady},
		{"raw canonical identity", &manifest.RawObject{APIVersion: manifest.HelmReleaseDomain + "/v2", Kind: id.Kind, Name: id.Name, Namespace: id.Namespace}, id, StatusReady},
		{"statusless", newCM(id.Name, id.Namespace), newCM(id.Name, id.Namespace).Named(), StatusReady},
		{"ResourceSet pending", resourceSet, resourceSet.Named(), StatusPending},
		{"status only canonical", nil, id, StatusReady},
		{"status only foreign", nil, manifest.NamedResource{Kind: id.Kind, Group: "example.test", Name: id.Name}, StatusReady},
		{"absent", nil, id, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			if tc.obj != nil {
				st.AddObject(tc.obj)
			}
			if tc.status != "" {
				st.UpdateStatus(tc.id, StatusInfo{Status: tc.status, Reason: "", Message: "observed"})
			}
			statusful := tc.obj != nil && manifest.KindOf(tc.obj).Status || tc.obj == nil && tc.id.KindInfo().Status
			for _, requested := range []bool{false, true} {
				t.Run(fmt.Sprint(requested), func(t *testing.T) {
					t.Parallel()
					var observation Observation
					obj, observation := st.Snapshot(tc.id, requested)
					assert.Equal(t, obj, tc.obj)
					snapshot61, _ := st.Snapshot(tc.id, requested)
					assert.Equal(t, snapshot61, tc.obj)
					assert.Equal(t, observation.Statusful, statusful)
					want, _ := st.GetStatus(tc.id)
					if tc.obj != nil && !statusful && !requested {
						want = StatusInfo{}
					}
					assert.Equal(t, observation.StatusInfo, want)
					var conditions []Condition
					if requested && tc.status != "" {
						conditions = []Condition{readyCondition(tc.status, "", "observed")}
					}
					assert.Diff(t, observation.Conditions, conditions)
					if len(observation.Conditions) != 0 {
						observation.Conditions[0].Message = "mutated"
						var fresh Observation
						_, fresh = st.Snapshot(tc.id, true)
						assert.Diff(t, fresh.Conditions, conditions)
						assert.Equal(t, fresh.StatusInfo, want)
					}
				})
			}
		})
	}
}

func TestStore_AddObject_ContentDedup(t *testing.T) {
	t.Parallel()
	for _, insert := range []struct {
		name string
		add  func(*Store, manifest.BaseManifest)
	}{
		{"single", (*Store).AddObject},
		{"batch", func(s *Store, obj manifest.BaseManifest) { s.AddObjects([]manifest.BaseManifest{obj}) }},
	} {
		t.Run(insert.name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name      string
				newObject func(string) manifest.BaseManifest
			}{
				{"raw", func(value string) manifest.BaseManifest {
					return &manifest.RawObject{Kind: "Deployment", APIVersion: "apps/v1", Name: "app", Namespace: "ns", Spec: map[string]any{"replicas": value}}
				}},
				{"typed", func(value string) manifest.BaseManifest {
					return &manifest.ConfigMap{Name: "app", Namespace: "ns", Data: map[string]any{"value": value}}
				}},
			} {
				for _, distinct := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/distinct=%t", tc.name, distinct), func(t *testing.T) {
						t.Parallel()
						s := New()
						obj := tc.newObject("1")
						seen := 0
						s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
							seen++
							if s.GetObject(id) == nil {
								t.Error("listener cannot see rendered object")
							}
						}, false)
						insert.add(s, obj)
						equal := obj
						if distinct {
							equal = tc.newObject("1")
						}
						insert.add(s, equal)
						assert.Equal(t, seen, 1)
						changed := tc.newObject("2")
						insert.add(s, changed)
						assert.Equal(t, seen, 2)
						assert.Equal(t, s.GetObject(obj.Named()), changed)
					})
				}
			}
		})
	}
}

func TestStore_AddObject_ConcurrentDedup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		newObject func() manifest.BaseManifest
	}{
		{"typed", func() manifest.BaseManifest {
			return &manifest.ConfigMap{Name: "app", Namespace: "ns", Data: map[string]any{"key": "value"}}
		}},
		{"raw", func() manifest.BaseManifest {
			return &manifest.RawObject{Kind: "Deployment", APIVersion: "apps/v1", Name: "app", Spec: map[string]any{"replicas": 1}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			var seen atomic.Int64
			s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen.Add(1) }, false)
			var wg sync.WaitGroup
			for range 32 {
				wg.Go(func() { s.AddObject(tc.newObject()) })
			}
			wg.Wait()
			assert.Equal(t, seen.Load(), int64(1))
		})
	}
}

func TestStore_AddObject_NestedContent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		spec  map[string]any
		equal bool
	}{
		{"equal", map[string]any{"items": []any{map[string]any{"value": "same"}, true, int64(1), 1.5}, "extra": []string{"a"}}, true},
		{"nested map changed", map[string]any{"items": []any{map[string]any{"value": "changed"}, true, int64(1), 1.5}, "extra": []string{"a"}}, false},
		{"slice length changed", map[string]any{"items": []any{map[string]any{"value": "same"}}, "extra": []string{"a"}}, false},
		{"scalar type changed", map[string]any{"items": []any{map[string]any{"value": "same"}, true, 1, 1.5}, "extra": []string{"a"}}, false},
		{"fallback changed", map[string]any{"items": []any{map[string]any{"value": "same"}, true, int64(1), 1.5}, "extra": []string{"b"}}, false},
		{"missing key", map[string]any{"items": []any{map[string]any{"value": "same"}, true, int64(1), 1.5}, "other": []string{"a"}}, false},
		{"nil spec", nil, false},
		{"empty spec", map[string]any{}, false},
	}
	base := &manifest.RawObject{APIVersion: "example.test/v1", Kind: "Widget", Name: "app", Spec: cases[0].spec}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			seen := 0
			s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen++ }, false)
			s.AddObject(base)
			clone := *base
			clone.Spec = tc.spec
			s.AddObject(&clone)
			want := 2
			if tc.equal {
				want = 1
			}
			assert.Equal(t, seen, want)
		})
	}
}

func TestObjectEqual_ConcurrentProjection(t *testing.T) {
	t.Parallel()
	parse := func() *manifest.Kustomization {
		t.Helper()
		obj, _, _, err := manifest.ParseDoc(map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
			"spec":     map[string]any{"path": "./app", "sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"}},
		}, manifest.ParseDocOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return obj.(*manifest.Kustomization)
	}
	first, second := parse(), parse()
	s := New()
	s.AddObject(first)
	start := make(chan struct{})
	var ready, workers sync.WaitGroup
	ready.Add(2)
	workers.Go(func() {
		ready.Done()
		<-start
		if _, err := first.RenderDocument(); err != nil {
			t.Error(err)
		}
	})
	workers.Go(func() {
		ready.Done()
		<-start
		s.AddObject(second)
	})
	ready.Wait()
	close(start)
	workers.Wait()
	got := s.GetObject(first.Named()).(*manifest.Kustomization)
	assert.Equal(t, got.Named(), first.Named())
	assert.Equal(t, got.Path, "./app")
	assert.Equal(t, got.SourceRef, second.SourceRef)
}

func TestObjectEqual_KustomizationContent(t *testing.T) {
	t.Parallel()
	parse := func(t *testing.T) *manifest.Kustomization {
		t.Helper()
		obj, _, _, err := manifest.ParseDoc(map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
			"spec": map[string]any{
				"path": "./app", "sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"},
				"postBuild": map[string]any{"substitute": map[string]any{"KEY": "same"}},
			},
		}, manifest.ParseDocOptions{})
		assert.NoError(t, err)
		return obj.(*manifest.Kustomization)
	}
	populate := func(t *testing.T, k *manifest.Kustomization) {
		t.Helper()
		_, err := k.RenderDocument()
		assert.NoError(t, err)
	}
	for _, tc := range []struct {
		name    string
		prepare func(*testing.T, *manifest.Kustomization, *manifest.Kustomization) *manifest.Kustomization
		change  func(*manifest.Kustomization)
		equal   bool
	}{
		{name: "cold caches", equal: true},
		{name: "one populated cache", equal: true, prepare: func(t *testing.T, a, b *manifest.Kustomization) *manifest.Kustomization {
			t.Helper()
			populate(t, a)
			return b
		}},
		{name: "independent populated caches", equal: true, prepare: func(t *testing.T, a, b *manifest.Kustomization) *manifest.Kustomization {
			t.Helper()
			populate(t, a)
			populate(t, b)
			return b
		}},
		{name: "absent cache", equal: true, prepare: func(t *testing.T, a, b *manifest.Kustomization) *manifest.Kustomization {
			t.Helper()
			populate(t, a)
			return &manifest.Kustomization{Name: b.Name, Namespace: b.Namespace, KustomizationSpec: b.KustomizationSpec}
		}},
		{name: "name", change: func(k *manifest.Kustomization) { k.Name = "other" }},
		{name: "namespace", change: func(k *manifest.Kustomization) { k.Namespace = "other" }},
		{name: "source", change: func(k *manifest.Kustomization) { k.SourceRef.Name = "other" }},
		{name: "path", change: func(k *manifest.Kustomization) { k.Path = "./other" }},
		{name: "labels", change: func(k *manifest.Kustomization) { k.Labels = map[string]string{"app": "other"} }},
		{name: "annotations", change: func(k *manifest.Kustomization) { k.Annotations = map[string]string{"note": "other"} }},
		{name: "nil and empty labels", change: func(k *manifest.Kustomization) { k.Labels = map[string]string{} }},
		{name: "nil and empty annotations", change: func(k *manifest.Kustomization) { k.Annotations = map[string]string{} }},
		{name: "nil and empty spec slice", change: func(k *manifest.Kustomization) { k.Components = []string{} }},
		{name: "nested spec", change: func(k *manifest.Kustomization) { k.PostBuild.Substitute["KEY"] = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := parse(t), parse(t)
			if tc.prepare != nil {
				second = tc.prepare(t, first, second)
			}
			if tc.change != nil {
				second = second.Clone()
				tc.change(second)
			}
			assert.Equal(t, objectEqual(first, second), tc.equal)
			assert.Equal(t, objectEqual(second, first), tc.equal)
			s := New()
			seen := 0
			s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { seen++ }, false)
			s.AddObject(first)
			s.AddObject(second)
			want := 2
			if tc.equal {
				want = 1
			}
			assert.Equal(t, seen, want)
		})
	}
}

func TestObjectEqual_RawObjectFields(t *testing.T) {
	t.Parallel()
	rawType := reflect.TypeFor[manifest.RawObject]()
	fields := make([]string, rawType.NumField())
	for i := range fields {
		fields[i] = rawType.Field(i).Name
	}
	assert.Diff(t, fields, []string{"Kind", "APIVersion", "Name", "Namespace", "Spec"})
	base := &manifest.RawObject{
		Kind: "Widget", APIVersion: "example.test/v1", Name: "app", Namespace: "ns",
		Spec: map[string]any{"items": []any{map[string]any{"value": "same"}}},
	}
	assert.Equal(t, objectEqual(base, base), true)
	for _, tc := range []struct {
		name   string
		change func(*manifest.RawObject)
		equal  bool
	}{
		{name: "equal distinct pointers", equal: true},
		{name: "kind", change: func(obj *manifest.RawObject) { obj.Kind = "Other" }},
		{name: "api version", change: func(obj *manifest.RawObject) { obj.APIVersion = "example.test/v2" }},
		{name: "name", change: func(obj *manifest.RawObject) { obj.Name = "other" }},
		{name: "namespace", change: func(obj *manifest.RawObject) { obj.Namespace = "other" }},
		{name: "spec", change: func(obj *manifest.RawObject) { obj.Spec = nil }},
		{name: "nested spec", change: func(obj *manifest.RawObject) {
			obj.Spec["items"].([]any)[0].(map[string]any)["value"] = "changed"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := base.Clone()
			assert.Equal(t, candidate == base, false)
			if tc.change != nil {
				tc.change(candidate)
			}
			assert.Equal(t, objectEqual(base, candidate), tc.equal)
			assert.Equal(t, objectEqual(candidate, base), tc.equal)
		})
	}
	assert.Diff(t, base.Spec, map[string]any{"items": []any{map[string]any{"value": "same"}}})
	// nil and empty maps stay distinct, as under reflect.DeepEqual.
	assert.Equal(t, objectEqual(&manifest.RawObject{Spec: map[string]any{}}, &manifest.RawObject{}), false)
}

func TestStore_AddObjectIdempotent(t *testing.T) {
	t.Parallel()
	s := New()
	cm := newCM("a", "ns")
	id := cm.Named()

	count := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)

	s.AddObject(cm)
	s.AddObject(cm)
	assert.Equal(t, count, 1) // exactly 1 event for two identical adds
	if got := s.GetObject(id); got == nil {
		t.Errorf("GetObject after AddObject returned nil")
	}
}

// TestStore_AddObject_CloneTriggersUpdate guards the immutability
// contract: callers that need to "mutate" a stored object must clone
// it, mutate the clone, and re-AddObject. The store's reflect.DeepEqual
// dedup recognizes the clone as different and fires EventObjectAdded.
func TestStore_AddObject_CloneTriggersUpdate(t *testing.T) {
	t.Parallel()
	s := New()
	cm := newCM("a", "ns")
	id := cm.Named()

	var seen int
	s.AddListener(EventObjectAdded, func(other manifest.NamedResource, _ any) {
		if other == id {
			seen++
		}
	}, false)

	s.AddObject(cm)
	clone := *cm
	clone.Data = map[string]any{"k": "v"}
	s.AddObject(&clone)

	assert.Equal(t, seen, 2) // initial + clone

	got, ok := s.GetObject(id).(*manifest.ConfigMap)
	if !ok {
		t.Fatalf("expected *manifest.ConfigMap, got %T", s.GetObject(id))
	}
	if got.Data["k"] != "v" {
		t.Errorf("store did not pick up the cloned value: %+v", got.Data)
	}
}

func TestStore_AddObjects_DispatchesAfterAllWrites(t *testing.T) {
	t.Parallel()
	s := New()
	a := newCM("a", "ns")
	b := &manifest.RawObject{Kind: manifest.KindConfigMap, APIVersion: "example.test/v1", Name: "a", Namespace: "ns"}
	c := &manifest.RawObject{Kind: "Deployment", APIVersion: "apps/v1", Name: "b", Namespace: "ns", Spec: map[string]any{"replicas": 1}}
	idA, idB, idC := a.Named(), b.Named(), c.Named()
	seen := map[manifest.NamedResource]int{}
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		seen[id]++
		assert.Equal(t, payload.(manifest.BaseManifest).Named(), id)
		assert.Equal(t, s.GetObject(idA), manifest.BaseManifest(a))
		assert.Equal(t, s.GetObject(idC), manifest.BaseManifest(c))
		if got := s.GetObject(idB); got == nil || got.Named() != idB {
			t.Errorf("batch listener cannot see foreign sibling: %v", got)
		}
	}, false)

	s.AddObjects([]manifest.BaseManifest{a, b, c})
	s.AddObjects([]manifest.BaseManifest{newCM("a", "ns"), b.Clone(), c.Clone()})
	assert.Diff(t, seen, map[manifest.NamedResource]int{idA: 1, idB: 1, idC: 1})
	newVersion := b.Clone()
	newVersion.APIVersion = "example.test/v2"
	assert.Equal(t, newVersion.Named(), idB)
	s.AddObjects([]manifest.BaseManifest{newVersion})
	assert.Equal(t, s.GetObject(idB), manifest.BaseManifest(newVersion))
	assert.Equal(t, len(s.ListObjects("example.test", manifest.KindConfigMap)), 1)
	assert.Diff(t, seen, map[manifest.NamedResource]int{idA: 1, idB: 2, idC: 1})
}

func TestStore_AddListener_Flush(t *testing.T) {
	t.Parallel()
	s := New()
	s.AddObject(newCM("a", "ns"))
	s.AddObject(newCM("b", "ns"))

	var got []string
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		got = append(got, id.Name)
	}, true)
	assert.Equal(t, len(got), 2) // both seeds replayed
}

func TestStore_AdmissionInvalidatesWithoutStatusReset(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	id := obj.Named()
	s.AddObject(obj)
	s.SetIncluded(id, false)
	s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: ReasonUnchanged, Message: "unchanged"})
	var reads ReadSet
	reads.Init(s, id, nil)
	reads.GetObject(id)
	if reads.Admitted() {
		t.Fatal("excluded execution admitted")
	}
	arrivals := 0
	s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
	s.SetIncluded(id, true)
	if reads.Valid() || arrivals != 0 {
		t.Fatalf("valid=%t arrivals=%d", reads.Valid(), arrivals)
	}
	if info, _ := s.GetStatus(id); info.Status != StatusReady || info.Outcome() != OutcomeUnchanged {
		t.Fatalf("admission rewrote status: %+v", info)
	}
}

func TestStore_UpdateStatus_Idempotent(t *testing.T) { //nolint:tparallel // Subtests exercise one ordered transition history.
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	var events []StatusInfo
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		events = append(events, payload.(StatusInfo))
	}, false)
	for _, tc := range []struct {
		name         string
		status       Status
		reason, want StatusReason
		count        int
	}{
		{"initial pending", StatusPending, "", ReasonReconciling, 1},
		{"identical pending", StatusPending, "", ReasonReconciling, 1},
		{"skip", StatusReady, ReasonSkipped, ReasonSkipped, 2},
		{"identical skip", StatusReady, ReasonSkipped, ReasonSkipped, 2},
		{"reason only", StatusReady, ReasonUnchanged, ReasonUnchanged, 3},
		{"ordinary reset", StatusReady, "", ReasonSucceeded, 4},
		{"identical ready", StatusReady, "", ReasonSucceeded, 4},
		{"custom", StatusReady, "Custom", "Custom", 5},
		{"failed reset", StatusFailed, "", ReasonFailed, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.UpdateStatus(id, StatusInfo{Status: tc.status, Reason: tc.reason, Message: "same message"})
			info, ok := s.GetStatus(id)
			if !ok || info.Status != tc.status || info.Reason != tc.want || len(events) != tc.count {
				t.Fatalf("status=%+v present=%v events=%d, want reason=%s events=%d", info, ok, len(events), tc.want, tc.count)
			}
			if events[len(events)-1] != info {
				t.Fatal("event lost status projection")
			}
			var replay StatusInfo
			u := s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) { replay = payload.(StatusInfo) }, true)
			u()
			if replay != info {
				t.Fatalf("replay=%+v, want %+v", replay, info)
			}
		})
	}
	assert.Equal(t, events[1].Reason, ReasonSkipped)
}

func TestStatusInfo_Outcome(t *testing.T) {
	t.Parallel()
	failure := &dependencyFailure{ids: []manifest.NamedResource{testutil.HelmReleaseID("", "dep")}}
	for _, tc := range []struct {
		name        string
		info        StatusInfo
		outcome     Outcome
		unavailable bool
	}{
		{name: "absent", outcome: OutcomePending},
		{name: "ready", info: StatusInfo{Status: StatusReady}, outcome: OutcomePassed},
		{name: "custom ready", info: StatusInfo{Status: StatusReady, Reason: "Custom", Message: "skipped: x"}, outcome: OutcomePassed},
		{name: "misleading skip", info: StatusInfo{Status: StatusReady, Message: "skipped: x"}, outcome: OutcomePassed},
		{name: "misleading unchanged", info: StatusInfo{Status: StatusReady, Message: "unchanged"}, outcome: OutcomePassed},
		{name: "misleading suspended", info: StatusInfo{Status: StatusReady, Message: "suspended"}, outcome: OutcomePassed},
		{name: "empty skipped", info: StatusInfo{Status: StatusReady, Reason: ReasonSkipped}, outcome: OutcomeSkipped, unavailable: true},
		{name: "empty unchanged", info: StatusInfo{Status: StatusReady, Reason: ReasonUnchanged}, outcome: OutcomeUnchanged},
		{name: "empty suspended", info: StatusInfo{Status: StatusReady, Reason: ReasonSuspended}, outcome: OutcomeSuspended, unavailable: true},
		{name: "skipped diagnostic", info: StatusInfo{Status: StatusReady, Reason: ReasonSkipped, Message: "reworded"}, outcome: OutcomeSkipped, unavailable: true},
		{name: "unchanged diagnostic", info: StatusInfo{Status: StatusReady, Reason: ReasonUnchanged, Message: "suspended"}, outcome: OutcomeUnchanged},
		{name: "suspended diagnostic", info: StatusInfo{Status: StatusReady, Reason: ReasonSuspended, Message: "unchanged"}, outcome: OutcomeSuspended, unavailable: true},
		{name: "failed before skip", info: StatusInfo{Status: StatusFailed, Reason: ReasonSkipped}, outcome: OutcomeFailed, unavailable: true},
		{name: "blocked before skip", info: StatusInfo{Status: StatusFailed, Reason: ReasonSkipped, failure: failure}, outcome: OutcomeBlocked, unavailable: true},
		{name: "pending before suspension", info: StatusInfo{Status: StatusPending, Reason: ReasonSuspended}, outcome: OutcomePending},
		{name: "refetching", info: StatusInfo{Status: StatusPending, Reason: StatusReason("CustomPending"), Message: "suspended"}, outcome: OutcomePending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.info.Outcome(), tc.outcome)
			assert.Equal(t, tc.info.IsUnavailable(), tc.unavailable)
		})
	}
}

func TestStore_SetCondition_NonReadyFiresStatusEvent(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("ns", "k")
	s.UpdateStatus(id, StatusInfo{Status: StatusPending, Reason: "", Message: "starting"})
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) {
		statusEvents++
	}, false)
	s.SetCondition(id, Condition{Type: ConditionHealthy, Status: metav1.ConditionTrue, Reason: "Healthy"})
	assert.Equal(t, statusEvents, 1)
	var observation Observation
	_, observation = s.Snapshot(id, true)
	conds := observation.Conditions
	if got := len(conds); got != 2 {
		t.Fatalf("expected 2 conditions, got %d", got)
	}
}

func TestStore_SetCondition_ReadyFiresStatusEvent(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("ns", "k")
	var events []StatusInfo
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, payload any) {
		events = append(events, payload.(StatusInfo))
	}, false)
	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: string(ReasonSkipped), Message: "ok"}
	s.SetCondition(id, cond)
	var observation Observation
	_, observation = s.Snapshot(id, true)
	snapshot := observation.Conditions
	cond.Reason = "Custom"
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	assert.Equal(t, len(events), 2)
	assert.Equal(t, events[0].Reason, ReasonSkipped)
	assert.Equal(t, events[1].Reason, StatusReason("Custom"))
	info, ok := s.GetStatus(id)
	if !ok || info != events[1] || info.Outcome() != OutcomePassed {
		t.Fatalf("projected status = %+v", info)
	}
	assert.Equal(t, snapshot[0].Reason, string(ReasonSkipped))
}

func TestStore_SetCondition_IdenticalIsNoOp(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("ns", "k")
	statusEvents := 0
	s.AddListener(EventStatusUpdated, func(_ manifest.NamedResource, _ any) { statusEvents++ }, false)

	cond := Condition{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded}
	s.SetCondition(id, cond)
	s.SetCondition(id, cond)
	assert.Equal(t, statusEvents, 1) // identical SetCondition is a no-op
}

func TestStore_FailedResources(t *testing.T) {
	t.Parallel()
	s := New()
	assert.Equal(t, len(s.FailedResources()), 0) // empty store

	ks := &manifest.Kustomization{Name: "x", Namespace: ""}
	id := ks.Named()
	s.AddObject(ks)
	s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "boom"})
	assert.Equal(t, len(s.FailedResources()), 1)

	// Phantom guard: a SetCondition that races after DeleteObject must
	// not resurrect a failure entry for an id that no longer exists.
	s.DeleteObject(id)
	s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "phantom"}) // simulates the race
	assert.Equal(t, len(s.FailedResources()), 0)                                         // phantom entry must not leak
}

func TestStore_ListObjects_ByKindIndex(t *testing.T) {
	t.Parallel()
	for _, core := range []bool{false, true} {
		for _, insertForeignFirst := range []bool{false, true} {
			for _, withdrawForeignFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("core=%t/insertForeignFirst=%t/withdrawForeignFirst=%t", core, insertForeignFirst, withdrawForeignFirst), func(t *testing.T) {
					t.Parallel()
					s := New()
					var typed manifest.BaseManifest = &manifest.Kustomization{Name: "a", Namespace: "ns1"}
					if core {
						typed = newCM("a", "ns1")
					}
					id := typed.Named()
					foreign := &manifest.RawObject{APIVersion: "example.test/v1", Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
					pair := []manifest.BaseManifest{typed, foreign}
					if insertForeignFirst {
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

							if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
								t.Fatalf("listed objects changed (-want +got): %s", diff)
							}
						})
					}
					inventory := []manifest.BaseManifest{typed, foreign, other}
					slices.SortFunc(inventory, func(a, b manifest.BaseManifest) int { return a.Named().Compare(b.Named()) })
					if diff := cmp.Diff(s.ListObjects("", ""), inventory, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
						t.Fatalf("stored objects changed (-want +got): %s", diff)
					}
					pair = []manifest.BaseManifest{typed, foreign}
					if withdrawForeignFirst {
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
							if diff := cmp.Diff(s.ListObjects(remaining.Named().Group, id.Kind), []manifest.BaseManifest{remaining}, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
								t.Fatalf("stored objects changed (-want +got): %s", diff)
							}
						}
					}
					if diff := cmp.Diff(s.ListObjects("", ""), []manifest.BaseManifest{other}, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
						t.Fatalf("stored objects changed (-want +got): %s", diff)
					}
				})
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

	if len(first) != len(second) {
		t.Fatalf("lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i].Named(), second[i].Named()
		if a != b {
			t.Errorf("position %d differs between calls: %v vs %v", i, a, b)
		}
	}
	// Verify the order is actually sorted by (namespace, name) within
	// the same kind.
	for i := 1; i < len(first); i++ {
		prev, cur := first[i-1].Named(), first[i].Named()
		if prev.Compare(cur) > 0 {
			t.Errorf("unsorted at [%d]: %v > %v", i, prev, cur)
		}
	}
}

func TestStore_AddListener_Unsubscribe(t *testing.T) {
	t.Parallel()
	s := New()
	count := 0
	unsub := s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.AddObject(newCM("a", "ns"))
	unsub()
	s.AddObject(newCM("b", "ns"))
	assert.Equal(t, count, 1) // post-unsubscribe add is silent
}

func TestStore_SetArtifact(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: "GitRepository", Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	count := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) {
		count++
	}, false)
	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // idempotent
	assert.Equal(t, count, 1)
	got := s.GetArtifact(id)
	if got == nil {
		t.Errorf("GetArtifact: nil")
	}
}

// TestStore_SetArtifact_DistinctPointerDedup pins Item 7's primary
// dedup contract: a re-set with a DISTINCT POINTER but
// content-equal artifact is a no-op. The KS / HR re-emit case —
// rendered docs decode into fresh maps every reconcile — drives
// this in production.
func TestStore_SetArtifact_DistinctPointerDedup(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")
	makeArt := func() *Emission {
		return &Emission{

			Manifests: []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "cm-1", "namespace": "default"},
					"data":       map[string]any{"k": "v"},
				},
			},
		}
	}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, makeArt())
	s.SetArtifact(id, makeArt()) // fresh pointer, identical content
	s.SetArtifact(id, makeArt()) // fresh pointer, identical content

	assert.Equal(t, events, 1) // content-equal re-sets dedup
}

// TestStore_SetArtifact_PointerIdentityShortCircuit pins Item 7's
// pointer-identity fast path: re-setting the SAME pointer skips
// reflection entirely. Source-controller refresh loops cache their
// own SourceArtifact and re-publish it on every tick; the
// short-circuit cuts that hot path from ~58 ns (reflect.DeepEqual on
// aliased pointers) to ~17 ns (the map lookup alone).
func TestStore_SetArtifact_PointerIdentityShortCircuit(t *testing.T) {
	t.Parallel()
	s := New()
	id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "r"}
	art := &SourceArtifact{Kind: "GitRepository", URL: "https://example", LocalPath: "/tmp/x"}

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, art)
	s.SetArtifact(id, art) // same pointer
	s.SetArtifact(id, art) // same pointer

	assert.Equal(t, events, 1) // same-pointer re-set short-circuits
}

// TestStore_SetArtifact_DifferentContent pins the inverse: a
// re-set with DIFFERENT content fires an event.
func TestStore_SetArtifact_DifferentContent(t *testing.T) {
	t.Parallel()
	s := New()
	id := testutil.KustomizationID("flux-system", "apps")

	events := 0
	s.AddListener(EventArtifactUpdated, func(_ manifest.NamedResource, _ any) { events++ }, false)

	s.SetArtifact(id, &Emission{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-1"}},
	}})
	s.SetArtifact(id, &Emission{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm-2"}},
	}})

	assert.Equal(t, events, 2) // distinct content fires each time
}

func TestStore_ListenerPanicIsolated(t *testing.T) {
	t.Parallel()
	s := New()
	other := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		panic("boom")
	}, false)
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		other++
	}, false)
	s.AddObject(newCM("a", "ns")) // should not crash
	assert.Equal(t, other, 1)     // sibling listener still ran
}

// AddListener with flush=true replays existing store contents into the
// new listener. A panic in the listener during replay must not crash
// the caller — controllers attach with flush=true in Start() and a
// crash here would tear down the whole reconcile pipeline before the
// orchestrator's Run loop ever ticked. Parity with the live `fire`
// path in TestStore_ListenerPanicIsolated.
func TestStore_ListenerPanicRecoveredDuringReplay(t *testing.T) {
	t.Parallel()
	s := New()
	s.AddObject(newCM("a", "ns"))
	s.AddObject(newCM("b", "ns"))
	// The panicky listener attaches with flush=true → replay fires
	// against every existing object. Without panic recovery the test
	// would fail with a runtime panic.
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		panic("boom during replay")
	}, true)
	// Confirm the Store is still healthy after the recovered panic.
	other := 0
	s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		other++
	}, true)
	assert.Equal(t, other, 2) // store healthy after recovered replay panic
}

func TestStore_Concurrency(t *testing.T) {
	t.Parallel()
	s := New()
	const N = 200

	var wg sync.WaitGroup
	wg.Go(func() {
		for range N {
			s.AddObject(newCM("a", "ns"))
		}
	})
	wg.Go(func() {
		for range N {
			_ = s.ListObjects("", "ConfigMap")
		}
	})
	wg.Wait()
}

// TestStore_AddListener_FlushNoDoubleFire stresses the
// AddObject-vs-AddListener race that the fireUnderLock pattern closes.
// Without the fix, a flush=true listener registering while a sibling
// goroutine ran AddObject for the same id could observe the object
// TWICE: once via the live event (listener was added before the
// writer's listener-snapshot ran) and again via the replay snapshot
// (which captured the same object). The Coalescer hid this in
// production by deduping per-id reconciles, but a test counter
// surfaces it.
func TestStore_AddListener_FlushNoDoubleFire(t *testing.T) {
	t.Parallel()
	const iterations = 200
	for range iterations {
		s := New()
		// Seed enough objects that the replay snapshot is non-trivial
		// and the race window is reachable.
		const seed = 8
		for i := range seed {
			s.AddObject(newCM(fmt.Sprintf("seed-%d", i), "ns"))
		}

		var (
			counts   sync.Map // id → atomic count
			racerID  = manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "racer"}
			racerObj = newCM("racer", "ns")
		)

		var startGate, done sync.WaitGroup
		startGate.Add(1)

		done.Go(func() {
			startGate.Wait()
			s.AddObject(racerObj)
		})
		done.Go(func() {
			startGate.Wait()
			s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				v, _ := counts.LoadOrStore(id, new(atomic.Int64))
				v.(*atomic.Int64).Add(1)
			}, true)
		})
		startGate.Done()
		done.Wait()

		v, _ := counts.LoadOrStore(racerID, new(atomic.Int64))
		if got := v.(*atomic.Int64).Load(); got != 1 {
			t.Fatalf("racer fired %d times on iteration; want exactly 1", got)
		}
	}
}

// TestStore_AddListenerNoFlush_NoMissedConcurrentWrites pins that a
// listener registered with flush=false against a hot writer doesn't
// miss the next live event. Pre-fix, the non-flush branch took
// set.mu but not the store lock, so a writer could:
//  1. Lock its shard, mutate.
//  2. Capture set.snapshot() under set.mu (independent of the shard).
//  3. Release the shard.
//  4. Dispatch to the pre-add snapshot.
//
// while the registrar slid set.add(fn) between steps 2 and 4. fn
// silently misses the live event. Holding every shard's RLock
// around set.add closes the window (writers fire under their own
// shard's write lock, exclusive with the registrar's RLock). Test
// races N writers against AddListener and asserts the listener
// observes every AddObject made after its own goroutine acquired
// the lock.
func TestStore_AddListenerNoFlush_NoMissedConcurrentWrites(t *testing.T) {
	t.Parallel()
	s := New()
	const writers = 64
	var (
		writerWG      sync.WaitGroup
		registerReady = make(chan struct{})
		registered    = make(chan struct{})
		seen          atomic.Int64
	)
	// Pre-populate so AddObject takes the "update existing" path
	// uniformly — keeps test deterministic.
	for i := range writers {
		s.AddObject(newCM("init"+strings.Repeat("x", i), "ns"))
	}
	writerWG.Go(func() {
		<-registerReady
		// Spin writes while the registrar runs. Some will land before
		// AddListener returns; some after.
		for i := range writers {
			s.AddObject(newCM("post"+strings.Repeat("x", i), "ns"))
		}
		<-registered // Make sure registrar is fully attached.
		// One more guaranteed-post-registration write that the
		// listener MUST observe.
		s.AddObject(newCM("definite-post", "ns"))
	})
	close(registerReady)
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Name == "definite-post" {
			seen.Add(1)
		}
	}, false)
	close(registered)
	writerWG.Wait()
	if got := seen.Load(); got != 1 {
		t.Errorf("listener missed the post-registration definite-post event (saw %d, want 1) — non-flush race regression", got)
	}
}

func TestStore_ListenerSnapshots(t *testing.T) {
	t.Parallel()
	t.Run("concurrent delivery", func(t *testing.T) {
		t.Parallel()
		s := New()
		const (
			numListeners = 50
			numFirers    = 10
			eventsPer    = 100
		)
		totalEvents := int64(numFirers * eventsPer)
		wantPerListener := totalEvents

		counts := make([]atomic.Int64, numListeners)
		for i := range numListeners {
			s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
				counts[i].Add(1)
			}, false)
		}

		var wg sync.WaitGroup
		for g := range numFirers {
			wg.Go(func() {
				for n := range eventsPer {
					// Distinct names per (goroutine, event) so the
					// AddObject dedup never short-circuits — each call
					// must fire EventObjectAdded.
					name := fmt.Sprintf("g%d-n%d", g, n)
					s.AddObject(newCM(name, "ns"))
				}
			})
		}
		wg.Wait()

		for i := range numListeners {
			if got := counts[i].Load(); got != wantPerListener {
				t.Errorf("listener[%d] observed %d events, want %d (listener snapshot lost an event?)", i, got, wantPerListener)
			}
		}
	})
	t.Run("mutation during captured dispatch", func(t *testing.T) {
		t.Parallel()
		s := New()
		paused := make(chan struct{})
		resume := make(chan struct{})
		var seen []int
		var first, fifth Unsubscribe
		first = s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
			close(paused)
			<-resume
			seen = append(seen, 1)
			first()
			fifth = s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
				seen = append(seen, 5)
			}, false)
		}, false)
		second := s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
			seen = append(seen, 2)
		}, false)
		third := s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
			seen = append(seen, 3)
		}, false)

		var wg sync.WaitGroup
		wg.Go(func() { s.AddObject(newCM("captured", "ns")) })
		<-paused
		second()
		second()
		fourth := s.AddListener(EventObjectAdded, func(_ manifest.NamedResource, _ any) {
			seen = append(seen, 4)
		}, false)
		close(resume)
		wg.Wait()
		assert.Diff(t, seen, []int{1, 2, 3})

		seen = nil
		s.AddObject(newCM("future", "ns"))
		assert.Diff(t, seen, []int{3, 4, 5})
		third()
		fourth()
		fifth()
		seen = nil
		s.AddObject(newCM("empty", "ns"))
		assert.Equal(t, len(seen), 0)
	})
}

func TestStore_SetConditionLocked_StableSliceLength(t *testing.T) {
	t.Parallel()
	s := New()
	id := (&manifest.Kustomization{Name: "a", Namespace: "ns"}).Named()

	for i := range 100 {
		// Alternate status/message so every call is non-equal — the
		// no-op short-circuit must NOT kick in, forcing each call
		// through the overwrite branch.
		status := StatusPending
		if i%2 == 0 {
			status = StatusReady
		}
		s.UpdateStatus(id, StatusInfo{Status: status, Reason: "", Message: fmt.Sprintf("iter-%d", i)})
	}

	var observation Observation
	_, observation = s.Snapshot(id, true)
	conds := observation.Conditions
	assert.Equal(t, len(conds), 1)
}

func TestStore_SetConditionLocked_AppendsDistinctTypes(t *testing.T) {
	t.Parallel()
	s := New()
	id := (&manifest.Kustomization{Name: "a", Namespace: "ns"}).Named()

	s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: "ok"})
	healthy := Condition{
		Type:    ConditionHealthy,
		Status:  metav1.ConditionTrue,
		Reason:  "Healthy",
		Message: "healthy",
	}
	s.SetCondition(id, healthy)

	var before Observation
	_, before = s.Snapshot(id, true)
	conds := before.Conditions
	if got := len(conds); got != 2 {
		t.Fatalf("conditions length = %d, want 2 (Ready + Healthy)", got)
	}
	want := []Condition{
		{Type: ConditionReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded, Message: "ok"},
		healthy,
	}
	assert.Diff(t, conds, want)

	s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "boom"})
	assert.Diff(t, conds, want)
	conds[0].Message = "caller mutation"
	var after Observation
	_, after = s.Snapshot(id, true)
	conds = after.Conditions
	if got := len(conds); got != 2 {
		t.Errorf("conditions length = %d after Ready transition, want 2", got)
	}
	want[0] = Condition{Type: ConditionReady, Status: metav1.ConditionFalse, Reason: ReasonFailed, Message: "boom"}
	assert.Diff(t, conds, want)
}

func TestStore_ShardedConcurrentDifferentKinds(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			s := New()
			arrived, release := make(chan struct{}, workers), make(chan struct{})
			var events [numEventKinds + 1]sync.Map
			for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
				s.AddListener(kind, func(id manifest.NamedResource, _ any) {
					count, _ := events[kind].LoadOrStore(id, new(atomic.Int64))
					count.(*atomic.Int64).Add(1)
					if kind == EventObjectAdded && strings.HasSuffix(id.Name, "-0") && !strings.HasPrefix(id.Name, "parent-") && id.Group != "example.test" {
						arrived <- struct{}{}
						<-release
					}
				}, false)
			}
			var wg sync.WaitGroup
			for w := range workers {
				wg.Go(func() {
					for i := range 20 {
						name := fmt.Sprintf("%d-%d", w, i)
						var typed manifest.BaseManifest = &manifest.Kustomization{Name: name, Namespace: "ns"}
						if w%2 != 0 {
							typed = &manifest.HelmRelease{Name: name, Namespace: "ns"}
						}
						foreign := &manifest.RawObject{APIVersion: "example.test/v1", Kind: typed.Named().Kind, Namespace: "ns", Name: name}
						objects := [2]manifest.BaseManifest{typed, foreign}
						if i%2 != 0 {
							objects[0], objects[1] = objects[1], objects[0]
						}
						for _, obj := range objects {
							id := obj.Named()
							parent := id
							parent.Name = "parent-" + id.Name
							s.AddObject(&manifest.RawObject{APIVersion: parent.Group + "/v1", Kind: parent.Kind, Name: parent.Name, Namespace: parent.Namespace})
							s.RecordEmitted(parent, &Emission{Children: []Child{{Obj: obj}}, Complete: true})
							s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: id.Group})
							s.SetArtifact(id, &SourceArtifact{URL: id.Group})
						}
						for _, obj := range objects {
							id := obj.Named()
							assert.Equal(t, s.GetObject(id), obj)
							status, ok := s.GetStatus(id)
							assert.Equal(t, ok, true)
							assert.Equal(t, status.Status, StatusReady)
							assert.Equal(t, status.Message, id.Group)
							assert.Diff(t, s.GetArtifact(id), Artifact(&SourceArtifact{URL: id.Group}))
							owners := testutil.Owners(s.Productions(id, nil))
							assert.Equal(t, len(owners), 1)
							assert.Equal(t, owners[0].Group, id.Group)
							for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
								count, ok := events[kind].Load(id)
								if !ok || count.(*atomic.Int64).Load() != 1 {
									t.Errorf("event %v for %v = %v", kind, id, count)
								}
							}
						}
						if i%4 >= 2 {
							objects[0], objects[1] = objects[1], objects[0]
						}
						removed := objects[0].Named()
						parent := removed
						parent.Name = "parent-" + removed.Name
						s.RecordEmitted(parent, &Emission{Complete: true})
						s.DeleteObject(parent)
						assert.Equal(t, s.GetObject(removed), nil)
						var removedObservation Observation
						_, removedObservation = s.Snapshot(removed, true)
						conds := removedObservation.Conditions
						assert.Equal(t, len(conds), 0)
						assert.Equal(t, s.GetArtifact(removed), nil)
						id := objects[1].Named()
						assert.Equal(t, s.GetObject(id), objects[1])
						status, ok := s.GetStatus(id)
						assert.Equal(t, ok, true)
						assert.Equal(t, status.Message, id.Group)
						assert.Diff(t, s.GetArtifact(id), Artifact(&SourceArtifact{URL: id.Group}))
						var observation Observation
						_, observation = s.Snapshot(id, true)
						conds = observation.Conditions
						assert.Diff(t, conds, []Condition{readyCondition(StatusReady, "", id.Group)})
						parent = id
						parent.Name = "parent-" + id.Name
						s.RecordEmitted(parent, &Emission{Complete: true})
						s.DeleteObject(parent)
					}
				})
			}
			for range workers {
				<-arrived
			}
			close(release)
			wg.Wait()
			assert.Equal(t, len(s.ListObjects("", "")), 0)
		})
	}
}

// TestStore_CrossKindOperationDoesntDeadlock pounds the cross-shard
// paths (ListObjects("", ""), FailedResources, AddListener(replay=true))
// from many goroutines while per-shard writers churn through different
// Kinds. The canonical-order lockAll/rLockAll rule prevents two
// cross-shard operations from deadlocking by taking shards in opposing
// orders. Any regression that locks shards out-of-order — e.g. a
// future lockAll variant that iterates from N-1 down to 0 — produces
// a hang within the 30s test timeout.
func TestStore_CrossKindOperationDoesntDeadlock(t *testing.T) {
	s := New()
	const goroutines = 32
	const ops = 30

	// Seed with a mix of kinds across multiple shards.
	for i := range 10 {
		s.AddObject(&manifest.Kustomization{Name: fmt.Sprintf("ks-%d", i), Namespace: "ns"})
		s.AddObject(&manifest.HelmRelease{Name: fmt.Sprintf("hr-%d", i), Namespace: "ns"})
		s.AddObject(newCM(fmt.Sprintf("cm-%d", i), "ns"))
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := range goroutines {
		wg.Go(func() {
			for i := range ops {
				switch (w + i) % 5 {
				case 0:
					_ = s.ListObjects("", "")
				case 1:
					_ = s.FailedResources()
				case 2:
					unsub := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) {}, true)
					unsub()
				case 3:
					ks := &manifest.Kustomization{Name: fmt.Sprintf("ks-w%d-i%d", w, i), Namespace: "ns"}
					s.AddObject(ks)
					s.UpdateStatus(ks.Named(), StatusInfo{Status: StatusFailed, Reason: "", Message: "boom"})
				case 4:
					hr := &manifest.HelmRelease{Name: fmt.Sprintf("hr-w%d-i%d", w, i), Namespace: "ns"}
					s.AddObject(hr)
					s.SetArtifact(hr.Named(), &Emission{Manifests: nil, Fingerprint: "x"})
				}
			}
		})
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		// Success — no deadlock.
	case <-time.After(30 * time.Second):
		t.Fatal("cross-shard ops deadlocked (lockAll canonical-order regression?)")
	}
}
