package store

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testStoreSnapshotCase(t, &tc) },
		)
	}
}

func TestStore_AddObject_ContentDedup(t *testing.T) {
	t.Parallel()
	t.Run("document metadata", func(t *testing.T) { t.Parallel(); testStoreAddObjectContentDedupDocumentMetadata(t) })
	for _, insert := range []struct {
		name string
		add  func(*Store, manifest.BaseManifest)
	}{
		{"single", func(s *Store, obj manifest.BaseManifest) { s.AddObject(obj) }},
		{"batch", func(s *Store, obj manifest.BaseManifest) { s.AddObjects([]manifest.BaseManifest{obj}) }},
	} {
		t.Run(insert.name, func(t *testing.T) {
			t.Parallel()
			testStoreAddObjectContentDedupCase(t, &insert)
		},
		)
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
		{
			"equal",
			map[string]any{
				"items": []any{map[string]any{"value": "same"}, true, int64(1), 1.5},
				"extra": []string{"a"},
			},
			true,
		},
		{
			"nested map changed",
			map[string]any{
				"items": []any{map[string]any{"value": "changed"}, true, int64(1), 1.5},
				"extra": []string{"a"},
			},
			false,
		},
		{
			"slice length changed",
			map[string]any{"items": []any{map[string]any{"value": "same"}}, "extra": []string{"a"}},
			false,
		},
		{
			"scalar type changed",
			map[string]any{"items": []any{map[string]any{"value": "same"}, true, 1, 1.5}, "extra": []string{"a"}},
			false,
		},
		{
			"fallback changed",
			map[string]any{
				"items": []any{map[string]any{"value": "same"}, true, int64(1), 1.5},
				"extra": []string{"b"},
			},
			false,
		},
		{
			"missing key",
			map[string]any{
				"items": []any{map[string]any{"value": "same"}, true, int64(1), 1.5},
				"other": []string{"a"},
			},
			false,
		},
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
		parsed, err := manifest.ParseDoc(map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1",
			"kind":       manifest.KindKustomization,
			"metadata":   map[string]any{"name": "app", "namespace": "ns"},
			"spec": map[string]any{
				"path":      "./app",
				"sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"},
			},
		}, manifest.ParseDocOptions{})
		obj := parsed.Object
		assert.NoError(t, err)
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
		{
			_, err := first.RenderDocument()
			assert.ErrorIf(t, err != nil, "%v", err)
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
		parsed2, err := manifest.ParseDoc(map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
			"metadata": map[string]any{"name": "app", "namespace": "ns"},
			"spec": map[string]any{
				"path": "./app", "sourceRef": map[string]any{"kind": "GitRepository", "name": "repo"},
				"postBuild": map[string]any{"substitute": map[string]any{"KEY": "same"}},
			},
		}, manifest.ParseDocOptions{})
		obj := parsed2.Object
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
			assert.Equal(t, manifest.Equal(first, second), tc.equal)
			assert.Equal(t, manifest.Equal(second, first), tc.equal)
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

//nolint:paralleltest,tparallel // Subtests finish clone checks before the parent reuses the shared original.
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
	assert.Equal(t, manifest.Equal(base, base), true)
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
			candidate := base.Clone()
			assert.Equal(t, candidate == base, false)
			if tc.change != nil {
				tc.change(candidate)
			}
			assert.Equal(t, manifest.Equal(base, candidate), tc.equal)
			assert.Equal(t, manifest.Equal(candidate, base), tc.equal)
		})
	}
	assert.Diff(t, base.Spec, map[string]any{"items": []any{map[string]any{"value": "same"}}})
	assert.Equal(t, manifest.Equal(&manifest.RawObject{Spec: map[string]any{}}, &manifest.RawObject{}), false)
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
	{
		got := s.GetObject(id)
		assert.ErrorIf(t, got == nil, "GetObject after AddObject returned nil")
	}
}

// TestStore_AddObject_CloneTriggersUpdate guards the immutability
// contract: callers that need to "mutate" a stored object must clone
// it, mutate the clone, and re-AddObject. The store's [reflect.DeepEqual]
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
	c := &manifest.RawObject{
		Kind:       "Deployment",
		APIVersion: "apps/v1",
		Name:       "b",
		Namespace:  "ns",
		Spec:       map[string]any{"replicas": 1},
	}
	idA, idB, idC := a.Named(), b.Named(), c.Named()
	seen := map[manifest.NamedResource]int{}
	s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		seen[id]++
		assert.Equal(t, payload.(manifest.BaseManifest).Named(), id)
		assert.Equal(t, s.GetObject(idA), manifest.BaseManifest(a))
		assert.Equal(t, s.GetObject(idC), manifest.BaseManifest(c))
		{
			got := s.GetObject(idB)
			assert.ErrorIf(t, got == nil || got.Named() != idB, "batch listener cannot see foreign sibling: %v", got)
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

func TestStore_AdmissionResetsUnchangedReadiness(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	id := obj.Named()
	s.AddObject(obj)
	s.SetIncluded(id, false)
	s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: ReasonUnchanged, Message: "unchanged"})
	var reads ReadSet
	reads.Init(s, id, nil)
	defer reads.Retire()
	_, previous := reads.Snapshot(id, true)
	assert.FatalIf(t, reads.Admitted(), "%s", "excluded execution admitted")
	arrivals := 0
	s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
	published, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var statuses atomic.Int32
	s.AddListener(EventStatusUpdated, func(got manifest.NamedResource, payload any) {
		if got != id || statuses.Add(1) != 1 {
			t.Error("unexpected status publication")
			return
		}
		info, _ := s.GetStatus(id)
		assert.Equal(t, payload.(StatusInfo), info)
		assert.Equal(t, info.Outcome(), OutcomePending)
		close(published)
		<-resume
	}, false)
	go func() {
		defer close(done)
		s.SetIncluded(id, true)
	}()
	<-published
	var current ReadSet
	current.Init(s, id, nil)
	defer current.Retire()
	_, observed := current.Snapshot(id, true)
	assert.Equal(t, current.Admitted(), true)
	assert.Equal(t, observed.Outcome(), OutcomePending)
	assert.Equal(t, observed.Conditions[0].Status, metav1.ConditionUnknown)
	s.SetIncluded(id, true)
	assert.Equal(t, current.Valid(), true)
	release()
	<-done
	if reads.Valid() || arrivals != 0 || statuses.Load() != 1 {
		t.Fatalf("valid=%t arrivals=%d statuses=%d", reads.Valid(), arrivals, statuses.Load())
	}
	assert.Equal(t, previous.Outcome(), OutcomeUnchanged)
	assert.Equal(t, previous.Conditions[0].Status, metav1.ConditionTrue)
}

//nolint:paralleltest,tparallel // Subtests apply successive transitions to one shared status history.
func TestStore_UpdateStatus_Idempotent(
	t *testing.T,
) {
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
			assert.FatalIf(
				t,
				!ok || info.Status != tc.status || info.Reason != tc.want || len(events) != tc.count,
				"status=%+v present=%v events=%d, want reason=%s events=%d",
				info,
				ok,
				len(events),
				tc.want,
				tc.count,
			)
			assert.FatalIf(t, events[len(events)-1] != info, "%s", "event lost status projection")
			var replay StatusInfo
			u := s.AddListener(
				EventStatusUpdated,
				func(_ manifest.NamedResource, payload any) { replay = payload.(StatusInfo) },
				true,
			)
			u()
			assert.FatalIf(t, replay != info, "replay=%+v, want %+v", replay, info)
		})
	}
	assert.Equal(t, events[1].Reason, ReasonSkipped)
}

func TestStatusInfo_Outcome(t *testing.T) {
	t.Parallel()
	failure := newDependencyFailure(
		&manifest.DependencyFailedError{Failed: []manifest.NamedResource{testutil.HelmReleaseID("", "dep")}},
	)
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
	{
		got := len(conds)
		assert.FatalIf(t, got != 2, "expected 2 conditions, got %d", got)
	}
}

func testStoreSnapshotCase(t *testing.T, testCaseInput *struct {
	name   string
	obj    manifest.BaseManifest
	id     manifest.NamedResource
	status Status
},
) {
	t.Helper()

	st := New()
	if testCaseInput.obj != nil {
		st.AddObject(testCaseInput.obj)
	}
	if testCaseInput.status != "" {
		st.UpdateStatus(
			testCaseInput.id,
			StatusInfo{Status: testCaseInput.status, Reason: "", Message: "observed"},
		)
	}
	statusful := testCaseInput.obj != nil && manifest.KindOf(testCaseInput.obj).Status ||
		testCaseInput.obj == nil && testCaseInput.id.KindInfo().Status
	for _, requested := range []bool{false, true} {
		t.Run(strconv.FormatBool(requested), func(t *testing.T) {
			t.Parallel()
			var observation Observation
			obj, observation := st.Snapshot(testCaseInput.id, requested)
			assert.Equal(t, obj, testCaseInput.obj)
			snapshot61, _ := st.Snapshot(testCaseInput.id, requested)
			assert.Equal(t, snapshot61, testCaseInput.obj)
			assert.Equal(t, observation.Statusful, statusful)
			want, _ := st.GetStatus(testCaseInput.id)
			if testCaseInput.obj != nil && !statusful && !requested {
				want = StatusInfo{}
			}
			assert.Equal(t, observation.StatusInfo, want)
			var conditions []Condition
			if requested && testCaseInput.status != "" {
				conditions = []Condition{readyCondition(testCaseInput.status, "", "observed")}
			}
			assert.Diff(t, observation.Conditions, conditions)
			if len(observation.Conditions) != 0 {
				observation.Conditions[0].Message = "mutated"
				var fresh Observation
				_, fresh = st.Snapshot(testCaseInput.id, true)
				assert.Diff(t, fresh.Conditions, conditions)
				assert.Equal(t, fresh.StatusInfo, want)
			}
		})
	}
}

func testStoreAddObjectContentDedupDocumentMetadata(t *testing.T) {
	t.Helper()

	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	arrivals := 0
	s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
	s.AddObject(obj)
	doc := map[string]any{"metadata": map[string]any{"name": obj.Name}}
	s.AddObject(obj, doc)
	next := manifest.DeepCopyMap(doc)
	next["metadata"].(map[string]any)["namespace"] = obj.Namespace
	s.AddObject(obj, next)
	assert.FatalIf(t, arrivals != 1, "unchanged source metadata caused %d arrivals, want 1", arrivals)
	_, retained, attempt := s.GetEmission(obj.Named())
	defer attempt.Finish()
	assert.FatalIf(
		t,
		!manifest.ValueEqual(retained, next),
		"%s",
		"suppressed arrival lost the authoritative document",
	)
}

func testStoreAddObjectContentDedupCase(t *testing.T, testCaseInput *struct {
	name string
	add  func(*Store, manifest.BaseManifest)
},
) {
	t.Helper()

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
					assert.ErrorIf(t, s.GetObject(id) == nil, "%s", "listener cannot see rendered object")
				}, false)
				testCaseInput.add(s, obj)
				equal := obj
				if distinct {
					equal = tc.newObject("1")
				}
				testCaseInput.add(s, equal)
				assert.Equal(t, seen, 1)
				changed := tc.newObject("2")
				testCaseInput.add(s, changed)
				assert.Equal(t, seen, 2)
				assert.Equal(t, s.GetObject(obj.Named()), changed)
			})
		}
	}
}
