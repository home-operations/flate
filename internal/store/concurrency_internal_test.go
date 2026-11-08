package store

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_Concurrency(t *testing.T) {
	t.Parallel()
	s := New()
	const operations = 200

	var wg sync.WaitGroup
	wg.Go(func() {
		for range operations {
			s.AddObject(newCM("a", "ns"))
		}
	})
	wg.Go(func() {
		for range operations {
			_ = s.ListObjects("", "ConfigMap")
		}
	})
	wg.Wait()
}

func TestStore_ShardedConcurrentDifferentKinds(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			testStoreShardedConcurrentDifferentKindsCase(
				t,
				&workers,
			)
		},
		)
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
	t.Parallel()

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
					s.RecordEmitted(hr.Named(), &Emission{Manifests: nil, Fingerprint: "x"}, nil)
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

func testStoreShardedConcurrentDifferentKindsCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	s := New()
	arrived, release := make(chan struct{}, (*testCaseInput)), make(chan struct{})
	var events [numEventKinds + 1]sync.Map
	for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
		s.AddListener(kind, func(id manifest.NamedResource, _ any) {
			count, _ := events[kind].LoadOrStore(id, new(atomic.Int64))
			count.(*atomic.Int64).Add(1)
			if kind == EventObjectAdded && strings.HasSuffix(id.Name, "-0") &&
				!strings.HasPrefix(id.Name, "parent-") &&
				id.Group != "example.test" {
				arrived <- struct{}{}
				<-release
			}
		}, false)
	}
	testStoreShardedConcurrentDifferentKindsCaseCompletion(
		t,
		&testStoreShardedConcurrentDifferentKindsCaseCompletionState{
			testCaseInput: testCaseInput,
			s:             s,
			arrived:       &arrived,
			release:       &release,
			events:        &events,
		},
	)
}

type testStoreShardedConcurrentDifferentKindsCaseCompletionState struct {
	testCaseInput *int
	s             *Store
	arrived       *chan struct{}
	release       *chan struct{}
	events        *[4]sync.Map
}

func testStoreShardedConcurrentDifferentKindsCaseCompletion(
	t *testing.T,
	scope *testStoreShardedConcurrentDifferentKindsCaseCompletionState,
) {
	t.Helper()
	var wg sync.WaitGroup
	for w := range *scope.testCaseInput {
		wg.Go(func() {
			testStoreShardedConcurrentWorker(t, scope, &w)
		})
	}
	for range *scope.testCaseInput {
		<-(*scope.arrived)
	}
	close((*scope.release))
	wg.Wait()
	assert.Equal(t, len(scope.s.ListObjects("", "")), 0)
}

func testStoreShardedConcurrentWorker(
	t *testing.T,
	scope *testStoreShardedConcurrentDifferentKindsCaseCompletionState,
	worker *int,
) {
	t.Helper()
	for i := range 20 {
		name := fmt.Sprintf("%d-%d", (*worker), i)
		var typed manifest.BaseManifest = &manifest.Kustomization{Name: name, Namespace: "ns"}
		if (*worker)%2 != 0 {
			typed = &manifest.HelmRelease{Name: name, Namespace: "ns"}
		}
		foreign := &manifest.RawObject{
			APIVersion: "example.test/v1",
			Kind:       typed.Named().Kind,
			Namespace:  "ns",
			Name:       name,
		}
		objects := [2]manifest.BaseManifest{typed, foreign}
		if i%2 != 0 {
			objects[0], objects[1] = objects[1], objects[0]
		}
		for _, obj := range objects {
			id := obj.Named()
			parent := id
			parent.Name = "parent-" + id.Name
			scope.s.AddObject(
				&manifest.RawObject{
					APIVersion: parent.Group + "/v1",
					Kind:       parent.Kind,
					Name:       parent.Name,
					Namespace:  parent.Namespace,
				},
			)
			scope.s.RecordEmitted(parent, &Emission{Children: []Child{{Obj: obj}}, Complete: true}, nil)
			scope.s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: id.Group})
			scope.s.SetArtifact(id, &SourceArtifact{URL: id.Group})
		}
		testStoreShardedConcurrentObjectChecks(t, scope, &objects)
		if i%4 >= 2 {
			objects[0], objects[1] = objects[1], objects[0]
		}
		removed := objects[0].Named()
		parent := removed
		parent.Name = "parent-" + removed.Name
		scope.s.RecordEmitted(parent, &Emission{Complete: true}, nil)
		scope.s.DeleteObject(parent)
		assert.Equal(t, scope.s.GetObject(removed), nil)
		var removedObservation Observation
		_, removedObservation = scope.s.Snapshot(removed, true)
		conds := removedObservation.Conditions
		assert.Equal(t, len(conds), 0)
		assert.Equal(t, scope.s.GetArtifact(removed, ArtifactContent), nil)
		id := objects[1].Named()
		assert.Equal(t, scope.s.GetObject(id), objects[1])
		status, ok := scope.s.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Message, id.Group)
		{
			diff := cmp.Diff(
				Artifact(&SourceArtifact{URL: id.Group}),
				scope.s.GetArtifact(id, ArtifactContent),
				cmpopts.IgnoreFields(SourceArtifact{}, "material"),
			)
			assert.ErrorIf(t, diff != "", "artifact mismatch (-want +got):\n%s", diff)
		}
		var observation Observation
		_, observation = scope.s.Snapshot(id, true)
		conds = observation.Conditions
		assert.Diff(t, conds, []Condition{readyCondition(StatusReady, "", id.Group)})
		parent = id
		parent.Name = "parent-" + id.Name
		scope.s.RecordEmitted(parent, &Emission{Complete: true}, nil)
		scope.s.DeleteObject(parent)
	}
}

func testStoreShardedConcurrentObjectChecks(
	t *testing.T,
	scope *testStoreShardedConcurrentDifferentKindsCaseCompletionState,
	objects *[2]manifest.BaseManifest,
) {
	t.Helper()
	for _, obj := range *objects {
		id := obj.Named()
		assert.Equal(t, scope.s.GetObject(id), obj)
		status, ok := scope.s.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Status, StatusReady)
		assert.Equal(t, status.Message, id.Group)
		{
			diff := cmp.Diff(
				Artifact(&SourceArtifact{URL: id.Group}),
				scope.s.GetArtifact(id, ArtifactContent),
				cmpopts.IgnoreFields(SourceArtifact{}, "material"),
			)
			assert.ErrorIf(t, diff != "", "artifact mismatch (-want +got):\n%s", diff)
		}
		owners := testutil.Owners(scope.s.Productions(id, nil))
		assert.Equal(t, len(owners), 1)
		assert.Equal(t, owners[0].Group, id.Group)
		for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
			count, ok := scope.events[kind].Load(id)
			assert.ErrorIf(
				t,
				!ok || count.(*atomic.Int64).Load() != 1,
				"event %v for %v = %v",
				kind,
				id,
				count,
			)
		}
	}
}
