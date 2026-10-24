package store

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/task"
)

func emissionChild(name, value string) Child {
	doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name, "namespace": "apps"}, "data": map[string]any{"value": value}}
	obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		panic(err)
	}
	return Child{Obj: obj, Doc: authoritative}
}
func accept(t *testing.T, s *Store, owner manifest.NamedResource, next *Emission) {
	t.Helper()
	s.RecordEmitted(owner, next, nil)
}
func ownerID(name string) manifest.NamedResource {
	return testutil.KustomizationID("apps", name)
}

func checkEffectiveObject(t *testing.T, s *Store, id manifest.NamedResource, want Child) {
	t.Helper()
	assert.Equal(t, s.GetObject(id), want.Obj)
	obj, _ := s.Snapshot(id, false)
	assert.Equal(t, obj, want.Obj)
	obj, _, attempt := s.GetEmission(id)
	defer attempt.Finish()
	assert.Equal(t, obj, want.Obj)
	if s.EmissionOwner(id) != (manifest.NamedResource{}) {
		assert.Diff(t, attempt.inputDoc, want.Doc)
	}
	for _, objects := range [][]manifest.BaseManifest{
		s.ListObjects(id.Group, id.Kind), s.ListObjects(id.Group, id.Kind),
		s.ListObjects("", ""), s.ListObjects("", ""),
	} {
		var found manifest.BaseManifest
		for i, object := range objects {
			if object.Named() == id {
				found = object
			}
			if i > 0 && objects[i-1].Named().Compare(object.Named()) >= 0 {
				t.Fatal("effective inventory is not sorted and unique")
			}
		}
		assert.Equal(t, found, want.Obj)
	}
	sh := s.shardFor(id)
	sh.mu.RLock()
	record, present := sh.conditions[id]
	record.conditions = slices.Clone(record.conditions)
	sh.mu.RUnlock()
	defer func() {
		sh.mu.Lock()
		if present {
			sh.conditions[id] = record
		} else {
			delete(sh.conditions, id)
		}
		sh.mu.Unlock()
	}()
	s.UpdateFailure(id, fmt.Errorf("%w: projection", manifest.ErrInput))
	_, failed := s.FailedResources()[id]
	assert.Equal(t, failed, want.Obj != nil)
	var replayed manifest.BaseManifest
	s.AddListener(EventObjectAdded, func(got manifest.NamedResource, data any) {
		if got == id {
			replayed = data.(manifest.BaseManifest)
		}
	}, true)()
	assert.Equal(t, replayed, want.Obj)
}

func TestRecordEmitted_Replacement(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
	a, b := emissionChild("a", "old"), emissionChild("b", "new")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	accept(t, s, p, &Emission{Children: []Child{a}, Manifests: []map[string]any{a.Doc}, Complete: true})
	accept(t, s, q, &Emission{Children: []Child{a}, Manifests: []map[string]any{a.Doc}, Complete: true})
	if got := s.Productions(a.Obj.Named(), nil); len(got) != 2 {
		t.Fatalf("independent supports: %v", got)
	}
	bad := emissionChild("a", "unequal")
	s.RecordEmitted(q, &Emission{Children: []Child{b, bad}, Complete: true}, nil)
	if s.GetObject(b.Obj.Named()) != b.Obj || s.GetObject(bad.Obj.Named()) != bad.Obj {
		t.Fatal("unequal contribution lost accepted content")
	}
	accept(t, s, p, &Emission{Complete: true})
	if s.GetObject(a.Obj.Named()) == nil {
		t.Fatal("withdrawal lost surviving supporter")
	}
	accept(t, s, q, &Emission{Complete: true})
	if s.GetObject(a.Obj.Named()) != nil {
		t.Fatal("complete empty retained unsupported output")
	}
	accept(t, s, p, &Emission{Children: []Child{a}, Manifests: []map[string]any{a.Doc}, Complete: true})
	s.UpdateFailure(p, fmt.Errorf("%w: failed successor", manifest.ErrInput))
	_, _, attempt := s.GetEmission(p)
	if len(attempt.Manifests) != 1 || attempt.Complete {
		t.Fatalf("failure lost accepted output: %+v", attempt)
	}
	attempt.Children = []Child{b}
	attempt.Manifests = []map[string]any{b.Doc}
	accept(t, s, p, &attempt)
	if s.GetObject(a.Obj.Named()) == nil || s.GetObject(b.Obj.Named()) == nil {
		t.Fatal("partial update lost predecessor")
	}
	attempt.Finish()
	_, _, progress := s.GetEmission(p)
	progress.Children = nil
	accept(t, s, p, &progress)
	_, _, retained := s.GetEmission(p)
	if len(retained.Manifests) != 2 {
		t.Fatalf("inherited partial documents duplicated: %d", len(retained.Manifests))
	}
	progress.Finish()
	for _, tc := range []struct {
		name string
		edit func(*Emission)
	}{
		{"unreadable", func(e *Emission) { e.Unreadable[0] = b.Obj.Named() }},
		{"child", func(e *Emission) { e.Children[0] = b }},
		{"documents", func(e *Emission) { e.Manifests[0] = b.Doc }},
		{"complete", func(e *Emission) { e.Complete = true }},
		{"binding", func(e *Emission) { e.binding.key = "new" }},
		{"outer_slices", func(e *Emission) {
			e.Children[0], e.Manifests[0], e.Unreadable[0] = b, b.Doc, b.Obj.Named()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := New()
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			_, _, initial := s.GetEmission(p)
			initial.Children, initial.Manifests = []Child{a}, []map[string]any{a.Doc}
			initial.Unreadable, initial.Fingerprint = []manifest.NamedResource{a.Obj.Named()}, "same"
			initial.Complete = tc.name != "complete"
			s.RecordEmitted(p, &initial, nil)
			predecessor := s.GetArtifact(p).(*Emission)
			_, _, attempt := s.GetEmission(p)
			defer attempt.Finish()
			attempt.Complete, attempt.Fingerprint = initial.Complete, "same"
			attempt.Children, attempt.Manifests, attempt.Unreadable = slices.Clone(attempt.Children), slices.Clone(attempt.Manifests), slices.Clone(attempt.Unreadable)
			tc.edit(&attempt)
			checkPredecessor := func() {
				t.Helper()
				if predecessor.Children[0].Obj != a.Obj || !specValueEqual(predecessor.Manifests[0], a.Doc) || predecessor.Unreadable[0] != a.Obj.Named() {
					t.Fatal("captured outer slice changed accepted predecessor")
				}
			}
			checkPredecessor()
			var events int
			defer s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
				if id == p {
					events++
				}
			}, false)()
			s.RecordEmitted(p, &attempt, nil)
			accepted := s.GetArtifact(p).(*Emission)
			if events != 1 || accepted == predecessor || accepted.Fingerprint != "same" {
				t.Fatal("equal fingerprint discarded changed emission")
			}
			if !slices.Equal(accepted.Unreadable, attempt.Unreadable) || accepted.Complete != attempt.Complete || accepted.binding != attempt.binding {
				t.Fatal("changed emission lost output or completion context")
			}
			if !slices.EqualFunc(accepted.Children, attempt.Children, func(a, b Child) bool { return a.Obj == b.Obj }) || !slices.EqualFunc(accepted.Manifests, attempt.Manifests, func(a, b map[string]any) bool { return specValueEqual(a, b) }) {
				t.Fatal("changed emission lost captured children or documents")
			}
			checkPredecessor()
			_, _, replay := s.GetEmission(p)
			defer replay.Finish()
			replay.binding = accepted.binding
			replay.Complete = accepted.Complete
			s.RecordEmitted(p, &replay, nil)
			if events != 1 || s.GetArtifact(p) != accepted {
				t.Fatal("equal replay republished accepted output")
			}
		})
	}
}

func TestRecordEmitted_ConcurrentSupport(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			t.Parallel()

			s := New()
			p := ownerID("p")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			covered := emissionChild("covered", "static")
			s.AddObject(covered.Obj)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				covered.Obj.Named(): {Productions: []manifest.Production{{Owner: emissionChild("0", "value").Obj.Named()}}},
			}})
			start := make(chan struct{})
			prepared := make(chan struct{}, workers)
			var wg sync.WaitGroup
			for i := range workers {
				wg.Go(func() {
					_, _, next := s.GetEmission(p)
					defer next.Finish()
					c := emissionChild(fmt.Sprint(i), "value")
					next.Children, next.Manifests = []Child{c}, []map[string]any{c.Doc}
					prepared <- struct{}{}
					<-start
					accept(t, s, p, &next)
				})
			}
			for range workers {
				<-prepared
			}
			close(start)
			wg.Wait()
			_, _, attempt := s.GetEmission(p)
			sh := s.shardFor(p)
			sh.mu.RLock()
			accepted := sh.slots[p].current.Load()
			sh.mu.RUnlock()
			if len(accepted.Children) != workers || len(accepted.Manifests) != workers {
				t.Fatalf("lost concurrent partial entries: children=%d documents=%d", len(accepted.Children), len(accepted.Manifests))
			}
			attempt.Finish()
			q := ownerID("q")
			s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
			accept(t, s, q, &Emission{Children: accepted.Children, Manifests: accepted.Manifests, Complete: true})
			start = make(chan struct{})
			for i := range workers {
				wg.Go(func() {
					<-start
					next := &Emission{Children: slices.Clone(accepted.Children), Manifests: slices.Clone(accepted.Manifests), Complete: true}
					for j, child := range next.Children {
						doc := manifest.DeepCopyMap(child.Doc)
						doc["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": fmt.Sprint(i)}
						next.Children[j].Doc = doc
						next.Manifests[j] = doc
					}
					if i%2 == 0 {
						slices.Reverse(next.Children)
						slices.Reverse(next.Manifests)
					}
					accept(t, s, p, next)
				})
			}
			capturedID := accepted.Children[0].Obj.Named()
			captured := s.Productions(capturedID, nil)
			marker := emissionChild("reader-anchor", "value")
			wg.Go(func() {
				<-start
				var observed []manifest.Production
				for _, proof := range captured {
					observed = append(observed, proof)
					s.AddObject(marker.Obj)
				}
				if !slices.Equal(observed, []manifest.Production{{Owner: p}, {Owner: q}}) {
					t.Errorf("concurrent captured productions=%v", observed)
				}
				for _, child := range accepted.Children {
					id := child.Obj.Named()
					parent, ok := s.RenderedParent(id, nil)
					if !ok || parent != p {
						t.Errorf("concurrent parent=%v/%t, want %v/true", parent, ok, p)
					}
					if got := s.Productions(id, nil); !slices.Equal(got, []manifest.Production{{Owner: p}, {Owner: q}}) {
						t.Errorf("concurrent productions=%v", got)
					}
					if obj := s.GetObject(id); obj == nil || obj.Named() != id {
						t.Error("concurrent object lost identity")
					}
					obj, _, next := s.GetEmission(id)
					next.Finish()
					if obj == nil || obj.Named() != id || s.EmissionOwner(id) != q {
						t.Error("concurrent rich read lost representative")
					}
					if !slices.ContainsFunc(s.ListObjects(id.Group, id.Kind), func(obj manifest.BaseManifest) bool { return obj.Named() == id }) {
						t.Error("concurrent listing lost child")
					}
				}
			})
			close(start)
			wg.Wait()
			for _, child := range accepted.Children {
				id := child.Obj.Named()
				if got := s.GetObject(id); got == nil || got.Named() != id {
					t.Fatal("reverse position lost object")
				}
				obj, _ := s.Snapshot(id, true)
				if obj == nil || obj.Named() != id {
					t.Fatal("reverse position changed snapshot identity")
				}
				if got := s.Productions(id, nil); len(got) != 2 || got[0].Owner != p || got[1].Owner != q {
					t.Fatalf("reverse position changed production: %v", got)
				}
			}
			assert.Diff(t, captured, []manifest.Production{{Owner: p}, {Owner: q}})
			s.SetArtifact(covered.Obj.Named(), &SourceArtifact{Key: "covered"})
			s.UpdateStatus(covered.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
			accept(t, s, p, &Emission{Complete: true})
			for _, child := range accepted.Children {
				parent, ok := s.RenderedParent(child.Obj.Named(), nil)
				if !ok || parent != q {
					t.Fatalf("withdrawal parent=%v/%t, want %v/true", parent, ok, q)
				}
				if got := s.Productions(child.Obj.Named(), nil); len(got) != 1 || got[0].Owner != q {
					t.Fatalf("withdrawal changed surviving contributor: %v", got)
				}
			}
			if s.GetObject(covered.Obj.Named()) != covered.Obj || s.GetArtifact(covered.Obj.Named()) == nil {
				t.Fatal("surviving Q support lost frozen descendant")
			}
			if info, ok := s.GetStatus(covered.Obj.Named()); !ok || info.Status != StatusReady {
				t.Fatal("surviving Q support lost Ready")
			}
			accept(t, s, q, &Emission{Complete: true})
			if s.GetObject(covered.Obj.Named()) != nil || s.GetArtifact(covered.Obj.Named()) != nil {
				t.Fatal("last boundary support left frozen descendant live")
			}
			if _, ok := s.GetStatus(covered.Obj.Named()); ok {
				t.Fatal("last boundary support left frozen descendant Ready")
			}
			for _, labels := range []bool{false, true} {
				t.Run(fmt.Sprintf("stable_replay/labels_%t", labels), func(t *testing.T) {
					t.Parallel()

					s := New()
					producer := &manifest.Kustomization{Name: "stable", Namespace: "apps"}
					s.AddObject(producer)
					child := emissionChild("stable", "value")
					_, _, initial := s.GetEmission(producer.Named())
					initial.Children, initial.Manifests, initial.Complete = []Child{child}, []map[string]any{child.Doc}, true
					s.RecordEmitted(producer.Named(), &initial, nil)
					involved := []*shard{s.shardFor(producer.Named()), s.shardFor(child.Obj.Named())}
					for _, target := range manifest.ProducerTargets(child.Obj) {
						involved = append(involved, s.shardFor(target.NamedResource))
					}
					var unrelated *shard
					for _, candidate := range s.shards {
						if !slices.Contains(involved, candidate) {
							unrelated = candidate
							break
						}
					}
					if unrelated == nil || slices.Contains(involved, unrelated) {
						t.Fatal("fixture has no unrelated shard")
					}
					start, ready, done := make(chan struct{}), make(chan struct{}, workers), make(chan struct{})
					var replay sync.WaitGroup
					for i := range workers {
						replay.Go(func() {
							_, _, next := s.GetEmission(producer.Named())
							defer next.Finish()
							next.Complete = true
							if labels {
								next.Children, next.Manifests = slices.Clone(next.Children), slices.Clone(next.Manifests)
								doc := manifest.DeepCopyMap(child.Doc)
								doc["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": fmt.Sprint(i)}
								next.Children[0].Doc, next.Manifests[0] = doc, doc
							}
							ready <- struct{}{}
							<-start
							s.RecordEmitted(producer.Named(), &next, nil)
						})
					}
					for range workers {
						<-ready
					}
					unrelated.mu.Lock()
					close(start)
					go func() { replay.Wait(); close(done) }()
					deadline := time.NewTimer(5 * time.Second)
					defer deadline.Stop()
					blocked := false
					select {
					case <-done:
					case <-deadline.C:
						blocked = true
					}
					unrelated.mu.Unlock()
					<-done
					if blocked {
						t.Fatal("stable replay waited for an unrelated shard")
					}
				})
			}

		})
	}
}

func TestRecordEmitted_WithdrawalVisibility(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, lateFirst := range []bool{false, true} {
			for _, shared := range []bool{false, true} {
				t.Run(fmt.Sprintf("workers=%d/lateFirst=%t/shared=%t", workers, lateFirst, shared), func(t *testing.T) {
					t.Parallel()

					s := New()
					p, k, l, q := ownerID("p"), ownerID("k"), ownerID("l"), ownerID("a")
					s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
					child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
					cycle := Child{Obj: &manifest.Kustomization{Name: l.Name, Namespace: l.Namespace}}
					data := emissionChild("shared", "value")
					late := emissionChild("late", "value")
					payload := &SourceArtifact{Key: "late-generated", Digest: "late-digest", FS: fstest.MapFS{"file": {Data: []byte("late bytes")}}}
					accept(t, s, p, &Emission{Children: []Child{child}, Complete: true})
					accept(t, s, k, &Emission{Children: []Child{cycle, data}, Manifests: []map[string]any{data.Doc}, Complete: true})
					accept(t, s, l, &Emission{Children: []Child{child}, Complete: true})
					independent := emissionChild("shared", "independent")
					if shared {
						s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
						accept(t, s, q, &Emission{Children: []Child{independent}, Manifests: []map[string]any{independent.Doc}, Complete: true})
					}
					for _, id := range []manifest.NamedResource{k, l, data.Obj.Named()} {
						s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
					}
					s.SetArtifact(data.Obj.Named(), &SourceArtifact{Key: "live-data"})
					if s.GetArtifact(data.Obj.Named()) == nil {
						t.Fatal("fixture data artifact was not retained before withdrawal")
					}
					captured, lateRelease, withdrawRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
					lateDone, withdrawDone := make(chan struct{}), make(chan struct{})
					start := make(chan struct{})
					var wg sync.WaitGroup
					wg.Go(func() {
						<-start
						_, _, attempt := s.GetEmission(k)
						attempt.Children = []Child{cycle, data, late}
						attempt.Manifests = []map[string]any{data.Doc, late.Doc}
						attempt.Complete = true
						close(captured)
						<-lateRelease
						s.RecordEmitted(k, &attempt, map[manifest.NamedResource]*SourceArtifact{late.Obj.Named(): payload, data.Obj.Named(): payload})
						attempt.Finish()
						close(lateDone)
					})
					wg.Go(func() {
						<-start
						<-captured
						<-withdrawRelease
						accept(t, s, p, &Emission{Complete: true})
						close(withdrawDone)
					})
					for i := 2; i < workers; i++ {
						wg.Go(func() {
							<-start
							<-captured
							owner := ownerID(fmt.Sprintf("independent-%d", i))
							s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
							value := emissionChild(owner.Name, "independent")
							accept(t, s, owner, &Emission{Children: []Child{value}, Complete: true})
						})
					}
					close(start)
					<-captured
					if lateFirst {
						close(lateRelease)
						<-lateDone
						close(withdrawRelease)
					} else {
						close(withdrawRelease)
						<-withdrawDone
						close(lateRelease)
					}
					wg.Wait()
					hidden := []manifest.NamedResource{k, l, late.Obj.Named()}
					if !shared {
						hidden = append(hidden, data.Obj.Named())
					} else {
						checkEffectiveObject(t, s, data.Obj.Named(), independent)
						assert.Equal(t, s.EmissionOwner(data.Obj.Named()), q)
					}
					if shared && s.GetArtifact(data.Obj.Named()) == payload {
						t.Fatal("hidden producer supplied bytes to the independently visible declaration")
					}
					for _, id := range hidden {
						s.UpdateFailure(id, fmt.Errorf("%w: hidden successor", manifest.ErrInput))
						s.SetArtifact(id, &SourceArtifact{Key: "late"})
						assert.Equal(t, s.EmissionOwner(id), manifest.NamedResource{})
						if s.GetObject(id) != nil || s.GetArtifact(id) != nil {
							t.Fatal("hidden payload leaked")
						}
						if obj, _ := s.Get[*manifest.Kustomization](id); obj != nil {
							t.Fatal("typed getter leaked")
						}
						if _, ok := s.GetStatus(id); ok {
							t.Fatal("hidden Ready leaked")
						}
						var observation Observation
						obj, observation := s.Snapshot(id, true)
						info := observation.StatusInfo
						conditions := observation.Conditions
						if obj != nil || info != (StatusInfo{}) || len(conditions) != 0 || observation.Statusful {
							t.Fatal("hidden snapshot leaked")
						}
						if obj, _, attempt := s.GetEmission(id); obj != nil {
							attempt.Finish()
							t.Fatal("hidden attempt leaked")
						}
						if len(s.Productions(id, nil)) != 0 {
							t.Fatal("hidden productions leaked")
						}
						if _, ok := s.FailedResources()[id]; ok {
							t.Fatal("hidden failure leaked")
						}
						s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
						if _, ok := s.GetStatus(id); ok {
							t.Fatal("hidden late Ready leaked")
						}
						checkEffectiveObject(t, s, id, Child{})
					}
					if len(s.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization)) != workers-1+boolInt(shared) {
						t.Fatal("typed listing leaked hidden cycle")
					}
					for event := EventObjectAdded; event <= EventArtifactUpdated; event++ {
						s.AddListener(event, func(id manifest.NamedResource, _ any) {
							if slices.Contains(hidden, id) {
								t.Error("hidden replay leaked")
							}
						}, true)()
					}
					if len(s.Warnings()) != 0 {
						t.Fatal("hidden payload contributed a conflict warning")
					}
					s.AddObject(&manifest.Kustomization{Name: k.Name, Namespace: k.Namespace})
					if s.GetObject(k) == nil || s.GetObject(l) == nil || s.GetObject(late.Obj.Named()) == nil {
						t.Fatal("external anchor did not restore supported descendants")
					}
					if s.GetArtifact(late.Obj.Named()) != payload {
						t.Fatal("reanchoring lost privately completed generated payload")
					}
					for _, id := range hidden {
						obj := s.GetObject(id)
						if snapshot, _ := s.Snapshot(id, false); snapshot != obj {
							t.Error("reanchored snapshot differs from effective object")
						}
						rich, _, attempt := s.GetEmission(id)
						assert.Equal(t, rich, obj)
						attempt.Finish()
						if artifact := s.GetArtifact(id); artifact != nil && ((id != late.Obj.Named() && id != data.Obj.Named()) || artifact != payload) {
							t.Error("reanchoring restored retained artifact")
						}
						if _, ok := s.GetStatus(id); ok {
							t.Error("reanchoring restored retained Ready")
						}
					}
					if !s.DeleteObject(k) {
						t.Fatal("independent cycle anchor was not removed")
					}
					for _, id := range hidden {
						if s.GetObject(id) != nil || s.GetArtifact(id) != nil {
							t.Error("removed anchor retained a detached cycle")
						}
					}
					if shared && s.GetObject(data.Obj.Named()) == nil {
						t.Fatal("removed cycle anchor lost surviving Q support")
					}
				})
			}
		}
	}
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestRecordEmitted_ContributionContext(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	for _, id := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	childWithLabels := func(owner string) Child {
		t.Helper()
		labels := map[string]any{}
		for _, key := range []string{"kustomize.toolkit.fluxcd.io/name", "kustomize.toolkit.fluxcd.io/namespace", "helm.toolkit.fluxcd.io/name", "helm.toolkit.fluxcd.io/namespace"} {
			labels[key] = owner
		}
		doc := map[string]any{"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization, "metadata": map[string]any{"name": "shared", "namespace": "apps", "labels": labels}, "spec": map[string]any{"path": "app"}}
		obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return Child{Obj: obj, Doc: authoritative}
	}
	a, b := childWithLabels("p"), childWithLabels("q")
	first := sourceBinding{source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "first", Namespace: "apps"}}
	second := first
	second.source.Name = "second"
	accept(t, s, p, &Emission{Children: []Child{a}, Manifests: []map[string]any{a.Doc}, Complete: true, binding: first})
	accept(t, s, q, &Emission{Children: []Child{b}, Manifests: []map[string]any{b.Doc}, Complete: true, binding: second})
	for _, contribution := range []struct {
		owner manifest.NamedResource
		child Child
	}{{p, a}, {q, b}} {
		_, _, inventory := s.GetEmission(contribution.owner)
		if diff := cmp.Diff(inventory.Children, []Child{contribution.child}, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreUnexported(Child{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
			t.Fatalf("emission children changed (-want +got): %s", diff)
		}
		inventory.Finish()
	}
	changed := childWithLabels("q")
	changed.Doc["metadata"].(map[string]any)["labels"].(map[string]any)["semantic"] = "different"
	s.RecordEmitted(q, &Emission{Children: []Child{changed}, Complete: true}, nil)
	if warnings := s.Warnings(); len(warnings) != 1 || warnings[0].Category != manifest.WarnConflictingProducers {
		t.Fatalf("semantic metadata conflict warning: %+v", warnings)
	}
	fresh := emissionChild("fresh", "value")
	accept(t, s, p, &Emission{Children: []Child{fresh}, Manifests: []map[string]any{fresh.Doc}, binding: second})
	oldProductions := s.Productions(a.Obj.Named(), nil)
	if !slices.ContainsFunc(oldProductions, func(p manifest.Production) bool { return p.Owner == ownerID("p") && p.Source == first.source }) {
		t.Fatal("partial update rebound predecessor context")
	}
	if got := s.Productions(fresh.Obj.Named(), nil); len(got) != 1 || got[0].Source != second.source {
		t.Fatal("partial update lost new source context")
	}
	_, _, attempt := s.GetEmission(p)
	attempt.Unreadable = []manifest.NamedResource{fresh.Obj.Named()}
	var notifications int
	unsub := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == p {
			notifications++
		}
	}, false)
	accept(t, s, p, &attempt)
	unsub()
	_, _, accepted := s.GetEmission(p)
	if notifications != 1 || !slices.Equal(accepted.Unreadable, attempt.Unreadable) {
		t.Fatal("context-only refresh was discarded")
	}
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("validity_memo/%d", workers), func(t *testing.T) {
			t.Parallel()

			s := New()
			for _, id := range []manifest.NamedResource{p, q} {
				s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			a, b := childWithLabels("p"), childWithLabels("q")
			s.RecordEmitted(p, &Emission{Children: []Child{a}, Complete: true}, nil)
			id := a.Obj.Named()
			_, _, attempt := s.GetEmission(id)
			attempt.Complete = true
			s.RecordEmitted(id, &attempt, nil)
			accepted := s.GetArtifact(id).(*Emission)
			s.RecordEmitted(q, &Emission{Children: []Child{b}, Complete: true}, nil)
			checkReplay := func(want bool) {
				t.Helper()
				seen := false
				s.AddListener(EventArtifactUpdated, func(event manifest.NamedResource, _ any) {
					if event == id {
						seen = true
					}
				}, true)()
				if seen != want {
					t.Errorf("artifact replay = %t, want %t", seen, want)
				}
			}
			checkReplay(true)
			if accepted.validated == nil || accepted.validated.Load() == nil || *accepted.validated.Load() != b.Obj {
				t.Fatal("listener replay did not memoize the equal representative")
			}
			validated := accepted.validated.Load()
			start := make(chan struct{})
			var readers sync.WaitGroup
			for range workers {
				readers.Go(func() {
					<-start
					for range 8 {
						if s.GetArtifact(id) != accepted {
							t.Error("equivalent input invalidated current artifact")
						}
						checkReplay(true)
					}
				})
			}
			close(start)
			readers.Wait()
			if accepted.validated.Load() != validated {
				t.Fatal("memo hits replaced the validated representative cell")
			}
			unequalDoc := manifest.DeepCopyMap(b.Doc)
			unequalDoc["spec"].(map[string]any)["path"] = "different"
			unequal, doc, _, err := manifest.ParseDoc(unequalDoc, manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			s.RecordEmitted(q, &Emission{Children: []Child{{Obj: unequal, Doc: doc}}, Complete: true}, nil)
			if s.GetArtifact(id) != nil {
				t.Fatal("unequal representative reused a validated artifact")
			}
			checkReplay(false)
			if validated := accepted.validated.Load(); validated == nil || *validated != b.Obj {
				t.Fatal("unequal input entered the validity memo")
			}
			replacement := *accepted
			replacement.input, replacement.inputDoc = unequal, doc
			s.RecordEmitted(id, &replacement, nil)
			current := s.GetArtifact(id).(*Emission)
			if current == accepted || current.validated == nil || current.validated == accepted.validated || current.validated.Load() != nil {
				t.Fatal("new emission retained its predecessor's memo")
			}
			s.RecordEmitted(q, &Emission{Children: []Child{b}, Complete: true}, nil)
			if s.GetArtifact(id) != nil {
				t.Fatal("old memo authorized the replacement emission")
			}
			checkReplay(false)
		})
	}

}

func TestRecordEmitted_ShadowedCoverageAncestorReplacement(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"withdrawn", "accepted-child", "other-source-current"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			s := New()
			source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
			s.AddObject(&manifest.GitRepository{Name: source.Name, Namespace: source.Namespace})
			s.SetArtifact(source, &SourceArtifact{Key: "stable", LocalPath: "source"})
			producer := func(name string, binding manifest.NamedResource) Child {
				t.Helper()
				doc := map[string]any{
					"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
					"metadata": map[string]any{"name": name, "namespace": "apps"},
					"spec": map[string]any{
						"path": name, "sourceRef": map[string]any{"kind": binding.Kind, "name": binding.Name},
					},
				}
				obj, authoritative, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return Child{Obj: obj, Doc: authoritative}
			}
			currentSource := source
			if mode == "other-source-current" {
				currentSource.Name = "other-source"
				s.AddObject(&manifest.GitRepository{Name: currentSource.Name, Namespace: currentSource.Namespace})
				s.SetArtifact(currentSource, &SourceArtifact{Key: "other-stable", LocalPath: "other-source"})
			}
			p, k, l := producer("p", source), producer("k", currentSource), producer("l", source)
			m, extra := emissionChild("m", "value"), emissionChild("extra", "value")
			var gp Child
			if mode == "other-source-current" {
				gp = producer("gp", source)
				s.AddObject(gp.Obj)
			} else {
				s.AddObject(p.Obj)
			}
			s.AddObject(l.Obj)

			publish := func(owner Child, children ...Child) {
				t.Helper()
				obj, _, attempt := s.GetEmission(owner.Obj.Named())
				defer attempt.Finish()
				if obj == nil {
					t.Fatalf("%v unavailable before publication", owner.Obj.Named())
				}
				attempt.Children, attempt.Manifests, attempt.Complete = children, nil, true
				for _, child := range children {
					attempt.Manifests = append(attempt.Manifests, child.Doc)
				}
				s.RecordEmitted(owner.Obj.Named(), &attempt, nil)
			}
			check := func(stage string, child Child, want bool) {
				t.Helper()
				id := child.Obj.Named()
				if got := s.GetObject(id) != nil; got != want {
					t.Errorf("%s: %v GetObject visible=%t, want %t", stage, id, got, want)
				}
				if got := slices.ContainsFunc(s.ListObjects("", ""), func(obj manifest.BaseManifest) bool { return obj.Named() == id }); got != want {
					t.Errorf("%s: %v ListObjects visible=%t, want %t", stage, id, got, want)
				}
			}
			if mode == "other-source-current" {
				publish(gp, p)
			}
			publish(l, m)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				l.Obj.Named(): {Productions: []manifest.Production{{Owner: k.Obj.Named(), Source: source, Authored: l.Obj.Named(), Path: "source/l.yaml", File: "l.yaml"}}},
			}})
			publish(p, k)
			for _, child := range []Child{k, l, m} {
				check("initial", child, true)
			}
			if t.Failed() {
				t.FailNow()
			}
			t.Log("initial K/L/M visibility preconditions passed")
			if mode == "accepted-child" {
				publish(k, l)
			} else {
				publish(k)
			}
			retained := mode != "withdrawn"
			check("completed K", k, true)
			check("completed K", l, retained)
			check("completed K", m, retained)
			if t.Failed() {
				t.FailNow()
			}
			t.Logf("completed K visibility preconditions passed: L/M retained=%t", retained)
			if mode == "other-source-current" {
				publish(gp, p, extra)
			} else {
				publish(p, k, extra)
			}
			check("changed ancestor", k, true)
			check("changed ancestor", extra, true)
			check("changed ancestor", l, retained)
			check("changed ancestor", m, retained)
		})
	}
}

func TestRecordEmitted_CoverageAndStaticBoundary(t *testing.T) {
	t.Parallel()

	for _, support := range []string{"producer-only", "bootstrap-orphan", "bootstrap-orphan-equal", "same-source-root", "other-source-root", "self-declared-root", "other-source-only", "declaration-only"} {
		t.Run(support, func(t *testing.T) {
			t.Parallel()

			s := New()
			p := ownerID("p")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			seed := emissionChild("seed", "old")
			s.AddObject(seed.Obj)
			source := manifest.BootstrapSourceID
			productions := []manifest.Production{{Owner: p, Source: source, Authored: seed.Obj.Named(), Path: "source/seed.yaml", File: "seed.yaml"}}
			provisional := support == "bootstrap-orphan" || support == "bootstrap-orphan-equal"
			if provisional {
				productions[0].Owner = manifest.NamedResource{}
			}
			independent := !provisional && support != "producer-only"
			anchor := source
			if support == "other-source-root" || support == "other-source-only" {
				anchor.Name = "other-source"
			}
			if independent {
				root := manifest.Production{Source: anchor, Authored: seed.Obj.Named(), Path: "source/seed.yaml", File: "seed.yaml"}
				if support == "self-declared-root" {
					root.Owner = seed.Obj.Named()
				}
				if support == "declaration-only" {
					root.Declaration = ownerID("declaration")
				}
				productions = append(productions, root)
				if support == "other-source-only" || support == "declaration-only" {
					productions = productions[1:]
				}
			}
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{seed.Obj.Named(): {Productions: productions}}})
			_, _, attempt := s.GetEmission(p)
			actual := emissionChild("seed", "changed")
			if support == "bootstrap-orphan-equal" {
				actual = emissionChild("seed", "old")
			}
			attempt.Children = []Child{actual}
			attempt.Manifests = []map[string]any{actual.Doc}
			attempt.Complete = true
			var events []manifest.BaseManifest
			unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, data any) {
				if id == seed.Obj.Named() {
					obj, _ := data.(manifest.BaseManifest)
					events = append(events, obj)
				}
			}, false)
			s.RecordEmitted(p, &attempt, nil)
			unsub()
			assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
			checkEffectiveObject(t, s, seed.Obj.Named(), actual)
			if independent {
				if s.GetObject(seed.Obj.Named()) != actual.Obj {
					t.Fatal("accepted output did not replace independent static content")
				}
				_, _, empty := s.GetEmission(p)
				empty.Children, empty.Manifests = nil, nil
				empty.Complete = true
				accept(t, s, p, &empty)
				empty.Finish()
				assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), manifest.NamedResource{})
				checkEffectiveObject(t, s, seed.Obj.Named(), seed)
				if got := s.Productions(seed.Obj.Named(), nil); s.GetObject(seed.Obj.Named()) != seed.Obj || len(got) != 1 || got[0] != productions[len(productions)-1] {
					t.Fatalf("complete empty retired independent source/input anchor: %v", got)
				}
				return
			}
			if provisional {
				if got := s.GetObject(seed.Obj.Named()); got != actual.Obj || len(events) != 1 || events[0] != actual.Obj {
					t.Fatalf("bootstrap replacement not published: %v, events %v", got, events)
				}
				if got := s.Productions(seed.Obj.Named(), nil); len(got) != 1 || got[0].Owner != p {
					t.Fatalf("retired bootstrap support: %v", got)
				}
			}
			_, _, empty := s.GetEmission(p)
			empty.Children = nil
			empty.Manifests = nil
			empty.Complete = true
			accept(t, s, p, &empty)
			if s.GetObject(seed.Obj.Named()) != nil || len(s.Productions(seed.Obj.Named(), nil)) != 0 {
				t.Fatal("complete empty resurrected covered prediction")
			}
			checkEffectiveObject(t, s, seed.Obj.Named(), Child{})
			s.AddObject(seed.Obj)
			checkEffectiveObject(t, s, seed.Obj.Named(), seed)
			if s.GetObject(seed.Obj.Named()) == nil {
				t.Fatal("explicit external anchor could not restore covered identity")
			}
		})
	}
	t.Run("anchoring_read_order", func(t *testing.T) {
		t.Parallel()
		s := New()
		p := ownerID("p")
		s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
		child := emissionChild("anchored", "value")
		id := child.Obj.Named()
		s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
		s.RecordEmitted(p, &Emission{Complete: true}, nil)
		s.lockAll()
		defer s.unlockAll()
		sh := s.shardFor(id)
		cell := sh.cells[id]
		assert.Equal(t, cell.hidden, true)
		cell.rooted, cell.static = true, child.Obj
		sh.cells[id] = cell
		assert.Equal(t, sh.objectLocked(id), nil)
		var view capture
		assert.Equal(t, sh.captureLocked(id, cell, &view), nil)
		assert.Equal(t, view.child.Obj, nil)
		assert.Equal(t, view.owner, (*producerSlot)(nil))
		cell.hidden = false
		sh.cells[id] = cell
		assert.Equal(t, sh.objectLocked(id), child.Obj)
		assert.Equal(t, sh.captureLocked(id, cell, &view), child.Obj)
		assert.Equal(t, view.child.Obj, child.Obj)
		assert.Equal(t, view.accepted, true)
	})
	for _, mode := range []string{"rooted_representative", "deleted_static"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			s := New()
			p := ownerID("p")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			seed, child := emissionChild("seed", "static"), emissionChild("seed", "emitted")
			s.AddObject(seed.Obj)
			s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
			assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
			if mode == "rooted_representative" {
				s.AddObject(child.Obj)
				q := ownerID("q")
				s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
				other := emissionChild("seed", "other")
				s.RecordEmitted(q, &Emission{Children: []Child{other}, Complete: true}, nil)
				assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), manifest.NamedResource{})
				warnings := s.Warnings()
				if len(warnings) != 1 {
					t.Fatalf("static conflict warnings: %+v", warnings)
				}
				assert.Equal(t, warnings[0].Resource, seed.Obj.Named())
				assert.Equal(t, warnings[0].Category, manifest.WarnConflictingProducers)
				assert.Equal(t, warnings[0].Count, 1)
				assert.Equal(t, warnings[0].Message, "conflicting producers; using static object")
				assert.Diff(t, warnings[0].Detail, []string{p.String(), q.String()})
				if s.GetObject(seed.Obj.Named()) != seed.Obj {
					t.Fatal("rooted read lost static authority")
				}
				checkEffectiveObject(t, s, seed.Obj.Named(), seed)
				replacement := emissionChild("seed", "replacement")
				s.AddObject(replacement.Obj, replacement.Doc)
				checkEffectiveObject(t, s, seed.Obj.Named(), replacement)
				_, document, replaced := s.GetEmission(seed.Obj.Named())
				assert.Diff(t, document, replacement.Doc)
				replaced.Finish()
				if !s.DeleteObject(seed.Obj.Named()) {
					t.Fatal("static authority was not deleted")
				}
				checkEffectiveObject(t, s, seed.Obj.Named(), other)
				return
			}
			if !s.DeleteObject(seed.Obj.Named()) || s.GetObject(seed.Obj.Named()) != child.Obj {
				t.Fatal("static deletion lost live emitted support")
			}
			assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
			checkEffectiveObject(t, s, seed.Obj.Named(), child)
		})
	}
	t.Run("never_emitted_static", func(t *testing.T) {
		t.Parallel()

		s := New()
		p := ownerID("p")
		s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
		seed := emissionChild("seed", "static")
		s.AddObject(seed.Obj)
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			seed.Obj.Named(): {Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}},
		}})
		_, _, attempt := s.GetEmission(p)
		attempt.Complete = true
		s.RecordEmitted(p, &attempt, nil)
		attempt.Finish()
		if s.GetObject(seed.Obj.Named()) != nil {
			t.Fatal("empty membership exposed hidden static prediction")
		}
	})
	t.Run("replayed_bootstrap_seed", func(t *testing.T) {
		t.Parallel()

		s := New()
		p := ownerID("p")
		s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
		seed := emissionChild("seed", "same")
		s.AddObject(seed.Obj)
		next := &Emission{Children: []Child{seed}, Manifests: []map[string]any{seed.Doc}, Complete: true}
		s.RecordEmitted(p, next, nil)
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			seed.Obj.Named(): {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
		}})
		s.RecordEmitted(p, next, nil)
		s.RecordEmitted(p, &Emission{Complete: true}, nil)
		if s.GetObject(seed.Obj.Named()) != nil {
			t.Fatal("equal replay failed to retire the bootstrap seed")
		}
	})

	for _, mode := range []string{"dependent-chain", "coverage-cycle", "boundary-support", "other-source-current"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			s := New()
			p, q, k, l := ownerID("p"), ownerID("q"), ownerID("k"), ownerID("l")
			child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
			leaf := &manifest.Kustomization{Name: l.Name, Namespace: l.Namespace}
			tail := emissionChild("tail", "static")
			s.AddObjects([]manifest.BaseManifest{
				&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace},
				&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace}, leaf, tail.Obj,
			})
			records := map[manifest.NamedResource]manifest.DiscoveryRecord{
				l:                {Productions: []manifest.Production{{Owner: k, Source: manifest.BootstrapSourceID}}},
				tail.Obj.Named(): {Productions: []manifest.Production{{Owner: l, Source: manifest.BootstrapSourceID}}},
			}
			if mode == "coverage-cycle" {
				s.AddObject(child.Obj)
				records[k] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: l, Source: manifest.BootstrapSourceID}}}
			}
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
			publish := func(owner manifest.NamedResource, present bool) {
				next := &Emission{Complete: true}
				if present {
					next.Children = []Child{child}
				}
				s.RecordEmitted(owner, next, nil)
			}
			check := func(want bool) {
				t.Helper()
				for _, id := range []manifest.NamedResource{k, l, tail.Obj.Named()} {
					sh := s.shardFor(id)
					if (s.GetObject(id) != nil) != want || sh.cells[id].hidden == want {
						t.Errorf("%v support/visibility disagree, want live=%t", id, want)
					}
					if sh.cells[id].static != nil && sh.cells[id].retired == want {
						t.Errorf("%v retirement disagrees, want live=%t", id, want)
					}
				}
			}
			publish(p, true)
			if mode == "other-source-current" {
				_, _, attempt := s.GetEmission(k)
				attempt.binding.source.Name = "other-source"
				attempt.Complete = true
				s.RecordEmitted(k, &attempt, nil)
				attempt.Finish()
			}
			check(true)
			if mode == "boundary-support" {
				publish(q, true)
			}
			publish(p, false)
			check(mode == "boundary-support")
			if mode == "other-source-current" {
				s.AddObject(child.Obj)
				check(true)
				if !s.DeleteObject(k) {
					t.Fatal("independent anchor was not removed")
				}
				check(false)
			}
			if mode == "boundary-support" {
				publish(q, false)
				check(false)
				for _, order := range [][]manifest.NamedResource{{p, q}, {q, p}} {
					publish(order[0], true)
					publish(order[1], true)
					check(true)
					publish(order[0], false)
					check(true)
					publish(order[1], false)
					check(false)
				}
			}
		})
	}
}

func TestRecordEmitted_DirectWritesPreserveSiblings(t *testing.T) {
	t.Parallel()

	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			t.Parallel()

			s := New()
			p, k := ownerID("p"), ownerID("k")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{k: {Productions: []manifest.Production{{Owner: p}}}}})
			child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
			accept(t, s, p, &Emission{Children: []Child{child}, Complete: true})
			grandchild := emissionChild("grandchild", "old")
			accept(t, s, k, &Emission{Children: []Child{grandchild}, Complete: true})
			accept(t, s, p, &Emission{Complete: true})
			replacement := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace, Path: "new"}
			sibling := emissionChild("sibling", "value")
			observed := false
			unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
				if id == k {
					observed = true
					if payload == nil || s.GetObject(k) != replacement || s.GetObject(grandchild.Obj.Named()) == nil {
						t.Error("direct reanchor did not activate retained outgoing support")
					}
					if batch && s.GetObject(sibling.Obj.Named()) == nil {
						t.Error("batch notified before its sibling was live")
					}
				}
			}, false)
			if batch {
				s.AddObjects([]manifest.BaseManifest{replacement, sibling.Obj})
			} else {
				s.AddObject(replacement)
			}
			unsub()
			if !observed {
				t.Fatal("direct update did not notify")
			}
			if s.GetObject(k) != replacement {
				t.Fatal("direct input update lost to predecessor contribution")
			}
			conflict := replacement.Clone()
			conflict.Path = "conflicting"
			s.RecordEmitted(p, &Emission{Children: []Child{{Obj: conflict}}, Complete: true}, nil)
			if s.GetObject(k) != replacement {
				t.Fatal("direct input lost its independent authority")
			}
			s.RecordEmitted(p, &Emission{Complete: true}, nil)

			q := ownerID("q")
			s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
			accept(t, s, q, &Emission{Children: []Child{grandchild}, Complete: true})
			if !s.DeleteObject(k) || s.GetObject(k) != nil || s.GetObject(grandchild.Obj.Named()) == nil {
				t.Fatal("direct support removal lost independent support or retained its root")
			}
			accept(t, s, q, &Emission{Complete: true})
			if s.GetObject(grandchild.Obj.Named()) != nil || s.GetArtifact(k) != nil {
				t.Fatal("deleted root retained outgoing live support")
			}
		})
	}
	for _, state := range []string{"member", "retired", "hidden"} {
		t.Run("equal_static/"+state, func(t *testing.T) {
			t.Parallel()

			s := New()
			p, k := ownerID("p"), ownerID("k")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			seed := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
			s.AddObject(seed)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{k: {Productions: []manifest.Production{{Owner: p}}}}})
			contribution := seed.Clone()
			s.RecordEmitted(p, &Emission{Children: []Child{{Obj: contribution}}, Complete: true}, nil)
			if state == "hidden" {
				child := emissionChild("grandchild", "retained")
				accept(t, s, k, &Emission{Children: []Child{child}, Complete: true})
			}
			if state != "member" {
				accept(t, s, p, &Emission{Complete: true})
				assert.Equal(t, s.GetObject(k), nil)
			}
			s.AddObject(seed)
			if state == "hidden" {
				assert.Equal(t, s.GetObject(emissionChild("grandchild", "retained").Obj.Named()) != nil, true)
			}
			s.RecordEmitted(p, &Emission{Complete: true}, nil)
			if s.GetObject(k) != seed {
				t.Fatal("equal direct write did not retain independent static authority")
			}
			var events int
			defer s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if id == k {
					events++
				}
			}, false)()
			s.RecordEmitted(p, &Emission{Children: []Child{{Obj: contribution}}, Complete: true}, nil)
			s.AddObject(seed)
			if s.GetObject(k) != seed || events != 0 {
				t.Fatal("equal static write changed the rooted representative")
			}
		})
	}
}

func TestStore_ProductionsScopedFileBridge(t *testing.T) {
	t.Parallel()

	t.Run("empty_direct_needs_accepted_root", func(t *testing.T) {
		t.Parallel()

		s := New()
		a, b := ownerID("a"), ownerID("b")
		source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
		target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
		bare := target
		bare.Namespace = ""
		first := manifest.Production{Owner: a, Source: source, Path: "a.yaml", File: "a.yaml"}
		second := manifest.Production{Owner: b, Source: source, Path: "b.yaml", File: "b.yaml"}
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			bare: {Productions: []manifest.Production{first, second}},
		}}
		s.SetDiscoveryIndex(index)
		s.coverage[a].binding.key = "a"
		s.coverage[b].binding.key = "b"
		assert.Diff(t, s.Productions(target, index), []manifest.Production{first})
	})

	a, b := ownerID("a"), ownerID("b")
	source := manifest.BootstrapSourceID
	other := source
	other.Name = "other"
	first := manifest.Production{Owner: a, Source: source, Path: "f.yaml", File: "f.yaml"}
	for _, tc := range []struct {
		name               string
		direct, bare, want []manifest.Production
	}{
		{
			name: "unrelated_source_chain", direct: []manifest.Production{{Source: source}},
			bare: []manifest.Production{{Source: other, Path: "e.yaml"}, {Owner: b, Source: other, Path: "p.yaml"}},
			want: []manifest.Production{{Source: source}},
		},
		{
			name: "foreign_source_chain",
			bare: []manifest.Production{first, {Source: other, Path: "e.yaml"}, {Owner: b, Source: other, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
		{
			name: "mixed_source_chain",
			bare: []manifest.Production{first, {Source: other, Path: "e.yaml"}, {Owner: b, Source: source, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
		{
			name: "direct_mixed_source_chain", direct: []manifest.Production{{Source: source}},
			bare: []manifest.Production{{Source: source, Path: "e.yaml"}, {Owner: b, Source: other, Path: "p.yaml"}},
			want: []manifest.Production{{Source: source}, {Source: source, Path: "e.yaml"}},
		},
		{
			name: "later_bridge_chain", direct: []manifest.Production{{Owner: a, Source: source}},
			bare: []manifest.Production{{Owner: b, Source: source, Path: "p.yaml"}, {Source: source, Path: "e.yaml"}},
			want: []manifest.Production{{Source: source, Path: "e.yaml"}, {Owner: a, Source: source}},
		},
		{
			name: "pathless_chain",
			bare: []manifest.Production{first, {Source: source}, {Owner: b, Source: source, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
		{
			name: "authored_namespace_chain",
			bare: []manifest.Production{first, {Source: source, Path: "e.yaml", Authored: manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "other", Name: "input"}}, {Owner: b, Source: source, Path: "p.yaml"}},
			want: []manifest.Production{first},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			target := emissionChild("input", "value").Obj.Named()
			bare := target
			bare.Namespace = ""
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				target: {Productions: tc.direct}, bare: {Productions: tc.bare},
			}}
			s.SetDiscoveryIndex(index)
			for _, id := range []manifest.NamedResource{a, b} {
				if c := s.coverage[id]; c != nil {
					c.binding.key = id.Name
				}
			}
			assert.Diff(t, s.Productions(target, nil), tc.want)
			assert.Diff(t, s.ReferenceFacts(target, nil).Productions, tc.want)
		})
	}

	t.Run("hidden_bare_only_after_withdrawal", func(t *testing.T) {
		t.Parallel()
		s := New()
		p, q := ownerID("emitter"), ownerID("discovered")
		for _, id := range []manifest.NamedResource{p, q} {
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		}
		child := emissionChild("withdrawn-bare-only", "value")
		target := child.Obj.Named()
		bare := target
		bare.Namespace = ""
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			bare: {Productions: []manifest.Production{{Owner: q, Source: manifest.BootstrapSourceID, Path: "input.yaml"}}},
		}}
		s.SetDiscoveryIndex(index)
		s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
		s.RecordEmitted(p, &Emission{Complete: true}, nil)
		sh := s.shardFor(target)
		sh.mu.RLock()
		hidden, members := sh.cells[target].hidden, len(sh.members[target])
		sh.mu.RUnlock()
		if !hidden || members != 0 || len(index.Records[target].Productions) != 0 {
			t.Fatalf("invalid withdrawal fixture: hidden=%t members=%d direct=%v", hidden, members, index.Records[target].Productions)
		}
		assert.Equal(t, len(s.Productions(target, nil)), 0)
		assert.Diff(t, s.ReferenceFacts(target, nil), ReferenceFacts{})
		parent, ok := s.RenderedParent(target, nil)
		assert.Equal(t, parent, manifest.NamedResource{})
		assert.Equal(t, ok, false)
	})

	for _, differentSource := range []bool{false, true} {
		for _, explicitNamespace := range []bool{false, true} {
			for _, differentBinding := range []bool{false, true} {
				t.Run(fmt.Sprintf("differentSource=%t/explicitNamespace=%t/differentBinding=%t", differentSource, explicitNamespace, differentBinding), func(t *testing.T) {
					t.Parallel()

					s := New()
					renderer, fileOwner := ownerID("renderer"), ownerID("file-owner")
					for _, id := range []manifest.NamedResource{renderer, fileOwner} {
						s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
					}
					source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
					other := source
					if differentSource {
						other.Name = "other"
					}
					target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
					authored := target
					authored.Namespace = ""
					if explicitNamespace {
						authored.Namespace = "other"
					}
					bare := target
					bare.Namespace = ""
					index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: []manifest.Production{{Owner: renderer, Source: source}}}, bare: {Productions: []manifest.Production{{Owner: fileOwner, Source: other, Authored: authored, Path: "source/input.yaml", File: "input.yaml"}}}}}
					s.SetDiscoveryIndex(index)
					if differentBinding {
						s.coverage[fileOwner].binding.key = "other"
					}
					result := s.Productions(target, index)
					facts := s.ReferenceFacts(target, index)
					assert.Diff(t, facts.Productions, result)
					parent, ok := s.RenderedParent(target, nil)
					if !ok || parent != renderer {
						t.Fatalf("rendered parent=%v/%t, want %v/true", parent, ok, renderer)
					}
					want := !differentSource && !explicitNamespace && !differentBinding
					if got := slices.ContainsFunc(result, func(p manifest.Production) bool { return p.Owner == fileOwner && p.Path != "" }); got != want {
						t.Fatalf("scoped bridge=%t want %t: %v", got, want, result)
					}
					proofs := []manifest.Production{{Owner: renderer, Source: source}}
					if want {
						proofs = append(proofs, index.Records[bare].Productions[0])
					}
					slices.SortFunc(proofs, compareProduction)
					assert.Diff(t, result, proofs)
					if _, ok := index.Lookup(manifest.NamedResource{Kind: target.Kind, Namespace: "unrelated", Name: target.Name}); ok {
						t.Fatal("exact index lookup gained wildcard semantics")
					}
				})
			}
		}
	}
	for _, tc := range []struct {
		name, path  string
		otherSource bool
	}{
		{name: "unowned_direct_source", otherSource: true},
		{name: "direct_path", path: "direct.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			target := emissionChild("input", "value").Obj.Named()
			bare := target
			bare.Namespace = ""
			source := manifest.BootstrapSourceID
			other := source
			if tc.otherSource {
				other.Name = "other"
			}
			proof := manifest.Production{Source: source, Path: tc.path, File: tc.path}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				target: {Productions: []manifest.Production{proof}},
				bare:   {Productions: []manifest.Production{{Owner: ownerID("parent"), Source: other, Path: "bare.yaml", File: "bare.yaml"}}},
			}}
			s.SetDiscoveryIndex(index)
			assert.Diff(t, s.Productions(target, nil), []manifest.Production{proof})
			if parent, ok := s.RenderedParent(target, nil); ok || parent != (manifest.NamedResource{}) {
				t.Fatalf("suppressed bridge parent=%v/%t", parent, ok)
			}
		})
	}

	for _, shadowed := range []bool{false, true} {
		t.Run(fmt.Sprintf("ordered_empty_direct/shadowed=%t", shadowed), func(t *testing.T) {
			t.Parallel()
			s := New()
			p, q, r := ownerID("p"), ownerID("q"), ownerID("r")
			for _, owner := range []manifest.NamedResource{p, q, r} {
				s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			}
			target := emissionChild("bridged", "value").Obj.Named()
			bare := target
			bare.Namespace = ""
			source := manifest.BootstrapSourceID
			other := source
			other.Name = "other"
			proofs := []manifest.Production{
				{Owner: r, Source: other},
				{Owner: p, Source: source, Authored: bare, Path: "first.yaml"},
				{Owner: r, Source: other, Path: "other.yaml"},
				{Owner: q, Source: source, Path: "matching.yaml"},
				{Source: source, Path: "unbound.yaml"},
				{Owner: r, Source: source, Path: "later.yaml"},
			}
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{bare: {Productions: proofs}}})
			want := []manifest.Production{proofs[1]}
			if shadowed {
				_, _, next := s.GetEmission(p)
				next.Complete = true
				s.RecordEmitted(p, &next, nil)
				next.Finish()
				want = nil
			}
			slices.SortFunc(want, compareProduction)
			assert.Diff(t, s.Productions(target, nil), want)
			parent, ok := s.RenderedParent(target, nil)
			if ok || parent != (manifest.NamedResource{}) {
				t.Fatalf("authored bridge parent=%v/%t", parent, ok)
			}
		})
	}

}

func TestRecordEmitted_AbsentProducerStaysPrivate(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("absent")
	child := emissionChild("private", "value")
	s.AddObject(child.Obj)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: []manifest.Production{{Owner: p}}},
	}})
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "static"})
	s.UpdateStatus(child.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	if s.GetArtifact(child.Obj.Named()) == nil {
		t.Fatal("fixture has no retained static artifact")
	}
	accept(t, s, p, &Emission{Children: []Child{child}, Manifests: []map[string]any{child.Doc}, Complete: true})
	s.UpdateFailure(p, fmt.Errorf("%w: private failure", manifest.ErrInput))
	s.SetArtifact(p, &SourceArtifact{Key: "private"})
	assert.Equal(t, s.EmissionOwner(ownerID("missing")), manifest.NamedResource{})
	for _, id := range []manifest.NamedResource{p, child.Obj.Named()} {
		assert.Equal(t, s.EmissionOwner(id), manifest.NamedResource{})
		if s.GetObject(id) != nil || s.GetArtifact(id) != nil {
			t.Fatal("absent producer leaked live object or artifact")
		}
		if _, ok := s.GetStatus(id); ok {
			t.Fatal("absent producer leaked terminal state")
		}
		var observation Observation
		obj, observation := s.Snapshot(id, true)
		info := observation.StatusInfo
		conditions := observation.Conditions
		if obj != nil || info != (StatusInfo{}) || len(conditions) != 0 {
			t.Fatal("absent producer leaked snapshot")
		}
		if obj, _, attempt := s.GetEmission(id); obj != nil || len(attempt.Children) != 0 || len(attempt.Manifests) != 0 {
			t.Fatal("absent producer leaked accepted output")
		}
	}
	if len(s.FailedResources()) != 0 || len(s.ListObjects("", "")) != 0 {
		t.Fatal("absent producer leaked inventory")
	}
	for event := EventObjectAdded; event <= EventArtifactUpdated; event++ {
		s.AddListener(event, func(manifest.NamedResource, any) { t.Error("absent producer leaked listener replay") }, true)()
	}
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	if s.GetObject(child.Obj.Named()) == nil {
		t.Fatal("external anchor failed to activate privately completed output")
	}
	if s.GetArtifact(child.Obj.Named()) != nil {
		t.Fatal("external anchor activated a retained static artifact")
	}
	if _, ok := s.GetStatus(child.Obj.Named()); ok {
		t.Fatal("external anchor activated retained static Ready")
	}
	if !s.shardFor(child.Obj.Named()).cells[child.Obj.Named()].retired {
		t.Fatal("actual membership restored shadowed static authority")
	}
	if _, ok := s.GetStatus(p); ok {
		t.Fatal("external anchor activated private Ready/Failed state")
	}
}

func TestStore_AddListener_ReplayWithdrawnDeclaredSecret(t *testing.T) {
	t.Parallel()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	for _, owner := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	}
	secret := &manifest.Secret{Name: "shared", Namespace: "apps"}
	external := &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "external", Namespace: "apps",
		Spec: map[string]any{"target": map[string]any{"name": secret.Name}},
	}
	accept(t, s, p, &Emission{Children: []Child{{Obj: secret}}, Complete: true})
	accept(t, s, q, &Emission{Children: []Child{{Obj: external}}, Complete: true})
	accept(t, s, p, &Emission{Complete: true})
	if s.GetObject(secret.Named()) != nil || s.GetObject(external.Named()) != external {
		t.Fatal("withdrawal did not preserve only the effective ExternalSecret")
	}
	assert.Equal(t, s.EmissionOwner(secret.Named()), manifest.NamedResource{})
	assert.Equal(t, s.EmissionOwner(external.Named()), q)
	proofs := s.Productions(secret.Named(), nil)
	if len(proofs) != 1 || proofs[0].Owner != q || proofs[0].Declaration != external.Named() {
		t.Fatalf("withdrawal lost declaration evidence: %v", proofs)
	}
	inventory := s.ListObjects("", "")
	if len(inventory) != 3 {
		t.Fatalf("effective inventory has %d objects, want two owners and ExternalSecret", len(inventory))
	}
	replayed := make(map[manifest.NamedResource][]any)
	var notifications int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, payload any) {
		replayed[id] = append(replayed[id], payload)
		notifications++
	}, true)
	if len(replayed) != len(inventory) || notifications != len(inventory) {
		t.Fatalf("replayed %d identities and %d notifications, want %d", len(replayed), notifications, len(inventory))
	}
	for _, obj := range inventory {
		if payloads := replayed[obj.Named()]; len(payloads) != 1 || payloads[0] != obj {
			t.Fatalf("replay for %v: got %v, want effective object %v once", obj.Named(), payloads, obj)
		}
	}
	if len(replayed[secret.Named()]) != 0 {
		t.Fatal("replay revived the withdrawn Secret")
	}
	if got := s.ListObjects("", ""); !slices.Equal(got, inventory) {
		t.Fatal("registration changed the effective inventory")
	}
	live := emissionChild("live", "value")
	s.AddObject(live.Obj)
	if s.GetObject(live.Obj.Named()) != live.Obj {
		t.Fatal("store did not retain an object added after replay")
	}
	if payloads := replayed[live.Obj.Named()]; len(payloads) != 1 || payloads[0] != live.Obj || notifications != len(inventory)+1 {
		t.Fatalf("live arrival: got %v and %d notifications, want one new notification", payloads, notifications)
	}
	unsub()
}

func TestStore_ReferenceFacts_ReplacementConsistency(t *testing.T) {
	t.Parallel()

	for _, namespace := range []string{"apps", ""} {
		t.Run("authored_namespace="+namespace, func(t *testing.T) {
			t.Parallel()

			s := New()
			owner := ownerID("owner")
			s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			secret := &manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"VALUE": "visible"}}
			target := secret.Named()
			authored := target
			authored.Namespace = namespace
			proof := manifest.Production{Owner: owner, Source: manifest.BootstrapSourceID, Authored: authored, Path: "input.yaml", File: "input.yaml"}
			records := map[manifest.NamedResource]manifest.DiscoveryRecord{authored: {Productions: []manifest.Production{proof}}}
			proofs := []manifest.Production{proof}
			if namespace == "" {
				scoped := manifest.Production{Owner: owner, Source: manifest.BootstrapSourceID}
				records[target] = manifest.DiscoveryRecord{Productions: []manifest.Production{scoped}}
				proofs = append([]manifest.Production{scoped}, proof)
			}
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
			s.AddObject(secret)
			before := s.ReferenceFacts(target, nil)
			if before.Data != secret || before.Accepted {
				t.Fatalf("file-backed facts=%+v", before)
			}
			assert.Diff(t, before.Productions, proofs)

			external := &manifest.RawObject{
				Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "external", Namespace: target.Namespace,
				Spec: map[string]any{"target": map[string]any{"name": target.Name}, "data": []any{map[string]any{"secretKey": "VALUE"}}},
			}
			_, _, next := s.GetEmission(owner)
			defer next.Finish()
			next.Children, next.Complete = []Child{{Obj: external}}, true
			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				<-start
				s.ReferenceFacts(target, nil)
			})
			wg.Go(func() {
				<-start
				s.RecordEmitted(owner, &next, nil)
			})
			close(start)
			wg.Wait()

			want := []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID, Declaration: external.Named()}}
			assert.Diff(t, s.ReferenceFacts(target, nil), ReferenceFacts{Productions: want})
			assert.Diff(t, s.Productions(target, nil), want)
			if s.GetObject(target) != nil || s.GetObject(external.Named()) != external {
				t.Fatal("replacement did not leave only the declaration")
			}
			parent, ok := s.RenderedParent(target, nil)
			assert.Equal(t, parent, manifest.NamedResource{})
			assert.Equal(t, ok, false)
		})
	}
}

func TestStore_ProductionsQueryAllocations(t *testing.T) {
	s := New()
	id := ownerID("known")
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {Productions: []manifest.Production{{Owner: ownerID("parent")}}},
	}})
	for _, target := range []manifest.NamedResource{ownerID("missing"), id} {
		t.Run(target.Name, func(t *testing.T) {
			if got := testing.AllocsPerRun(10, func() {
				s.Productions(target, nil)
			}); got != 0 {
				t.Fatalf("production query allocations=%v, want 0", got)
			}
			if got := testing.AllocsPerRun(10, func() {
				s.ReferenceFacts(target, nil)
			}); got != 0 {
				t.Fatalf("reference facts allocations=%v, want 0", got)
			}
		})
	}
	obj := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
	s.AddObject(obj)
	var reads ReadSet
	measure := func(productions bool) float64 {
		return testing.AllocsPerRun(10, func() {
			reads.Init(s, obj.Named(), nil)
			reads.GetObject(obj.Named())
			if productions {
				reads.Productions(obj.Named(), nil)
			}
			reads.Retire()
			for range s.Sweep() {
			}
		})
	}
	baseline := measure(false)
	if got := measure(true); got != baseline {
		t.Fatalf("empty captured production query allocations=%v, capture alone=%v", got, baseline)
	}
}

func TestStore_RenderedParentAllocations(t *testing.T) {
	for _, name := range []string{"thin", "frozen", "bridge", "missing"} {
		t.Run(name, func(t *testing.T) {
			s := New()
			owner := ownerID("parent")
			s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			child := emissionChild("child", "value")
			id := child.Obj.Named()
			switch name {
			case "thin":
				s.RecordEmitted(owner, &Emission{Children: []Child{child}, Complete: true}, nil)
			case "frozen", "bridge":
				key := id
				proof := manifest.Production{Owner: owner}
				if name == "bridge" {
					key.Namespace = ""
					proof.Path = "child.yaml"
				}
				s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{key: {Productions: []manifest.Production{proof}}}})
			}
			var parent manifest.NamedResource
			var ok bool
			allocs := testing.AllocsPerRun(10, func() { parent, ok = s.RenderedParent(id, nil) })
			if allocs != 0 || ok != (name != "missing") || (ok && parent != owner) {
				t.Fatalf("parent=%v/%t allocations=%v", parent, ok, allocs)
			}
			if name == "thin" {
				var obj manifest.BaseManifest
				allocs = testing.AllocsPerRun(10, func() { obj = s.GetObject(id) })
				if allocs != 0 || obj != child.Obj {
					t.Fatalf("effective object=%v allocations=%v", obj, allocs)
				}
				allocs = testing.AllocsPerRun(10, func() { parent = s.EmissionOwner(id) })
				if allocs != 0 || parent != owner {
					t.Fatalf("content owner=%v allocations=%v", parent, allocs)
				}
			}
		})
	}

}

func TestStore_ProductionsRetainsAcceptedOwner(t *testing.T) {
	t.Parallel()

	t.Run("frozen_borrow", func(t *testing.T) {
		t.Parallel()

		s := New()
		p, q := ownerID("p"), ownerID("q")
		for _, id := range []manifest.NamedResource{p, q} {
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		}
		child := emissionChild("shared", "value")
		proof := manifest.Production{Owner: p, Source: manifest.BootstrapSourceID}
		backing := []manifest.Production{proof, {Owner: ownerID("sentinel")}, {}, {}}
		original := slices.Clone(backing)
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			child.Obj.Named(): {Productions: backing[:1]},
		}}
		s.SetDiscoveryIndex(index)
		s.AddObject(child.Obj)
		accept(t, s, q, &Emission{Children: []Child{child}, Manifests: []map[string]any{child.Doc}, Complete: true})
		snapshot := s.Productions(child.Obj.Named(), nil)
		facts := s.ReferenceFacts(child.Obj.Named(), nil)
		assert.Diff(t, facts.Data, child.Obj.(manifest.Data))
		if !facts.Accepted {
			t.Fatalf("accepted selected facts=%+v", facts)
		}
		want := []manifest.Production{proof, {Owner: q}}
		accept(t, s, q, &Emission{Complete: true})
		if got := snapshot; !slices.Equal(got, want) {
			t.Fatalf("frozen/runtime snapshot changed: got %v, want %v", got, want)
		}
		assert.Diff(t, facts.Productions, want)
		parent, ok := s.RenderedParent(child.Obj.Named(), nil)
		if !ok || parent != p {
			t.Fatalf("frozen surviving parent=%v/%t, want %v/true", parent, ok, p)
		}
		if !slices.Equal(backing, original) {
			t.Fatalf("query mutated borrowed discovery evidence: got %v, want %v", backing, original)
		}
		owned := child
		owned.Productions = []manifest.Production{proof}
		s.RecordEmitted(p, &Emission{Children: []Child{owned}}, nil)
		borrowed := s.Productions(child.Obj.Named(), nil)
		if len(borrowed) != 1 || &borrowed[0] != &backing[0] {
			t.Fatal("duplicate accepted evidence copied frozen backing")
		}

		for _, evidence := range [][]manifest.Production{
			{{Owner: q}, proof, proof},
			{proof, proof, {Owner: q}},
		} {
			index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Obj.Named(): {Productions: evidence},
			}}
			original = slices.Clone(evidence)
			if got := s.Productions(child.Obj.Named(), index); !slices.Equal(got, want) {
				t.Fatalf("query did not sort/deduplicate frozen evidence: got %v, want %v", got, want)
			}
			if !slices.Equal(evidence, original) {
				t.Fatalf("query reordered borrowed discovery evidence: got %v, want %v", evidence, original)
			}
		}
	})
	t.Run("hidden_owner_rooted_child", func(t *testing.T) {
		t.Parallel()
		s := New()
		p := ownerID("absent")
		child := emissionChild("rooted", "value")
		s.AddObject(child.Obj)
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			child.Obj.Named(): {Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}},
		}})
		s.RecordEmitted(p, &Emission{Children: []Child{child}}, nil)
		s.AddObject(child.Obj)
		if s.GetObject(child.Obj.Named()) != child.Obj || s.GetObject(p) != nil {
			t.Fatal("fixture needs an independently rooted child and hidden owner")
		}
		assert.Equal(t, len(s.Productions(child.Obj.Named(), nil)), 0)
		if parent, ok := s.RenderedParent(child.Obj.Named(), nil); ok || parent != (manifest.NamedResource{}) {
			t.Fatalf("hidden owner parent=%v/%t", parent, ok)
		}
	})

	for _, filtered := range []int{0, 1} {
		t.Run(fmt.Sprintf("filtered_frozen_%d", filtered), func(t *testing.T) {
			t.Parallel()

			s := New()
			p, q, r := ownerID("p"), ownerID("q"), ownerID("r")
			owners := []manifest.NamedResource{p, q, r}
			for _, id := range owners {
				s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			child := emissionChild("shared", "value")
			proofs := []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}, {Owner: q, Source: manifest.BootstrapSourceID}, {Owner: r, Source: manifest.BootstrapSourceID}}
			original := slices.Clone(proofs)
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Obj.Named(): {Productions: proofs},
			}})
			s.AddObject(child.Obj)
			_, _, attempt := s.GetEmission(owners[filtered])
			attempt.Complete = true
			accept(t, s, owners[filtered], &attempt)
			attempt.Finish()
			snapshot := s.Productions(child.Obj.Named(), nil)
			parent, ok := s.RenderedParent(child.Obj.Named(), nil)
			wantParent := p
			if filtered == 0 {
				wantParent = q
			}
			if !ok || parent != wantParent {
				t.Fatalf("filtered parent=%v/%t, want %v/true", parent, ok, wantParent)
			}
			if !slices.Equal(proofs, original) {
				t.Fatalf("filter mutated borrowed discovery evidence: got %v, want %v", proofs, original)
			}
			want := slices.Delete(slices.Clone(original), filtered, filtered+1)
			for _, id := range owners {
				_, _, attempt := s.GetEmission(id)
				attempt.Complete = true
				accept(t, s, id, &attempt)
				attempt.Finish()
			}
			if got := snapshot; !slices.Equal(got, want) {
				t.Fatalf("filtered snapshot changed after withdrawal: got %v, want %v", got, want)
			}
			if got := s.Productions(child.Obj.Named(), nil); len(got) != 0 {
				t.Fatalf("hidden frozen evidence remains visible: %v", got)
			}
		})
	}
	t.Run("captured_concurrent", func(t *testing.T) {
		t.Parallel()

		s := New()
		child := emissionChild("shared", "value")
		var owners []manifest.NamedResource
		var want []manifest.Production
		for i := range 4 {
			id := ownerID(fmt.Sprint(3 - i))
			owners = append(owners, id)
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			proof := manifest.Production{Owner: id, Source: manifest.BootstrapSourceID}
			want = append(want, proof)
			owned := child
			owned.Productions = []manifest.Production{proof, proof}
			accept(t, s, id, &Emission{Children: []Child{owned}, Manifests: []map[string]any{owned.Doc}, Complete: true})
		}
		slices.SortFunc(want, compareProduction)
		snapshot := s.Productions(child.Obj.Named(), nil)
		var wg sync.WaitGroup
		for _, id := range owners {
			wg.Go(func() {
				replacement := emissionChild(id.Name, "replacement")
				accept(t, s, id, &Emission{Children: []Child{replacement}, Manifests: []map[string]any{replacement.Doc}, Complete: true})
				accept(t, s, id, &Emission{Complete: true})
			})
		}
		wg.Wait()
		got := snapshot
		if !slices.Equal(got, want) {
			t.Fatalf("captured productions changed after concurrent replacement/withdrawal: got %v, want %v", got, want)
		}
		if got := s.Productions(child.Obj.Named(), nil); len(got) != 0 {
			t.Fatalf("withdrawn productions remain visible: %v", got)
		}
		for range snapshot {
			s.AddObject(child.Obj)
		}
	})
	for _, ownEvidence := range []bool{false, true} {
		t.Run(fmt.Sprint(ownEvidence), func(t *testing.T) {
			t.Parallel()

			s := New()
			p, q := ownerID("p"), ownerID("q")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			child := emissionChild("child", "value")
			proof := manifest.Production{Owner: q, Authored: child.Obj.Named(), File: "proof.yaml", Source: manifest.BootstrapSourceID}
			if ownEvidence {
				proof.Owner = p
			}
			child.Productions = []manifest.Production{proof}
			accept(t, s, p, &Emission{Children: []Child{child}, Manifests: []map[string]any{child.Doc}, Complete: true})
			got := s.Productions(child.Obj.Named(), nil)
			want := 2
			if ownEvidence {
				want = 1
			}
			if len(got) != want || !slices.Contains(got, proof) || !slices.ContainsFunc(got, func(v manifest.Production) bool { return v.Owner == p }) {
				t.Fatalf("accepted owner lost or redundant thin evidence added: %v", got)
			}
			parent, ok := s.RenderedParent(child.Obj.Named(), nil)
			if ok == ownEvidence || (!ownEvidence && parent != p) {
				t.Fatalf("authored parent=%v/%t, own evidence=%t", parent, ok, ownEvidence)
			}
			accept(t, s, p, &Emission{Complete: true})
			if got := s.Productions(child.Obj.Named(), nil); len(got) != 0 || s.GetObject(child.Obj.Named()) != nil {
				t.Fatalf("withdrawal retained accepted evidence: %v", got)
			}
			withdrawn := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Obj.Named(): {Productions: []manifest.Production{{Owner: q}}},
			}}
			assert.Equal(t, len(s.Productions(child.Obj.Named(), withdrawn)), 0)
			if parent, ok := s.RenderedParent(child.Obj.Named(), withdrawn); ok || parent != (manifest.NamedResource{}) {
				t.Fatalf("hidden explicit-index parent=%v/%t", parent, ok)
			}

		})
	}
}

func TestRecordEmitted_ConflictingProducers(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()

				s := New()
				owners := make([]manifest.NamedResource, workers)
				children := make([]Child, workers)
				release := make([]chan struct{}, workers)
				done := make(chan struct{})
				var wg sync.WaitGroup
				for i := range workers {
					owners[i] = ownerID(fmt.Sprint(i))
					children[i] = emissionChild("shared", fmt.Sprint(i))
					s.AddObject(&manifest.Kustomization{Name: owners[i].Name, Namespace: owners[i].Namespace})
					release[i] = make(chan struct{})
					wg.Go(func() {
						defer func() { done <- struct{}{} }()
						<-release[i]
						accept(t, s, owners[i], &Emission{Children: []Child{children[i]}, Manifests: []map[string]any{children[i].Doc}, Complete: true})
					})
				}
				for i := range workers {
					if reverse {
						i = workers - 1 - i
					}
					close(release[i])
					<-done
				}
				wg.Wait()
				winner := children[workers-1]
				id := winner.Obj.Named()
				check := func(child Child, owner manifest.NamedResource) {
					t.Helper()
					assert.Equal(t, s.EmissionOwner(id), owner)
					var observation Observation
					obj, observation := s.Snapshot(id, true)
					info := observation.StatusInfo
					conditions := observation.Conditions
					assert.Equal(t, obj, child.Obj)
					want, _ := s.GetStatus(id)
					assert.Equal(t, info, want)
					assert.Equal(t, conditions == nil, true)
					checkEffectiveObject(t, s, id, child)
				}
				check(winner, owners[workers-1])
				warnings := s.Warnings()
				want := make([]string, workers)
				for i, owner := range owners {
					want[i] = owner.String()
				}
				if len(warnings) != 1 || warnings[0].Resource != id || warnings[0].Category != "ConflictingProducers" || !slices.Equal(warnings[0].Detail, want) || warnings[0].Count != 1 {
					t.Fatalf("conflict warning: %+v", warnings)
				}
				assert.Equal(t, warnings[0].Message, "conflicting producers; using "+owners[workers-1].String())
				accept(t, s, owners[workers-1], &Emission{Complete: true})
				check(children[workers-2], owners[workers-2])
				warnings = s.Warnings()
				if workers == 2 && len(warnings) != 0 || workers == 4 && (len(warnings) != 1 || !slices.Equal(warnings[0].Detail, want[:workers-1])) {
					t.Fatalf("withdrawn producer remained in warning: %+v", warnings)
				}
				if len(warnings) != 0 {
					assert.Equal(t, warnings[0].Message, "conflicting producers; using "+owners[workers-2].String())
				}
				for _, owner := range owners {
					accept(t, s, owner, &Emission{Children: []Child{winner}, Manifests: []map[string]any{winner.Doc}, Complete: true})
				}
				check(winner, owners[workers-1])
				if warnings := s.Warnings(); len(warnings) != 0 {
					t.Fatalf("equal content warned: %+v", warnings)
				}
			})
		}
	}
}

func TestRecordEmitted_SharedChildArrival(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			t.Parallel()

			s := New()
			child := &manifest.HelmChartSource{Name: "shared", Namespace: "apps", Chart: "app-template"}
			var arrivals atomic.Int64
			unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if id == child.Named() {
					arrivals.Add(1)
				}
			}, false)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range workers {
				owner := ownerID(fmt.Sprint(i))
				s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
				wg.Go(func() {
					<-start
					synthetic := *child
					s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: &synthetic}}}, nil)
					s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: &synthetic}, emissionChild(owner.Name, "complete")}, Complete: true}, nil)
				})
			}
			close(start)
			wg.Wait()
			unsub()
			if got := arrivals.Load(); got != 1 {
				t.Fatalf("identical shared child arrivals=%d want 1", got)
			}
		})
	}
}

func TestRecordEmitted_NeverEmittedCoverageWithdrawal(t *testing.T) {
	t.Parallel()

	seedRetained := func(t *testing.T, s *Store, id manifest.NamedResource) {
		t.Helper()
		s.SetArtifact(id, &SourceArtifact{Key: "static-l"})
		s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
		if s.GetObject(id) == nil || s.GetArtifact(id) == nil {
			t.Fatal("fixture L not initially live with an artifact")
		}
		if info, ok := s.GetStatus(id); !ok || info.Status != StatusReady {
			t.Fatal("fixture L not initially Ready")
		}
	}
	fixture := func(t *testing.T, kName, lName string, reverse bool) (*Store, manifest.NamedResource, manifest.NamedResource, Child) {
		t.Helper()
		s := New()
		p, k := ownerID("p"), ownerID(kName)
		parent := &manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}
		l := emissionChild(lName, "value")
		if reverse {
			s.AddObject(l.Obj)
			s.AddObject(parent)
		} else {
			s.AddObject(parent)
			s.AddObject(l.Obj)
		}
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			l.Obj.Named(): {Productions: []manifest.Production{{Owner: k, Source: manifest.BootstrapSourceID, Authored: l.Obj.Named(), Path: "l.yaml", File: "l.yaml"}}},
		}})
		s.RecordEmitted(p, &Emission{Children: []Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}}, Complete: true}, nil)
		if s.GetObject(k) == nil || !slices.ContainsFunc(s.ListObjects("", ""), func(obj manifest.BaseManifest) bool { return obj.Named() == l.Obj.Named() }) {
			t.Fatal("fixture K/L not initially visible")
		}
		seedRetained(t, s, l.Obj.Named())
		if s.shardFor(k).slots[k].current.Load() != nil {
			t.Fatal("fixture K unexpectedly emitted")
		}
		return s, p, k, l
	}
	checkHidden := func(t *testing.T, s *Store, id manifest.NamedResource) {
		t.Helper()
		if s.GetObject(id) != nil {
			t.Errorf("%v object stays visible after withdrawal", id)
		}
		if slices.ContainsFunc(s.ListObjects("", ""), func(obj manifest.BaseManifest) bool { return obj.Named() == id }) {
			t.Errorf("%v stays listed after withdrawal", id)
		}
		if s.GetArtifact(id) != nil {
			t.Errorf("%v artifact stays live after withdrawal", id)
		}
		if _, ok := s.GetStatus(id); ok {
			t.Errorf("%v status stays live after withdrawal", id)
		}
		sh := s.shardFor(id)
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		var view capture
		sh.captureLocked(id, sh.cells[id], &view)
		child := view.child
		var owner manifest.NamedResource
		if view.owner != nil {
			owner = view.owner.id
		}
		if !sh.cells[id].hidden || !sh.cells[id].retired || child.Obj != nil || owner != (manifest.NamedResource{}) {
			t.Errorf("%v hidden, retirement and representative disagree", id)
		}
		if slot := sh.slots[id]; slot != nil && (slot.visible.Load() || !slot.hidden.Load()) {
			t.Errorf("%v slot remains live", id)
		}
	}
	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		s, p, k, l := fixture(t, "k", "l", false)
		s.RecordEmitted(p, &Emission{Complete: true}, nil)
		checkHidden(t, s, k)
		checkHidden(t, s, l.Obj.Named())
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("reanchor/batch=%t", batch), func(t *testing.T) {
				t.Parallel()

				s, p, k, l := fixture(t, "k", "l", false)
				s.RecordEmitted(p, &Emission{Complete: true}, nil)
				checkHidden(t, s, k)
				checkHidden(t, s, l.Obj.Named())
				s.SetArtifact(l.Obj.Named(), &SourceArtifact{Key: "private"})
				s.UpdateStatus(l.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
				anchor := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
				if batch {
					s.AddObjects([]manifest.BaseManifest{anchor})
				} else {
					s.AddObject(anchor)
				}
				if s.GetObject(k) != anchor || s.GetObject(l.Obj.Named()) != l.Obj {
					t.Fatal("external anchor did not restore eligible frozen descendants")
				}
				if s.GetArtifact(l.Obj.Named()) != nil {
					t.Error("reanchoring restored a retained artifact")
				}
				if _, ok := s.GetStatus(l.Obj.Named()); ok {
					t.Error("reanchoring restored retained Ready")
				}
				sh := s.shardFor(l.Obj.Named())
				if sh.cells[l.Obj.Named()].retired || sh.cells[l.Obj.Named()].hidden {
					t.Error("restored frozen descendant remains retired or hidden")
				}
				seedRetained(t, s, l.Obj.Named())
				if !s.DeleteObject(k) {
					t.Fatal("independent anchor was not removed")
				}
				checkHidden(t, s, k)
				checkHidden(t, s, l.Obj.Named())
			})
		}
	})
	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()

				kName, lName := "k", "l"
				if reverse {
					kName, lName = "z", "a"
				}
				s, p, k, l := fixture(t, kName, lName, reverse)
				tasks := task.NewBounded(workers)
				captured, observing, withdrawn := make(chan struct{}), make(chan struct{}), make(chan struct{})
				late := emissionChild("late", "private")
				tasks.Go(t.Context(), "late-publication", func(context.Context) {
					_, _, attempt := s.GetEmission(k)
					defer attempt.Finish()
					attempt.Children, attempt.Complete = []Child{late}, true
					close(captured)
					tasks.YieldSlot(func() { <-withdrawn })
					checkHidden(t, s, l.Obj.Named())
					s.RecordEmitted(k, &attempt, nil)
					s.SetArtifact(l.Obj.Named(), &SourceArtifact{Key: "late-private"})
					s.UpdateStatus(l.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
				})
				tasks.Go(t.Context(), "withdrawal", func(context.Context) {
					tasks.YieldSlot(func() { <-captured; <-observing })
					s.RecordEmitted(p, &Emission{Complete: true}, nil)
					close(withdrawn)
				})
				tasks.Go(t.Context(), "observer", func(context.Context) {
					close(observing)
					for {
						select {
						case <-withdrawn:
							checkHidden(t, s, l.Obj.Named())
							return
						default:
							s.GetObject(l.Obj.Named())
							s.GetArtifact(l.Obj.Named())
							s.GetStatus(l.Obj.Named())
						}
					}
				})
				tasks.BlockTillDone()
				if tasks.Failures() != 0 {
					t.Fatal("bounded task panicked")
				}
				for _, id := range []manifest.NamedResource{k, l.Obj.Named(), late.Obj.Named()} {
					checkHidden(t, s, id)
				}
				s.AddObject(&manifest.Kustomization{Name: k.Name, Namespace: k.Namespace})
				if s.GetObject(late.Obj.Named()) != late.Obj || s.GetObject(l.Obj.Named()) != nil {
					t.Fatal("reanchoring must restore actual membership without restoring shadowed coverage")
				}
				if !s.shardFor(l.Obj.Named()).cells[l.Obj.Named()].retired {
					t.Fatal("shadowed static prediction was not retired")
				}
				if !s.DeleteObject(k) {
					t.Fatal("late publisher anchor was not removed")
				}
				checkHidden(t, s, late.Obj.Named())
			})
		}
	}
}

func TestRecordEmitted_SeedDocumentArrival(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example"},
	}
	obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.AddObject(obj)
	foreignDoc := manifest.DeepCopyMap(doc)
	foreignDoc["apiVersion"] = "example.test/v1"
	foreign, _, _, err := manifest.ParseDoc(foreignDoc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.AddObject(foreign)
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: doc}, {Obj: foreign, Doc: foreignDoc}}, Manifests: []map[string]any{foreignDoc, doc}, Complete: true}, nil)
	if arrivals != 0 {
		t.Fatalf("equal seed acquired document with %d arrivals", arrivals)
	}
	_, _, accepted := s.GetEmission(p)
	if len(accepted.Manifests) != 2 || !specValueEqual(accepted.Manifests[0], foreignDoc) || !specValueEqual(accepted.Manifests[1], doc) {
		t.Fatal("suppressed arrival lost authoritative document")
	}
	if got := s.Productions(obj.Named(), nil); len(got) != 1 || got[0].Owner != p {
		t.Fatalf("accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["apiVersion"] = manifest.SourceDomain + "/v1beta2"
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")}, Manifests: []map[string]any{next}}, nil)
	if arrivals != 1 {
		t.Fatalf("changed authoritative document arrivals = %d, want 1", arrivals)
	}
	_, _, accepted = s.GetEmission(p)
	if len(accepted.Manifests) != 2 || !specValueEqual(accepted.Manifests[0], next) || !specValueEqual(accepted.Manifests[1], foreignDoc) {
		t.Fatalf("partial version update lost cross-group documents: %v", accepted.Manifests)
	}
	if got := s.Productions(foreign.Named(), nil); len(got) != 1 || got[0].Owner != p {
		t.Fatalf("cross-group support = %v", got)
	}

	next = manifest.DeepCopyMap(next)
	next["metadata"].(map[string]any)["annotations"] = map[string]any{"semantic": "changed"}
	next["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": p.Name}
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: next}}, Complete: true}, nil)
	if arrivals != 1 {
		t.Fatalf("unchanged source document metadata caused %d arrivals, want 1", arrivals)
	}
	_, retained, attempt := s.GetEmission(obj.Named())
	defer attempt.Finish()
	if !specValueEqual(retained, next) {
		t.Fatal("suppressed arrival lost updated document metadata")
	}

}

type admissionBarrier struct {
	*manifest.ConfigMap
	calls    atomic.Int64
	prepared chan struct{}
	release  <-chan struct{}
}

func (b *admissionBarrier) Named() manifest.NamedResource {
	// Two collection lookups and one proven-target lookup precede prepareEmission indexing.
	if b.calls.Add(1) == 4 {
		close(b.prepared)
		<-b.release
	}
	return b.ConfigMap.Named()
}

func TestRecordEmitted_ReadsInvalidatedBeforeCommit(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"stable", "replacement", "payload"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			owner := ownerID("owner")
			s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
			input := emissionChild("input", "old")
			s.AddObject(input.Obj)
			acceptedInput := emissionChild("accepted-input", "old")
			s.AddObject(acceptedInput.Obj)
			var accepted ReadSet
			accepted.Init(s, owner, nil)
			defer accepted.Retire()
			accepted.GetObject(acceptedInput.Obj.Named())
			acceptedHead := accepted.input(acceptedInput.Obj.Named()).object.head
			child := &manifest.ConfigMap{Name: "output", Namespace: "apps"}
			output := &Emission{Children: []Child{{Obj: child}}, Fingerprint: "accepted", Complete: true}
			accepted.Bind(output)
			s.RecordEmitted(owner, output, nil)
			previous := s.GetArtifact(owner).(*Emission)
			var reads ReadSet
			reads.Init(s, owner, nil)
			defer reads.Retire()
			reads.GetObject(input.Obj.Named())
			rejectedHead := reads.input(input.Obj.Named()).object.head
			if acceptedHead == rejectedHead {
				t.Fatal("accepted and successor-only inputs share a registration head")
			}
			name := child.Name
			if mode == "replacement" {
				name = "replacement"
			}
			release := make(chan struct{})
			barrier := &admissionBarrier{
				ConfigMap: &manifest.ConfigMap{Name: name, Namespace: "apps"},
				prepared:  make(chan struct{}),
				release:   release,
			}
			next := &Emission{Children: []Child{{Obj: barrier}}, Fingerprint: "stale", Complete: true}
			reads.Bind(next)
			resume := sync.OnceFunc(func() { close(release) })
			var payloads map[manifest.NamedResource]*SourceArtifact
			if mode == "payload" {
				payloads = map[manifest.NamedResource]*SourceArtifact{child.Named(): {Key: "stale", FS: fstest.MapFS{}}}
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.RecordEmitted(owner, next, payloads)
			}()
			t.Cleanup(func() { resume(); <-done })
			select {
			case <-barrier.prepared:
			case <-done:
				t.Fatal("publication did not reach the admission barrier")
			}
			if !reads.Valid() {
				t.Fatal("reads invalid before concurrent input replacement")
			}
			s.AddObject(emissionChild("input", "new").Obj)
			if reads.Valid() {
				t.Fatal("input replacement did not invalidate the pending publication")
			}
			resume()
			<-done
			if s.GetArtifact(owner) != previous || s.GetObject(child.Named()) != child || s.GetArtifact(child.Named()) != nil {
				t.Fatal("stale proposal replaced authoritative output")
			}
			if mode == "replacement" && s.GetObject(barrier.ConfigMap.Named()) != nil {
				t.Fatal("stale proposal installed a replacement child")
			}
			if previous.OutputReads != &accepted || accepted.retired.Load() {
				t.Error("rejected proposal retired accepted output provenance")
			}
			reads.Retire()
			assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
			if !reads.retired.Load() || rejectedHead.first.Load() != nil {
				t.Error("sweep retained rejected attempt registrations")
			}
			if acceptedHead.first.Load() == nil || s.GetArtifact(owner) != previous {
				t.Error("sweep removed accepted output or its input registration")
			}
			s.AddObject(emissionChild("accepted-input", "new").Obj)
			if !slices.Contains(slices.Collect(s.Sweep()), owner) {
				t.Fatal("accepted input change did not notify its owner after successor rejection")
			}
		})
	}
}

func TestRecordEmitted_ArtifactPredictions(t *testing.T) {
	t.Parallel()
	generator := func(template string) Child {
		return Child{Obj: &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": template}}}}}
	}
	consumer := func(name string) Child {
		return Child{Obj: &manifest.Kustomization{Name: "consumer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: name}}}
	}
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
	for _, mode := range []string{"proposal", "frozen", "other-owner", "other-source", "other-pin"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			s.AddObject(owner)
			binding := sourceBinding{source: manifest.BootstrapSourceID, key: "pin", root: "/view"}
			g, c := generator("app-{app}"), consumer(target.Name)
			s.SetArtifact(binding.source, &SourceArtifact{Key: binding.key, LocalPath: binding.root})
			if mode != "proposal" {
				provenance := manifest.Production{Owner: owner.Named(), Source: binding.source, File: "consumer.yaml", Path: "/view/consumer.yaml"}
				if mode == "other-owner" {
					provenance.Owner.Name = "other"
				}
				if mode == "other-source" {
					provenance.Source.Name = "other"
				}
				decl := provenance
				decl.Declaration, decl.File, decl.Path = g.Obj.Named(), "generator.yaml", ""
				s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					c.Obj.Named(): {File: provenance.File, Path: provenance.Path, Refs: []manifest.NamedResource{target}, Productions: []manifest.Production{provenance}},
					target:        {Productions: []manifest.Production{decl}},
				}})
				// The frozen source identity and pin are captured before publication.
				if contribution := s.coverage[owner.Named()]; contribution != nil {
					contribution.binding = binding
				}
			}
			proposal := &Emission{Children: []Child{g}, Complete: true, binding: binding}
			if mode == "proposal" {
				proposal.Children = append(proposal.Children, c)
			}
			if contribution := s.coverage[owner.Named()]; contribution != nil {
				proposal.coverage = contribution.records
			}
			if mode == "other-pin" {
				proposal.binding.key = "other-pin"
			}
			s.RecordEmitted(owner.Named(), proposal, nil)
			want := mode == "proposal" || mode == "frozen"
			check := func(want bool) {
				t.Helper()
				_, _, current := s.GetEmission(owner.Named())
				defer current.Finish()
				var predicted bool
				for _, a := range s.GetArtifact(owner.Named()).(*Emission).entries {
					for _, p := range a.targets {
						predicted = predicted || p.NamedResource == target
					}
				}
				if predicted != want {
					t.Fatalf("predicted = %t, want %t", predicted, want)
				}
				facts := s.ReferenceFacts(target, nil)
				assert.Diff(t, facts.Productions, s.Productions(target, nil))
				if facts.Data != nil || facts.Accepted || s.GetObject(target) != nil || s.GetArtifact(target) != nil {
					t.Fatal("prediction supplied content")
				}
			}
			check(want)
			_, _, next := s.GetEmission(owner.Named())
			next.Children = []Child{g}
			next.Complete = true
			next.binding = proposal.binding
			s.RecordEmitted(owner.Named(), &next, nil)
			check(want)
			next.Finish()
			if mode == "frozen" {
				_, _, replacement := s.GetEmission(owner.Named())
				replacement.Children, replacement.Complete, replacement.binding = []Child{g, consumer("app-web")}, true, proposal.binding
				s.RecordEmitted(owner.Named(), &replacement, nil)
				check(false)
				replacement.Finish()
				for range 2 {
					_, _, withoutConsumer := s.GetEmission(owner.Named())
					withoutConsumer.Children, withoutConsumer.Complete, withoutConsumer.binding = []Child{g}, true, proposal.binding
					s.RecordEmitted(owner.Named(), &withoutConsumer, nil)
					check(false)
					withoutConsumer.Finish()
				}
			}
			_, _, next = s.GetEmission(owner.Named())
			next.Children, next.Complete, next.binding = []Child{generator("different-{app}")}, true, proposal.binding
			s.RecordEmitted(owner.Named(), &next, nil)
			check(false)
			next.Finish()
		})
	}

	t.Run("replacement_and_partial_retention", func(t *testing.T) {
		t.Parallel()
		s := New()
		owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
		s.AddObject(owner)
		g := generator("app-{app}")
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(target.Name)}, Complete: true}, nil)
		replacement := target
		replacement.Name = "app-web"
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(replacement.Name)}, Complete: true}, nil)
		if len(s.Productions(replacement, nil)) != 1 {
			t.Fatal("reference replacement lost new prediction")
		}
		if len(s.Productions(target, nil)) != 0 {
			t.Fatal("reference replacement retained old prediction")
		}
		_, _, current := s.GetEmission(owner.Named())
		if current.Children[0].Obj != g.Obj {
			t.Fatal("unchanged declaration replaced")
		}
		current.Finish()
		s.RecordEmitted(owner.Named(), &Emission{}, nil)
		if len(s.Productions(replacement, nil)) != 1 {
			t.Fatal("incomplete empty withdrew prediction")
		}
		s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
		if len(s.Productions(replacement, nil)) != 0 {
			t.Fatal("successful empty retained prediction")
		}
	})

	t.Run("retained_consumer", func(t *testing.T) {
		t.Parallel()
		s := New()
		owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
		s.AddObject(owner)
		g, retained := generator("app-{app}"), consumer(target.Name)
		retained.Obj = &manifest.Kustomization{Name: "retained", Namespace: "apps", SourceRef: retained.Obj.(*manifest.Kustomization).SourceRef}
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(target.Name), retained}, Complete: true}, nil)
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer("app-web")}, Complete: true}, nil)
		if len(s.Productions(target, nil)) != 1 {
			t.Fatal("retained consumer lost its declaration's prediction")
		}
	})

	t.Run("retry_rebuilds_admissions", func(t *testing.T) {
		t.Parallel()
		s := New()
		owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
		s.AddObject(owner)
		g := generator("app-{app}")
		binding := sourceBinding{source: manifest.BootstrapSourceID, key: "pin", root: "/view"}
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(target.Name)}, Complete: true, binding: binding}, nil)
		release := make(chan struct{})
		barrier := &admissionBarrier{
			ConfigMap: &manifest.ConfigMap{Name: "barrier", Namespace: "apps"},
			prepared:  make(chan struct{}),
			release:   release,
		}
		barrierID := barrier.ConfigMap.Named()
		resume := sync.OnceFunc(func() { close(release) })
		var writers sync.WaitGroup
		t.Cleanup(func() {
			resume()
			writers.Wait()
		})
		writers.Go(func() {
			s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, {Obj: barrier}, {Obj: &manifest.ConfigMap{Name: "inert", Namespace: "apps"}}}, Complete: true, binding: binding}, nil)
		})
		<-barrier.prepared
		replacement := target
		replacement.Name = "app-web"
		published := make(chan struct{})
		writers.Go(func() {
			defer close(published)
			s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(replacement.Name)}, Complete: true, binding: binding}, nil)
		})
		<-published
		check := func() {
			t.Helper()
			accepted := s.GetArtifact(owner.Named()).(*Emission)
			var targets []manifest.ProducerTarget
			for _, a := range accepted.entries {
				if a.child.Obj.Named() == g.Obj.Named() {
					targets = a.targets
				}
			}
			assert.Diff(t, targets, []manifest.ProducerTarget{{NamedResource: replacement}})
			assert.Diff(t, s.Productions(replacement, nil), []manifest.Production{{Owner: owner.Named(), Declaration: g.Obj.Named(), Source: binding.source}})
			if len(s.Productions(target, nil)) != 0 {
				t.Error("retry retained superseded prediction")
			}
			for _, id := range []manifest.NamedResource{target, replacement} {
				facts := s.ReferenceFacts(id, nil)
				assert.Diff(t, facts.Productions, s.Productions(id, nil))
				if facts.Data != nil || facts.Accepted || s.GetObject(id) != nil || s.GetArtifact(id) != nil {
					t.Error("prediction supplied content")
				}
			}
		}
		check()
		resume()
		writers.Wait()
		if s.GetObject(barrierID) != barrier {
			t.Fatal("retry did not accept the paused proposal")
		}
		check()
	})

	t.Run("concurrent_proposals", func(t *testing.T) {
		t.Parallel()
		s := New()
		owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
		s.AddObject(owner)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, name := range []string{"app-api", "app-web"} {
			wg.Go(func() {
				<-start
				s.RecordEmitted(owner.Named(), &Emission{Children: []Child{generator("app-{app}"), consumer(name)}, Complete: true}, nil)
			})
		}
		close(start)
		wg.Wait()
		accepted := s.GetArtifact(owner.Named()).(*Emission)
		var candidates []manifest.NamedResource
		for _, a := range accepted.entries {
			for ref := range manifest.References(a.child.Obj, manifest.ReferenceRelations) {
				candidates = append(candidates, ref.NamedResource)
			}
		}
		want := manifest.ProducerTargets(generator("app-{app}").Obj, candidates...)
		for _, a := range accepted.entries {
			if a.child.Obj.Named() == generator("").Obj.Named() {
				assert.Diff(t, a.targets, want)
			}
		}
	})
}

func TestRecordEmitted_IdenticalArtifactEmission(t *testing.T) {
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	generator := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "app-{app}"}}}}
	consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindExternalArtifact, Name: "app-api"}}
	next := &Emission{Children: []Child{{Obj: generator}, {Obj: consumer}}, Complete: true}
	s.RecordEmitted(owner.Named(), next, nil)
	accepted := s.GetArtifact(owner.Named())
	if got := testing.AllocsPerRun(10, func() { s.RecordEmitted(owner.Named(), next, nil) }); got != 0 {
		t.Fatalf("identical dynamic emission allocated %v times, want zero", got)
	}
	if s.GetArtifact(owner.Named()) != accepted {
		t.Fatal("identical emission replaced the accepted proposal")
	}
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
	facts := s.ReferenceFacts(target, nil)
	assert.Diff(t, facts.Productions, s.Productions(target, nil))
	assert.Diff(t, facts.Productions, []manifest.Production{{Owner: owner.Named(), Declaration: generator.Named()}})
	if facts.Data != nil || facts.Accepted {
		t.Fatalf("prediction supplied authority: %+v", facts)
	}
}

func TestRecordEmitted_PartialSubsetPreservesCompletedOutput(t *testing.T) {
	t.Parallel()
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	s.AddObject(owner)
	first, last := emissionChild("first", "a"), emissionChild("last", "b")
	_, _, complete := s.GetEmission(owner.Named())
	complete.Children = []Child{first, last}
	complete.Manifests = []map[string]any{first.Doc, last.Doc}
	complete.Fingerprint = manifest.Fingerprint(complete.Manifests)
	complete.Complete = true
	s.RecordEmitted(owner.Named(), &complete, nil)
	complete.Finish()
	var observer, writer ReadSet
	observer.Init(s, ownerID("observer"), nil)
	writer.Init(s, owner.Named(), nil)
	defer observer.Retire()
	defer writer.Retire()
	before := observer.GetArtifact(owner.Named()).(*Emission)
	var objects, artifacts int
	stopObjects := s.AddListener(EventObjectAdded, func(manifest.NamedResource, any) { objects++ }, false)
	defer stopObjects()
	stopArtifacts := s.AddListener(EventArtifactUpdated, func(manifest.NamedResource, any) { artifacts++ }, false)
	defer stopArtifacts()
	_, _, partial := writer.GetEmission(owner.Named())
	partial.Children = partial.Children[len(partial.Children)-1:]
	s.RecordEmitted(owner.Named(), &partial, nil)
	partial.Finish()
	after := s.GetArtifact(owner.Named()).(*Emission)
	if !observer.Valid() || objects != 0 || artifacts != 0 {
		t.Fatalf("equal partial subset invalidated output: valid=%t objects=%d artifacts=%d", observer.Valid(), objects, artifacts)
	}
	if !after.Complete || after.Fingerprint != before.Fingerprint {
		t.Fatal("equal partial subset lost completed render identity")
	}
	if !slices.EqualFunc(after.Children, before.Children, childEqual) {
		t.Fatal("equal partial subset changed its children")
	}
	assert.Diff(t, after.Manifests, before.Manifests)
}

func TestRecordEmitted_RetiresOnlyReplacedOutputBorrower(t *testing.T) {
	t.Parallel()
	for _, payload := range []bool{false, true} {
		for _, mode := range []string{"partial", "rebound", "rejected"} {
			t.Run(fmt.Sprintf("payload=%t/%s", payload, mode), func(t *testing.T) {
				t.Parallel()
				s := New()
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				s.AddObject(owner)
				var first, second ReadSet
				first.Init(s, owner.Named(), nil)
				second.Init(s, owner.Named(), nil)
				defer first.Retire()
				defer second.Retire()
				_, _, output := first.GetEmission(owner.Named())
				child := emissionChild("output", "old")
				output.Children, output.Manifests = []Child{child}, []map[string]any{child.Doc}
				output.Fingerprint = manifest.Fingerprint(output.Manifests)
				output.Complete = true
				first.Bind(&output)
				var payloads map[manifest.NamedResource]*SourceArtifact
				if payload {
					payloads = map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "content"}}
				}
				s.RecordEmitted(owner.Named(), &output, payloads)
				output.Finish()
				_, _, next := second.GetEmission(owner.Named())
				defer next.Finish()
				next.Complete = mode != "partial"
				if mode == "rejected" {
					child = emissionChild("output", "new")
					next.Children, next.Manifests = []Child{child}, []map[string]any{child.Doc}
					next.Fingerprint = manifest.Fingerprint(next.Manifests)
					sh := s.shardFor(owner.Named())
					sh.mu.Lock()
					sh.slots[owner.Named()].generation = ^uint64(0)
					sh.mu.Unlock()
				}
				second.Bind(&next)
				if mode == "rejected" {
					func() {
						defer func() {
							if message, ok := recover().(string); !ok || message != "store: authority generation exhausted" {
								t.Error("expected exhausted publication rejection")
							}
						}()
						s.RecordEmitted(owner.Named(), &next, nil)
					}()
				} else {
					s.RecordEmitted(owner.Named(), &next, nil)
				}
				current := s.GetArtifact(owner.Named()).(*Emission)
				if mode == "rebound" {
					if !first.retired.Load() || current.OutputReads != &second {
						t.Fatal("accepted rebound retained the predecessor borrower")
					}
				} else if first.retired.Load() || current.OutputReads != &first {
					t.Fatal("publication retired an output borrower it did not replace")
				}
				if !current.OutputReads.Valid() {
					t.Fatal("current output lost valid provenance")
				}
			})
		}
	}
}

func TestRecordEmitted_RejectedPublicationPreservesOutputWatches(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"stable", "replacement"} {
		for _, rejection := range []string{"invalid_reads", "stale_generation"} {
			t.Run(route+"/"+rejection, func(t *testing.T) {
				t.Parallel()
				s := New()
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				a := &manifest.Kustomization{Name: "input-a", Namespace: "apps", Path: "old"}
				b := &manifest.Kustomization{Name: "input-b", Namespace: "apps", Path: "old"}
				s.AddObjects([]manifest.BaseManifest{owner, a, b})
				var first, second ReadSet
				first.Init(s, owner.Named(), nil)
				second.Init(s, owner.Named(), nil)
				defer first.Retire()
				defer second.Retire()
				_, _, output := first.GetEmission(owner.Named())
				assert.Equal(t, first.GetObject(a.Named()), manifest.BaseManifest(a))
				child := emissionChild("output", "accepted")
				output.Children, output.Manifests = []Child{child}, []map[string]any{child.Doc}
				output.Fingerprint = manifest.Fingerprint(output.Manifests)
				output.Complete = true
				first.Bind(&output)
				s.RecordEmitted(owner.Named(), &output, nil)
				output.Finish()
				accepted := s.GetArtifact(owner.Named()).(*Emission)

				tasks := task.NewBounded(2)
				captured, stale := make(chan struct{}), make(chan struct{})
				var next Emission
				tasks.Go(t.Context(), "publish", func(context.Context) {
					_, _, next = second.GetEmission(owner.Named())
					defer next.Finish()
					assert.Equal(t, second.GetObject(b.Named()), manifest.BaseManifest(b))
					proposedChild := emissionChild("output", "proposed")
					if route == "replacement" {
						proposedChild = emissionChild("replacement", "proposed")
					}
					next.Children, next.Manifests = []Child{proposedChild}, []map[string]any{proposedChild.Doc}
					next.Fingerprint = manifest.Fingerprint(next.Manifests)
					next.Complete = true
					second.Bind(&next)
					close(captured)
					tasks.YieldSlot(func() { <-stale })
					if rejection == "stale_generation" {
						if !second.Valid() || next.inputHead.generation == output.inputHead.generation {
							t.Error("fixture did not isolate stale generation with valid reads")
						}
					} else if second.Valid() {
						t.Error("fixture did not invalidate proposal reads")
					}
					s.RecordEmitted(owner.Named(), &next, nil)
				})
				tasks.Go(t.Context(), "stale", func(context.Context) {
					defer close(stale)
					tasks.YieldSlot(func() { <-captured })
					if rejection == "invalid_reads" {
						changed := b.Clone()
						changed.Path = "new"
						s.AddObject(changed)
					} else {
						next.inputHead.generation--
					}
				})
				tasks.BlockTillDone()
				if tasks.Failures() != 0 {
					t.Fatal("publication task panicked")
				}
				current := s.GetArtifact(owner.Named()).(*Emission)
				if current != accepted || current.OutputReads != &first {
					t.Fatal("rejection replaced the accepted emission or borrower")
				}
				if first.retired.Load() || !first.Valid() {
					t.Error("rejection retired or invalidated the accepted output borrower")
				}
				for id := range s.Sweep() {
					t.Errorf("rejection invalidated unchanged accepted owner %v", id)
				}
				_, _, retained := s.GetEmission(owner.Named())
				defer retained.Finish()
				if diff := cmp.Diff(retained.Children, []Child{child}, cmp.AllowUnexported(Child{})); diff != "" {
					t.Fatalf("rejection changed accepted children (-want +got): %s", diff)
				}
				assert.Diff(t, retained.Manifests, output.Manifests)

				changed := a.Clone()
				changed.Path = "new"
				s.AddObject(changed)
				if delivered := slices.Collect(s.Sweep()); !slices.Contains(delivered, owner.Named()) {
					t.Error("accepted input change did not deliver its owner through Sweep")
				}
				if first.Valid() {
					t.Error("accepted output remained valid after its input changed")
				}
				_, _, invalid := s.GetEmission(owner.Named())
				defer invalid.Finish()
				if len(invalid.Children) != 0 || len(invalid.Manifests) != 0 {
					t.Fatal("invalid accepted output retained children or manifests")
				}
			})
		}
	}
}

func TestReadSet_CloseRetiresHiddenBorrowedOutput(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"changed", "missing", "source_hidden"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s := New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			var declaration manifest.BaseManifest = &manifest.Kustomization{Name: "consumer", Namespace: "apps", Path: "old"}
			if scenario == "source_hidden" {
				declaration = &manifest.GitRepository{Name: "consumer", Namespace: "apps"}
			}
			s.AddObject(parent)
			for i := range 64 {
				s.AddObject(&manifest.ConfigMap{Name: fmt.Sprintf("input-%d", i), Namespace: parent.Namespace})
			}
			s.UpdateStatus(parent.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
			publish := func(children ...Child) {
				s.RecordEmitted(parent.Named(), &Emission{Children: children, Complete: true}, nil)
			}
			publish(Child{Obj: declaration})
			var first, second ReadSet
			first.Init(s, declaration.Named(), nil)
			defer first.Retire()
			first.GetObject(declaration.Named())
			first.GetStatus(parent.Named())
			first.RequireData(parent.Named())
			first.ListObjects("", manifest.KindConfigMap)
			if scenario == "source_hidden" {
				s.SetArtifact(declaration.Named(), &SourceArtifact{Reads: &first, Key: "content"})
			} else {
				_, _, output := first.GetEmission(declaration.Named())
				child := emissionChild("output", "old")
				output.Children, output.Manifests = []Child{child}, []map[string]any{child.Doc}
				output.Complete = true
				first.Bind(&output)
				s.RecordEmitted(declaration.Named(), &output, nil)
				output.Finish()
			}
			first.Accept()
			if scenario == "changed" {
				changed := *declaration.(*manifest.Kustomization)
				changed.Path = "new"
				publish(Child{Obj: &changed})
			} else {
				publish()
			}
			if s.GetArtifact(declaration.Named()) != nil {
				t.Fatal("changed parent input left the old artifact visible")
			}
			second.Init(s, declaration.Named(), nil)
			second.GetObject(declaration.Named())
			second.Close()
			for range s.Sweep() {
			}
			registrations := 0
			for input := first.inputs; input != nil; input = input.next {
				for kind := objectFacet; kind <= bucketFacet; kind++ {
					cell := input.cell(kind)
					if cell == nil || cell.head == nil {
						continue
					}
					for current := cell.head.first.Load(); current != nil; current = current.next.Load() {
						if current == cell {
							registrations++
						}
					}
				}
			}
			if !first.retired.Load() || registrations != 0 {
				t.Fatalf("cancelled successor left hidden borrower active: retired=%t registrations=%d", first.retired.Load(), registrations)
			}
			if first.GetObject(declaration.Named()) != declaration {
				t.Fatal("closing discarded the accepted read snapshot")
			}
			if !slices.Contains(slices.Collect(first.Required()), parent.Named()) {
				t.Fatal("closing discarded the accepted required-input footprint")
			}
			sh := s.shardFor(declaration.Named())
			sh.mu.RLock()
			var retained *ReadSet
			if scenario == "source_hidden" {
				retained = sh.artifacts[declaration.Named()].value.(*SourceArtifact).Reads
			} else {
				retained = sh.slots[declaration.Named()].current.Load().OutputReads
			}
			sh.mu.RUnlock()
			if retained != &first {
				t.Fatal("closing discarded borrowed output provenance")
			}
		})
	}
}

func TestRecordEmitted_IndirectAuthorityArrival(t *testing.T) {
	t.Parallel()
	st := New()
	root := &manifest.Kustomization{Name: "root", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	child := &manifest.OCIRepository{Name: "child", Namespace: "apps", URL: "oci://accepted.example"}
	st.AddObject(root)
	accept(t, st, root.Named(), &Emission{Children: []Child{{Obj: producer}}, Complete: true})
	accept(t, st, producer.Named(), &Emission{Children: []Child{{Obj: child}}, Complete: true})
	var arrivals atomic.Int64
	unsub := st.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == child.Named() {
			arrivals.Add(1)
		}
	}, false)
	defer unsub()
	accept(t, st, root.Named(), &Emission{Children: []Child{{Obj: producer.Clone()}, emissionChild("sibling", "new")}, Complete: true})
	if st.GetDeclaration(child.Named()) != child || arrivals.Load() != 0 {
		t.Fatalf("unchanged descendant arrivals=%d, want 0", arrivals.Load())
	}
}

func TestGetDeclaration_CoveredAuthority(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"covered", "failed", "empty", "accepted", "accepted failed", "surviving", "withdrawn", "self", "independent", "root"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			st := New()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			peer := &manifest.Kustomization{Name: "peer", Namespace: "apps"}
			raw := &manifest.OCIRepository{Name: "source", Namespace: "apps", URL: "oci://raw.example"}
			st.AddObjects([]manifest.BaseManifest{parent, peer, raw})
			proofs := []manifest.Production{{Owner: parent.Named(), Source: manifest.BootstrapSourceID}}
			switch state {
			case "self":
				proofs = append(proofs, manifest.Production{Owner: raw.Named()})
			case "independent":
				proofs = append(proofs, manifest.Production{Source: manifest.BootstrapSourceID})
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: proofs}}})
			st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "other children"})
			var want manifest.BaseManifest
			switch state {
			case "self", "independent":
				want = raw
			case "root":
				accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: raw}}})
				st.AddObject(raw)
				want = raw
			case "failed":
				st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusFailed, Reason: "", Message: "failed before child"})
			case "empty":
				accept(t, st, parent.Named(), &Emission{Complete: true})
			case "accepted", "accepted failed", "surviving", "withdrawn":
				accepted := *raw
				accepted.URL = "oci://accepted.example"
				accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: &accepted}}})
				want = &accepted
				if state == "accepted failed" {
					st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusFailed, Reason: "", Message: "failed successor"})
				}
				if state == "surviving" {
					accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: &accepted}}})
				}
				if state == "surviving" || state == "withdrawn" {
					accept(t, st, parent.Named(), &Emission{Complete: true})
				}
				if state == "withdrawn" {
					want = nil
				}
			}
			if got := st.GetDeclaration(raw.Named()); got != want {
				t.Fatalf("effective=%v want=%v", got, want)
			}
			got, _, inventory := st.GetEmission(raw.Named())
			defer inventory.Finish()
			if got != want {
				t.Fatalf("emission input=%v want=%v", got, want)
			}
			if state == "covered" && st.GetObject(raw.Named()) != raw {
				t.Fatal("coverage hid raw admission identity")
			}
		})
	}
}

func TestRecordEmitted_ReplacedInputRejected(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"entry_stable", "entry_replacement", "locked_stable", "locked_replacement"} {
		for _, loss := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/loss_%t", route, loss), func(t *testing.T) {
				t.Parallel()
				st := New()
				owner, peer := &manifest.Kustomization{Name: "owner", Namespace: "apps"}, &manifest.Kustomization{Name: "peer", Namespace: "apps"}
				input := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "A"}
				st.AddObjects([]manifest.BaseManifest{owner, peer, input})
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{input.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}}}})
				accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: input}}, Complete: true})
				child := emissionChild("prior", "A")
				accept(t, st, input.Named(), &Emission{Children: []Child{child}, Manifests: []map[string]any{child.Doc}, Complete: true})
				tasks := task.NewBounded(2)
				captured, replaced := make(chan struct{}), make(chan struct{})
				tasks.Go(t.Context(), "publish", func(context.Context) {
					var reads ReadSet
					reads.Init(st, input.Named(), nil)
					defer reads.Retire()
					_, _, next := reads.GetEmission(input.Named())
					defer next.Finish()
					next.Fingerprint = "stale"
					if strings.HasSuffix(route, "replacement") {
						next.Children = []Child{emissionChild("stale", "A")}
					}
					sh := st.shardFor(input.Named())
					sh.mu.RLock()
					slot := sh.slots[input.Named()]
					previous := slot.current.Load()
					generation := slot.generation
					sh.mu.RUnlock()
					var proposal *Emission
					if strings.HasPrefix(route, "locked_") {
						var prepared []admission
						for _, c := range next.Children {
							prepared = append(prepared, admission{child: c, targets: manifest.ProducerTargets(c.Obj), binding: next.binding})
						}
						proposal = prepareEmission(&next, previous, prepared, make(map[manifest.NamedResource]int, len(prepared)))
					}
					close(captured)
					tasks.YieldSlot(func() { <-replaced })
					result := publicationRejected
					switch route {
					case "locked_stable":
						result = st.commitStable(slot, previous, proposal, generation)
					case "locked_replacement":
						result = st.commitReplacement(input.Named(), previous, proposal, generation)
					default:
						st.RecordEmitted(input.Named(), &next, nil)
					}
					if result != publicationRejected || reads.Valid() {
						t.Error("stale inventory accepted")
					}
				})
				tasks.Go(t.Context(), "replace", func(context.Context) {
					tasks.YieldSlot(func() { <-captured })
					if loss {
						accept(t, st, peer.Named(), &Emission{Complete: true})
						if st.GetObject(input.Named()) != input || st.GetDeclaration(input.Named()) != nil {
							t.Error("fixture did not retain raw input without authority")
						}
					} else {
						replacement := input.Clone()
						replacement.Path = "B"
						accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true})
					}
					close(replaced)
				})
				tasks.BlockTillDone()
				if tasks.Failures() != 0 {
					t.Fatal("publication task panicked")
				}
				_, _, retained := st.GetEmission(input.Named())
				defer retained.Finish()
				if retained.Fingerprint == "stale" || len(retained.Children) != 1 || retained.Children[0].Obj != child.Obj {
					t.Fatal("rejection replaced accepted output")
				}
				assert.Diff(t, retained.Children[0].Doc, child.Doc)
				assert.Diff(t, retained.Manifests, []map[string]any{child.Doc})
			})
		}
	}
}

func TestRecordEmitted_EqualDeclarationRetainsCapture(t *testing.T) {
	t.Parallel()
	st := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	input := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
	st.AddObject(parent)
	accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: input}}, Fingerprint: "parent-A", Complete: true})
	captured, _, inventory := st.GetEmission(input.Named())
	defer inventory.Finish()
	if captured != input {
		t.Fatal("fixture did not capture accepted child")
	}
	var arrivals atomic.Int64
	stop := st.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == input.Named() {
			arrivals.Add(1)
		}
	}, false)
	defer stop()
	equivalent := input.Clone()
	accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: equivalent}}, Fingerprint: "parent-B", Complete: true})
	if !objectEqual(st.GetDeclaration(input.Named()), captured) || arrivals.Load() != 0 {
		t.Fatalf("equal publication invalidated capture without arrival: arrivals=%d", arrivals.Load())
	}
	inventory.Fingerprint, inventory.Complete = "child-A", true
	st.RecordEmitted(input.Named(), &inventory, nil)
}

func TestRecordEmitted_SameOutputUsesCurrentInput(t *testing.T) {
	t.Parallel()
	s := New()
	a := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "A"}
	s.AddObject(a)
	_, _, first := s.GetEmission(a.Named())
	defer first.Finish()
	first.Children = []Child{emissionChild("child", "same")}
	first.Fingerprint, first.Complete = manifest.Fingerprint("same"), true
	s.RecordEmitted(a.Named(), &first, nil)
	b := a.Clone()
	b.Path = "B"
	s.AddObject(b)
	_, _, next := s.GetEmission(b.Named())
	defer next.Finish()
	next.Complete, next.Fingerprint = true, first.Fingerprint
	s.RecordEmitted(b.Named(), &next, nil)
	current := s.GetArtifact(b.Named()).(*Emission)
	if current.input != b || current.Fingerprint != first.Fingerprint || s.GetObject(first.Children[0].Obj.Named()) == nil {
		t.Fatalf("equal output retained obsolete input: input=%v fingerprint=%q", current.input, current.Fingerprint)
	}
}

func TestReferenceFacts_PendingPeerAfterEmptySupplier(t *testing.T) {
	t.Parallel()
	s := New()
	first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
	empty := &manifest.Kustomization{Name: "empty", Namespace: "apps"}
	s.AddObjects([]manifest.BaseManifest{first, empty})
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{{Owner: first.Named()}, {Owner: empty.Named()}}},
	}})
	s.UpdateStatus(first.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "pending supplier"})
	_, _, next := s.GetEmission(empty.Named())
	defer next.Finish()
	next.Complete = true
	s.RecordEmitted(empty.Named(), &next, nil)
	facts := s.ReferenceFacts(target, nil)
	if len(facts.Productions) != 1 || facts.Productions[0].Owner != first.Named() || facts.Data != nil || facts.Accepted {
		t.Fatalf("pending peer lost after empty supplier: facts=%+v", facts)
	}
}

func TestRecordEmitted_CrossGroupSeedDocumentArrival(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("publisher")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	doc := map[string]any{
		"apiVersion": manifest.SourceDomain + "/v1", "kind": manifest.KindHelmRepository,
		"metadata": map[string]any{"name": "shared", "namespace": "apps"},
		"spec":     map[string]any{"url": "https://charts.example"},
	}
	obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.AddObject(obj)
	foreignDoc := manifest.DeepCopyMap(doc)
	foreignDoc["apiVersion"] = "example.test/v1"
	foreign, _, _, err := manifest.ParseDoc(foreignDoc, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.AddObject(foreign)
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(p, captureEmission(s, p, &Emission{Children: []Child{{Obj: obj, Doc: doc}, {Obj: foreign, Doc: foreignDoc}}, Manifests: []map[string]any{foreignDoc, doc}, Complete: true}), nil)
	if arrivals != 0 {
		t.Fatalf("equal seed acquired document with %d arrivals", arrivals)
	}
	_, _, accepted := s.GetEmission(p)
	if len(accepted.Manifests) != 2 || !specValueEqual(accepted.Manifests[0], foreignDoc) || !specValueEqual(accepted.Manifests[1], doc) {
		t.Fatal("suppressed arrival lost authoritative document")
	}
	if got := s.Productions(obj.Named(), nil); len(got) != 1 || got[0].Owner != p {
		t.Fatalf("accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["apiVersion"] = manifest.SourceDomain + "/v1beta2"
	s.RecordEmitted(p, captureEmission(s, p, &Emission{Children: []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")}, Manifests: []map[string]any{next}}), nil)
	if arrivals != 1 {
		t.Fatalf("changed authoritative document arrivals = %d, want 1", arrivals)
	}
	_, _, accepted = s.GetEmission(p)
	if len(accepted.Manifests) != 2 || !specValueEqual(accepted.Manifests[0], next) || !specValueEqual(accepted.Manifests[1], foreignDoc) {
		t.Fatalf("partial version update lost cross-group documents: %v", accepted.Manifests)
	}
	if got := s.Productions(foreign.Named(), nil); len(got) != 1 || got[0].Owner != p {
		t.Fatalf("cross-group support = %v", got)
	}

}

func TestRecordEmitted_PayloadPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		reverse, persistent bool
	}{
		{name: "memory/a-first"},
		{name: "memory/z-first", reverse: true},
		{name: "persistent/a-first", persistent: true},
		{name: "persistent/z-first", persistent: true, reverse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z, zz := ownerID("a"), ownerID("z"), ownerID("zz")
			for _, id := range []manifest.NamedResource{a, z, zz} {
				s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			firstChild, lastChild, declaration := emissionChild("generated", "a"), emissionChild("generated", "z"), emissionChild("generated", "zz")
			target := declaration.Obj.Named()
			root := ""
			if tc.persistent {
				root = t.TempDir()
			}
			cache, err := sourcecache.New(t.Context(), root)
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, cache.Close()) })
			acquire := func(value string) *SourceArtifact {
				t.Helper()
				input, descriptor, key, err := cache.Acquire(t.Context(), sourcecache.ArchiveKey{Content: value, Policy: "fixed"}, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(_ context.Context, target sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					writer := tar.NewWriter(target)
					if err := writer.AddFS(fstest.MapFS{"file": {Data: []byte(value)}}); err != nil {
						return sourcecache.ProducerResult{}, err
					}
					return sourcecache.ProducerResult{}, writer.Close()
				})
				assert.NoError(t, err)
				return &SourceArtifact{Kind: target.Kind, URL: "https://" + value + ".test", LocalPath: "/" + value, LocalRoot: "/root-" + value, Key: key, Digest: descriptor.Annotations["flate.tree-digest"], Revision: "revision-" + value, Metadata: map[string]string{"origin": value}, FS: input}
			}
			first, last := acquire("a"), acquire("z")
			first.Size, last.Size, last.InputAlias = 1, 2, true
			peer := New()
			peer.AddObject(&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace})
			peer.RecordEmitted(a, &Emission{Children: []Child{firstChild}, Complete: true}, map[manifest.NamedResource]*SourceArtifact{target: first})
			owners, payloads := []manifest.NamedResource{a, z}, []*SourceArtifact{first, last}
			children := []Child{firstChild, lastChild}
			if tc.reverse {
				slices.Reverse(owners)
				slices.Reverse(payloads)
				slices.Reverse(children)
			}
			start, done := make(chan struct{}), make(chan struct{})
			tasks := task.NewBounded(2)
			for i := range owners {
				tasks.Go(t.Context(), owners[i].Name, func(context.Context) {
					if i == 1 {
						tasks.YieldSlot(func() { <-start })
					}
					s.RecordEmitted(owners[i], &Emission{Children: []Child{children[i]}, Manifests: []map[string]any{children[i].Doc}, Complete: true}, map[manifest.NamedResource]*SourceArtifact{target: payloads[i]})
					if i == 0 {
						close(start)
						tasks.YieldSlot(func() { <-done })
					} else {
						close(done)
					}
				})
			}
			tasks.BlockTillDone()
			if tasks.Failures() != 0 {
				t.Fatal("publication panicked")
			}
			s.RecordEmitted(zz, &Emission{Children: []Child{declaration}, Manifests: []map[string]any{declaration.Doc}, Complete: true}, nil)
			assert.Diff(t, s.Warnings(), []manifest.Warning{{Resource: target, Category: manifest.WarnConflictingProducers, Count: 1, Message: "conflicting producers; using " + zz.String(), Detail: []string{a.String(), z.String(), zz.String()}}})
			seen := 0
			unsubscribe := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
				if id == target {
					seen++
					if value != last {
						t.Error("replay selected a different payload")
					}
				}
			}, true)
			unsubscribe()
			if seen != 1 {
				t.Fatalf("generated payload replay count = %d", seen)
			}
			if s.EmissionOwner(target) != zz || s.GetArtifact(target) != last {
				t.Fatal("declaration winner suppressed greatest byte contributor")
			}
			checkDeclaration := func() {
				t.Helper()
				obj, doc, attempt := s.GetEmission(target)
				defer attempt.Finish()
				assert.Equal(t, obj, declaration.Obj)
				assert.Diff(t, doc, declaration.Doc)
				assert.Equal(t, s.EmissionOwner(target), zz)
			}
			checkDeclaration()
			if data, err := fs.ReadFile(first.FS, "file"); err != nil || string(data) != "a" {
				t.Fatalf("losing payload = %q, %v", data, err)
			}
			opened, err := last.FS.Open("file")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = opened.Close() }()
			s.RecordEmitted(z, &Emission{Complete: true}, nil)
			if s.GetArtifact(target) != first {
				t.Fatal("withdrawal did not select the whole surviving descriptor")
			}
			checkDeclaration()
			if data, err := fs.ReadFile(s.GetArtifact(target).(*SourceArtifact).FS, "file"); err != nil || string(data) != "a" {
				t.Fatalf("surviving payload = %q, %v", data, err)
			}
			s.RecordEmitted(a, &Emission{Complete: true}, nil)
			if s.GetObject(target) == nil || s.GetArtifact(target) != nil {
				t.Fatal("declaration-only survivor retained byte authority")
			}
			checkDeclaration()
			if data, err := io.ReadAll(opened); err != nil || string(data) != "z" {
				t.Fatalf("already-open withdrawn backing = %q, %v", data, err)
			}
			if peer.GetArtifact(target) != first {
				t.Fatal("withdrawal discarded another store's shared cache backing")
			}
			peer.RecordEmitted(a, &Emission{Complete: true}, nil)
			for _, payload := range []*SourceArtifact{first, last} {
				if data, err := fs.ReadFile(payload.FS, "file"); err != nil || string(data) != payload.Metadata["origin"] {
					t.Fatalf("unowned cache backing = %q, %v", data, err)
				}
			}
		})
	}
}

func TestRecordEmitted_PayloadProvenance(t *testing.T) {
	t.Parallel()
	s := New()
	p := ownerID("producer")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	child := emissionChild("target", "same")
	target := child.Obj.Named()
	payload := &SourceArtifact{Kind: target.Kind, Key: "key", Digest: "digest", FS: fstest.MapFS{"file": {Data: []byte("bytes")}}}
	_, _, attempt := s.GetEmission(p)
	defer attempt.Finish()
	attempt.Children = []Child{child}
	attempt.Fingerprint = "same"
	s.RecordEmitted(p, &attempt, map[manifest.NamedResource]*SourceArtifact{target: payload})
	if attempt.Fingerprint != "" || s.GetArtifact(p).(*Emission).Fingerprint != "" {
		t.Fatal("partial payload publication retained successful fingerprint eligibility")
	}
	attempt.Children, attempt.Complete = []Child{child}, true
	attempt.Fingerprint = "same"
	s.RecordEmitted(p, &attempt, nil)
	if s.GetArtifact(target) != payload {
		t.Fatal("same-attempt nil finalization removed payload")
	}
	_, _, replay := s.GetEmission(p)
	defer replay.Finish()
	replay.Complete = true
	s.RecordEmitted(p, &replay, nil)
	if s.GetArtifact(target) != payload {
		t.Fatal("borrowed replay removed payload")
	}
	s.RecordEmitted(p, &Emission{Children: []Child{child}, Manifests: replay.Manifests, Fingerprint: replay.Fingerprint, Complete: true}, nil)
	if s.GetArtifact(target) != nil {
		t.Fatal("fresh declaration-only replacement retained bytes")
	}
	s.RecordEmitted(p, &Emission{Children: []Child{child}, Fingerprint: "same", Complete: true}, map[manifest.NamedResource]*SourceArtifact{target: payload})
	other := emissionChild("other", "new")
	_, _, replacement := s.GetEmission(p)
	defer replacement.Finish()
	replacement.Children, replacement.Complete = []Child{child, other}, true
	s.RecordEmitted(p, &replacement, map[manifest.NamedResource]*SourceArtifact{other.Obj.Named(): payload})
	if s.GetArtifact(target) != nil || s.GetArtifact(other.Obj.Named()) != payload {
		t.Fatal("new attempt borrowed an earlier payload for its fresh declaration-only child")
	}
}

func TestRecordEmitted_PayloadUpdate(t *testing.T) {
	t.Parallel()
	s := New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(owner)
	child := emissionChild("generated", "same")
	target := child.Obj.Named()
	first := &SourceArtifact{Key: "first", Digest: "first", FS: fstest.MapFS{"file": {Data: []byte("first")}}}
	second := &SourceArtifact{Key: "second", Digest: "second", FS: fstest.MapFS{"file": {Data: []byte("second")}}}
	_, _, initial := s.GetEmission(owner.Named())
	initial.Children, initial.Manifests, initial.Fingerprint, initial.Complete = []Child{child}, []map[string]any{child.Doc}, "same", true
	s.RecordEmitted(owner.Named(), &initial, map[manifest.NamedResource]*SourceArtifact{target: first})
	var updates []*SourceArtifact
	defer s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, value any) {
		if id == target {
			updates = append(updates, value.(*SourceArtifact))
		}
	}, false)()
	_, _, next := s.GetEmission(owner.Named())
	defer next.Finish()
	next.Complete = true
	s.RecordEmitted(owner.Named(), &next, map[manifest.NamedResource]*SourceArtifact{target: second})
	if !slices.Equal(updates, []*SourceArtifact{second}) {
		t.Fatal("payload-only replacement did not notify the whole descriptor exactly once")
	}
	obj, doc, input := s.GetEmission(target)
	defer input.Finish()
	assert.Equal(t, obj, child.Obj)
	assert.Diff(t, doc, child.Doc)
	assert.Equal(t, s.GetArtifact(target), Artifact(second))
	equal := *second
	equal.FS = fstest.MapFS{"file": {Data: []byte("second")}}
	s.RecordEmitted(owner.Named(), &next, map[manifest.NamedResource]*SourceArtifact{target: &equal})
	if !slices.Equal(updates, []*SourceArtifact{second}) {
		t.Fatal("semantically equal payload with distinct backing notified readers")
	}
	s.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
	if s.GetArtifact(target) != nil || !slices.Equal(updates, []*SourceArtifact{second, nil}) {
		t.Fatal("last payload withdrawal did not remove authority and notify readers")
	}
}

func TestRecordEmitted_PayloadWarnings(t *testing.T) {
	t.Parallel()
	for _, equal := range []bool{false, true} {
		t.Run(fmt.Sprintf("equal=%t", equal), func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z, zz := ownerID("a"), ownerID("z"), ownerID("zz")
			for _, id := range []manifest.NamedResource{a, z, zz} {
				s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			}
			child := emissionChild("generated", "same")
			target := child.Obj.Named()
			s.RecordEmitted(zz, &Emission{Children: []Child{child}, Complete: true}, nil)
			first := &SourceArtifact{Key: "a", Digest: "a", Revision: "a", Metadata: map[string]string{"origin": "a"}, FS: fstest.MapFS{"file": {Data: []byte("a")}}}
			last := &SourceArtifact{Key: "a", Digest: "a", Revision: "a", Metadata: map[string]string{"origin": "a"}, FS: fstest.MapFS{"file": {Data: []byte("a")}}}
			if !equal {
				last.Key, last.Digest, last.Revision = "z", "z", "z"
				last.Metadata, last.FS = map[string]string{"origin": "z"}, fstest.MapFS{"file": {Data: []byte("z")}}
			}
			s.RecordEmitted(a, &Emission{Children: []Child{child}, Complete: true}, map[manifest.NamedResource]*SourceArtifact{target: first})
			s.RecordEmitted(z, &Emission{Children: []Child{child}, Complete: true}, map[manifest.NamedResource]*SourceArtifact{target: last})
			var want []manifest.Warning
			if !equal {
				want = []manifest.Warning{{Resource: target, Category: manifest.WarnConflictingProducers, Count: 1, Message: "conflicting producers; using " + zz.String(), Detail: []string{a.String(), z.String(), zz.String()}}}
			}
			assert.Diff(t, s.Warnings(), want)
			assert.Equal(t, s.EmissionOwner(target), zz)
			assert.Equal(t, s.GetArtifact(target), Artifact(last))
		})
	}
}

func TestGetEmission_CompleteFingerprint(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			t.Parallel()
			s := New()
			owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(owner)
			_, _, next := s.GetEmission(owner.Named())
			next.Complete, next.Fingerprint = true, "same"
			s.RecordEmitted(owner.Named(), &next, nil)
			sh := s.shardFor(owner.Named())
			sh.mu.Lock()
			accepted := *sh.slots[owner.Named()].current.Load()
			accepted.Complete = complete
			sh.slots[owner.Named()].current.Store(&accepted)
			sh.mu.Unlock()
			_, _, attempt := s.GetEmission(owner.Named())
			defer attempt.Finish()
			want := ""
			if complete {
				want = "same"
			}
			output, replayable := attempt.Replay("same")
			assert.Equal(t, replayable, complete)
			assert.Equal(t, output.Fingerprint, want)
			assert.Equal(t, attempt.Complete, false)
		})
	}
}

func TestGetEmission_AuthoritativeDocument(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			t.Parallel()
			s := New()
			raw := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"value": "first"}}
			doc := map[string]any{"metadata": map[string]any{"uid": "authored"}, "spec": raw.Spec}
			added := 0
			defer s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if id == raw.Named() {
					added++
				}
			}, false)()
			s.AddObject(raw, doc)
			if added != 1 {
				t.Fatalf("initial admission events = %d, want 1", added)
			}
			if batch {
				s.AddObjects([]manifest.BaseManifest{raw.Clone()})
			} else {
				s.AddObject(raw.Clone())
			}
			if added != 1 {
				t.Fatalf("equal object-only admission events = %d, want 1", added)
			}
			_, retained, attempt := s.GetEmission(raw.Named())
			attempt.Finish()
			if !specValueEqual(retained, doc) {
				t.Fatal("equal object-only admission discarded document")
			}
			changed := raw.Clone()
			changed.Spec["value"] = "second"
			if batch {
				s.AddObjects([]manifest.BaseManifest{changed})
			} else {
				s.AddObject(changed)
			}
			if added != 2 {
				t.Fatalf("changed object-only admission events = %d, want 2", added)
			}
			_, retained, attempt = s.GetEmission(raw.Named())
			attempt.Finish()
			if retained != nil {
				t.Fatal("changed object-only admission retained stale authority")
			}
			replacement := map[string]any{"metadata": map[string]any{"uid": "replacement"}}
			var captured ReadSet
			captured.Init(s, ownerID("document-reader"), nil)
			captured.GetObject(raw.Named())
			s.AddObject(changed, replacement)
			if captured.Valid() {
				t.Fatal("meaningful document-only replacement kept the captured generator valid")
			}
			captured.Retire()
			if added != 3 {
				t.Fatalf("replacement document admission events = %d, want 3", added)
			}
			_, retained, attempt = s.GetEmission(raw.Named())
			attempt.Finish()
			if !specValueEqual(retained, replacement) {
				t.Fatal("explicit document did not replace equal object's authority")
			}
			s.AddObject(changed, replacement)
			if added != 3 {
				t.Fatalf("equal document admission events = %d, want 3", added)
			}
			_, _, completed := s.GetEmission(raw.Named())
			defer completed.Finish()
			completed.Complete = true
			s.RecordEmitted(raw.Named(), &completed, nil)
			if s.GetArtifact(raw.Named()) == nil {
				t.Fatal("completed document input is unavailable")
			}
			s.AddObject(changed, doc)
			if s.GetArtifact(raw.Named()) != nil {
				t.Fatal("changed document retained completed work for the same object pointer")
			}
		})
	}
}

func TestGetEmission_SelectedBinding(t *testing.T) {
	t.Parallel()
	s := New()
	parent := ownerID("parent")
	s.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
	obj := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}
	child := Child{Obj: obj, Doc: map[string]any{"apiVersion": obj.APIVersion, "kind": obj.Kind}}
	first := sourceBinding{source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "first"}, key: "first"}
	second := sourceBinding{source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "second"}, key: "second"}
	s.RecordEmitted(parent, &Emission{Children: []Child{child}, Complete: true, binding: first}, nil)
	_, _, attempt := s.GetEmission(obj.Named())
	defer attempt.Finish()
	attempt.Fingerprint, attempt.Complete = "success", true
	s.RecordEmitted(obj.Named(), &attempt, nil)
	accepted := s.GetArtifact(obj.Named())
	if accepted == nil {
		t.Fatal("fresh inventory unavailable")
	}
	s.RecordEmitted(parent, &Emission{Children: []Child{child}, Complete: true, binding: first, Fingerprint: "changed"}, nil)
	_, doc, equivalent := s.GetEmission(obj.Named())
	defer equivalent.Finish()
	if equivalent.inputHead.binding != first || !specValueEqual(doc, child.Doc) || func() bool { output, ok := equivalent.Replay("success"); return !ok || output.Fingerprint != "success" }() || s.GetArtifact(obj.Named()) != accepted {
		t.Fatal("equivalent declaration storage invalidated accepted work")
	}
	s.RecordEmitted(parent, &Emission{Children: []Child{child}, Complete: true, binding: second}, nil)
	_, doc, rebound := s.GetEmission(obj.Named())
	defer rebound.Finish()
	if rebound.inputHead.binding != second || !specValueEqual(doc, child.Doc) || rebound.Fingerprint != "" || s.GetArtifact(obj.Named()) != nil {
		t.Fatal("selected document and binding did not invalidate old replay together")
	}
}

func TestRecordEmitted_BorrowedPayloadOwners(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"complete", "close", "rejected/output", "rejected/a", "rejected/b", "rejected/c"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			s.AddObject(owner)
			var output, first, second, third ReadSet
			output.Init(s, owner.Named(), nil)
			defer output.Retire()
			input := emissionChild("input-output", "output")
			s.AddObject(input.Obj)
			output.GetObject(input.Obj.Named())
			initial := &Emission{Complete: true}
			output.Bind(initial)
			s.RecordEmitted(owner.Named(), initial, nil)
			first.Init(s, owner.Named(), nil)
			second.Init(s, owner.Named(), nil)
			third.Init(s, owner.Named(), nil)
			defer first.Retire()
			defer second.Retire()
			defer third.Close()
			registrations := []*registrationHead{output.input(input.Obj.Named()).object.head}
			publish := func(reads *ReadSet, name string) {
				t.Helper()
				input := emissionChild("input-"+name, name)
				s.AddObject(input.Obj)
				reads.GetObject(input.Obj.Named())
				registrations = append(registrations, reads.input(input.Obj.Named()).object.head)
				_, _, next := reads.GetEmission(owner.Named())
				defer next.Finish()
				child := emissionChild(name, name)
				next.Children, next.Manifests = []Child{child}, []map[string]any{child.Doc}
				reads.Bind(&next)
				s.RecordEmitted(owner.Named(), &next, map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: name}})
			}
			publish(&first, "a")
			publish(&second, "b")
			publish(&third, "c")
			accepted := s.GetArtifact(owner.Named()).(*Emission)
			if len(accepted.Children) != 3 || !accepted.Retains(&first) || !accepted.Retains(&second) || !accepted.Retains(&third) {
				t.Fatal("partial publication lost a borrowed payload owner")
			}
			if first.retired.Load() || second.retired.Load() {
				t.Fatal("partial publication retired a descriptor still borrowed by the inventory")
			}
			for _, head := range registrations {
				if head.first.Load() == nil {
					t.Fatal("partial publication released a borrowed payload owner's registration")
				}
			}
			if mode == "close" {
				third.Close()
				for _, reads := range []*ReadSet{&output, &first, &second, &third} {
					if !reads.retired.Load() {
						t.Fatal("closing retained a payload owner's registrations")
					}
				}
				assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
				for _, head := range registrations {
					if head.first.Load() != nil {
						t.Fatal("sweep retained a closed payload owner's registration")
					}
				}
				return
			}
			if mode != "complete" {
				var rejected ReadSet
				rejected.Init(s, owner.Named(), nil)
				defer rejected.Retire()
				input := emissionChild("successor-only", "old")
				s.AddObject(input.Obj)
				rejected.GetObject(input.Obj.Named())
				rejectedHead := rejected.input(input.Obj.Named()).object.head
				for _, head := range registrations {
					if head == rejectedHead {
						t.Fatal("successor-only input shares an accepted registration head")
					}
				}
				release := make(chan struct{})
				barrier := &admissionBarrier{
					ConfigMap: &manifest.ConfigMap{Name: "replacement", Namespace: "apps"},
					prepared:  make(chan struct{}),
					release:   release,
				}
				next := &Emission{Children: []Child{{Obj: barrier}}, Fingerprint: "stale", Complete: true}
				rejected.Bind(next)
				resume := sync.OnceFunc(func() { close(release) })
				done := make(chan struct{})
				go func() {
					defer close(done)
					s.RecordEmitted(owner.Named(), next, nil)
				}()
				t.Cleanup(func() { resume(); <-done })
				select {
				case <-barrier.prepared:
				case <-done:
					t.Fatal("publication did not reach the admission barrier")
				}
				if !rejected.Valid() {
					t.Fatal("reads invalid before concurrent input replacement")
				}
				s.AddObject(emissionChild("successor-only", "new").Obj)
				if rejected.Valid() {
					t.Fatal("input replacement did not invalidate the pending publication")
				}
				resume()
				<-done
				if s.GetArtifact(owner.Named()) != accepted || s.GetObject(barrier.ConfigMap.Named()) != nil {
					t.Fatal("stale proposal replaced accepted payload inventory")
				}
				if accepted.OutputReads != &output || accepted.payloadReads != &first || len(accepted.borrowed) != 2 {
					t.Fatal("rejected proposal changed accepted output and payload provenance")
				}
				for _, reads := range []*ReadSet{&output, &first, &second, &third} {
					if reads.retired.Load() || !accepted.Retains(reads) {
						t.Error("rejected proposal retired accepted output or borrowed payload reads")
					}
				}
				rejected.Retire()
				assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
				if !rejected.retired.Load() || rejectedHead.first.Load() != nil {
					t.Error("sweep retained rejected attempt registrations")
				}
				for _, head := range registrations {
					if head.first.Load() == nil {
						t.Error("sweep removed an accepted output or borrowed payload registration")
					}
				}
				if s.GetArtifact(owner.Named()) != accepted {
					t.Fatal("sweep removed accepted payload inventory")
				}
				name := mode[len("rejected/"):]
				s.AddObject(emissionChild("input-"+name, "new").Obj)
				if !slices.Contains(slices.Collect(s.Sweep()), owner.Named()) {
					t.Fatal("accepted input change did not notify its owner after successor rejection")
				}
				return
			}
			last := emissionChild("c", "c")
			_, _, next := third.GetEmission(owner.Named())
			defer next.Finish()
			next.Children, next.Manifests, next.Complete = []Child{last}, []map[string]any{last.Doc}, true
			third.Bind(&next)
			s.RecordEmitted(owner.Named(), &next, nil)
			if !first.retired.Load() || !second.retired.Load() || third.retired.Load() {
				t.Fatal("completion did not release exactly the superseded payload owners")
			}
			if s.GetArtifact(last.Obj.Named()) == nil {
				t.Fatal("completion removed the current execution's accepted descriptor")
			}
		})
	}
}

func TestGetDeclaration_IndependenceLifecycle(t *testing.T) {
	t.Parallel()
	for _, support := range []string{"uncovered", "independent", "covered"} {
		for _, route := range []string{"pre_index", "post_index", "document", "batch", "included", "preflight", "readset", "readmit", "anchor", "batch_anchor"} {
			t.Run(support+"/"+route, func(t *testing.T) {
				t.Parallel()
				st := New()
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				raw := &manifest.OCIRepository{Name: "input", Namespace: "apps", URL: "oci://raw.example"}
				st.AddObject(owner)
				proofs := []manifest.Production{{Owner: owner.Named()}}
				independent := support == "independent"
				if independent {
					proofs = append(proofs, manifest.Production{})
				}
				if support == "uncovered" {
					proofs, independent = nil, true
				}
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: proofs}}}
				if route == "anchor" || route == "batch_anchor" {
					index.Records[ownerID("descendant")] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: raw.Named()}}}
				}
				if route == "pre_index" {
					st.AddObject(raw)
					if st.GetDeclaration(raw.Named()) != raw {
						t.Fatal("pre-index declaration lost independent support")
					}
				}
				st.SetDiscoveryIndex(index)
				switch route {
				case "post_index":
					st.AddObject(raw)
				case "document":
					st.AddObject(raw, map[string]any{"apiVersion": manifest.SourceDomain + "/v1"})
				case "batch":
					st.AddObjects([]manifest.BaseManifest{raw})
				case "included", "preflight":
					if route == "included" {
						st.SetIncluded(raw.Named(), false)
					} else {
						st.SetPreflight(raw.Named(), "preflight")
					}
					st.AddObject(raw)
				case "readset":
					st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Key: "source"})
					var reads ReadSet
					reads.Init(st, owner.Named(), nil)
					defer reads.Retire()
					reads.Admit(raw, manifest.BootstrapSourceID, manifest.NamedResource{})
				case "readmit":
					st.AddObject(raw)
					if !st.DeleteObject(raw.Named()) {
						t.Fatal("fixture did not delete its first admission")
					}
					st.AddObject(raw)
				case "anchor", "batch_anchor":
					if route == "anchor" {
						st.AddObject(raw)
					} else {
						st.AddObjects([]manifest.BaseManifest{raw})
					}
				}
				var want manifest.BaseManifest
				if independent || route == "anchor" || route == "batch_anchor" {
					want = raw
				}
				assert.Equal(t, st.GetObject(raw.Named()), manifest.BaseManifest(raw))
				assert.Equal(t, st.GetDeclaration(raw.Named()), want)
				got, _, emission := st.GetEmission(raw.Named())
				defer emission.Finish()
				assert.Equal(t, got, want)
				var reads ReadSet
				reads.Init(st, ownerID("reader"), nil)
				defer reads.Retire()
				assert.Equal(t, reads.GetDeclaration(raw.Named()), want)
				assert.Equal(t, reads.input(raw.Named()).view.independent, independent)
				if route == "included" {
					assert.Equal(t, reads.input(raw.Named()).view.excluded, true)
				}
				if route == "preflight" {
					assert.Equal(t, reads.input(raw.Named()).view.preflight, "preflight")
				}
				if route == "document" {
					added := 0
					defer st.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
						if id == raw.Named() {
							added++
						}
					}, false)()
					doc := map[string]any{"apiVersion": manifest.SourceDomain + "/v1beta2"}
					st.AddObject(raw, doc)
					if reads.Valid() {
						t.Fatal("document replacement retained the captured generation")
					}
					assert.Equal(t, reads.GetDeclaration(raw.Named()), want)
					assert.Equal(t, reads.input(raw.Named()).view.independent, independent)
					assert.Diff(t, reads.input(raw.Named()).view.child.Doc, map[string]any{"apiVersion": manifest.SourceDomain + "/v1"})
					var current ReadSet
					current.Init(st, ownerID("current"), nil)
					defer current.Retire()
					got, document, replacement := current.GetEmission(raw.Named())
					defer replacement.Finish()
					assert.Equal(t, got, want)
					assert.Equal(t, st.GetDeclaration(raw.Named()), want)
					assert.Diff(t, document, doc)
					assert.Equal(t, current.input(raw.Named()).view.independent, independent)
					if replacement.inputHead.generation <= reads.input(raw.Named()).view.generation {
						t.Fatal("document replacement did not advance the generation")
					}
					st.AddObject(raw, manifest.DeepCopyMap(doc))
					if !current.Valid() || added != 1 {
						t.Fatalf("equal document admission: valid=%t events=%d, want true and 1", current.Valid(), added)
					}
					assert.Equal(t, st.GetDeclaration(raw.Named()), want)
					assert.Equal(t, current.GetDeclaration(raw.Named()), want)
					assert.Equal(t, current.input(raw.Named()).view.independent, independent)
				}
			})
		}
	}
}

func TestRecordEmitted_IndependencePublication(t *testing.T) {
	t.Parallel()
	for _, independent := range []bool{false, true} {
		for _, route := range []string{"first", "stable", "replacement", "hidden_producer", "reachability", "target", "expanded_coverage", "expanded_inventory", "metadata"} {
			t.Run(fmt.Sprintf("independent_%t/%s", independent, route), func(t *testing.T) {
				t.Parallel()
				st := New()
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				child := emissionChild("output", "first")
				proofs := []manifest.Production{{Owner: owner.Named()}}
				if route == "expanded_coverage" {
					proofs[0].Owner = ownerID("nested")
				}
				if independent {
					proofs = append(proofs, manifest.Production{})
				}
				id := child.Obj.Named()
				if route == "hidden_producer" {
					id = owner.Named()
					proofs[0].Owner = ownerID("covering")
				} else {
					st.AddObject(owner)
				}
				records := map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: proofs}}
				if route == "reachability" || route == "expanded_coverage" {
					records[ownerID("descendant")] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: id}}}
				}
				if route == "target" {
					child = Child{Obj: &manifest.RawObject{APIVersion: "objectbucket.io/v1alpha1", Kind: "ObjectBucketClaim", Name: id.Name, Namespace: id.Namespace}}
				}
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
				sh := st.shardFor(id)
				sh.mu.RLock()
				_, present := sh.cells[id]
				sh.mu.RUnlock()
				if present {
					t.Fatal("index manufactured an absent object cell")
				}
				if route == "metadata" {
					st.SetIncluded(id, false)
					st.SetPreflight(id, "preflight")
				}
				var absent ReadSet
				absent.Init(st, ownerID("absent"), nil)
				defer absent.Retire()
				assert.Equal(t, absent.GetDeclaration(id), nil)
				_, _, output := st.GetEmission(owner.Named())
				defer output.Finish()
				output.Complete = true
				if route != "reachability" {
					output.Children = []Child{child}
				}
				nested := &manifest.Kustomization{Name: "nested", Namespace: "apps"}
				if route == "expanded_coverage" || route == "expanded_inventory" {
					output.Children = []Child{{Obj: nested}}
				}
				st.RecordEmitted(owner.Named(), &output, nil)
				if route == "expanded_inventory" {
					st.RecordEmitted(nested.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
					st.RecordEmitted(owner.Named(), &Emission{Complete: true}, nil)
				}
				if !slices.Contains(slices.Collect(st.Sweep()), absent.ID()) || absent.Valid() {
					t.Fatal("first publication missed the registered absence")
				}
				if route == "stable" || route == "replacement" {
					child = emissionChild("output", "second")
					children := []Child{child}
					if route == "replacement" {
						children = append(children, emissionChild("extra", "value"))
					}
					st.RecordEmitted(owner.Named(), &Emission{Children: children, Complete: true}, nil)
				}
				sh.mu.RLock()
				cell := sh.cells[id]
				fact := cell.independent
				sh.mu.RUnlock()
				if route == "metadata" {
					assert.Equal(t, cell.excluded, true)
					assert.Equal(t, cell.preflight, "preflight")
				}
				assert.Equal(t, fact, independent)
				if route == "reachability" || route == "target" || route == "expanded_coverage" || route == "expanded_inventory" {
					assert.Equal(t, st.GetDeclaration(id), nil)
					assert.Equal(t, st.GetObject(id), nil)
				} else if route != "hidden_producer" {
					assert.Equal(t, st.GetDeclaration(id), child.Obj)
					var reads ReadSet
					reads.Init(st, owner.Named(), nil)
					defer reads.Retire()
					_, _, next := reads.GetEmission(owner.Named())
					defer next.Finish()
					reads.Bind(&next)
					var want manifest.BaseManifest
					if independent {
						want = child.Obj
					}
					assert.Equal(t, reads.GetDeclaration(id), want)
				}
			})
		}
	}
}
