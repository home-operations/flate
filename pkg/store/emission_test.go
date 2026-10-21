package store

import (
	"context"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
	s.RecordEmitted(owner, next)
}
func ownerID(name string) manifest.NamedResource {
	return testutil.KustomizationID("apps", name)
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
	s.RecordEmitted(q, &Emission{Children: []Child{b, bad}, Complete: true})
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
	_, attempt := s.GetEmission(p)
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
	_, progress := s.GetEmission(p)
	progress.Children = nil
	accept(t, s, p, &progress)
	_, retained := s.GetEmission(p)
	if len(retained.Manifests) != 2 {
		t.Fatalf("inherited partial documents duplicated: %d", len(retained.Manifests))
	}
	progress.Finish()
	for _, tc := range []struct {
		name string
		edit func(*Emission)
	}{
		{"render", func(e *Emission) { e.Render = []byte("new") }},
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
			_, initial := s.GetEmission(p)
			initial.Children, initial.Manifests = []Child{a}, []map[string]any{a.Doc}
			initial.Unreadable, initial.Render, initial.Fingerprint = []manifest.NamedResource{a.Obj.Named()}, []byte("old"), "same"
			initial.Complete = tc.name != "complete"
			s.RecordEmitted(p, &initial)
			predecessor := s.GetArtifact(p).(*Emission)
			_, attempt := s.GetEmission(p)
			defer attempt.Finish()
			attempt.Complete = initial.Complete
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
			s.RecordEmitted(p, &attempt)
			accepted := s.GetArtifact(p).(*Emission)
			if events != 1 || accepted == predecessor || accepted.Fingerprint != "same" {
				t.Fatal("equal fingerprint discarded changed emission")
			}
			if string(accepted.Render) != string(attempt.Render) || !slices.Equal(accepted.Unreadable, attempt.Unreadable) || accepted.Complete != attempt.Complete || accepted.binding != attempt.binding {
				t.Fatal("changed emission lost output or completion context")
			}
			if !slices.EqualFunc(accepted.Children, attempt.Children, func(a, b Child) bool { return a.Obj == b.Obj }) || !slices.EqualFunc(accepted.Manifests, attempt.Manifests, func(a, b map[string]any) bool { return specValueEqual(a, b) }) {
				t.Fatal("changed emission lost captured children or documents")
			}
			checkPredecessor()
			_, replay := s.GetEmission(p)
			defer replay.Finish()
			replay.binding = accepted.binding
			s.RecordEmitted(p, &replay)
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
					_, next := s.GetEmission(p)
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
			_, attempt := s.GetEmission(p)
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
			close(start)
			wg.Wait()
			for _, child := range accepted.Children {
				id := child.Obj.Named()
				if got := s.GetObject(id); got == nil || got.Named() != id {
					t.Fatal("reverse position lost object")
				}
				if obj, _ := s.Snapshot(id); obj == nil || obj.Named() != id {
					t.Fatal("reverse position changed snapshot identity")
				}
				if got := s.Productions(id, nil); len(got) != 2 || got[0].Owner != p || got[1].Owner != q {
					t.Fatalf("reverse position changed production: %v", got)
				}
			}
			s.SetArtifact(covered.Obj.Named(), &SourceArtifact{Key: "covered"})
			s.UpdateStatus(covered.Obj.Named(), StatusReady, "", "")
			accept(t, s, p, &Emission{Complete: true})
			for _, child := range accepted.Children {
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
					_, initial := s.GetEmission(producer.Named())
					initial.Children, initial.Manifests, initial.Complete = []Child{child}, []map[string]any{child.Doc}, true
					s.RecordEmitted(producer.Named(), &initial)
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
							_, next := s.GetEmission(producer.Named())
							defer next.Finish()
							next.Complete = true
							if labels {
								doc := manifest.DeepCopyMap(child.Doc)
								doc["metadata"].(map[string]any)["labels"] = map[string]any{"kustomize.toolkit.fluxcd.io/name": fmt.Sprint(i)}
								next.Children[0].Doc, next.Manifests[0] = doc, doc
							}
							ready <- struct{}{}
							<-start
							s.RecordEmitted(producer.Named(), &next)
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
					p, k, l, q := ownerID("p"), ownerID("k"), ownerID("l"), ownerID("q")
					s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
					child := Child{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}
					cycle := Child{Obj: &manifest.Kustomization{Name: l.Name, Namespace: l.Namespace}}
					data := emissionChild("shared", "value")
					late := emissionChild("late", "value")
					accept(t, s, p, &Emission{Children: []Child{child}, Complete: true})
					accept(t, s, k, &Emission{Children: []Child{cycle, data}, Manifests: []map[string]any{data.Doc}, Complete: true})
					accept(t, s, l, &Emission{Children: []Child{child}, Complete: true})
					if shared {
						s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
						accept(t, s, q, &Emission{Children: []Child{data}, Manifests: []map[string]any{data.Doc}, Complete: true})
					}
					for _, id := range []manifest.NamedResource{k, l, data.Obj.Named()} {
						s.UpdateStatus(id, StatusReady, "", "")
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
						_, attempt := s.GetEmission(k)
						attempt.Children = []Child{cycle, data, late}
						attempt.Manifests = []map[string]any{data.Doc, late.Doc}
						attempt.Complete = true
						close(captured)
						<-lateRelease
						accept(t, s, k, &attempt)
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
					} else if s.GetObject(data.Obj.Named()) == nil {
						t.Fatal("independent Q support lost")
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
						if obj, conditions := s.Snapshot(id); obj != nil || len(conditions) != 0 {
							t.Fatal("hidden snapshot leaked")
						}
						if obj, attempt := s.GetEmission(id); obj != nil {
							attempt.Finish()
							t.Fatal("hidden attempt leaked")
						}
						if len(s.Productions(id, nil)) != 0 {
							t.Fatal("hidden productions leaked")
						}
						if _, ok := s.FailedResources()[id]; ok {
							t.Fatal("hidden failure leaked")
						}
						s.UpdateStatus(id, StatusReady, "", "")
						if _, ok := s.GetStatus(id); ok {
							t.Fatal("hidden late Ready leaked")
						}
						if slices.ContainsFunc(s.ListObjects("", ""), func(obj manifest.BaseManifest) bool { return obj.Named() == id }) {
							t.Fatal("hidden inventory leaked")
						}
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
					s.AddObject(&manifest.Kustomization{Name: k.Name, Namespace: k.Namespace})
					if s.GetObject(k) == nil || s.GetObject(l) == nil || s.GetObject(late.Obj.Named()) == nil {
						t.Fatal("external anchor did not restore supported descendants")
					}
					for _, id := range hidden {
						if s.GetArtifact(id) != nil {
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
		_, inventory := s.GetEmission(contribution.owner)
		if diff := cmp.Diff(inventory.Children, []Child{contribution.child}, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
			t.Fatalf("emission children changed (-want +got): %s", diff)
		}
		inventory.Finish()
	}
	changed := childWithLabels("q")
	changed.Doc["metadata"].(map[string]any)["labels"].(map[string]any)["semantic"] = "different"
	s.RecordEmitted(q, &Emission{Children: []Child{changed}, Complete: true})
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
	_, attempt := s.GetEmission(p)
	attempt.Render = []byte("receipt")
	attempt.Unreadable = []manifest.NamedResource{fresh.Obj.Named()}
	var notifications int
	unsub := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == p {
			notifications++
		}
	}, false)
	accept(t, s, p, &attempt)
	unsub()
	_, accepted := s.GetEmission(p)
	if notifications != 1 || string(accepted.Render) != "receipt" || !slices.Equal(accepted.Unreadable, attempt.Unreadable) {
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
			s.RecordEmitted(p, &Emission{Children: []Child{a}, Complete: true})
			id := a.Obj.Named()
			_, attempt := s.GetEmission(id)
			attempt.Complete = true
			s.RecordEmitted(id, &attempt)
			accepted := s.GetArtifact(id).(*Emission)
			s.RecordEmitted(q, &Emission{Children: []Child{b}, Complete: true})
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
			s.RecordEmitted(q, &Emission{Children: []Child{{Obj: unequal, Doc: doc}}, Complete: true})
			if s.GetArtifact(id) != nil {
				t.Fatal("unequal representative reused a validated artifact")
			}
			checkReplay(false)
			if validated := accepted.validated.Load(); validated == nil || *validated != b.Obj {
				t.Fatal("unequal input entered the validity memo")
			}
			replacement := *accepted
			replacement.input, replacement.inputDoc = unequal, doc
			s.RecordEmitted(id, &replacement)
			current := s.GetArtifact(id).(*Emission)
			if current == accepted || current.validated == nil || current.validated == accepted.validated || current.validated.Load() != nil {
				t.Fatal("new emission retained its predecessor's memo")
			}
			s.RecordEmitted(q, &Emission{Children: []Child{b}, Complete: true})
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
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				l.Obj.Named(): {Productions: []manifest.Production{{Owner: k.Obj.Named(), Source: source, Authored: l.Obj.Named(), Path: "source/l.yaml", File: "l.yaml"}}},
			}})
			publish := func(owner Child, children ...Child) {
				t.Helper()
				obj, attempt := s.GetEmission(owner.Obj.Named())
				defer attempt.Finish()
				if obj == nil {
					t.Fatalf("%v unavailable before publication", owner.Obj.Named())
				}
				attempt.Children, attempt.Manifests, attempt.Complete = children, nil, true
				for _, child := range children {
					attempt.Manifests = append(attempt.Manifests, child.Doc)
				}
				s.RecordEmitted(owner.Obj.Named(), &attempt)
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
			publish(p, k)
			publish(l, m)
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
			_, attempt := s.GetEmission(p)
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
			s.RecordEmitted(p, &attempt)
			unsub()
			assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
			if independent {
				if s.GetObject(seed.Obj.Named()) != actual.Obj {
					t.Fatal("accepted output did not replace independent static content")
				}
				_, empty := s.GetEmission(p)
				empty.Children, empty.Manifests = nil, nil
				empty.Complete = true
				accept(t, s, p, &empty)
				empty.Finish()
				assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), manifest.NamedResource{})
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
			_, empty := s.GetEmission(p)
			empty.Children = nil
			empty.Manifests = nil
			empty.Complete = true
			accept(t, s, p, &empty)
			if s.GetObject(seed.Obj.Named()) != nil || len(s.Productions(seed.Obj.Named(), nil)) != 0 {
				t.Fatal("complete empty resurrected covered prediction")
			}
			s.AddObject(seed.Obj)
			if s.GetObject(seed.Obj.Named()) == nil {
				t.Fatal("explicit external anchor could not restore covered identity")
			}
		})
	}
	for _, mode := range []string{"rooted_representative", "deleted_static"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			s := New()
			p := ownerID("p")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
			seed, child := emissionChild("seed", "static"), emissionChild("seed", "emitted")
			s.AddObject(seed.Obj)
			s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true})
			assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
			if mode == "rooted_representative" {
				s.AddObject(child.Obj)
				q := ownerID("q")
				s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
				other := emissionChild("seed", "other")
				s.RecordEmitted(q, &Emission{Children: []Child{other}, Complete: true})
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
				return
			}
			if !s.DeleteObject(seed.Obj.Named()) || s.GetObject(seed.Obj.Named()) != child.Obj {
				t.Fatal("static deletion lost live emitted support")
			}
			assert.Equal(t, s.EmissionOwner(seed.Obj.Named()), p)
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
		_, attempt := s.GetEmission(p)
		attempt.Complete = true
		s.RecordEmitted(p, &attempt)
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
		s.RecordEmitted(p, next)
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			seed.Obj.Named(): {Productions: []manifest.Production{{Source: manifest.BootstrapSourceID}}},
		}})
		s.RecordEmitted(p, next)
		s.RecordEmitted(p, &Emission{Complete: true})
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
				s.RecordEmitted(owner, next)
			}
			check := func(want bool) {
				t.Helper()
				for _, id := range []manifest.NamedResource{k, l, tail.Obj.Named()} {
					sh := s.shardFor(id)
					if (s.GetObject(id) != nil) != want || sh.hidden[id] == want {
						t.Errorf("%v support/visibility disagree, want live=%t", id, want)
					}
					if sh.static[id] != nil && sh.retired[id] == want {
						t.Errorf("%v retirement disagrees, want live=%t", id, want)
					}
				}
			}
			publish(p, true)
			if mode == "other-source-current" {
				_, attempt := s.GetEmission(k)
				attempt.binding.source.Name = "other-source"
				attempt.Complete = true
				s.RecordEmitted(k, &attempt)
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
			s.RecordEmitted(p, &Emission{Children: []Child{{Obj: conflict}}, Complete: true})
			if s.GetObject(k) != replacement {
				t.Fatal("direct input lost its independent authority")
			}
			s.RecordEmitted(p, &Emission{Complete: true})

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
	t.Run("equal_static", func(t *testing.T) {
		t.Parallel()

		s := New()
		p, k := ownerID("p"), ownerID("k")
		s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
		seed := &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}
		s.AddObject(seed)
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{k: {Productions: []manifest.Production{{Owner: p}}}}})
		contribution := seed.Clone()
		s.RecordEmitted(p, &Emission{Children: []Child{{Obj: contribution}}, Complete: true})
		s.AddObject(seed)
		s.RecordEmitted(p, &Emission{Complete: true})
		if s.GetObject(k) != seed {
			t.Fatal("equal direct write did not retain independent static authority")
		}
		var events int
		defer s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
			if id == k {
				events++
			}
		}, false)()
		s.RecordEmitted(p, &Emission{Children: []Child{{Obj: contribution}}, Complete: true})
		s.AddObject(seed)
		if s.GetObject(k) != seed || events != 0 {
			t.Fatal("equal static write changed the rooted representative")
		}
	})
}

func TestStore_ProductionsScopedFileBridge(t *testing.T) {
	t.Parallel()

	for _, differentSource := range []bool{false, true} {
		for _, explicitNamespace := range []bool{false, true} {
			t.Run(fmt.Sprintf("differentSource=%t/explicitNamespace=%t", differentSource, explicitNamespace), func(t *testing.T) {
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
				result := s.Productions(target, index)
				want := !differentSource && !explicitNamespace
				if got := slices.ContainsFunc(result, func(p manifest.Production) bool { return p.Owner == fileOwner && p.Path != "" }); got != want {
					t.Fatalf("scoped bridge=%t want %t: %v", got, want, result)
				}
				if _, ok := index.Lookup(manifest.NamedResource{Kind: target.Kind, Namespace: "unrelated", Name: target.Name}); ok {
					t.Fatal("exact index lookup gained wildcard semantics")
				}
			})
		}
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
	s.UpdateStatus(child.Obj.Named(), StatusReady, "", "")
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
		if obj, conditions := s.Snapshot(id); obj != nil || len(conditions) != 0 {
			t.Fatal("absent producer leaked snapshot")
		}
		if obj, attempt := s.GetEmission(id); obj != nil || len(attempt.Children) != 0 || len(attempt.Manifests) != 0 {
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
	if !s.shardFor(child.Obj.Named()).retired[child.Obj.Named()] {
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
		want := []manifest.Production{proof, {Owner: q}}
		accept(t, s, q, &Emission{Complete: true})
		if got := snapshot; !slices.Equal(got, want) {
			t.Fatalf("frozen/runtime snapshot changed: got %v, want %v", got, want)
		}
		if !slices.Equal(backing, original) {
			t.Fatalf("query mutated borrowed discovery evidence: got %v, want %v", backing, original)
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
			_, attempt := s.GetEmission(owners[filtered])
			attempt.Complete = true
			accept(t, s, owners[filtered], &attempt)
			attempt.Finish()
			snapshot := s.Productions(child.Obj.Named(), nil)
			if !slices.Equal(proofs, original) {
				t.Fatalf("filter mutated borrowed discovery evidence: got %v, want %v", proofs, original)
			}
			want := slices.Delete(slices.Clone(original), filtered, filtered+1)
			for _, id := range owners {
				_, attempt := s.GetEmission(id)
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
			accept(t, s, p, &Emission{Complete: true})
			if got := s.Productions(child.Obj.Named(), nil); len(got) != 0 || s.GetObject(child.Obj.Named()) != nil {
				t.Fatalf("withdrawal retained accepted evidence: %v", got)
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
					assert.Equal(t, s.GetObject(id), child.Obj)
					obj, _ := s.Snapshot(id)
					assert.Equal(t, obj, child.Obj)
					replayed := false
					s.AddListener(EventObjectAdded, func(got manifest.NamedResource, data any) {
						if got == id {
							replayed = true
							assert.Equal(t, data.(manifest.BaseManifest), child.Obj)
						}
					}, true)()
					assert.Equal(t, replayed, true)
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
					s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: &synthetic}}})
					s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: &synthetic}, emissionChild(owner.Name, "complete")}, Complete: true})
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
		s.UpdateStatus(id, StatusReady, "", "")
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
		s.RecordEmitted(p, &Emission{Children: []Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}}, Complete: true})
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
		child, owner := sh.representativeLocked(id)
		if !sh.hidden[id] || !sh.retired[id] || child.Obj != nil || owner != (manifest.NamedResource{}) {
			t.Errorf("%v hidden, retirement and representative disagree", id)
		}
		if slot := sh.slots[id]; slot != nil && (slot.visible.Load() || !slot.hidden.Load()) {
			t.Errorf("%v slot remains live", id)
		}
	}
	t.Run("simple", func(t *testing.T) {
		t.Parallel()

		s, p, k, l := fixture(t, "k", "l", false)
		s.RecordEmitted(p, &Emission{Complete: true})
		checkHidden(t, s, k)
		checkHidden(t, s, l.Obj.Named())
		for _, batch := range []bool{false, true} {
			t.Run(fmt.Sprintf("reanchor/batch=%t", batch), func(t *testing.T) {
				t.Parallel()

				s, p, k, l := fixture(t, "k", "l", false)
				s.RecordEmitted(p, &Emission{Complete: true})
				checkHidden(t, s, k)
				checkHidden(t, s, l.Obj.Named())
				s.SetArtifact(l.Obj.Named(), &SourceArtifact{Key: "private"})
				s.UpdateStatus(l.Obj.Named(), StatusReady, "", "")
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
				if sh.retired[l.Obj.Named()] || sh.hidden[l.Obj.Named()] {
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
					_, attempt := s.GetEmission(k)
					defer attempt.Finish()
					attempt.Children, attempt.Complete = []Child{late}, true
					close(captured)
					tasks.YieldSlot(func() { <-withdrawn })
					checkHidden(t, s, l.Obj.Named())
					s.RecordEmitted(k, &attempt)
					s.SetArtifact(l.Obj.Named(), &SourceArtifact{Key: "late-private"})
					s.UpdateStatus(l.Obj.Named(), StatusReady, "", "")
				})
				tasks.Go(t.Context(), "withdrawal", func(context.Context) {
					tasks.YieldSlot(func() { <-captured; <-observing })
					s.RecordEmitted(p, &Emission{Complete: true})
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
				if !s.shardFor(l.Obj.Named()).retired[l.Obj.Named()] {
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
	var arrivals int
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == obj.Named() {
			arrivals++
		}
	}, false)
	defer unsub()
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: doc}}, Manifests: []map[string]any{doc}, Complete: true})
	if arrivals != 0 {
		t.Fatalf("equal seed acquired document with %d arrivals", arrivals)
	}
	_, accepted := s.GetEmission(p)
	if len(accepted.Manifests) != 1 || !specValueEqual(accepted.Manifests[0], doc) {
		t.Fatal("suppressed arrival lost authoritative document")
	}
	if got := s.Productions(obj.Named(), nil); len(got) != 1 || got[0].Owner != p {
		t.Fatalf("accepted support = %v", got)
	}
	next := manifest.DeepCopyMap(doc)
	next["metadata"].(map[string]any)["annotations"] = map[string]any{"semantic": "changed"}
	s.RecordEmitted(p, &Emission{Children: []Child{{Obj: obj, Doc: next}, emissionChild("other", "value")}, Complete: true})
	if arrivals != 1 {
		t.Fatalf("changed authoritative document arrivals = %d, want 1", arrivals)
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
			s.RecordEmitted(owner.Named(), proposal)
			want := mode == "proposal" || mode == "frozen"
			check := func(want bool) {
				t.Helper()
				_, current := s.GetEmission(owner.Named())
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
				if s.GetObject(target) != nil || s.GetArtifact(target) != nil {
					t.Fatal("prediction supplied content")
				}
			}
			check(want)
			_, next := s.GetEmission(owner.Named())
			next.Children = []Child{g}
			next.Complete = true
			next.binding = proposal.binding
			s.RecordEmitted(owner.Named(), &next)
			check(want)
			next.Finish()
			if mode == "frozen" {
				_, replacement := s.GetEmission(owner.Named())
				replacement.Children, replacement.Complete, replacement.binding = []Child{g, consumer("app-web")}, true, proposal.binding
				s.RecordEmitted(owner.Named(), &replacement)
				check(false)
				replacement.Finish()
				for range 2 {
					_, withoutConsumer := s.GetEmission(owner.Named())
					withoutConsumer.Children, withoutConsumer.Complete, withoutConsumer.binding = []Child{g}, true, proposal.binding
					s.RecordEmitted(owner.Named(), &withoutConsumer)
					check(false)
					withoutConsumer.Finish()
				}
			}
			_, next = s.GetEmission(owner.Named())
			next.Children, next.Complete, next.binding = []Child{generator("different-{app}")}, true, proposal.binding
			s.RecordEmitted(owner.Named(), &next)
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
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(target.Name)}, Complete: true})
		replacement := target
		replacement.Name = "app-web"
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(replacement.Name)}, Complete: true})
		if len(s.Productions(replacement, nil)) != 1 {
			t.Fatal("reference replacement lost new prediction")
		}
		if len(s.Productions(target, nil)) != 0 {
			t.Fatal("reference replacement retained old prediction")
		}
		_, current := s.GetEmission(owner.Named())
		if current.Children[0].Obj != g.Obj {
			t.Fatal("unchanged declaration replaced")
		}
		current.Finish()
		s.RecordEmitted(owner.Named(), &Emission{})
		if len(s.Productions(replacement, nil)) != 1 {
			t.Fatal("incomplete empty withdrew prediction")
		}
		s.RecordEmitted(owner.Named(), &Emission{Complete: true})
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
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(target.Name), retained}, Complete: true})
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer("app-web")}, Complete: true})
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
		s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(target.Name)}, Complete: true, binding: binding})
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
			s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, {Obj: barrier}, {Obj: &manifest.ConfigMap{Name: "inert", Namespace: "apps"}}}, Complete: true, binding: binding})
		})
		<-barrier.prepared
		replacement := target
		replacement.Name = "app-web"
		published := make(chan struct{})
		writers.Go(func() {
			defer close(published)
			s.RecordEmitted(owner.Named(), &Emission{Children: []Child{g, consumer(replacement.Name)}, Complete: true, binding: binding})
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
				if s.GetObject(id) != nil || s.GetArtifact(id) != nil {
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
				s.RecordEmitted(owner.Named(), &Emission{Children: []Child{generator("app-{app}"), consumer(name)}, Complete: true})
			})
		}
		close(start)
		wg.Wait()
		accepted := s.GetArtifact(owner.Named()).(*Emission)
		var candidates []manifest.NamedResource
		for _, a := range accepted.entries {
			for ref := range manifest.References(a.child.Obj) {
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
	s.RecordEmitted(owner.Named(), next)
	accepted := s.GetArtifact(owner.Named())
	if got := testing.AllocsPerRun(10, func() { s.RecordEmitted(owner.Named(), next) }); got != 0 {
		t.Fatalf("identical dynamic emission allocated %v times, want zero", got)
	}
	if s.GetArtifact(owner.Named()) != accepted {
		t.Fatal("identical emission replaced the accepted proposal")
	}
	target := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "app-api"}
	assert.Diff(t, s.Productions(target, nil), []manifest.Production{{Owner: owner.Named(), Declaration: generator.Named()}})
}
