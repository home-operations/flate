package store

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
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
	return manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: name}
}

func TestRecordEmitted_Replacement(t *testing.T) {
	s := New()
	p, q := ownerID("p"), ownerID("q")
	s.AddObject(&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace})
	a, b := emissionChild("a", "old"), emissionChild("b", "new")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	accept(t, s, p, &Emission{Children: []Child{a}, Manifests: []map[string]any{a.Doc}, Complete: true})
	accept(t, s, q, &Emission{Children: []Child{a}, Manifests: []map[string]any{a.Doc}, Complete: true})
	if got := slices.Collect(s.Productions(a.Obj.Named(), nil)); len(got) != 2 {
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
}

func TestRecordEmitted_ConcurrentSupport(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			s := New()
			p := ownerID("p")
			s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
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
				if got := slices.Collect(s.Productions(id, nil)); len(got) != 2 || got[0].Owner != p || got[1].Owner != q {
					t.Fatalf("reverse position changed production: %v", got)
				}
				if views := slices.Collect(s.MetadataViews(id)); len(views) != 2 || views[0] != child.Obj || views[1] != child.Obj {
					t.Fatal("ownership-label refresh lost original contributor views")
				}
			}
			accept(t, s, p, &Emission{Complete: true})
			for _, child := range accepted.Children {
				if got := slices.Collect(s.Productions(child.Obj.Named(), nil)); len(got) != 1 || got[0].Owner != q {
					t.Fatalf("withdrawal changed surviving contributor: %v", got)
				}
			}
			for _, labels := range []bool{false, true} {
				t.Run(fmt.Sprintf("stable_replay/labels_%t", labels), func(t *testing.T) {
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
	for _, workers := range []int{2, 4} {
		for _, lateFirst := range []bool{false, true} {
			for _, shared := range []bool{false, true} {
				t.Run(fmt.Sprintf("workers=%d/lateFirst=%t/shared=%t", workers, lateFirst, shared), func(t *testing.T) {
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
						if s.GetObject(id) != nil || s.GetArtifact(id) != nil {
							t.Fatal("hidden payload leaked")
						}
						if obj, _ := s.Get[*manifest.Kustomization](id); obj != nil {
							t.Fatal("typed getter leaked")
						}
						if _, ok := s.GetStatus(id); ok {
							t.Fatal("hidden Ready leaked")
						}
						if len(s.GetConditions(id)) != 0 {
							t.Fatal("hidden conditions leaked")
						}
						if obj, conditions := s.Snapshot(id); obj != nil || len(conditions) != 0 {
							t.Fatal("hidden snapshot leaked")
						}
						if obj, attempt := s.GetEmission(id); obj != nil {
							attempt.Finish()
							t.Fatal("hidden attempt leaked")
						}
						if len(slices.Collect(s.MetadataViews(id))) != 0 || len(slices.Collect(s.Productions(id, nil))) != 0 {
							t.Fatal("hidden metadata leaked")
						}
						if _, ok := s.FailedResources()[id]; ok {
							t.Fatal("hidden failure leaked")
						}
						s.UpdateStatus(id, StatusReady, "")
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
					if _, ok := s.GetStatus(k); ok {
						t.Fatal("reanchoring restored retained Ready")
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

func TestRecordEmitted_MetadataViewsAndContext(t *testing.T) {
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
	if views := slices.Collect(s.MetadataViews(a.Obj.Named())); len(views) != 2 || views[0] != a.Obj || views[1] != b.Obj {
		t.Fatal("equal contribution discarded original metadata view")
	}
	changed := childWithLabels("q")
	changed.Doc["metadata"].(map[string]any)["labels"].(map[string]any)["semantic"] = "different"
	s.RecordEmitted(q, &Emission{Children: []Child{changed}, Complete: true})
	if warnings := s.Warnings(); len(warnings) != 1 || warnings[0].Category != manifest.WarnConflictingProducers {
		t.Fatalf("semantic metadata conflict warning: %+v", warnings)
	}
	fresh := emissionChild("fresh", "value")
	accept(t, s, p, &Emission{Children: []Child{fresh}, Manifests: []map[string]any{fresh.Doc}, binding: second})
	oldProductions := slices.Collect(s.Productions(a.Obj.Named(), nil))
	if !slices.ContainsFunc(oldProductions, func(p manifest.Production) bool { return p.Owner == ownerID("p") && p.Source == first.source }) {
		t.Fatal("partial update rebound predecessor context")
	}
	if got := slices.Collect(s.Productions(fresh.Obj.Named(), nil)); len(got) != 1 || got[0].Source != second.source {
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
			replacement.Render = []byte("new input")
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

func TestRecordEmitted_CoverageAndStaticBoundary(t *testing.T) {
	for _, support := range []string{"producer-only", "bootstrap-orphan", "bootstrap-orphan-equal", "same-source-root", "other-source-root", "self-declared-root", "other-source-only", "declaration-only"} {
		t.Run(support, func(t *testing.T) {
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
			if independent {
				if s.GetObject(seed.Obj.Named()) != actual.Obj {
					t.Fatal("accepted output did not replace independent static content")
				}
				_, empty := s.GetEmission(p)
				empty.Children, empty.Manifests = nil, nil
				empty.Complete = true
				accept(t, s, p, &empty)
				empty.Finish()
				if got := slices.Collect(s.Productions(seed.Obj.Named(), nil)); s.GetObject(seed.Obj.Named()) != seed.Obj || len(got) != 1 || got[0] != productions[len(productions)-1] {
					t.Fatalf("complete empty retired independent source/input anchor: %v", got)
				}
				return
			}
			if provisional {
				if got := s.GetObject(seed.Obj.Named()); got != actual.Obj || len(events) != 1 || events[0] != actual.Obj {
					t.Fatalf("bootstrap replacement not published: %v, events %v", got, events)
				}
				if got := slices.Collect(s.MetadataViews(seed.Obj.Named())); len(got) != 1 || got[0] != actual.Obj {
					t.Fatalf("retired bootstrap metadata: %v", got)
				}
				if got := slices.Collect(s.Productions(seed.Obj.Named(), nil)); len(got) != 1 || got[0].Owner != p {
					t.Fatalf("retired bootstrap support: %v", got)
				}
			}
			_, empty := s.GetEmission(p)
			empty.Children = nil
			empty.Manifests = nil
			empty.Complete = true
			accept(t, s, p, &empty)
			if s.GetObject(seed.Obj.Named()) != nil || len(slices.Collect(s.Productions(seed.Obj.Named(), nil))) != 0 {
				t.Fatal("complete empty resurrected covered prediction")
			}
			s.AddObject(seed.Obj)
			if s.GetObject(seed.Obj.Named()) == nil {
				t.Fatal("explicit external anchor could not restore covered identity")
			}
		})
	}
}

func TestRecordEmitted_DirectWritesPreserveSiblings(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
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
}

func TestStore_ProductionsScopedFileBridge(t *testing.T) {
	for _, differentSource := range []bool{false, true} {
		for _, explicitNamespace := range []bool{false, true} {
			t.Run(fmt.Sprintf("differentSource=%t/explicitNamespace=%t", differentSource, explicitNamespace), func(t *testing.T) {
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
				result := slices.Collect(s.Productions(target, index))
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
	s := New()
	p := ownerID("absent")
	child := emissionChild("private", "value")
	accept(t, s, p, &Emission{Children: []Child{child}, Manifests: []map[string]any{child.Doc}, Complete: true})
	s.UpdateFailure(p, fmt.Errorf("%w: private failure", manifest.ErrInput))
	s.SetArtifact(p, &SourceArtifact{Key: "private"})
	for _, id := range []manifest.NamedResource{p, child.Obj.Named()} {
		if s.GetObject(id) != nil || s.GetArtifact(id) != nil {
			t.Fatal("absent producer leaked live object or artifact")
		}
		if _, ok := s.GetStatus(id); ok || len(s.GetConditions(id)) != 0 {
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
	if _, ok := s.GetStatus(p); ok {
		t.Fatal("external anchor activated private Ready/Failed state")
	}
}

func TestStore_ProductionsRetainsAcceptedOwner(t *testing.T) {
	for _, ownEvidence := range []bool{false, true} {
		t.Run(fmt.Sprint(ownEvidence), func(t *testing.T) {
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
			got := slices.Collect(s.Productions(child.Obj.Named(), nil))
			want := 2
			if ownEvidence {
				want = 1
			}
			if len(got) != want || !slices.Contains(got, proof) || !slices.ContainsFunc(got, func(v manifest.Production) bool { return v.Owner == p }) {
				t.Fatalf("accepted owner lost or redundant thin evidence added: %v", got)
			}
			accept(t, s, p, &Emission{Complete: true})
			if got := slices.Collect(s.Productions(child.Obj.Named(), nil)); len(got) != 0 || s.GetObject(child.Obj.Named()) != nil {
				t.Fatalf("withdrawal retained accepted evidence: %v", got)
			}
		})
	}
}

func TestStore_FootprintOrder(t *testing.T) {
	s := New()
	p := ownerID("p")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	for i := range 16 {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: fmt.Sprintf("%02d", 15-i)}
		index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}}
	}
	s.SetDiscoveryIndex(index)
	_, attempt := s.GetEmission(p)
	defer attempt.Finish()
	var previous manifest.NamedResource
	count := 0
	for id := range attempt.Footprint() {
		if count > 0 && previous.Compare(id) >= 0 {
			t.Fatalf("footprint escaped setup map order: %s before %s", previous, id)
		}
		previous = id
		count++
	}
	if count != 16 {
		t.Fatalf("footprint size=%d", count)
	}
}

func TestRecordEmitted_ConflictingProducers(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
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
				if s.GetObject(id) != winner.Obj {
					t.Fatal("winner depends on publication order")
				}
				if obj, _ := s.Snapshot(id); obj != winner.Obj {
					t.Fatal("snapshot did not select the winner")
				}
				unsub := s.AddListener(EventObjectAdded, func(got manifest.NamedResource, data any) {
					if got == id && data != winner.Obj {
						t.Error("replay did not select the winner")
					}
				}, true)
				unsub()
				warnings := s.Warnings()
				want := make([]string, workers)
				for i, owner := range owners {
					want[i] = owner.String()
				}
				if len(warnings) != 1 || warnings[0].Resource != id || warnings[0].Category != "ConflictingProducers" || !slices.Equal(warnings[0].Detail, want) || warnings[0].Count != 1 {
					t.Fatalf("conflict warning: %+v", warnings)
				}
				accept(t, s, owners[workers-1], &Emission{Complete: true})
				if s.GetObject(id) != children[workers-2].Obj {
					t.Fatal("withdrawal did not select the surviving winner")
				}
				warnings = s.Warnings()
				if workers == 2 && len(warnings) != 0 || workers == 4 && (len(warnings) != 1 || !slices.Equal(warnings[0].Detail, want[:workers-1])) {
					t.Fatalf("withdrawn producer remained in warning: %+v", warnings)
				}
				for _, owner := range owners {
					accept(t, s, owner, &Emission{Children: []Child{winner}, Manifests: []map[string]any{winner.Doc}, Complete: true})
				}
				if warnings := s.Warnings(); len(warnings) != 0 {
					t.Fatalf("equal content warned: %+v", warnings)
				}
			})
		}
	}
}

func TestRecordEmitted_SharedChildArrival(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
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
