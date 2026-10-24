package store

import (
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCapture_ProducerRecordLifetime(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	child := emissionChild("input", "old")
	s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: true})
	previous := weak.Make(s.GetArtifact(producer.Named()).(*Emission))
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	s.RecordEmitted(producer.Named(), &Emission{Children: []Child{emissionChild("input", "new")}, Complete: true})
	runtime.GC()
	if previous.Value() != nil {
		t.Fatal("captured child retained the superseded producer's execution history")
	}
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	assert.Equal(t, reads.Valid(), false)
	runtime.KeepAlive(s)
	runtime.KeepAlive(&reads)
}

func TestReadRegistration_CollectionLabelReplacement(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	provider := &manifest.ResourceSetInputProvider{Name: "provider", Namespace: "apps", Labels: map[string]string{"selected": "false"}}
	publish := func(obj *manifest.ResourceSetInputProvider) {
		s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: obj}}, Complete: true})
	}
	publish(provider)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	objects := reads.ListObjects(provider.Named().Group, provider.Named().Kind)
	assert.Equal(t, len(objects), 1)
	assert.Equal(t, objects[0].(*manifest.ResourceSetInputProvider).Labels["selected"], "false")
	sh := s.shardFor(provider.Named())
	sh.mu.RLock()
	before := sh.byName[kindID{provider.Named().Group, provider.Named().Kind}].generation
	sh.mu.RUnlock()
	changed := *provider
	changed.Labels = map[string]string{"selected": "true"}
	publish(&changed)
	sh.mu.RLock()
	after := sh.byName[kindID{provider.Named().Group, provider.Named().Kind}].generation
	sh.mu.RUnlock()
	if after <= before {
		t.Fatal("same-identity label publication bypassed collection replacement")
	}
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.ID()})
	assert.Equal(t, reads.Valid(), false)
	objects = s.ListObjects(provider.Named().Group, provider.Named().Kind)
	assert.Equal(t, objects[0].(*manifest.ResourceSetInputProvider).Labels["selected"], "true")
}

func TestReadRegistration_CrossShardLostWakeup(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before-link", "after-link", "after-validation"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s := New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(producer)
			old := emissionChild("input", "old")
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{old}, Complete: true})
			if kindShardIndex(producer.Named().Kind) == kindShardIndex(old.Obj.Named().Kind) {
				t.Fatal("fixture must span distinct shards")
			}
			var reads ReadSet
			reads.Init(s, ownerID("reader"), make([]Input, 3))
			if phase == "before-link" {
				sh := s.shardFor(old.Obj.Named())
				sh.mu.RLock()
				var view capture
				sh.captureLocked(old.Obj.Named(), sh.cells[old.Obj.Named()], &view)
				sh.mu.RUnlock()
				reads.sample(view.owner, view.record)
			} else {
				assert.Equal(t, reads.GetObject(old.Obj.Named()), old.Obj)
				if phase == "after-validation" && !reads.Valid() {
					t.Fatal("unchanged captured input was invalid")
				}
			}
			published := make(chan struct{})
			go func() {
				s.RecordEmitted(producer.Named(), &Emission{Children: []Child{emissionChild("input", "new")}, Complete: true})
				close(published)
			}()
			<-published
			if phase == "before-link" {
				reads.activateProducers()
				if reads.Valid() {
					t.Fatal("activation certified an obsolete producer record")
				}
			} else {
				assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
				if reads.Valid() {
					t.Fatal("publication was lost after activation")
				}
			}
			reads.Retire()
			for range s.Sweep() {
			}
		})
	}
}

func TestDerivedCapture_UnselectedContributorStableCommit(t *testing.T) {
	t.Parallel()
	s := New()
	a, z := ownerID("a"), ownerID("z")
	s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace}, &manifest.Kustomization{Name: z.Name, Namespace: z.Namespace}})
	child := emissionChild("shared", "first")
	s.RecordEmitted(a, &Emission{Children: []Child{child}, Complete: true})
	s.RecordEmitted(z, &Emission{Children: []Child{child}, Complete: true})
	var reads ReadSet
	reads.Init(s, ownerID("reader"), make([]Input, 4))
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	_ = reads.Productions(child.Obj.Named(), nil)
	before := headsForTest(s, child.Obj.Named())
	s.RecordEmitted(a, &Emission{Children: []Child{emissionChild("shared", "second")}, Complete: true})
	assert.Equal(t, headsForTest(s, child.Obj.Named()).object, before.object)
	assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	if reads.Valid() {
		t.Fatal("unselected contributor's old record remained valid")
	}
	reads.Retire()
	for range s.Sweep() {
	}
}

func TestDerivedCapture_UnselectedContributorReplacement(t *testing.T) {
	t.Parallel()
	for _, invalidated := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid_predecessor", true: "invalid_predecessor"}[invalidated], func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z := ownerID("a"), ownerID("z")
			input := emissionChild("input", "old")
			s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace}, &manifest.Kustomization{Name: z.Name, Namespace: z.Namespace}, input.Obj})
			var previous, content, contributors ReadSet
			previous.Init(s, a, nil)
			previous.GetObject(input.Obj.Named())
			content.Init(s, ownerID("content"), nil)
			contributors.Init(s, ownerID("contributors"), nil)
			defer func() {
				previous.Retire()
				content.Retire()
				contributors.Retire()
				for range s.Sweep() {
				}
			}()
			child := emissionChild("shared", "first")
			s.RecordEmitted(a, &Emission{Reads: &previous, Children: []Child{child}, Complete: true})
			s.RecordEmitted(z, &Emission{Children: []Child{child}, Complete: true})
			assert.Equal(t, content.GetObject(child.Obj.Named()), child.Obj)
			assert.Diff(t, contributors.Productions(child.Obj.Named(), nil), []manifest.Production{{Owner: a}, {Owner: z}})
			before := headsForTest(s, child.Obj.Named())
			if invalidated {
				s.AddObject(emissionChild("input", "new").Obj)
			}
			s.RecordEmitted(a, &Emission{Children: []Child{child, emissionChild("other", "new")}, Complete: true})
			assert.Equal(t, headsForTest(s, child.Obj.Named()).object, before.object)
			assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
			assert.Equal(t, content.Valid(), true)
			assert.Equal(t, contributors.Valid(), false)
		})
	}
}

func TestCapture_StableSegmentLifetime(t *testing.T) {
	t.Parallel()
	s := New()
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	for i := range 33 {
		reads.GetObject(ownerID(string(rune('a' + i))))
	}
	if !reads.Valid() {
		t.Fatal("segment growth changed original registrations")
	}
	target := ownerID("a")
	s.AddObject(&manifest.Kustomization{Name: target.Name, Namespace: target.Namespace})
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	reads.Retire()
	for range s.Sweep() {
	}
	for _, sh := range s.shards {
		sh.mu.RLock()
		if sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load() {
			t.Error("retired absence remains reachable from the shard")
		}
		for _, cell := range sh.cells {
			if cell.links != nil && (cell.links.first.Load() != nil || cell.links.detached.Load() || cell.links.pending.Load()) {
				t.Error("retired segment remains reachable from an authority")
			}
		}
		sh.mu.RUnlock()
	}
	var successor ReadSet
	successor.Init(s, ownerID("successor"), nil)
	successor.GetObject(target)
	old := reads.input(target).cells[objectFacet-1].head
	current := successor.input(target).cells[objectFacet-1].head
	if old != &s.shardFor(target).absence || current != s.shardFor(target).cells[target].links {
		t.Fatal("successor did not capture the admitted authority")
	}
	published := make(chan struct{})
	go func() {
		s.AddObject(&manifest.Kustomization{Name: target.Name, Namespace: target.Namespace, Path: "new"})
		close(published)
	}()
	<-published
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{successor.id})
	successor.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestReadRegistration_RetirementUnderSupersession(t *testing.T) {
	t.Parallel()
	s := New()
	id := ownerID("input")
	var previous, successor ReadSet
	previous.Init(s, ownerID("old"), nil)
	successor.Init(s, ownerID("new"), nil)
	previous.GetObject(id)
	successor.GetObject(id)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() { ready.Done(); <-start; previous.Retire(); done <- struct{}{} }()
	go func() {
		ready.Done()
		<-start
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		done <- struct{}{}
	}()
	ready.Wait()
	close(start)
	<-done
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{successor.id})
	successor.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestReadRegistration_ActivationUnderReadLock(t *testing.T) {
	t.Parallel()
	for _, facet := range []string{"object", "condition", "artifact", "bucket"} {
		for _, present := range []bool{false, true} {
			name := "absent"
			if present {
				name = "present"
			}
			t.Run(facet+"/"+name, func(t *testing.T) {
				t.Parallel()
				s := New()
				id := ownerID("input")
				publish := func() {
					switch facet {
					case "object", "bucket":
						s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "new"})
					case "condition":
						s.UpdateStatus(id, StatusReady, "", "new")
					case "artifact":
						s.SetArtifact(id, &SourceArtifact{Revision: "new"})
					}
				}
				if present {
					publish()
				}
				var reads [2]ReadSet
				var ready, done sync.WaitGroup
				ready.Add(len(reads))
				done.Add(len(reads))
				start := make(chan struct{})
				sh := s.shardFor(id)
				sh.mu.RLock()
				for i := range reads {
					go func() {
						defer done.Done()
						reads[i].Init(s, ownerID(string(rune('a'+i))), nil)
						ready.Done()
						<-start
						switch facet {
						case "object":
							reads[i].GetObject(id)
						case "condition":
							_, _ = reads[i].Snapshot(id, true)
						case "artifact":
							reads[i].GetArtifact(id)
						case "bucket":
							reads[i].ListObjects(id.Group, id.Kind)
						}
					}()
				}
				ready.Wait()
				close(start)
				done.Wait()
				sh.mu.RUnlock()
				if present {
					switch facet {
					case "object", "bucket":
						s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "changed"})
					case "condition":
						s.UpdateStatus(id, StatusFailed, "", "changed")
					case "artifact":
						s.SetArtifact(id, &SourceArtifact{Revision: "changed"})
					}
				} else {
					publish()
				}
				invalid := slices.Collect(s.Sweep())
				slices.SortFunc(invalid, manifest.NamedResource.Compare)
				assert.Diff(t, invalid, []manifest.NamedResource{reads[0].id, reads[1].id})
				for i := range reads {
					reads[i].Retire()
				}
				assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
			})
		}
	}
}

func TestReadRegistration_RepeatedFacetPreservesFirstStamp(t *testing.T) {
	t.Parallel()
	s := New()
	var reads ReadSet
	id := ownerID("input")
	reads.Init(s, ownerID("reader"), nil)
	assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(nil))
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(nil))
	if reads.Valid() {
		t.Fatal("a later lookup replaced consumed absence")
	}
	reads.Retire()
	for range s.Sweep() {
	}
}

func TestReadRegistration_ReclamationOnlyAndCutoff(t *testing.T) {
	t.Parallel()
	s := New()
	id := ownerID("input")
	version := func(path string) *manifest.Kustomization {
		return &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: path}
	}
	s.AddObject(version("zero"))
	var previous, successor, fresh ReadSet
	previous.Init(s, ownerID("previous"), nil)
	successor.Init(s, ownerID("successor"), nil)
	fresh.Init(s, ownerID("fresh"), nil)
	previous.GetObject(id)
	s.AddObject(version("one"))
	successor.GetObject(id)
	detached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for invalid := range s.Sweep() {
			if invalid != previous.id {
				t.Errorf("first cutoff invalidated %s, want %s", invalid, previous.id)
			}
			close(detached)
			<-resume
		}
	}()
	<-detached
	previous.Retire()
	s.AddObject(version("two"))
	fresh.GetObject(id)
	close(resume)
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{successor.id})
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	if !fresh.Valid() {
		t.Fatal("reattachment invalidated a new live registration")
	}
	generation := headsForTest(s, id).object
	successor.Retire()
	fresh.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	assert.Equal(t, headsForTest(s, id).object, generation)
	sh := s.shardFor(id)
	sh.mu.RLock()
	head := sh.cells[id].authority
	sh.mu.RUnlock()
	if head.links != nil && (head.links.first.Load() != nil || head.links.pending.Load() || head.links.detached.Load()) {
		t.Fatal("reclamation left a live or pending registration")
	}
}

func TestReadRegistration_AbsenceDoesNotAdmitObjects(t *testing.T) {
	t.Parallel()
	s := New()
	var reads [2]ReadSet
	var done sync.WaitGroup
	done.Add(len(reads))
	for i := range reads {
		go func() {
			defer done.Done()
			reads[i].Init(s, ownerID(string(rune('a'+i))), nil)
			reads[i].GetObject(ownerID("missing"))
		}()
	}
	done.Wait()
	for _, sh := range s.shards {
		sh.mu.RLock()
		count := len(sh.cells)
		sh.mu.RUnlock()
		assert.Equal(t, count, 0)
	}
	assert.Equal(t, len(s.ListObjects("", "")), 0)
	s.AddObject(&manifest.Kustomization{Name: "unrelated", Namespace: "apps"})
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	for i := range reads {
		if !reads[i].Valid() {
			t.Fatal("unrelated admission invalidated consumed absence")
		}
	}
	s.AddObject(&manifest.Kustomization{Name: "missing", Namespace: "apps"})
	invalid := slices.Collect(s.Sweep())
	slices.SortFunc(invalid, manifest.NamedResource.Compare)
	assert.Diff(t, invalid, []manifest.NamedResource{reads[0].id, reads[1].id})
	for i := range reads {
		reads[i].Retire()
	}
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestReadRegistration_AbsentAdmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"batch", "emission", "hidden-producer"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			child := emissionChild("input", "new")
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			if mode == "emission" {
				s.AddObject(producer)
			}
			id := child.Obj.Named()
			if mode == "hidden-producer" {
				id = producer.Named()
			}
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			reads.GetObject(id)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if mode == "batch" {
					s.AddObjects([]manifest.BaseManifest{child.Obj})
				} else {
					s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: true})
				}
			}()
			<-done
			assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
			if reads.Valid() {
				t.Fatal("first authority admission left consumed absence valid")
			}
			reads.Retire()
			assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
		})
	}
}

func TestReadRegistration_AbsentDetachedAdmission(t *testing.T) {
	t.Parallel()
	s := New()
	var first, later, retired, fresh ReadSet
	for _, reads := range []*ReadSet{&first, &later, &retired, &fresh} {
		reads.Init(s, ownerID("reader"), nil)
	}
	first.id, later.id = ownerID("first-reader"), ownerID("later-reader")
	firstID, laterID := ownerID("first-input"), ownerID("later-input")
	later.GetObject(laterID)
	retired.GetObject(ownerID("retired-input"))
	first.GetObject(firstID)
	retired.Retire()
	s.AddObject(&manifest.Kustomization{Name: firstID.Name, Namespace: firstID.Namespace})
	detached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for id := range s.Sweep() {
			assert.Equal(t, id, first.id)
			close(detached)
			<-resume
		}
	}()
	<-detached
	first.Retire()
	fresh.GetObject(ownerID("still-absent"))
	s.AddObject(&manifest.Kustomization{Name: laterID.Name, Namespace: laterID.Namespace})
	close(resume)
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{later.id})
	if !fresh.Valid() {
		t.Fatal("new live absence registration was invalidated")
	}
	later.Retire()
	fresh.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	sh := s.shardFor(laterID)
	if sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load() {
		t.Fatal("retired absence registrations survived reclamation")
	}
}

func TestReadRegistration_AbsenceExhaustionIsAtomic(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"object", "condition", "artifact", "emission"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.ConfigMap{Name: "new", Namespace: "apps"}
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(producer)
			var reads ReadSet
			reads.Init(s, producer.Named(), nil)
			switch operation {
			case "condition":
				reads.GetStatus(obj.Named())
			case "artifact":
				reads.GetArtifact(obj.Named())
			default:
				reads.GetObject(obj.Named())
			}
			sh := s.shardFor(obj.Named())
			generation := ^uint64(0)
			if operation == "object" || operation == "emission" {
				generation--
			}
			sh.absence.generation.Store(generation)
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				switch operation {
				case "object":
					s.AddObject(obj)
				case "condition":
					s.UpdateStatus(obj.Named(), StatusReady, "", "")
				case "artifact":
					s.SetArtifact(obj.Named(), &SourceArtifact{})
				case "emission":
					s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: obj}}, Complete: true})
				}
			}()
			if !panicked || sh.absence.generation.Load() != generation || !reads.Valid() {
				t.Fatalf("panic=%t generation=%d valid=%t", panicked, sh.absence.generation.Load(), reads.Valid())
			}
			if s.GetObject(obj.Named()) != nil || s.GetArtifact(obj.Named()) != nil {
				t.Fatal("exhaustion partially admitted target")
			}
			if _, ok := s.GetStatus(obj.Named()); ok {
				t.Fatal("exhaustion partially published status")
			}
			if s.GetArtifact(producer.Named()) != nil {
				t.Fatal("exhaustion partially published inventory")
			}
		})
	}
}

func TestProductions_ProjectedRawFamily(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                         string
		separateFile, separateSource bool
	}{
		{name: "shared file"}, {name: "different file", separateFile: true}, {name: "different source", separateSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			raw := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings"}
			target := raw
			target.Namespace = "apps"
			first := testutil.KustomizationID("apps", "first")
			second := testutil.KustomizationID("other", "second")
			p := manifest.Production{Owner: first, Source: manifest.BootstrapSourceID, Authored: raw, File: "settings.yaml", Path: "/settings.yaml"}
			other := p
			other.Owner = second
			selected := p
			if tc.separateFile {
				selected.File, selected.Path = "another.yaml", "/another.yaml"
			}
			if tc.separateSource {
				selected.Source = manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "foreign"}
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw: {Productions: []manifest.Production{p, other}}, target: {Productions: []manifest.Production{selected}},
			}}
			st.SetDiscoveryIndex(index)
			var reads ReadSet
			reads.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
			got := reads.Productions(target, index)
			want := []manifest.Production{p, other}
			if tc.separateFile || tc.separateSource {
				want = []manifest.Production{selected}
			}
			assert.Diff(t, got, want)
			assert.Diff(t, st.Productions(target, index), want)
		})
	}
}

func TestCapture_RawSupportExcludesReadiness(t *testing.T) {
	t.Parallel()
	st := New()
	owner := ownerID("producer")
	source := manifest.BootstrapSourceID
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	st.SetArtifact(source, &SourceArtifact{Key: "content"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {Productions: []manifest.Production{{Owner: owner, Source: source, Authored: id, File: "settings.yaml", Path: "/settings.yaml"}}},
	}})
	st.UpdateStatus(owner, StatusPending, "", "pending")
	var reads ReadSet
	reads.Init(st, ownerID("consumer"), nil)
	for range reads.Promotions(id, nil) {
	}
	reads.GetStatus(owner)
	reads.Admit(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "raw"}}, source, owner)
	st.UpdateStatus(owner, StatusFailed, "", "render failed")
	if reads.Valid() || st.GetObject(id) == nil {
		t.Fatal("readiness transition must invalidate its reader without withdrawing source bytes")
	}
	st.RecordEmitted(owner, &Emission{Complete: true})
	if st.GetObject(id) != nil {
		t.Fatal("producer replacement retained unsupported source bytes")
	}
}

func TestReadSet_MatchesMutableArtifact(t *testing.T) {
	t.Parallel()
	st := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	artifact := &SourceArtifact{LocalPath: "input", FS: fstest.MapFS{}}
	st.SetArtifact(source.Named(), artifact)
	var before, current ReadSet
	before.Init(st, ownerID("consumer"), nil)
	before.GetArtifact(source.Named())
	current.Init(st, ownerID("consumer"), nil)
	if !current.Matches(&before) {
		t.Fatal("the same mutable artifact was treated as different material")
	}
	st.SetArtifact(source.Named(), &SourceArtifact{LocalPath: artifact.LocalPath, FS: artifact.FS})
	var replacement ReadSet
	replacement.Init(st, ownerID("consumer"), nil)
	if replacement.Matches(&before) {
		t.Fatal("a new mutable artifact was inferred equal from its location")
	}
}

func TestReadSet_UnsettledProducerSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		known bool
	}{{"empty", false}, {"known", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
			second := &manifest.Kustomization{Name: "second", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, first, second})
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}})
			}
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Retire()
			before := reads.UnsettledProducers(source.Named())
			st.RecordEmitted(second.Named(), &Emission{Children: []Child{{Obj: source}}})
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}, Complete: true})
			}
			if after := reads.UnsettledProducers(source.Named()); !slices.Equal(before, after) {
				t.Fatalf("availability query changed within one captured execution: before=%v after=%v", before, after)
			}
		})
	}
}

func TestRequire_RetainsPredicates(t *testing.T) {
	t.Parallel()
	for _, expressions := range [][]string{
		{"false", ""}, {"", "false"}, {"true", "false"},
		{"false", "true", "false", "", "true", "1 == 2", "1 == 2", ""},
	} {
		t.Run(strings.Join(expressions, "/"), func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
			s.AddObject(obj)
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			var want []manifest.DependencyRef
			for _, expr := range expressions {
				ref := manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: expr}
				reads.Require(ref)
				if !slices.Contains(want, ref) {
					want = append(want, ref)
				}
			}
			assert.Diff(t, slices.Collect(reads.Gates()), want)
			assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
			assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
			if reads.inputs != &reads.local || reads.inputs.next != nil {
				t.Fatal("predicates created multiple captured authorities for one identity")
			}
			for range reads.Gates() {
				break
			}
			got := 0
			for range reads.Gates() {
				got++
				if got == 2 {
					break
				}
			}
			assert.Equal(t, got, 2)
		})
	}
}

func TestRequire_ReleasesPredicates(t *testing.T) {
	t.Parallel()
	s := New()
	var reads ReadSet
	inputs := make([]Input, 1)
	for _, retirement := range []bool{false, true} {
		reads.Init(s, ownerID("reader"), inputs)
		for _, id := range []manifest.NamedResource{ownerID("first"), ownerID("second")} {
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: "true"})
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: "false"})
		}
		if retirement {
			reads.Retire()
			for input := reads.inputs; input != nil; input = input.next {
				if input.gates != nil {
					t.Fatal("retirement retained overflow predicates")
				}
			}
		}
	}
	reads.Init(s, ownerID("reader"), inputs)
	assert.Equal(t, len(slices.Collect(reads.Gates())), 0)
	if reads.local.gates != nil || inputs[0].gates != nil {
		t.Fatal("reset retained overflow predicates")
	}
}
