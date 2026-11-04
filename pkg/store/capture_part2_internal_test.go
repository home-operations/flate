package store

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadRegistration_AbsentAdmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"batch", "emission", "hidden-producer"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testReadRegistrationAbsentAdmissionCase(t, &mode)
		},
		)
	}
}

func TestReadRegistration_AbsentDetachedAdmission(t *testing.T) {
	t.Parallel()
	for _, mask := range []ArtifactRead{0, ArtifactContent, ArtifactRevision, ArtifactOrigin, ArtifactContent | ArtifactRevision | ArtifactOrigin} {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			t.Parallel()
			testReadRegistrationAbsentDetachedAdmissionCase(
				t,
				&mask,
			)
		},
		)
	}
}

func TestReadRegistration_AbsenceExhaustionIsAtomic(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"object", "condition", "artifact", "revision", "origin", "union", "emission"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testReadRegistrationAbsenceExhaustionIsAtomicCase(
				t,
				&operation,
			)
		},
		)
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
			p := manifest.Production{
				Owner:    first,
				Source:   manifest.BootstrapSourceID,
				Authored: raw,
				File:     "settings.yaml",
				Path:     "/settings.yaml",
			}
			other := p
			other.Owner = second
			selected := p
			if tc.separateFile {
				selected.File, selected.Path = "another.yaml", "/another.yaml"
			}
			if tc.separateSource {
				selected.Source = manifest.NamedResource{
					Group: manifest.SourceDomain,
					Kind:  manifest.KindGitRepository,
					Name:  "foreign",
				}
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw: {
					Productions: []manifest.Production{p, other},
				},
				target: {Productions: []manifest.Production{selected}},
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
		id: {
			Productions: []manifest.Production{
				{Owner: owner, Source: source, Authored: id, File: "settings.yaml", Path: "/settings.yaml"},
			},
		},
	}})
	st.UpdateStatus(owner, StatusInfo{Status: StatusPending, Reason: "", Message: "pending"})
	var reads ReadSet
	reads.Init(st, ownerID("consumer"), nil)
	for range reads.Promotions(id, nil) {
	}
	reads.GetStatus(owner)
	reads.Admit(
		&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "raw"}},
		source,
		owner,
	)
	st.UpdateStatus(owner, StatusInfo{Status: StatusFailed, Reason: "", Message: "render failed"})
	{
		assert.FatalIf(
			t,
			reads.Valid() || st.GetObject(id) == nil,
			"%s",
			"readiness transition must invalidate its reader without withdrawing source bytes",
		)
	}
	st.RecordEmitted(owner, &Emission{Complete: true}, nil)
	{
		assert.FatalIf(t, st.GetObject(id) != nil, "%s", "producer replacement retained unsupported source bytes")
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
	before.GetArtifact(source.Named(), ArtifactContent)
	current.Init(st, ownerID("consumer"), nil)
	{
		assert.FatalIf(
			t,
			!current.Matches(&before),
			"%s",
			"the same mutable artifact was treated as different material",
		)
	}
	st.SetArtifact(source.Named(), artifact.WithReads(&ReadSet{}))
	{
		assert.FatalIf(
			t,
			!before.Valid() || !current.Matches(&before),
			"%s",
			"refreshing registrations replaced the acquired mutable material",
		)
	}
	st.SetArtifact(source.Named(), &SourceArtifact{LocalPath: artifact.LocalPath, FS: artifact.FS})
	var replacement ReadSet
	replacement.Init(st, ownerID("consumer"), nil)
	{
		assert.FatalIf(
			t,
			replacement.Matches(&before),
			"%s",
			"a new mutable artifact was inferred equal from its location",
		)
	}
}

func TestReadSet_MatchesRawCollection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		value   string
		matches bool
	}{
		{"equal distinct content", "same", true},
		{"changed nested content", "changed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := &manifest.RawObject{
				Kind:       "Widget",
				APIVersion: "example.test/v1",
				Name:       "app",
				Spec:       map[string]any{"items": []any{map[string]any{"value": "same"}}},
			}
			second := first.Clone()
			second.Spec["items"].([]any)[0].(map[string]any)["value"] = tc.value
			beforeStore, currentStore := New(), New()
			beforeStore.AddObject(first)
			currentStore.AddObject(second)
			var before, current ReadSet
			before.Init(beforeStore, ownerID("reader"), nil)
			current.Init(currentStore, ownerID("reader"), nil)
			defer before.Retire()
			defer current.Retire()
			id := first.Named()
			objects := before.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], manifest.BaseManifest(first))
			assert.Equal(t, current.Matches(&before), tc.matches)
			objects = current.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], manifest.BaseManifest(second))
			assert.Equal(t, objects[0] == first, false)
			assert.Diff(t, first.Spec, map[string]any{"items": []any{map[string]any{"value": "same"}}})
		})
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
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}}, nil)
			}
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Retire()
			before := reads.UnsettledProducers(source.Named())
			st.RecordEmitted(second.Named(), &Emission{Children: []Child{{Obj: source}}}, nil)
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}, Complete: true}, nil)
			}
			{
				after := reads.UnsettledProducers(source.Named())
				assert.FatalIf(
					t,
					!slices.Equal(before, after),
					"availability query changed within one captured execution: before=%v after=%v",
					before,
					after,
				)
			}
		})
	}
}

func TestRequire_RetainsPredicates(t *testing.T) {
	t.Parallel()
	for _, expressions := range [][]string{
		{"false", ""},
		{"", "false"},
		{"true", "false"},
		{"false", "true", "false", "", "true", "1 == 2", "1 == 2", ""},
	} {
		t.Run(strings.Join(expressions, "/"), func(t *testing.T) {
			t.Parallel()
			testRequireRetainsPredicatesCase(t, &expressions)
		},
		)
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("facts/reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			testRequireRetainsPredicatesCase2(t, &reverse)
		},
		)
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
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: new("true")})
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: new("false")})
		}
		if retirement {
			reads.Retire()
			for input := reads.inputs; input != nil; input = input.next {
				{
					details := input.details.Load()
					assert.FatalIf(
						t,
						details != nil && details.gates != nil,
						"%s",
						"retirement retained overflow predicates",
					)
				}
			}
		}
	}
	reads.Init(s, ownerID("reader"), inputs)
	assert.Equal(t, len(slices.Collect(reads.Gates())), 0)
	{
		assert.FatalIf(
			t,
			reads.localDetails.gates != nil || inputs[0].details.Load() != nil,
			"%s",
			"reset retained overflow predicates",
		)
	}
}

func TestRequire_CapturedIdentityDuringAdmission(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(nil))
	ref := manifest.DependencyRef{
		NamedResource: obj.Named(),
		ReadyExpr:     new("true"),
		Artifact:      true,
		ChartSource:   true,
		ArtifactOnly:  true,
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		reads.Require(ref)
	})
	workers.Go(func() {
		<-start
		s.AddObject(obj)
	})
	close(start)
	workers.Wait()
	assert.Diff(t, slices.Collect(reads.Gates()), []Gate{{DependencyRef: ref}})
	assert.Equal(t, s.GetObject(obj.Named()), manifest.BaseManifest(obj))
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(nil))
}

func TestReadSet_DeclarationAuthority(t *testing.T) {
	t.Parallel()
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	supplier := &manifest.Kustomization{Name: "supplier", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps", URL: "oci://same.example"}
	s.AddObjects([]manifest.BaseManifest{owner, supplier, raw})
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
			},
		},
	)
	var pending ReadSet
	pending.Init(s, ownerID("waiting"), nil)
	defer pending.Retire()
	{
		assert.FatalIf(
			t,
			pending.GetDeclaration(raw.Named()) != nil || pending.GetObject(raw.Named()) != raw,
			"%s",
			"raw visibility supplied authority",
		)
	}
	s.RecordEmitted(supplier.Named(), &Emission{Children: []Child{{Obj: raw}}, Complete: true}, nil)
	invalidated := slices.Collect(s.Sweep())
	{
		assert.FatalIf(
			t,
			!slices.Contains(invalidated, pending.ID()) || pending.GetDeclaration(raw.Named()) != nil,
			"%s",
			"arrival failed to wake or changed the first captured view",
		)
	}
	var accepted ReadSet
	accepted.Init(s, ownerID("accepted"), nil)
	defer accepted.Retire()
	{
		assert.FatalIf(
			t,
			accepted.GetDeclaration(raw.Named()) != raw || accepted.Matches(&pending),
			"%s",
			"same bytes concealed authority arrival",
		)
	}
	s.RecordEmitted(supplier.Named(), &Emission{Complete: true}, nil)
	{
		assert.FatalIf(
			t,
			accepted.Valid() || s.GetObject(raw.Named()) != raw || s.GetDeclaration(raw.Named()) != nil,
			"%s",
			"withdrawal lost raw identity or retained authority",
		)
	}
	var withdrawn ReadSet
	withdrawn.Init(s, ownerID("withdrawn"), nil)
	defer withdrawn.Retire()
	{
		assert.FatalIf(
			t,
			withdrawn.Matches(&accepted) || pending.Valid(),
			"%s",
			"same bytes concealed withdrawal or reused authority after ABA",
		)
	}
	s.RecordEmitted(supplier.Named(), &Emission{Children: []Child{{Obj: raw}}, Complete: true}, nil)
	var supplied ReadSet
	supplied.Init(s, ownerID("supplied"), nil)
	defer supplied.Retire()
	supplied.GetDeclaration(raw.Named())
	s.AddObject(raw)
	{
		assert.FatalIf(t, supplied.Valid(), "%s", "rooting failed to invalidate the supplier capture")
	}
	var rooted ReadSet
	rooted.Init(s, ownerID("rooted"), nil)
	defer rooted.Retire()
	{
		assert.FatalIf(
			t,
			!rooted.Matches(&supplied) || rooted.GetDeclaration(raw.Named()) != raw,
			"%s",
			"equal effective content could not rebind rooted authority",
		)
	}
}

func TestCapture_PayloadAuthority(t *testing.T) {
	t.Parallel()
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("static=%t", static), func(t *testing.T) {
			t.Parallel()
			testCapturePayloadAuthorityCase(t, &static)
		},
		)
	}
}

func TestRecordEmitted_StalePayloadRejected(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	input := ownerID("input")
	s.SetArtifact(input, &SourceArtifact{Key: "first"})
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	defer reads.Retire()
	reads.GetArtifact(input, ArtifactContent)
	_, _, next := reads.GetEmission(producer.Named())
	defer next.Finish()
	child := emissionChild("output", "old")
	next.Children, next.Complete = []Child{child}, true
	reads.Bind(&next)
	s.SetArtifact(input, &SourceArtifact{Key: "changed"})
	s.RecordEmitted(
		producer.Named(),
		&next,
		map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "stale"}},
	)
	{
		assert.FatalIf(
			t,
			s.GetObject(child.Obj.Named()) != nil || s.GetArtifact(child.Obj.Named(), ArtifactContent) != nil ||
				s.HasInventory(producer.Named()),
			"%s",
			"invalid captured inputs published stale declaration or bytes",
		)
	}
}

func testReadRegistrationAbsentAdmissionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	child := emissionChild("input", "new")
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	if (*testCaseInput) == "emission" {
		s.AddObject(producer)
	}
	id := child.Obj.Named()
	if (*testCaseInput) == "hidden-producer" {
		id = producer.Named()
	}
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	reads.GetObject(id)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if (*testCaseInput) == "batch" {
			s.AddObjects([]manifest.BaseManifest{child.Obj})
		} else {
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
		}
	}()
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	{
		assert.FatalIf(t, reads.Valid(), "%s", "first authority admission left consumed absence valid")
	}
	reads.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func testReadRegistrationAbsentDetachedAdmissionCase(
	t *testing.T,
	testCaseInput *ArtifactRead,
) {
	t.Helper()

	s := New()
	read := func(r *ReadSet, id manifest.NamedResource) {
		if (*testCaseInput) == 0 {
			r.GetObject(id)
		} else {
			r.GetArtifact(id, (*testCaseInput))
		}
	}
	publish := func(id manifest.NamedResource) {
		if (*testCaseInput) == 0 {
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		} else {
			s.SetArtifact(
				id,
				&SourceArtifact{
					Revision: "present",
					Metadata: map[string]string{"org.opencontainers.image.revision": ""},
				},
			)
		}
	}
	var first, later, retired, fresh ReadSet
	for _, reads := range []*ReadSet{&first, &later, &retired, &fresh} {
		reads.Init(s, ownerID("reader"), nil)
	}
	first.id, later.id = ownerID("first-reader"), ownerID("later-reader")
	firstID, laterID := ownerID("first-input"), ownerID("later-input")
	read(&later, laterID)
	read(&retired, ownerID("retired-input"))
	read(&first, firstID)
	retired.Retire()
	publish(firstID)
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
	read(&fresh, ownerID("still-absent"))
	publish(laterID)
	close(resume)
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{later.id})
	{
		assert.FatalIf(t, !fresh.Valid(), "%s", "new live absence registration was invalidated")
	}
	later.Retire()
	fresh.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	sh := s.shardFor(laterID)
	{
		assert.FatalIf(
			t,
			sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load(),
			"%s",
			"retired absence registrations survived reclamation",
		)
	}
}

func testReadRegistrationAbsenceExhaustionIsAtomicCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := New()
	obj := &manifest.ConfigMap{Name: "new", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	switch *testCaseInput {
	case "condition":
		reads.GetStatus(obj.Named())
	case "artifact", "revision", "origin", "union":
		reads.GetArtifact(obj.Named(), ArtifactContent|ArtifactRevision|ArtifactOrigin)
	default:
		reads.GetObject(obj.Named())
	}
	sh := s.shardFor(obj.Named())
	generation := ^uint64(0)
	if (*testCaseInput) == "object" || (*testCaseInput) == "emission" {
		generation--
	}
	if (*testCaseInput) == "union" {
		generation -= 2
	}
	sh.absence.generation.Store(generation)
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		switch *testCaseInput {
		case "object":
			s.AddObject(obj)
		case "condition":
			s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
		case "artifact":
			s.SetArtifact(obj.Named(), &SourceArtifact{})
		case "revision":
			s.SetArtifact(obj.Named(), &SourceArtifact{Revision: "next"})
		case "origin":
			s.SetArtifact(
				obj.Named(),
				&SourceArtifact{Metadata: map[string]string{"org.opencontainers.image.revision": ""}},
			)
		case "union":
			s.SetArtifact(
				obj.Named(),
				&SourceArtifact{
					Revision: "next",
					Metadata: map[string]string{"org.opencontainers.image.revision": ""},
				},
			)
		case "emission":
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: obj}}, Complete: true}, nil)
		}
	}()
	if !panicked || sh.absence.generation.Load() != generation || !reads.Valid() {
		t.Fatalf("panic=%t generation=%d valid=%t", panicked, sh.absence.generation.Load(), reads.Valid())
	}
	{
		assert.FatalIf(
			t,
			s.GetObject(obj.Named()) != nil || s.GetArtifact(obj.Named(), ArtifactContent) != nil,
			"%s",
			"exhaustion partially admitted target",
		)
	}
	{
		_, ok := s.GetStatus(obj.Named())
		assert.FatalIf(t, ok, "%s", "exhaustion partially published status")
	}
	{
		assert.FatalIf(
			t,
			s.GetArtifact(producer.Named(), ArtifactContent) != nil,
			"%s",
			"exhaustion partially published inventory",
		)
	}
}

func testRequireRetainsPredicatesCase(t *testing.T, testCaseInput *[]string,
) {
	t.Helper()

	s := New()
	obj := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
	s.AddObject(obj)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	var want []Gate
	for _, expr := range *testCaseInput {
		ref := manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: new(expr)}
		reads.Require(ref)
		if !slices.ContainsFunc(
			want,
			func(prior Gate) bool { return sameReadinessGate(new(prior.DependencyRef), new(ref)) },
		) {
			want = append(want, Gate{DependencyRef: ref})
		}
	}
	assert.Diff(t, slices.Collect(reads.Gates()), want)
	assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
	{
		assert.FatalIf(
			t,
			reads.inputs != &reads.local || reads.inputs.next != nil,
			"%s",
			"predicates created multiple captured authorities for one identity",
		)
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
}

func testRequireRetainsPredicatesCase2(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	s.AddObject(obj)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	refs := []manifest.DependencyRef{
		{NamedResource: obj.Named()},
		{NamedResource: obj.Named(), ReadyExpr: new("")},
		{NamedResource: obj.Named(), ReadyExpr: new("true")},
		{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true},
		{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, ChartSource: true},
		{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, ArtifactOnly: true},
	}
	if *testCaseInput {
		slices.Reverse(refs)
		reads.RequireMaterializer(obj.Named())
	}
	for _, ref := range refs {
		reads.Require(ref)
	}
	for _, ref := range refs {
		if ref.ReadyExpr != nil {
			ref.ReadyExpr = new(*ref.ReadyExpr)
		}
		reads.Require(ref)
	}
	reads.RequireMaterializer(obj.Named())
	want := make([]Gate, 0, len(refs)+1)
	for _, ref := range refs {
		want = append(want, Gate{DependencyRef: ref})
	}
	materializer := Gate{NamedResource: obj.Named(), Materializer: true}
	if *testCaseInput {
		want = append([]Gate{materializer}, want...)
	} else {
		want = append(want, materializer)
	}
	assert.Diff(t, slices.Collect(reads.Gates()), want)
	assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
	{
		assert.FatalIf(
			t,
			reads.inputs != &reads.local || reads.inputs.next != nil,
			"%s",
			"gate facts created more than one captured authority",
		)
	}
}
