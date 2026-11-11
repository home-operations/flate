package store

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/task"
)

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
	accept(
		t,
		st,
		root.Named(),
		&Emission{Children: []Child{{Obj: producer.Clone()}, emissionChild("sibling", "new")}, Complete: true},
	)
	if st.GetDeclaration(child.Named()) != child || arrivals.Load() != 0 {
		t.Fatalf("unchanged descendant arrivals=%d, want 0", arrivals.Load())
	}
}

func TestGetDeclaration_CoveredAuthority(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"covered", "failed", "empty", "accepted", "accepted failed", "surviving", "withdrawn", "self", "independent", "root"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testGetDeclarationCoveredAuthorityCase(t, &state)
		},
		)
	}
}

func TestRecordEmitted_ReplacedInputRejected(t *testing.T) {
	t.Parallel()
	t.Run("artifact source replacement", func(t *testing.T) {
		t.Parallel()
		st := New()
		input := &manifest.RawObject{
			APIVersion: "source.extensions.fluxcd.io/v1beta1",
			Kind:       "ArtifactGenerator",
			Name:       "producer",
			Namespace:  "apps",
			Spec: map[string]any{
				"sources": []any{map[string]any{"kind": manifest.KindGitRepository, "name": "source"}},
			},
		}
		source := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Name:      "source",
			Namespace: "apps",
		}
		st.AddObject(input)
		st.SetArtifact(source, &SourceArtifact{Key: "first"})
		var reads ReadSet
		reads.Init(st, input.Named(), nil)
		defer reads.Retire()
		_, _, stale := reads.GetEmission(input.Named())
		defer stale.Finish()
		stale.Children, stale.Complete, stale.Fingerprint = []Child{emissionChild("stale", "first")}, true, "stale"
		second := &SourceArtifact{Key: "second"}
		captured, replaced := make(chan struct{}), make(chan struct{})
		tasks := task.NewBounded(2)
		tasks.Go(t.Context(), "publish", func(context.Context) {
			close(captured)
			tasks.YieldSlot(func() { <-replaced })
			st.RecordEmitted(input.Named(), &stale, nil)
			assert.Equal(t, reads.Valid(), false)
		})
		tasks.Go(t.Context(), "replace", func(context.Context) {
			tasks.YieldSlot(func() { <-captured })
			st.SetArtifact(source, second)
			close(replaced)
		})
		tasks.BlockTillDone()
		assert.Equal(t, tasks.Failures(), int64(0))
		_, _, retained := st.GetEmission(input.Named())
		defer retained.Finish()
		assert.Equal(t, len(retained.Children), 0)
		assert.Equal(t, retained.binding.source, source)
		assert.Equal(t, retained.binding.artifact, second)
	})

	for _, route := range []string{"entry_stable", "entry_replacement", "locked_stable", "locked_replacement"} {
		for _, loss := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/loss_%t", route, loss), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedReplacedInputRejectedCase(
					t,
					&testRecordEmittedReplacedInputRejectedCaseInputs{route: &route, loss: &loss},
				)
			},
			)
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
	assert.FatalIf(t, captured != input, "%s", "fixture did not capture accepted child")
	var arrivals atomic.Int64
	stop := st.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == input.Named() {
			arrivals.Add(1)
		}
	}, false)
	defer stop()
	equivalent := input.Clone()
	accept(
		t,
		st,
		parent.Named(),
		&Emission{Children: []Child{{Obj: equivalent}}, Fingerprint: "parent-B", Complete: true},
	)
	if !manifest.Equal(st.GetDeclaration(input.Named()), captured) || arrivals.Load() != 0 {
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
	current := s.GetArtifact(b.Named(), ArtifactContent).(*Emission)
	if current.input != b || current.Fingerprint != first.Fingerprint ||
		s.GetObject(first.Children[0].Obj.Named()) == nil {
		t.Fatalf("equal output retained obsolete input: input=%v fingerprint=%q", current.input, current.Fingerprint)
	}
}

func testGetDeclarationCoveredAuthorityCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	st := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	peer := &manifest.Kustomization{Name: "peer", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "source", Namespace: "apps", URL: "oci://raw.example"}
	st.AddObjects([]manifest.BaseManifest{parent, peer, raw})
	proofs := []manifest.Production{
		{
			Owner:    parent.Named(),
			Source:   manifest.BootstrapSourceID,
			Authored: raw.Named(),
			Path:     "/input/source.yaml",
			File:     "source.yaml",
		},
	}
	switch *testCaseInput {
	case "self":
		proofs = append(proofs, manifest.Production{Owner: raw.Named()})
	case "independent":
		proofs = append(proofs, manifest.Production{Source: manifest.BootstrapSourceID})
	}
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: proofs}},
		},
	)
	st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "other children"})
	var want manifest.BaseManifest
	switch *testCaseInput {
	case "self", "independent":
		want = raw
	case "root":
		accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: raw}}})
		st.AddObject(raw)
		want = raw
	case "failed":
		st.UpdateStatus(
			parent.Named(),
			StatusInfo{Status: StatusFailed, Reason: "", Message: "failed before child"},
		)
	case "empty":
		accept(t, st, parent.Named(), &Emission{Complete: true})
	case "accepted", "accepted failed", "surviving", "withdrawn":
		accepted := *raw
		accepted.URL = "oci://accepted.example"
		accept(t, st, parent.Named(), &Emission{Children: []Child{{Obj: &accepted}}})
		want = &accepted
		if (*testCaseInput) == "accepted failed" {
			st.UpdateStatus(
				parent.Named(),
				StatusInfo{Status: StatusFailed, Reason: "", Message: "failed successor"},
			)
		}
		if (*testCaseInput) == "surviving" {
			accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: &accepted}}})
		}
		if (*testCaseInput) == "surviving" || (*testCaseInput) == "withdrawn" {
			accept(t, st, parent.Named(), &Emission{Complete: true})
		}
		if (*testCaseInput) == "withdrawn" {
			want = nil
		}
	}
	{
		got := st.GetDeclaration(raw.Named())
		assert.FatalIf(t, got != want, "effective=%v want=%v", got, want)
	}
	got, _, inventory := st.GetEmission(raw.Named())
	defer inventory.Finish()
	assert.FatalIf(t, got != want, "emission input=%v want=%v", got, want)
	assert.FatalIf(
		t,
		(*testCaseInput) == "covered" && st.GetObject(raw.Named()) != raw,
		"%s",
		"coverage hid raw admission identity",
	)
}

type testRecordEmittedReplacedInputRejectedCaseInputs struct {
	route *string
	loss  *bool
}

func testRecordEmittedReplacedInputRejectedCase(
	t *testing.T,
	testCaseInput *testRecordEmittedReplacedInputRejectedCaseInputs,
) {
	t.Helper()

	st := New()
	owner, peer := &manifest.Kustomization{
		Name:      "owner",
		Namespace: "apps",
	}, &manifest.Kustomization{
		Name:      "peer",
		Namespace: "apps",
	}
	input := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "A"}
	st.AddObjects([]manifest.BaseManifest{owner, peer, input})
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
			},
		},
	)
	accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: input}}, Complete: true})
	child := emissionChild("prior", "A")
	accept(
		t,
		st,
		input.Named(),
		&Emission{
			Children:  []Child{child},
			Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
			Complete:  true,
		},
	)
	tasks := task.NewBounded(2)
	captured, replaced := make(chan struct{}), make(chan struct{})
	tasks.Go(t.Context(), "publish", func(context.Context) {
		var reads ReadSet
		reads.Init(st, input.Named(), nil)
		defer reads.Retire()
		_, _, next := reads.GetEmission(input.Named())
		defer next.Finish()
		next.Fingerprint = "stale"
		if strings.HasSuffix((*testCaseInput.route), "replacement") {
			next.Children = []Child{emissionChild("stale", "A")}
		}
		sh := st.shardFor(input.Named())
		sh.mu.RLock()
		slot := sh.slots[input.Named()]
		previous := slot.current.Load()
		generation := slot.generation
		sh.mu.RUnlock()
		var proposal *Emission
		if strings.HasPrefix((*testCaseInput.route), "locked_") {
			prepared := make([]admission, 0, len(next.Children))
			cValues := next.Children
			for cIndex := range cValues {
				c := cValues[cIndex]
				prepared = append(
					prepared,
					admission{child: c, targets: manifest.ProducerTargets(c.Obj), binding: next.binding},
				)
			}
			proposal = prepareEmission(
				&next,
				previous,
				prepared,
				make(map[manifest.NamedResource]int, len(prepared)),
			)
		}
		close(captured)
		tasks.YieldSlot(func() { <-replaced })
		result := publicationRejected
		switch *testCaseInput.route {
		case "locked_stable":
			result = st.commitStable(slot, previous, proposal, generation)
		case "locked_replacement":
			result = st.commitReplacement(input.Named(), previous, proposal, generation)
		default:
			st.RecordEmitted(input.Named(), &next, nil)
		}
		assert.ErrorIf(t, result != publicationRejected || reads.Valid(), "%s", "stale inventory accepted")
	})
	tasks.Go(t.Context(), "replace", func(context.Context) {
		tasks.YieldSlot(func() { <-captured })
		if *testCaseInput.loss {
			accept(t, st, peer.Named(), &Emission{Complete: true})
			assert.ErrorIf(
				t,
				st.GetObject(input.Named()) != input || st.GetDeclaration(input.Named()) != nil,
				"%s",
				"fixture did not retain raw input without authority",
			)
		} else {
			replacement := input.Clone()
			replacement.Path = "B"
			accept(t, st, peer.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true})
		}
		close(replaced)
	})
	tasks.BlockTillDone()
	assert.FatalIf(t, tasks.Failures() != 0, "%s", "publication task panicked")
	_, _, retained := st.GetEmission(input.Named())
	defer retained.Finish()
	assert.FatalIf(t, retained.Fingerprint == "stale" || len(retained.Children) != 1 ||
		retained.Children[0].Obj != child.Obj, "%s", "rejection replaced accepted output")
	assert.Diff(t, retained.Children[0].Doc, child.Doc)
	assert.Diff(t, testutil.DocumentMaps(retained.Manifests), []map[string]any{child.Doc})
}
