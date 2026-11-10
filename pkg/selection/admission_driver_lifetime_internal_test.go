package selection

import (
	"context"
	"errors"
	"maps"
	"runtime"
	"slices"
	"testing"
	"testing/fstest"
	"weak"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmit_LocalPreparationLifetime(t *testing.T) {
	t.Parallel()
	for _, ownership := range []string{"local", "caller"} {
		for _, outcome := range []string{"success", "cancellation", "acquisition error", "expired authority"} {
			t.Run(ownership+"/"+outcome, func(t *testing.T) {
				t.Parallel()
				testAdmitPreparationOutcome(t, ownership, outcome)
			})
		}
	}
	t.Run("accepted data survives caller reuse", testAdmitAcceptedDataSurvivesCallerReuse)
	for _, outcome := range []string{"renew proof", "renew then expire authority"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			testAdmitProofRenewal(t, outcome)
		})
	}
}

func testAdmitPreparationOutcome(t *testing.T, ownership, outcome string) {
	t.Helper()
	session, facts := newLifetimeAdmissionSession(t, Request{Changed: true})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	facts.outcome, facts.cancel = outcome, cancel
	facts.resolveErr = manifest.NewError(manifest.ErrInput, nil, "root acquisition failed")
	origin := &manifest.ConfigMap{Name: "authority", Namespace: "apps"}
	facts.store.AddObject(origin)
	var authority store.ReadSet
	authority.Init(facts.store, origin.Named(), nil)
	defer authority.Retire()
	authority.GetObject(origin.Named())
	facts.authority = origin
	var work manifest.WorkRecorder
	authority.SetWork(&work)
	owner := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "history-owner"}
	child := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "history-child"}
	options := AdmissionOptions{Stage: Construction, Authority: &authority}
	options.EntryHistory.Owner = owner
	options.EntryHistory.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
		child: {ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"payload"}}},
	})
	var preparation *Preparation
	if ownership == "caller" {
		options.Request.Preparation = &preparation
	}
	defer func() { preparation.Retire() }()
	before := session.Snapshot()
	result, accepted, err := session.admitTest(ctx, options)
	assert.Equal(t, accepted, outcome == "success")
	assert.Equal(t, errors.Is(err, context.Canceled), outcome == "cancellation")
	assert.Equal(t, errors.Is(err, facts.resolveErr), outcome == "acquisition error")
	if outcome == "success" || outcome == "expired authority" {
		assert.NoError(t, err)
	}
	assert.Equal(t, facts.resolutions, 1)
	assert.Equal(t, authority.Valid(), outcome != "expired authority")
	if accepted {
		assert.Equal(t, result.Request.Reads == &authority, true)
		assert.Equal(t, result.Request.History[owner].Complete, true)
		assert.Equal(t, len(result.Request.History), 1)
	} else {
		assert.Equal(t, session.Snapshot() == before, true)
		assert.Equal(t, result.Request.Reads == nil, true)
		assert.Equal(t, result.Request.History == nil, true)
		assert.Equal(t, len(result.Objects)+len(result.Needed)+len(result.Demands), 0)
	}
	if ownership == "caller" {
		assert.FatalIf(t, preparation == nil, "caller preparation was not retained")
		assert.Equal(t, preparation.closure.session == session, true)
		assert.Equal(t, preparation.closure.proposal == nil, true)
		assert.Equal(t, preparation.evidence != nil, true)
		if accepted {
			assert.Equal(t, result.Request.Preparation == &preparation, true)
		}
		preparation.Retire()
		assert.Equal(t, preparation.closure.session == nil, true)
		assert.Equal(t, preparation.evidence == nil, true)
	} else {
		assert.Equal(t, result.Request.Preparation == nil, true)
		assert.Equal(t, len(result.Request.Acquired), 0)
	}
	for range facts.store.Sweep() {
	}
	if ownership == "local" || !accepted {
		runtime.GC()
		for _, capture := range facts.captures {
			assert.Equal(t, capture.Value() == nil, true)
		}
	}
	runtime.KeepAlive(result)
	runtime.KeepAlive(session)
	runtime.KeepAlive(&authority)
}

func testAdmitProofRenewal(t *testing.T, outcome string) {
	t.Helper()
	session, facts := newLifetimeAdmissionSession(t, Request{Changed: true})
	origin := &manifest.ConfigMap{Name: "authority", Namespace: "apps"}
	facts.store.AddObject(origin)
	var authority store.ReadSet
	authority.Init(facts.store, origin.Named(), nil)
	defer authority.Retire()
	authority.GetObject(origin.Named())
	qualification := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "qualification"}
	data := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "data"}
	authority.RequireQualification(qualification)
	authority.RequireData(data)
	var work manifest.WorkRecorder
	authority.SetWork(&work)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	facts.outcome, facts.authority, facts.preparation = outcome, origin, &preparation
	options := AdmissionOptions{
		Request: PrepareRequest{Preparation: &preparation}, Stage: Construction, Authority: &authority,
	}
	options.EntryHistory.Owner = origin.Named()
	options.EntryHistory.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
		origin.Named(): {ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"payload"}}},
	})
	result, accepted, err := session.admitTest(t.Context(), options)
	assert.NoError(t, err)
	assert.Equal(t, accepted, outcome == "renew proof")
	assert.Equal(t, authority.Valid(), accepted)
	assert.Equal(t, facts.resolutions, 2)
	assert.FatalIf(t, len(facts.proofs) != 2, "proofs = %v", facts.proofs)
	assert.Equal(t, facts.proofs[0] == facts.proofs[1], false)
	assert.Equal(t, facts.proofs[0].Valid(), false)
	for _, proof := range facts.proofs {
		assert.Equal(t, proof == &authority, false)
		assert.Equal(t, proof.ID(), origin.Named())
		assert.Equal(t, proof.Work() == &work, true)
		assert.Diff(
			t,
			maps.Collect(proof.Required()),
			map[manifest.NamedResource]bool{qualification: true, data: false},
		)
	}
	if accepted {
		assert.Equal(t, result.Request.Reads == &authority, true)
		assert.Equal(t, result.Request.Preparation == &preparation, true)
	} else {
		assert.Equal(t, result.Request.Reads == nil, true)
		assert.Equal(t, result.Request.Preparation == nil, true)
		assert.Equal(t, result.Request.History == nil, true)
	}
}

func testAdmitAcceptedDataSurvivesCallerReuse(t *testing.T) {
	t.Helper()
	t.Parallel()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	session, facts := newLifetimeAdmissionSession(t, Request{Roots: []manifest.NamedResource{parent.Named()}})
	facts.store.AddObject(parent)
	_, retry := publishDecision(t, session, &Admission{
		Needed: []manifest.NamedResource{parent.Named()}, Selected: []manifest.NamedResource{parent.Named()},
		Dirty: []manifest.NamedResource{parent.Named()}, Ordinary: []manifest.NamedResource{parent.Named()},
	}, 0)
	assert.Equal(t, retry, false)
	first := &manifest.ConfigMap{Name: "first", Namespace: "apps"}
	second := &manifest.ConfigMap{Name: "second", Namespace: "apps"}
	historyOwner := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "history-owner"}
	warning := manifest.Warning{Category: manifest.WarnPathConfig, Message: "accepted child", Detail: []string{"first"}}
	firstRecord := manifest.DiscoveryRecord{
		Refs: []manifest.NamedResource{first.Named()}, Productions: []manifest.Production{{Owner: parent.Named()}},
	}
	secondRecord := manifest.DiscoveryRecord{
		Refs: []manifest.NamedResource{second.Named()}, Productions: []manifest.Production{{Owner: parent.Named()}},
	}
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	options := AdmissionOptions{
		Request: PrepareRequest{
			Owner:       parent.Named(),
			Preparation: &preparation,
			Generation: []store.Child{
				{Obj: first, SourceSupport: manifest.BootstrapSourceID, Warnings: []manifest.Warning{warning}},
			},
		},
		Stage: Execution,
	}
	options.EntryHistory.Owner, options.EntryHistory.Records = historyOwner, maps.All(
		map[manifest.NamedResource]manifest.DiscoveryRecord{first.Named(): firstRecord},
	)
	accepted, committed, err := session.admitTest(t.Context(), options)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, accepted.Request.Preparation == &preparation, true)
	assert.Equal(t, slices.Contains(accepted.Needed, first.Named()), true)
	assert.FatalIf(t, len(accepted.Objects) != 1, "accepted objects = %v", accepted.Objects)
	assert.Equal(t, accepted.Objects[0].Obj == first, true)
	assert.Diff(t, accepted.Objects[0].Warnings, []manifest.Warning{warning})
	needed, selected := slices.Clone(accepted.Needed), slices.Clone(accepted.Selected)
	dirty, ordinary := slices.Clone(accepted.Dirty), slices.Clone(accepted.Ordinary)
	blocked, namespaces := slices.Clone(accepted.Blocked), slices.Clone(accepted.Namespaces)
	objects, absent := slices.Clone(accepted.Objects), slices.Clone(accepted.Absent)
	ownerPreparation := preparation
	options.Request.Generation = []store.Child{{Obj: second, SourceSupport: manifest.BootstrapSourceID}}
	options.Request.History = accepted.Request.History
	options.EntryHistory.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
		second.Named(): secondRecord,
	})
	next, committed, err := session.admitTest(t.Context(), options)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, preparation == ownerPreparation, true)
	assert.Equal(t, next.Request.Preparation == &preparation, true)
	assert.Equal(t, slices.Contains(next.Needed, second.Named()), true)
	assert.Equal(t, preparation.closure.session == session, true)
	preparation.Retire()
	assert.Equal(t, preparation.closure.session == nil, true)
	assert.Equal(t, preparation.evidence == nil, true)
	assert.Diff(t, accepted.Needed, needed)
	assert.Diff(t, accepted.Selected, selected)
	assert.Diff(t, accepted.Dirty, dirty)
	assert.Diff(t, accepted.Ordinary, ordinary)
	assert.Diff(t, accepted.Blocked, blocked)
	assert.Diff(t, accepted.Namespaces, namespaces)
	if diff := cmp.Diff(objects, accepted.Objects, cmpopts.IgnoreUnexported(store.Child{})); diff != "" {
		t.Errorf("accepted objects changed (-want +got):\n%s", diff)
	}
	assert.Diff(t, accepted.Objects[0].Warnings, []manifest.Warning{{
		Category: manifest.WarnPathConfig, Message: "accepted child", Detail: []string{"first"},
	}})
	assert.Diff(t, accepted.Absent, absent)
	assert.Diff(t, maps.Collect(accepted.Request.History[historyOwner].Records),
		map[manifest.NamedResource]manifest.DiscoveryRecord{first.Named(): firstRecord})
	assert.Equal(t, accepted.Request.History[historyOwner].Complete, true)
	assert.Diff(t, maps.Collect(next.Request.History[historyOwner].Records),
		map[manifest.NamedResource]manifest.DiscoveryRecord{second.Named(): secondRecord})
}

type lifetimeAdmissionFacts struct {
	*solverFacts

	outcome     string
	cancel      context.CancelFunc
	resolveErr  error
	authority   *manifest.ConfigMap
	resolutions int
	captures    []weak.Pointer[store.ReadSet]
	preparation **Preparation
	proofs      []*store.ReadSet
}

//nolint:gocritic // The fixture MUST retain the census result signature.
func (f *lifetimeAdmissionFacts) ResolveRoot(
	ctx context.Context,
	reads *store.ReadSet,
	source manifest.NamedResource,
	root string,
) (string, bool, error) {
	f.resolutions++
	f.captures = append(f.captures, weak.Make(reads))
	if f.preparation != nil {
		f.proofs = append(f.proofs, (*f.preparation).closure.proposal.Request.Reads)
	}
	switch f.outcome {
	case "cancellation":
		f.cancel()
	case "acquisition error":
		return "", false, f.resolveErr
	case "expired authority":
		f.store.AddObject(manifest.NewConfigMap(f.authority.Named(), map[string]any{"value": "replacement"}))
	case "renew proof", "renew then expire authority":
		if f.resolutions == 1 {
			object := &manifest.ConfigMap{Name: "proof-input", Namespace: "apps"}
			f.proofs[0].GetObject(object.Named())
			reads.GetObject(object.Named())
			f.store.AddObject(object)
		} else if f.outcome == "renew then expire authority" {
			f.store.AddObject(manifest.NewConfigMap(f.authority.Named(), map[string]any{"value": "replacement"}))
		}
	}
	return f.solverFacts.ResolveRoot(ctx, reads, source, root)
}

func newLifetimeAdmissionSession(t *testing.T, request Request) (*Session, *lifetimeAdmissionFacts) {
	t.Helper()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
	facts := &lifetimeAdmissionFacts{solverFacts: &solverFacts{store: st}}
	facts.context.Root = root
	session, err := New(st, facts, request)
	assert.NoError(t, err)
	return session, facts
}
