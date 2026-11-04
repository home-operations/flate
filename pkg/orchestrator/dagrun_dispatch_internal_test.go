package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestDAGDispatch_ResourceSetDedup(t *testing.T) {
	t.Parallel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, nil, true)
	c.Configure(base.Options{})
	c.StartLifecycle()

	rs := &manifest.ResourceSet{Name: "dedup", Namespace: "ns", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: value}
`}
	s.AddObject(rs)
	var arrivals atomic.Int64
	unsub := s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals.Add(1) }, false)
	t.Cleanup(unsub)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c, controllers: base.Controllers{ResourceSet: c}}}
	first := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer first.Assessment.Close()
	if first.Assessment.Outcome() == schedule.Blocked || !first.Assessment.Valid() || arrivals.Load() != 1 {
		t.Fatalf("fresh assessment=%+v arrivals=%d", first.Assessment, arrivals.Load())
	}
	before := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	var reader store.ReadSet
	reader.Init(s, testutil.KustomizationID("ns", "reader"), nil)
	defer reader.Retire()
	reader.GetArtifact(rs.Named(), store.ArtifactContent)
	var statusEvents, artifactEvents atomic.Int64
	unsubStatus := s.AddListener(
		store.EventStatusUpdated,
		func(manifest.NamedResource, any) { statusEvents.Add(1) },
		false,
	)
	defer unsubStatus()
	unsubArtifact := s.AddListener(
		store.EventArtifactUpdated,
		func(manifest.NamedResource, any) { artifactEvents.Add(1) },
		false,
	)
	defer unsubArtifact()
	second := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer second.Assessment.Close()
	after := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	if second.Assessment.Outcome() == schedule.Blocked || !second.Assessment.Valid() || !reader.Valid() ||
		arrivals.Load() != 1 ||
		statusEvents.Load() != 0 ||
		artifactEvents.Load() != 0 ||
		before.Fingerprint != after.Fingerprint ||
		!cmp.Equal(before.Manifests, after.Manifests) {
		t.Fatalf(
			"dedup assessment=%+v reader valid=%t object/status/artifact events=%d/%d/%d",
			second.Assessment,
			reader.Valid(),
			arrivals.Load()-1,
			statusEvents.Load(),
			artifactEvents.Load(),
		)
	}
	s.UpdateStatus(rs.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: ""})
	skipped := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer skipped.Assessment.Close()
	info, _ := s.GetStatus(rs.Named())
	after = s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	if skipped.Assessment.Outcome() == schedule.Blocked || info.Outcome() != store.OutcomeSkipped ||
		info.Message != "" ||
		!reader.Valid() ||
		arrivals.Load() != 1 ||
		before.Fingerprint != after.Fingerprint ||
		!cmp.Equal(before.Manifests, after.Manifests) {
		t.Fatalf("typed skip lost its outcome: status=%+v arrivals=%d", info, arrivals.Load())
	}
}

func TestDAGDispatch_ResourceSetClassification(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"dedup", "fresh fingerprint identical docs", "missing artifact", "empty fingerprint", "blocked gate", "failed gate", "preflight failure", "filter skip", "vanished", "render failure", "suspended", "refresh suspended"} {
		for _, selector := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/selector_%v", name, selector), func(t *testing.T) {
				t.Parallel()
				testDAGDispatchResourceSetClassificationCase(
					t,
					testDAGDispatchResourceSetClassificationCaseInputs{name: &name, selector: &selector},
				)
			},
			)
		}
	}
	for _, mode := range []string{"pending_peer", "ready_without_child", "failed_without_child", "accepted_pending_owner", "self_owned", "rooted"} {
		t.Run("covered/"+mode, func(t *testing.T) {
			t.Parallel()
			testDAGDispatchResourceSetClassificationCase2(
				t,
				&mode,
			)
		},
		)
	}
}

func TestDAGDispatch_ConcurrentResourceSetUpdate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, nil, true)
	c.Configure(base.Options{})
	c.StartLifecycle()

	rs := &manifest.ResourceSet{
		Name:      "concurrent",
		Namespace: "ns",
		Inputs:    []fluxopv1.ResourceSetInput{{}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: initial}
`,
	}
	s.AddObject(rs)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c, controllers: base.Controllers{ResourceSet: c}}}
	d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	s.UpdateStatus(rs.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "queued"})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unsub := s.AddListener(store.EventStatusUpdated, func(nid manifest.NamedResource, _ any) {
		if nid == rs.Named() {
			once.Do(func() {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	}, false)
	defer unsub()
	fresh := *rs
	fresh.ResourcesTemplate = strings.ReplaceAll(rs.ResourcesTemplate, "initial", "changed")
	s.AddObject(&fresh)
	var dispatches atomic.Int64
	dispatch := fixtureDispatcher{
		store: s,
		dispatch: func(ctx context.Context, id schedule.NodeID, level manifest.DrainLevel) schedule.Result {
			result := d.Dispatch(ctx, id, level)
			{
				assert.ErrorIf(
					t,
					dispatches.Add(1) == 1 && result.Assessment.Valid(),
					"%v",
					"superseded declaration retained a valid assessment",
				)
			}
			return result
		},
	}
	scheduler := schedule.New(tasks, dispatch)
	scheduler.Seed([]schedule.NodeID{rs.Named()})
	done := make(chan error, 1)
	go func() { done <- scheduler.Run(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	newest := fresh
	newest.ResourcesTemplate = strings.ReplaceAll(fresh.ResourcesTemplate, "changed", "newest")
	newest.InputsFrom = []fluxopv1.InputProviderReference{
		{
			Kind:     manifest.KindResourceSetInputProvider,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}},
		},
	}
	tasks.Go(ctx, "update", func(context.Context) {
		s.AddObject(&newest)
		close(release)
	})
	{
		{
			err := <-done
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	if dispatches.Load() != 2 {
		t.Fatalf("captured attempts=%d, want 2", dispatches.Load())
	}
	before := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	var reader store.ReadSet
	reader.Init(s, testutil.KustomizationID("ns", "reader"), nil)
	defer reader.Retire()
	reader.GetArtifact(rs.Named(), store.ArtifactContent)
	settled := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer settled.Assessment.Close()
	after := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	{
		assert.FatalIf(
			t,
			settled.Assessment.Outcome() == schedule.Blocked || !settled.Assessment.Valid() || !reader.Valid() ||
				before.Fingerprint != after.Fingerprint ||
				!cmp.Equal(
					before.Manifests,
					after.Manifests,
				),
			"settled newest content did not deduplicate: %+v",
			settled.Assessment,
		)
	}
	cm, ok := s.Get[*manifest.ConfigMap](
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "output"},
	)
	{
		assert.FatalIf(t, !ok || cm.Data["key"] != "newest", "latest concurrent content was lost: %+v", cm)
	}
}

type testDAGDispatchResourceSetClassificationCaseInputs struct {
	name     *string
	selector *bool
}

func testDAGDispatchResourceSetClassificationCase(
	t *testing.T,
	testCaseInput testDAGDispatchResourceSetClassificationCaseInputs,
) {
	t.Helper()

	s := store.New()
	tasks := task.NewBounded(2)
	c := resourceset.New(s, tasks, nil, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	rs := &manifest.ResourceSet{
		Name:      "matrix",
		Namespace: "ns",
		Inputs:    []fluxopv1.ResourceSetInput{{}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}
data: {key: fixed}
`,
	}
	if *testCaseInput.selector {
		rs.InputsFrom = []fluxopv1.InputProviderReference{
			{
				Kind:     manifest.KindResourceSetInputProvider,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}},
			},
		}
	}
	s.AddObject(rs)
	d := dagDispatcher{&Orchestrator{store: s, rsc: c, controllers: base.Controllers{ResourceSet: c}}}
	initial := d.Dispatch(t.Context(), rs.Named(), manifest.DrainNone)
	defer initial.Assessment.Close()
	{
		assert.FatalIf(
			t,
			initial.Assessment.Outcome() == schedule.Blocked || !initial.Assessment.Valid(),
			"fresh setup failed: %+v",
			initial.Assessment,
		)
	}
	before := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)

	opts := base.Options{}
	wantUnresolved := false
	fresh := *rs
	switch *testCaseInput.name {
	case "fresh fingerprint identical docs":
		fresh.Inputs = []fluxopv1.ResourceSetInput{{"unused": &apiextensionsv1.JSON{Raw: []byte(`"new"`)}}}
		s.AddObject(&fresh)
	case "missing artifact":
		s.DeleteObject(rs.Named())
		s.AddObject(rs)
	case "empty fingerprint":
		s.RecordEmitted(rs.Named(), &store.Emission{Manifests: before.Manifests}, nil)
	case "blocked gate", "failed gate":
		fresh.DependsOn = []fluxopv1.Dependency{
			{
				APIVersion: manifest.FluxOperatorDomain + "/v1",
				Kind:       manifest.KindResourceSet,
				Name:       "gate",
				Namespace:  "ns",
			},
		}
		for _, dep := range fresh.DependsOn {
			manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
		}
		s.AddObject(&fresh)
		wantUnresolved = true
		if (*testCaseInput.name) == "failed gate" {
			gate := &manifest.ResourceSet{Name: "gate", Namespace: "ns"}
			s.AddObject(gate)
			s.UpdateStatus(
				gate.Named(),
				store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "gate failed"},
			)
			wantUnresolved = false
		}
	case "preflight failure":
		opts.PreflightFailure = func(manifest.NamedResource) (string, bool) { return "cycle", true }
	case "filter skip":
		opts.Loader = admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   change.NewSet(nil),
				Index:     indexForTest(nil),
				Inventory: s,
				Root:      "",
			},
		)
		admissiontest.Commit(t, opts.Loader, nil)
		opts.Filter = opts.Loader.Options.Filter
	case "vanished":
		s.DeleteObject(rs.Named())
		s.RecordEmitted(rs.Named(), before, nil)
	case "suspended":
		fresh.Suspend = true
		s.AddObject(&fresh)
	case "refresh suspended":
		gate := &manifest.ResourceSet{Name: "gate", Namespace: "ns", Suspend: true}
		s.AddObject(gate)
		fresh.DependsOn = []fluxopv1.Dependency{
			{
				APIVersion: manifest.FluxOperatorDomain + "/v1",
				Kind:       manifest.KindResourceSet,
				Name:       "gate",
				Namespace:  "ns",
			},
		}
		for _, dep := range fresh.DependsOn {
			manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
		}
		s.AddObject(&fresh)
		s.UpdateStatus(
			gate.Named(),
			store.StatusInfo{
				Status:  store.StatusReady,
				Reason:  store.ReasonSuspended,
				Message: "suspended",
			},
		)
		s.UpdateStatus(
			rs.Named(),
			store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "queued"},
		)
		var published atomic.Bool
		unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
			info, ok := payload.(store.StatusInfo)
			if id == rs.Named() && ok && info.String() == "resolving dependencies" &&
				!published.Swap(true) {
				suspended := fresh
				suspended.Suspend = true
				s.AddObject(&suspended)
			}
		}, false)
		t.Cleanup(unsub)
	case "render failure":
		fresh.ResourcesTemplate = "<< .missing.field >>"
		s.AddObject(&fresh)
	}
	testDAGDispatchResourceSetClassificationCaseTail(
		t,
		&testDAGDispatchResourceSetClassificationCaseTailInputs{
			c:              &c,
			s:              &s,
			tasks:          &tasks,
			opts:           &opts,
			d:              &d,
			rs:             &rs,
			testCaseInput:  &testCaseInput,
			wantUnresolved: &wantUnresolved,
			before:         &before,
		},
	)
}

func testDAGDispatchResourceSetClassificationCaseTail(
	t *testing.T,
	tailCaseInput *testDAGDispatchResourceSetClassificationCaseTailInputs,
) {
	t.Helper()
	(*tailCaseInput.c) = resourceset.New((*tailCaseInput.s), (*tailCaseInput.tasks), nil, true)
	(*tailCaseInput.c).
		Configure((*tailCaseInput.opts))
	(*tailCaseInput.c).
		StartLifecycle()
	tailCaseInput.d.
		o.rsc = (*tailCaseInput.c)
	tailCaseInput.d.
		o.controllers.ResourceSet = (*tailCaseInput.c)
	var arrivals, outputArrivals, outputWithdrawals atomic.Int64
	unsub := (*tailCaseInput.s).AddListener(store.EventObjectAdded, func(id manifest.NamedResource, payload any) {
		arrivals.Add(1)
		if id == (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: (*tailCaseInput.rs).Namespace, Name: "output"}) {
			if payload == nil {
				outputWithdrawals.Add(1)
			} else {
				outputArrivals.Add(1)
			}
		}
	}, false)
	t.Cleanup(unsub)
	var result schedule.Result
	if (*tailCaseInput.testCaseInput.name) == "refresh suspended" {
		scheduler := schedule.New(
			(*tailCaseInput.tasks),
			fixtureDispatcher{store: (*tailCaseInput.s), dispatch: tailCaseInput.d.Dispatch},
		)
		scheduler.Seed([]schedule.NodeID{(*tailCaseInput.rs).Named()})
		{
			{
				err := scheduler.Run(t.Context())
				assert.FatalIf(t, err != nil, "%v", err)
			}
		}
		result = scheduler.Results()[(*tailCaseInput.rs).Named()]
	} else {
		result = tailCaseInput.d.Dispatch(t.Context(), (*tailCaseInput.rs).Named(), manifest.DrainNone)
		defer result.Assessment.Close()
	}
	{
		assert.FatalIf(
			t,
			result.Assessment == nil ||
				result.Assessment.Outcome() == schedule.Blocked != (*tailCaseInput.wantUnresolved),
			"assessment=%+v, want unresolved=%t",
			result.Assessment,
			(*tailCaseInput.wantUnresolved),
		)
	}
	if (*tailCaseInput.testCaseInput.name) == "dedup" && arrivals.Load() != 0 {
		t.Fatalf("no-op emitted %d arrivals", arrivals.Load())
	}
	if (*tailCaseInput.testCaseInput.name) == "fresh fingerprint identical docs" {
		after := (*tailCaseInput.s).GetArtifact((*tailCaseInput.rs).Named(), store.ArtifactContent).(*store.Emission)
		{
			assert.FatalIf(
				t, (*tailCaseInput.before).
					Fingerprint == after.Fingerprint, "%v",
				"fresh fingerprint must replace artifact identity",
			)
		}
		{
			{
				diff := cmp.Diff((*tailCaseInput.before).Manifests, after.Manifests)
				assert.FatalIf(
					t,
					diff != "",
					"fixture documents must stay identical (-want +got): %s",
					diff,
				)
			}
		}
	}
	if (*tailCaseInput.testCaseInput.name) == "filter skip" {
		info, ok := (*tailCaseInput.s).GetStatus((*tailCaseInput.rs).Named())
		_, _, retained := (*tailCaseInput.s).GetEmission((*tailCaseInput.rs).Named())
		{
			assert.FatalIf(
				t,
				!ok || info.Outcome() != store.OutcomeUnchanged || info.Message != "unchanged" ||
					len(retained.Children) != 0 ||
					len(retained.Manifests) != 0,
				"filter retained invalid output: %+v",
				info,
			)
		}
	}
	if (*tailCaseInput.testCaseInput.name) == "suspended" ||
		(*tailCaseInput.testCaseInput.name) == "refresh suspended" {
		info, _ := (*tailCaseInput.s).GetStatus((*tailCaseInput.rs).Named())
		_, _, retained := (*tailCaseInput.s).GetEmission((*tailCaseInput.rs).Named())
		output := manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: (*tailCaseInput.rs).Namespace,
			Name:      "output",
		}
		if info.Outcome() != store.OutcomeSuspended || len(retained.Manifests) != 0 ||
			len(retained.Children) != 0 || (*tailCaseInput.s).
			GetObject(output) != nil ||
			outputArrivals.Load() != 0 ||
			outputWithdrawals.Load() != 1 {
			t.Fatalf(
				"suspension retained or replayed stale output: status=%+v arrivals=%d withdrawals=%d",
				info,
				outputArrivals.Load(),
				outputWithdrawals.Load(),
			)
		}
	}
}

func testDAGDispatchResourceSetClassificationCase2(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := store.New()
	tasks := task.NewBounded(2)
	parent := &manifest.ResourceSet{Name: "a-owner", Namespace: "apps"}
	peer := &manifest.ResourceSet{Name: "z-peer", Namespace: "apps"}
	child := &manifest.ResourceSet{
		Name:              "child",
		Namespace:         "apps",
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\n",
	}
	s.AddObjects([]manifest.BaseManifest{parent, peer, child})
	proofs := []manifest.Production{{Owner: parent.Named()}, {Owner: peer.Named()}}
	if (*testCaseInput) == "self_owned" {
		proofs = append(proofs, manifest.Production{Owner: child.Named()})
	}
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{child.Named(): {Productions: proofs}},
		},
	)
	s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(peer.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	switch *testCaseInput {
	case "pending_peer":
		s.UpdateStatus(
			peer.Named(),
			store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing"},
		)
	case "failed_without_child":
		s.UpdateStatus(
			parent.Named(),
			store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "owner failed"},
		)
	case "accepted_pending_owner", "rooted":
		s.RecordEmitted(parent.Named(), &store.Emission{Children: []store.Child{{Obj: child}}}, nil)
		s.UpdateStatus(
			parent.Named(),
			store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing peers"},
		)
		if (*testCaseInput) == "rooted" {
			s.AddObject(child)
		}
	}
	c := resourceset.New(s, tasks, nil, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	result := (dagDispatcher{&Orchestrator{store: s, rsc: c, controllers: base.Controllers{ResourceSet: c}}}).Dispatch(
		t.Context(),
		child.Named(),
		manifest.DrainNone,
	)
	defer result.Assessment.Close()
	run := result.Assessment.(*base.Execution)
	info, _ := s.GetStatus(child.Named())
	switch *testCaseInput {
	case "pending_peer":
		if run.Outcome() != schedule.Blocked ||
			!slices.Equal(run.Blocked, []manifest.NamedResource{peer.Named()}) ||
			s.GetArtifact(child.Named(), store.ArtifactContent) != nil {
			t.Fatalf(
				"pending materializer did not park exact peer: outcome=%v blocked=%v",
				run.Outcome(),
				run.Blocked,
			)
		}
	case "ready_without_child", "failed_without_child":
		{
			assert.FatalIf(t, run.Outcome() != schedule.DependencyFailed ||
				!slices.Equal(info.BlockedBy(), []manifest.NamedResource{parent.Named(), peer.Named()}) ||
				s.GetArtifact(
					child.Named(),
					store.ArtifactContent,
				) != nil, "non-supplying owners lost provenance: %+v", info)
		}
	default:
		{
			assert.FatalIf(t, run.Outcome() != schedule.Terminal || info.Status != store.StatusReady ||
				s.GetArtifact(
					child.Named(),
					store.ArtifactContent,
				) == nil, "effective declaration did not render: %+v", info)
		}
	}
}

type testDAGDispatchResourceSetClassificationCaseTailInputs struct {
	c              **resourceset.Controller
	s              **store.Store
	tasks          **task.Service
	opts           *base.Options
	d              *dagDispatcher
	rs             **manifest.ResourceSet
	testCaseInput  *testDAGDispatchResourceSetClassificationCaseInputs
	wantUnresolved *bool
	before         **store.Emission
}
