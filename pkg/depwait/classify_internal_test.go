package depwait

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestClassify_CoreStates(t *testing.T) {
	t.Parallel()

	s := store.New()
	ready := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "ready")}
	failed := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "failed")}
	pending := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "pending")}
	statusless := manifest.DependencyRef{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "ns",
		Name:      "statusless",
	}
	absent := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "absent")}
	s.AddObject(&manifest.Kustomization{Name: ready.Name, Namespace: ready.Namespace})
	s.UpdateStatus(ready.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(failed.NamedResource, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"})
	s.UpdateStatus(pending.NamedResource, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"})
	s.AddObject(&manifest.HelmChartSource{Name: "statusless", Namespace: "ns"})

	w := &Waiter{Store: s}

	{
		got := w.Classify(t.Context(), ready, manifest.DrainNone)
		assert.ErrorIf(t, got.Kind != schedule.Terminal, "ready: got %+v, want schedule.Terminal", got)
	}
	for _, reason := range []store.StatusReason{store.ReasonSkipped, store.ReasonUnchanged, store.ReasonSuspended} {
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()
			testClassifyCoreStatesCase4(
				t,
				testClassifyCoreStatesCase4Inputs{s: &s, ready: &ready, w: &w, reason: &reason},
			)
		},
		)
	}
	{
		got := w.Classify(
			t.Context(),
			failed,
			manifest.DrainNone,
		)
		assert.ErrorIf(t, got.Kind != schedule.DependencyFailed ||
			got.Message != "boom", "failed: got %+v, want schedule.DependencyFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	{
		got := w.Classify(t.Context(), pending, manifest.DrainNone)
		assert.ErrorIf(t, got.Kind != schedule.Blocked, "pending@none: got %+v, want schedule.Blocked", got)
	}
	{
		got := w.Classify(t.Context(), pending, manifest.DrainCascade)
		assert.ErrorIf(
			t,
			got.Kind != schedule.Blocked,
			"pending@cascade: got %+v, want schedule.Blocked (cascade keeps Pending blocked)",
			got,
		)
	}
	{
		got := w.Classify(
			t.Context(),
			pending,
			manifest.DrainForce,
		)
		assert.ErrorIf(t, got.Kind != schedule.DependencyFailed ||
			got.Message != "not ready", "pending@force: got %+v, want schedule.DependencyFailed 'not ready'", got)
	}
	// Present with no Ready condition (a synthetic HelmChart before its source
	// worker's first write) classifies exactly like Pending.
	for level, want := range map[manifest.DrainLevel]Classification{
		manifest.DrainNone:    {Kind: schedule.Blocked},
		manifest.DrainCascade: {Kind: schedule.Blocked},
		manifest.DrainForce:   {Kind: schedule.DependencyFailed, Message: "not ready"},
	} {
		{
			got := w.Classify(t.Context(), statusless, level)
			assert.ErrorIf(
				t,
				!reflect.DeepEqual( //nolint:govet // The complete classification, including its error field, MUST be compared.
					got,
					want,
				),
				"statusless@%v: got %+v, want %+v",
				level,
				got,
				want,
			)
		}
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	{
		got := w.Classify(t.Context(), absent, manifest.DrainNone)
		assert.ErrorIf(t, got.Kind != schedule.Blocked, "absent@none: got %+v, want schedule.Blocked", got)
	}
	{
		got := w.Classify(
			t.Context(),
			absent,
			manifest.DrainCascade,
		)
		assert.ErrorIf(t, got.Kind != schedule.DependencyFailed ||
			got.Message != "dependency not found", "absent@cascade: got %+v, want schedule.DependencyFailed 'dependency not found'", got)
	}
	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			t.Parallel()
			testClassifyCoreStatesCase5(
				t,
				testClassifyCoreStatesCase5Inputs{s: &s, ready: &ready, w: &w, group: &group},
			)
		},
		)
	}
	for _, allow := range []bool{false, true} {
		caseWaiter := *w
		caseWaiter.AllowMissingCRDs = allow
		for _, group := range []string{"apiextensions.k8s.io", "example.test", ""} {
			t.Run(fmt.Sprintf("CRD/group=%s/allow=%t", group, allow), func(t *testing.T) {
				t.Parallel()
				testClassifyCoreStatesCase6(
					t,
					testClassifyCoreStatesCase6Inputs{allow: &allow, caseWaiter: &caseWaiter, group: &group},
				)
			},
			)
		}
	}
}

func TestClassify_ReplaceReadyExpr(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "infra")}
	s.UpdateStatus(dep.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	dep.ReadyExpr = new(`dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`) // not satisfied

	w := &Waiter{Store: s}
	{
		got := w.Classify(t.Context(), dep, manifest.DrainNone)
		assert.ErrorIf(
			t,
			got.Kind != schedule.Blocked,
			"replace ReadyExpr false@none: got %+v, want schedule.Blocked",
			got,
		)
	}
	{
		got := w.Classify(t.Context(), dep, manifest.DrainCascade)
		assert.ErrorIf(t, got.Kind != schedule.DependencyFailed ||
			got.Message != "readyExpr timeout: context deadline exceeded", "replace ReadyExpr false@cascade: got %+v, want schedule.DependencyFailed 'readyExpr timeout: context deadline exceeded'",
			got,
		)
	}
}

func TestClassify_EdgeReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		materializer bool
		status       store.Status
		obj          manifest.BaseManifest
		expr         string
		want         schedule.Kind
	}{
		{name: "ordinary ResourceSet remains existence", obj: &manifest.ResourceSet{Name: "dependency", Namespace: "apps"}, want: schedule.Terminal},
		{name: "materializer ResourceSet pending", obj: &manifest.ResourceSet{Name: "dependency", Namespace: "apps"}, materializer: true, want: schedule.Blocked},
		{name: "materializer ResourceSet ready", obj: &manifest.ResourceSet{Name: "dependency", Namespace: "apps"}, materializer: true, status: store.StatusReady, want: schedule.Terminal},
		{name: "materializer ResourceSet failed", obj: &manifest.ResourceSet{Name: "dependency", Namespace: "apps"}, materializer: true, status: store.StatusFailed, want: schedule.DependencyFailed},
		{name: "ready waits for pending", obj: &manifest.Kustomization{Name: "dependency", Namespace: "apps"}, want: schedule.Blocked},
		{name: "raw Flux group remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: manifest.FluxKustomizeDomain + "/v1", Name: "dependency", Namespace: "apps"}, want: schedule.Terminal},
		{name: "foreign kind remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: "example.com/v1", Name: "dependency", Namespace: "apps"}, want: schedule.Terminal},
		{name: "ConfigMap ignores pending and CEL", obj: &manifest.ConfigMap{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: schedule.Terminal},
		{name: "Secret ignores pending and CEL", obj: &manifest.Secret{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: schedule.Terminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			s.AddObject(tc.obj)
			status := tc.status
			if status == "" {
				status = store.StatusPending
			}
			s.UpdateStatus(tc.obj.Named(), store.StatusInfo{Status: status, Message: "working"})
			w := &Waiter{Store: s, Materializer: tc.materializer}
			got := w.Classify(
				t.Context(),
				manifest.DependencyRef{NamedResource: tc.obj.Named(), ReadyExpr: new(tc.expr)},
				manifest.DrainNone,
			)
			assert.FatalIf(t, got.Kind != tc.want, "classification = %+v, want %v", got, tc.want)
		})
	}
}

func TestClassify_DependencyFailureRetainsCause(t *testing.T) {
	t.Parallel()
	s := store.New()
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	s.AddObject(&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace})
	s.AddObject(&manifest.Kustomization{Name: b.Name, Namespace: b.Namespace})
	failure := &manifest.DependencyFailedError{
		Parent: a,
		Failed: []manifest.NamedResource{b},
		Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{
			b: {Subject: manifest.DiagnosticFor(b, nil), Message: "not ready"},
		},
	}
	s.UpdateFailure(a, failure)
	w := Waiter{Store: s, Parent: b}
	ref := manifest.DependencyRef{NamedResource: a}
	first := w.Classify(t.Context(), ref, manifest.DrainForce)
	assert.FatalIf(t, first.Kind != schedule.DependencyFailed ||
		first.DiagnosticReason(a).String() != a.String()+": "+failure.Error(), "derived failure: %+v", first)
	w.Materializer = true
	{
		got := w.Classify(
			t.Context(),
			ref,
			manifest.DrainForce,
		)
		assert.FatalIf(t, got.Kind != schedule.DependencyFailed ||
			got.Message != "declaration not emitted", "materializer recursively expanded its own consumer failure: %+v", got)
	}
	w.Materializer = false
	nested := &manifest.DependencyFailedError{
		Parent:  b,
		Failed:  []manifest.NamedResource{a},
		Reasons: map[manifest.NamedResource]manifest.DiagnosticReason{a: first.DiagnosticReason(a)},
	}
	s.UpdateFailure(b, nested)
	w.Parent = a
	next := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: b}, manifest.DrainForce)
	assert.FatalIf(
		t,
		next.Kind != schedule.DependencyFailed ||
			next.DiagnosticReason(b).String() != b.String()+": "+nested.Error(),
		"nested failure lost its cause: %+v",
		next,
	)
	{
		got := w.Classify(
			t.Context(),
			manifest.DependencyRef{NamedResource: b},
			manifest.DrainForce,
		)
		assert.FatalIf(
			t,
			!reflect.DeepEqual( //nolint:govet // The complete classification, including its error field, MUST be compared.
				got,
				next,
			),
			"unchanged failure changed: %+v",
			got,
		)
	}
	s.UpdateStatus(a, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "leaf failure"})
	{
		got := w.Classify(t.Context(), ref, manifest.DrainForce)
		assert.FatalIf(t, got.Message != "leaf failure", "direct failure lost its reason: %+v", got)
	}
	owner := testutil.KustomizationID("flux-system", "sync")
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", owner)
	s.AddObject(entry)
	s.UpdateStatus(entry.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "entry failure"})
	captured := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: entry.Named()}, manifest.DrainForce)
	s.AddObject(manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", a))
	reason := captured.DiagnosticReason(entry.Named())
	assert.FatalIf(t, captured.Kind != schedule.DependencyFailed || reason.Subject.ID != entry.Named() ||
		reason.Subject.Resource != owner ||
		reason.Subject.Path != "clusters/test" ||
		reason.Message != "entry failure", "classification lost captured entry context: %+v", reason)
	reason.Subject.Resource.Name = "mutated"
	assert.FatalIf(
		t,
		captured.DiagnosticReason(entry.Named()).Subject.Resource != owner,
		"%v",
		"exposed reason changed the captured subject",
	)
}

func TestClassify_OrdinaryPromotion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		materialize bool
		withdraw    bool
		status      store.Status
		want        Classification
	}{
		{name: "absent", want: Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}},
		{name: "materialized", materialize: true, want: Classification{Kind: schedule.Terminal}},
		{name: "withdrawn", materialize: true, withdraw: true, want: Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}},
		{name: "ready", materialize: true, status: store.StatusReady, want: Classification{Kind: schedule.Terminal}},
		{name: "failed", materialize: true, status: store.StatusFailed, want: Classification{Kind: schedule.DependencyFailed, Message: "boom"}},
		{name: "pending", materialize: true, status: store.StatusPending, want: Classification{Kind: schedule.Blocked}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testClassifyOrdinaryPromotionCase(t, &tc)
		},
		)
	}
}

func TestClassify_AdmittedDependencyRejectsExcludedReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		expr *string
	}{
		{name: "builtin"},
		{name: "cel", expr: new(`dep.status.conditions.exists(c, c.type == "Ready" && c.status == "True")`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			upstream := &manifest.Kustomization{Name: "upstream", Namespace: "apps"}
			s.AddObject(upstream)
			s.SetIncluded(upstream.Named(), false)
			s.UpdateStatus(upstream.Named(), store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged})
			classify := func() schedule.Kind {
				t.Helper()
				var reads store.ReadSet
				reads.Init(s, testutil.KustomizationID("apps", "consumer"), nil)
				defer reads.Retire()
				w := Waiter{Store: &reads}
				return w.Classify(
					t.Context(),
					manifest.DependencyRef{NamedResource: upstream.Named(), ReadyExpr: tc.expr},
					manifest.DrainNone,
				).Kind
			}
			{
				got := classify()
				assert.FatalIf(t, got != schedule.Terminal, "excluded dependency: got %v", got)
			}
			s.SetIncluded(upstream.Named(), true)
			{
				got := classify()
				assert.FatalIf(
					t,
					got != schedule.Blocked,
					"readmitted dependency reused excluded readiness: got %v, want blocked",
					got,
				)
			}
			s.UpdateStatus(upstream.Named(), store.StatusInfo{Status: store.StatusReady})
			{
				got := classify()
				assert.FatalIf(t, got != schedule.Terminal, "completed dependency: got %v", got)
			}
		})
	}
}

type testClassifyCoreStatesCaseInputs struct {
	s      **store.Store
	ready  *manifest.DependencyRef
	w      **Waiter
	reason *store.StatusReason
}

func testClassifyCoreStatesCase(t *testing.T, testCaseInput testClassifyCoreStatesCaseInputs) {
	t.Helper()
	(*testCaseInput.s).UpdateStatus(
		testCaseInput.ready.NamedResource,
		store.StatusInfo{Status: store.StatusReady, Reason: (*testCaseInput.reason), Message: ""},
	)
	{
		got := (*testCaseInput.w).Classify(t.Context(), (*testCaseInput.ready), manifest.DrainNone)
		assert.FatalIf(
			t,
			got.Kind != schedule.Terminal,
			"typed Ready reason %s blocked dependency: %+v", (*testCaseInput.reason), got,
		)
	}
}

type testClassifyCoreStatesCase2Inputs struct {
	s     **store.Store
	ready *manifest.DependencyRef
	w     **Waiter
	group *string
}

func testClassifyCoreStatesCase2(t *testing.T, testCaseInput testClassifyCoreStatesCase2Inputs) {
	t.Helper()

	foreign := (*testCaseInput.ready)
	foreign.Group = (*testCaseInput.group)
	{
		got := (*testCaseInput.w).Classify(t.Context(), foreign, manifest.DrainNone)
		assert.FatalIf(t, got.Kind != schedule.Blocked, "absent lookalike borrowed Ready status: %+v", got)
	}
	{
		got := (*testCaseInput.w).Classify(
			t.Context(),
			foreign,
			manifest.DrainCascade,
		)
		assert.FatalIf(t, got.Kind != schedule.DependencyFailed ||
			got.Message != "dependency not found", "absent lookalike at drain: %+v", got)
	}
	(*testCaseInput.s).UpdateStatus(
		foreign.NamedResource,
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "foreign status"},
	)
	{
		got := (*testCaseInput.w).Classify(t.Context(), foreign, manifest.DrainNone)
		assert.FatalIf(
			t,
			got.Kind != schedule.Terminal,
			"status-only lookalike acquired Flux readiness: %+v",
			got,
		)
	}
}

type testClassifyCoreStatesCase3Inputs struct {
	w     *Waiter
	allow *bool
	group *string
}

func testClassifyCoreStatesCase3(t *testing.T, testCaseInput testClassifyCoreStatesCase3Inputs) {
	t.Helper()

	dep := manifest.DependencyRef{
		Group: (*testCaseInput.group),
		Kind:  manifest.KindCustomResourceDefinition,
		Name:  "widgets.example.test",
	}
	for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
		want := Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}
		if level == manifest.DrainNone {
			want = Classification{Kind: schedule.Blocked}
		} else if (*testCaseInput.allow) && (*testCaseInput.group) == "apiextensions.k8s.io" {
			want = Classification{Kind: schedule.Terminal}
		}
		{
			got := testCaseInput.w.Classify(t.Context(), dep, level)
			assert.FatalIf(
				t,
				!reflect.DeepEqual( //nolint:govet // The complete classification, including its error field, MUST be compared.
					got,
					want,
				),
				"missing CRD at %v: %+v, want %+v",
				level,
				got,
				want,
			)
		}
	}
}

func testClassifyOrdinaryPromotionCase(t *testing.T, testCaseInput *struct {
	name string

	materialize bool
	withdraw    bool
	status      store.Status
	want        Classification
},
) {
	t.Helper()

	st := store.New()
	var data manifest.BaseManifest = &manifest.ConfigMap{Name: "input", Namespace: "apps"}
	if testCaseInput.status != "" {
		data = &manifest.Kustomization{Name: "input", Namespace: "apps"}
		defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
			st.UpdateStatus(id, store.StatusInfo{Status: testCaseInput.status, Reason: "", Message: "boom"})
		}, false)()
	}
	promotion := &inputPromotion{store: st, data: data, materialize: testCaseInput.materialize}
	if testCaseInput.withdraw {
		defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
			st.DeleteObject(id)
		}, false)()
		promotion.before = func() {
			if promotion.calls > 1 {
				promotion.materialize = false
			}
		}
	}
	w := Waiter{Store: st, Existence: promotion}
	got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: data.Named()}, manifest.DrainCascade)
	assert.FatalIf(t, testCaseInput.want.Kind == schedule.DependencyFailed &&
		got.DiagnosticReason(
			data.Named(),
		).Subject != manifest.DiagnosticFor(
			data.Named(),
			data,
		), "%v", "classification lost the captured dependency subject")
	if promotion.calls != 1 ||
		!reflect.DeepEqual( //nolint:govet // The complete classification, including its error field, MUST be compared.
			got,
			testCaseInput.want,
		) {
		t.Fatalf("promotion calls=%d Classification=%+v, want %+v", promotion.calls, got, testCaseInput.want)
	}
}

type testClassifyCoreStatesCase4Inputs struct {
	s      **store.Store
	ready  *manifest.DependencyRef
	w      **Waiter
	reason *store.StatusReason
}

func testClassifyCoreStatesCase4(t *testing.T, testCaseInput testClassifyCoreStatesCase4Inputs) {
	t.Helper()

	testClassifyCoreStatesCase(
		t,
		testClassifyCoreStatesCaseInputs(testCaseInput),
	)
}

type testClassifyCoreStatesCase5Inputs struct {
	s     **store.Store
	ready *manifest.DependencyRef
	w     **Waiter
	group *string
}

func testClassifyCoreStatesCase5(t *testing.T, testCaseInput testClassifyCoreStatesCase5Inputs) {
	t.Helper()

	testClassifyCoreStatesCase2(
		t,
		testClassifyCoreStatesCase2Inputs(testCaseInput),
	)
}

type testClassifyCoreStatesCase6Inputs struct {
	allow      *bool
	caseWaiter *Waiter
	group      *string
}

func testClassifyCoreStatesCase6(t *testing.T, testCaseInput testClassifyCoreStatesCase6Inputs) {
	t.Helper()

	testClassifyCoreStatesCase3(
		t,
		testClassifyCoreStatesCase3Inputs{
			w:     testCaseInput.caseWaiter,
			allow: testCaseInput.allow,
			group: testCaseInput.group,
		},
	)
}
