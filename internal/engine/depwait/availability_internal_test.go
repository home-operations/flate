package depwait

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"

	"github.com/home-operations/flate/internal/engine/schedule"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAvailable_OptionalMaterializerAbsence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, use                                                      string
		status                                                         store.Status
		level                                                          manifest.DrainLevel
		optional, allowed, generated, offline, private, data, producer bool
		want                                                           schedule.Kind
		omit                                                           bool
	}{
		{name: "required substitution absent", use: "substituteFrom", want: schedule.Blocked},
		{name: "required substitution settled absence", use: "substituteFrom", level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "allowed substitution absent", use: "substituteFrom", allowed: true, want: schedule.Terminal, omit: true},
		{name: "allowed substitution settled absence", use: "substituteFrom", allowed: true, level: manifest.DrainCascade, want: schedule.Terminal, omit: true},
		{name: "allowed substitution pending", use: "substituteFrom", allowed: true, producer: true, status: store.StatusPending, want: schedule.Blocked},
		{name: "allowed substitution failed", use: "substituteFrom", allowed: true, producer: true, status: store.StatusFailed, want: schedule.Terminal, omit: true},
		{name: "allowed substitution available", use: "substituteFrom", allowed: true, producer: true, status: store.StatusReady, data: true, want: schedule.Terminal},
		{name: "allowed substitution file waiting", use: "substituteFrom", allowed: true, offline: true, want: schedule.Blocked},
		{name: "allowed substitution file absent", use: "substituteFrom", allowed: true, offline: true, level: manifest.DrainCascade, want: schedule.Terminal, omit: true},
		{name: "optional absent", optional: true, want: schedule.Terminal, omit: true},
		{name: "optional pending", optional: true, producer: true, status: store.StatusPending, want: schedule.Blocked},
		{name: "optional pending cascade", optional: true, producer: true, status: store.StatusPending, level: manifest.DrainCascade, want: schedule.Blocked},
		{name: "optional exhausted", optional: true, producer: true, status: store.StatusPending, level: manifest.DrainForce, want: schedule.Terminal, omit: true},
		{name: "optional failed", optional: true, producer: true, status: store.StatusFailed, want: schedule.Terminal, omit: true},
		{name: "optional finished absent", optional: true, producer: true, status: store.StatusReady, want: schedule.Terminal, omit: true},
		{name: "required failed", producer: true, status: store.StatusFailed, want: schedule.DependencyFailed},
		{name: "required pending", producer: true, status: store.StatusPending, want: schedule.Blocked},
		{name: "required available", producer: true, status: store.StatusReady, data: true, want: schedule.Terminal},
		{name: "failed materializer with available data", producer: true, status: store.StatusFailed, data: true, want: schedule.Terminal},
		{name: "generated strict", generated: true, want: schedule.Terminal, omit: true},
		{name: "generated allowed", generated: true, allowed: true, want: schedule.Terminal, omit: true},
		{name: "required unknown", want: schedule.Blocked},
		{name: "required unknown cascade", level: manifest.DrainCascade, want: schedule.DependencyFailed},
		{name: "allowed unknown", allowed: true, want: schedule.Terminal, omit: true},
		{name: "allowed settled absence", allowed: true, level: manifest.DrainCascade, want: schedule.Terminal, omit: true},
		{name: "private strict", private: true, want: schedule.Blocked},
		{name: "private allowed", private: true, allowed: true, want: schedule.Terminal, omit: true},
		{name: "optional file waiting", optional: true, offline: true, want: schedule.Blocked},
		{name: "optional file absent", optional: true, offline: true, level: manifest.DrainCascade, want: schedule.Terminal, omit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAvailableOptionalMaterializerAbsenceCase(t, &tc)
		},
		)
	}
}

func TestAvailable_RequiredMaterializerCycle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                            string
		transitive, optional, unrelated bool
	}{
		{name: "direct"},
		{name: "transitive", transitive: true},
		{name: "optional", optional: true},
		{name: "unrelated", unrelated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAvailableRequiredMaterializerCycleCase(t, &tc)
		},
		)
	}
}

func testAvailableOptionalMaterializerAbsenceCase(
	t *testing.T,
	testCaseInput *struct {
		name, use string

		status    store.Status
		level     manifest.DrainLevel
		optional  bool
		allowed   bool
		generated bool
		offline   bool
		private   bool
		data      bool
		producer  bool
		want      schedule.
				Kind
		omit bool
	},
) {
	t.Helper()

	s := store.New()
	owner := testutil.KustomizationID("apps", "producer")
	input := store.ReferenceFacts{
		Generated: testCaseInput.generated,
		Offline:   testCaseInput.offline,
		Private:   testCaseInput.private,
	}
	if testCaseInput.producer {
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
		s.UpdateStatus(
			owner,
			store.StatusInfo{Status: testCaseInput.status, Reason: "", Message: "materializer failed"},
		)
		input.Materializers = []manifest.NamedResource{owner}
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	if testCaseInput.data {
		input.Data = &manifest.ConfigMap{
			Name:      id.Name,
			Namespace: id.Namespace,
			Data:      map[string]any{"VALUE": "present"},
		}
		s.AddObject(input.Data.(*manifest.ConfigMap))
	}
	var reads store.ReadSet
	parent := testutil.KustomizationID("apps", "reader")
	reads.Init(s, parent, nil)
	w := Waiter{
		Store: &reads, Parent: parent, Missing: NewMissingPolicy(
			nil),
	}
	if testCaseInput.allowed {
		w.Missing = NewMissingPolicy([]string{manifest.KindConfigMap})
	}
	use := "valuesFrom"
	if testCaseInput.use != "" {
		use = testCaseInput.use
	}
	got := w.Available(
		t.Context(),
		DataDependency{
			NamedResource: id, Optional: testCaseInput.optional, Use: use, Key: "VALUE",
			Input: input,
		},
		testCaseInput.level,
	)
	assert.FatalIf(
		t,
		got.Kind != testCaseInput.want || (got.Result.Effect() == values.Omitted) != testCaseInput.omit,
		"availability=%+v, want kind=%v omit=%t",
		got,
		testCaseInput.want,
		testCaseInput.omit,
	)
	assert.FatalIf(
		t,
		testCaseInput.want == schedule.Blocked && len(got.Blocked) != 1,
		"blocked inputs=%v",
		got.Blocked,
	)
	for required := range reads.Required() {
		t.Fatalf("availability made %s a required failure cause", required)
	}
	assert.FatalIf(
		t,
		got.Kind == schedule.DependencyFailed && got.Reason.Subject.ID != id,
		"materializer failure became target cause: %+v",
		got,
	)
}

func testAvailableRequiredMaterializerCycleCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		transitive bool
		optional   bool
		unrelated  bool
	},
) {
	t.Helper()

	st := store.New()
	parent := testutil.KustomizationID("apps", "consumer")
	owner := testutil.KustomizationID("apps", "producer")
	cause := parent
	if testCaseInput.transitive {
		cause = testutil.KustomizationID("apps", "middle")
	}
	if testCaseInput.unrelated {
		cause = testutil.KustomizationID("apps", "other")
	}
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	st.UpdateFailure(
		owner,
		&manifest.DependencyFailedError{Parent: owner, Failed: []manifest.NamedResource{cause}},
	)
	if testCaseInput.transitive {
		st.UpdateFailure(
			cause,
			&manifest.DependencyFailedError{Parent: cause, Failed: []manifest.NamedResource{parent}},
		)
	}
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	data := &manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "raw"}}
	var reads store.ReadSet
	reads.Init(st, parent, nil)
	w := Waiter{
		Store: &reads, Parent: parent, Missing: NewMissingPolicy(
			nil),
	}
	got := w.Available(t.Context(), DataDependency{
		NamedResource: id, Optional: testCaseInput.optional,
		Input: store.ReferenceFacts{Data: data, Materializers: []manifest.NamedResource{owner}},
	}, manifest.DrainForce)
	want := schedule.DependencyFailed
	if testCaseInput.optional || testCaseInput.unrelated {
		want = schedule.Terminal
	}
	assert.FatalIf(
		t,
		got.Kind != want || (len(got.Failed) != 0) != (want == schedule.DependencyFailed),
		"availability=%+v, want %v",
		got,
		want,
	)
	if testCaseInput.optional || testCaseInput.unrelated {
		assert.Equal(t, got.Result.Effect(), values.Used)
		assert.Equal(t, got.Result.Materialized(), true)
	}
}

func TestAvailable_PartialIndexedProductionArrival(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, use string
		allowed   bool
		effect    values.Effect
	}{
		{name: "required values", use: "valuesFrom", effect: values.Omitted},
		{name: "allowed values", use: "valuesFrom", allowed: true, effect: values.Omitted},
		{name: "allowed substitution", use: "substituteFrom", allowed: true, effect: values.Used},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			st.AddObject(producer)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
			id := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
			declaration := manifest.NamedResource{
				Group:     "external-secrets.io",
				Kind:      "ExternalSecret",
				Namespace: id.Namespace,
				Name:      "cluster",
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{
					{Owner: producer.Named(), Declaration: declaration},
					{Owner: producer.Named(), Authored: id, Path: "input.yaml", File: "input.yaml"},
				}},
			}})
			supplied := &manifest.Secret{
				Name:       id.Name,
				Namespace:  id.Namespace,
				StringData: map[string]any{"first": "supplied"},
			}
			st.AddDeclaration(store.Declaration{Obj: supplied, Role: store.SuppliedDeclaration})
			consumer := testutil.KustomizationID("apps", "consumer")
			var before store.ReadSet
			before.Init(st, consumer, nil)
			defer before.Close()
			facts := before.ReferenceFacts(id, nil, consumer)
			assert.Equal(t, facts.Original, manifest.Data(nil))
			assert.Equal(t, facts.Generated && facts.Offline, true)
			ref := manifest.Reference{NamedResource: id, Key: "second", Use: tc.use}
			if tc.use == "substituteFrom" {
				ref.Key = ""
				facts.Keys = manifest.KeySet{Names: []string{"first", "second"}}
			}
			result := values.Resolve(&values.ResolveRequest{Reference: ref, Facts: facts})
			assert.NoError(t, result.Err())
			assert.Equal(t, result.Effect(), tc.effect)
			firstRef := ref
			firstRef.Key, firstRef.Use = "first", "valuesFrom"
			first := values.Resolve(&values.ResolveRequest{Reference: firstRef, Facts: facts})
			assert.NoError(t, first.Err())
			assert.Equal(t, first.Effect(), values.Used)
			assert.Equal(t, first.Value(), "supplied")
			var kinds []string
			if tc.allowed {
				kinds = []string{manifest.KindSecret}
			}
			w := Waiter{Store: &before, Parent: consumer, Missing: NewMissingPolicy(kinds)}
			waiting := w.Available(t.Context(), DataDependency{Reference: ref, Input: facts}, manifest.DrainNone)
			assert.Equal(t, waiting.Kind, schedule.Blocked)
			assert.Diff(t, waiting.Blocked, []manifest.NamedResource{producer.Named()})
			release, published := make(chan struct{}), make(chan struct{})
			go func() {
				<-release
				data := &manifest.Secret{
					Name:       id.Name,
					Namespace:  id.Namespace,
					StringData: map[string]any{"second": "materialized"},
				}
				st.RecordEmitted(
					producer.Named(),
					&store.Emission{Children: []store.Child{{Obj: data}}, Complete: true},
					nil,
				)
				st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
				close(published)
			}()
			close(release)
			<-published
			assert.Equal(t, before.Valid(), false)
			var after store.ReadSet
			after.Init(st, consumer, nil)
			defer after.Close()
			facts = after.ReferenceFacts(id, nil, consumer)
			w.Store = &after
			ready := w.Available(t.Context(), DataDependency{Reference: ref, Input: facts}, manifest.DrainNone)
			assert.Equal(t, ready.Kind, schedule.Terminal)
			assert.Equal(t, ready.Result.Effect() == values.Omitted, false)
			if tc.use == "valuesFrom" {
				assert.Equal(t, ready.Result.Value(), "materialized")
			}
			materialized, err := ready.Result.Data().Value("second")
			assert.NoError(t, err)
			assert.Equal(t, materialized.Found, true)
			assert.Equal(t, materialized.Value, "materialized")
			firstValue, err := ready.Result.Data().Value("first")
			assert.NoError(t, err)
			assert.Equal(t, firstValue.Value, "supplied")
		})
	}
}

func TestAvailable_AcceptedDataDoesNotCaptureProducerReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		key      string
		optional bool
		want     schedule.Kind
		omit     bool
	}{
		{name: "readable", key: "VALUE", want: schedule.Terminal},
		{name: "optional missing key", key: "absent", optional: true, want: schedule.Terminal, omit: true},
		{name: "required missing key", key: "absent", want: schedule.DependencyFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "readable"}}
			parent := testutil.HelmReleaseID("apps", "consumer")
			st.AddObject(producer)
			st.RecordEmitted(
				producer.Named(),
				&store.Emission{Children: []store.Child{{Obj: data}}, Complete: true},
				nil,
			)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			var reads store.ReadSet
			reads.Init(st, parent, nil)
			defer reads.Close()
			input := reads.ReferenceFacts(data.Named(), nil, parent)
			assert.Equal(t, input.Accepted, true)
			assert.Diff(t, input.Materializers, []manifest.NamedResource{producer.Named()})
			w := Waiter{Store: &reads, Parent: parent, Missing: NewMissingPolicy(nil)}
			result := w.Available(t.Context(), DataDependency{
				NamedResource: data.Named(), Key: tc.key, Use: "valuesFrom", Optional: tc.optional, Input: input,
			}, manifest.DrainNone)
			assert.Equal(t, result.Kind, tc.want)
			assert.Equal(t, result.Result.Effect() == values.Omitted, tc.omit)
			assert.NoError(t, result.Err)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
			assert.Equal(t, reads.Valid(), true)
			st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestAvailable_OriginalDataDoesNotCaptureProducerReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		key      string
		optional bool
		want     schedule.Kind
		omit     bool
	}{
		{name: "readable", key: "VALUE", want: schedule.Terminal},
		{name: "optional missing key", key: "absent", optional: true, want: schedule.Terminal, omit: true},
		{name: "required missing key", key: "absent", want: schedule.DependencyFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			data := &manifest.ConfigMap{Name: "input", Namespace: "apps", Data: map[string]any{"VALUE": "readable"}}
			parent := testutil.HelmReleaseID("apps", "consumer")
			st.AddObject(producer)
			st.AddObject(data)
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				data.Named(): {Productions: []manifest.Production{
					{Owner: producer.Named(), Authored: data.Named(), Path: "input.yaml"},
					{Authored: data.Named(), Path: "input.yaml"},
				}},
			}})
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			var reads store.ReadSet
			reads.Init(st, parent, nil)
			defer reads.Close()
			input := reads.ReferenceFacts(data.Named(), nil, parent)
			assert.Equal(t, input.Accepted, false)
			assert.Equal(t, input.Original, manifest.Data(data))
			assert.Diff(t, input.Materializers, []manifest.NamedResource{producer.Named()})
			w := Waiter{Store: &reads, Parent: parent, Missing: NewMissingPolicy(nil)}
			result := w.Available(t.Context(), DataDependency{
				NamedResource: data.Named(), Key: tc.key, Use: "valuesFrom", Optional: tc.optional, Input: input,
			}, manifest.DrainNone)
			assert.Equal(t, result.Kind, tc.want)
			assert.Equal(t, result.Result.Effect() == values.Omitted, tc.omit)
			assert.NoError(t, result.Err)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
			assert.Equal(t, reads.Valid(), true)
			st.DeleteObject(data.Named())
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestAvailable_CapturesMaterializerFailures(t *testing.T) {
	t.Parallel()
	st := store.New()
	parent := testutil.KustomizationID("apps", "consumer")
	producers := []manifest.NamedResource{
		testutil.KustomizationID("apps", "first"),
		testutil.KustomizationID("apps", "second"),
	}
	for _, id := range producers {
		st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		st.UpdateFailure(id, &manifest.DependencyFailedError{Parent: id, Failed: []manifest.NamedResource{parent}})
	}
	var reads store.ReadSet
	reads.Init(st, parent, nil)
	defer reads.Close()
	waiter := Waiter{Store: &reads, Parent: parent, Missing: NewMissingPolicy(nil)}
	target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "input"}
	got := waiter.Available(
		t.Context(),
		DataDependency{
			NamedResource: target,
			Use:           "substituteFrom",
			Input:         store.ReferenceFacts{Generated: true, Materializers: producers},
		},
		manifest.DrainForce,
	)
	assert.Equal(t, got.Kind, schedule.DependencyFailed)
	captured, ok := errors.AsType[*manifest.DependencyFailedError](got.Result.Err())
	assert.Equal(t, ok, true)
	assert.Diff(t, captured.Failed, producers)
	assert.Diff(t, got.Failed, producers)
	for _, id := range producers {
		assert.Equal(t, captured.Reasons[id].Subject.ID, id)
	}
}
