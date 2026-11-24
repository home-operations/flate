package base

import (
	"context"
	"errors"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDataInputs_ProbeRequiresPromotion(t *testing.T) {
	t.Parallel()
	for _, object := range []manifest.BaseManifest{
		&manifest.Kustomization{
			Name: "consumer", Namespace: "apps",
			PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "input"},
			}},
		},
		&manifest.HelmRelease{
			Name: "consumer", Namespace: "apps",
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "input"}},
		},
	} {
		t.Run(object.Named().Kind, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(object)
			input := &manifest.ConfigMap{
				Name:      "input",
				Namespace: "apps",
				Data:      map[string]any{"VALUE": "same", "values.yaml": "{}"},
			}
			calls := 0
			c := New(st, nil)
			c.Configure(Options{Existence: &gatePromotion{promote: func(id manifest.NamedResource) bool {
				assert.Equal(t, id, input.Named())
				calls++
				st.AddObject(input)
				return true
			}}})
			var probe Execution
			probe.InitProbe(c, object)
			defer probe.Close()
			inputs, err := probe.DataInputs(
				t.Context(),
				object,
				values.NewStoreProvider(&probe.Reads, probe.Reads.ID()),
			)
			assert.Equal(t, err != nil, true)
			assert.Equal(t, len(inputs), 0)
			assert.Equal(t, calls, 0)
			assert.Equal(t, st.GetObject(input.Named()) == nil, true)
			_, statusExists := st.GetStatus(object.Named())
			assert.Equal(t, statusExists, false)
			assert.Equal(t, len(st.Warnings()), 0)
			assert.Equal(t, probe.Reads.Valid(), true)

			var normal Execution
			normal.Init(c, object.Named())
			defer normal.Close()
			inputs, err = normal.DataInputs(
				t.Context(),
				object,
				values.NewStoreProvider(&normal.Reads, normal.Reads.ID()),
			)
			assert.NoError(t, err)
			assert.Equal(t, len(inputs), 1)
			assert.Equal(t, inputs[0].Input.Data, manifest.Data(input))
			assert.Equal(t, calls, 1)
			assert.Equal(t, probe.Reads.Valid(), false)
			resumed := false
			assert.NoError(t, normal.AwaitData(t.Context(), DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
					resumed = true
					return nil
				},
			}, manifest.DrainNone))
			assert.Equal(t, resumed, true)
			probe.Close()
			probe.Init(c, object.Named())
			inputs, err = probe.DataInputs(t.Context(), object, values.NewStoreProvider(&probe.Reads, probe.Reads.ID()))
			assert.NoError(t, err)
			assert.Equal(t, len(inputs), 1)
			assert.Equal(t, calls, 2)
		})
	}
}

func TestAwaitData_ProbeTracksMaterializerEvidence(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{
		Name: "consumer", Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "input", Optional: true},
		}},
	}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	source := (&manifest.GitRepository{Name: "repo", Namespace: "apps"}).Named()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: "apps"}
	for _, object := range []manifest.BaseManifest{owner, producer} {
		st.AddObject(object)
	}
	st.SetArtifact(source, &store.SourceArtifact{Content: "before"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input: {Productions: []manifest.Production{{Owner: producer.Named(), Source: source}}},
	}})
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	c := New(st, nil)
	assess := func() (*Execution, error) {
		e := new(Execution)
		e.InitProbe(c, owner)
		t.Cleanup(e.Close)
		inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
		assert.NoError(t, err)
		err = e.AwaitData(t.Context(), DataWait{
			Inputs: inputs,
			Resume: func(_ context.Context, _ manifest.DrainLevel, omitted []values.Result) error {
				assert.Equal(t, len(omitted), 1)
				assert.Equal(t, omitted[0].ID(), input)
				return nil
			},
		}, manifest.DrainNone)
		return e, err
	}
	first, err := assess()
	blocked, ok := errors.AsType[*depwait.BlockedError](err)
	assert.Equal(t, ok, true)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer.Named()})
	assert.Equal(t, first.Reads.Valid(), true)
	st.SetArtifact(source, &store.SourceArtifact{Content: "after"})
	assert.Equal(t, first.Reads.Valid(), false)

	second, err := assess()
	_, ok = errors.AsType[*depwait.BlockedError](err)
	assert.Equal(t, ok, true)
	assert.Equal(t, second.Reads.Valid(), true)
	st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
	assert.Equal(t, second.Reads.Valid(), false)
	settled, err := assess()
	assert.NoError(t, err)
	assert.Equal(t, settled.Reads.Valid(), true)
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	assert.Equal(t, settled.Reads.Valid(), false)
	_, err = assess()
	blocked, ok = errors.AsType[*depwait.BlockedError](err)
	assert.Equal(t, ok, true)
	assert.Diff(t, blocked.Deps, []manifest.NamedResource{producer.Named()})
	_, statusExists := st.GetStatus(owner.Named())
	assert.Equal(t, statusExists, false)
	assert.Equal(t, len(st.Warnings()), 0)
	assert.Equal(t, st.GetArtifact(owner.Named(), store.ArtifactContent) == nil, true)
}

func TestDataInputs_PreflightPrecedesCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "input", Optional: true},
		},
	}}
	st.AddObject(owner)
	st.SetPreflight(owner.Named(), store.PreflightFailure{Message: "captured admission failure"})
	var e Execution
	e.Init(New(st, nil), owner.Named())
	defer e.Close()
	inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, len(inputs), 0)
	_, statusKnown := st.GetStatus(owner.Named())
	assert.Equal(t, statusKnown, false)
	st.AddObject(&manifest.ConfigMap{Name: "input", Namespace: owner.Namespace})
	assert.Equal(t, e.Reads.Valid(), true)
}

func TestDataInputs_ProbeRequiresRawProductionMaterialization(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
		SubstituteFrom: []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "input", Optional: true},
		},
	}}
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "input", Namespace: owner.Namespace}
	st.AddObject(owner)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input: {Productions: []manifest.Production{{Path: "input.yaml", Source: manifest.BootstrapSourceID}}},
	}})
	var e Execution
	e.InitProbe(New(st, nil), owner)
	defer e.Close()
	inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
	assert.Equal(t, errors.Is(err, errIncompleteHistory), true)
	assert.Equal(t, len(inputs), 0)
	assert.Equal(t, st.GetObject(input) == nil, true)
	_, statusKnown := st.GetStatus(owner.Named())
	assert.Equal(t, statusKnown, false)
}

func TestAwaitData_ReadableSupplyDoesNotRequireProducer(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	for _, object := range []manifest.BaseManifest{owner, producer} {
		st.AddObject(object)
	}
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	secret := &manifest.Secret{Name: "input", Namespace: "apps", StringData: map[string]any{"value": "supplied"}}
	st.AddDeclaration(store.Declaration{Obj: secret, Origin: manifest.SuppliedDeclaration})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		secret.Named(): {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}})
	var execution Execution
	execution.Init(New(st, nil), owner.Named())
	defer execution.Close()
	facts := execution.Reads.ReferenceFacts(secret.Named(), nil, owner.Named())
	assert.Diff(t, facts.Materializers, []manifest.NamedResource{producer.Named()})
	resumed := false
	assert.NoError(t, execution.AwaitData(t.Context(), DataWait{
		Inputs: []depwait.DataDependency{
			{NamedResource: secret.Named(), Key: "value", Use: "valuesFrom", Input: facts},
		},
		Resume: func(context.Context, manifest.DrainLevel, []values.Result) error { resumed = true; return nil },
	}, manifest.DrainNone))
	assert.Equal(t, resumed, true)
	for required := range execution.Reads.Required() {
		assert.Equal(t, required == producer.Named(), false)
	}
}

func TestAwaitData_EncryptedSubstitutionProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		optional bool
		allowed  []string
	}{
		{name: "required mixed input"},
		{name: "permitted mixed input", allowed: []string{manifest.KindSecret}},
		{name: "optional mixed input", optional: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
				SubstituteFrom: []kustomizev1.SubstituteReference{
					{Kind: manifest.KindSecret, Name: "input", Optional: tc.optional},
				},
			}}
			input := &manifest.Secret{
				Name:      "input",
				Namespace: "apps",
				StringData: map[string]any{
					"PLAIN":    "readable",
					"SUPPLIED": "",
					"ALPHA":    "ENC[AES256_GCM,data:alpha,iv:y,tag:z,type:str]",
					"ZETA":     "ENC[AES256_GCM,data:zeta,iv:y,tag:z,type:str]",
				},
			}
			st.AddObject(owner)
			st.AddDeclaration(store.Declaration{Obj: input})
			c := New(st, nil)
			c.Configure(Options{Missing: depwait.NewMissingPolicy(tc.allowed)})
			var e Execution
			e.Init(c, owner.Named())
			defer e.Close()
			inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
			assert.NoError(t, err)
			assert.Equal(t, len(e.Unreadable), 1)
			resumed := false
			assert.NoError(t, e.AwaitData(t.Context(), DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
					resumed = true
					return nil
				},
			}, manifest.DrainNone))
			assert.Equal(t, resumed, true)
			assert.Equal(t, len(st.Warnings()), 0)
			e.Reads.Accept(store.AssessmentReconciled)
			assert.Equal(t, len(st.Warnings()), 0)
			assert.Equal(t, inputs[0].Result.Effect(), values.Used)
			assert.Diff(t, inputs[0].Result.UnreadableKeys(), []string{"ALPHA", "ZETA"})
		})
	}
}

func TestAwaitData_OrderedAssessments(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	owner.ValuesFrom = []manifest.ValuesReference{
		{Kind: manifest.KindConfigMap, Name: "input", ValuesKey: "first"},
		{Kind: manifest.KindSecret, Name: "absent", Optional: true},
		{Kind: manifest.KindConfigMap, Name: "input", ValuesKey: "last"},
	}
	data := &manifest.ConfigMap{
		Name:      "input",
		Namespace: "apps",
		Data:      map[string]any{"first": "a: first", "last": "a: last"},
	}
	for _, object := range []manifest.BaseManifest{owner, data} {
		st.AddObject(object)
	}
	var e Execution
	e.Init(New(st, task.NewBounded(2)), owner.Named())
	defer e.Close()
	inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
	assert.NoError(t, err)
	resumed := false
	assert.NoError(t, e.AwaitData(t.Context(), DataWait{
		Inputs: inputs,
		Resume: func(_ context.Context, _ manifest.DrainLevel, results []values.Result) error {
			resumed = true
			assert.Equal(t, len(results), 3)
			for position := range results {
				assert.Equal(t, results[position].Position(), position)
			}
			assert.Equal(t, results[0].Effect(), values.Used)
			assert.Equal(t, results[0].Value(), "a: first")
			assert.Equal(t, results[1].Effect(), values.Omitted)
			assert.Equal(t, results[2].Effect(), values.Used)
			assert.Equal(t, results[2].Value(), "a: last")
			return nil
		},
	}, manifest.DrainNone))
	assert.Equal(t, resumed, true)
	e.Reads.Accept(store.AssessmentReconciled)
	assert.Equal(t, len(st.Warnings()), 0)
}

func TestAwaitData_MalformedMaterialReturnsDirectly(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		missingFirst bool
	}{
		{name: "single malformed input"},
		{name: "missing then malformed input", missingFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{
				Name: "consumer", Namespace: "apps",
				PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "input"},
				}},
			}
			if tc.missingFirst {
				owner.PostBuild.SubstituteFrom = append([]kustomizev1.SubstituteReference{{
					Kind: manifest.KindConfigMap, Name: "absent",
				}}, owner.PostBuild.SubstituteFrom...)
			}
			for _, object := range []manifest.BaseManifest{owner, &manifest.ConfigMap{
				Name: "input", Namespace: "apps",
				Data: map[string]any{"VALUE": map[string]any{"nested": "invalid"}},
			}} {
				st.AddObject(object)
			}
			var e Execution
			e.Init(New(st, task.NewBounded(2)), owner.Named())
			defer e.Close()
			inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
			assert.NoError(t, err)
			assert.Equal(t, len(inputs), len(owner.PostBuild.SubstituteFrom))
			assert.FatalIf(
				t,
				inputs[len(inputs)-1].Result.MaterialError() == nil,
				"expected malformed material before settlement",
			)
			resumed := false
			err = e.AwaitData(t.Context(), DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
					resumed = true
					return nil
				},
			}, manifest.DrainForce)
			assert.Equal(t, resumed, false)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			domain, ok := errors.AsType[*manifest.Error](err)
			assert.FatalIf(t, !ok, "expected direct material error, got %T: %v", err, err)
			assert.Equal(t, err, error(domain))
			assert.Equal(t, domain.Category(), manifest.ErrInput)
			assert.Equal(t, domain.Error(), "data[VALUE]: unsupported value type map[string]interface {}")
			assert.Equal(t, domain.Cause().Error(), "data[VALUE]: unsupported value type map[string]interface {}")
			_, aggregate := errors.AsType[*manifest.DependencyFailedError](err)
			assert.Equal(t, aggregate, false)
		})
	}
}

func TestAwaitData_FailedAndPendingInputs(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		level manifest.DrainLevel
	}{
		{name: "pending producer", level: manifest.DrainCascade},
		{name: "forced producer failure", level: manifest.DrainForce},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "missing"}
			produced := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "produced"}
			for _, object := range []manifest.BaseManifest{owner, producer} {
				st.AddObject(object)
			}
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
			resumed := false
			assess := func() error {
				var e Execution
				e.Init(New(st, task.NewBounded(2)), owner.Named())
				defer e.Close()
				return e.AwaitData(t.Context(), DataWait{
					Inputs: []depwait.DataDependency{
						{NamedResource: missing},
						{NamedResource: produced, Input: store.ReferenceFacts{
							Materializers: []manifest.NamedResource{producer.Named()},
						}},
					},
					Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
						resumed = true
						return nil
					},
				}, tc.level)
			}
			err := assess()
			assert.Equal(t, resumed, false)
			if tc.level < manifest.DrainForce {
				pending, ok := errors.AsType[*pendingDataError](err)
				assert.FatalIf(t, !ok, "expected pending settlement, got %v", err)
				assert.Diff(t, pending.assessment.Blocked, []manifest.NamedResource{producer.Named()})
				assert.Diff(t, pending.assessment.Failed.Failed, []manifest.NamedResource{missing})
				_, terminal := errors.AsType[*manifest.DependencyFailedError](err)
				assert.Equal(t, terminal, false)
			} else {
				failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
				assert.FatalIf(t, !ok, "expected dependency failure, got %v", err)
				assert.Diff(t, failed.Failed, []manifest.NamedResource{missing, produced})
				assert.Diff(t, failed.Reasons[produced], manifest.DiagnosticReason{
					Subject: manifest.DiagnosticFor(produced, nil), Message: "not found; supply a readable document",
					Causes: []manifest.DiagnosticReason{{
						Subject: manifest.DiagnosticFor(producer.Named(), producer), Message: "not ready",
					}},
				})
			}
			st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
			st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
			err = assess()
			assert.Equal(t, resumed, false)
			failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
			assert.FatalIf(t, !ok, "expected both absent inputs after producer settlement, got %v", err)
			assert.Diff(t, failed.Failed, []manifest.NamedResource{missing, produced})
			for _, id := range failed.Failed {
				assert.Diff(t, failed.Reasons[id], manifest.DiagnosticReason{
					Subject: manifest.DiagnosticFor(id, nil), Message: "not found; supply a readable document",
				})
			}
		})
	}
	t.Run("retained assessment", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
		producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
		missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "missing"}
		produced := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "produced"}
		for _, object := range []manifest.BaseManifest{owner, producer} {
			st.AddObject(object)
		}
		st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
		var e Execution
		e.Init(New(st, task.NewBounded(2)), owner.Named())
		defer e.Close()
		resumed := false
		err := e.AwaitData(t.Context(), DataWait{
			Inputs: []depwait.DataDependency{
				{NamedResource: missing, Use: "substituteFrom"},
				{NamedResource: produced, Use: "substituteFrom", Optional: true, Input: store.ReferenceFacts{
					Materializers: []manifest.NamedResource{producer.Named()},
				}},
			},
			Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
				resumed = true
				return nil
			},
		}, manifest.DrainCascade)
		pending, ok := errors.AsType[*pendingDataError](err)
		assert.FatalIf(t, !ok, "expected retained assessment, got %T: %v", err, err)
		assert.Equal(t, e.Reads.Valid(), true)
		err = pending.Settle(t.Context(), manifest.DrainCascade)
		next, ok := errors.AsType[*pendingDataError](err)
		assert.FatalIf(t, !ok, "expected pending settlement, got %T: %v", err, err)
		assert.Equal(t, next, pending)
		assert.Equal(t, e.Reads.Valid(), true)
		err = pending.Settle(t.Context(), manifest.DrainForce)
		assert.Equal(t, resumed, false)
		domain, ok := errors.AsType[*manifest.Error](err)
		assert.FatalIf(t, !ok, "expected sole input error, got %T: %v", err, err)
		assert.Equal(t, err, error(domain))
		failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
		assert.FatalIf(t, !ok, "expected missing input error, got %T: %v", err, err)
		assert.Diff(t, failed.Failed, []manifest.NamedResource{missing})
		assert.Equal(
			t,
			err.Error(),
			"dependencies failed: ConfigMap/apps/missing: not found; supply a readable document",
		)
	})
}

func TestAwaitData_GateFailureWithDataInputs(t *testing.T) {
	t.Parallel()
	const (
		gateMessage = "dependencies failed: Kustomization.kustomize.toolkit.fluxcd.io/apps/gate: gate render failed"
		firstCause  = "dependencies failed: ConfigMap/apps/first-vars: not found; supply a readable document"
		secondCause = "dependencies failed: ConfigMap/apps/second-vars: not found; supply a readable document"
		combined    = "dependencies failed: ConfigMap/apps/first-vars: not found; supply a readable document; ConfigMap/apps/second-vars: not found; supply a readable document; Kustomization.kustomize.toolkit.fluxcd.io/apps/gate: gate render failed"
	)
	for _, tc := range []struct {
		name         string
		failedCount  int
		inputMessage string
		message      string
		details      string
	}{
		{
			name: "healthy inputs", message: gateMessage,
			details: gateMessage + "\n  input error",
		},
		{
			name: "one failed input", failedCount: 1,
			message:      firstCause + "; Kustomization.kustomize.toolkit.fluxcd.io/apps/gate: gate render failed",
			inputMessage: "not found; supply a readable document",
			details: strings.Join([]string{
				firstCause + "; Kustomization.kustomize.toolkit.fluxcd.io/apps/gate: gate render failed",
				"  " + firstCause + "; Kustomization.kustomize.toolkit.fluxcd.io/apps/gate: gate render failed", firstCause,
				"    " + firstCause + "; Kustomization.kustomize.toolkit.fluxcd.io/apps/gate: gate render failed", "      input error",
				"    " + firstCause, "      " + firstCause, "        " + firstCause, "          input error",
			}, "\n"),
		},
		{
			name: "two failed inputs", failedCount: 2, message: combined,
			inputMessage: "not found; supply a readable document",
			details: strings.Join([]string{
				combined,
				"  " + combined, firstCause, secondCause,
				"    " + combined, "      input error",
				"    " + firstCause, secondCause,
				"      " + firstCause, "        " + firstCause, "          input error",
				"      " + secondCause, "        " + secondCause, "          input error",
			}, "\n"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
			gate := &manifest.Kustomization{Name: "gate", Namespace: "apps"}
			healthy := &manifest.ConfigMap{Name: "healthy", Namespace: "apps", Data: map[string]any{"VALUE": "ready"}}
			for _, object := range []manifest.BaseManifest{owner, gate, healthy} {
				st.AddObject(object)
			}
			st.UpdateStatus(gate.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "gate render failed"})
			var e Execution
			e.Init(New(st, task.NewBounded(2)), owner.Named())
			defer e.Close()
			gateErr := e.Require(
				t.Context(),
				[]manifest.DependencyRef{{NamedResource: gate.Named()}},
				manifest.DrainForce,
			)
			assert.Equal(t, gateErr != nil, true)
			inputs := make([]depwait.DataDependency, 1, 1+tc.failedCount)
			inputs[0] = depwait.DataDependency{
				NamedResource: healthy.Named(),
				Input:         store.ReferenceFacts{Data: manifest.Data(healthy)},
			}
			ids := make([]manifest.NamedResource, 0, 1+tc.failedCount)
			for _, name := range []string{"first-vars", "second-vars"}[:tc.failedCount] {
				id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}
				inputs = append(inputs, depwait.DataDependency{NamedResource: id, Use: "substituteFrom"})
				ids = append(ids, id)
			}
			ids = append(ids, gate.Named())
			resumed := false
			err := e.AwaitData(t.Context(), DataWait{
				Inputs: inputs,
				Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
					resumed = true
					return nil
				},
			}, manifest.DrainForce)
			assert.Equal(t, resumed, false)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, errors.Is(err, manifest.ErrObjectNotFound), tc.failedCount != 0)
			failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
			assert.FatalIf(t, !ok, "expected dependency failure, got %v", err)
			concrete := error(failed)
			if tc.failedCount != 0 {
				domain, classified := errors.AsType[*manifest.Error](err)
				assert.FatalIf(t, !classified, "expected classified input error, got %T: %v", err, err)
				concrete = domain
			}
			assert.Equal(t, err, concrete)
			assert.Equal(t, failed.Parent, owner.Named())
			assert.Diff(t, failed.Subject, manifest.DiagnosticFor(owner.Named(), owner))
			assert.Diff(t, failed.Failed, ids)
			assert.Equal(t, len(failed.Reasons), len(ids))
			for _, id := range ids {
				message := tc.inputMessage
				if id == gate.Named() {
					message = "gate render failed"
				}
				assert.Diff(t, failed.Reasons[id], manifest.DiagnosticReason{
					Subject: manifest.DiagnosticFor(id, nil), Message: message,
				})
			}
			assert.Equal(t, err.Error(), tc.message)
			assert.Equal(t, manifest.ErrorDetails(err), tc.details)
		})
	}
}

func TestAwaitData_MultipleFailuresRetainSyntheticProducer(t *testing.T) {
	t.Parallel()
	st := store.New()
	producer := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", manifest.NamedResource{
		Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "sync",
	})
	owner := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	missing := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "missing"}
	produced := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "produced"}
	for _, object := range []manifest.BaseManifest{owner, producer} {
		st.AddObject(object)
	}
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending, Message: "root failure"})
	var e Execution
	e.Init(New(st, task.NewBounded(2)), owner.Named())
	defer e.Close()
	err := e.AwaitData(t.Context(), DataWait{
		Inputs: []depwait.DataDependency{
			{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "produced", Input: store.ReferenceFacts{
				Materializers: []manifest.NamedResource{producer.Named()},
			}},
			{NamedResource: missing},
		},
		Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
			t.Fatal("failed inputs resumed")
			return nil
		},
	}, manifest.DrainForce)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	assert.FatalIf(t, !ok, "expected aggregate, got %v", err)
	assert.Diff(t, failed.Failed, []manifest.NamedResource{missing, produced})
	assert.Equal(t, len(failed.Reasons), 2)
	assert.Diff(t, failed.Reasons[produced], manifest.DiagnosticReason{
		Subject: manifest.DiagnosticFor(produced, nil), Message: "not found; supply a readable document",
		Causes: []manifest.DiagnosticReason{{
			Subject: manifest.DiagnosticFor(producer.Named(), producer), Message: "not ready",
		}},
	})
	assert.Diff(t, failed.Reasons[missing], manifest.DiagnosticReason{
		Subject: manifest.DiagnosticFor(missing, nil), Message: "not found; supply a readable document",
	})
	assert.Equal(
		t,
		failed.Error(),
		"dependencies failed: ConfigMap/apps/missing: not found; supply a readable document; ConfigMap/apps/produced: dependencies failed: Kustomization.kustomize.toolkit.fluxcd.io/flux-system/sync (entry path \"clusters/test\"): not ready",
	)
}

func TestAwaitData_SharedPendingProducer(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps"}
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	ids := []manifest.NamedResource{
		{Kind: manifest.KindSecret, Namespace: "apps", Name: "first"},
		{Kind: manifest.KindSecret, Namespace: "apps", Name: "second"},
	}
	for _, object := range []manifest.BaseManifest{owner, producer} {
		st.AddObject(object)
	}
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusPending})
	assess := func(level manifest.DrainLevel) (*Execution, error) {
		e := new(Execution)
		e.Init(New(st, task.NewBounded(2)), owner.Named())
		t.Cleanup(e.Close)
		inputs := make([]depwait.DataDependency, 0, len(ids))
		for _, id := range ids {
			inputs = append(inputs, depwait.DataDependency{
				NamedResource: id, Use: "valuesFrom", Key: "uri",
				Input: store.ReferenceFacts{Materializers: []manifest.NamedResource{producer.Named()}},
			})
		}
		err := e.AwaitData(t.Context(), DataWait{
			Inputs: inputs,
			Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
				t.Fatal("missing inputs resumed")
				return nil
			},
		}, level)
		return e, err
	}
	e, err := assess(manifest.DrainCascade)
	pending, ok := errors.AsType[*pendingDataError](err)
	assert.FatalIf(t, !ok, "expected producer wait, got %T: %v", err, err)
	assert.Diff(t, pending.assessment.Blocked, []manifest.NamedResource{producer.Named()})
	assert.Equal(t, e.Reads.Valid(), true)
	err = pending.Settle(t.Context(), manifest.DrainForce)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	assert.FatalIf(t, !ok, "expected both missing inputs, got %T: %v", err, err)
	assert.Diff(t, failed.Failed, ids)
	assert.Equal(t, len(failed.Reasons), len(ids))
	for _, id := range ids {
		assert.Diff(t, failed.Reasons[id], manifest.DiagnosticReason{
			Subject: manifest.DiagnosticFor(id, nil), Message: "not found; supply a readable document",
			Causes: []manifest.DiagnosticReason{{
				Subject: manifest.DiagnosticFor(producer.Named(), producer), Message: "not ready",
			}},
		})
	}
	st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
	st.UpdateStatus(producer.Named(), store.StatusInfo{Status: store.StatusReady})
	assert.Equal(t, e.Reads.Valid(), false)
	fresh, err := assess(manifest.DrainForce)
	assert.Equal(t, fresh.Reads.Valid(), true)
	failed, ok = errors.AsType[*manifest.DependencyFailedError](err)
	assert.FatalIf(t, !ok, "expected fresh missing inputs, got %T: %v", err, err)
	assert.Diff(t, failed.Failed, ids)
	for _, id := range ids {
		assert.Diff(t, failed.Reasons[id], manifest.DiagnosticReason{
			Subject: manifest.DiagnosticFor(id, nil), Message: "not found; supply a readable document",
		})
	}
}

func TestAwaitData_ExistingInputMissingKeyReturnsDirectly(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{
		Kind: manifest.KindSecret, Name: "input", ValuesKey: "uri",
	}}}
	for _, object := range []manifest.BaseManifest{owner, &manifest.Secret{
		Name: "input", Namespace: "apps", StringData: map[string]any{"other": "value"},
	}} {
		st.AddObject(object)
	}
	var e Execution
	e.Init(New(st, task.NewBounded(2)), owner.Named())
	defer e.Close()
	inputs, err := e.DataInputs(t.Context(), owner, values.NewStoreProvider(&e.Reads, e.Reads.ID()))
	assert.NoError(t, err)
	assert.Equal(t, inputs[0].Result.NeedsInput(), false)
	err = e.AwaitData(t.Context(), DataWait{
		Inputs: inputs,
		Resume: func(context.Context, manifest.DrainLevel, []values.Result) error {
			t.Fatal("missing key resumed")
			return nil
		},
	}, manifest.DrainForce)
	assert.Equal(t, errors.Is(err, manifest.ErrInvalidValuesReference), true)
	assert.Equal(t, errors.Is(err, manifest.ErrObjectNotFound), false)
	assert.Equal(t, err.Error(), "valuesFrom Secret apps/input key \"uri\" not found")
	_, aggregate := errors.AsType[*manifest.DependencyFailedError](err)
	assert.Equal(t, aggregate, false)
}
