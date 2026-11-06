package resourceset

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_QualificationDemandPromotion(t *testing.T) {
	t.Parallel()
	c, rs := qualificationResourceSet(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: << inputs.value | quote >>}\n",
		"stable",
	)
	id := rs.Named()
	var publications atomic.Int64
	stop := c.Store.AddListener(store.EventArtifactUpdated, func(other manifest.NamedResource, payload any) {
		if output, ok := payload.(*store.Emission); other == id && ok && output.Complete {
			publications.Add(1)
		}
	}, false)
	defer stop()
	screened := reconcileQualificationResourceSet(t, c, id)
	assert.Equal(t, screened.Outcome(), schedule.Terminal)
	assert.Equal(t, screened.Disposition, store.AssessmentQualificationSettled)
	assert.Equal(t, screened.Valid(), true)
	assert.Equal(t, screened.Progressed(), false)
	assert.Equal(t, c.Store.HasInventory(id), false)
	assert.Equal(t, c.Store.GetArtifact(id, store.ArtifactContent), store.Artifact(nil))
	status, _ := c.Store.GetStatus(id)
	assert.Equal(t, status.Status == store.StatusReady, false)
	assert.Equal(t, status.Outcome() == store.OutcomePassed, false)
	assert.Equal(t, publications.Load(), int64(0))

	c.Store.SetIncluded(id, true, true)
	assert.Equal(t, screened.Valid(), false)
	fresh := reconcileQualificationResourceSet(t, c, id)
	assert.Equal(t, fresh.Disposition, store.AssessmentReconciled)
	assert.Equal(t, fresh.Valid(), true)
	assert.Equal(t, fresh.Progressed(), true)
	status, _ = c.Store.GetStatus(id)
	assert.Equal(t, status.Outcome(), store.OutcomePassed)
	output, ok := c.Store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok || !output.Complete || len(output.Manifests) != 1, "missing admitted render: %+v", output)
	assert.Equal(t, publications.Load(), int64(1))

	reused := reconcileQualificationResourceSet(t, c, id)
	assert.Equal(t, reused.Disposition, store.AssessmentReused)
	assert.Equal(t, reused.Valid(), true)
	assert.Equal(t, reused.Progressed(), false)
	assert.Equal(t, publications.Load(), int64(1))
	replayed := c.Store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	assert.Equal(t, replayed.Fingerprint, output.Fingerprint)
	assert.Diff(t, replayed.Manifests, output.Manifests)
}

func TestReconcile_QualificationResolvedProvider(t *testing.T) {
	t.Parallel()
	for _, marker := range []string{"", manifest.FluxKustomizeDomain, manifest.FluxOperatorDomain} {
		name := marker
		if name == "" {
			name = "negative"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, rs := qualificationResourceSet(t, `<< fail "ResourceSet body ran" >>`, marker)
			run := reconcileQualificationResourceSet(t, c, rs.Named())
			assert.Equal(t, run.Outcome(), schedule.Terminal)
			assert.Equal(t, run.Valid(), true)
			info, _ := c.Store.GetStatus(rs.Named())
			if marker == "" {
				assert.Equal(t, run.Disposition, store.AssessmentQualificationSettled)
				assert.Equal(t, info.Status == store.StatusFailed, false)
				assert.Equal(t, info.Status == store.StatusReady, false)
			} else {
				assert.Equal(t, run.Disposition, store.AssessmentReconciled)
				assert.Equal(t, info.Status, store.StatusFailed)
				assert.Equal(t, strings.Contains(info.Message, "ResourceSet body ran"), true)
			}
			assert.Equal(t, c.Store.GetArtifact(rs.Named(), store.ArtifactContent), store.Artifact(nil))
		})
	}
}

func qualificationResourceSet(t *testing.T, body, value string) (*Controller, *manifest.ResourceSet) {
	t.Helper()
	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks, nil)
	c.Configure(base.Options{})
	c.StartLifecycle()
	t.Cleanup(tasks.BlockTillDone)
	provider := staticRSIP("input", "apps", nil, map[string]string{"value": value})
	rs := &manifest.ResourceSet{
		Name: "candidate", Namespace: "apps",
		InputsFrom:        []fluxopv1.InputProviderReference{{Name: provider.Name}},
		ResourcesTemplate: body,
	}
	st.AddObjects([]manifest.BaseManifest{provider, rs})
	st.SetIncluded(rs.Named(), true, false)
	return c, rs
}

func reconcileQualificationResourceSet(t *testing.T, c *Controller, id manifest.NamedResource) *base.Execution {
	t.Helper()
	var run *base.Execution
	c.Tasks.Go(t.Context(), "qualification", func(ctx context.Context) {
		run = c.ReconcileNode(ctx, id, manifest.DrainNone, schedule.Result{})
	})
	c.Tasks.BlockTillDone()
	assert.Equal(t, c.Tasks.Failures(), 0)
	t.Cleanup(run.Close)
	return run
}

func TestReconcile_QualificationDrain(t *testing.T) {
	t.Parallel()
	c, producer := qualificationResourceSet(t, `<< fail "ordinary body ran" >>`, "")
	status := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: c.Tasks, Store: c.Store, ID: producer.Named(), ReconcileNode: c.ReconcileNode,
	})
	assert.Equal(t, status.Status == store.StatusReady, false)
	assert.Equal(t, status.Status == store.StatusFailed, false)
	assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
}

func TestReconcile_QualificationOrdinaryDemand(t *testing.T) {
	t.Parallel()
	c, producer := qualificationResourceSet(t, `<< fail "ordinary body ran" >>`, "")
	c.Store.SetIncluded(producer.Named(), true, true)
	run := reconcileQualificationResourceSet(t, c, producer.Named())
	assert.Equal(t, run.Disposition, store.AssessmentReconciled)
	status, _ := c.Store.GetStatus(producer.Named())
	assert.Equal(t, status.Status, store.StatusFailed)
	assert.Equal(t, strings.Contains(status.Message, "ordinary body ran"), true)
}

func TestReconcile_QualificationPendingInput(t *testing.T) {
	t.Parallel()
	c, producer := qualificationResourceSet(t, `<< fail "resolved body ran" >>`, manifest.FluxOperatorDomain)
	input := staticRSIP("input", "apps", nil, map[string]string{"value": manifest.FluxOperatorDomain})
	c.Store.DeleteObject(input.Named())
	pending := reconcileQualificationResourceSet(t, c, producer.Named())
	assert.Equal(t, pending.Outcome(), schedule.Blocked)
	assert.Equal(t, pending.Disposition == store.AssessmentQualificationSettled, false)
	c.Tasks.Go(t.Context(), "forced qualification", func(ctx context.Context) {
		pending.Settle(ctx, manifest.DrainForce)
	})
	c.Tasks.BlockTillDone()
	assert.Equal(t, pending.Outcome() == schedule.Blocked, false)
	assert.Equal(t, pending.Disposition == store.AssessmentQualificationSettled, false)
	status, _ := c.Store.GetStatus(producer.Named())
	assert.Equal(t, status.Status, store.StatusFailed)
	c.Store.AddObject(input)
	assert.Equal(t, pending.Valid(), false)
	fresh := reconcileQualificationResourceSet(t, c, producer.Named())
	assert.Equal(t, fresh.Disposition, store.AssessmentReconciled)
	status, _ = c.Store.GetStatus(producer.Named())
	assert.Equal(t, strings.Contains(status.Message, "resolved body ran"), true)
	assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
}

func TestReconcile_QualificationInputReplacement(t *testing.T) {
	t.Parallel()
	c, producer := qualificationResourceSet(t, `<< fail "replacement body ran" >>`, "stable")
	settled := reconcileQualificationResourceSet(t, c, producer.Named())
	assert.Equal(t, settled.Disposition, store.AssessmentQualificationSettled)
	c.Store.AddObject(staticRSIP("input", "apps", nil, map[string]string{"value": manifest.FluxOperatorDomain}))
	assert.Equal(t, settled.Valid(), false)
	fresh := reconcileQualificationResourceSet(t, c, producer.Named())
	assert.Equal(t, fresh.Disposition, store.AssessmentReconciled)
	status, _ := c.Store.GetStatus(producer.Named())
	assert.Equal(t, strings.Contains(status.Message, "replacement body ran"), true)
	assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
}
