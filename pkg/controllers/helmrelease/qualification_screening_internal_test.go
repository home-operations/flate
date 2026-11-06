package helmrelease

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_QualificationDemandPromotion(t *testing.T) {
	t.Parallel()
	c, hr := qualificationRelease(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered, namespace: apps}\ndata: {value: {{ .Values.value | quote }}}\n",
		"value: stable\n",
	)
	id := hr.Named()
	var publications atomic.Int64
	stop := c.Store.AddListener(store.EventArtifactUpdated, func(other manifest.NamedResource, payload any) {
		if output, ok := payload.(*store.Emission); other == id && ok && output.Complete {
			publications.Add(1)
		}
	}, false)
	defer stop()
	screened := reconcileQualificationRelease(t, c, id)
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
	_, _, empty := c.Store.GetEmission(id)
	assert.Equal(t, empty.Complete, false)
	assert.Equal(t, empty.Render, (*manifest.RenderReceipt)(nil))
	empty.Finish()

	c.Store.SetIncluded(id, true, true)
	assert.Equal(t, screened.Valid(), false)
	fresh := reconcileQualificationRelease(t, c, id)
	assert.Equal(t, fresh.Disposition, store.AssessmentReconciled)
	assert.Equal(t, fresh.Valid(), true)
	assert.Equal(t, fresh.Progressed(), true)
	status, _ = c.Store.GetStatus(id)
	assert.FatalIf(t, status.Outcome() != store.OutcomePassed, "fresh status = %+v", status)
	output, ok := c.Store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		!ok || !output.Complete || output.Render == nil || len(output.Manifests) != 1,
		"missing admitted render: %+v",
		output,
	)
	assert.Equal(t, publications.Load(), int64(1))

	reused := reconcileQualificationRelease(t, c, id)
	assert.Equal(t, reused.Disposition, store.AssessmentReused)
	assert.Equal(t, reused.Valid(), true)
	assert.Equal(t, reused.Progressed(), false)
	assert.Equal(t, publications.Load(), int64(1))
	replayed := c.Store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
	assert.Equal(t, replayed.Fingerprint, output.Fingerprint)
	assert.Diff(t, replayed.Manifests, output.Manifests)
}

func TestReconcile_QualificationResolvedValues(t *testing.T) {
	t.Parallel()
	for _, marker := range []string{"", manifest.FluxKustomizeDomain, manifest.FluxOperatorDomain} {
		name := marker
		if name == "" {
			name = "negative"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c, hr := qualificationRelease(t, `{{ fail "Helm body ran" }}`, "marker: '"+marker+"'\n")
			run := reconcileQualificationRelease(t, c, hr.Named())
			assert.Equal(t, run.Outcome(), schedule.Terminal)
			assert.Equal(t, run.Valid(), true)
			info, _ := c.Store.GetStatus(hr.Named())
			if marker == "" {
				assert.Equal(t, run.Disposition, store.AssessmentQualificationSettled)
				assert.Equal(t, info.Status == store.StatusFailed, false)
				assert.Equal(t, info.Status == store.StatusReady, false)
			} else {
				assert.Equal(t, run.Disposition, store.AssessmentReconciled)
				assert.Equal(t, info.Status, store.StatusFailed)
				assert.FatalIf(t, !strings.Contains(info.Message, "Helm body ran"), "body status = %+v", info)
			}
			assert.Equal(t, c.Store.GetArtifact(hr.Named(), store.ArtifactContent), store.Artifact(nil))
		})
	}
}

func qualificationRelease(t *testing.T, body, values string) (*Controller, *manifest.HelmRelease) {
	t.Helper()
	c, st := newTestController(t, nil)
	source := &manifest.OCIRepository{Name: "chart", Namespace: "apps", URL: "oci://example.test/chart"}
	input := &manifest.ConfigMap{Name: "values", Namespace: "apps", Data: map[string]any{"values.yaml": values}}
	hr := (&manifest.HelmRelease{
		Name: "candidate", Namespace: "apps",
		ChartRef:   &helmv2.CrossNamespaceSourceReference{Kind: manifest.KindOCIRepository, Name: source.Name},
		ValuesFrom: []helmv2.ValuesReference{{Kind: manifest.KindConfigMap, Name: input.Name}},
	}).WithNamespace("apps").(*manifest.HelmRelease)
	st.AddObjects([]manifest.BaseManifest{source, input, hr})
	st.SetArtifact(source.Named(), &store.SourceArtifact{
		Kind:     manifest.KindOCIRepository,
		Revision: "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
		Digest:   "sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
		FS: fstest.MapFS{
			"chart/Chart.yaml":            {Data: []byte("apiVersion: v2\nname: candidate\nversion: 1.0.0\n")},
			"chart/templates/result.yaml": {Data: []byte(body)},
		},
	})
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	st.SetIncluded(hr.Named(), true, false)
	return c, hr
}

func reconcileQualificationRelease(t *testing.T, c *Controller, id manifest.NamedResource) *base.Execution {
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
	c, producer := qualificationRelease(t, `{{ fail "ordinary body ran" }}`, "")
	status := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: c.Tasks, Store: c.Store, ID: producer.Named(), ReconcileNode: c.ReconcileNode,
	})
	assert.Equal(t, status.Status == store.StatusReady, false)
	assert.Equal(t, status.Status == store.StatusFailed, false)
	assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
}

func TestReconcile_QualificationOrdinaryDemand(t *testing.T) {
	t.Parallel()
	c, producer := qualificationRelease(t, `{{ fail "ordinary body ran" }}`, "")
	c.Store.SetIncluded(producer.Named(), true, true)
	run := reconcileQualificationRelease(t, c, producer.Named())
	assert.Equal(t, run.Disposition, store.AssessmentReconciled)
	status, _ := c.Store.GetStatus(producer.Named())
	assert.Equal(t, status.Status, store.StatusFailed)
	assert.Equal(t, strings.Contains(status.Message, "ordinary body ran"), true)
}

func TestReconcile_QualificationPendingInput(t *testing.T) {
	t.Parallel()
	c, producer := qualificationRelease(
		t,
		`{{ fail "resolved body ran" }}`,
		"marker: '"+manifest.FluxOperatorDomain+"'\n",
	)
	input := &manifest.ConfigMap{
		Name:      "values",
		Namespace: "apps",
		Data:      map[string]any{"values.yaml": "marker: '" + manifest.FluxOperatorDomain + "'\n"},
	}
	c.Store.DeleteObject(input.Named())
	pending := reconcileQualificationRelease(t, c, producer.Named())
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
	fresh := reconcileQualificationRelease(t, c, producer.Named())
	assert.Equal(t, fresh.Disposition, store.AssessmentReconciled)
	status, _ = c.Store.GetStatus(producer.Named())
	assert.Equal(t, strings.Contains(status.Message, "resolved body ran"), true)
	assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
}

func TestReconcile_QualificationInputReplacement(t *testing.T) {
	t.Parallel()
	c, producer := qualificationRelease(t, `{{ fail "replacement body ran" }}`, "marker: stable\n")
	settled := reconcileQualificationRelease(t, c, producer.Named())
	assert.Equal(t, settled.Disposition, store.AssessmentQualificationSettled)
	c.Store.AddObject(
		&manifest.ConfigMap{
			Name:      "values",
			Namespace: "apps",
			Data:      map[string]any{"values.yaml": "marker: '" + manifest.FluxOperatorDomain + "'\n"},
		},
	)
	assert.Equal(t, settled.Valid(), false)
	fresh := reconcileQualificationRelease(t, c, producer.Named())
	assert.Equal(t, fresh.Disposition, store.AssessmentReconciled)
	status, _ := c.Store.GetStatus(producer.Named())
	assert.Equal(t, strings.Contains(status.Message, "replacement body ran"), true)
	assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
}

func TestReconcile_QualificationInputError(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"capability", "OCI revision"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			c, producer := qualificationRelease(t, `{{ fail "body ran" }}`, "marker: stable\n")
			message := `could not parse "invalid" as version`
			if fault == "capability" {
				c.Options.KubeVersion = "invalid"
			} else {
				input := *c.Store.GetArtifact(producer.ChartSource(), store.ArtifactContent).(*store.SourceArtifact)
				input.Revision = "invalid"
				c.Store.SetArtifact(producer.ChartSource(), &input)
				message = "track oci chart version"
			}
			run := reconcileQualificationRelease(t, c, producer.Named())
			assert.Equal(t, run.Disposition, store.AssessmentReconciled)
			status, _ := c.Store.GetStatus(producer.Named())
			assert.Equal(t, status.Status, store.StatusFailed)
			assert.Equal(t, strings.Contains(status.Message, message), true)
			assert.Equal(t, c.Store.HasInventory(producer.Named()), false)
		})
	}
}
