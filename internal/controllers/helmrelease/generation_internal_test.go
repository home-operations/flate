package helmrelease

import (
	"context"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/selection"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/schedule"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestGeneratedObjects_PublishBeforeSourceWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, replacement string }{
		{manifest.KindHelmChart, "failure"},
		{manifest.KindOCIRepository, "failure"},
		{manifest.KindHelmChart, manifest.KindOCIRepository},
		{manifest.KindHelmChart, "chartRef"},
		{manifest.KindOCIRepository, "chartRef"},
	} {
		t.Run(tc.kind+"/"+tc.replacement, func(t *testing.T) {
			t.Parallel()
			testGeneratedObjectsPublishBeforeSourceWaitCase(
				t,
				&tc,
			)
		},
		)
	}
}

func testGeneratedObjectsPublishBeforeSourceWaitCase(
	t *testing.T,
	testCaseInput *struct {
		kind string

		replacement string
	},
) {
	t.Helper()

	template := &helmv2.HelmChartTemplate{
		Kind: testCaseInput.kind,
		Spec: helmv2.HelmChartTemplateSpec{
			Chart:   "app",
			Version: "1.0.0",
			SourceRef: &helmv2.CrossNamespaceObjectReference{
				Kind:      manifest.KindHelmRepository,
				Name:      "late",
				Namespace: "sources",
			},
		},
	}
	if testCaseInput.kind == manifest.KindOCIRepository {
		template.Spec = helmv2.HelmChartTemplateSpec{URL: "oci://registry.example/charts/app"}
	}
	hr := (&manifest.HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: template}}).WithNamespace("apps").(*manifest.HelmRelease)
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{hr.Named(): {File: "release.yaml"}},
	}
	filterLoader, _ := admissiontest.New(
		t,
		admissiontest.Config{
			Changes:   change.NewSet([]string{"release.yaml"}),
			Index:     index,
			Inventory: filtertest.MapLister{},
		},
	)
	admissiontest.Commit(t, filterLoader.Options.Session, nil)
	filter := filterLoader.Options.Session
	c, st := newTestController(t, filterLoader)
	reconcile := func(level manifest.DrainLevel) *base.Execution {
		var run *base.Execution
		c.Tasks.Go(t.Context(), "release", func(ctx context.Context) {
			run = c.ReconcileNode(ctx, hr.Named(), level, schedule.Result{})
		})
		c.Tasks.BlockTillDone()
		return run
	}
	st.AddObject(hr)
	c.KeepEmitted(
		t.Context(),
		manifest.NamedResource{},
		selection.PrepareRequest{Generation: []store.Child{{Obj: hr}}, Reads: nil},
	)
	id := hr.ChartSource()
	arrivals := 0
	stop := st.AddListener(store.EventObjectAdded, func(got manifest.NamedResource, value any) {
		if got == id && value != nil {
			arrivals++
			assert.ErrorIf(
				t,
				!filter.Snapshot().Reconcile(got),
				"%v",
				"generated source arrived before keep-set admission",
			)
		}
	}, false)
	defer stop()
	first := reconcile(manifest.DrainNone)
	defer first.Close()
	if first.Outcome() != schedule.Blocked || st.GetDeclaration(id) == nil || arrivals != 1 {
		t.Fatalf("first outcome=%v source=%v arrivals=%d", first.Outcome(), st.GetDeclaration(id), arrivals)
	}
	_, _, partial := st.GetEmission(hr.Named())
	defer partial.Finish()
	assert.FatalIf(
		t,
		len(partial.Children) != 1 || !partial.Children[0].Generated || len(partial.Manifests) != 0 ||
			!st.ReportHidden(id),
		"provisional inventory=%+v",
		partial,
	)
	{
		_, ready := st.GetStatus(id)
		assert.FatalIf(t, ready, "%v", "parent assigned readiness to its generated source")
	}
	st.SetArtifact(
		id,
		&store.SourceArtifact{
			Key:      "chart",
			Content:  "chart-generation",
			Revision: "1.0.0@sha256:abcdef12345675476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
			FS: fstest.MapFS{
				"chart/Chart.yaml": &fstest.MapFile{
					Data: []byte("apiVersion: v2\nname: app\nversion: 1.0.0\n"),
				},
				"chart/templates/output.yaml": &fstest.MapFile{
					Data: []byte(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: complete}\n",
					),
				},
			},
		},
	)
	st.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady})
	second := reconcile(manifest.DrainNone)
	defer second.Close()
	{
		info, _ := st.GetStatus(hr.Named())
		assert.FatalIf(t, info.Status != store.StatusReady, "render after source arrival: %+v", info)
	}
	_, _, completed := st.GetEmission(hr.Named())
	defer completed.Finish()
	assert.FatalIf(t, len(completed.Manifests) != 1 ||
		!slices.ContainsFunc(
			completed.Children,
			func(child store.Child) bool { return child.Generated && child.Obj.Named() == id },
		), "%v", "complete render lost the current generated source")
	shouldReturn := testGeneratedObjectsPublishBeforeSourceWaitCaseTail(
		t,
		&testGeneratedObjectsPublishBeforeSourceWaitCaseTailInputs{
			testCaseInput: &testCaseInput,
			hr:            &hr,
			st:            &st,
			id:            &id,
			reconcile:     &reconcile,
			filter:        &filter,
			completed:     &completed,
		},
	)
	if shouldReturn {
		return
	}
}

func testGeneratedObjectsPublishBeforeSourceWaitCaseTail(
	t *testing.T,
	tailCaseInput *testGeneratedObjectsPublishBeforeSourceWaitCaseTailInputs,
) bool {
	t.Helper()
	if (*tailCaseInput.testCaseInput).replacement != "failure" {
		return checkGeneratedSourceReplacement(t, tailCaseInput)
	}
	(*tailCaseInput.st).
		UpdateStatus((*tailCaseInput.id), store.StatusInfo{Status: store.StatusFailed, Message: "source failed"})
	failed := (*tailCaseInput.reconcile)(manifest.DrainForce)
	defer failed.Close()
	{
		info, _ := (*tailCaseInput.st).GetStatus((*tailCaseInput.hr).Named())
		assert.FatalIf(
			t,
			info.Status != store.StatusFailed,
			"source failure was hidden by fingerprint replay: %+v",
			info,
		)
	}
	assert.FatalIf(
		t,
		(*tailCaseInput.st).GetObject((*tailCaseInput.id)) == nil,
		"%v",
		"failed render discarded its current generated source",
	)
	return false
}

type testGeneratedObjectsPublishBeforeSourceWaitCaseTailInputs struct {
	testCaseInput **struct {
		kind string

		replacement string
	}

	hr **manifest.
		HelmRelease

	st **store.
		Store

	id *manifest.
		NamedResource

	reconcile *func(
		level manifest.
			DrainLevel) *base.
		Execution

	filter **selection.Session

	completed *store.
			Emission
}

func checkGeneratedSourceReplacement(
	t *testing.T,
	tailCaseInput *testGeneratedObjectsPublishBeforeSourceWaitCaseTailInputs,
) bool {
	t.Helper()
	next := (*tailCaseInput.hr).Clone()
	if (*tailCaseInput.testCaseInput).replacement == "chartRef" {
		next.HelmReleaseSpec.Chart = nil
		next.ChartRef = &helmv2.CrossNamespaceSourceReference{
			Kind: manifest.KindOCIRepository,
			Name: "replacement",
		}
	} else {
		next.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{
			Kind: manifest.KindOCIRepository,
			Spec: helmv2.HelmChartTemplateSpec{URL: "oci://registry.example/charts/replacement"},
		}
	}
	next = next.WithNamespace((*tailCaseInput.hr).Namespace).(*manifest.HelmRelease)
	sibling := (*tailCaseInput.hr).Clone()
	sibling.Name = "sibling"
	(*tailCaseInput.st).
		AddObject(sibling)
	(*tailCaseInput.st).
		AddObject(next)
	replacement := next.ChartSource()
	if (*tailCaseInput.testCaseInput).replacement == "chartRef" {
		(*tailCaseInput.st).
			AddObject(
				manifest.NewOCIRepository(
					sourcev1.OCIRepository{
						Name:      replacement.Name,
						Namespace: replacement.Namespace,
						Spec:      sourcev1.OCIRepositorySpec{URL: "oci://registry.example/charts/replacement"},
					},
				),
			)
		(*tailCaseInput.st).
			UpdateStatus(replacement, store.StatusInfo{Status: store.StatusPending})
	}
	withdrawn := make(chan struct{}, 1)
	stopWithdrawal := (*tailCaseInput.st).AddListener(
		store.EventObjectAdded,
		func(got manifest.NamedResource, _ any) {
			if got == (*tailCaseInput.id) && (*tailCaseInput.st).GetObject((*tailCaseInput.id)) == nil {
				{
					info, _ := (*tailCaseInput.st).GetStatus(
						replacement,
					)
					assert.ErrorIf(t, info.Status == store.StatusReady || (*tailCaseInput.st).
						GetArtifact(
							replacement,
							store.ArtifactContent,
						) != nil, "%v", "obsolete source withdrew after replacement settled")
				}
				withdrawn <- struct{}{}
			}
		},
		false,
	)
	defer stopWithdrawal()
	blocked := (*tailCaseInput.reconcile)(manifest.DrainNone)
	defer blocked.Close()
	select {
	case <-withdrawn:
	default:
		t.Fatal("obsolete generated source did not withdraw before the replacement wait")
	}
	if !blocked.Reads.Valid() {
		blocked = (*tailCaseInput.reconcile)(manifest.DrainNone)
		defer blocked.Close()
	}
	if blocked.Outcome() != schedule.Blocked {
		info, _ := (*tailCaseInput.st).GetStatus((*tailCaseInput.hr).Named())
		t.Fatalf(
			"replacement outcome=%v status=%#v readsValid=%t admitted=%t source=%v",
			blocked.Outcome(),
			info,
			blocked.Reads.Valid(),
			blocked.Reads.Admitted(),
			replacement,
		)
	}
	assert.Diff(t, blocked.Blocked, []manifest.NamedResource{replacement})
	assert.Equal(t, (*tailCaseInput.filter).Snapshot().Reconcile(next.Named()), true)
	assert.Equal(t, (*tailCaseInput.filter).Snapshot().Reconcile(sibling.Named()), false)
	_, _, waiting := (*tailCaseInput.st).GetEmission((*tailCaseInput.hr).Named())
	defer waiting.Finish()
	assert.FatalIf(
		t,
		waiting.Complete || len(waiting.Failures) != 0,
		"%v",
		"source transition forced completion or failure",
	)
	childValues := tailCaseInput.completed.Children
	for childIndex := range childValues {
		child := childValues[childIndex]
		if child.Generated {
			continue
		}
		assert.FatalIf(t, !slices.ContainsFunc(
			waiting.Children,
			func(current store.Child) bool { return current.Obj.Named() == child.Obj.Named() },
		) || (*tailCaseInput.st).
			GetObject(
				child.Obj.Named(),
			) == nil, "%v", "blocked replacement discarded ordinary partial inventory")
	}
	assert.Diff(
		t,
		testutil.DocumentMaps(waiting.Manifests),
		testutil.DocumentMaps(tailCaseInput.completed.Manifests),
	)
	assert.FatalIf(
		t, (*tailCaseInput.st).
			GetObject((*tailCaseInput.id)) != nil || (*tailCaseInput.st).
			HasGeneratedObjects(
				(*tailCaseInput.hr).Named(),
			) != ((*tailCaseInput.testCaseInput).replacement != "chartRef"), "%v",
		"blocked replacement retained obsolete generated membership",
	)
	return true
}
