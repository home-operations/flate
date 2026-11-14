package orchestrator

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/selection"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSelect_PreparedStatusesExcludeOffRosterPrerequisites(t *testing.T) {
	t.Parallel()
	victim := testutil.KustomizationID("flux-system", "victim")
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "flux-system",
		Name:      victim.Name,
	}
	extra := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSet,
		Namespace: "flux-system",
		Name:      "victim",
	}
	run := selectedRun(t, &Result{})
	run.store.AddObject(&manifest.Kustomization{Name: victim.Name, Namespace: victim.Namespace})
	run.store.AddObject(&manifest.HelmChartSource{Name: chart.Name, Namespace: chart.Namespace})
	run.store.AddObject(&manifest.ResourceSet{Name: extra.Name, Namespace: extra.Namespace})
	run.store.UpdateFailure(chart, errors.New("chart failed"))
	run.store.UpdateFailure(extra, errors.New("generator failed"))
	run.store.UpdateFailure(
		victim,
		&manifest.DependencyFailedError{Parent: victim, Failed: []manifest.NamedResource{chart}},
	)
	run.Result.Failed = run.store.FailedResources()
	run.Result.Blocked = map[manifest.NamedResource][]manifest.NamedResource{victim: {chart}}
	run.Err = &selection.FailuresError{Message: "raw failures"}
	projectedResults0, projectionErrors0 := Select(
		[]Rendered{run},
		run.Err,
		[]manifest.NamedResource{{Group: victim.Group, Kind: victim.Kind, Name: victim.Name}},
		nil,
	)
	results, _, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	res := results[0]
	assert.Diff(
		t,
		slices.SortedFunc(maps.Keys(res.Statuses), manifest.NamedResource.Compare),
		[]manifest.NamedResource{chart, victim, extra},
	)
	assert.Equal(t, res.Statuses[victim].String(), "blocked by "+chart.NamespacedName())
	assert.Equal(t, res.Statuses[victim].IsDependencyFailure(), true)
	assert.Diff(t, res.Statuses[victim].BlockedBy(), []manifest.NamedResource{chart})
	{
		_, exists := res.Statuses[chart]
		assert.FatalIf(
			t,
			!exists || res.Counts[victim.Kind] != 1,
			"%v",
			"authored chart missing from diagnostics or root count changed",
		)
	}

	bootstrap := &manifest.GitRepository{
		Name:      manifest.BootstrapSourceID.Name,
		Namespace: manifest.BootstrapSourceID.Namespace,
	}
	ordinary := &manifest.GitRepository{Name: "ordinary", Namespace: "flux-system"}
	otherNamespace := &manifest.GitRepository{Name: bootstrap.Name, Namespace: "other"}
	foreignChart := &manifest.RawObject{
		APIVersion: "foreign.example/v1",
		Kind:       chart.Kind,
		Namespace:  chart.Namespace,
		Name:       chart.Name,
	}
	statusless := &manifest.Kustomization{Name: "statusless", Namespace: victim.Namespace}
	for _, obj := range []manifest.BaseManifest{bootstrap, ordinary, otherNamespace, foreignChart, statusless} {
		run.store.AddObject(obj)
	}
	run.store.UpdateStatus(
		bootstrap.Named(),
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "bootstrap failed"},
	)
	run.store.UpdateStatus(ordinary.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	run.store.UpdateStatus(otherNamespace.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	run.store.UpdateStatus(
		foreignChart.Named(),
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "foreign chart failed"},
	)
	run.Result.Failed = run.store.FailedResources()
	run.Result.Manifests = nil
	for _, obj := range run.store.ListObjects("", "") {
		run.Result.Manifests = append(
			run.Result.Manifests,
			testutil.DocumentBatch(obj, []map[string]any{
				{"kind": "ConfigMap", "metadata": map[string]any{"name": obj.Named().Name}},
			}),
		)
	}
	for _, tc := range []struct {
		name     string
		root     manifest.NamedResource
		objects  []manifest.BaseManifest
		statuses []manifest.NamedResource
	}{
		{
			name: "render roster with missing status", root: manifest.NamedResource{Group: victim.Group, Kind: victim.Kind},
			objects:  []manifest.BaseManifest{statusless, run.store.GetObject(victim)},
			statuses: []manifest.NamedResource{chart, victim, statusless.Named(), extra, foreignChart.Named()},
		},
		{
			name: "explicit chart", root: manifest.NamedResource{Group: chart.Group, Kind: chart.Kind},
			objects:  []manifest.BaseManifest{run.store.GetObject(chart)},
			statuses: []manifest.NamedResource{chart, victim, extra, foreignChart.Named()},
		},
		{
			name: "explicit source", root: manifest.NamedResource{Group: bootstrap.Named().Group, Kind: bootstrap.Named().Kind},
			objects:  []manifest.BaseManifest{bootstrap, ordinary, otherNamespace},
			statuses: []manifest.NamedResource{chart, ordinary.Named(), otherNamespace.Named(), victim, extra, foreignChart.Named()},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectPreparedStatusesExcludeOffRosterPrerequisitesCase(
				t,
				testSelectPreparedStatusesExcludeOffRosterPrerequisitesCaseInputs{
					run:        &run,
					statusless: &statusless,
					tc:         &tc,
				},
			)
		},
		)
	}
	t.Run("extra failure query filters", func(t *testing.T) {
		t.Parallel()
		testSelectPreparedStatusesExcludeOffRosterPrerequisitesExtraFailureQueryFilters(t)
	},
	)
}

func TestSelect_CascadesAndSharedVictims(t *testing.T) {
	t.Parallel()
	root, mid, leaf, missing, dual := testutil.KustomizationID(
		"flux-system",
		"root",
	), testutil.KustomizationID(
		"flux-system",
		"mid",
	), testutil.KustomizationID(
		"flux-system",
		"leaf",
	), testutil.KustomizationID(
		"flux-system",
		"missing",
	), testutil.KustomizationID(
		"flux-system",
		"dual",
	)
	for _, tc := range []struct {
		name    string
		blocked map[manifest.NamedResource][]manifest.NamedResource
		want    map[manifest.NamedResource][]manifest.NamedResource
	}{
		{name: "transitive", blocked: map[manifest.NamedResource][]manifest.NamedResource{mid: {root}, leaf: {mid}}, want: map[manifest.NamedResource][]manifest.NamedResource{root: {leaf, mid}}},
		{name: "shared", blocked: map[manifest.NamedResource][]manifest.NamedResource{dual: {root, missing, root}}, want: map[manifest.NamedResource][]manifest.NamedResource{root: {dual}, missing: {dual}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(
				t,
				&Result{Failed: make(map[manifest.NamedResource]store.StatusInfo), Blocked: tc.blocked},
			)
			run.Result.Failed[root] = store.StatusInfo{Status: store.StatusFailed, Message: "root boom"}
			for id := range tc.blocked {
				run.Result.Failed[id] = store.StatusInfo{
					Status:  store.StatusFailed,
					Message: "nested dependencies failed",
				}
			}
			run.Err = &selection.FailuresError{Message: "raw failures"}
			projectedResults1, projectionErrors1 := Select(
				[]Rendered{run},
				run.Err,
				[]manifest.NamedResource{testutil.KustomizationID("", "")},
				nil,
			)
			selected, err, _ := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
			assert.Diff(t, selected[0].Causes, tc.want)
			assert.FatalIf(t, err == nil || strings.Contains(err.Error(), "nested dependencies failed") ||
				!strings.Contains(
					err.Error(),
					fmt.Sprintf("(+%d blocked", len(tc.blocked)),
				), "cascade error did not count distinct victims: %v", err)
			{
				_, ok := errors.AsType[*selection.FailuresError](err)
				assert.FatalIf(t, !ok, "aggregate lost failure type: %T", err)
			}
		})
	}
}

func TestSelect_InvalidQueryAndInitialization(t *testing.T) {
	t.Parallel()
	run := selectedRun(t, &Result{})
	valid := []manifest.NamedResource{testutil.KustomizationID("", "")}
	for _, roots := range [][]manifest.NamedResource{
		nil,
		{{}},
		{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}},
		{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "different"}},
		{{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization}, {Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "different"}},
	} {
		{
			projectedResults2, projectionErrors2 := Select(
				[]Rendered{run},
				nil,
				roots,
				nil,
			)
			_, err, _ := projectedResults2, projectionErrors2.Selected, projectionErrors2.Remaining
			assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) ||
				!errors.Is(err, manifest.ErrFlux), "invalid query lacks domain identity: %+v %v", roots, err)
		}
	}
	initErr := errors.New("initialization failed")
	{
		projectedResults3, projectionErrors3 := Select(
			[]Rendered{{Err: initErr}},
			initErr,
			valid,
			nil,
		)
		selected, err, _ := projectedResults3, projectionErrors3.Selected, projectionErrors3.Remaining
		assert.FatalIf(t, len(selected) != 1 ||
			!errors.Is(err, initErr), "initialization error changed: %+v %v", selected, err)
	}
	{
		projectedResults4, projectionErrors4 := Select([]Rendered{{}}, nil, valid, nil)
		_, err, _ := projectedResults4, projectionErrors4.Selected, projectionErrors4.Remaining
		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "incomplete run accepted: %v", err)
	}
}

func TestSelect_AncestryCyclesAndOwnEvidence(t *testing.T) {
	t.Parallel()
	child, parent, unrelated := testutil.KustomizationID(
		"flux-system",
		"child",
	), testutil.KustomizationID(
		"flux-system",
		"parent",
	), testutil.KustomizationID(
		"flux-system",
		"unrelated",
	)
	orig := selectedRun(t, &Result{})
	current := selectedRun(t,
		&Result{
			Failed: map[manifest.NamedResource]store.StatusInfo{
				unrelated: {Status: store.StatusFailed, Message: "unrelated failed"},
			},
		},
	)
	for _, run := range []Rendered{orig, current} {
		run.store.AddObject(&manifest.Kustomization{Name: child.Name, Namespace: child.Namespace})
		run.store.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
		publishFixture(t, run.store, parent, []manifest.NamedResource{child})
		publishFixture(t, run.store, child, []manifest.NamedResource{parent})
	}
	current.store.UpdateStatus(
		child,
		store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "own skip"},
	)
	current.store.UpdateStatus(
		parent,
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "ancestor failed"},
	)
	projectedResults5, projectionErrors5 := Select(
		[]Rendered{orig, current},
		&selection.FailuresError{Message: "raw failures"},
		[]manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Name: child.Name}},
		nil,
	)
	results, _, _ := projectedResults5, projectionErrors5.Selected, projectionErrors5.Remaining
	assert.Equal(t, results[1].Unavailable[child].String(), "own skip")
	assert.Equal(t, len(results[0].Unavailable), 0)
	current.store.UpdateStatus(
		child,
		store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonUnchanged, Message: "unchanged"},
	)
	projectedResults6, projectionErrors6 := Select(
		[]Rendered{orig, current},
		&selection.FailuresError{Message: "raw failures"},
		[]manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Name: child.Name}},
		nil,
	)
	results, _, _ = projectedResults6, projectionErrors6.Selected, projectionErrors6.Remaining
	assert.Equal(t, results[1].Unavailable[child].String(), "ancestor failed")
	current.store.UpdateStatus(parent, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	projectedResults7, projectionErrors7 := Select(
		[]Rendered{orig, current},
		&selection.FailuresError{Message: "raw failures"},
		[]manifest.NamedResource{{Group: child.Group, Kind: child.Kind, Name: child.Name}},
		nil,
	)
	results, _, _ = projectedResults7, projectionErrors7.Selected, projectionErrors7.Remaining
	assert.Equal(t, len(results[1].Unavailable), 0)
}

func TestSelect_NamespaceSnapshots(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		namespace string
		changes   [2][]string
		disabled  bool
		want      [2][]string
	}{
		{name: "explicit", namespace: "media", changes: [2][]string{{"networking.yaml"}, {"default.yaml"}}, want: [2][]string{{"cluster", "media"}, {"cluster", "media"}}},
		{name: "inferred", changes: [2][]string{{"media.yaml"}, {"networking.yaml"}}, want: [2][]string{{"cluster", "media"}, {"cluster", "networking"}}},
		{name: "inferred multiple", changes: [2][]string{{"media.yaml", "networking.yaml"}, {"default.yaml"}}, want: [2][]string{{"cluster", "media", "networking"}, {"cluster", "default"}}},
		{name: "disabled", disabled: true, want: [2][]string{{"cluster", "default", "media", "networking"}, {"cluster", "default", "media", "networking"}}},
		{name: "empty active", want: [2][]string{{"cluster", "default", "media", "networking"}, {"cluster", "default", "media", "networking"}}},
	} {
		for _, count := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s_%d_side", tc.name, count), func(t *testing.T) {
				t.Parallel()
				testSelectNamespaceSnapshotsCase(t, testSelectNamespaceSnapshotsCaseInputs{tc: &tc, count: &count})
			},
			)
		}
	}
}

func TestSelect_AcceptedPrerequisites(t *testing.T) {
	t.Parallel()
	dep := testutil.HelmReleaseID("other", "dep")
	provider := manifest.NamedResource{
		Group:     "fluxcd.controlplane.io",
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "apps",
		Name:      "provider",
	}
	rs := &manifest.ResourceSet{Name: "selected", Namespace: "apps"}
	rs.DependsOn = []fluxopv1.Dependency{
		{APIVersion: dep.Group + "/v2", Kind: dep.Kind, Name: dep.Name, Namespace: dep.Namespace},
	}
	manifest.KindOf(&manifest.RawObject{APIVersion: dep.Group + "/v2", Kind: dep.Kind}).Namespaced()
	rs.InputsFrom = []fluxopv1.InputProviderReference{{Name: provider.Name}, {}}
	for _, tc := range []struct {
		name   string
		object manifest.BaseManifest
		deps   []manifest.NamedResource
	}{
		{"ResourceSet", rs, []manifest.NamedResource{dep, provider}},
		{"DependsOn", &manifest.Kustomization{Name: "selected", Namespace: "apps"}, []manifest.NamedResource{dep}},
		{"Source", &manifest.Kustomization{Name: "selected", Namespace: "apps", SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "source"}}, []manifest.NamedResource{{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectAcceptedPrerequisitesCase(t, &tc)
		},
		)
	}
}

func TestSelect_UnavailabilitySeparatesAncestryOwners(t *testing.T) {
	t.Parallel()
	present, absent := testutil.KustomizationID(
		"flux-system",
		"a-present",
	), testutil.KustomizationID(
		"flux-system",
		"z-absent",
	)
	middle, oldRoot, newRoot := testutil.KustomizationID(
		"flux-system",
		"middle",
	), testutil.KustomizationID(
		"flux-system",
		"old-root",
	), testutil.KustomizationID(
		"flux-system",
		"new-root",
	)
	orig, current := selectedRun(t, &Result{}), selectedRun(t, &Result{})
	for _, id := range []manifest.NamedResource{present, absent} {
		orig.store.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	current.store.AddObject(&manifest.Kustomization{Name: present.Name, Namespace: present.Namespace})
	current.store.UpdateStatus(oldRoot, store.StatusInfo{Status: store.StatusFailed, Message: "old ancestor failed"})
	publishFixture(t, orig.store, oldRoot, []manifest.NamedResource{middle})
	publishFixture(t, orig.store, middle, []manifest.NamedResource{present, absent})
	publishFixture(t, current.store, newRoot, []manifest.NamedResource{middle})
	publishFixture(t, current.store, middle, []manifest.NamedResource{present})
	projectedResults8, projectionErrors8 := Select(
		[]Rendered{orig, current},
		nil,
		[]manifest.NamedResource{{Group: present.Group, Kind: present.Kind}},
		nil,
	)
	selected, err, _ := projectedResults8, projectionErrors8.Selected, projectionErrors8.Remaining
	assert.FatalIf(t, err != nil, "%v", err)
	{
		_, ok := selected[1].Unavailable[present]
		assert.FatalIf(t, ok, "%v", "moved producer inherited old ancestry")
	}
	assert.Equal(t, selected[1].Unavailable[absent].String(), "old ancestor failed")
	t.Run("entry correspondence", testEntryCorrespondence)
}

func TestSelect_SingleRunOmitsExternalDisclosures(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		reason                   store.StatusReason
		message                  string
		requestedName, namespace string
		emptyNamespace, local    bool
		nonParticipant           bool
		inferred, labels         bool
		indexed                  bool
		failedParent             bool
		want                     bool
	}{
		{name: "typed skipped", reason: store.ReasonSkipped, want: true},
		{name: "typed unchanged", reason: store.ReasonUnchanged, message: "different words", want: true},
		{name: "unchanged declaration", reason: store.ReasonUnchanged, indexed: true, want: true},
		{name: "selected declaration", indexed: true},
		{name: "failed producer declaration", indexed: true, failedParent: true},
		{name: "suspended", reason: store.ReasonSuspended},
		{name: "misleading skip", message: "skipped: external source"},
		{name: "misleading unchanged", message: "unchanged"},
		{name: "matched name", requestedName: "external", reason: store.ReasonSkipped, want: true},
		{name: "unmatched name", requestedName: "other", reason: store.ReasonSkipped},
		{name: "matched namespace", namespace: "apps", reason: store.ReasonSkipped, want: true},
		{name: "unmatched namespace", namespace: "other", reason: store.ReasonSkipped},
		{name: "empty object namespace", namespace: "apps", emptyNamespace: true, reason: store.ReasonSkipped, want: true},
		{name: "local source", local: true, reason: store.ReasonSkipped},
		{name: "non-participating kind", nonParticipant: true, reason: store.ReasonSkipped},
		{name: "inferred namespace excludes output", requestedName: "external", inferred: true, reason: store.ReasonSkipped, want: true},
		{name: "labels exclude output", labels: true, reason: store.ReasonSkipped, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectSingleRunOmitsExternalDisclosuresCase(
				t,
				&tc,
			)
		},
		)
	}
}

type testSelectPreparedStatusesExcludeOffRosterPrerequisitesCaseInputs struct {
	run        *Rendered
	statusless **manifest.Kustomization
	tc         *struct {
		name string

		root     manifest.NamedResource
		objects  []manifest.BaseManifest
		statuses []manifest.NamedResource
	}
}

func testSelectPreparedStatusesExcludeOffRosterPrerequisitesCase(
	t *testing.T,
	testCaseInput testSelectPreparedStatusesExcludeOffRosterPrerequisitesCaseInputs,
) {
	t.Helper()

	projectedResults9, projectionErrors9 := Select(
		[]Rendered{(*testCaseInput.run)},
		testCaseInput.run.Err,
		[]manifest.NamedResource{testCaseInput.tc.root},
		nil,
	)
	results, _, _ := projectedResults9, projectionErrors9.Selected, projectionErrors9.Remaining
	res := results[0]
	assert.FatalIf(
		t,
		!slices.Equal(res.Objects, testCaseInput.tc.objects),
		"%v",
		"selected objects differ from the render roster",
	)
	assert.Diff(t, res.Counts, map[string]int{testCaseInput.tc.root.Kind: len(testCaseInput.tc.objects)})
	wantDocs := make(map[manifest.NamedResource][]map[string]any)
	for _, obj := range testCaseInput.tc.objects {
		wantDocs[obj.Named()] = outputByParent(testCaseInput.run.Result.Manifests)[obj.Named()]
	}
	assert.Diff(t, outputByParent(res.Manifests), wantDocs)
	slices.SortFunc(testCaseInput.tc.statuses, manifest.NamedResource.Compare)
	assert.Diff(
		t,
		slices.SortedFunc(maps.Keys(res.Statuses), manifest.NamedResource.Compare),
		testCaseInput.tc.statuses,
	)
	assert.Equal(t, maps.Equal(res.Failed, testCaseInput.run.Result.Failed), true)
	if info, ok := res.Statuses[(*testCaseInput.statusless).Named()]; ok {
		assert.Equal(t, info.Status, store.StatusFailed)
		assert.Equal(t, info.String(), "no status reported")
	}
	_, exists := testCaseInput.run.store.GetStatus((*testCaseInput.statusless).Named())
	assert.Equal(t, exists, false)
	assert.Equal(t, maps.Equal(testCaseInput.run.store.FailedResources(), testCaseInput.run.Result.Failed), true)
}

func testSelectPreparedStatusesExcludeOffRosterPrerequisitesExtraFailureQueryFilters(t *testing.T) {
	t.Helper()

	run := selectedRun(t, &Result{})
	obj := &manifest.Kustomization{Name: "selected", Namespace: "apps"}
	run.store.AddObject(obj)
	run.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	eligible := &manifest.ResourceSet{Name: obj.Name, Namespace: obj.Namespace}
	for _, extra := range []*manifest.ResourceSet{
		eligible,
		{Name: "other", Namespace: obj.Namespace},
		{Name: obj.Name, Namespace: "other"},
	} {
		run.store.AddObject(extra)
		run.store.UpdateStatus(
			extra.Named(),
			store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "extra failure"},
		)
	}
	run.Result.Failed = run.store.FailedResources()
	projectedResults10, projectionErrors10 := Select([]Rendered{run}, nil, []manifest.NamedResource{obj.Named()}, nil)
	selected, _, _ := projectedResults10, projectionErrors10.Selected, projectionErrors10.Remaining
	assert.Diff(
		t,
		slices.SortedFunc(maps.Keys(selected[0].Statuses), manifest.NamedResource.Compare),
		[]manifest.NamedResource{obj.Named(), eligible.Named()},
	)
	assert.FatalIf(
		t,
		!slices.Equal(selected[0].Objects, []manifest.BaseManifest{obj}),
		"%v",
		"selected objects differ from the render roster",
	)
	assert.Equal(t, len(run.store.FailedResources()), 3)
}

type testSelectNamespaceSnapshotsCaseInputs struct {
	tc *struct {
		name string

		namespace string
		changes   [2][]string
		disabled  bool
		want      [2][]string
	}
	count *int
}

func testSelectNamespaceSnapshotsCase(t *testing.T, testCaseInput testSelectNamespaceSnapshotsCaseInputs) {
	t.Helper()

	runs := make([]Rendered, (*testCaseInput.count))
	for i := range runs {
		run := selectedRun(t, &Result{Warnings: []manifest.Warning{{Message: "global"}}})
		files := make(map[manifest.NamedResource]string)
		for _, namespace := range []string{"", "default", "media", "networking"} {
			name := namespace
			if name == "" {
				name = "cluster"
			}
			obj := &manifest.Kustomization{Name: name, Namespace: namespace}
			run.store.AddObject(obj)
			files[obj.Named()] = namespace + ".yaml"
		}
		run.index = indexForTest(files)
		runs[i] = run
	}
	for invocation := range 2 {
		for i, run := range runs {
			if !testCaseInput.tc.disabled {
				run.loader, run.selection = admissiontest.New(
					t,
					admissiontest.Config{
						Changes:   change.NewSet(testCaseInput.tc.changes[(i+invocation)%2]),
						Index:     run.index,
						Inventory: run.store,
						Root:      "",
					},
				)
				admissiontest.Commit(t, run.selection, nil)
			}
			runs[i] = run
		}
		projectedResults11, projectionErrors11 := Select(
			runs,
			nil,
			[]manifest.NamedResource{testutil.KustomizationID(testCaseInput.tc.namespace, "")},
			nil,
		)
		selected, err, remainder := projectedResults11, projectionErrors11.Selected, projectionErrors11.Remaining
		assert.NoError(t, err)
		assert.NoError(t, remainder)
		assert.Equal(t, len(selected), (*testCaseInput.count))
		for i, result := range selected {
			var names []string
			for _, obj := range result.Objects {
				names = append(names, obj.Named().Name)
			}
			slices.Sort(names)
			want := testCaseInput.tc.want[(i+invocation)%2]
			assert.Diff(t, names, want)
			assert.Equal(t, result.Counts[manifest.KindKustomization], len(want))
		}
	}
}

func testSelectAcceptedPrerequisitesCase(t *testing.T, testCaseInput *struct {
	name string

	object manifest.BaseManifest
	deps   []manifest.NamedResource
},
) {
	t.Helper()

	run := selectedRun(t, &Result{Failed: make(map[manifest.NamedResource]store.StatusInfo)})
	run.store.AddObject(testCaseInput.object)
	for _, id := range testCaseInput.deps {
		run.Result.Failed[id] = store.StatusInfo{Status: store.StatusFailed, Message: "dependency failed"}
	}
	reads := new(store.ReadSet)
	reads.Init(run.store, testCaseInput.object.Named(), nil)
	reads.GetObject(testCaseInput.object.Named())
	for _, dep := range testCaseInput.deps {
		reads.GetObject(dep)
		reads.Require(manifest.DependencyRef{NamedResource: dep})
	}
	reads.Accept(store.AssessmentReconciled)
	reads.Retire()
	for range run.store.Sweep() {
	}
	projectedResults12, projectionErrors12 := Select(
		[]Rendered{run},
		&selection.FailuresError{Message: "raw failures"},
		[]manifest.NamedResource{testCaseInput.object.Named()},
		nil,
	)
	selected, err, _ := projectedResults12, projectionErrors12.Selected, projectionErrors12.Remaining
	if err == nil || len(selected[0].Failed) != len(testCaseInput.deps) || len(selected[0].Unrelated) != 0 {
		t.Fatalf("accepted prerequisites omitted: %+v %v", selected[0], err)
	}
}
