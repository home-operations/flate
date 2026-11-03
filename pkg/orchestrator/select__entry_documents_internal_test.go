package orchestrator

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSelect_EntryDocuments(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		owner manifest.NamedResource
	}{
		{name: "path only"},
		{name: "Kustomization", owner: testutil.KustomizationID("flux-system", "sync")},
		{name: "FluxInstance", owner: manifest.NamedResource{Group: "fluxcd.controlplane.io", Kind: "FluxInstance", Namespace: "flux-system", Name: "flux"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectEntryDocumentsCase(t, &tc)
		},
		)
	}
	for _, tc := range []struct {
		name    string
		roots   []manifest.NamedResource
		labels  map[string]string
		want    []string
		objects int
	}{
		{name: "all", roots: []manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, want: []string{"child-hr", "child-ks", "driver-hr", "driver-ks", "foreign", "static"}, objects: 2},
		{name: "named driver", roots: []manifest.NamedResource{testutil.HelmReleaseID("", "driver-hr")}, want: []string{"child-hr"}, objects: 1},
		{name: "mixed kinds", roots: []manifest.NamedResource{testutil.HelmReleaseID("", ""), {Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}}, want: []string{"child-hr"}, objects: 1},
		{name: "driver kind", roots: []manifest.NamedResource{testutil.KustomizationID("", "")}, want: []string{"child-ks", "driver-hr", "driver-ks", "foreign", "static"}, objects: 1},
		{name: "namespace", roots: []manifest.NamedResource{testutil.KustomizationID("apps", ""), testutil.HelmReleaseID("apps", "")}, want: []string{"child-hr", "child-ks", "driver-hr", "driver-ks", "static"}, objects: 2},
		{name: "labels", roots: []manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, labels: map[string]string{"selected": "yes"}, want: []string{"child-ks", "driver-ks", "static"}, objects: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectEntryDocumentsCase2(t, &tc)
		},
		)
	}

	for _, shared := range []bool{true, false} {
		t.Run(fmt.Sprintf("paired output coverage/shared=%t", shared), func(t *testing.T) {
			t.Parallel()
			testSelectEntryDocumentsCase3(t, &shared)
		},
		)
	}

	t.Run(
		"failed and skipped declarations",
		func(t *testing.T) { t.Parallel(); testSelectEntryDocumentsFailedAndSkippedDeclarations(t) },
	)

	t.Run(
		"winning emissions retain driver descendants",
		func(t *testing.T) { t.Parallel(); testSelectEntryDocumentsWinningEmissionsRetainDriverDescendants(t) },
	)

	t.Run("proof-only batch", func(t *testing.T) { t.Parallel(); testSelectEntryDocumentsProofOnlyBatch(t) })

	t.Run(
		"changed-only both sides",
		func(t *testing.T) { t.Parallel(); testSelectEntryDocumentsChangedOnlyBothSides(t) },
	)
}

func TestSelect_EntryBatchSubjects(t *testing.T) {
	t.Parallel()
	run := selectedRun(nil)
	entry := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		"clusters/test",
		testutil.KustomizationID("apps", "captured"),
	)
	replacement := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID,
		"clusters/test",
		testutil.KustomizationID("apps", "replacement"),
	)
	ordinary := &manifest.Kustomization{Name: "ordinary", Namespace: "apps"}
	run.store.AddObject(replacement)
	run.store.AddObject(ordinary)
	side := selectionSide{run: run, result: &selection.Selection{
		Objects: []manifest.BaseManifest{ordinary},
		Manifests: []manifest.DocumentBatch{
			{Parent: entry},
			{Parent: ordinary},
			{},
		},
	}}
	side.prepareSubjects()
	assert.Diff(t, side.result.Subjects, map[manifest.NamedResource]manifest.DiagnosticIdentity{
		entry.Named(): {
			ID:       entry.Named(),
			Resource: testutil.KustomizationID("apps", "captured"),
			Path:     "clusters/test",
		},
	})
	assert.Equal(t, side.result.Manifests[0].Parent, manifest.BaseManifest(entry))
	assert.Equal(t, side.result.Manifests[1].Parent, manifest.BaseManifest(ordinary))
	assert.Equal(t, side.result.Manifests[2].Parent, nil)
}

func publishEntryForTest(run Rendered, docs []map[string]any) {
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	run.store.AddObject(entry)
	children, retained, failures := base.New(run.store, nil).
		ParseChildren(entry, testutil.DocumentOccurrences(docs), manifest.ParseDocOptions{})
	run.store.RecordEmitted(
		entry.Named(),
		&store.Emission{Children: children, Manifests: retained, Failures: failures, Complete: true},
		nil,
	)
	run.store.UpdateStatus(entry.Named(), store.StatusInfo{Status: store.StatusReady})
}

func testEntryCorrespondence(t *testing.T) {
	t.Helper()
	t.Parallel()
	for _, names := range [][2]string{{"owner", ""}, {"", "owner"}, {"old", "new"}, {"winner-a", "winner-b"}} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_to_%s/reverse_%t", names[0], names[1], reverse), func(t *testing.T) {
				t.Parallel()
				testEntryCorrespondenceCase(t, testEntryCorrespondenceCaseInputs{names: &names, reverse: &reverse})
			},
			)
		}
	}
}

func TestSelect_SyntheticFailureContext(t *testing.T) {
	t.Parallel()
	for _, owner := range []manifest.NamedResource{{}, testutil.KustomizationID("flux-system", "sync"), {Group: "fluxcd.controlplane.io", Kind: "FluxInstance", Namespace: "flux-system", Name: "flux"}} {
		t.Run(owner.String(), func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{})
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", owner)
			authored := &manifest.Kustomization{Name: "entry", Namespace: "apps"}
			run.store.AddObject(entry)
			run.store.AddObject(authored)
			run.store.UpdateFailure(entry.Named(), manifest.NewError(manifest.ErrInput, nil, "invalid package"))
			run.store.UpdateFailure(authored.Named(), manifest.NewError(manifest.ErrInput, nil, "authored failure"))
			run.Result.Failed = run.store.FailedResources()
			run.Err = run.aggregateFailures(run.Result.Failed)
			selected, err, _ := Select(
				[]Rendered{run},
				run.Err,
				[]manifest.NamedResource{testutil.KustomizationID("", "")},
				nil,
			)
			{
				assert.FatalIf(t, err == nil || strings.Contains(err.Error(), entry.Named().String()) ||
					!strings.Contains(err.Error(), `entry path "clusters/test"`) ||
					!strings.Contains(err.Error(), authored.Named().String()), "selected diagnostic=%v", err)
			}
			{
				assert.FatalIf(
					t,
					owner != (manifest.NamedResource{}) && !strings.Contains(err.Error(), owner.String()),
					"real owner lost: %v",
					err,
				)
			}
			if len(selected[0].Failed) != 2 || len(selected[0].Objects) != 1 ||
				selected[0].Objects[0].Named() != authored.Named() {
				t.Fatalf("display changed selection: %+v", selected[0])
			}
		})
	}
}

func testSelectEntryDocumentsCase(t *testing.T, testCaseInput *struct {
	name string

	owner manifest.NamedResource
},
) {
	t.Helper()

	run := selectedRun(&Result{})
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", testCaseInput.owner)
	run.store.AddObject(entry)
	run.store.UpdateStatus(entry.Named(), store.StatusInfo{Status: store.StatusReady})
	run.entryBinding = discovery.Binding{Producer: entry.Named(), Key: entry.Named()}
	docs := []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings"}},
	}
	run.Result.Manifests = []manifest.DocumentBatch{{Parent: entry, Documents: docs}}
	selected, err, remainder := Select(
		[]Rendered{run},
		nil,
		[]manifest.NamedResource{testutil.KustomizationID("", "")},
		nil,
	)
	assert.NoError(t, err)
	assert.NoError(t, remainder)
	res := selected[0]
	assert.Diff(t, res.Subjects, map[manifest.NamedResource]manifest.DiagnosticIdentity{
		entry.Named(): {ID: entry.Named(), Resource: testCaseInput.owner, Path: "clusters/test"},
	})
	assert.Equal(t, len(res.Objects)+len(res.Statuses)+len(res.Counts), 0)
	assert.Equal(t, len(res.Failed)+len(res.Blocked)+len(res.Warnings), 0)
	if len(res.Manifests) != 1 {
		t.Fatalf("selected batches: %+v", res.Manifests)
	}
	assert.Equal(t, res.Manifests[0].Parent, manifest.BaseManifest(entry))
	assert.Diff(t, res.Manifests[0].Documents, docs)
	assert.Diff(
		t,
		res.ProducerKeys,
		map[manifest.NamedResource]manifest.NamedResource{entry.Named(): entry.Named()},
	)
}

func testSelectEntryDocumentsCase2(t *testing.T, testCaseInput *struct {
	name string

	roots   []manifest.NamedResource
	labels  map[string]string
	want    []string
	objects int
},
) {
	t.Helper()

	run := selectedRun(&Result{})
	ks := &manifest.Kustomization{
		Name:      "driver-ks",
		Namespace: "apps",
		Labels:    map[string]string{"selected": "yes"},
	}
	hr := &manifest.HelmRelease{Name: "driver-hr", Namespace: "apps"}
	for _, obj := range []manifest.BaseManifest{ks, hr, &manifest.ConfigMap{Name: "dependency", Namespace: "apps"}} {
		run.store.AddObject(obj)
	}
	document := func(version, kind, namespace, name string) map[string]any {
		return map[string]any{
			"apiVersion": version,
			"kind":       kind,
			"metadata":   map[string]any{"name": name, "namespace": namespace},
		}
	}
	entryDocuments := []map[string]any{
		document("v1", manifest.KindConfigMap, "apps", "static"),
		document("v1", manifest.KindConfigMap, "other", "foreign"),
		document("kustomize.toolkit.fluxcd.io/v1", manifest.KindKustomization, "apps", ks.Name),
		document("helm.toolkit.fluxcd.io/v2", manifest.KindHelmRelease, "apps", hr.Name),
	}
	for _, i := range []int{0, 2} {
		entryDocuments[i]["metadata"].(map[string]any)["labels"] = map[string]any{"selected": "yes"}
	}
	publishEntryForTest(run, entryDocuments)
	for _, obj := range []manifest.BaseManifest{ks, hr} {
		name := "child-ks"
		if obj == hr {
			name = "child-hr"
		}
		run.store.RecordEmitted(
			obj.Named(),
			&store.Emission{
				Manifests: testutil.DocumentOccurrences(
					[]map[string]any{document("v1", manifest.KindConfigMap, "unrelated", name)},
				),
				Complete: true,
			},
			nil,
		)
		run.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	run.Result.Manifests = run.collectManifests()
	selected, err, _ := Select([]Rendered{run}, nil, testCaseInput.roots, testCaseInput.labels)
	assert.NoError(t, err)
	res := selected[0]
	assert.Equal(t, len(res.Objects), testCaseInput.objects)
	assert.Equal(t, len(res.Statuses), testCaseInput.objects)
	var names []string
	for _, batch := range res.Manifests {
		for _, doc := range batch.Documents {
			names = append(names, manifest.DocIdentity(doc).Name)
		}
	}
	slices.Sort(names)
	assert.Diff(t, names, testCaseInput.want)
}

func testSelectEntryDocumentsCase3(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	runs := make([]Rendered, 0, 2)
	for side := range 2 {
		run := selectedRun(&Result{})
		run.ID.Side = manifest.TreeSide(side)
		parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
		if side == 1 && !(*testCaseInput) {
			parent.Name = "other"
		}
		anchor := &manifest.Kustomization{Name: "anchor", Namespace: "apps"}
		release := &manifest.HelmRelease{Name: "release", Namespace: "media"}
		run.store.AddObjects([]manifest.BaseManifest{parent, anchor, release})
		declaration := map[string]any{
			"apiVersion": "helm.toolkit.fluxcd.io/v2",
			"kind":       manifest.KindHelmRelease,
			"metadata":   map[string]any{"name": release.Name, "namespace": release.Namespace},
		}
		static := map[string]any{
			"apiVersion": "v1",
			"kind":       manifest.KindConfigMap,
			"metadata":   map[string]any{"name": "unchanged", "namespace": release.Namespace},
		}
		publishEntryForTest(run, []map[string]any{static})
		run.store.RecordEmitted(
			parent.Named(),
			&store.Emission{
				Children:  []store.Child{{Obj: release, Doc: declaration}},
				Manifests: testutil.DocumentOccurrences([]map[string]any{declaration}),
				Complete:  true,
			},
			nil,
		)
		run.filter = change.NewFilter()
		selected := []manifest.NamedResource{anchor.Named()}
		if side == 1 {
			selected = []manifest.NamedResource{release.Named()}
		}
		_, retry := run.filter.AddEmitted(
			[]manifest.NamedResource{parent.Named(), anchor.Named(), release.Named()},
			selected,
			nil,
			0,
		)
		assert.Equal(t, retry, false)
		run.Result.Manifests = run.collectManifests()
		runs = append(runs, run)
	}
	selected, err, _ := Select(
		runs,
		nil,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
		nil,
	)
	assert.NoError(t, err)
	snapshots := [2]diff.Snapshot{}
	for i, side := range selected {
		snapshots[i] = diff.Snapshot{
			Manifests:    side.Manifests,
			Unavailable:  side.Unavailable,
			ProducerKeys: side.ProducerKeys,
		}
	}
	body, _, _, err := diff.Compare(snapshots[0], snapshots[1], nil, diff.Options{Format: diff.FormatJSON})
	assert.NoError(t, err)
	var output struct {
		Changes []json.RawMessage `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(body, &output))
	want := 0
	if !(*testCaseInput) {
		want = 1
	}
	assert.Equal(t, len(output.Changes), want)
}

func testSelectEntryDocumentsFailedAndSkippedDeclarations(t *testing.T) {
	t.Helper()

	for _, outcome := range []store.StatusInfo{
		{Status: store.StatusFailed, Message: "real driver failed"},
		{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "real driver skipped"},
		{Status: store.StatusReady, Reason: store.ReasonSuspended, Message: "suspended"},
	} {
		t.Run(outcome.Message, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{})
			ks := &manifest.Kustomization{Name: "actual", Namespace: "apps"}
			run.store.AddObject(ks)
			run.store.UpdateStatus(ks.Named(), outcome)
			publishEntryForTest(
				run,
				[]map[string]any{
					{
						"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
						"kind":       manifest.KindKustomization,
						"metadata":   map[string]any{"name": ks.Name, "namespace": ks.Namespace},
					},
				},
			)
			run.Result.Manifests = run.collectManifests()
			run.Result.Failed = run.store.FailedResources()
			if outcome.Status == store.StatusFailed {
				run.Err = &FailuresError{Message: outcome.Message}
			}
			selected, err, _ := Select(
				[]Rendered{run},
				run.Err,
				[]manifest.NamedResource{testutil.KustomizationID("", "")},
				nil,
			)
			{
				assert.FatalIf(
					t,
					(err != nil) != (outcome.Status == store.StatusFailed),
					"failure attribution: %v",
					err,
				)
			}
			if len(selected[0].Manifests) != 1 || selected[0].Manifests[0].Parent == nil ||
				len(selected[0].Objects) != 1 {
				t.Fatalf("real driver declaration lost: %+v", selected[0])
			}
		})
	}
}

func testSelectEntryDocumentsWinningEmissionsRetainDriverDescendants(t *testing.T) {
	t.Helper()

	run := selectedRun(&Result{})
	ks := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	hr := &manifest.HelmRelease{Name: "actual", Namespace: "apps"}
	run.store.AddObject(ks)
	run.store.AddObject(hr)
	declaration := map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2",
		"kind":       manifest.KindHelmRelease,
		"metadata":   map[string]any{"name": hr.Name, "namespace": hr.Namespace},
	}
	original := map[string]any{
		"apiVersion": "v1",
		"kind":       manifest.KindConfigMap,
		"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
		"data":       map[string]any{"marker": "built"},
	}
	winner := manifest.DeepCopyMap(original)
	winner["data"] = map[string]any{"marker": "emitted"}
	shared := &manifest.ConfigMap{Name: "shared", Namespace: "apps"}
	publishEntryForTest(run, []map[string]any{declaration, original})
	run.store.RecordEmitted(
		ks.Named(),
		&store.Emission{
			Children:  []store.Child{{Obj: hr, Doc: declaration}, {Obj: shared, Doc: winner}},
			Manifests: testutil.DocumentOccurrences([]map[string]any{declaration, winner}),
			Complete:  true,
		},
		nil,
	)
	descendant := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "descendant", "namespace": "apps"},
	}
	run.store.RecordEmitted(
		hr.Named(),
		&store.Emission{Manifests: testutil.DocumentOccurrences([]map[string]any{descendant}), Complete: true},
		nil,
	)
	for _, obj := range []manifest.BaseManifest{ks, hr} {
		run.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	run.Result.Manifests = run.collectManifests()
	selected, err, _ := Select(
		[]Rendered{run},
		nil,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
		nil,
	)
	assert.NoError(t, err)
	counts := map[string]int{}
	for _, batch := range selected[0].Manifests {
		{
			assert.FatalIf(t, batch.Parent == nil, "%v", "winning emissions duplicated as ownerless documents")
		}
		for _, doc := range batch.Documents {
			counts[manifest.DocIdentity(doc).Name]++
			if manifest.DocIdentity(doc) == shared.Named() {
				assert.Diff(t, doc["data"], map[string]any{"marker": "emitted"})
			}
		}
	}
	assert.Diff(t, counts, map[string]int{"actual": 1, "shared": 1, "descendant": 1})
	selected, err, _ = Select([]Rendered{run}, nil, []manifest.NamedResource{hr.Named()}, nil)
	assert.NoError(t, err)
	{
		assert.FatalIf(t, len(selected[0].Manifests) != 1 ||
			manifest.DocIdentity(
				selected[0].Manifests[0].Documents[0],
			).Name != "descendant", "%v", "deduplicated driver lost its independent output")
	}
}

func testSelectEntryDocumentsProofOnlyBatch(t *testing.T) {
	t.Helper()

	for _, namespace := range []string{"", "apps"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{})
			run.filter = change.NewFilter()
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
			_, retry := run.filter.AddEmitted([]manifest.NamedResource{entry.Named()}, nil, nil, 0)
			assert.Equal(t, retry, false)
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       manifest.KindConfigMap,
				"metadata":   map[string]any{"name": "unselected", "namespace": namespace},
			}
			publishEntryForTest(run, []map[string]any{doc})
			run.Result.Manifests = run.collectManifests()
			assert.Equal(t, len(run.Result.Manifests), 1)
			selected, err, _ := Select(
				[]Rendered{run},
				nil,
				[]manifest.NamedResource{testutil.KustomizationID("", "")},
				nil,
			)
			assert.NoError(t, err)
			assert.Equal(t, len(selected[0].Manifests), 0)
		})
	}
}

func testSelectEntryDocumentsChangedOnlyBothSides(t *testing.T) {
	t.Helper()

	runs := make([]Rendered, 0, 2)
	for _, name := range []string{"removed", "added"} {
		run := selectedRun(&Result{})
		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       manifest.KindConfigMap,
			"metadata":   map[string]any{"name": name, "namespace": "apps"},
		}
		unrelated := map[string]any{
			"apiVersion": "v1",
			"kind":       manifest.KindConfigMap,
			"metadata":   map[string]any{"name": "unrelated", "namespace": "apps"},
		}
		publishEntryForTest(run, []map[string]any{doc, unrelated})
		entry := manifest.NewEntryKustomization(
			manifest.BootstrapSourceID,
			".",
			testutil.KustomizationID("apps", name),
		)
		run.store.AddObject(entry)
		run.entryBinding = discovery.Binding{Producer: entry.Named(), Key: entry.Named()}
		id := manifest.DocIdentity(doc)
		run.index = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{id: {}},
			Readers: map[string][]manifest.FileReader{
				"kustomization.yaml": {{Reader: id, Source: manifest.BootstrapSourceID}},
			},
		}
		run.loader = admissiontest.New(
			t,
			admissiontest.Config{
				Changes:   change.NewSet([]string{"kustomization.yaml"}),
				Index:     run.index,
				Inventory: run.store,
				Root:      "",
			},
		)
		admissiontest.Commit(t, run.loader, nil)
		run.filter = run.loader.Options.Filter
		run.Result.Manifests = run.collectManifests()
		runs = append(runs, run)
	}
	selected, err, _ := Select(
		runs,
		nil,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
		nil,
	)
	assert.NoError(t, err)
	for i, name := range []string{"removed", "added"} {
		if len(selected[i].Manifests) != 1 || len(selected[i].Manifests[0].Documents) != 2 ||
			manifest.DocIdentity(selected[i].Manifests[0].Documents[0]).Name != name {
			t.Fatalf("side %d selected wrong changed documents: %+v", i, selected[i].Manifests)
		}
		assert.Equal(t, len(selected[i].Objects), 0)
		assert.Equal(t, len(selected[i].Statuses), 0)
		assert.Equal(t, len(selected[i].Counts), 0)
		id := runs[i].entryBinding.Producer
		assert.Equal(t, selected[i].Manifests[0].Parent.Named(), id)
		assert.Diff(t, selected[i].ProducerKeys, map[manifest.NamedResource]manifest.NamedResource{id: id})
		assert.Diff(t, selected[i].Subjects, map[manifest.NamedResource]manifest.DiagnosticIdentity{
			id: {ID: id, Resource: testutil.KustomizationID("apps", name), Path: "."},
		})
	}
}

type testEntryCorrespondenceCaseInputs struct {
	names   *[2]string
	reverse *bool
}

func testEntryCorrespondenceCase(t *testing.T, testCaseInput testEntryCorrespondenceCaseInputs) {
	t.Helper()

	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	runs := []Rendered{selectedRun(&Result{}), selectedRun(&Result{})}
	for i, name := range *testCaseInput.names {
		obj := entry.Clone()
		if name != "" {
			obj = &manifest.Kustomization{Name: name, Namespace: "apps", Path: "."}
		}
		runs[i].store.AddObject(obj)
		runs[i].entryBinding = discovery.Binding{
			Producer: obj.Named(),
			Key:      entry.Named(),
			Source:   manifest.BootstrapSourceID,
			Path:     ".",
		}
		runs[i].store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady})
		if i == 0 {
			docs := []map[string]any{
				{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata":   map[string]any{"name": "root-inventory"},
				},
			}
			runs[i].Result.Manifests = []manifest.DocumentBatch{{Parent: obj, Documents: docs}}
		} else {
			runs[i].store.UpdateStatus(
				obj.Named(),
				store.StatusInfo{Status: store.StatusFailed, Message: "entry failed"},
			)
		}
	}
	old := runs[0].entryBinding.Producer
	unrelated := &manifest.Kustomization{Name: old.Name, Namespace: old.Namespace, Path: "./unrelated"}
	if !old.ReportHidden() && old != runs[1].entryBinding.Producer {
		runs[1].store.AddObject(unrelated)
		runs[1].store.UpdateStatus(unrelated.Named(), store.StatusInfo{Status: store.StatusReady})
		runs[1].Result.Manifests = []manifest.DocumentBatch{
			{
				Parent: unrelated,
				Documents: []map[string]any{
					{
						"apiVersion": "v1",
						"kind":       "ConfigMap",
						"metadata":   map[string]any{"name": "unrelated-output"},
					},
				},
			},
		}
	}
	if *testCaseInput.reverse {
		slices.Reverse(runs)
	}
	selected, _, _ := Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID("", "")}, nil)
	failed := 1
	if *testCaseInput.reverse {
		failed = 0
	}
	if selected[failed].Unavailable[entry.Named()].String() != "entry failed" {
		t.Fatalf("entry correspondence read an unrelated status: %+v", selected[failed].Unavailable)
	}
	left := diff.Snapshot{
		Subjects:     selected[0].Subjects,
		Manifests:    selected[0].Manifests,
		Unavailable:  selected[0].Unavailable,
		ProducerKeys: selected[0].ProducerKeys,
	}
	right := diff.Snapshot{
		Subjects:     selected[1].Subjects,
		Manifests:    selected[1].Manifests,
		Unavailable:  selected[1].Unavailable,
		ProducerKeys: selected[1].ProducerKeys,
	}
	output, _, notes, err := diff.Compare(left, right, nil, diff.Options{Format: diff.FormatJSON})
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			len(notes) != 1 || notes[0].Count != 1 || notes[0].Parent.NamedResource != old,
			"withheld inventory attribution=%+v",
			notes,
		)
	}
	{
		assert.FatalIf(t, strings.Contains(string(output), "root-inventory") ||
			(!old.ReportHidden() && old != runs[failed].entryBinding.Producer && !strings.Contains(string(output), "unrelated-output")), "suppression leaked inventory or hid retargeted owner: %s", output)
	}
}
