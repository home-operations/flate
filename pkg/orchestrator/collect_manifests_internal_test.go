package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestCollectManifests_SupportDeclarations(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(owner)
	documents := []map[string]any{
		{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": "selected", "namespace": "apps"},
		},
		{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "GitRepository",
			"metadata":   map[string]any{"name": "support", "namespace": "apps"},
			"spec":       map[string]any{"url": "https://fixture.invalid/repo"},
		},
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "sibling", "namespace": "apps"}},
	}
	children, documents := parseChildrenForTest(owner.Named(), documents, manifest.ParseDocOptions{})
	st.RecordEmitted(
		owner.Named(),
		&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(documents), Complete: true},
		nil,
	)
	filter := change.NewFilter()
	_, retry := filter.AddEmitted(
		[]manifest.NamedResource{owner.Named(), children[1].Obj.Named()},
		[]manifest.NamedResource{children[0].Obj.Named()},
		nil,
		0,
	)
	assert.Equal(t, retry, false)
	o := &Orchestrator{store: st, filter: filter}
	assert.Diff(
		t,
		outputByParent(o.collectManifests()),
		map[manifest.NamedResource][]map[string]any{owner.Named(): documents[:2]},
	)
}

func TestCollectManifests_FailedSuccessorRetainsCompleted(t *testing.T) {
	t.Parallel()

	st := store.New()

	ready := testutil.HelmReleaseID("app", "ready")
	st.AddObject(&manifest.HelmRelease{Name: ready.Name, Namespace: ready.Namespace})
	st.RecordEmitted(ready, &store.Emission{Manifests: testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "ready-cm"}},
	})}, nil)
	st.UpdateStatus(ready, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})

	failedHR := testutil.HelmReleaseID("app", "broken")
	hr := &manifest.HelmRelease{Name: failedHR.Name, Namespace: failedHR.Namespace}
	st.AddObject(hr)
	_, _, completed := st.GetEmission(failedHR)
	completed.Manifests = testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "broken-deploy"}},
	})
	completed.Complete = true
	st.RecordEmitted(failedHR, &completed, nil)
	completed.Finish()
	successor := hr.Clone()
	successor.Chart.Name = "different"
	st.AddObject(successor)
	{
		assert.FatalIf(
			t,
			st.GetArtifact(failedHR, store.ArtifactContent) != nil,
			"%v",
			"successor exposed a current-input artifact",
		)
	}
	failure := "values don't meet the specifications of the schema(s)"
	st.UpdateStatus(failedHR, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: failure})

	failedKS := testutil.KustomizationID("flux-system", "apps")
	st.AddObject(&manifest.Kustomization{Name: failedKS.Name, Namespace: failedKS.Namespace})
	st.RecordEmitted(failedKS, &store.Emission{Manifests: testutil.DocumentOccurrences([]map[string]any{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "apps"}},
	})}, nil)
	st.UpdateStatus(failedKS, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "kustomize build boom"})

	o := &Orchestrator{store: st}
	got := outputByParent(o.collectManifests())

	{
		{
			_, ok := got[ready]
			assert.ErrorIf(t, !ok, "passing HelmRelease %s must contribute its rendered children", ready)
		}
	}
	{
		{
			docs, ok := got[failedHR]
			assert.ErrorIf(
				t,
				!ok || len(docs) != 1,
				"failed HelmRelease %s lost completed output; got %d docs",
				failedHR,
				len(docs),
			)
		}
	}
	{
		{
			_, ok := got[failedKS]
			assert.ErrorIf(t, !ok, "failed Kustomization %s lost completed output", failedKS)
		}
	}
	{
		{
			info, ok := st.FailedResources()[failedHR]
			assert.ErrorIf(
				t,
				!ok || !strings.Contains(info.Message, failure),
				"failed successor lost diagnostics: %+v",
				info,
			)
		}
	}
	st.DeleteObject(failedHR)
	{
		{
			_, ok := outputByParent(o.collectManifests())[failedHR]
			assert.FatalIf(t, ok, "%v", "hidden producer contributed completed output")
		}
	}
	t.Run("ResourceSets", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		owners := []*manifest.ResourceSet{{Name: "a", Namespace: "apps"}, {Name: "z", Namespace: "apps"}}
		parent := testutil.KustomizationID("apps", "parent")
		st.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
		var winningDocs []map[string]any
		var winningChild store.Child
		for _, owner := range owners {
			owner.ResourcesTemplate = "accepted-" + owner.Name
			st.AddObject(owner)
			index.Records[owner.Named()] = manifest.DiscoveryRecord{Parent: parent}
			_, _, completed := st.GetEmission(owner.Named())
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
				"data":       map[string]any{"value": owner.Name},
			}
			children, retained := parseChildrenForTest(owner.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
			completed.Children, completed.Manifests = children, testutil.DocumentOccurrences(retained)
			completed.Complete = true
			st.RecordEmitted(owner.Named(), &completed, nil)
			completed.Finish()
			st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			winningDocs, winningChild = testutil.DocumentMaps(completed.Manifests), completed.Children[0]
		}
		winner := owners[1].Named()
		successor := *owners[1]
		successor.ResourcesTemplate = "invalid successor"
		st.AddObject(&successor)
		assert.Equal(t, st.GetArtifact(winner, store.ArtifactContent), store.Artifact(nil))
		st.UpdateStatus(
			winner,
			store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "invalid ResourceSet template"},
		)
		o := &Orchestrator{store: st, index: index}
		assert.Diff(
			t,
			outputByParent(o.collectManifests()),
			map[manifest.NamedResource][]map[string]any{parent: winningDocs},
		)
		assert.Equal(t, st.GetObject(winningChild.Obj.Named()), winningChild.Obj)
		assert.Equal(t, st.EmissionOwner(winningChild.Obj.Named()), winner)
		assert.Diff(t, st.Warnings(), []manifest.Warning{
			{
				Resource: winningChild.Obj.Named(),
				Subject:  manifest.DiagnosticFor(winningChild.Obj.Named(), nil),
				Related:  []manifest.NamedResource{owners[0].Named(), winner, winner},
				Category: manifest.WarnConflictingProducers,
				Count:    1,
				Message:  "conflicting producers; using " + winner.String(),
				Detail:   []string{owners[0].Named().String(), winner.String()},
			},
		})
		info, ok := st.FailedResources()[winner]
		assert.Equal(t, ok, true)
		assert.Equal(t, info.Message, "invalid ResourceSet template")
	})
}

func TestCollectManifests_MissingParent(t *testing.T) {
	t.Parallel()

	st := store.New()
	parent := testutil.KustomizationID("apps", "missing")
	rs := &manifest.ResourceSet{Name: "resources", Namespace: "apps"}
	st.AddObject(rs)
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "settings", "namespace": "apps"},
	}
	children, docs := testutil.ParseChildren(base.New(st, nil), rs, []map[string]any{doc}, manifest.ParseDocOptions{})
	st.RecordEmitted(
		rs.Named(),
		&store.Emission{Children: children, Manifests: testutil.DocumentOccurrences(docs), Complete: true},
		nil,
	)
	o := &Orchestrator{
		store: st,
		index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			rs.Named(): {Parent: parent},
		}},
	}
	{
		{
			batches := o.collectManifests()
			assert.FatalIf(t, len(batches) != 0, "missing parent contributed output: %+v", batches)
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_EntryApplicationConflictingProducers(t *testing.T) {
	run := func() *Result {
		t.Helper()
		root := t.TempDir()
		const url = "https://example.test/entry/conflicts"
		testutil.WriteFile(
			t,
			root,
			"entry/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/literal.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: literal, namespace: apps}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/workload.yaml",
			testutil.KustomizationYAML("workload", "apps", "  path: ./workload\n"),
		)
		testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: []\n")
		for _, producer := range []struct{ name, path string }{
			{name: "a", path: "./entry"},
			{name: "z", path: "./elsewhere"},
		} {
			testutil.WriteFile(
				t,
				root,
				"entry/"+producer.name+".yaml",
				fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: apps}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: fluxcd.controlplane.io/v1
      kind: FluxInstance
      metadata: {name: flux, namespace: apps}
      spec:
        sync: {name: self, kind: GitRepository, url: '%s', ref: main, path: %s}
`, producer.name, url, producer.path),
			)
		}
		o, err := newTestOrchestrator(
			t,
			Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}},
			Config{CacheDir: t.TempDir(), Concurrency: 2},
		)
		assert.NoError(t, err)
		assert.NoError(t, o.bootstrap(t.Context()))
		assert.Equal(t, o.entryBinding.Producer, manifest.NamedResource{})
		id := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      "FluxInstance",
			Namespace: "apps",
			Name:      "flux",
		}
		firstPublication := make(chan struct{})
		var released sync.Once
		o.controllers.ResourceSet = entryPublicationController{
			reconcileController: o.rsc,
			tasks:               o.tasks,
			first:               firstPublication,
		}
		var mu sync.Mutex
		var published []string
		unsubscribe := o.store.AddListener(store.EventObjectAdded, func(added manifest.NamedResource, payload any) {
			if added == id {
				obj := payload.(*manifest.RawObject)
				application, valid := obj.EntryApplication()
				if valid {
					mu.Lock()
					published = append(published, application.Path)
					mu.Unlock()
					if application.Path == "./entry" {
						released.Do(func() { close(firstPublication) })
					}
				}
			}
		}, false)
		defer unsubscribe()
		result, err := o.collectResult(o.run(t.Context()))
		assert.NoError(t, err)
		assert.Diff(t, published, []string{"./entry", "./elsewhere"})
		assert.Equal(t, o.entryBinding.Producer, manifest.NamedResource{})
		winner := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "z",
		}
		assert.Equal(t, o.store.EmissionOwner(id), winner)
		obj := o.store.GetObject(id).(*manifest.RawObject)
		application, valid := obj.EntryApplication()
		assert.Equal(t, valid, true)
		assert.Equal(t, application.Path, "./elsewhere")
		for _, docs := range outputByParent(result.Manifests) {
			for _, doc := range docs {
				name, _ := manifest.DocMetadata(doc)
				{
					assert.ErrorIf(
						t,
						manifest.DocKind(doc) == manifest.KindConfigMap && name == "literal",
						"%v",
						"retired application admitted the literal root document",
					)
				}
			}
		}
		if len(result.Warnings) != 1 {
			t.Errorf("warnings: %+v", result.Warnings)
		}
		if len(result.Warnings) == 1 {
			warning := result.Warnings[0]
			assert.Equal(t, warning.Resource, id)
			assert.Equal(t, warning.Category, manifest.WarnConflictingProducers)
			assert.Equal(t, warning.Count, 1)
			assert.Equal(t, warning.Message, "conflicting producers; using "+winner.String())
			loser := winner
			loser.Name = "a"
			assert.Diff(t, warning.Detail, []string{loser.String(), winner.String()})
		}
		return result
	}
	first, second := run(), run()
	assert.Diff(t, outputByParent(first.Manifests), outputByParent(second.Manifests))
	assert.Diff(t, first.Warnings, second.Warnings)
}

type entryPublicationController struct {
	reconcileController

	tasks *task.Service
	first <-chan struct{}
}

func (c entryPublicationController) ReconcileNode(
	ctx context.Context,
	id manifest.NamedResource,
	level manifest.DrainLevel,
) *base.Execution {
	if id.Name == "z" {
		c.tasks.YieldSlot(func() {
			select {
			case <-c.first:
			case <-ctx.Done():
			}
		})
	}
	return c.reconcileController.ReconcileNode(ctx, id, level)
}

func TestCollectManifests_ConflictingProducers(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()
				testCollectManifestsConflictingProducersCase(
					t,
					testCollectManifestsConflictingProducersCaseInputs{workers: &workers, reverse: &reverse},
				)
			},
			)
		}
	}
}

func TestCollectManifests_ParentlessResourceSetSharedChild(t *testing.T) {
	t.Parallel()

	for _, parentlessWinner := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("parentlessWinner=%t/reverse=%t", parentlessWinner, reverse), func(t *testing.T) {
				t.Parallel()
				testCollectManifestsParentlessResourceSetSharedChildCase(
					t,
					testCollectManifestsParentlessResourceSetSharedChildCaseInputs{
						parentlessWinner: &parentlessWinner,
						reverse:          &reverse,
					},
				)
			},
			)
		}
	}
}

func TestCollectManifests_ResourceSetConflictingProducers(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret, "ServiceAccount"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", kind, reverse), func(t *testing.T) {
				t.Parallel()
				testCollectManifestsResourceSetConflictingProducersCase(
					t,
					testCollectManifestsResourceSetConflictingProducersCaseInputs{kind: &kind, reverse: &reverse},
				)
			},
			)
		}
	}
}

type testCollectManifestsConflictingProducersCaseInputs struct {
	workers *int
	reverse *bool
}

func testCollectManifestsConflictingProducersCase(
	t *testing.T,
	testCaseInput testCollectManifestsConflictingProducersCaseInputs,
) {
	t.Helper()

	st := store.New()
	owners := make([]manifest.NamedResource, (*testCaseInput.workers))
	release := make([]chan struct{}, (*testCaseInput.workers))
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range *testCaseInput.workers {
		obj := &manifest.Kustomization{Name: strconv.Itoa(i), Namespace: "apps"}
		owners[i] = obj.Named()
		st.AddObject(obj)
		release[i] = make(chan struct{})
		wg.Go(func() {
			<-release[i]
			doc := map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
				"data":       map[string]any{"value": strconv.Itoa(i)},
			}
			children, docs := parseChildrenForTest(
				owners[i],
				[]map[string]any{doc},
				manifest.ParseDocOptions{},
			)
			st.RecordEmitted(
				owners[i],
				&store.Emission{
					Children:  children,
					Manifests: testutil.DocumentOccurrences(docs),
					Complete:  true,
				},
				nil,
			)
			done <- struct{}{}
		})
	}
	order := slices.Clone(release)
	if *testCaseInput.reverse {
		slices.Reverse(order)
	}
	for _, ch := range order {
		close(ch)
		<-done
	}
	wg.Wait()
	o := &Orchestrator{store: st}
	got := outputByParent(o.collectManifests())
	{
		assert.FatalIf(t, len(got) != (*testCaseInput.workers), "output lost producer documents: %v", got)
	}
	for i, owner := range owners {
		if len(got[owner]) != 1 || got[owner][0]["data"].(map[string]any)["value"] != strconv.Itoa(i) {
			t.Fatalf("output lost %s contribution: %v", owner, got[owner])
		}
	}
	for _, owner := range owners {
		children, docs := parseChildrenForTest(
			owner,
			got[owners[(*testCaseInput.workers)-1]],
			manifest.ParseDocOptions{},
		)
		st.RecordEmitted(
			owner,
			&store.Emission{
				Children:  children,
				Manifests: testutil.DocumentOccurrences(docs),
				Complete:  true,
			},
			nil,
		)
	}
	{
		{
			got := outputByParent(o.collectManifests())
			assert.FatalIf(t, len(got) != (*testCaseInput.workers), "equal ordinary producer groups changed: %v", got)
		}
	}
}

type testCollectManifestsParentlessResourceSetSharedChildCaseInputs struct {
	parentlessWinner *bool
	reverse          *bool
}

func testCollectManifestsParentlessResourceSetSharedChildCase(
	t *testing.T,
	testCaseInput testCollectManifestsParentlessResourceSetSharedChildCaseInputs,
) {
	t.Helper()

	st := store.New()
	parent := &manifest.Kustomization{Name: "p", Namespace: "apps"}
	a := &manifest.ResourceSet{Name: "a", Namespace: "apps"}
	z := &manifest.ResourceSet{Name: "z", Namespace: "apps"}
	st.AddObject(parent)
	st.AddObject(a)
	st.AddObject(z)
	attributed := z.Named()
	if *testCaseInput.parentlessWinner {
		attributed = a.Named()
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		attributed: {Parent: parent.Named()},
	}}
	o := &Orchestrator{store: st, index: index}
	ids := []manifest.NamedResource{a.Named(), z.Named()}
	if *testCaseInput.reverse {
		slices.Reverse(ids)
	}
	var winningDocs []map[string]any
	for _, id := range ids {
		batch := []map[string]any{
			{
				"apiVersion": "example.com/v1",
				"kind":       "Widget",
				"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
				"spec":       map[string]any{"value": id.Name},
			},
			{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
				"data":       map[string]any{"key": id.Name},
			},
			{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata":   map[string]any{"name": "shared", "namespace": "apps"},
				"stringData": map[string]any{"key": id.Name},
			},
		}
		children, docs := parseChildrenForTest(id, batch, manifest.ParseDocOptions{})
		{
			assert.FatalIf(
				t,
				len(children) != len(batch) || len(docs) != len(batch),
				"%s must publish every child: %v, %v",
				id,
				children,
				docs,
			)
		}
		st.RecordEmitted(
			id,
			&store.Emission{
				Children:  children,
				Manifests: testutil.DocumentOccurrences(docs),
				Complete:  true,
			},
			nil,
		)
		if id == z.Named() {
			winningDocs = docs
		}
	}
	want := map[manifest.NamedResource][]map[string]any{}
	if !(*testCaseInput.parentlessWinner) {
		want[parent.Named()] = winningDocs
	}
	assert.Diff(t, outputByParent(o.collectManifests()), want)
	assert.Diff(t, outputByParent(o.collectManifests()), want)
	cm, ok := st.Get[*manifest.ConfigMap](
		manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "shared"},
	)
	{
		assert.FatalIf(t, !ok, "%v", "missing shared ConfigMap")
	}
	assert.Diff(t, cm.Data, map[string]any{"key": "z"})
	warnings := st.Warnings()
	assert.Equal(t, len(warnings), 3)
	{
		wValues := warnings
		for wIndex := range wValues {
			w := wValues[wIndex]

			assert.Equal(t, st.EmissionOwner(w.Resource), z.Named())
			assert.Equal(t, w.Message, "conflicting producers; using "+z.Named().String())
			assert.Diff(t, w.Detail, []string{a.Named().String(), z.Named().String()})
		}
	}
}

type testCollectManifestsResourceSetConflictingProducersCaseInputs struct {
	kind    *string
	reverse *bool
}

func testCollectManifestsResourceSetConflictingProducersCase(
	t *testing.T,
	testCaseInput testCollectManifestsResourceSetConflictingProducersCaseInputs,
) {
	t.Helper()

	st := store.New()
	owners := []*manifest.ResourceSet{{Name: "a", Namespace: "apps"}, {Name: "z", Namespace: "apps"}}
	parents := []manifest.NamedResource{
		testutil.KustomizationID("apps", "parent-a"),
		testutil.KustomizationID("apps", "parent-z"),
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	emissions := make([]store.Emission, 2)
	for i, owner := range owners {
		st.AddObject(&manifest.Kustomization{Name: parents[i].Name, Namespace: parents[i].Namespace})
		st.AddObject(owner)
		index.Records[owner.Named()] = manifest.DiscoveryRecord{Parent: parents[i]}
		doc := map[string]any{
			"apiVersion": "v1",
			"kind":       (*testCaseInput.kind),
			"metadata": map[string]any{
				"name":        "shared",
				"namespace":   "apps",
				"annotations": map[string]any{"value": owner.Name},
			},
		}
		if (*testCaseInput.kind) == manifest.KindConfigMap {
			doc["data"] = map[string]any{"value": owner.Name}
		}
		if (*testCaseInput.kind) == manifest.KindSecret {
			doc["stringData"] = map[string]any{"value": owner.Name}
		}
		children, docs := parseChildrenForTest(
			owner.Named(),
			[]map[string]any{doc},
			manifest.ParseDocOptions{},
		)
		emissions[i] = store.Emission{
			Children:  children,
			Manifests: testutil.DocumentOccurrences(docs),
			Complete:  true,
		}
	}
	release, done := make(chan struct{}), make(chan struct{})
	first, second := 0, 1
	if *testCaseInput.reverse {
		first, second = second, first
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		st.RecordEmitted(owners[first].Named(), &emissions[first], nil)
		close(release)
	})
	wg.Go(func() {
		<-release
		st.RecordEmitted(owners[second].Named(), &emissions[second], nil)
		close(done)
	})
	<-done
	wg.Wait()
	o := &Orchestrator{store: st, index: index}
	id := emissions[1].Children[0].Obj.Named()
	check := func(winner int) {
		t.Helper()
		want := map[manifest.NamedResource][]map[string]any{
			parents[winner]: testutil.DocumentMaps(emissions[winner].Manifests),
		}
		assert.Diff(t, outputByParent(o.collectManifests()), want)
		assert.Diff(t, outputByParent(o.collectManifests()), want)
		assert.Equal(t, st.EmissionOwner(id), owners[winner].Named())
		assert.Equal(t, st.GetObject(id), emissions[winner].Children[0].Obj)
	}
	check(1)
	assert.Diff(t, st.Warnings(), []manifest.Warning{
		{
			Resource: id,
			Subject:  manifest.DiagnosticFor(id, nil),
			Related:  []manifest.NamedResource{owners[0].Named(), owners[1].Named(), owners[1].Named()},
			Category: manifest.WarnConflictingProducers,
			Count:    1,
			Message: "conflicting producers; using " + owners[1].Named().
				String(),
			Detail: []string{owners[0].Named().String(), owners[1].Named().String()},
		},
	})
	st.RecordEmitted(owners[1].Named(), &store.Emission{Complete: true}, nil)
	check(0)
	assert.Equal(t, len(st.Warnings()), 0)
	st.RecordEmitted(owners[1].Named(), &emissions[1], nil)
	st.RecordEmitted(owners[0].Named(), &emissions[1], nil)
	check(1)
	assert.Equal(t, len(st.Warnings()), 0)
}
