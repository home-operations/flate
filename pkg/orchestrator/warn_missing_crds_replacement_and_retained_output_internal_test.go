package orchestrator

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	resourcesetctrl "github.com/home-operations/flate/pkg/controllers/resourceset"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestWarnMissingCRDs_ReplacementAndRetainedOutput(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"unchanged", "remove", "add", "failed gate", "filter remove", "filter add", "dedup"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testWarnMissingCRDsReplacementAndRetainedOutputCase(
				t,
				&state,
			)
		},
		)
	}
}

func TestMissingCRDs_SharedWarningsAndOutput(t *testing.T) {
	t.Parallel()

	var first *Result
	cache := t.TempDir()
	dir := t.TempDir()
	testutil.GitInit(t, dir)
	for range 2 {
		testutil.WriteFile(t, dir, "flux/ks.yaml", ksYAML("apps", "apps", ""))
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources:\n- sets.yaml\n")
		var sets strings.Builder
		for _, name := range []string{"z", "a", "unrelated"} {
			dependencies := ""
			if name != "unrelated" {
				dependencies = ` dependsOn:
 - {apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: z.example.com}
 - {apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: a.example.com}
 - {apiVersion: apiextensions.k8s.io/v1, kind: CustomResourceDefinition, name: a.example.com}
`
			}
			fmt.Fprintf(&sets, `---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: flux-system}
spec:
%s resources:
 - apiVersion: example.com/v1
   kind: Widget
   metadata: {name: %s, namespace: flux-system}
   spec: {value: %s}
`, name, dependencies, name, name)
		}
		testutil.WriteFile(t, dir, "apps/sets.yaml", sets.String())
		renderedSingleton2, err := renderTreesForTest(t.Context(), t,
			[]Tree{{Path: dir, RepoRoot: dir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}},
			Config{CacheDir: cache, Concurrency: 2, AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}},
		)
		assert.FatalIf(t, len(renderedSingleton2) != 1, "missing render: %v", err)
		o := renderedSingleton2[0].Orchestrator
		res := renderedSingleton2[0].Result
		assert.FatalIf(t, err != nil, "%v", err)
		want := slices.Grow([]manifest.Warning{}, 2-0)
		for _, name := range []string{"a.example.com", "z.example.com"} {
			crd := manifest.NamedResource{
				Group: "apiextensions.k8s.io",
				Kind:  manifest.KindCustomResourceDefinition,
				Name:  name,
			}
			want = append(
				want,
				manifest.Warning{
					Resource: crd,
					Subject:  manifest.DiagnosticFor(crd, nil),
					Related: []manifest.NamedResource{
						{
							Group:     "fluxcd.controlplane.io",
							Kind:      manifest.KindResourceSet,
							Namespace: "flux-system",
							Name:      "a",
						},
						{
							Group:     "fluxcd.controlplane.io",
							Kind:      manifest.KindResourceSet,
							Namespace: "flux-system",
							Name:      "z",
						},
					},
					Category: manifest.WarnMissingCRD,
					Count:    1,
					Message:  "CRD " + crd.String() + " absent from offline inputs; accepted for ResourceSet.fluxcd.controlplane.io/flux-system/a, ResourceSet.fluxcd.controlplane.io/flux-system/z",
				},
			)
		}
		assert.Diff(t, res.Warnings, want)
		for _, name := range []string{"a", "z", "unrelated"} {
			id := manifest.NamedResource{
				Group:     "fluxcd.controlplane.io",
				Kind:      manifest.KindResourceSet,
				Namespace: "flux-system",
				Name:      name,
			}
			status, ok := o.store.GetStatus(id)
			assert.Equal(t, ok, true)
			assert.Equal(t, status.Status, store.StatusReady)
			art, ok := o.store.GetArtifact(id, store.ArtifactContent).(*store.Emission)
			assert.FatalIf(t, !ok || len(art.Manifests) != 1, "%s artifact=%+v", id, art)
			raw, ok := o.store.GetObject(manifest.NamedResource{Group: "example.com", Kind: "Widget", Namespace: "flux-system", Name: name}).(*manifest.RawObject)
			assert.FatalIf(t, !ok, "missing emitted Widget/%s", name)
			assert.Diff(t, raw.Spec, map[string]any{"value": name})
		}
		if first == nil {
			first = res
		} else {
			assert.Diff(t, outputByParent(res.Manifests), outputByParent(first.Manifests))
			assert.Diff(t, res.Warnings, first.Warnings)
		}
	}
}

func testWarnMissingCRDsReplacementAndRetainedOutputCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	s := store.New()
	tasks := task.NewBounded(2)
	o := &Orchestrator{
		store: s,
		cfg: treeConfig{
			Config: Config{AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}},
			Tree:   Tree{},
		},
	}
	parent := testutil.KustomizationID("flux-system", "parent")
	s.UpdateStatus(parent, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	opts := base.Options{AllowMissingKinds: []string{manifest.KindCustomResourceDefinition}}
	c := resourcesetctrl.New(s, tasks, nil)
	c.Configure(opts)
	c.StartLifecycle()

	t.Cleanup(tasks.BlockTillDone)
	crd := fluxopv1.Dependency{
		APIVersion: "apiextensions.k8s.io/v1",
		Kind:       manifest.KindCustomResourceDefinition,
		Name:       "widgets.example.com",
	}
	rs := &manifest.ResourceSet{
		Name:              "app",
		Namespace:         "ns",
		ResourcesTemplate: "apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: old}\nspec: {value: old}\n",
	}
	if (*testCaseInput) != "add" && (*testCaseInput) != "filter add" {
		rs.DependsOn = []fluxopv1.Dependency{crd}
	}
	for _, dep := range rs.DependsOn {
		manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
	}
	s.AddObject(rs)
	initial := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainCascade, schedule.Result{})
	assert.FatalIf(t, initial.Outcome() == schedule.Blocked,
		"initial render blocked: %v", initial.Blocked)

	t.Cleanup(initial.Close)
	before := s.GetArtifact(rs.Named(), store.ArtifactContent)
	fresh := *rs
	fresh.ResourceSetSpec = *rs.DeepCopy()
	switch *testCaseInput {
	case "remove", "filter remove":
		fresh.DependsOn = nil
	case "add", "filter add":
		fresh.DependsOn = []fluxopv1.Dependency{crd}
	case "failed gate":
		fresh.DependsOn = append(
			fresh.DependsOn,
			fluxopv1.Dependency{
				APIVersion: manifest.CoreAPIVersion,
				Kind:       manifest.KindConfigMap,
				Name:       "missing",
			},
		)
	}
	if (*testCaseInput) == "remove" || (*testCaseInput) == "add" {
		fresh.ResourcesTemplate = strings.ReplaceAll(rs.ResourcesTemplate, "old", "new")
	}
	for _, dep := range fresh.DependsOn {
		manifest.KindOf(&manifest.RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
	}
	s.AddObject(&fresh)
	if strings.HasPrefix((*testCaseInput), "filter") {
		opts.Loader, opts.Session = admissiontest.New(
			t,
			admissiontest.Config{Changes: change.NewSet(nil), Index: indexForTest(nil), Inventory: s, Root: ""},
		)
		admissiontest.Commit(t, opts.Session, nil)

		c = resourcesetctrl.New(s, tasks, nil)
		c.Configure(opts)
		c.StartLifecycle()

		s.SetIncluded(rs.Named(), false, false)
	}
	publications := 0
	u := s.AddListener(store.EventArtifactUpdated, func(manifest.NamedResource, any) { publications++ }, false)
	defer u()
	current := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainCascade, schedule.Result{})
	assert.FatalIf(t, current.Outcome() == schedule.Blocked,
		"final render blocked: %v", current.Blocked)

	initial.Retire()
	t.Cleanup(current.Close)
	info, _ := s.GetStatus(rs.Named())
	wantStatus := store.StatusReady
	if (*testCaseInput) == "failed gate" {
		wantStatus = store.StatusFailed
		assert.FatalIf(
			t,
			!strings.Contains(info.Message, "dependency not found"),
			"primary error missing: %+v",
			info,
		)
	}
	assert.Equal(t, info.Status, wantStatus)
	if (*testCaseInput) == "dedup" {
		accepted := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
		assert.Diff(t, accepted.Manifests, before.(*store.Emission).Manifests)
	}
	wantPublications := 1
	if (*testCaseInput) == "unchanged" || (*testCaseInput) == "dedup" {
		wantPublications = 0
	}
	o.warnMissingCRDs()
	assert.Equal(t, publications, wantPublications)
	wantWarnings := 1
	if (*testCaseInput) == "remove" || strings.HasPrefix((*testCaseInput), "filter") ||
		(*testCaseInput) == "failed gate" {
		wantWarnings = 0
	}
	assert.Equal(t, len(s.Warnings()), wantWarnings)
	if wantWarnings == 1 {
		assert.Equal(t, s.Warnings()[0].Count, 1)
	}
	var wantRequired []manifest.NamedResource
	if (*testCaseInput) == "unchanged" || (*testCaseInput) == "add" || (*testCaseInput) == "dedup" {
		wantRequired = []manifest.NamedResource{{Group: "apiextensions.k8s.io", Kind: crd.Kind, Name: crd.Name}}
	}
	if (*testCaseInput) == "failed gate" {
		wantRequired = []manifest.NamedResource{
			{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "missing"},
			{Group: "apiextensions.k8s.io", Kind: crd.Kind, Name: crd.Name},
		}
	}
	assert.Diff(
		t,
		slices.SortedFunc(maps.Keys(maps.Collect(s.RequiredInputs(rs.Named()))), manifest.NamedResource.Compare),
		wantRequired,
	)
	assertRetainedCRDManifests(t, s, rs, testCaseInput)
}

func assertRetainedCRDManifests(t *testing.T, s *store.Store, rs *manifest.ResourceSet, testCaseInput *string) {
	t.Helper()
	_, _, accepted := s.GetEmission(rs.Named())
	raw := accepted.Manifests
	wantNames := []string{"old"}
	if strings.HasPrefix((*testCaseInput), "filter") || (*testCaseInput) == "failed gate" {
		wantNames = []string{}
	}
	if (*testCaseInput) == "remove" || (*testCaseInput) == "add" {
		wantNames = []string{"new"}
	}
	names := slices.Grow([]string{}, len(raw)-0)
	for _, doc := range raw {
		metadata := manifest.DocMetadata(doc.Document)
		name, ns := metadata.Name, metadata.Namespace
		names = append(names, name)
		assert.Equal(t, ns, "")
		assert.Equal(t, doc.Document["spec"].(map[string]any)["value"].(string), name)
	}
	slices.Sort(names)
	slices.Sort(wantNames)
	assert.Diff(t, names, wantNames)
}
