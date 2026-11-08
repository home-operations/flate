package resourceset

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/schedule"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/testutil"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/resourceset"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReconcile_SuspensionAfterRefresh(t *testing.T) {
	t.Parallel()
	for _, suspended := range []bool{false, true} {
		t.Run(fmt.Sprintf("suspended=%t", suspended), func(t *testing.T) {
			t.Parallel()
			testReconcileSuspensionAfterRefreshCase(
				t,
				&suspended,
			)
		},
		)
	}
}

func TestReconcile_InputsFromPreservesAuthoredKind(t *testing.T) {
	t.Parallel()
	c, s := newController(t)
	provider := staticRSIP("inputs", "apps", nil, map[string]string{"user": "alice"})
	s.AddObject(provider)
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "fluxcd.controlplane.io/v1", "kind": manifest.KindResourceSet,
		"metadata": map[string]any{"name": "app", "namespace": "apps"},
		"spec": map[string]any{
			"inputsFrom":        []any{map[string]any{"kind": "Unsupported", "name": "inputs"}},
			"resourcesTemplate": "apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: << inputs.user >>, namespace: apps}\n",
		},
	}, manifest.ParseDocOptions{DefaultNamespace: ""})
	obj := parsed.Object
	assert.FatalIf(t, err != nil, "%v", err)
	rs := obj.(*manifest.ResourceSet)
	s.AddObject(rs)
	want := manifest.NamedResource{Kind: "Unsupported", Namespace: "apps", Name: provider.Name}
	{
		blocked := c.ReconcileNode(
			t.Context(),
			rs.Named(),
			manifest.DrainNone,
			schedule.Result{},
		).Blocked
		assert.FatalIf(t, len(blocked) != 1 ||
			blocked[0] != want, "blocked = %v, want [%s]", blocked, want)
	}
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want Failed", info)
	}
	{
		art := s.GetArtifact(rs.Named(), store.ArtifactContent)
		assert.FatalIf(t, art != nil, "unsupported input kind rendered: artifact = %+v", art)
	}
	assert.FatalIf(t, rs.InputsFrom[0].Kind != "Unsupported", "%v", "authored input kind mutated")
}

func TestReconcile_WarningsBeforeExit(t *testing.T) {
	t.Parallel()
	for _, hit := range []bool{false, true} {
		t.Run(fmt.Sprintf("hit=%t", hit), func(t *testing.T) {
			t.Parallel()
			c, s := newController(t)
			provider := &manifest.ResourceSetInputProvider{
				Name:      "dynamic",
				Namespace: "apps",
				Type:      fluxopv1.InputProviderGitHubBranch,
			}
			rs := &manifest.ResourceSet{
				Name:      "reader",
				Namespace: "apps",
				Inputs: []fluxopv1.ResourceSetInput{
					{"name": {Raw: []byte(`"inline"`)}},
				},
				InputsFrom:        []fluxopv1.InputProviderReference{{Name: provider.Name}},
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
			}
			if !hit {
				rs.ResourcesTemplate = "<< invalid template"
			}
			s.AddObjects([]manifest.BaseManifest{rs, provider})
			s.UpdateStatus(provider.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			if hit {
				_, _, err := renderProviders(rs, resourceset.StoreResolver(s), func(content any) (bool, error) {
					s.RecordEmitted(
						rs.Named(),
						&store.Emission{Fingerprint: manifest.Fingerprint(content), Complete: true},
						nil,
					)
					return true, nil
				})
				assert.FatalIf(t, err != nil, "%v", err)
			}
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
			)
			assert.FatalIf(
				t,
				(info.Status == store.StatusReady) != hit,
				"reconcile status = %+v, hit=%t",
				info,
				hit,
			)
			warnings := s.Warnings()
			assert.FatalIf(t, len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider ||
				warnings[0].Resource != rs.Named() ||
				warnings[0].Count != 1, "early return lost warnings: %+v", warnings)
		})
	}
}

func testReconcileSuspensionAfterRefreshCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	st := store.New()
	tasks := task.NewBounded(2)
	c := New(st, tasks, nil)
	var deliveries atomic.Int64
	c.Configure(base.Options{})
	c.StartLifecycle()

	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
	st.AddObject(parent)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	provider := staticRSIP("provider", "flux-system", nil, map[string]string{"user": "rs-refresh-output"})
	st.AddObject(provider)
	st.UpdateStatus(provider.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	doc := map[string]any{
		"apiVersion": fluxopv1.GroupVersion.String(), "kind": manifest.KindResourceSet,
		"metadata": map[string]any{"name": "fleet", "namespace": "flux-system"},
		"spec": map[string]any{
			"inputsFrom": []any{
				map[string]any{"kind": manifest.KindResourceSetInputProvider, "name": provider.Name},
			},
			"resourcesTemplate": "apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: stale-output}\n" + "---\n" + testutil.KustomizationYAML(
				"rs-refresh-child",
				"",
				"  {path: ./child, sourceRef: {kind: GitRepository, name: flux-system}}\n",
			),
		},
	}
	publishDocumentsResult1 := publishDocuments(
		c,
		parent.Named(),
		[]map[string]any{doc},
		manifest.ParseDocOptions{DefaultNamespace: parent.Named().Namespace},
	)
	children, _ := publishDocumentsResult1.Children, publishDocumentsResult1.Documents
	assert.FatalIf(t, len(children) != 1, "initial publication: %v", children)
	rs := children[0].Obj.(*manifest.ResourceSet)
	parsed2, err := manifest.ParseDoc(
		manifest.DeepCopyMap(doc),
		manifest.ParseDocOptions{DefaultNamespace: ""},
	)
	original := parsed2.Object
	assert.FatalIf(t, err != nil, "%v", err)
	st.UpdateStatus(rs.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: ""})
	replacement := manifest.DeepCopyMap(doc)
	replacement["spec"].(map[string]any)["resourcesTemplate"] = "apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: << inputs.user >>}\n" + "---\n" + testutil.KustomizationYAML(
		"rs-refresh-child",
		"",
		"  {path: ./child, sourceRef: {kind: GitRepository, name: flux-system}}\n",
	)
	if *testCaseInput {
		replacement["metadata"].(map[string]any)["annotations"] = map[string]any{
			fluxopv1.ReconcileAnnotation: fluxopv1.DisabledValue,
		}
	}
	var observations atomic.Int64
	var fired, published atomic.Bool
	unsub := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		info, ok := payload.(store.StatusInfo)
		if id != rs.Named() || !ok || info.Message != "resolving dependencies" {
			return
		}
		observations.Add(1)
		if fired.Swap(true) {
			return
		}
		publishDocuments(
			c,
			parent.Named(),
			[]map[string]any{replacement},
			manifest.ParseDocOptions{DefaultNamespace: parent.Named().Namespace},
		)
		published.Store(true)
	}, false)
	t.Cleanup(unsub)
	controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: tasks, Store: st, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, observations.Load() < 1 || !published.Load() || tasks.Failures() != 0,

		"observations=%d published=%t panics=%d",
		observations.Load(),
		published.Load(),
		tasks.Failures())

	fresh, ok := st.Get[*manifest.ResourceSet](rs.Named())
	assert.FatalIf(t, !ok || fresh == rs || fresh.Suspend != (*testCaseInput) ||
		fresh.ResourcesTemplate != replacement["spec"].(map[string]any)["resourcesTemplate"], "canonical replacement = %+v", fresh)
	{
		diff := cmp.Diff(original, rs)
		assert.FatalIf(t, diff != "", "original mutated (-want +got):\n%s", diff)
	}
	{
		owners := testutil.Owners(st.Productions(rs.Named(), nil))
		assert.FatalIf(
			t,
			!slices.Contains(owners, parent.Named()),
			"missing independent contributor: %v",
			owners,
		)
	}
	info, _ := st.GetStatus(rs.Named())
	art, _ := st.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	child := testutil.KustomizationID("flux-system", "rs-refresh-child")
	if *testCaseInput {
		assert.ErrorIf(
			t,
			info.Outcome() != store.OutcomeSuspended || info.Message != "suspended",
			"status = %+v, want Ready/suspended",
			info,
		)
		_, _, inventory := st.GetEmission(rs.Named())
		assert.ErrorIf(
			t,
			art != nil || st.GetObject(child) != nil || deliveries.Load() != 0 || len(inventory.Children) != 0,
			"disabled replacement rendered: artifact=%+v child=%v raw deliveries=%d",
			art,
			st.GetObject(child),
			deliveries.Load(),
		)
	} else if info.Status != store.StatusReady || !hasRenderedDoc(art, "Widget", "rs-refresh-output") || st.GetObject(child) == nil {
		t.Fatalf(
			"active replacement output: status=%+v artifact=%+v child=%v raw deliveries=%d",
			info,
			art,
			st.GetObject(child),
			deliveries.Load(),
		)
	}
}
