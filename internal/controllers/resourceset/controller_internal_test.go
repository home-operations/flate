package resourceset

import (
	"context"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/schedule"

	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/testutil"

	"k8s.io/apimachinery/pkg/runtime/schema"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/resourceset"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

// newController wires a bare ResourceSet controller against a fresh
// store for synchronous unit dispatch (no orchestrator / scheduler).
func newController(t *testing.T) (*Controller, *store.Store) {
	t.Helper()
	s := store.New()
	tasks := task.NewBounded(0)
	c := New(s, tasks, nil)
	c.Configure(base.Options{})
	c.StartLifecycle()
	t.Cleanup(func() {
		tasks.BlockTillDone()
	})
	return c, s
}

func staticRSIP(
	name, ns string,
	labels map[string]string,
	defaults map[string]string,
) *manifest.ResourceSetInputProvider {
	vals := fluxopv1.ResourceSetInput{}
	for k, v := range defaults {
		raw := apiextensionsv1.JSON{Raw: []byte(`"` + v + `"`)}
		vals[k] = &raw
	}
	return &manifest.ResourceSetInputProvider{
		Name: name, Namespace: ns,
		Type:          fluxopv1.InputProviderStatic,
		DefaultValues: vals,
		Labels:        labels,
	}
}

// findDoc reports whether the RS artifact carries a doc of the given kind
// and metadata.name.
func hasRenderedDoc(art *store.Emission, kind, name string) bool {
	if art == nil {
		return false
	}
	for _, doc := range art.Manifests {
		if k, _ := doc.Document["kind"].(string); k != kind {
			continue
		}
		md, _ := doc.Document["metadata"].(map[string]any)
		if n, _ := md["name"].(string); n == name {
			return true
		}
	}
	return false
}

// TestReconcile_NamedInputsFrom_ParksOnRSIP pins (a): a ResourceSet with
// a NAMED inputsFrom parks on the RSIP (blocked) until the RSIP is in the
// store, then renders the full input set once it arrives.
func TestReconcile_NamedInputsFrom_ParksOnRSIP(t *testing.T) {
	t.Parallel()
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "acl", Namespace: "flux-system",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Name: "rsip"},
		},
		ResourcesTemplate: `---
apiVersion: example.com/v1
kind: Widget
metadata: {name: << inputs.user >>, namespace: flux-system}
`,
	}
	s.AddObject(rs)

	// First dispatch (no RSIP yet): the named dep is absent, so the node
	// blocks rather than terminalizing.
	blocked := c.ReconcileNode(context.Background(), rs.Named(), 0, schedule.Result{}).Blocked
	assert.FatalIf(
		t,
		len(blocked) != 1,
		"first dispatch blocked = %v, want [rsip] (parked on the named RSIP)",
		blocked,
	)
	want := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "flux-system",
		Name:      "rsip",
	}
	assert.FatalIf(t, blocked[0] != want,
		"blocked on %v, want %v", blocked[0], want)

	// RSIP arrives → re-dispatch renders the Widget from its inputs.
	s.AddObject(staticRSIP("rsip", "flux-system", nil, map[string]string{"user": "alice"}))
	s.UpdateStatus(want, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want Ready", info)
	art, _ := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		!hasRenderedDoc(art, "Widget", "alice"),
		"expected rendered Widget/alice; artifact = %+v",
		art,
	)
	assert.ErrorIf(t, art.Fingerprint == "", "%v", "artifact missing fingerprint")
}

func TestReconcile_SelectorInputsFrom_LateRSIP(t *testing.T) {
	t.Parallel()
	s := store.New()
	tasks := task.NewBounded(2)
	c := New(s, tasks, nil)
	c.Configure(base.Options{})
	c.StartLifecycle()

	rs := selectorFixture("acl", "blue")
	s.AddObject(rs)
	{
		_, err := c.newExecution(
			rs.Named(),
		).run.RequireRefresh(
			t.Context(),
			rs,
			manifest.GateInputs,
			manifest.DrainNone,
		)
		assert.FatalIf(t, err != nil, "%v", "selector-only input must not require an ID gate")
	}
	provider := staticRSIP("rsip", "ns", map[string]string{"color": "blue"}, nil)
	child := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: provider.Name}
	var inventories []int
	unsub := s.AddListener(store.EventArtifactUpdated, func(id manifest.NamedResource, payload any) {
		emission, ok := payload.(*store.Emission)
		if id != rs.Named() || !ok || !emission.Complete {
			return
		}
		inventories = append(inventories, len(emission.Manifests))
		switch len(inventories) {
		case 1:
			assert.ErrorIf(t, len(emission.Manifests) != 0, "%v", "empty selector query rendered an input")
			s.AddObject(provider)
		case 2:
			assert.ErrorIf(
				t,
				!hasRenderedDoc(emission, "ConfigMap", provider.Name) || s.GetObject(child) == nil,
				"%v",
				"late provider was not published as an output",
			)
			removed := *provider
			removed.Labels = map[string]string{"color": "red"}
			s.AddObject(&removed)
		}
	}, false)
	defer unsub()
	dispatches := 0
	info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: tasks,
		Store: s,
		ID:    rs.Named(),
		ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, previous schedule.Result) *base.Execution {
			dispatches++
			return c.ReconcileNode(ctx, id, level, previous)
		},
	})
	assert.FatalIf(
		t,
		info.Status != store.StatusReady || dispatches != 3 || !slices.Equal(inventories, []int{0, 1, 0}),
		"status=%+v dispatches=%d inventories=%v, want Ready, 3, [0 1 0]",
		info,
		dispatches,
		inventories,
	)
	artifact, _ := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, artifact == nil || len(artifact.Manifests) != 0 || s.GetObject(child) != nil,
		"removed selector member survived: artifact=%+v child=%v", artifact, s.GetObject(child))

	assert.FatalIf(
		t,
		rs.InputsFrom[0].Selector.MatchLabels["color"] != "blue",
		"%v",
		"query changed the authored selector",
	)
}

func TestReconcile_SelectorInputs_InvalidSelector(t *testing.T) {
	t.Parallel()
	c, s := newController(t)
	rs := selectorFixture("invalid", "blue")
	rs.InputsFrom = []fluxopv1.InputProviderReference{
		{
			Selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "color", Operator: "invalid"}},
			},
		},
	}
	s.AddObject(rs)
	resolve := resourceset.StoreResolver(s)
	{
		renderWithAdmissionResult1, renderWithAdmissionResult1Err := renderWithAdmission(rs, resolve, "previous")
		_, fp, err := renderWithAdmissionResult1.documents, renderWithAdmissionResult1.fingerprint, renderWithAdmissionResult1Err
		assert.FatalIf(t, err == nil || fp != "", "invalid selector fingerprint=%q error=%v", fp, err)
	}
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusFailed, "status=%+v, want Failed", info)
	}
	{
		artifact := s.GetArtifact(rs.Named(), store.ArtifactContent)
		assert.FatalIf(t, artifact != nil, "invalid selector published artifact=%v", artifact)
	}
}

// TestReconcile_EmitsFluxChildToStore pins (c): a Flux-kind child (a
// Kustomization) emitted by the RS render lands in the store as a
// reconcilable object (AddObject), not merely as rendered data.
func TestReconcile_EmitsFluxChildToStore(t *testing.T) {
	t.Parallel()
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "fleet", Namespace: "flux-system",
		Inputs: []fluxopv1.ResourceSetInput{
			{"path": &apiextensionsv1.JSON{Raw: []byte(`"first"`)}},
			{"path": &apiextensionsv1.JSON{Raw: []byte(`"second"`)}},
		},
		ResourcesTemplate: "---\n" +
			testutil.KustomizationYAML("child", "", `  path: '<< inputs.path >>'
  sourceRef: {kind: GitRepository, name: flux-system}
`) + "---\n" +
			`apiVersion: apps/v1
kind: Deployment
metadata: {name: bare}
` + "---\n" +
			`apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: bare}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: config}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: explicit, namespace: other}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: fleet}
spec: {}
`,
	}
	s.AddObject(rs)

	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want Ready", info)
	childID := testutil.KustomizationID("flux-system", "child")
	child, ok := s.Get[*manifest.Kustomization](childID)
	assert.FatalIf(
		t,
		!ok || child == nil,
		"expected RS-emitted Kustomization %s in store (AddObject), got ok=%v",
		childID,
		ok,
	)
	assert.FatalIf(t, child.Path != "first",
		"emitted path = %q, want first", child.Path)

	projection, _ := manifest.ProjectProducer(rs, nil)
	for _, object := range projection.Objects {
		assert.FatalIf(
			t,
			object.Named() == childID && object.(*manifest.Kustomization).Path != child.Path,
			"%v",
			"projected child differs from controller emission",
		)
	}
	art := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	for _, child := range art.Children {
		assert.FatalIf(t, child.Obj.Named() == rs.Named(), "%v", "effective self child published")
	}
	before := make([]map[string]any, len(art.Manifests))
	for i, doc := range art.Manifests {
		before[i] = manifest.DeepCopyMap(doc.Document)
		metadata := manifest.DocMetadata(doc.Document)
		name, ns := metadata.Name, metadata.Namespace
		want := rs.Namespace
		if name == "explicit" {
			want = "other"
		} else if manifest.DocKind(doc.Document) == "ClusterRole" {
			want = ""
		}
		authored := ""
		if name == "explicit" {
			authored = "other"
		}
		assert.FatalIf(t, ns != authored, "rendered namespace = %q, want authored absence", ns)
		assert.FatalIf(t, s.GetObject(
			manifest.NamedResource{
				Group: schema.FromAPIVersionAndKind(
					manifest.DocAPIVersion(doc.Document),
					manifest.DocKind(doc.Document),
				).Group,
				Kind:      manifest.DocKind(doc.Document),
				Namespace: want,
				Name:      name,
			},
		) == nil, "missing effective child %s/%s", want, name)
	}
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "repeat reconcile: %+v", info)
	}
	{
		delta := cmp.Diff(before, testutil.DocumentMaps(art.Manifests))
		assert.FatalIf(t, delta != "", "replay mutated output (-want +got):\n%s", delta)
	}
}

func TestReconcile_RenderError_MarksFailed(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"template", "provider export", "provider resolution", "typed child"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			testReconcileRenderErrorMarksFailedCase(t, &fault)
		},
		)
	}
}

func TestReconcile_NamedOnlyRetainsNamedGate(t *testing.T) {
	t.Parallel()
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "named", Namespace: "flux-system",
		InputsFrom: []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Name: "rsip"}},
	}
	s.AddObject(rs)
	execution := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone, schedule.Result{})
	defer execution.Close()
	want := manifest.NamedResource{
		Group:     manifest.FluxOperatorDomain,
		Kind:      manifest.KindResourceSetInputProvider,
		Namespace: "flux-system",
		Name:      "rsip",
	}
	assert.FatalIf(t, !slices.Equal(execution.Blocked, []manifest.NamedResource{want}),
		"named-only dependencies = %v, want [%s]", execution.Blocked, want)

	unknown := c.ReconcileNode(
		t.Context(),
		manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "ghost"},
		manifest.DrainNone,
		schedule.Result{},
	)
	defer unknown.Close()
	assert.FatalIf(t, unknown.Outcome() == schedule.Blocked,
		"absent declaration has dependencies: %v", unknown.Blocked)
}

func TestReconcile_AlreadyReady_NoTransientPending(t *testing.T) {
	t.Parallel()
	c, s := newController(t)
	rs := &manifest.ResourceSet{Name: "ready", Namespace: "apps", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}`}
	s.AddObject(rs)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: rs.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "first status = %+v, want Ready", info)
	}
	artifact := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	var authority store.ReadSet
	authority.Init(s, manifest.NamedResource{Name: "artifact-reader"}, nil)
	defer authority.Retire()
	authority.GetArtifact(rs.Named(), store.ArtifactContent)
	unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		assert.ErrorIf(
			t,
			id == rs.Named() && payload.(store.StatusInfo).Status == store.StatusPending,
			"%v",
			"already-Ready ResourceSet transiently became Pending",
		)
	}, false)
	t.Cleanup(unsub)
	c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone, schedule.Result{})
	{
		info, _ := s.GetStatus(rs.Named())
		assert.ErrorIf(t, info.Status != store.StatusReady, "final status = %+v, want Ready", info)
	}
	{
		current := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
		assert.ErrorIf(t, current.Fingerprint != artifact.Fingerprint ||
			!authority.Valid(), "%v", "no-op reconcile replaced artifact")
	}
}

func TestReconcile_DisabledUsesSuspensionGate(t *testing.T) {
	t.Parallel()
	for _, value := range []string{fluxopv1.DisabledValue, "DISABLED"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			c, s := newController(t)
			doc := map[string]any{
				"apiVersion": fluxopv1.GroupVersion.String(),
				"kind":       manifest.KindResourceSet,
				"metadata": map[string]any{
					"name":        "disabled",
					"namespace":   "default",
					"annotations": map[string]any{fluxopv1.ReconcileAnnotation: value},
				},
				"spec": map[string]any{
					"inputsFrom":        []any{map[string]any{"name": "missing"}},
					"resourcesTemplate": "<< inputs.missing >>",
				},
			}
			parsed, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
			obj := parsed.Object
			assert.FatalIf(t, err != nil, "%v", err)
			s.AddObject(obj)
			blocked := c.ReconcileNode(t.Context(), obj.Named(), manifest.DrainNone, schedule.Result{}).Blocked
			assert.FatalIf(t, len(blocked) != 0, "disabled ResourceSet blocked on %v", blocked)
			info, _ := s.GetStatus(obj.Named())
			assert.FatalIf(
				t,
				info.Outcome() != store.OutcomeSuspended || info.Message != "suspended",
				"status = %+v, want Ready/suspended",
				info,
			)
			{
				art := s.GetArtifact(obj.Named(), store.ArtifactContent)
				assert.FatalIf(t, art != nil, "disabled ResourceSet rendered: %v", art)
			}
		})
	}
}

func testReconcileRenderErrorMarksFailedCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "broken", Namespace: "flux-system",
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
	}
	provider := staticRSIP("input", rs.Namespace, nil, map[string]string{"value": "valid"})
	switch *testCaseInput {
	case "provider export":
		rs.InputsFrom = []fluxopv1.InputProviderReference{
			{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{}},
		}
	case "provider resolution":
		rs.InputsFrom = []fluxopv1.InputProviderReference{
			{Kind: manifest.KindResourceSetInputProvider, Name: provider.Name},
		}
		s.AddObject(provider)
	}
	s.AddObject(rs)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         s,
				ID:            rs.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "initial render: %+v", info)
	}
	artifact := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, artifact.Fingerprint == "", "%v", "successful render lacks replay candidate")
	switch *testCaseInput {
	case "template":
		changed := *rs
		changed.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: << .nonexistent.field >>}\n"
		s.AddObject(&changed)
	case "typed child":
		changed := *rs
		changed.ResourcesTemplate += "---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: invalid}\nspec: {interval: []}\n"
		s.AddObject(&changed)
	case "provider export":
		changed := *provider
		changed.DefaultValues = fluxopv1.ResourceSetInput{"value": {Raw: []byte("invalid json")}}
		s.AddObject(&changed)
	case "provider resolution":
		changed := *rs
		changed.InputsFrom = []fluxopv1.InputProviderReference{
			{Kind: manifest.KindResourceSetInputProvider, Name: "missing"},
		}
		s.AddObject(&changed)
	}
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         s,
				ID:            rs.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want Failed", info)
	}
	if (*testCaseInput) == "typed child" {
		failed := s.GetArtifact(rs.Named(), store.ArtifactContent).(*store.Emission)
		_, replayed := failed.Replay(artifact.Fingerprint)
		assert.FatalIf(
			t,
			failed.Complete || failed.Footprint() != nil || replayed || len(failed.Failures) != 1 ||
				len(failed.Manifests) != 2 ||
				!hasRenderedDoc(
					failed,
					"ConfigMap",
					"output",
				),
			"invalid child retained successful publication: %+v",
			failed,
		)
		assert.FatalIf(t, s.GetObject(
			manifest.NamedResource{
				Group:     manifest.HelmReleaseDomain,
				Kind:      manifest.KindHelmRelease,
				Namespace: rs.Namespace,
				Name:      "invalid",
			},
		) != nil, "%v", "invalid generated child was admitted")
	}
}
