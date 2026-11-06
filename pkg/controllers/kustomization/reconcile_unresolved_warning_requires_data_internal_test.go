package kustomization

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_UnresolvedWarningRequiresData(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system"},
		PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
			{Kind: manifest.KindSecret, Name: "missing-secret"},
			{Kind: manifest.KindConfigMap, Name: "missing-configmap"},
		}},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.Equal(t, info.Status, store.StatusFailed)
	{
		warnings := s.Warnings()
		assert.FatalIf(t, len(warnings) != 0, "failed data gate published warnings: %+v", warnings)
	}
	{
		artifact := s.GetArtifact(ks.Named(), store.ArtifactContent)
		assert.FatalIf(t, artifact != nil, "failed data gate published output: %+v", artifact)
	}
}

func TestReconcile_UnresolvedWarningReplay(t *testing.T) {
	t.Parallel()
	for _, replay := range []bool{false, true} {
		name := "metadata before dispatch"
		if replay {
			name = "metadata after completion"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testReconcileUnresolvedWarningReplayCase(t, &replay)
		},
		)
	}
}

// TestReconcile_FingerprintDedup_SkipsRender locks the dedup
// short-circuit from PR #220: a second AddObject with byte-equal
// post-Prepare spec must not re-run kustomize.Render. We force
// the issue by AddObject'ing the same KS twice and verifying the
// artifact's identity persists (SetArtifact dedupes via DeepEqual).
func TestReconcile_FingerprintDedup_SkipsRender(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/cm.yaml", namespaceIdentityDocs)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps", TargetNamespace: "release",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission).Fingerprint
	art := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	before := make([]map[string]any, len(art.Manifests))
	for i, doc := range art.Manifests {
		before[i] = manifest.DeepCopyMap(doc.Document)
	}

	// Re-AddObject a copy with kustomize ownership labels stamped, then drive a
	// second reconcile: the labels differ so the body re-runs, but the
	// fingerprint matches → second render skipped, artifact fingerprint
	// unchanged.
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	c.Tasks.Go(
		t.Context(),
		"reconcile",
		func(ctx context.Context) { c.ReconcileNode(ctx, ks.Named(), 0, schedule.Result{}) },
	)
	c.Tasks.BlockTillDone()
	secondFP := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission).Fingerprint
	assert.ErrorIf(
		t,
		firstFP != secondFP,
		"fingerprint changed across label-only re-AddObject: %q vs %q",
		firstFP,
		secondFP,
	)
	assert.Diff(t, testutil.DocumentMaps(art.Manifests), before)
	for _, doc := range art.Manifests {
		metadata := manifest.DocMetadata(doc.Document)
		name, ns := metadata.Name, metadata.Namespace
		want := ks.TargetNamespace
		if manifest.DocAPIVersion(doc.Document) == "rbac.authorization.k8s.io/v1" {
			want = ""
		}
		assert.Equal(t, ns, want)
		assert.ErrorIf(t, s.GetObject(
			manifest.NamedResource{
				Group: schema.FromAPIVersionAndKind(
					manifest.DocAPIVersion(doc.Document),
					manifest.DocKind(doc.Document),
				).Group,
				Kind:      manifest.DocKind(doc.Document),
				Namespace: want,
				Name:      name,
			},
		) == nil, "replayed child %s/%s missing", want, name)
		assert.ErrorIf(t, want != "" &&
			s.GetObject(
				manifest.NamedResource{
					Group: schema.FromAPIVersionAndKind(
						manifest.DocAPIVersion(doc.Document),
						manifest.DocKind(doc.Document),
					).Group,
					Kind: manifest.DocKind(doc.Document),
					Name: name,
				},
			) != nil, "replay published duplicate bare child %s", name)
	}
}

func TestReconcile_AlreadyReady_NoTransientPending(t *testing.T) {
	t.Parallel()
	for _, immutable := range []bool{false, true} {
		t.Run(strconv.FormatBool(immutable), func(t *testing.T) {
			t.Parallel()
			testReconcileAlreadyReadyNoTransientPendingCase(
				t,
				&immutable,
			)
		},
		)
	}
}

// TestReconcile_GenuineReRender_DoesDowngrade is the byte-determinism
// guard for Part A: a re-reconcile whose effective spec CHANGED
// (fingerprint differs, e.g. #102 structural-parent-injected
// targetNamespace) MUST still downgrade to Pending before re-rendering,
// so a dependent re-gates on the new output rather than reading a
// stale-Ready parent. Proves the wasReady guard suppresses only the
// pre-fingerprint progress writes, not the genuine "rendering"
// downgrade.
func TestReconcile_GenuineReRender_DoesDowngrade(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	freezeSourceForTest(t, s)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}
	firstFP := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission).Fingerprint

	var mu sync.Mutex
	var sawPending bool
	s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != ks.Named() {
			return
		}
		if info, ok := payload.(store.StatusInfo); ok && info.Status == store.StatusPending {
			mu.Lock()
			sawPending = true
			mu.Unlock()
		}
	}, false)

	changed := ks.Clone()
	changed.TargetNamespace = "elsewhere"
	s.AddObject(changed)
	c.Tasks.Go(
		t.Context(),
		"reconcile",
		func(ctx context.Context) { c.ReconcileNode(ctx, ks.Named(), 0, schedule.Result{}) },
	)
	c.Tasks.BlockTillDone()

	var secondFP string
	if art, ok := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission); ok && art.Fingerprint != firstFP {
		secondFP = art.Fingerprint
	}

	mu.Lock()
	defer mu.Unlock()
	assert.ErrorIf(
		t,
		!sawPending,
		"%v",
		"genuine re-render did not downgrade to Pending; dependents would read a stale-Ready parent",
	)
	assert.ErrorIf(
		t,
		secondFP == "" || secondFP == firstFP,
		"fingerprint did not change after spec mutation (%q → %q); not a genuine re-render",
		firstFP,
		secondFP,
	)
}

// TestReconcile_MissingPathFails covers the error path: spec.path
// points at a directory that doesn't exist → kustomize.Render
// errors → reconcile returns it → status flips Failed.
func TestReconcile_MissingPathFails(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	ks := &manifest.Kustomization{
		Name: "broken", Namespace: "flux-system",
		Path: "./nonexistent",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	assert.ErrorIf(t, info.Message == "", "%v", "expected non-empty failure message")
}

// TestReconcile_DependsOnFailed cascades a dep failure: when a KS's
// dependsOn target is in StatusFailed, reconcile must return
// DependencyFailedError without rendering.
func TestReconcile_DependsOnFailed(t *testing.T) {
	t.Parallel()
	c, s, _ := newControllerWithFixture(t)
	dep := &manifest.Kustomization{Name: "dep", Namespace: "flux-system"}
	s.AddObject(dep)
	s.UpdateStatus(
		dep.Named(),
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "synthetic dep failure"},
	)

	ks := &manifest.Kustomization{
		Name: "depender", Namespace: "flux-system",
		Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
		DependsOn: []kustomizev1.DependencyReference{{Name: dep.Name, Namespace: dep.Namespace}},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.FatalIf(t, info.Status != store.StatusFailed, "status = %+v, want StatusFailed", info)
	assert.ErrorIf(t, info.Message == "", "%v", "expected failure message from dep cascade")
}

func testReconcileUnresolvedWarningReplayCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	c, s, _ := newControllerWithFixture(t)
	freezeSourceForTest(t, s)
	declaration := &manifest.RawObject{
		APIVersion: "external-secrets.io/v1",
		Kind:       "ExternalSecret",
		Name:       "cluster-secrets",
		Namespace:  "flux-system",
		Spec:       map[string]any{"target": map[string]any{"name": "cluster-secrets"}},
	}
	s.AddObject(declaration)
	input := manifest.NamedResource{
		Kind:      manifest.KindSecret,
		Namespace: "flux-system",
		Name:      "cluster-secrets",
	}
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input: {Productions: []manifest.Production{{Declaration: declaration.Named()}}},
			},
		},
	)
	consumers := make([]*manifest.Kustomization, 2)
	for i := range consumers {
		parsed, err := manifest.ParseDoc(map[string]any{
			"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
			"kind":       "Kustomization",
			"metadata":   map[string]any{"name": fmt.Sprintf("apps-%d", i), "namespace": "flux-system"},
			"spec": map[string]any{
				"path":      "./apps",
				"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
				"postBuild": map[string]any{
					"substituteFrom": []any{map[string]any{"kind": "Secret", "name": "cluster-secrets"}},
				},
			},
		}, manifest.ParseDocOptions{})
		obj := parsed.Object
		assert.NoError(t, err)
		consumers[i] = obj.(*manifest.Kustomization)
		s.AddObject(consumers[i])
	}
	first := consumers[0].Named()
	start := make(chan struct{})
	completed := make(chan struct{})
	var accepted *store.Emission
	var authority store.ReadSet
	var replayEvents atomic.Int64
	var stopReplay func()
	var renders atomic.Int64
	stopRenders := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == first && payload.(store.StatusInfo).Message == "rendering" {
			renders.Add(1)
		}
	}, false)
	defer stopRenders()
	for i, ks := range consumers {
		c.Tasks.Go(t.Context(), ks.Named().String(), func(ctx context.Context) {
			<-start
			{
				deps := c.ReconcileNode(ctx, ks.Named(), manifest.DrainNone, schedule.Result{}).Blocked
				assert.ErrorIf(t, len(deps) != 0, "blocked dependencies = %v", deps)
			}
			if i == 0 {
				accepted, _ = s.GetArtifact(first, store.ArtifactContent).(*store.Emission)
				close(completed)
			}
		})
	}
	stamped := consumers[0].Clone()
	stamped.Labels = map[string]string{"kustomize.toolkit.fluxcd.io/name": "parent"}
	if *testCaseInput {
		close(start)
		<-completed
	}
	s.AddObject(stamped)
	assert.Equal(t, s.GetObject(first), manifest.BaseManifest(stamped))
	if *testCaseInput {
		authority.Init(s, manifest.NamedResource{Name: "output-reader"}, nil)
		authority.GetArtifact(first, store.ArtifactContent)
		assert.FatalIf(
			t,
			!authority.Valid(),
			"%v",
			"captured output was already invalid before metadata replay",
		)
		stops := make([]store.Unsubscribe, 0, 3)
		for _, kind := range []store.EventKind{store.EventObjectAdded, store.EventStatusUpdated, store.EventArtifactUpdated} {
			stops = append(stops, s.AddListener(kind, func(id manifest.NamedResource, _ any) {
				if id == first {
					t.Logf("metadata replay event=%v", kind)
					replayEvents.Add(1)
				}
			}, false))
		}
		stopReplay = func() {
			for _, stop := range stops {
				stop()
			}
		}
	}

	testReconcileUnresolvedWarningReplayCaseTail(
		t,
		&testReconcileUnresolvedWarningReplayCaseTailInputs{
			testCaseInput: &testCaseInput,
			start:         &start,
			c:             &c,
			first:         &first,
			accepted:      &accepted,
			s:             &s,
			stopReplay:    &stopReplay,
			authority:     &authority,
			replayEvents:  &replayEvents,
			renders:       &renders,
			consumers:     &consumers,
		},
	)
}

func testReconcileUnresolvedWarningReplayCaseTail(
	t *testing.T,
	tailCaseInput *testReconcileUnresolvedWarningReplayCaseTailInputs,
) {
	t.Helper()
	if !(*(*tailCaseInput.testCaseInput)) {
		close((*tailCaseInput.start))
	} else {
		(*tailCaseInput.c).
			Tasks.Go(t.Context(), "metadata replay", func(ctx context.Context) {
			{
				deps := (*tailCaseInput.c).ReconcileNode(
					ctx,
					(*tailCaseInput.first),
					manifest.DrainNone,
					schedule.Result{},
				).Blocked
				assert.ErrorIf(t, len(deps) != 0, "replay blocked dependencies = %v", deps)
			}
		})
	}
	(*tailCaseInput.c).
		Tasks.BlockTillDone()
	assert.Equal(t, (*tailCaseInput.c).Tasks.Failures(), int64(0))
	assert.FatalIf(
		t, (*tailCaseInput.accepted) ==
			nil || (*tailCaseInput.accepted).Fingerprint == "" || len((*tailCaseInput.accepted).Manifests) != 1, "first emission = %+v", (*tailCaseInput.accepted),
	)
	current := (*tailCaseInput.s).GetArtifact((*tailCaseInput.first), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(
		t,
		current.Fingerprint != (*tailCaseInput.accepted).Fingerprint,
		"%v",
		"metadata replay changed accepted fingerprint",
	)
	assert.Diff(t, current.Manifests, (*tailCaseInput.accepted).Manifests)
	if *(*tailCaseInput.testCaseInput) {
		(*tailCaseInput.stopReplay)()
		if !tailCaseInput.authority.Valid() || tailCaseInput.replayEvents.Load() != 0 {
			t.Fatalf(
				"metadata replay changed output authority: valid=%t events=%d", tailCaseInput.authority.
					Valid(), tailCaseInput.replayEvents.
					Load(),
			)
		}
		tailCaseInput.authority.
			Retire()
	}
	if tailCaseInput.renders.Load() != 1 {
		t.Fatalf("metadata replay rendered %d times, want 1", tailCaseInput.renders.Load())
	}

	for _, ks := range *tailCaseInput.consumers {
		info, _ := (*tailCaseInput.s).GetStatus(ks.Named())
		assert.Equal(t, info.Status, store.StatusReady)
		other := (*tailCaseInput.s).GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(
			t,
			other.Fingerprint == "" || len(other.Manifests) != 1,
			"consumer emission = %+v",
			other,
		)
		assert.Diff(t, other.Manifests[0].Document["data"], map[string]any{"greeting": "hi"})
	}
	warnings := (*tailCaseInput.s).Warnings()
	assert.FatalIf(
		t,
		len(warnings) != 1 || warnings[0].Category != manifest.WarnUnresolvedSubstitution,
		"warnings = %+v, want one unresolved Secret group",
		warnings,
	)
	assert.Equal(t, warnings[0].Resource, manifest.NamedResource{})
	assert.Equal(t, warnings[0].Count, 2)
	t.Logf(
		"replay=%t; both consumers Ready; accepted output reused; warning=%+v",
		(*(*tailCaseInput.testCaseInput)),
		warnings[0],
	)
}

func testReconcileAlreadyReadyNoTransientPendingCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	c, s, root := newControllerWithFixture(t)
	if *testCaseInput {
		freezeSourceForTest(t, s)
	}
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	s.AddObject(ks)
	{
		info := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         c.Tasks,
				Store:         s,
				ID:            ks.Named(),
				ReconcileNode: c.ReconcileNode,
			},
		)
		assert.FatalIf(t, info.Status != store.StatusReady, "status = %+v, want StatusReady", info)
	}
	first := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	var sawPending atomic.Bool
	s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); id == ks.Named() && ok &&
			info.Status == store.StatusPending {
			sawPending.Store(true)
		}
	}, false)
	stamped := ks.Clone()
	stamped.Labels = map[string]string{
		"kustomize.toolkit.fluxcd.io/name":      "parent",
		"kustomize.toolkit.fluxcd.io/namespace": "flux-system",
	}
	s.AddObject(stamped)
	c.Tasks.Go(
		t.Context(),
		"reconcile",
		func(ctx context.Context) { c.ReconcileNode(ctx, ks.Named(), 0, schedule.Result{}) },
	)
	c.Tasks.BlockTillDone()
	assert.ErrorIf(
		t,
		sawPending.Load(),
		"%v",
		"equal validated content transiently downgraded Ready to Pending",
	)
	{
		info, _ := s.GetStatus(ks.Named())
		assert.ErrorIf(t, info.Status != store.StatusReady, "KS not Ready after no-op re-run: %+v", info)
	}
	if !(*testCaseInput) {
		testutil.WriteFile(
			t,
			root,
			"apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: default}\ndata: {greeting: changed}\n",
		)
		c.Tasks.Go(
			t.Context(),
			"reconcile",
			func(ctx context.Context) { c.ReconcileNode(ctx, ks.Named(), 0, schedule.Result{}) },
		)
		c.Tasks.BlockTillDone()
		next := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
		if !sawPending.Load() || next.Fingerprint == first.Fingerprint ||
			next.Manifests[0].Document["data"].(map[string]any)["greeting"] != "changed" {
			t.Fatalf(
				"mutable source reused stale content: pending=%v output=%+v",
				sawPending.Load(),
				next.Manifests,
			)
		}
	}
}

type testReconcileUnresolvedWarningReplayCaseTailInputs struct {
	testCaseInput **bool

	start *chan struct{}

	c **Controller

	first *manifest.
		NamedResource

	accepted **store.
			Emission

	s **store.
		Store

	stopReplay *func()

	authority *store.
			ReadSet

	replayEvents *atomic.
			Int64

	renders *atomic.
		Int64

	consumers *[]*manifest.
			Kustomization
}
