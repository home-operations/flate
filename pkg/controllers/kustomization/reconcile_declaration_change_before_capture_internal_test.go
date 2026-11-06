package kustomization

import (
	"context"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestReconcile_DeclarationChangeBeforeCapture(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"withdrawn", "suspended", "replacement"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testReconcileDeclarationChangeBeforeCaptureCase(
				t,
				&state,
			)
		},
		)
	}
}

func TestReconcile_SuppliedAdvisoriesOnReplay(t *testing.T) {
	t.Parallel()
	c, st, root := newControllerWithFixture(t)
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '${VALUE}'}\n",
	)
	freezeSourceForTest(t, st)
	input := &manifest.ConfigMap{Name: "settings", Namespace: "flux-system", Data: map[string]any{"VALUE": "equal"}}
	st.AddDeclaration(
		store.Declaration{
			Obj:       input,
			Doc:       testutil.ObjectDocument(input),
			Origin:    manifest.SuppliedDeclaration,
			InputPath: "supply.yaml",
		},
	)
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: manifest.KindGitRepository, Name: "flux-system"},
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: input.Name}},
		},
	}
	st.AddObject(ks)
	var previous *base.Execution
	var first *store.Emission
	for i, value := range []string{"", "equal", "", "fresh"} {
		repository := &manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace, Data: map[string]any{}}
		if value != "" {
			repository.Data["VALUE"] = value
		}
		st.AddObject(repository)
		var next *base.Execution
		c.Tasks.Go(
			t.Context(),
			"reconcile",
			func(ctx context.Context) {
				next = c.ReconcileNode(ctx, ks.Named(), manifest.DrainNone, schedule.Result{})
			},
		)
		c.Tasks.BlockTillDone()
		if previous != nil {
			previous.Retire()
		}
		previous = next
		t.Cleanup(next.Close)
		assert.FatalIf(
			t,
			next.Outcome() == schedule.Blocked || !next.Valid() ||
				(next.Disposition == store.AssessmentReused) != (i == 1 || i == 2),
			"phase %d: execution=%+v",
			i,
			next,
		)
		current := st.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
		if i == 0 {
			first = current
		} else {
			assert.FatalIf(
				t,
				i < 3 && (current.Fingerprint != first.Fingerprint || !cmp.Equal(current.Manifests, first.Manifests)),
				"%v",
				"equal-output collision transition changed render output",
			)
		}
		var want []store.SuppliedOverride
		if value != "" {
			want = []store.SuppliedOverride{
				{Resource: input.Named(), SupplyPath: "supply.yaml", Keys: []string{"VALUE"}},
			}
		}
		assert.Diff(t, st.AdvisoryEvidence().Overrides, want)
		wantValue := "equal"
		if i == 3 {
			wantValue = "fresh"
		}
		assert.Equal(t, current.Manifests[0].Document["data"].(map[string]any)["value"], any(wantValue))
	}
}

func TestReconcile_RequiredPostBuildNotWaived(t *testing.T) {
	t.Parallel()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		for _, allowed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/allowed=%t", kind, allowed), func(t *testing.T) {
				t.Parallel()
				testReconcileRequiredPostBuildNotWaivedCase(
					t,
					testReconcileRequiredPostBuildNotWaivedCaseInputs{kind: &kind, allowed: &allowed},
				)
			},
			)
		}
	}
}

func testReconcileDeclarationChangeBeforeCaptureCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	c, st, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "b/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"b/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-b, namespace: flux-system}\n",
	)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
	peer := &manifest.Kustomization{Name: "peer", Namespace: "flux-system"}
	parsed2, err := manifest.ParseDoc(
		map[string]any{
			"apiVersion": manifest.FluxKustomizeDomain + "/v1",
			"kind":       manifest.KindKustomization,
			"metadata":   map[string]any{"name": "input", "namespace": "flux-system"},
			"spec": map[string]any{
				"path":      "./apps",
				"sourceRef": map[string]any{"kind": manifest.KindGitRepository, "name": "flux-system"},
			},
		},
		manifest.ParseDocOptions{},
	)
	parsed := parsed2.Object
	assert.FatalIf(t, err != nil, "%v", err)
	input := parsed.(*manifest.Kustomization)
	st.AddObjects([]manifest.BaseManifest{parent, peer, input})
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
			},
		},
	)
	st.UpdateStatus(
		parent.Named(),
		store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "materializing"},
	)
	publish := func(obj *manifest.Kustomization) {
		t.Helper()
		next := captureEmission(st, peer.Named(), &store.Emission{Complete: true})
		defer next.Finish()
		if obj != nil {
			next.Children = []store.Child{{Obj: obj}}
		}
		st.RecordEmitted(peer.Named(), next, nil)
	}
	publish(input)
	entered, changed, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	var expansions atomic.Int64
	stop := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id != input.Named() {
			return
		}
		info := payload.(store.StatusInfo)
		if info.Message == "expanding substitutions" {
			expansions.Add(1)
		}
		if info.Message == "resolving source artifact" && once.CompareAndSwap(false, true) {
			close(entered)
			c.Tasks.YieldSlot(func() { <-changed })
		}
	}, false)
	defer stop()
	var run *base.Execution
	c.Tasks.Go(t.Context(), "reconcile", func(ctx context.Context) {
		defer close(finished)
		run = c.ReconcileNode(ctx, input.Named(), manifest.DrainNone, schedule.Result{})
	})
	c.Tasks.Go(t.Context(), "replace", func(context.Context) {
		reached := false
		c.Tasks.YieldSlot(func() {
			select {
			case <-entered:
				reached = true
			case <-finished:
			}
		})
		if !reached {
			t.Error("reconcile never reached source capture")
			return
		}
		defer close(changed)
		if (*testCaseInput) == "withdrawn" {
			publish(nil)
		} else {
			replacement := input.Clone()
			replacement.Path, replacement.Suspend = "./b", (*testCaseInput) == "suspended"
			publish(replacement)
		}
	})
	c.Tasks.BlockTillDone()
	assert.FatalIf(t, c.Tasks.Failures() != 0, "%v", "capture worker panicked")
	assert.FatalIf(
		t,
		run.Valid() || expansions.Load() != 0 || st.GetArtifact(input.Named(), store.ArtifactContent) != nil,
		"stale source capture continued: valid=%t expansions=%d",
		run.Valid(),
		expansions.Load(),
	)

	testReconcileDeclarationChangeBeforeCaptureCaseTail(
		t,
		&testReconcileDeclarationChangeBeforeCaptureCaseTailInputs{
			run:           &run,
			c:             &c,
			input:         &input,
			testCaseInput: &testCaseInput,
			parent:        &parent,
			expansions:    &expansions,
			st:            &st,
		},
	)
}

func testReconcileDeclarationChangeBeforeCaptureCaseTail(
	t *testing.T,
	tailCaseInput *testReconcileDeclarationChangeBeforeCaptureCaseTailInputs,
) {
	t.Helper()
	(*tailCaseInput.run).
		Close()
	(*tailCaseInput.c).
		Tasks.Go(
		t.Context(),
		"reconcile-current",
		func(ctx context.Context) {
			(*tailCaseInput.run) = (*tailCaseInput.c).ReconcileNode(
				ctx, (*tailCaseInput.input).Named(), manifest.DrainNone,
				schedule.Result{},
			)
		},
	)
	(*tailCaseInput.c).
		Tasks.BlockTillDone()

	switch *(*tailCaseInput.testCaseInput) {
	case "withdrawn":
		if !slices.Equal((*tailCaseInput.run).Blocked, []manifest.NamedResource{(*tailCaseInput.parent).Named()}) ||
			tailCaseInput.expansions.Load() != 0 || (*tailCaseInput.st).
			GetArtifact((*tailCaseInput.input).Named(), store.ArtifactContent) != nil {
			t.Fatalf(
				"lost authority: blocked=%v expansions=%d",
				(*tailCaseInput.run).Blocked,
				tailCaseInput.expansions.Load(),
			)
		}
	case "suspended":
		info, _ := (*tailCaseInput.st).GetStatus((*tailCaseInput.input).Named())
		if (*tailCaseInput.run).Outcome() == schedule.Blocked || tailCaseInput.expansions.Load() != 0 ||
			info.Outcome() != store.OutcomeSuspended || (*tailCaseInput.st).
			GetArtifact((*tailCaseInput.input).Named(), store.ArtifactContent) != nil {
			t.Fatalf(
				"suspended capture continued: blocked=%v expansions=%d status=%v", (*tailCaseInput.run).
					Blocked, tailCaseInput.expansions.
					Load(), info,
			)
		}
	default:
		emission, ok := (*tailCaseInput.st).GetArtifact((*tailCaseInput.input).Named(), store.ArtifactContent).(*store.Emission)
		if (*tailCaseInput.run).Outcome() == schedule.Blocked || !ok || !emission.Complete ||
			len(emission.Manifests) != 1 ||
			emission.Manifests[0].Document["metadata"].(map[string]any)["name"] != "from-b" {
			t.Fatalf("replacement did not prepare B: blocked=%v output=%v", (*tailCaseInput.run).Blocked, emission)
		}
	}
}

type testReconcileRequiredPostBuildNotWaivedCaseInputs struct {
	kind    *string
	allowed *bool
}

func testReconcileRequiredPostBuildNotWaivedCase(
	t *testing.T,
	testCaseInput testReconcileRequiredPostBuildNotWaivedCaseInputs,
) {
	t.Helper()

	original, st, _ := newControllerWithFixture(t)
	freezeSourceForTest(t, st)
	trees := kustomize.NewTreeCache(cachetest.New(t, t.TempDir(), sourcecache.New), nil, original.Tasks)
	t.Cleanup(trees.Close)
	opts := Options{}
	if *testCaseInput.allowed {
		opts.AllowMissingKinds = []string{
			manifest.KindSecret,
			manifest.KindConfigMap,
			manifest.KindCustomResourceDefinition,
		}
	}
	c := New(st, original.Tasks, trees)
	c.Configure(opts, sourcecache.RetryConfig{})
	c.StartLifecycle()
	ks := &manifest.Kustomization{
		Name:      "consumer",
		Namespace: "flux-system",
		Path:      "apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository,
			Name: "flux-system",
		},
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: (*testCaseInput.kind), Name: "missing"}},
		},
	}
	st.AddObject(ks)
	dispatch := controllertest.Dispatch{
		Tasks:         c.Tasks,
		Store:         st,
		ID:            ks.Named(),
		ReconcileNode: c.ReconcileNode,
	}
	info := controllertest.DispatchToFixpoint(t.Context(), t, dispatch)
	assert.FatalIf(
		t,
		info.Status != store.StatusFailed || st.GetArtifact(ks.Named(), store.ArtifactContent) != nil,
		"required absent postBuild input admitted: %+v",
		info,
	)
	input := manifest.NamedResource{Kind: (*testCaseInput.kind), Namespace: ks.Namespace, Name: "missing"}
	if (*testCaseInput.kind) == manifest.KindSecret {
		st.AddObject(&manifest.Secret{Name: input.Name, Namespace: input.Namespace})
	} else {
		st.AddObject(&manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace})
	}
	info = controllertest.DispatchToFixpoint(t.Context(), t, dispatch)
	assert.Equal(t, info.Status, store.StatusReady)
	assert.Equal(t, st.DeleteObject(input), true)
	info = controllertest.DispatchToFixpoint(t.Context(), t, dispatch)
	assert.Equal(t, info.Status, store.StatusFailed)
}

type testReconcileDeclarationChangeBeforeCaptureCaseTailInputs struct {
	run **base.
		Execution

	c **Controller

	input **manifest.
		Kustomization

	testCaseInput **string

	parent **manifest.
		Kustomization

	expansions *atomic.
			Int64

	st **store.
		Store
}
