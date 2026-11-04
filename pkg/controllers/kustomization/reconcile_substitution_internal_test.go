package kustomization

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func effectiveSubstitutionCases() []struct {
	name, expression string
	first, second    map[string]string
	inline           map[string]string
	strategy         kustomizev1.SubstituteStrategy
	disabled, same   bool
} {
	return []struct {
		name, expression string
		first, second    map[string]string
		inline           map[string]string
		strategy         kustomizev1.SubstituteStrategy
		disabled, same   bool
	}{
		{
			name:       "unused",
			expression: "${USED}",
			first:      map[string]string{"USED": "kept", "OTHER": "first"},
			second:     map[string]string{"USED": "kept", "OTHER": "second"},
			same:       true,
		},
		{
			name:       "used",
			expression: "${USED}",
			first:      map[string]string{"USED": "first"},
			second:     map[string]string{"USED": "second"},
		},
		{
			name:       "absent default",
			expression: "${MISSING:=fallback}",
			first:      map[string]string{"OTHER": "first"},
			second:     map[string]string{"OTHER": "second"},
			same:       true,
		},
		{
			name:       "default becomes present",
			expression: "${MISSING:=fallback}",
			first:      map[string]string{"OTHER": "first"},
			second:     map[string]string{"OTHER": "first", "MISSING": "provided"},
		},
		{
			name:       "empty becomes absent",
			expression: "${USED:=fallback}",
			first:      map[string]string{"OTHER": "kept", "USED": ""},
			second:     map[string]string{"OTHER": "kept"},
		},
		{
			name:       "eager operand with present value",
			expression: "${USED:=${OTHER}}",
			first:      map[string]string{"USED": "kept", "OTHER": "first"},
			second:     map[string]string{"USED": "kept", "OTHER": "second"},
		},
		{
			name:       "nested operand",
			expression: "${USED:=${OTHER}}",
			first:      map[string]string{"OTHER": "first"},
			second:     map[string]string{"OTHER": "second"},
		},
		{
			name:       "activation",
			expression: "${MISSING:=fallback}",
			first:      map[string]string{},
			second:     map[string]string{"OTHER": "enabled"},
		},
		{
			name:       "inline override",
			expression: "${USED}",
			inline:     map[string]string{"USED": "inline"},
			first:      map[string]string{"USED": "first"},
			second:     map[string]string{"USED": "second"},
			same:       true,
		},
		{
			name:       "disabled",
			expression: "${ARR[@]}",
			first:      map[string]string{"OTHER": "first"},
			second:     map[string]string{"OTHER": "second"},
			disabled:   true,
			same:       true,
		},
		{
			name:       "active without names",
			expression: "literal",
			first:      map[string]string{"OTHER": "first"},
			second:     map[string]string{"OTHER": "second"},
			same:       true,
		},
		{
			name:       "deactivation",
			expression: "${MISSING:=fallback}",
			first:      map[string]string{"OTHER": "enabled"},
			second:     map[string]string{},
		},
		{
			name:       "Always empty bag",
			expression: "${MISSING:=fallback}",
			strategy:   kustomizev1.SubstituteStrategyAlways,
			first:      map[string]string{},
			second:     map[string]string{"OTHER": "enabled"},
			same:       true,
		},
		{
			name:       "transformed",
			expression: "${USED^^}",
			first:      map[string]string{"USED": "first"},
			second:     map[string]string{"USED": "second"},
		},
		{
			name:       "replacement is not recursive",
			expression: "${USED}",
			first:      map[string]string{"USED": "${OTHER}", "OTHER": "first"},
			second:     map[string]string{"USED": "${OTHER}", "OTHER": "second"},
			same:       true,
		},
		{
			name:       "escaped",
			expression: "$${USED}",
			first:      map[string]string{"USED": "first"},
			second:     map[string]string{"USED": "second"},
			same:       true,
		},
	}
}

func TestReconcile_EffectiveSubstitutions(t *testing.T) {
	t.Parallel()
	for _, tc := range effectiveSubstitutionCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReconcileEffectiveSubstitutionsCase(t, &tc)
		},
		)
	}
}

func TestReconcile_EffectiveSubstitutionDiagnostics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		strict bool
		bad    manifest.BaseManifest
	}{
		{
			name: "unused invalid name",
			bad: &manifest.ConfigMap{
				Name:      "vars",
				Namespace: "flux-system",
				Data:      map[string]any{"USED": "kept", "INVALID-NAME": "unused"},
			},
		},
		{
			name: "unused malformed value",
			bad: &manifest.ConfigMap{
				Name:      "vars",
				Namespace: "flux-system",
				Data:      map[string]any{"USED": "kept", "OTHER": []any{"invalid"}},
			},
		},
		{
			name: "unused malformed Secret",
			bad: &manifest.Secret{
				Name:       "vars",
				Namespace:  "flux-system",
				StringData: map[string]any{"USED": "kept"},
				Data:       map[string]any{"OTHER": "%%%"},
			},
		},
		{name: "missing required reference"},
		{
			name:   "eager strict missing operand",
			strict: true,
			bad:    &manifest.ConfigMap{Name: "vars", Namespace: "flux-system", Data: map[string]any{"USED": "kept"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReconcileEffectiveSubstitutionDiagnosticsCase(
				t,
				&tc,
			)
		},
		)
	}
}

func TestReconcile_StampedSubstitutionExclusion(t *testing.T) {
	t.Parallel()
	c, s, root := newControllerWithFixture(t)
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {greeting: '${ARR[@]}'}\n",
	)
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "./apps",
		CommonMetadata: &kustomizev1.CommonMetadata{
			Annotations: map[string]string{"kustomize.toolkit.fluxcd.io/substitute": "disabled"},
		},
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"OTHER": "active"}},
	}
	s.AddObject(ks)
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.Equal(t, info.Status, store.StatusReady)
	art := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	assert.Equal(t, art.Manifests[0].Document["data"].(map[string]any)["greeting"], any("${ARR[@]}"))
	next := ks.Clone()
	next.CommonMetadata = nil
	s.AddObject(next)
	info = controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.Equal(t, info.Status, store.StatusFailed)
	assert.Equal(t, ks.CommonMetadata.Annotations["kustomize.toolkit.fluxcd.io/substitute"], "disabled")
}

func TestRender_SuspendedSkipsRender(t *testing.T) {
	t.Parallel()
	c, st, _ := newControllerWithFixture(t)
	freezeSourceForTest(t, st)
	initial := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	guard := &guardedSourceFS{FS: initial.FS}
	counted := *initial
	counted.FS = guard
	st.SetArtifact(manifest.BootstrapSourceID, &counted)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps", Suspend: true,
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind: manifest.KindGitRepository, Name: "flux-system", Namespace: "flux-system",
		},
	}
	st.AddObject(ks)
	execution := c.newExecution(ks.Named())
	defer execution.run.Close()
	sourceID, applyIgnore, err := kustomize.SourceInput(&execution.run.Reads, ks)
	assert.NoError(t, err)
	want := execution.run.Checkpoint()
	assert.FatalIf(t, want == nil, "%v", "suspended declaration passed admission")
	before := guard.opened.Load()
	got := execution.render(t.Context(), ks, sourceID, applyIgnore)
	assert.ErrorIf(t, !errors.Is(got, want), "render error = %v, want admission error %v", got, want)
	{
		opens := guard.opened.Load() - before
		assert.ErrorIf(t, opens != 0, "rejected render opened source %d times, want 0", opens)
	}
	{
		artifact := st.GetArtifact(ks.Named(), store.ArtifactContent)
		assert.ErrorIf(t, artifact != nil, "rejected attempt published %T", artifact)
	}
}

func TestReconcile_ImmutableRootChangedRemoteLiteral(t *testing.T) {
	t.Parallel()
	var changed atomic.Bool
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		literal := "before"
		if changed.Load() {
			literal = "after"
		}
		fmt.Fprintf(
			w,
			"apiVersion: v1\n"+
				"kind: ConfigMap\n"+
				"metadata: {name: output, namespace: apps}\n"+
				"data: {value: '${VALUE}', literal: %s}\n",
			literal,
		)
	}))
	t.Cleanup(server.Close)
	fixture, st, root := newControllerWithFixture(t)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: ["+server.URL+"/resource.yaml]\n")
	freezeSourceForTest(t, st)
	cacheDir := t.TempDir()
	open := func() (*Controller, func()) {
		owner, err := sourcecache.New(t.Context(), cacheDir)
		assert.NoError(t, err)
		network := new(source.Network)
		inputs := source.NewHTTPInputs(owner, network, fixture.Tasks)
		trees := kustomize.NewTreeCache(owner, &inputs, fixture.Tasks)
		controller := New(st, fixture.Tasks, trees, true)
		controller.Configure(Options{}, sourcecache.RetryConfig{})
		controller.StartLifecycle()
		closeResources := sync.OnceFunc(func() {
			trees.Close()
			inputs.Close()
			network.Close()
			assert.NoError(t, owner.Close())
		})
		t.Cleanup(closeResources)
		return controller, closeResources
	}
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "./apps",
		PostBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "kept"}},
	}
	st.AddObject(ks)
	first, closeResources := open()
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: first.Tasks, Store: st, ID: ks.Named(), ReconcileNode: first.ReconcileNode},
	)
	assert.Equal(t, info.Status, store.StatusReady)
	before := st.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	closeResources()
	changed.Store(true)
	second, _ := open()
	reads := &store.ReadSet{}
	reads.Init(st, ks.Named(), nil)
	count := requests.Load()
	_, complete := second.PreviousRender(t.Context(), ks, reads)
	reads.Close()
	assert.Equal(t, complete, false)
	assert.Equal(t, requests.Load(), count)
	reused := false
	info = controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: second.Tasks, Store: st, ID: ks.Named(),
		ReconcileNode: func(
			ctx context.Context,
			id manifest.NamedResource,
			level manifest.DrainLevel,
		) *base.Execution {
			ex := second.ReconcileNode(ctx, id, level)
			reused = ex.Reused
			return ex
		},
	})
	assert.Equal(t, info.Status, store.StatusReady)
	after := st.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
	assert.Equal(t, reused, false)
	assert.Equal(t, before.Fingerprint != after.Fingerprint, true)
	assert.Equal(t, after.Manifests[0].Document["data"].(map[string]any)["value"], any("kept"))
	assert.Equal(t, after.Manifests[0].Document["data"].(map[string]any)["literal"], any("after"))
}

func substitutionData(vars map[string]string) map[string]any {
	data := make(map[string]any, len(vars))
	for name, value := range vars {
		data[name] = value
	}
	return data
}

func testReconcileEffectiveSubstitutionsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		expression string
		first      map[string]string
		second     map[string]string
		inline     map[string]string
		strategy   kustomizev1.
				SubstituteStrategy
		disabled bool
		same     bool
	},
) {
	t.Helper()

	fixture, s, root := newControllerWithFixture(t)
	c := New(s, fixture.Tasks, fixture.Trees, true)
	c.Configure(Options{}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	metadata := "{name: hello, namespace: default}"
	if testCaseInput.disabled {
		metadata = "{name: hello, namespace: default, annotations: {kustomize.toolkit.fluxcd.io/substitute: disabled}}"
	}
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: %s\ndata: {greeting: %q}\n",
			metadata, testCaseInput.expression,
		),
	)
	ks := &manifest.Kustomization{
		Name: "apps", Namespace: "flux-system", Path: "./apps",
		SourceRef: kustomizev1.CrossNamespaceSourceReference{
			Kind:      manifest.KindGitRepository,
			Name:      "flux-system",
			Namespace: "flux-system",
		},
		PostBuild: &kustomizev1.PostBuild{
			SubstituteStrategy: testCaseInput.strategy,
			Substitute:         testCaseInput.inline,
			SubstituteFrom:     []manifest.SubstituteReference{{Kind: manifest.KindConfigMap, Name: "vars"}},
		},
	}
	s.AddObject(ks)
	var first *store.Emission
	for i, vars := range []map[string]string{testCaseInput.first, testCaseInput.second} {
		data := substitutionData(vars)
		provider := &manifest.ConfigMap{Name: "vars", Namespace: ks.Namespace, Data: data}
		s.AddObject(provider)
		s.UpdateStatus(provider.Named(), store.StatusInfo{Status: store.StatusReady})
		replayed := false
		info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
			Tasks: c.Tasks, Store: s, ID: ks.Named(),
			ReconcileNode: func(
				ctx context.Context,
				id manifest.NamedResource,
				level manifest.DrainLevel,
			) *base.Execution {
				ex := c.ReconcileNode(ctx, id, level)
				replayed = ex.Reused
				return ex
			},
		})
		assert.FatalIf(t, info.Status != store.StatusReady, "run %d: %+v", i, info)
		current := s.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission)
		if i == 0 {
			first = current
			continue
		}
		{
			equal := first.Fingerprint == current.Fingerprint
			assert.ErrorIf(
				t,
				equal != testCaseInput.same,
				"equal effective fingerprint = %t, want %t",
				equal,
				testCaseInput.same,
			)
		}
		assert.Equal(t, replayed, testCaseInput.same)
		if testCaseInput.same {
			assert.Diff(t, current.Manifests, first.Manifests)
		}
		freshFixture, freshStore, freshRoot := newControllerWithFixture(t)
		fresh := New(freshStore, freshFixture.Tasks, freshFixture.Trees, true)
		fresh.Configure(Options{}, sourcecache.RetryConfig{})
		fresh.StartLifecycle()
		raw, err := os.ReadFile(filepath.Join(root, "apps", "cm.yaml"))
		assert.NoError(t, err)
		testutil.WriteFile(t, freshRoot, "apps/cm.yaml", string(raw))
		freshStore.AddObject(ks)
		freshStore.AddObject(provider)
		freshStore.UpdateStatus(provider.Named(), store.StatusInfo{Status: store.StatusReady})
		full := controllertest.DispatchToFixpoint(
			t.Context(),
			t,
			controllertest.Dispatch{
				Tasks:         fresh.Tasks,
				Store:         freshStore,
				ID:            ks.Named(),
				ReconcileNode: fresh.ReconcileNode,
			},
		)
		assert.Equal(t, full.Status, store.StatusReady)
		assert.Diff(
			t,
			current.Manifests,
			freshStore.GetArtifact(ks.Named(), store.ArtifactContent).(*store.Emission).Manifests,
		)
	}
}

func testReconcileEffectiveSubstitutionDiagnosticsCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		strict bool
		bad    manifest.
			BaseManifest
	},
) {
	t.Helper()

	fixture, s, root := newControllerWithFixture(t)
	c := New(s, fixture.Tasks, fixture.Trees, true)
	c.Configure(Options{StrictSubstitutions: testCaseInput.strict}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	expression := "${USED}"
	if testCaseInput.strict {
		expression = "${USED:=${MISSING}}"
	}
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello}\ndata: {greeting: %q}\n",
			expression,
		),
	)
	kind := manifest.KindConfigMap
	if testCaseInput.bad != nil {
		kind = testCaseInput.bad.Named().Kind
	}
	ks := &manifest.Kustomization{
		Name:      "apps",
		Namespace: "flux-system",
		Path:      "./apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: kind, Name: "vars"}},
		},
	}
	s.AddObject(ks)
	var good manifest.BaseManifest = &manifest.ConfigMap{
		Name:      "vars",
		Namespace: ks.Namespace,
		Data:      map[string]any{"USED": "kept", "MISSING": "present"},
	}
	if kind == manifest.KindSecret {
		good = &manifest.Secret{
			Name:       "vars",
			Namespace:  ks.Namespace,
			StringData: map[string]any{"USED": "kept"},
		}
	}
	s.AddObject(good)
	s.UpdateStatus(good.Named(), store.StatusInfo{Status: store.StatusReady})
	info := controllertest.DispatchToFixpoint(
		t.Context(),
		t,
		controllertest.Dispatch{Tasks: c.Tasks, Store: s, ID: ks.Named(), ReconcileNode: c.ReconcileNode},
	)
	assert.Equal(t, info.Status, store.StatusReady)
	if testCaseInput.bad == nil {
		s.DeleteObject(good.Named())
	} else {
		s.AddObject(testCaseInput.bad)
		s.UpdateStatus(good.Named(), store.StatusInfo{Status: store.StatusReady})
	}
	reused := false
	info = controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{
		Tasks: c.Tasks, Store: s, ID: ks.Named(),
		ReconcileNode: func(
			ctx context.Context,
			id manifest.NamedResource,
			level manifest.DrainLevel,
		) *base.Execution {
			ex := c.ReconcileNode(ctx, id, level)
			reused = ex.Reused
			return ex
		},
	})
	assert.Equal(t, reused, false)
	assert.Equal(t, info.Status, store.StatusFailed)
}
