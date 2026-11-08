package helmrelease

import (
	"context"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/controllers/base"
	"github.com/home-operations/flate/internal/helm"
	"github.com/home-operations/flate/internal/schedule"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReconcile_MutableChartAndSourceValuesReplay(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	root := t.TempDir()
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "values/overrides.yaml", "value: first\n")
	writeTemplate := func(value string) {
		t.Helper()
		testutil.WriteFile(
			t,
			root,
			"chart/templates/output.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}-first'}\ndata: {value: '{{ .Values.value }}', source: "+value+"}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}-second'}\ndata: {value: '{{ .Values.value }}', source: "+value+"}\n",
		)
	}
	writeTemplate("one")
	input := &chartCaptureFS{FS: os.DirFS(root)}
	source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), &store.SourceArtifact{Kind: manifest.KindGitRepository, FS: input, LocalPath: root})
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	hr := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Name: "chart",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: source.Namespace,
				Name:      source.Name,
			},
		},
		ChartValuesFiles: []string{"values/overrides.yaml"},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "chart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindGitRepository,
						Name:       source.Name,
						Namespace:  source.Namespace,
						APIVersion: manifest.SourceDomain + "/v1",
					},
					ValuesFiles: []string{"values/overrides.yaml"},
				},
			},
		},
	}
	st.AddObject(hr)
	var executions []*base.Execution
	t.Cleanup(func() {
		for _, execution := range executions {
			execution.Close()
		}
		for range st.Sweep() {
		}
	})
	var pending atomic.Bool
	stop := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if info, ok := payload.(store.StatusInfo); id == hr.Named() && ok && info.Status == store.StatusPending {
			pending.Store(true)
		}
	}, false)
	t.Cleanup(stop)
	reconcile := func(reused bool) *store.Emission {
		t.Helper()
		pending.Store(false)
		before := input.captures.Load()
		var next *base.Execution
		c.Tasks.Go(t.Context(), "reconcile", func(ctx context.Context) {
			next = c.ReconcileNode(ctx, hr.Named(), manifest.DrainNone, schedule.Result{})
		})
		c.Tasks.BlockTillDone()
		assert.FatalIf(
			t,
			c.Tasks.Failures() != 0 || next == nil || next.Outcome() == schedule.Blocked ||
				(next.Disposition == store.AssessmentReused) != reused,
			"reconcile: failures=%d execution=%+v, want reused=%t",
			c.Tasks.Failures(),
			next,
			reused,
		)
		assert.FatalIf(t, reused && (pending.Load() || input.captures.Load()-before != 1),

			"equal replay: pending=%t chart captures=%d, want false/1",
			pending.Load(),
			input.captures.Load()-before)

		if len(executions) != 0 {
			executions[len(executions)-1].Retire()
		}
		executions = append(executions, next)
		for range st.Sweep() {
		}
		return st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	}
	testReconcileMutableChartAndSourceValuesReplayTail(
		t,
		&testReconcileMutableChartAndSourceValuesReplayTailInputs{
			hr:            &hr,
			reconcile:     &reconcile,
			st:            &st,
			writeTemplate: &writeTemplate,
			source:        &source,
			input:         &input,
			root:          &root,
		},
	)
}

func testReconcileMutableChartAndSourceValuesReplayTail(
	t *testing.T,
	tailCaseInput *testReconcileMutableChartAndSourceValuesReplayTailInputs,
) {
	t.Helper()
	assertOutput := func(output *store.Emission, value, source string) {
		t.Helper()
		assert.FatalIf(t, len(output.Manifests) != 2,
			"documents=%d, want 2", len(output.Manifests))

		for i, doc := range output.Manifests {
			result1 := manifest.DocMetadata(doc.Document)
			name, namespace := result1.Name, result1.Namespace
			data, _ := doc.Document["data"].(map[string]any)
			wantName := []string{"app-first", "app-second"}[i]
			assert.FatalIf(
				t,
				name != wantName || namespace != (*tailCaseInput.hr).Namespace || data["value"] != value ||
					data["source"] != source,
				"document=%v, want %s/%s value=%s source=%s",
				doc,
				(*tailCaseInput.hr).Namespace,
				wantName,
				value,
				source,
			)
		}
	}
	initial := (*tailCaseInput.reconcile)(false)
	assertOutput(initial, "first", "one")
	stamped := (*tailCaseInput.hr).Clone()
	stamped.Labels = map[string]string{"kustomize.toolkit.fluxcd.io/name": "parent"}
	(*tailCaseInput.st).
		AddObject(stamped)
	equal := (*tailCaseInput.reconcile)(true)

	assert.FatalIf(
		t,
		equal.Fingerprint != initial.Fingerprint || !cmp.Equal(equal.Manifests, initial.Manifests),
		"%v",
		"metadata-only replay changed content",
	)

	(*tailCaseInput.writeTemplate)("two")
	(*tailCaseInput.st).
		SetArtifact((*tailCaseInput.source).Named(), &store.SourceArtifact{
			Kind:      manifest.KindGitRepository,
			FS:        (*tailCaseInput.input),
			LocalPath: (*tailCaseInput.root),
		},
		)
	withChart := (*tailCaseInput.reconcile)(false)
	assertOutput(withChart, "first", "two")

	assert.FatalIf(
		t,
		withChart.Fingerprint == equal.Fingerprint,
		"%v",
		"same-path mutable chart change kept the old render key",
	)

	testutil.WriteFile(t, (*tailCaseInput.root), "values/overrides.yaml", "value: second\n")
	(*tailCaseInput.st).
		SetArtifact((*tailCaseInput.source).Named(), &store.SourceArtifact{
			Kind:      manifest.KindGitRepository,
			FS:        (*tailCaseInput.input),
			LocalPath: (*tailCaseInput.root),
		},
		)
	withValues := (*tailCaseInput.reconcile)(false)
	assertOutput(withValues, "second", "two")

	assert.FatalIf(
		t,
		withValues.Fingerprint == withChart.Fingerprint,
		"%v",
		"external source values change kept the old render key",
	)

	final := (*tailCaseInput.reconcile)(true)
	assertOutput(final, "second", "two")

	assert.FatalIf(
		t,
		final.Fingerprint != withValues.Fingerprint || !cmp.Equal(final.Manifests, withValues.Manifests),
		"%v",
		"unchanged mutable source replay changed output",
	)
}

func TestReconcile_ListNormalizationFailure(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/list.yaml", `apiVersion: v1
kind: ConfigMapList
items:
- apiVersion: v1
  metadata: {name: invalid}
  data: {value: '{{ .Values.required }}'}
`)
	src := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{FS: os.DirFS(dir), LocalPath: dir})
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady})
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	st.AddObject(parent)
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "apps",
		Values:    map[string]any{"required": ""},
		Chart:     manifest.HelmChart{Name: "mychart", Source: src.Named()},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "mychart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       src.Named().Kind,
						Name:       src.Named().Name,
						Namespace:  src.Named().Namespace,
						APIVersion: "",
					},
				},
			},
		},
	}
	controllertest.Emit(
		t.Context(),
		c.Controller,
		parent.Named(),
		&store.Emission{
			Children:   []store.Child{{Obj: hr}},
			Complete:   true,
			Unreadable: []manifest.NamedResource{{Kind: manifest.KindSecret, Namespace: "apps", Name: "unreadable"}},
		},
	)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
	info := reconcileFixture(t, c, hr.Named())

	assert.FatalIf(
		t,
		info.Status != store.StatusFailed || !strings.Contains(info.Message, "Object 'Kind' is missing"),
		"invalid list did not retain its original failure: %+v",
		info,
	)

	{
		art, _ := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(t, art != nil && art.Complete, "%v", "failed render published completed output")
	}

	assert.FatalIf(
		t,
		st.GetObject(
			manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "invalid", Namespace: "apps"},
		) != nil,
		"%v",
		"failed render published a repaired child",
	)

	assert.FatalIf(t, slices.ContainsFunc(
		st.Warnings(),
		func(w manifest.Warning) bool { return w.Category == manifest.WarnUnresolvedSubstitution },
	), "%v", "failed render reported successful placeholder substitution")
}

func TestReconcile_UnreadableParentDoesNotFillRequiredValues(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "empty", value: ""},
		{name: "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testReconcileUnreadableParentDoesNotFillRequiredValuesCase1(t, &tc)
		})
	}
}

func testReconcileUnreadableParentDoesNotFillRequiredValuesCase1(t *testing.T, tc *struct {
	name  string
	value any
},
) {
	t.Helper()

	c, st := newTestController(t, nil)
	cacheDir := t.TempDir()
	c.Helm = helm.NewClient(cachetest.New(t, cacheDir, sourcecache.New))
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		dir,
		"chart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rescued, namespace: apps}\ndata:\n  value: {{ required \"missing required value\" .Values.required | quote }}\n",
	)
	source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(dir), LocalPath: dir},
	)
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	hr := &manifest.HelmRelease{
		Name:      "emitter",
		Namespace: "apps",
		Chart:     manifest.HelmChart{Name: "chart", Source: source.Named()},
		Values:    map[string]any{"required": tc.value},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "chart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       source.Named().Kind,
						Name:       source.Named().Name,
						Namespace:  source.Named().Namespace,
						APIVersion: "",
					},
				},
			},
		},
	}
	original := hr.Clone()
	parent := &manifest.Kustomization{
		Name:      "parent",
		Namespace: "apps",
		PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []manifest.SubstituteReference{{Kind: manifest.KindSecret, Name: "unreadable"}},
		},
	}
	st.AddObject(parent)
	controllertest.Emit(
		t.Context(),
		c.Controller,
		parent.Named(),
		&store.Emission{
			Children: []store.Child{{Obj: hr}},
			Complete: true,
			Unreadable: []manifest.NamedResource{
				{Kind: manifest.KindSecret, Name: "unreadable", Namespace: "apps"},
			},
		},
	)
	st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady})
	info := reconcileFixture(t, c, hr.Named())
	assert.ErrorIf(t, info.Status != store.StatusFailed || !strings.Contains(info.Message, "missing required value"),
		"required chart value did not retain its original failure: %+v", info)

	stored := st.GetObject(hr.Named()).(*manifest.HelmRelease)
	assert.Diff(t, stored.Values, original.Values)
	if emission, _ := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission); emission != nil &&
		(emission.Complete || emission.Render != nil || len(emission.Manifests) != 0) {
		t.Error("failed chart published completed output or a successful receipt")
	}
	assert.ErrorIf(t, st.GetObject(
		manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "rescued", Namespace: "apps"},
	) != nil, "%s", "failed chart published a fabricated ConfigMap")
	assert.ErrorIf(t, slices.ContainsFunc(st.Warnings(), func(w manifest.Warning) bool {
		return w.Resource == hr.Named() && w.Category == manifest.WarnUnresolvedSubstitution
	}), "%s", "failed chart reported successful substitution")

	t.Run("history", func(t *testing.T) {
		t.Parallel()
		if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
			t.Skip(reason)
		}
		fresh := helm.NewClient(cachetest.New(t, cacheDir, sourcecache.New))
		for _, inputs := range []map[string]any{original.Values, {"required": "invalid"}} {
			if _, complete := fresh.PreviousRender(
				t.Context(),
				hr,
				inputs,
				helm.RenderOptions{Sources: helm.NewSources(st)},
			); complete {
				t.Errorf("failed chart has successful history for values %#v", inputs)
			}
		}
	})
}

func TestReconcile_SuppliedAdvisoriesOnReplay(t *testing.T) {
	t.Parallel()
	c, st := newTestController(t, nil)
	root := t.TempDir()
	testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"chart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: '{{ .Values.value }}'}\n",
	)
	source := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
	st.AddObject(source)
	st.SetArtifact(
		source.Named(),
		&store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(root), LocalPath: root},
	)
	st.UpdateStatus(source.Named(), store.StatusInfo{Status: store.StatusReady})
	input := &manifest.ConfigMap{
		Name:      "values",
		Namespace: "apps",
		Data:      map[string]any{"values.yaml": "value: equal\n"},
	}
	st.AddDeclaration(
		store.Declaration{
			Obj:       input,
			Doc:       testutil.ObjectDocument(input),
			Origin:    manifest.SuppliedDeclaration,
			InputPath: "supply.yaml",
		},
	)
	hr := &manifest.HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{
		Chart: &helmv2.HelmChartTemplate{
			Spec: helmv2.HelmChartTemplateSpec{
				Chart: "chart",
				SourceRef: &helmv2.CrossNamespaceObjectReference{
					Kind:      manifest.KindGitRepository,
					Name:      source.Name,
					Namespace: source.Namespace,
				},
			},
		},
		ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: input.Name}},
	}}
	st.AddObject(hr)
	var previous *base.Execution
	var first *store.Emission
	testReconcileSuppliedAdvisoriesOnReplaySegment(
		t,
		&testReconcileSuppliedAdvisoriesOnReplaySegmentInputs{
			c:        &c,
			st:       &st,
			input:    &input,
			hr:       &hr,
			previous: &previous,
			first:    &first,
		},
	)
}

type testReconcileMutableChartAndSourceValuesReplayTailInputs struct {
	hr **manifest.
		HelmRelease

	reconcile *func(
		reused bool) *store.
		Emission

	st **store.
		Store

	writeTemplate *func(
		value string)

	source **manifest.
		GitRepository

	input **chartCaptureFS

	root *string
}

type testReconcileSuppliedAdvisoriesOnReplaySegmentInputs struct {
	c        **Controller
	st       **store.Store
	input    **manifest.ConfigMap
	hr       **manifest.HelmRelease
	previous **base.Execution
	first    **store.Emission
}

func testReconcileSuppliedAdvisoriesOnReplaySegment(
	t *testing.T,
	statementInput *testReconcileSuppliedAdvisoriesOnReplaySegmentInputs,
) {
	t.Helper()
	for i, value := range []string{"", "equal", "", "fresh"} {
		repository := &manifest.ConfigMap{
			Name:      (*statementInput.input).Name,
			Namespace: (*statementInput.input).Namespace,
			Data:      map[string]any{},
		}
		if value != "" {
			repository.Data["values.yaml"] = "value: " + value + "\n"
		}
		(*statementInput.st).AddObject(repository)
		next := statementInput.dispatchReplay(t)
		if (*statementInput.previous) != nil {
			(*statementInput.previous).Retire()
		}
		(*statementInput.previous) = next
		t.Cleanup(next.Close)

		assert.FatalIf(
			t,
			next.Outcome() == schedule.Blocked || !next.Valid() ||
				(next.Disposition == store.AssessmentReused) != (i == 1 || i == 2),
			"phase %d: execution=%+v",
			i,
			next,
		)

		if info, ok := (*statementInput.st).GetStatus((*statementInput.hr).Named()); !ok ||
			info.Status != store.StatusReady {
			t.Fatalf(
				"phase %d: status=%+v, chart declaration=%+v, want Ready",
				i,
				info, (*statementInput.hr).HelmReleaseSpec.Chart,
			)
		}
		current := (*statementInput.st).GetArtifact((*statementInput.hr).Named(), store.ArtifactContent).(*store.Emission)

		assert.FatalIf(
			t,
			!current.Complete || len(current.Manifests) != 1,
			"phase %d: incomplete rendered emission: %+v",
			i,
			current,
		)

		if i == 0 {
			(*statementInput.first) = current
		} else {
			assert.FatalIf(
				t,
				i < 3 &&
					(current.Fingerprint != (*statementInput.first).Fingerprint || !cmp.Equal(current.Manifests, (*statementInput.first).Manifests)),
				"%v",
				"equal-output collision transition changed render output",
			)
		}
		var want []store.SuppliedOverride
		if value != "" {
			want = []store.SuppliedOverride{
				{Resource: (*statementInput.input).Named(), SupplyPath: "supply.yaml", Keys: []string{"values.yaml"}},
			}
		}
		assert.Diff(t, (*statementInput.st).AdvisoryEvidence().Overrides, want)
		wantValue := "equal"
		if i == 3 {
			wantValue = "fresh"
		}
		assert.Equal(t, current.Manifests[0].Document["data"].(map[string]any)["value"], any(wantValue))
	}
}

func (p *testReconcileSuppliedAdvisoriesOnReplaySegmentInputs) dispatchReplay(t *testing.T) *base.Execution {
	t.Helper()
	var next *base.Execution
	d := controllertest.Dispatch{
		Tasks: (*p.c).Tasks,
		Store: (*p.st),
		ReconcileNode: func(ctx context.Context, id manifest.NamedResource, level manifest.DrainLevel, previous schedule.Result) *base.Execution {
			run := (*p.c).reconcileNodeForTest(ctx, id, level, previous)
			if id == (*p.hr).Named() {
				next = run
			}
			return run
		},
	}
	scheduler := schedule.New((*p.c).Tasks, d)
	stop := (*p.st).AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if (*p.c).Owns(id) || id.Group == manifest.SourceDomain && id.Kind == manifest.KindHelmChart {
			scheduler.OnArrival(id, true)
		}
	}, true)
	scheduler.Seed([]schedule.NodeID{(*p.hr).Named()})
	assert.NoError(t, scheduler.Run(t.Context()))
	stop()
	assert.Equal(t, (*p.c).Tasks.Failures(), 0)
	return next
}
