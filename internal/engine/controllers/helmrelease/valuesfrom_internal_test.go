package helmrelease

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/depwait"
	"github.com/home-operations/flate/internal/engine/helm"
	"github.com/home-operations/flate/internal/engine/schedule"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/values"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestController_ValuesFromExternalSecretMerge(t *testing.T) {
	t.Parallel()
	c, st := newTestControllerWithOptions(t, &base.Options{})
	publishRawFixture(t, c, &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
		Name: "app-creds", Namespace: "default",
		Spec: map[string]any{
			"target": map[string]any{
				"name": "app-values",
				"template": map[string]any{
					"mergePolicy": "Merge",
					"data":        map[string]any{"rendered.yaml": "{{ .values }}"},
				},
			},
			"data": []any{map[string]any{"secretKey": "values.yaml"}},
		},
	})
	hr := &manifest.HelmRelease{
		Name: "app", Namespace: "default",
		ValuesFrom: []manifest.ValuesReference{{
			Kind: manifest.KindSecret, Name: "app-values", ValuesKey: "values.yaml",
		}},
	}
	provider := values.NewStoreProvider(st, manifest.NamedResource{})
	inputs := resolveTestInputs(hr, provider, c.Missing)
	assert.Equal(t, len(inputs), 1)
	input := &inputs[0]
	assert.NoError(t, input.Err())
	assert.Equal(t, input.Effect(), values.Omitted)
	facts := provider.Lookup(input.ID())
	assert.Equal(t, facts.Generated, true)
	assert.Equal(t, slices.Contains(facts.Keys.Names, "values.yaml"), true)
	assert.Equal(t, facts.Keys.Complete, true)
}

func TestController_OmittedValuesAfterWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		waitValues   bool
		materialized bool
		unreadable   bool
		mixed        bool
		optional     bool
		duplicate    bool
		fail         bool
		wantGreeting string
	}{
		{name: "values wait, still missing", waitValues: true, wantGreeting: "fallback"},
		{name: "values wait, materialized", waitValues: true, materialized: true, wantGreeting: "supplied"},
		{name: "chart wait, still missing", wantGreeting: "fallback"},
		{name: "chart wait, materialized", materialized: true, wantGreeting: "supplied"},
		{name: "chart failed", fail: true},
		{name: "optional unreadable values file", unreadable: true, wantGreeting: "fallback"},
		{name: "duplicate omitted ref", duplicate: true, wantGreeting: "fallback"},
		{name: "mixed encrypted and readable duplicate positions", unreadable: true, mixed: true, wantGreeting: "readable"},
		{name: "optional absent values", optional: true, wantGreeting: "fallback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				testControllerOmittedValuesAfterWaitCase1(t, &tc)
			}
		})
	}
}

func testControllerOmittedValuesAfterWaitCase1(t *testing.T, tc *struct {
	name         string
	waitValues   bool
	materialized bool
	unreadable   bool
	mixed        bool
	optional     bool
	duplicate    bool
	fail         bool
	wantGreeting string
},
) {
	t.Helper()

	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: demo}
data:
  greeting: {{ .Values.greeting | default "fallback" | quote }}
`)
	c, st := newTestControllerWithOptions(
		t,
		&base.Options{Missing: depwait.NewMissingPolicy([]string{manifest.KindSecret})},
	)
	src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
	st.AddObject(src)
	st.SetArtifact(src.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
	})
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "fetching"})
	if tc.waitValues {
		st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	}
	publishRawFixture(t, c, &manifest.RawObject{
		Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
		Name: "app-creds", Namespace: "default",
		Spec: map[string]any{
			"target":   map[string]any{"name": "app-values"},
			"dataFrom": []any{map[string]any{"extract": map[string]any{"key": "input"}}},
		},
	})
	hr := &manifest.HelmRelease{
		Name:      "demo",
		Namespace: "default",
		Chart: manifest.HelmChart{
			Name: "mychart",
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "flux-system",
				Name:      "charts",
			},
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "mychart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindGitRepository,
						Name:       "charts",
						Namespace:  "flux-system",
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "app-values"}},
		},
	}
	if tc.unreadable && !tc.mixed || tc.optional {
		hr.ValuesFrom[0].Optional = true
	}
	if tc.duplicate {
		hr.ValuesFrom = append(
			hr.ValuesFrom,
			manifest.ValuesReference{Kind: manifest.KindSecret, Name: "app-values", ValuesKey: "other.yaml"},
		)
	}
	if tc.mixed {
		hr.ValuesFrom = append(
			hr.ValuesFrom,
			hr.ValuesFrom[0],
			manifest.ValuesReference{Kind: manifest.KindSecret, Name: "app-values", ValuesKey: "readable.yaml"},
			manifest.ValuesReference{
				Kind:      manifest.KindSecret,
				Name:      "app-values",
				ValuesKey: "optional.yaml",
				Optional:  true,
			},
		)
	}
	if tc.waitValues {
		hr.ValuesFrom = append(
			hr.ValuesFrom,
			manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: "chart-values"},
		)
	}
	st.AddObject(hr)
	if blocked := c.ReconcileNode(
		t.Context(),
		hr.Named(),
		manifest.DrainNone,
		schedule.Result{},
	).Blocked; len(
		blocked,
	) == 0 {
		t.Fatal("expected reconciliation to wait for a dependency")
	}
	assert.Equal(t, len(st.Warnings()), 0)
	st.AddObject(&manifest.ConfigMap{
		Name: "chart-values", Namespace: "default",
		Data: map[string]any{"values.yaml": "{}"},
	})
	if tc.materialized || tc.unreadable {
		st.AddDeclaration(omittedValuesInput(t, tc.unreadable, tc.mixed))
	}

	status := store.StatusReady
	if tc.fail {
		status = store.StatusFailed
	}
	st.UpdateStatus(src.Named(), store.StatusInfo{Status: status, Reason: "", Message: "chart source"})
	assert.Equal(t, reconcileFixture(t, c, hr.Named()).Status, status)
	assert.Equal(t, len(st.Warnings()), 0)
	if tc.mixed {
		assert.Equal(t, len(hr.ValuesFrom), 4)
		var omissions []manifest.Warning
		warnings := st.Warnings()
		for i := range warnings {
			warning := &warnings[i]
			if warning.Category == manifest.WarnOmittedValues {
				omissions = append(omissions, *warning)
			}
		}
		assert.Diff(t, omissions, []manifest.Warning(nil))
	}
	for valueIndex := range st.Warnings() {
		warning := st.Warnings()[valueIndex]
		assert.Equal(t, warning.Count, 1)
	}
	if tc.fail {
		return
	}
	art, ok := st.GetArtifact(hr.Named(), store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok, "%s", "HelmRelease artifact was not written")

	assert.Equal(t, renderedConfigMapValue(testutil.DocumentMaps(art.Manifests), "greeting"), tc.wantGreeting)
}

func TestResolvePreRenderValuesFrom_FailedChartRefTerminates(t *testing.T) {
	t.Parallel()
	c, s := newTestControllerWithOptions(
		t,
		&base.Options{Missing: depwait.NewMissingPolicy([]string{manifest.KindSecret, manifest.KindConfigMap})},
	)
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	s.AddObject(chart)
	s.UpdateStatus(
		chart.Named(),
		store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "chart unavailable"},
	)
	hr := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Source: manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindHelmChart,
				Namespace: chart.Namespace,
				Name:      chart.Name,
			},
		},
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      manifest.KindHelmChart,
			Name:      chart.Name,
			Namespace: chart.Namespace,
		},
	}
	s.AddObject(hr)
	attempts := 0
	unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == hr.Named() && payload.(store.StatusInfo).Message == "resolving dependencies" {
			attempts++

			assert.FatalIf(t, attempts > 1, "%v", "retried a failed chartRef with no removable valuesFrom refs")

			// Identical status writes suppress events, so reset the progress message.
			s.UpdateStatus(
				id,
				store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "checking references"},
			)
		}
	}, false)
	t.Cleanup(unsub)
	err := c.newExecution(hr.Named()).reconcile(t.Context(), hr, manifest.DrainForce)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)

	assert.FatalIf(t, !ok, "error = %v, want DependencyFailedError", err)

	assert.Diff(t, failed.Failed, []manifest.NamedResource{chart.Named()})
	assert.Equal(t, attempts, 1)
}

func TestController_ValuesFromMixedProductionPrecedence(t *testing.T) {
	t.Parallel()

	for _, declarationFirst := range []bool{false, true} {
		for _, materializer := range []string{"none", "pending", "actual"} {
			t.Run(fmt.Sprintf("declarationFirst=%t/%s", declarationFirst, materializer), func(t *testing.T) {
				t.Parallel()
				testControllerValuesFromMixedProductionPrecedenceCase(
					t,
					testControllerValuesFromMixedProductionPrecedenceCaseInputs{
						declarationFirst: &declarationFirst,
						materializer:     &materializer,
					},
				)
			},
			)
		}
	}
}

func TestController_PrepareRequiresDeclaration(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"failed", "withdrawn", "suspended"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			testControllerPrepareRequiresDeclarationCase(
				t,
				&state,
			)
		},
		)
	}
}

func TestPreviousRender_OptionalExpectedInput(t *testing.T) {
	t.Parallel()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	c, st := newTestController(t, nil)
	var err error
	c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
	c.Helm = helm.NewClient(c.Cache, nil)
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
	testutil.WriteFile(t, dir, "chart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n")
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
			ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "optional", Optional: true}},
		},
	}
	st.AddObject(hr)
	inputs := resolveTestInputs(hr, values.NewStoreProvider(st, manifest.NamedResource{}), c.Missing)
	seed, err := helm.Prepare(
		hr,
		helm.NewSources(st, nil).Declaration,
		inputs,
		c.Helm.ValuesCache(),
	)
	assert.NoError(t, err)
	_, receipt, err := testutil.RenderMaps(
		c.Helm.Template(
			t.Context(),
			seed,
			seed.Values,
			helm.RenderOptions{
				Sources: helm.NewSources(st, nil),
				Omitted: values.OmittedIDs(inputs),
			},
		),
	)
	assert.NoError(t, err)
	assert.NoError(
		t,
		c.Cache.RecordRender(
			t.Context(),
			sourcecache.RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: receipt,
				Sources: []manifest.SourceEvidence{},
				Records: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {},
			},
		),
	)
	probe := func() bool {
		t.Helper()
		reads := &store.ReadSet{}
		reads.Init(st, hr.Named(), nil)
		defer func() {
			reads.Retire()
			for range st.Sweep() {
			}
		}()
		preparedHistory344 := c.PreviousRender(t.Context(), hr, schedule.Result{})
		if preparedHistory344.Assessment != nil {
			t.Cleanup(preparedHistory344.Assessment.Close)
		}
		records, narrow := preparedHistory344.Records, preparedHistory344.Complete
		if records != nil {
			assert.Equal(t, narrow, true)
		}
		return records != nil
	}
	assert.Equal(t, probe(), persistent)
	publishRawFixture(
		t,
		c,
		&manifest.RawObject{
			APIVersion: "external-secrets.io/v1",
			Kind:       "ExternalSecret",
			Name:       "declaration",
			Namespace:  "apps",
			Spec:       map[string]any{"target": map[string]any{"name": "optional"}},
		},
	)
	producer := testutil.KustomizationID("apps", "producer-root")
	st.UpdateStatus(producer, store.StatusInfo{Status: store.StatusPending, Message: "waiting for input"})
	status, _ := st.GetStatus(producer)

	assert.FatalIf(t, probe(), "%v", "optional expected input supplied complete history")

	after, _ := st.GetStatus(producer)
	assert.Equal(t, after, status)
	assert.Equal(t, len(st.Warnings()), 0)
	assert.Equal(t, st.GetArtifact(hr.Named(), store.ArtifactContent) == nil, true)
}

func TestController_AllowMissingKindsAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, allowed string
		want          bool
	}{
		{manifest.KindSecret, manifest.KindSecret, true},
		{manifest.KindSecret, manifest.KindConfigMap, false},
		{manifest.KindConfigMap, manifest.KindConfigMap, true},
		{manifest.KindConfigMap, manifest.KindSecret, false},
	} {
		t.Run(tc.kind+"/"+tc.allowed, func(t *testing.T) {
			t.Parallel()
			c, st := newTestControllerWithOptions(
				t,
				&base.Options{Missing: depwait.NewMissingPolicy([]string{tc.allowed})},
			)
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
			testutil.WriteFile(
				t,
				dir,
				"chart/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n",
			)
			src := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
			st.AddObject(src)
			st.SetArtifact(
				src.Named(),
				&store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(dir), LocalPath: dir},
			)
			st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady})
			hr := &manifest.HelmRelease{
				Name:      "emitter",
				Namespace: "apps",
				Chart:     manifest.HelmChart{Name: "chart", Source: src.Named()},
				HelmReleaseSpec: helmv2.HelmReleaseSpec{
					Chart: &helmv2.HelmChartTemplate{
						Spec: helmv2.HelmChartTemplateSpec{
							Chart: "chart",
							SourceRef: &helmv2.CrossNamespaceObjectReference{
								Kind:       manifest.KindGitRepository,
								Name:       src.Name,
								Namespace:  src.Namespace,
								APIVersion: manifest.SourceDomain + "/v1",
							},
						},
					},
					ValuesFrom: []manifest.ValuesReference{{Kind: tc.kind, Name: "missing"}},
				},
			}
			st.AddObject(hr)
			info := controllertest.DispatchToFixpoint(
				t.Context(),
				t,
				controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode},
			)

			assert.FatalIf(t, (info.Status == store.StatusReady) != tc.want, "kind selection: %+v", info)
			assert.FatalIf(t, tc.want && len(st.Warnings()) != 1,
				"omitted input lost warning: %+v", st.Warnings())
		})
	}
}

func TestPreviousRender_AllowMissingKindsAdmission(t *testing.T) {
	t.Parallel()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	for _, kind := range []string{manifest.KindSecret, manifest.KindConfigMap} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			c, st := newTestControllerWithOptions(t, &base.Options{Missing: depwait.NewMissingPolicy([]string{kind})})
			c.Cache = cachetest.New(t, t.TempDir(), sourcecache.New)
			c.Helm = helm.NewClient(c.Cache, nil)
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "chart/Chart.yaml", "apiVersion: v2\nname: fixture\nversion: 1.0.0\n")
			testutil.WriteFile(
				t,
				dir,
				"chart/templates/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child}\n",
			)
			src := &manifest.GitRepository{Name: "charts", Namespace: "apps"}
			st.AddObject(src)
			st.SetArtifact(
				src.Named(),
				&store.SourceArtifact{Kind: manifest.KindGitRepository, FS: os.DirFS(dir), LocalPath: dir},
			)
			st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady})
			hr := &manifest.HelmRelease{
				Name:      "emitter",
				Namespace: "apps",
				Chart:     manifest.HelmChart{Name: "chart", Source: src.Named()},
				HelmReleaseSpec: helmv2.HelmReleaseSpec{
					Chart: &helmv2.HelmChartTemplate{
						Spec: helmv2.HelmChartTemplateSpec{
							Chart: "chart",
							SourceRef: &helmv2.CrossNamespaceObjectReference{
								Kind:       manifest.KindGitRepository,
								Name:       src.Name,
								Namespace:  src.Namespace,
								APIVersion: manifest.SourceDomain + "/v1",
							},
						},
					},
					ValuesFrom: []manifest.ValuesReference{{Kind: kind, Name: "missing"}},
				},
			}
			st.AddObject(hr)
			inputs := resolveTestInputs(hr, values.NewStoreProvider(st, manifest.NamedResource{}), c.Missing)
			seed, err := helm.Prepare(
				hr,
				helm.NewSources(st, nil).Declaration,
				inputs,
				c.Helm.ValuesCache(),
			)
			assert.NoError(t, err)
			_, receipt, err := c.Helm.Template(
				t.Context(),
				seed,
				seed.Values,
				helm.RenderOptions{
					Sources: helm.NewSources(st, nil),
					Omitted: values.OmittedIDs(inputs),
				},
			)
			assert.NoError(t, err)
			assert.NoError(
				t,
				c.Cache.RecordRender(
					t.Context(),
					sourcecache.RenderAcceptance{
						Owner:   hr.Named(),
						Receipt: receipt,
						Sources: []manifest.SourceEvidence{},
						Records: func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {},
					},
				),
			)
			for _, selected := range []bool{true, false} {
				c.Missing = depwait.NewMissingPolicy(nil)
				if selected {
					c.Missing = depwait.NewMissingPolicy([]string{kind})
				}
				reads := &store.ReadSet{}
				reads.Init(st, hr.Named(), nil)
				preparedHistory514 := c.PreviousRender(t.Context(), hr, schedule.Result{})
				if preparedHistory514.Assessment != nil {
					t.Cleanup(preparedHistory514.Assessment.Close)
				}
				records, _ := preparedHistory514.Records, preparedHistory514.Complete
				reads.Retire()

				assert.FatalIf(
					t,
					(records != nil) != (selected && persistent),
					"history admitted=%t selected=%t",
					records != nil,
					selected,
				)
			}
		})
	}
}

type testControllerValuesFromMixedProductionPrecedenceCaseInputs struct {
	declarationFirst *bool
	materializer     *string
}

func testControllerValuesFromMixedProductionPrecedenceCase(
	t *testing.T,
	testCaseInput testControllerValuesFromMixedProductionPrecedenceCaseInputs,
) {
	t.Helper()

	c, s := newTestControllerWithOptions(t, &base.Options{})
	target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "a",
	}
	if !(*testCaseInput.declarationFirst) {
		owner.Name = "z"
	}
	if (*testCaseInput.materializer) == "pending" {
		s.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					target: {
						Productions: []manifest.Production{
							{
								Owner:    owner,
								Authored: target,
								Path:     "source/values.yaml",
								File:     "values.yaml",
							},
						},
					},
				},
			},
		)
	}
	publishRawFixture(
		t,
		c,
		&manifest.RawObject{
			APIVersion: "external-secrets.io/v1",
			Kind:       "ExternalSecret",
			Namespace:  "apps",
			Name:       "declaration",
			Spec: map[string]any{
				"target":   map[string]any{"name": "values"},
				"dataFrom": []any{map[string]any{"extract": map[string]any{"key": "input"}}},
			},
		},
	)
	if (*testCaseInput.materializer) == "actual" {
		s.AddObject(
			&manifest.Secret{
				Name:       target.Name,
				Namespace:  target.Namespace,
				StringData: map[string]any{"values.yaml": "real"},
			},
		)
	}
	execution := c.newExecution(
		manifest.NamedResource{
			Group:     manifest.HelmReleaseDomain,
			Kind:      manifest.KindHelmRelease,
			Namespace: "apps",
			Name:      "reader",
		},
	)
	s.UpdateStatus(
		testutil.KustomizationID("apps", "producer-root"),
		store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""},
	)
	input := execution.run.Data().Lookup(target)
	w := depwait.Waiter{
		Store:   &execution.run.Reads,
		Parent:  execution.run.Reads.ID(),
		Missing: depwait.NewMissingPolicy([]string{manifest.KindSecret}),
	}
	result := w.Available(
		t.Context(),
		depwait.DataDependency{
			NamedResource: target, Key: "values.yaml", Use: "valuesFrom",
			Input: input,
		},
		manifest.DrainNone,
	)
	hasProducer, omit := input.Generated, result.Result.Effect() == values.Omitted

	assert.FatalIf(
		t,
		omit != ((*testCaseInput.materializer) == "none") || omit && !hasProducer,
		"mixed evidence omitted=%t producer=%t",
		omit,
		hasProducer,
	)
}

func testControllerPrepareRequiresDeclarationCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	c, st := newTestControllerWithOptions(
		t,
		&base.Options{Missing: depwait.NewMissingPolicy([]string{manifest.KindSecret, manifest.KindConfigMap})},
	)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	input := &manifest.HelmRelease{
		Name:      "input",
		Namespace: "apps",
		Chart: manifest.HelmChart{
			Name: "chart",
			Source: manifest.NamedResource{
				Group: manifest.SourceDomain,
				Kind:  manifest.KindHelmRepository,
				Name:  "absent",
			},
		},
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart: &helmv2.HelmChartTemplate{
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:   "chart",
					Version: "",
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						Kind:       manifest.KindHelmRepository,
						Name:       "absent",
						Namespace:  "",
						APIVersion: manifest.SourceDomain + "/v1",
					},
				},
			},
		},
	}
	for _, object := range []manifest.BaseManifest{parent, input} {
		st.AddObject(object)
	}
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				input.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
			},
		},
	)
	switch *testCaseInput {
	case "failed":
		st.UpdateStatus(
			parent.Named(),
			store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "materializer failed"},
		)
	case "withdrawn":
		st.DeleteObject(input.Named())
	case "suspended":
		accepted := input.Clone()
		accepted.Suspend = true
		st.RecordEmitted(
			parent.Named(),
			captureEmission(st, parent.Named(), &store.Emission{Children: []store.Child{{Obj: accepted}}}),
			nil,
		)
	}
	run := c.ReconcileNode(t.Context(), input.Named(), manifest.DrainNone, schedule.Result{})

	info, _ := st.GetStatus(input.Named())
	assert.FatalIf(t, (*testCaseInput) == "suspended" && run.Outcome() == schedule.Blocked ||
		st.GetArtifact(input.Named(), store.ArtifactContent) != nil ||
		len(st.Warnings()) != 0,
		"declaration loss reached preparation: wait=%+v status=%+v", run.Blocked, info)
	switch *testCaseInput {
	case "withdrawn":
		assert.Diff(t, run.Blocked, []manifest.NamedResource{input.Named(), parent.Named()})
	case "failed":
		assert.Equal(t, run.Outcome(), schedule.DependencyFailed)
		assert.Diff(t, info.BlockedBy(), []manifest.NamedResource{parent.Named()})
	}

	assert.FatalIf(
		t, (*testCaseInput) ==
			"failed" && !info.IsDependencyFailure(), "materializer failure lost provenance: %+v",
		info,
	)

	assert.FatalIf(
		t, (*testCaseInput) ==
			"suspended" && info.Outcome() != store.OutcomeSuspended, "effective suspension lost: %+v",
		info,
	)
}

func omittedValuesInput(t *testing.T, unreadable, mixed bool) store.Declaration {
	t.Helper()
	value := "greeting: supplied\n"
	if unreadable {
		value = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	}
	data := map[string]any{"values.yaml": value}
	if mixed {
		data["readable.yaml"] = "greeting: readable\n"
	}
	parsed, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "v1", "kind": manifest.KindSecret,
		"metadata":   map[string]any{"name": "app-values", "namespace": "default"},
		"stringData": data,
	}, manifest.ParseDocOptions{})
	assert.NoError(t, err)
	return store.Declaration{Obj: parsed.Object, Doc: parsed.Document, Warnings: parsed.Warnings}
}
