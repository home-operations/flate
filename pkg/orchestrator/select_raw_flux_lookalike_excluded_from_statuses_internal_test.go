package orchestrator

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSelect_RawFluxLookalikeExcludedFromStatuses(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindBucket, manifest.KindKustomization, manifest.KindHelmRelease, manifest.KindResourceSet} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			run := selectedRun(&Result{})
			obj := &manifest.RawObject{
				Kind:       kind,
				APIVersion: "seaweed.seaweedfs.com/v1",
				Name:       "foreign",
				Namespace:  "apps",
			}
			run.store.AddObject(obj)
			run.store.UpdateStatus(obj.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			selected, err, _ := Select(
				[]Rendered{run},
				nil,
				[]manifest.NamedResource{{Group: obj.Named().Group, Kind: obj.Kind}},
				nil,
			)
			if err != nil || len(selected[0].Statuses) != 0 {
				t.Fatalf("foreign Flux lookalike entered roster: %+v %v", selected[0].Statuses, err)
			}
		})
	}
}

func TestSelect_ExplicitChartVersion(t *testing.T) {
	t.Parallel()
	selectVersions := func(t *testing.T, run Rendered, obj manifest.BaseManifest) map[manifest.NamedResource]string {
		t.Helper()
		id := obj.Named()
		var objects []manifest.BaseManifest
		var versions map[manifest.NamedResource]string
		for i := range 2 {
			selected, err, diagnostic := Select(
				[]Rendered{run},
				nil,
				[]manifest.NamedResource{{Group: id.Group, Kind: id.Kind}},
				nil,
			)
			assert.NoError(t, err)
			assert.NoError(t, diagnostic)
			assert.FatalIf(
				t,
				len(selected) != 1 || len(selected[0].Objects) != 1 || selected[0].Objects[0] != obj,
				"selected rows: %+v, want %s",
				selected,
				id,
			)
			if i == 1 {
				{
					diff := cmp.Diff(
						objects,
						selected[0].Objects,
						cmpopts.IgnoreUnexported(
							manifest.HelmRelease{},
							manifest.OCIRepository{},
							manifest.HelmChartSource{},
						),
					)
					assert.FatalIf(t, diff != "", "%v", diff)
				}
				assert.Diff(t, selected[0].ChartVersions, versions)
			}
			objects = selected[0].Objects
			versions = selected[0].ChartVersions
		}
		return versions
	}
	for _, ns := range []struct{ namespace, source string }{{"", "apps"}, {"foreign", "foreign"}} {
		for _, tc := range []struct {
			name, kind, want string
			ref              *manifest.OCIRepositoryRef
			absent           bool
		}{
			{name: "digest", ref: &manifest.OCIRepositoryRef{Digest: "sha256:abc", SemVer: ">=1.0.0", Tag: "1.2.3"}, want: "sha256:abc"},
			{name: "semver", ref: &manifest.OCIRepositoryRef{SemVer: ">=1.0.0", Tag: "1.2.3"}, want: ">=1.0.0"},
			{name: "tag", ref: &manifest.OCIRepositoryRef{Tag: "1.2.3"}, want: "1.2.3"},
			{name: "empty reference", ref: &manifest.OCIRepositoryRef{}},
			{name: "nil reference"},
			{name: "missing source", absent: true},
			{name: "helm chart", kind: manifest.KindHelmChart, want: "2.3.4"},
			{name: "empty helm chart", kind: manifest.KindHelmChart},
			{name: "missing helm chart", kind: manifest.KindHelmChart, absent: true},
			{name: "nonapplicable source", kind: manifest.KindExternalArtifact},
		} {
			t.Run(tc.name+"/source_"+ns.namespace, func(t *testing.T) {
				t.Parallel()
				testSelectExplicitChartVersionCase3(
					t,
					testSelectExplicitChartVersionCase3Inputs{selectVersions: &selectVersions, ns: &ns, tc: &tc},
				)
			},
			)
		}
	}
	oci := &manifest.OCIRepository{Name: "chart", Namespace: "apps"}
	oci.Reference = &sourcev1.OCIRepositoryRef{Tag: "1.2.3"}
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	chart.Version = "2.3.4"
	for _, obj := range []manifest.BaseManifest{oci, chart, &manifest.ConfigMap{Name: "config", Namespace: "apps"}} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			testSelectExplicitChartVersionCase4(
				t,
				testSelectExplicitChartVersionCase4Inputs{selectVersions: &selectVersions, obj: &obj},
			)
		},
		)
	}
}

func TestSelect_DiagnosticOnlyRuns(t *testing.T) {
	t.Parallel()
	failure, teardown := errors.New("bootstrap failed"), errors.New("shared teardown")
	warning := manifest.Warning{Category: manifest.WarnInputParseFailed, Message: "before failure", Count: 1}
	coverage := manifest.Warning{
		Resource: testutil.KustomizationID("ns", "partial"),
		Category: manifest.WarnIncompleteInputCoverage,
		Message:  "coverage before failure",
		Count:    1,
	}
	for _, scenario := range []string{"single", "orig", "current"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testSelectDiagnosticOnlyRunsCase(
				t,
				testSelectDiagnosticOnlyRunsCaseInputs{
					failure:  &failure,
					teardown: &teardown,
					warning:  &warning,
					coverage: &coverage,
					scenario: &scenario,
				},
			)
		},
		)
	}
}

func TestSelect_SourceWarningAttribution(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"one", "two"} {
		testutil.WriteFile(
			t,
			root,
			name+".yaml",
			testutil.KustomizationYAML(
				name,
				"apps",
				"  path: ./app\n  sourceRef: {kind: GitRepository, name: "+name+"}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: []\n")
	original, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
	assert.NoError(t, err)
	current, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
	assert.NoError(t, err)
	status := manifest.Warning{
		Category: manifest.WarnBaselineStatusUnavailable,
		Message:  "Git status unavailable",
		Detail:   []string{"status failure"},
	}
	raw := manifest.Warning{
		Resource: manifest.BootstrapSourceID,
		Category: manifest.WarnSkippedSubmodule,
		Message:  "skipping submodule",
		Detail:   []string{"app/module"},
	}
	current.Warnings = []manifest.Warning{status, raw}
	for range 2 {
		renderedTrees1, err := RenderTrees(
			t.Context(),
			[]Tree{
				{
					RepoRoot:  root,
					ID:        manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					Input:     original,
					BaseInput: current,
				},
				{
					RepoRoot:  root,
					ID:        manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:     current,
					BaseInput: original,
				},
			},
			Config{Concurrency: 2},
		)
		orig, head := renderedTrees1[0], renderedTrees1[1]
		assert.NoError(t, err)
		for _, name := range []string{"one", "two"} {
			artifact := head.store.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: name}, store.ArtifactContent).(*store.SourceArtifact)
			assert.Diff(t, artifact.Warnings, []manifest.Warning(nil))
		}
		selected, err, diagnostic := Select(
			[]Rendered{orig, head},
			err,
			[]manifest.NamedResource{testutil.KustomizationID("", "")},
			nil,
		)
		assert.NoError(t, err)
		assert.NoError(t, diagnostic)
		for _, warning := range selected[0].Warnings {
			assert.FatalIf(
				t,
				warning.Category == status.Category || warning.Category == raw.Category,
				"current evidence escaped to orig: %+v",
				warning,
			)
		}
		statusCount, modules := 0, 0
		for _, warning := range selected[1].Warnings {
			if warning.Category == status.Category {
				statusCount++
				assert.Equal(t, warning.Count, 1)
				assert.Equal(t, warning.Resource, manifest.NamedResource{})
			}
			if warning.Category == raw.Category {
				modules++
				want := raw
				want.Subject = manifest.DiagnosticFor(raw.Resource, nil)
				want.Count = 1
				assert.Diff(t, warning, want)
			}
		}
		assert.Equal(t, statusCount, 1)
		assert.Equal(t, modules, 1)
	}
	assert.Diff(t, current.Warnings, []manifest.Warning{status, raw})
}

func TestRenderTrees_InputWarningOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	warning := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "before failure", Count: 1}
	runs, err := RenderTrees(
		t.Context(),
		[]Tree{
			{
				ID:    manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path:  root,
				Input: &store.SourceArtifact{LocalPath: root, Warnings: []manifest.Warning{warning}},
			},
		},
		Config{Concurrency: 2},
	)
	assert.FatalIf(t, len(runs) != 1, "missing diagnostic run: %v", err)
	o, result := runs[0].Orchestrator, runs[0].Result
	assert.FatalIf(t, o != nil || result == nil || !errors.Is(err, manifest.ErrInput) ||
		len(
			result.Manifests,
		)+len(
			result.Failed,
		) != 0, "failed bootstrap exposed content: %v, %+v, %v", o, result, err)
	assert.Diff(t, result.Warnings, []manifest.Warning{warning})
}

func TestSelect_ComparedRootFailureOutsideLocalNamespace(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		for _, required := range []bool{false, true} {
			t.Run(fmt.Sprintf("namespace/required=%t/reverse=%t", required, reverse), func(t *testing.T) {
				t.Parallel()
				testSelectComparedRootFailureOutsideLocalNamespaceCase(
					t,
					testSelectComparedRootFailureOutsideLocalNamespaceCaseInputs{
						reverse:  &reverse,
						required: &required,
					},
				)
			},
			)
		}
	}
	for _, compared := range []bool{true, false} {
		t.Run(fmt.Sprintf("compared=%t", compared), func(t *testing.T) {
			t.Parallel()
			testSelectComparedRootFailureOutsideLocalNamespaceCase2(
				t,
				&compared,
			)
		},
		)
	}
}

type testSelectExplicitChartVersionCaseInputs struct {
	selectVersions *func(t *testing.
			T, run Rendered, obj manifest.
			BaseManifest) map[manifest.NamedResource]string
	ns *struct {
		namespace string
		source    string
	}
	tc *struct {
		name string

		kind   string
		want   string
		ref    *manifest.OCIRepositoryRef
		absent bool
	}
}

func testSelectExplicitChartVersionCase(t *testing.T, testCaseInput testSelectExplicitChartVersionCaseInputs) {
	t.Helper()

	run := selectedRun(&Result{})
	kind := testCaseInput.tc.kind
	if kind == "" {
		kind = manifest.KindOCIRepository
	}
	hr := &manifest.HelmRelease{
		Name:      "app",
		Namespace: "authored",
		ChartRef: &helmv2.CrossNamespaceSourceReference{
			Kind:      kind,
			Namespace: testCaseInput.ns.namespace,
			Name:      "chart",
		},
	}

	hr = hr.WithNamespace("apps").(*manifest.HelmRelease)
	run.store.AddObject(hr)
	if !testCaseInput.tc.absent {
		switch kind {
		case manifest.KindHelmChart:
			source := &manifest.HelmChartSource{Name: "chart", Namespace: testCaseInput.ns.source}
			source.Version = testCaseInput.tc.want
			run.store.AddObject(source)
		case manifest.KindExternalArtifact:
			run.store.AddObject(&manifest.ExternalArtifact{Name: "chart", Namespace: testCaseInput.ns.source})
		default:
			source := &manifest.OCIRepository{Name: "chart", Namespace: testCaseInput.ns.source}
			source.Reference = (*sourcev1.OCIRepositoryRef)(testCaseInput.tc.ref)
			run.store.AddObject(source)
		}
	}
	versions := (*testCaseInput.selectVersions)(t, run, hr)
	if hr.Chart.Version != "" {
		t.Fatalf("selection mutated the declared chart projection: %q", hr.Chart.Version)
	}
	version, ok := versions[hr.Named()]
	assert.Equal(t, version, testCaseInput.tc.want)
	assert.Equal(t, ok, testCaseInput.tc.want != "")
}

type testSelectExplicitChartVersionCase2Inputs struct {
	selectVersions *func(t *testing.
			T, run Rendered, obj manifest.
			BaseManifest) map[manifest.NamedResource]string
	obj *manifest.BaseManifest
}

func testSelectExplicitChartVersionCase2(t *testing.T, testCaseInput testSelectExplicitChartVersionCase2Inputs) {
	t.Helper()

	run := selectedRun(&Result{})
	run.store.AddObject((*testCaseInput.obj))
	{
		versions := (*testCaseInput.selectVersions)(t, run, (*testCaseInput.obj))
		assert.FatalIf(t, versions != nil, "non-release chart versions: %+v", versions)
	}
}

type testSelectDiagnosticOnlyRunsCaseInputs struct {
	failure  *error
	teardown *error
	warning  *manifest.Warning
	coverage *manifest.Warning
	scenario *string
}

func testSelectDiagnosticOnlyRunsCase(t *testing.T, testCaseInput testSelectDiagnosticOnlyRunsCaseInputs) {
	t.Helper()

	failed := Rendered{
		Result: &Result{
			Warnings:  []manifest.Warning{(*testCaseInput.warning), (*testCaseInput.coverage)},
			Manifests: []manifest.DocumentBatch{{Documents: []map[string]any{{"kind": "ConfigMap"}}}},
		},
		Err: (*testCaseInput.failure),
	}
	complete := selectedRun(
		&Result{
			Warnings: []manifest.Warning{
				{Category: manifest.WarnSkippedSubmodule, Message: "complete", Count: 1},
			},
		},
	)
	complete.store.AddObject(&manifest.Kustomization{Name: "completed", Namespace: "ns"})
	runs := []Rendered{failed}
	runErr := (*testCaseInput.teardown)
	if (*testCaseInput.scenario) == "orig" {
		runs = []Rendered{failed, complete}
		runErr = errors.Join(errors.Join((*testCaseInput.failure), nil), (*testCaseInput.teardown))
	}
	if (*testCaseInput.scenario) == "current" {
		runs = []Rendered{complete, failed}
		runErr = errors.Join(errors.Join(nil, (*testCaseInput.failure)), (*testCaseInput.teardown))
	}
	selected, err, diagnostic := Select(
		runs,
		runErr,
		[]manifest.NamedResource{testutil.KustomizationID("ns", "named")},
		nil,
	)
	assert.FatalIf(
		t,
		!errors.Is(err, (*testCaseInput.failure)) || !errors.Is(err, (*testCaseInput.teardown)) || !errors.Is(diagnostic, (*testCaseInput.failure)) ||
			!errors.Is(diagnostic, (*testCaseInput.teardown)) ||
			len(selected) != len(runs),
		"lost failed inputs: %+v, %v, %v",
		selected,
		err,
		diagnostic,
	)
	for i, side := range selected {
		want := []manifest.Warning{(*testCaseInput.warning)}
		if runs[i].Result == complete.Result {
			want = complete.Result.Warnings
		}
		assert.Diff(t, side.Warnings, want)
		assert.FatalIf(t, len(
			side.Objects,
		)+len(
			side.Counts,
		)+len(
			side.Statuses,
		)+len(
			side.Manifests,
		)+len(
			side.Failed,
		)+len(
			side.Unavailable,
		) != 0, "partial inventory escaped: %+v", side)
	}
}

type testSelectComparedRootFailureOutsideLocalNamespaceCaseInputs struct {
	reverse  *bool
	required *bool
}

func testSelectComparedRootFailureOutsideLocalNamespaceCase(
	t *testing.T,
	testCaseInput testSelectComparedRootFailureOutsideLocalNamespaceCaseInputs,
) {
	t.Helper()

	parent := &manifest.Kustomization{Name: "parent", Namespace: "monitoring"}
	child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
	failed, other := selectedRun(&Result{}), selectedRun(&Result{})
	failed.filter, other.filter = change.NewFilter(), change.NewFilter()
	failed.store.AddObject(parent)
	failed.Result.Failed = map[manifest.NamedResource]store.StatusInfo{
		parent.Named(): {Status: store.StatusFailed, Message: "external producer failed"},
	}
	failed.Err = &FailuresError{Message: "external producer failed"}
	other.store.AddObject(child)
	owner := parent.Named()
	if !(*testCaseInput.required) {
		owner.Name = "other"
	}
	setParentsForTest(
		other.Orchestrator,
		map[manifest.NamedResource]manifest.NamedResource{child.Named(): owner},
	)
	_, retry := other.filter.AddEmitted(
		change.MembershipUpdate{
			Needed:   []manifest.NamedResource{child.Named(), owner},
			Selected: []manifest.NamedResource{child.Named()},
			Dirty:    nil,
			Ordinary: []manifest.NamedResource{child.Named(), owner},
		},
		0,
	)
	assert.Equal(t, retry, false)
	runs := []Rendered{failed, other}
	index := 0
	if *testCaseInput.reverse {
		slices.Reverse(runs)
		index = 1
	}
	selected, err, remainder := Select(
		runs,
		failed.Err,
		[]manifest.NamedResource{testutil.HelmReleaseID("apps", "")},
		nil,
	)
	assert.Equal(t, err != nil, (*testCaseInput.required))
	assert.NoError(t, remainder)
	_, retained := selected[index].Failed[parent.Named()]
	assert.Equal(t, retained, (*testCaseInput.required))
	_, cause := selected[index].Causes[parent.Named()]
	assert.Equal(t, cause, (*testCaseInput.required))
}

func testSelectComparedRootFailureOutsideLocalNamespaceCase2(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	runs := make([]Rendered, 0, 2)
	failed := &manifest.HelmRelease{Name: "failed", Namespace: "media"}
	anchor := &manifest.Kustomization{Name: "anchor", Namespace: "apps"}
	other := &manifest.HelmRelease{Name: "other", Namespace: "media"}
	for side := range 2 {
		run := selectedRun(&Result{})
		run.store.AddObjects([]manifest.BaseManifest{failed, anchor, other})
		run.filter = change.NewFilter()
		selected := anchor.Named()
		if side == 1 {
			selected = other.Named()
			if *testCaseInput {
				selected = failed.Named()
			}
		} else {
			run.Result.Failed = map[manifest.NamedResource]store.StatusInfo{
				failed.Named(): {Status: store.StatusFailed, Message: "required chart unavailable"},
			}
			run.Err = &FailuresError{Message: "required chart unavailable"}
		}
		_, retry := run.filter.AddEmitted(
			change.MembershipUpdate{
				Needed:   []manifest.NamedResource{failed.Named(), anchor.Named(), other.Named()},
				Selected: []manifest.NamedResource{selected},
				Dirty:    nil,
				Ordinary: []manifest.NamedResource{failed.Named(), anchor.Named(), other.Named()},
			},
			0,
		)
		assert.Equal(t, retry, false)
		runs = append(runs, run)
	}
	selected, err, _ := Select(
		runs,
		runs[0].Err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
		nil,
	)
	assert.Equal(t, err != nil, (*testCaseInput))
	_, retained := selected[0].Failed[failed.Named()]
	assert.Equal(t, retained, (*testCaseInput))
}

type testSelectExplicitChartVersionCase3Inputs struct {
	selectVersions *func(t *testing.T, run Rendered, obj manifest.BaseManifest) map[manifest.NamedResource]string
	ns             *struct {
		namespace string
		source    string
	}
	tc *struct {
		name   string
		kind   string
		want   string
		ref    *manifest.OCIRepositoryRef
		absent bool
	}
}

func testSelectExplicitChartVersionCase3(t *testing.T, testCaseInput testSelectExplicitChartVersionCase3Inputs) {
	t.Helper()

	testSelectExplicitChartVersionCase(
		t,
		testSelectExplicitChartVersionCaseInputs(testCaseInput),
	)
}

type testSelectExplicitChartVersionCase4Inputs struct {
	selectVersions *func(t *testing.T, run Rendered, obj manifest.BaseManifest) map[manifest.NamedResource]string
	obj            *manifest.BaseManifest
}

func testSelectExplicitChartVersionCase4(t *testing.T, testCaseInput testSelectExplicitChartVersionCase4Inputs) {
	t.Helper()

	testSelectExplicitChartVersionCase2(
		t,
		testSelectExplicitChartVersionCase2Inputs(testCaseInput),
	)
}
