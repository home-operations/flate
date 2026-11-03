package cli

import (
	"cmp"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func TestFailureModel(t *testing.T) {
	t.Parallel()
	id := func(name string) manifest.NamedResource {
		return testutil.KustomizationID("flux-system", name)
	}
	failed := func(ids ...manifest.NamedResource) map[manifest.NamedResource]store.StatusInfo {
		m := make(map[manifest.NamedResource]store.StatusInfo, len(ids))
		for _, id := range ids {
			m[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " boom"}
		}
		return m
	}
	root, mid, leaf := id("root"), id("mid"), id("leaf")
	missing, dependent, dual, peer := id("missing"), id("dependent"), id("dual"), id("peer")
	for _, tc := range []struct {
		name   string
		result selection.Selection
		want   output.Model
		raw    *orchestrator.Result
	}{
		{
			name: "GroupsCascadeUnderRoots",
			result: selection.Selection{
				Failed:  failed(root, mid, leaf, dependent),
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{mid: {root}, leaf: {mid}, dependent: {missing}},
				Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {leaf, mid}, missing: {dependent}},
			},
			want: output.Model{
				Primary: []output.Primary{{ID: root, Msg: "root boom", Blocks: []manifest.NamedResource{leaf, mid}}},
				Missing: []output.Missing{{ID: missing, RequiredBy: []manifest.NamedResource{dependent}}},
				Blocked: 3,
			},
		},
		{
			name:   "PrimaryOnly",
			result: selection.Selection{Failed: failed(root), Causes: map[manifest.NamedResource][]manifest.NamedResource{root: nil}},
			want:   output.Model{Primary: []output.Primary{{ID: root, Msg: "root boom"}}},
		},
		{name: "Empty"},
		{
			name: "BlockedCountsDistinctResources",
			result: selection.Selection{
				Failed:  failed(root, dual),
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{dual: {root, missing}},
				Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {dual}, missing: {dual}},
			},
			want: output.Model{
				Primary: []output.Primary{{ID: root, Msg: "root boom", Blocks: []manifest.NamedResource{dual}}},
				Missing: []output.Missing{{ID: missing, RequiredBy: []manifest.NamedResource{dual}}},
				Blocked: 1,
			},
		},
		{
			name: "PreparedCycleRoot",
			raw: &orchestrator.Result{
				Failed: map[manifest.NamedResource]store.StatusInfo{
					root: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + peer.String()},
					peer: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + root.String()},
					leaf: {Status: store.StatusFailed, Message: "dependencies failed"},
				},
				Blocked: map[manifest.NamedResource][]manifest.NamedResource{root: {peer}, peer: {root}, leaf: {root}},
			},
			want: output.Model{
				Primary: []output.Primary{
					{ID: peer, Msg: "dependency cycle: blocked by " + root.String()},
					{ID: root, Msg: "dependency cycle: blocked by " + peer.String(), Blocks: []manifest.NamedResource{leaf}},
				},
				Blocked: 1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := &tc.result
			if tc.raw != nil {
				failed := maps.Clone(tc.raw.Failed)
				blocked := maps.Clone(tc.raw.Blocked)
				for id, deps := range blocked {
					blocked[id] = slices.Clone(deps)
				}
				o, err := completedOrchestrator(t)
				assert.NoError(t, err)
				selected, err, _ := orchestrator.Select(
					[]orchestrator.Rendered{{Orchestrator: o, Result: tc.raw}},
					&orchestrator.FailuresError{
						Message: "raw failures",
					},
					[]manifest.NamedResource{testutil.KustomizationID("", "")},
					nil,
				)
				assert.FatalIf(t, len(selected) != 1 || err == nil ||
					!strings.Contains(
						err.Error(),
						fmt.Sprintf("(+%d blocked", tc.want.Blocked),
					), "selected error and report counts disagree: %v", err)
				result = selected[0]
				assert.Equal(t, maps.Equal(tc.raw.Failed, failed), true)
				assert.Diff(t, tc.raw.Blocked, blocked)
			}
			got := failureModel(result)
			assert.Diff(t, got, tc.want)
			assert.Equal(t, len(got.Primary)+len(got.Missing) == 0, len(tc.want.Primary)+len(tc.want.Missing) == 0)
		})
	}
}

func runNamedScopeFailedDataOwnerName(t *testing.T, tc *struct {
	name     string
	kind     string
	extra    string
	optional bool
	broken   bool
	absent   bool
	chain    bool
}, selectedYAML *string,
) {
	t.Helper()

	root := writeNamedScopeFixture(t)
	appendScopeSpec(
		t,
		root,
		"cluster/hr.yaml",
		fmt.Sprintf("  valuesFrom: [{kind: %s, name: values, optional: %t}]\n", tc.kind, tc.optional),
	)
	testutil.WriteFile(
		t,
		root,
		"cluster/producer.yaml",
		testutil.KustomizationYAML(
			"producer",
			"flux-system",
			"  interval: 10m\n  path: ./values\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	if tc.chain {
		appendScopeSpec(t, root, "cluster/producer.yaml", "  dependsOn: [{name: producer-chain}]\n")
		testutil.WriteFile(
			t,
			root,
			"cluster/producer-chain.yaml",
			testutil.KustomizationYAML(
				"producer-chain",
				"flux-system",
				"  path: ./values\n  dependsOn: [{name: missing-producer-root, namespace: other}]\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
	}
	inputFile := "values/input.yaml"
	resources := "resources: [input.yaml"
	if tc.broken {
		resources += ", missing.yaml"
	}
	testutil.WriteFile(t, root, "values/kustomization.yaml", resources+"]\n")
	input := "apiVersion: v1\nkind: " + tc.kind + "\nmetadata: {name: values, namespace: apps}\ndata:\n  values.yaml: 'greeting: hello'\n"
	if tc.absent {
		input = "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: values-producer, namespace: apps}\nspec:\n  target: {name: values}\n  dataFrom: [{extract: {key: values}}]\n"
	}
	testutil.WriteFile(t, root, inputFile, input)
	var allowed []string
	if tc.extra != "" {
		allowed = []string{manifest.KindSecret, manifest.KindConfigMap}
	}
	cfg := orchestrator.Config{CacheDir: t.TempDir(), Concurrency: 2, AllowMissingKinds: allowed}
	runs, err := orchestrator.RenderTrees(
		t.Context(),
		[]orchestrator.Tree{
			{
				ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path:     filepath.Join(root, "cluster"),
				RepoRoot: root,
			},
		},
		cfg,
	)
	assert.FatalIf(t, len(runs) != 1, "missing run: %v", err)
	run := runs[0]
	res, runErr := run.Result, run.Err
	assert.FatalIf(t, run.Orchestrator == nil || res == nil, "%v", "full-mode fixture did not complete")
	hr := testutil.HelmReleaseID("apps", "selected")
	producer := testutil.KustomizationID("flux-system", "producer")
	wantFailure := tc.broken && !tc.optional
	requiredFailure := tc.broken && !tc.optional && !tc.absent
	_, readerFailed := res.Failed[hr]
	assert.FatalIf(
		t,
		(len(outputByParent(res.Manifests)[hr]) == 0) != requiredFailure || readerFailed != requiredFailure ||
			(len(res.Blocked[hr]) != 0) != requiredFailure,
		"consumer did not render independently: failed=%v blocked=%v",
		res.Failed,
		res.Blocked,
	)
	{
		_, failed := res.Failed[producer]
		assert.FatalIf(
			t,
			failed != tc.broken,
			"producer failure=%t, want %t: %v",
			failed,
			tc.broken,
			res.Failed,
		)
	}
	args := []string{
		"build",
		"hr",
		"selected",
		"--path",
		filepath.Join(root, "cluster"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	}
	if tc.extra != "" {
		args = append(args, tc.extra)
	}
	stdout, stderr, code := runCLI(t, args...)
	selection, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{run}, runErr,
		[]manifest.NamedResource{{Group: hr.Group, Kind: hr.Kind, Name: hr.Name}}, nil)
	assert.FatalIf(t, len(selection) != 1, "selection failed: %v", selectedErr)
	if _, failed := selection[0].Failed[producer]; failed != wantFailure ||
		(selectedErr != nil) != wantFailure {
		t.Fatalf(
			"required owner failure projection violated: failed=%v unrelated=%v error=%v",
			selection[0].Failed,
			selection[0].Unrelated,
			selectedErr,
		)
	}
	for warningIndex := range selection[0].Unrelated {
		warning := selection[0].Unrelated[warningIndex]
		assert.FatalIf(
			t,
			wantFailure && warning.Resource == producer,
			"required owner became unrelated: %+v",
			warning,
		)
	}

	assert.FatalIf(
		t,
		(code == 1) != wantFailure || strings.Contains(stdout, "selected-rendered") == requiredFailure,
		"authored owner policy violated: %d %s\n%s",
		code,
		stdout,
		stderr,
	)
	if !requiredFailure {
		(*selectedYAML) = cmp.Or((*selectedYAML), stdout)
		assert.FatalIf(
			t,
			stdout != (*selectedYAML),
			"producer health changed selected YAML: want %s, got %s",
			(*selectedYAML),
			stdout,
		)
	}
	assert.FatalIf(
		t,
		requiredFailure && !strings.Contains(stderr, "values"),
		"required input failure missing: %s",
		stderr,
	)
	assert.FatalIf(t, tc.chain &&
		(!strings.Contains(stderr, "producer-chain") || !strings.Contains(stderr, "missing-producer-root")), "producer's transitive root missing: %s", stderr)
	assert.FatalIf(
		t,
		tc.broken && !strings.Contains(stderr, "producer"),
		"producer diagnostic missing: %s",
		stderr,
	)
}

func runNamedScopeSubstituteOwnersName(t *testing.T, tc *struct {
	name     string
	secret   bool
	self     bool
	broken   bool
	absent   bool
	required bool
},
) {
	t.Helper()

	root := writeNamedScopeFixture(t)
	kind := "ConfigMap"
	if tc.secret {
		kind = "Secret"
	}
	appendScopeSpec(
		t,
		root,
		"cluster/ks.yaml",
		fmt.Sprintf(
			"  postBuild:\n    substituteFrom: [{kind: %s, name: scope-input, optional: %t}]\n",
			kind,
			!tc.self && !tc.required,
		),
	)
	if !tc.absent {
		testutil.WriteFile(
			t,
			root,
			"cluster/producer.yaml",
			testutil.KustomizationYAML(
				"producer",
				"flux-system",
				"  path: ./values\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		resources := "resources: [input.yaml"
		if tc.broken {
			resources += ", missing.yaml"
		}
		testutil.WriteFile(t, root, "values/kustomization.yaml", resources+"]\n")
		input := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: scope-input, namespace: flux-system}\ndata: {SCOPE: ready}\n"
		if tc.secret {
			input = "apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: scope-input, namespace: flux-system}\nspec:\n  dataFrom: [{extract: {key: scope-input}}]\n"
		}
		testutil.WriteFile(t, root, "values/input.yaml", input)
		if tc.self {
			testutil.WriteFile(t, root, "selected/kustomization.yaml", "resources: [cm.yaml, input.yaml]\n")
			testutil.WriteFile(t, root, "selected/input.yaml", input)
		}
	}
	flags := commonFlags{path: filepath.Join(root, "cluster"), cacheDir: t.TempDir(), concurrency: 2}
	runs, runErr := orchestrator.RenderTrees(t.Context(), inputTrees(&flags), buildOrchCfg(&flags))
	assert.FatalIf(t, len(runs) != 1, "missing run: %v", runErr)
	run := runs[0]
	res := run.Result
	id := testutil.KustomizationID("flux-system", "selected")
	requiredFailure := tc.broken && tc.required
	assert.FatalIf(t, res == nil || (len(outputByParent(res.Manifests)[id]) == 0) != requiredFailure ||
		(len(res.Blocked[id]) != 0) != requiredFailure, "consumer did not render independently: %+v", res)
	out, stderr, code := runCLI(
		t,
		"build",
		"ks",
		"selected",
		"--path",
		flags.path,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	assert.FatalIf(
		t,
		(code == 1) != requiredFailure || strings.Contains(out, "name: selected") == requiredFailure,
		"substitution owner policy: code=%d output=%s diagnostics=%s",
		code,
		out,
		stderr,
	)
	assert.FatalIf(
		t,
		tc.broken && !strings.Contains(stderr, "producer"),
		"producer failure missing: %s",
		stderr,
	)
}

func runNamedScopeDiffOwningKustomizationFailsName(t *testing.T, tc *struct {
	name        string
	failingSide string
	layout      string
},
) {
	t.Helper()

	root, orig := t.TempDir(), t.TempDir()
	releasePath := "apps/hr.yaml"
	resources := "resources: [hr.yaml]\n"
	if tc.layout == "escape" {
		releasePath = "shared/hr.yaml"
		resources = "resources: [../shared/hr.yaml]\n"
	}
	for _, dir := range []string{root, orig} {
		testutil.GitInit(t, dir)
		testutil.WriteFile(t, dir, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
		testutil.WriteFile(
			t,
			dir,
			"charts/app/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: selected-rendered, namespace: apps}\ndata: {greeting: hello}\n",
		)
		testutil.WriteFile(t, dir, "apps/kustomization.yaml", resources)
		testutil.WriteFile(t, dir, releasePath, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: ./charts/app
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
		for _, name := range []string{"apps", "other"} {
			testutil.WriteFile(
				t,
				dir,
				"flux/"+name+".yaml",
				testutil.KustomizationYAML(
					name,
					"flux-system",
					"  interval: 10m\n  path: ./"+name+"\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			)
		}
		testutil.WriteFile(t, dir, "other/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			dir,
			"other/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: other, namespace: apps}\n",
		)
	}
	if tc.layout == "plain" {
		appendScopeSpec(t, root, releasePath, "  values: {greeting: changed}\n")
	}
	if tc.layout == "moved" {
		assert.NoError(t, os.Rename(
			filepath.Join(root, releasePath),
			filepath.Join(root, "other/hr.yaml"),
		))
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: apps, namespace: apps}\n",
		)
		testutil.WriteFile(t, root, "other/kustomization.yaml", "resources: [cm.yaml, hr.yaml]\n")
	}
	failing := root
	if tc.failingSide == "orig" {
		failing = orig
	}
	appendScopeSpec(
		t,
		failing,
		"flux/apps.yaml",
		"  postBuild: {substituteFrom: [{kind: ConfigMap, name: nope}]}\n",
	)
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--path-orig",
		filepath.Join(orig, "flux"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	}
	_, stderr, code := runCLI(t, append([]string{"diff", "hr", "selected"}, flags...)...)
	switch {
	case tc.layout == "moved" && tc.failingSide == "current":

		assert.FatalIf(t, code != 0 || strings.Contains(stderr, "flux-system/nope") ||
			strings.Contains(
				stderr,
				"reconcile completed",
			), "former owning Kustomization became fatal: %d %s", code, stderr)
		_, stderr, code = runCLI(
			t,
			"build",
			"hr",
			"selected",
			"--path",
			filepath.Join(root, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		assert.FatalIf(t, code != 0, "former owning Kustomization became fatal in build: %d %s", code, stderr)

	case code != 1 || !strings.Contains(stderr, "✗ 1 failed") || !strings.Contains(stderr, "ConfigMap  flux-system/nope  not found"):

		t.Fatalf("owning Kustomization failure hidden: %d %s", code, stderr)
	}
	_, stderr, code = runCLI(t, append([]string{"diff", "ks", "other"}, flags...)...)
	assert.FatalIf(t, code != 0, "unrelated owning Kustomization became fatal: %d %s", code, stderr)
	if tc.layout == "plain" {
		baseline := orig
		if failing == orig {
			baseline = root
		}
		_, stderr, code = runCLI(
			t,
			"build",
			"hr",
			"selected",
			"--path",
			filepath.Join(failing, "flux"),
			"--path-orig",
			filepath.Join(baseline, "flux"),
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		assert.FatalIf(
			t,
			code != 1 || !strings.Contains(stderr, "1 failed") || !strings.Contains(stderr, "flux-system/nope"),
			"changed-only owning Kustomization failure hidden: %d %s",
			code,
			stderr,
		)
	}
}
