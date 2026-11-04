package cli

import (
	"cmp"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
)

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
	cli := runCLI(t, args...)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
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
	cli := runCLI(
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
	out, stderr, code := cli.stdout, cli.stderr, cli.code
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
