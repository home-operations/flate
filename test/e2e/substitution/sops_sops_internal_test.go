package substitution_test

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-global logging.
func TestE2E_SOPSUnreadableEmptyVariableSet(t *testing.T) {
	const cipher = "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"
	for _, tc := range []struct {
		name, strategy, inline, readable, annotations, want              string
		supply, strict, fail, noPostBuild, skipSecret, mixed, skipConfig bool
	}{
		{name: "default inactive", want: "${VALUE}"},
		{name: "default skipped", skipSecret: true, want: "${VALUE}"},
		{name: "active inline", inline: "    substitute: {READABLE: inline}\n"},
		{name: "active readable", readable: "  READABLE: repository\n"},
		{name: "always empty", strategy: "    substituteStrategy: Always\n"},
		{name: "strict active", inline: "    substitute: {READABLE: inline}\n", strict: true, fail: true},
		{name: "resource disabled", inline: "    substitute: {READABLE: inline}\n", annotations: "  annotations: {kustomize.toolkit.fluxcd.io/substitute: disabled}\n", want: "${VALUE}"},
		{name: "dummy supply", supply: true, want: "dummy"},
		{name: "mixed emitted", mixed: true, supply: true, readable: "  READABLE: repository\n  EMPTY: ''\n", want: "dummy"},
		{name: "mixed skipped", mixed: true, supply: true, readable: "  READABLE: repository\n  EMPTY: ''\n", skipSecret: true, skipConfig: true, want: "dummy"},
		{name: "no postBuild", noPostBuild: true, want: "${VALUE}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testE2ESOPSUnreadableEmptyVariableSetCase1(t, &tc, cipher)
		})
	}
}

func testE2ESOPSUnreadableEmptyVariableSetCase1(t *testing.T, tc *struct {
	name        string
	strategy    string
	inline      string
	readable    string
	annotations string
	want        string
	supply      bool
	strict      bool
	fail        bool
	noPostBuild bool
	skipSecret  bool
	mixed       bool
	skipConfig  bool
}, cipher string,
) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml]\n")
	postBuild := "  postBuild:\n    substituteFrom: [{kind: Secret, name: input}]\n" + tc.inline + tc.strategy
	if tc.mixed {
		postBuild = strings.ReplaceAll(
			postBuild,
			"name: input}]",
			"name: input}, {kind: ConfigMap, name: settings}]",
		)
	}
	if tc.noPostBuild {
		postBuild = ""
	}
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n"+postBuild),
	)
	resources := "resources: [secret.yaml, cm.yaml]\n"
	if tc.mixed {
		resources = "resources: [secret.yaml, cm.yaml, settings.yaml]\n"
		testutil.WriteFile(
			t,
			root,
			"app/settings.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: settings\n  namespace: apps\n  labels: {retained: repository}\n  annotations: {example.test/note: repository}\ndata: {CONFIG_PLAIN: repository, CONFIG_EMPTY: '', CONFIG_ENCRYPTED: '"+cipher+"'}\nsops: {mac: encrypted}\n",
		)
	}
	testutil.WriteFile(t, root, "app/kustomization.yaml", resources)
	testutil.WriteFile(
		t,
		root,
		"app/secret.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata:\n  name: input\n  namespace: apps\n  labels: {retained: repository}\n  annotations: {example.test/note: repository}\ntype: Opaque\nstringData:\n  VALUE: '"+cipher+"'\n"+tc.readable+"sops: {mac: encrypted}\n",
	)
	extra := ""
	if tc.mixed {
		extra = "\n  supplied: '${CONFIG_ENCRYPTED}'"
	}
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: rendered\n  namespace: apps\n"+tc.annotations+"data:\n  value: '${VALUE}'"+extra+"\n",
	)
	args := []string{
		"build",
		"all",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"--skip-kinds",
		"",
	}
	if tc.skipSecret {
		args[len(args)-1] = "Secret"
	}
	if tc.skipConfig {
		args[len(args)-1] = "Secret,ConfigMap"
	}
	if tc.supply {
		input := t.TempDir()
		testutil.WriteFile(
			t,
			input,
			"dummy.yaml",
			"apiVersion: v1\nkind: Secret\nmetadata: {name: input, namespace: apps}\nstringData: {VALUE: dummy, READABLE: supplied, EMPTY: supplied, SUPPLIED_ONLY: supplied}\n",
		)
		if tc.mixed {
			testutil.WriteFile(
				t,
				input,
				"config.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {CONFIG_PLAIN: supplied, CONFIG_EMPTY: supplied, CONFIG_ENCRYPTED: supplied, CONFIG_ONLY: supplied}\n",
			)
			args = append(args, "--supply", filepath.Join(input, "config.yaml"))
		}
		args = append(args, "--supply", filepath.Join(input, "dummy.yaml"))
	}
	testE2ESOPSUnreadableEmptyVariableSetCase1Tail(
		t,
		&testE2ESOPSUnreadableEmptyVariableSetCase1TailInputs{tc: &tc, args: &args},
	)
}

func testE2ESOPSUnreadableEmptyVariableSetCase1Part1(
	t *testing.T, inputs *testE2ESOPSUnreadableEmptyVariableSetCase1Part1Inputs,
) int {
	t.Helper()
	assert.FatalIf(t, inputs.doc["sops"] != nil,
		"emitted SOPS metadata: %v", inputs.doc)

	if (*inputs.id) == (*inputs.rendered) {
		want := map[string]any{"value": (*inputs.tc).want}
		if (*inputs.tc).want == "" {
			want["value"] = nil
		}
		if (*inputs.tc).mixed {
			want["supplied"] = "supplied"
		}
		assert.Diff(t, inputs.doc["data"], want)
		return 1
	}
	metadata := inputs.doc["metadata"].(map[string]any)
	assert.Equal(t, metadata["labels"].(map[string]any)["retained"], any("repository"))
	assert.Equal(t, metadata["annotations"].(map[string]any)["example.test/note"], any("repository"))
	if (*inputs.id) == (*inputs.settings) {
		assert.Diff(t, inputs.doc["data"], map[string]any{"CONFIG_PLAIN": "repository", "CONFIG_EMPTY": ""})
		return 2
	}
	assert.Equal(t, inputs.doc["type"], any("Opaque"))
	want := map[string]any{}
	if (*inputs.tc).readable != "" {
		want["READABLE"] = "repository"
	}
	if (*inputs.tc).mixed {
		want["EMPTY"] = ""
	}
	got, _ := inputs.doc["stringData"].(map[string]any)
	if len(want) == 0 {
		assert.Equal(t, len(got), 0)
	} else {
		assert.Diff(t, got, want)
	}
	data, _ := inputs.doc["data"].(map[string]any)
	assert.Equal(t, len(data), 0)
	return 0
}

//nolint:paralleltest // CLI execution changes process-global logging.
func TestE2E_SOPSUnreadableDiffBothDirections(t *testing.T) {
	for _, change := range []string{"ciphertext", "plaintext", "metadata"} {
		t.Run(change, func(t *testing.T) {
			testSOPSUnreadableDiffCase(t, &change)
		})
	}
}

func testSOPSUnreadableDiffCase(t *testing.T, change *string) {
	t.Helper()
	roots := []string{t.TempDir(), t.TempDir()}
	for i, root := range roots {
		plain, label := "before", "before"
		if i == 1 && (*change) == "plaintext" {
			plain = "after"
		}
		if i == 1 && (*change) == "metadata" {
			label = "after"
		}
		cipher := "ENC[AES256_GCM,data:before,iv:y,tag:z,type:str]"
		if i == 1 {
			cipher = "ENC[AES256_GCM,data:after,iv:y,tag:z,type:str]"
		}
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [secret.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"secret.yaml",
			"apiVersion: v1\nkind: Secret\nmetadata:\n  name: input\n  namespace: apps\n  labels: {revision: '"+label+"'}\ntype: Opaque\nstringData: {VALUE: '"+cipher+"', PLAIN: '"+plain+"'}\nsops: {mac: encrypted}\n",
		)
	}
	for _, direction := range [][2]int{{0, 1}, {1, 0}} {
		result3 := harness.RunCLIBuffers(
			"diff",
			"all",
			"--path",
			roots[direction[0]],
			"--path-orig",
			roots[direction[1]],
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
			"--skip-kinds",
			"",
		)
		stdout, stderr, code := result3.Stdout, result3.Stderr, result3.Code
		assert.FatalIf(t, code != 0 || strings.Contains(stdout+stderr, "ENC[AES256_GCM") ||
			(strings.TrimSpace(stdout) == "") != ((*change) == "ciphertext"),
			"%s direction=%v exit=%d stdout=%s stderr=%s", (*change), direction, code, stdout, stderr)
	}
}

type testE2ESOPSUnreadableEmptyVariableSetCase1TailInputs struct {
	tc **struct {
		name string

		strategy string

		inline string

		readable string

		annotations string
		want        string
		supply      bool
		strict      bool
		fail        bool
		noPostBuild bool
		skipSecret  bool
		mixed       bool
		skipConfig  bool
	}

	args *[]string
}

func testE2ESOPSUnreadableEmptyVariableSetCase1Tail(
	t *testing.T,
	inputs *testE2ESOPSUnreadableEmptyVariableSetCase1TailInputs,
) {
	t.Helper()

	if (*inputs.tc).strict {
		(*inputs.args) = append((*inputs.args), "--strict-substitutions")
	}
	result1 := harness.RunCLIBuffers((*inputs.args)...)
	stdout, stderr, code := result1.Stdout, result1.Stderr, result1.Code
	assert.FatalIf(t, (code != 0) != (*inputs.tc).fail,
		"exit=%d stdout=%s stderr=%s", code, stdout, stderr)

	if (*inputs.tc).fail {
		return
	}
	result2 := harness.RunCLIBuffers((*inputs.args)...)
	again, diagnostic, nextCode := result2.Stdout, result2.Stderr, result2.Code
	assert.FatalIf(
		t,
		code != nextCode || stdout != again || stderr != diagnostic,
		"%s",
		"replayed output or diagnostics changed",
	)

	wantWarnings := 1
	if (*inputs.tc).mixed {
		wantWarnings = 2
	}
	assert.FatalIf(
		t,
		strings.Contains(stdout+stderr, "ENC[") || strings.Count(stderr, "[SOPSUnreadable]") != wantWarnings,
		"unreadable data or advisory: stdout=%s stderr=%s",
		stdout,
		stderr,
	)

	docs, err := testutil.SplitDocs([]byte(stdout))
	assert.FatalIf(t, err != nil, "%s", err)

	objects := make(map[manifest.NamedResource]map[string]any)
	for _, doc := range docs {
		id := manifest.DocIdentity(doc)
		if id.Kind == manifest.KindSecret || id.Kind == manifest.KindConfigMap {
			assert.FatalIf(t, objects[id] != nil,
				"duplicate emitted identity: %s", id)

			objects[id] = doc
		}
	}
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "input"}
	rendered := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "rendered"}
	settings := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	var wantIDs []manifest.NamedResource
	if !(*inputs.tc).skipSecret {
		wantIDs = append(wantIDs, secret)
	}
	if !(*inputs.tc).skipConfig {
		wantIDs = append(wantIDs, rendered)
		if (*inputs.tc).mixed {
			wantIDs = append(wantIDs, settings)
		}
	}
	assert.Diff(
		t,
		slices.SortedFunc(maps.Keys(objects), manifest.NamedResource.Compare),
		slices.SortedFunc(slices.Values(wantIDs), manifest.NamedResource.Compare),
	)
	for id, doc := range objects {
		ctrl := testE2ESOPSUnreadableEmptyVariableSetCase1Part1(
			t,
			&testE2ESOPSUnreadableEmptyVariableSetCase1Part1Inputs{
				doc:      doc,
				id:       &id,
				rendered: &rendered,
				tc:       inputs.tc,
				settings: &settings,
			},
		)
		switch ctrl {
		case 1:
			continue
		case 2:
			continue
		}
	}
}

type testE2ESOPSUnreadableEmptyVariableSetCase1Part1Inputs struct {
	doc      map[string]any
	id       *manifest.NamedResource
	rendered *manifest.NamedResource
	tc       **struct {
		name        string
		strategy    string
		inline      string
		readable    string
		annotations string
		want        string
		supply      bool
		strict      bool
		fail        bool
		noPostBuild bool
		skipSecret  bool
		mixed       bool
		skipConfig  bool
	}
	settings *manifest.NamedResource
}
