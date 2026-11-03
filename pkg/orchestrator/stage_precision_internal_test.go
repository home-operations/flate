package orchestrator

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_StagePrecision(t *testing.T) {
	for _, field := range []string{"timeout", "interval", "wait", "prune", "path"} {
		t.Run(field, func(t *testing.T) {
			testRenderTreesStagePrecisionCase(t, &field)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_HelmStagePrecision(t *testing.T) {
	for _, shape := range []struct{ name, path string }{
		{"home-ops", "kubernetes/apps/actions-runner-controller"},
		{"Biohazard", "kube/clusters/biohazard/flux/apps"},
		{"angelnu", "clusters/angelnu/apps"},
	} {
		for _, field := range []string{"patch", "annotation", "interval"} {
			t.Run(shape.name+"/"+field, func(t *testing.T) {
				testRenderTreesHelmStagePrecisionCase(
					t,
					testRenderTreesHelmStagePrecisionCaseInputs{shape: &shape, field: &field},
				)
			},
			)
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_EffectiveGlobalVariable(t *testing.T) {
	trees := make([]Tree, 2)
	for side := range trees {
		root := t.TempDir()
		value := "old"
		if side == 1 {
			value = "new"
		}
		testutil.WriteFile(
			t,
			root,
			"flux/vars.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: global, namespace: apps}\ndata: {NFS_MUSIC: "+
				value+
				", OTHER: same}\n",
		)
		for _, reader := range []string{"music", "unrelated"} {
			testutil.WriteFile(
				t,
				root,
				"flux/"+
					reader+
					".yaml",
				substitutionKS(reader, reader, "  postBuild: {substituteFrom: [{kind: ConfigMap, name: global}]}\n"),
			)
			testutil.WriteFile(t, root, reader+"/kustomization.yaml", "resources: [cm.yaml]\n")
			expression := "${OTHER}"
			if reader == "music" {
				expression = "${NFS_MUSIC}"
			}
			testutil.WriteFile(
				t,
				root,
				reader+
					"/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+
					reader+
					", namespace: apps}\ndata: {value: '"+
					expression+
					"'}\n",
			)
		}
		trees[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	want := stagePrecisionFull(t, trees)
	cacheDir := t.TempDir()
	for run := range 2 {
		runs, _, builds := stagePrecisionRun(t, trees, cacheDir, run)
		t.Logf(
			"STAGE_COUNT shape=angelnu field=NFS_MUSIC run=%d effective=%d unrelated=%d",
			run,
			builds["./music"],
			builds["./unrelated"],
		)
		assert.Equal(t, builds["./unrelated"], 0)
		if run == 0 {
			assert.Equal(t, builds["./music"], 2)
		}
		assert.Diff(t, string(stagePrecisionDiff(t, runs)), string(want))
	}
}

type generatedVariableCase struct {
	name, expression, packageBody, operand, fluxPatch string
}

func generatedVariableCases() []generatedVariableCase {
	return []generatedVariableCase{
		{
			name: "JSON patch file", expression: "fixed",
			packageBody: "patches:\n- target: {kind: ConfigMap, name: payload}\n  path: x.txt\n",
			operand:     "- op: replace\n  path: /metadata/annotations/value\n  value: '${CHANGED}'\n",
		},
		{
			name: "escaped Flux JSON patch", expression: "fixed",
			fluxPatch: `  patches:
  - target: {kind: ConfigMap, name: payload}
    patch: '[{"op":"replace","path":"/metadata/annotations/value","value":"\u0024{CHANGED}"}]'
`,
		},
		{
			name: "escaped inline JSON patch", expression: "fixed",
			packageBody: `patches:
- target: {kind: ConfigMap, name: payload}
  patch: '[{"op":"replace","path":"/metadata/annotations/value","value":"\u0024{CHANGED}"}]'
`,
		},
		{
			name: "escaped JSON patch file", expression: "fixed",
			packageBody: "patches:\n- target: {kind: ConfigMap, name: payload}\n  path: x.txt\n",
			operand:     `[{"op":"replace","path":"/metadata/annotations/value","value":"\u0024{CHANGED}"}]`,
		},
		{
			name: "escaped inline strategic patch", expression: "fixed",
			packageBody: `patches:
- patch: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: payload, namespace: apps, annotations: {value: "\u0024{CHANGED}"}}
`,
		},
		{
			name: "escaped strategic patch file", expression: "fixed",
			packageBody: "patches: [{path: x.txt}]\n",
			operand: `apiVersion: v1
kind: ConfigMap
metadata: {name: payload, namespace: apps, annotations: {value: "\u0024{CHANGED}"}}
`,
		},
		{
			name:        "ConfigMap files",
			expression:  "fixed",
			packageBody: "configMapGenerator: [{name: generated, files: [x.txt]}]\n",
			operand:     "${CHANGED}",
		},
		{
			name:        "ConfigMap envs",
			expression:  "fixed",
			packageBody: "configMapGenerator: [{name: generated, envs: [x.txt]}]\n",
			operand:     "VALUE=${CHANGED}\n",
		},
		{
			name:       "replacements",
			expression: "prefix{CHANGED}",
			packageBody: "replacements:\n" +
				"- source: {kind: ConfigMap, name: payload, fieldPath: data.prefix}\n" +
				"  targets:\n" +
				"  - select: {kind: ConfigMap, name: payload}\n" +
				"    fieldPaths: [metadata.annotations.value]\n" +
				"    options: {delimiter: '{', index: 0}\n",
		},
		{
			name:       "vars",
			expression: "$(DOLLAR){CHANGED}",
			packageBody: "vars:\n" +
				"- name: DOLLAR\n" +
				"  objref: {apiVersion: v1, kind: ConfigMap, name: payload}\n" +
				"  fieldref: {fieldpath: data.prefix}\n",
		},
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_EffectiveGeneratedVariable(t *testing.T) {
	for _, tc := range generatedVariableCases() {
		t.Run(tc.name, func(t *testing.T) {
			trees := make([]Tree, 2)
			for side := range trees {
				root := t.TempDir()
				value := []string{"old", "new"}[side]
				testutil.WriteFile(
					t,
					root,
					"flux/settings.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {CHANGED: "+
						value+
						", OTHER: same}\n",
				)
				testutil.WriteFile(
					t,
					root,
					"flux/reader.yaml",
					substitutionKS(
						"reader",
						"app",
						"  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n"+tc.fluxPatch,
					),
				)
				testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [payload.yaml]\n"+tc.packageBody)
				testutil.WriteFile(
					t,
					root,
					"app/payload.yaml",
					"apiVersion: v1\n"+
						"kind: ConfigMap\n"+
						"metadata: {name: payload, namespace: apps, annotations: {value: '"+
						tc.expression+
						"'}}\ndata: {prefix: '$'}\n",
				)
				testutil.WriteFile(t, root, "app/x.txt", tc.operand)
				trees[side] = Tree{
					Path:     filepath.Join(root, "flux"),
					RepoRoot: root,
					ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
				}
			}
			want := stagePrecisionFull(t, trees)
			assert.Equal(t, strings.Contains(string(want), "old"), true)
			assert.Equal(t, strings.Contains(string(want), "new"), true)
			cacheDir := t.TempDir()
			for run := range 2 {
				runs, _, builds := stagePrecisionRun(t, trees, cacheDir, run)
				if run == 0 {
					assert.Equal(t, builds["./app"], 2)
				}
				assert.Diff(t, string(stagePrecisionDiff(t, runs)), string(want))
			}
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_ChangedDeclarationPreparation(t *testing.T) {
	trees := make([]Tree, 2)
	for side := range trees {
		root := t.TempDir()
		path := []string{"before", "after"}[side]
		testutil.WriteFile(t, root, "flux/parent.yaml", substitutionKS("parent", path, ""))
		for _, directory := range []string{"before", "after"} {
			body := "resources: [child.yaml]\n" +
				"patches:\n" +
				"- target: {kind: Kustomization, name: child}\n" +
				"  patch: '[{\"op\":\"add\",\"path\":\"/spec/nameSuffix\",\"value\":\"-" +
				directory +
				"\"}]'\n"
			testutil.WriteFile(t, root, directory+"/kustomization.yaml", body)
			testutil.WriteFile(t, root, directory+"/child.yaml", substitutionKS("child", "child", ""))
		}
		testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"child/cm.yaml",
			"apiVersion: v1\n"+
				"kind: ConfigMap\n"+
				"metadata: {name: payload, namespace: apps}\n"+
				"data: {value: same}\n",
		)
		trees[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	want := stagePrecisionFull(t, trees)
	cacheDir := t.TempDir()
	for run := range 2 {
		runs, _, builds := stagePrecisionRun(t, trees, cacheDir, run)
		if run == 0 {
			assert.Equal(t, builds["./child"], 2)
		}
		assert.Diff(t, string(stagePrecisionDiff(t, runs)), string(want))
	}
}

func stagePrecisionRun(t *testing.T, trees []Tree, cacheDir string, run int) ([]Rendered, int, map[string]int) {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	runs, err := RenderTrees(t.Context(), trees, Config{Concurrency: 2, CacheDir: cacheDir})
	assert.NoError(t, err)
	assert.Equal(t, len(runs), 2)
	for _, result := range runs {
		assert.NoError(t, result.Err)
		assert.Equal(t, len(result.Result.Failed), 0)
	}
	builds := map[string]int{}
	var sides [2]struct {
		builds map[string]int
		helm   int
	}
	for i := range sides {
		sides[i].builds = map[string]int{}
	}
	helm := 0
	stagePrecisionRunSegment(t, &stagePrecisionRunSegmentInputs{trees: &trees, run: &run, logs: &logs, builds: &builds, sides: &sides, helm: &helm})
	return stagePrecisionRunTail(
		t,
		&stagePrecisionRunTailInputs{sides: &sides, run: &run, builds: &builds, helm: &helm, runs: &runs},
	)
}

func stagePrecisionRunTail(t *testing.T, tailCaseInput *stagePrecisionRunTailInputs) ([]Rendered, int, map[string]int) {
	t.Helper()
	for side, work := range *tailCaseInput.sides {
		t.Logf(
			"STAGE_WORK case=%s run=%d side=%d builds=%v helm=%d",
			t.Name(),
			(*tailCaseInput.run),
			side,
			work.builds,
			work.helm,
		)
	}
	if (*tailCaseInput.run) == 1 && sourcecache.RenderPersistenceDisabledReason() == "" {
		assert.Equal(t, len((*tailCaseInput.builds)), 0)
		assert.Equal(t, (*tailCaseInput.helm), 0)
	}
	return (*tailCaseInput.runs), (*tailCaseInput.helm), (*tailCaseInput.builds)
}

func stagePrecisionFull(t *testing.T, trees []Tree) []byte {
	t.Helper()
	full := make([]Rendered, len(trees))
	for side, tree := range trees {
		runs, err := RenderTrees(t.Context(), []Tree{tree}, Config{Concurrency: 2, CacheDir: t.TempDir()})
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	return stagePrecisionDiff(t, full)
}

func stagePrecisionDiff(t *testing.T, runs []Rendered) []byte {
	t.Helper()
	selected, selectedErr, err := Select(
		runs,
		nil,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
		nil,
	)
	assert.NoError(t, err)
	body, warnings, suppressed, err := diff.Compare(
		diff.Snapshot{
			Subjects:     selected[0].Subjects,
			Manifests:    selected[0].Manifests,
			ProducerKeys: selected[0].ProducerKeys,
			Unavailable:  selected[0].Unavailable,
		},
		diff.Snapshot{
			Subjects:     selected[1].Subjects,
			Manifests:    selected[1].Manifests,
			ProducerKeys: selected[1].ProducerKeys,
			Unavailable:  selected[1].Unavailable,
		},
		selectedErr, diff.Options{Format: diff.FormatJSON})
	assert.NoError(t, err)
	assert.Equal(t, len(warnings), 0)
	assert.Equal(t, len(suppressed), 0)
	return body
}

func testRenderTreesStagePrecisionCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	trees := make([]Tree, 2)
	for side := range trees {
		root := t.TempDir()
		path, extra := "parent", ""
		if side == 1 {
			switch *testCaseInput {
			case "timeout":
				extra = "  timeout: 2m\n"
			case "interval":
				extra = "  interval: 2m\n"
			case "wait":
				extra = "  wait: true\n"
			case "prune":
				extra = "  prune: true\n"
			case "path":
				path = "moved"
			}
		}
		declaration := substitutionKS("parent", path, extra)
		if (*testCaseInput) == "interval" && side == 1 {
			declaration = strings.Replace(declaration, "  interval: 1m\n", "", 1)
		}
		testutil.WriteFile(t, root, "flux/parent.yaml", declaration)
		for _, directory := range []string{"parent", "moved"} {
			testutil.WriteFile(t, root, directory+"/kustomization.yaml", "resources: [child.yaml]\n")
			testutil.WriteFile(t, root, directory+"/child.yaml", substitutionKS("child", "child", ""))
		}
		testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"child/cm.yaml",
			"apiVersion: v1\n"+
				"kind: ConfigMap\n"+
				"metadata: {name: payload, namespace: apps}\n"+
				"data: {value: same}\n",
		)
		trees[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	want := stagePrecisionFull(t, trees)
	cacheDir := t.TempDir()
	for run := range 2 {
		runs, _, builds := stagePrecisionRun(t, trees, cacheDir, run)
		assert.Equal(t, builds["./child"], 0)
		assert.Diff(t, string(stagePrecisionDiff(t, runs)), string(want))
	}
}

type testRenderTreesHelmStagePrecisionCaseInputs struct {
	shape *struct {
		name string

		path string
	}
	field *string
}

func testRenderTreesHelmStagePrecisionCase(t *testing.T, testCaseInput testRenderTreesHelmStagePrecisionCaseInputs) {
	t.Helper()

	trees := make([]Tree, 2)
	for side := range trees {
		root := t.TempDir()
		declaration := substitutionKS("parent", testCaseInput.shape.path, "")
		app := "resources: [release.yaml]\n"
		if side == 1 {
			switch *testCaseInput.field {
			case "patch":
				declaration += "  patches:\n" +
					"  - target: {kind: HelmRelease, name: app}\n" +
					"    patch: '[{\"op\":\"add\",\"path\":\"/metadata/annotations\",\"value\":{\"precision\":\"ch" +
					"anged\"}}]'\n"
			case "annotation":
				app += "commonAnnotations: {precision: changed}\n"
			case "interval":
				declaration = strings.Replace(declaration, "interval: 1m", "interval: 2m", 1)
			}
		}
		testutil.WriteFile(t, root, "flux/parent.yaml", declaration)
		testutil.WriteFile(t, root, testCaseInput.shape.path+"/kustomization.yaml", app)
		testutil.WriteFile(
			t,
			root, testCaseInput.shape.path+
				"/release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\n"+
				"kind: HelmRelease\n"+
				"metadata: {name: app, namespace: apps}\n"+
				"spec:\n"+
				" interval: 1m\n"+
				" chart: {spec: {chart: ./chart, sourceRef: {kind: GitRepository, name: flux-syst"+
				"em, namespace: flux-system}}}\n"+
				" values: {value: same}\n",
		)
		testutil.WriteFile(t, root, "chart/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(
			t,
			root,
			"chart/templates/cm.yaml",
			"apiVersion: v1\n"+
				"kind: ConfigMap\n"+
				"metadata: {name: payload, namespace: apps}\n"+
				"data: {value: '{{ .Values.value }}'}\n",
		)
		trees[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	want := stagePrecisionFull(t, trees)
	cacheDir := t.TempDir()
	for run := range 2 {
		runs, helm, builds := stagePrecisionRun(t, trees, cacheDir, run)
		t.Logf(
			"STAGE_COUNT shape=%s field=%s run=%d parent=%d helm=%d",
			testCaseInput.shape.name,
			(*testCaseInput.field),
			run,
			builds["./"+testCaseInput.shape.path],
			helm,
		)
		assert.Equal(t, helm, 0)
		assert.Diff(t, string(stagePrecisionDiff(t, runs)), string(want))
	}
}

type stagePrecisionRunTailInputs struct {
	sides *[2]struct {
		builds map[string]int
		helm   int
	}
	run    *int
	builds *map[string]int
	helm   *int
	runs   *[]Rendered
}

type stagePrecisionRunSegmentInputs struct {
	trees  *[]Tree
	run    *int
	logs   *bytes.Buffer
	builds *map[string]int
	sides  *[2]struct {
		builds map[string]int
		helm   int
	}
	helm *int
}

func stagePrecisionRunSegment(t *testing.T, statementInput *stagePrecisionRunSegmentInputs) {
	t.Helper()
	for line := range bytes.Lines((*statementInput.logs).Bytes()) {
		var event struct {
			Msg    string `json:"Msg"`
			Path   string `json:"Path"`
			Source string `json:"Source"`
		}
		assert.NoError(t, json.Unmarshal(line, &event))
		switch event.Msg {
		case "kustomize: build":
			(*statementInput.builds)[event.Path]++
		case "helm: render":
			(*statementInput.helm)++
		}
		if event.Msg == "kustomize: build" || event.Msg == "helm: render" {
			side := -1
			for i, tree := range *statementInput.trees {
				if strings.HasPrefix(event.Source, tree.RepoRoot) {
					side = i
				}
			}
			if side >= 0 {
				if event.Msg == "kustomize: build" {
					(*statementInput.sides)[side].builds[event.Path]++
				}
				if event.Msg == "helm: render" {
					(*statementInput.sides)[side].helm++
				}
			}
			t.Logf("STAGE_SIDE case=%s run=%d side=%d body=%s path=%s", t.Name(), (*statementInput.run), side, event.Msg, event.Path)
		}
	}
}
