package orchestrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_UnchangedDeclarationMove(t *testing.T) {
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	registry := testutil.NewChartRegistry(t, []map[string]string{inlineChartFiles(t)}, false)
	host := strings.TrimSuffix(strings.TrimPrefix(registry.URL, "oci://"), "/demo")
	testutil.WriteFileAt(t, registryConfig, fmt.Sprintf(`{"auths":{%q:{"auth":"Zml4dHVyZTpwYXNzd29yZA=="}}}`, host))
	trees := make([]Tree, 2)
	for side := range trees {
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"flux/owners.yaml",
			substitutionKS(
				"old-owner",
				"old",
				"",
			)+"---\n"+substitutionKS(
				"new-owner",
				"new",
				"",
			)+"---\n"+substitutionKS(
				"reader",
				"reader",
				"",
			),
		)
		for i, owner := range []string{"old", "new"} {
			resources := "resources: []\n"
			if i == side {
				resources = "resources: [source.yaml]\n"
				testutil.WriteFile(
					t,
					root,
					owner+"/source.yaml",
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {interval: 1h, url: '"+registry.URL+"', insecure: true, ref: {tag: 0.1.0}, layerSelector: {mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip, operation: copy}}\n",
				)
			}
			testutil.WriteFile(t, root, owner+"/kustomization.yaml", resources)
		}
		testutil.WriteFile(t, root, "reader/kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"reader/release.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}}\n",
		)
		trees[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	delta := func(runs []Rendered) []byte {
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
			selectedErr,
			diff.Options{Format: diff.FormatJSON},
		)
		assert.NoError(t, err)
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, len(suppressed), 0)
		return body
	}
	full := make([]Rendered, 2)
	for side, tree := range trees {
		runs, err := RenderTrees(
			t.Context(),
			[]Tree{tree},
			Config{Concurrency: 2, CacheDir: t.TempDir(), RegistryConfig: registryConfig},
		)
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	want := delta(full)
	for run := range 2 {
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		runs, err := RenderTrees(
			t.Context(),
			trees,
			Config{Concurrency: 2, CacheDir: t.TempDir(), RegistryConfig: registryConfig},
		)
		slog.SetDefault(previous)
		assert.NoError(t, err)
		builds := map[string]int{}
		renders := 0
		for line := range bytes.Lines(logs.Bytes()) {
			var event struct {
				Msg  string `json:"Msg"`
				Path string `json:"Path"`
			}
			assert.NoError(t, json.Unmarshal(line, &event))
			if event.Msg == "kustomize: build" {
				builds[event.Path]++
			}
			if event.Msg == "helm: render" {
				renders++
			}
		}
		t.Logf("run=%d builds=%v Helm renders=%d", run, builds, renders)
		assert.Equal(t, fmt.Sprint(builds), "map[./new:2 ./old:2]")
		assert.Equal(t, renders, 0)
		assert.Diff(t, string(delta(runs)), string(want))
	}
}

func TestRenderTrees_DeclarationMoveChanges(t *testing.T) {
	t.Parallel()

	for _, scenario := range []string{"changed", "added", "removed", "build_input"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testRenderTreesDeclarationMoveChangesCase(
				t,
				&scenario,
			)
		},
		)
	}
}

func testRenderTreesDeclarationMoveChangesCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	files := inlineChartFiles(t)
	files["demo/templates/configmap.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-output}\ndata: {payload: '{{ .Values.payload | default \"default\" }}'}\n"
	registry := testutil.NewChartRegistry(t, []map[string]string{files}, false)
	host := strings.TrimSuffix(strings.TrimPrefix(registry.URL, "oci://"), "/demo")
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(
		t,
		registryConfig,
		fmt.Sprintf(`{"auths":{%q:{"auth":"Zml4dHVyZTpwYXNzd29yZA=="}}}`, host),
	)
	trees := make([]Tree, 2)
	for side := range trees {
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"flux/owners.yaml",
			substitutionKS(
				"old-owner",
				"old",
				"",
			)+"---\n"+substitutionKS(
				"new-owner",
				"new",
				"",
			)+"---\n"+substitutionKS(
				"reader",
				"reader",
				"",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"flux/source.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {interval: 1h, url: '"+registry.URL+"', insecure: true, ref: {tag: 0.1.0}, layerSelector: {mediaType: application/vnd.cncf.helm.chart.content.v1.tar+gzip, operation: copy}}\n",
		)
		for i, owner := range []string{"old", "new"} {
			resources := "resources: []\n"
			if i == side && ((*testCaseInput) != "added" || side != 0) &&
				((*testCaseInput) != "removed" || side != 1) {
				resources = "resources: [values.yaml]\n"
				payload := "same"
				if (*testCaseInput) == "changed" {
					payload = fmt.Sprintf("side-%d", side)
				}
				testutil.WriteFile(
					t,
					root,
					owner+"/values.yaml",
					fmt.Sprintf(
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata: {values.yaml: 'payload: %s'}\n",
						payload,
					),
				)
				if (*testCaseInput) == "build_input" {
					resources += fmt.Sprintf(
						"patches:\n- target: {kind: ConfigMap, name: values}\n  patch: '[{\"op\":\"replace\",\"path\":\"/data/values.yaml\",\"value\":\"payload: side-%d\"}]'\n",
						side,
					)
				}
			}
			testutil.WriteFile(t, root, owner+"/kustomization.yaml", resources)
		}
		testutil.WriteFile(t, root, "reader/kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"reader/release.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}, valuesFrom: [{kind: ConfigMap, name: values, optional: true}]}\n",
		)
		trees[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	delta := func(runs []Rendered) []byte {
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
			selectedErr,
			diff.Options{Format: diff.FormatJSON},
		)
		assert.NoError(t, err)
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, len(suppressed), 0)
		return body
	}
	full := make([]Rendered, 2)
	for side, tree := range trees {
		runs, err := RenderTrees(
			t.Context(),
			[]Tree{tree},
			Config{Concurrency: 2, CacheDir: t.TempDir(), RegistryConfig: registryConfig},
		)
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	want := delta(full)
	for range 2 {
		runs, err := RenderTrees(
			t.Context(),
			trees,
			Config{Concurrency: 2, CacheDir: t.TempDir(), RegistryConfig: registryConfig},
		)
		assert.NoError(t, err)
		assert.Diff(t, string(delta(runs)), string(want))
	}
}
