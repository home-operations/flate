package orchestrator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRenderTrees_RequiredOpaqueInput(t *testing.T) {
	for _, scenario := range []string{"tier_2", "tier_1", "tier_1_builder", "generated"} {
		t.Run(scenario, func(t *testing.T) {
			testRenderTreesRequiredOpaqueInputCase(t, &scenario)
		},
		)
	}
}

func testRenderTreesRequiredOpaqueInputCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	tier1 := strings.HasPrefix((*testCaseInput), "tier_1")
	charts := map[string][]byte{}
	var downloads [3]atomic.Int64
	var index strings.Builder
	index.WriteString("apiVersion: v1\nentries:\n")
	for i := range 3 {
		name := fmt.Sprintf("demo-%d", i)
		object := "required-settings"
		if i != 0 {
			object = fmt.Sprintf("unrelated-%d", i)
		}
		charts["/"+name+".tgz"] = testutil.TarGz(t, map[string]string{
			name + "/Chart.yaml":        "apiVersion: v2\nname: " + name + "\nversion: 1.0.0\n",
			name + "/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + object + ", namespace: apps}\ndata: {VALUE: supplied}\n",
		}, 0o644, gzip.DefaultCompression)
		fmt.Fprintf(
			&index,
			"  %s:\n  - apiVersion: v2\n    name: %s\n    version: 1.0.0\n    urls: [%s.tgz]\n",
			name,
			name,
			name,
		)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/index.yaml" {
			_, _ = fmt.Fprint(w, index.String())
			return
		}
		if body, ok := charts[r.URL.Path]; ok {
			for i := range downloads {
				if r.URL.Path == fmt.Sprintf("/demo-%d.tgz", i) {
					downloads[i].Add(1)
				}
			}
			_, _ = w.Write(body)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	trees := make([]Tree, 2)
	testRenderTreesRequiredOpaqueInputCaseSegment(
		t,
		&testRenderTreesRequiredOpaqueInputCaseSegmentInputs{
			testCaseInput: &testCaseInput,
			tier1:         &tier1,
			server:        &server,
			trees:         &trees,
		},
	)

	testRenderTreesRequiredOpaqueInputCaseTail(
		t,
		&testRenderTreesRequiredOpaqueInputCaseTailInputs{
			downloads:     &downloads,
			tier1:         &tier1,
			testCaseInput: &testCaseInput,
			trees:         &trees,
		},
	)
}

func testRenderTreesRequiredOpaqueInputCaseTail(
	t *testing.T,
	tailCaseInput *testRenderTreesRequiredOpaqueInputCaseTailInputs,
) {
	t.Helper()
	var warmCache string
	var persistent bool
	render := func(label string, input []Tree) ([]Rendered, error) {
		t.Helper()
		var before [3]int64
		for i := range before {
			before[i] = tailCaseInput.downloads[i].Load()
		}
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		defer slog.SetDefault(previous)
		cacheDir := t.TempDir()
		cacheDir = opaqueInputCache(label, cacheDir, &warmCache)
		owner, openErr := sourcecache.New(t.Context(), cacheDir)
		assert.NoError(t, openErr)
		defer func() { assert.NoError(t, owner.Close()) }()
		persistent = owner.CanPersistRenders()
		runs, err := RenderTrees(t.Context(), input, Config{Concurrency: 2, CacheDir: cacheDir, Cache: owner})
		counts := opaqueInputWork(t, &logs)
		fetched := opaqueInputDownloadDelta(tailCaseInput.downloads, before)
		t.Logf("%s work=%v chart_downloads=%v error=%v", label, counts, fetched, err)
		if strings.HasPrefix(label, "cold_") {
			tailCaseInput.assertOpaqueInputColdWork(t, counts, fetched)
			if owner.CanPersistRenders() && (*(*tailCaseInput.testCaseInput)) != "generated" {
				assertOpaqueInputInventory(t, runs, owner, label)
			}
		}
		if strings.HasPrefix(label, "warm_") {
			assert.Equal(t, counts["helm: render"], 0)
			assert.Equal(t, fetched, [3]int64{})
		}
		for side, run := range runs {
			t.Logf("%s side=%d error=%v", label, side, run.Err)
		}
		return runs, err
	}
	delta := func(runs []Rendered, runErr error) ([]byte, error) {
		t.Helper()
		selected, selectedErr, err := Select(
			runs,
			runErr,
			[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
			nil,
		)
		if err != nil {
			return nil, err
		}
		body, _, _, err := diff.Compare(
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
		return body, errors.Join(err, runErr)
	}
	full := make([]Rendered, 2)
	for side, tree := range *tailCaseInput.trees {
		runs, err := render(fmt.Sprintf("full_%d", side), []Tree{tree})
		assert.NoError(t, err)
		full[side] = runs[0]
	}
	want, fullErr := delta(full, nil)
	assert.NoError(t, fullErr)
	{
		assert.FatalIf(
			t, (*(*tailCaseInput.testCaseInput)) !=
				"generated" && !bytes.Contains(want, []byte("supplied-side-1")), "independent full delta lacks substituted output: %s",
			want,
		)
	}
	t.Logf("independent full delta: %s", want)
	for run := range 2 {
		runs, err := render(fmt.Sprintf("cold_%d", run), (*tailCaseInput.trees))
		got, selectedErr := delta(runs, err)
		assert.Equal(t, selectedErr == nil, fullErr == nil)
		assert.Equal(t, string(got), string(want))
		if run == 0 && persistent {
			warm, warmErr := render("warm_0", (*tailCaseInput.trees))
			warmDelta, selectedErr := delta(warm, warmErr)
			assert.Equal(t, selectedErr == nil, fullErr == nil)
			assert.Equal(t, string(warmDelta), string(want))
		}
	}
}

type testRenderTreesRequiredOpaqueInputCaseTailInputs struct {
	downloads     *[3]atomic.Int64
	tier1         *bool
	testCaseInput **string
	trees         *[]Tree
}

type testRenderTreesRequiredOpaqueInputCaseSegmentInputs struct {
	testCaseInput **string
	tier1         *bool
	server        **httptest.Server
	trees         *[]Tree
}

func testRenderTreesRequiredOpaqueInputCaseSegment(
	t *testing.T,
	statementInput *testRenderTreesRequiredOpaqueInputCaseSegmentInputs,
) {
	t.Helper()
	for side := range *statementInput.trees {
		root := t.TempDir()
		resources := "resources: [consumer.yaml, repository.yaml, releases.yaml"
		dependencies := ""
		if *statementInput.tier1 {
			resources += ", generator.yaml"
			dependencies = "  dependsOn: [{name: generator}]\n"
			testutil.WriteFile(t, root, "flux/generator.yaml", substitutionKS("generator", "producer", ""))
			testutil.WriteFile(t, root, "producer/kustomization.yaml", "resources: [release.yaml]\n")
		}
		testutil.WriteFile(t, root, "flux/kustomization.yaml", resources+"]\n")
		consumer := substitutionKS(
			"consumer",
			"app",
			dependencies+"  postBuild: {substituteFrom: [{kind: ConfigMap, name: required-settings}]}\n",
		)
		if (*(*statementInput.testCaseInput)) == "tier_1_builder" {
			consumer = substitutionKS(
				"consumer",
				"app",
				"  postBuild: {substituteFrom: [{kind: ConfigMap, name: required-settings}]}\n",
			)
			testutil.WriteFile(t, root, "consumer-parent/kustomization.yaml", "resources: [consumer.yaml]\n")
			testutil.WriteFile(t, root, "consumer-parent/consumer.yaml", consumer)
			consumer = substitutionKS("consumer-parent", "consumer-parent", dependencies)
		}
		if (*(*statementInput.testCaseInput)) == "generated" {
			consumer = strings.ReplaceAll(
				consumer,
				"kind: ConfigMap, name: required-settings",
				"kind: Secret, name: required-settings",
			)
			testutil.WriteFile(
				t,
				root,
				"flux/external.yaml",
				"apiVersion: external-secrets.io/v1\nkind: ExternalSecret\nmetadata: {name: required-settings, namespace: apps}\nspec: {dataFrom: [{extract: {key: remote}}]}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"flux/kustomization.yaml",
				"resources: [consumer.yaml, repository.yaml, releases.yaml, external.yaml]\n",
			)
		}
		testutil.WriteFile(t, root, "flux/consumer.yaml", consumer)
		testutil.WriteFile(
			t,
			root,
			"flux/repository.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: charts, namespace: apps}\nspec: {interval: 1h, url: '"+(*statementInput.server).URL+"'}\n",
		)
		var releases strings.Builder
		for i := range 3 {
			var declaration strings.Builder
			fmt.Fprintf(
				&declaration,
				"---\napiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: producer-%d, namespace: apps}\nspec:\n  interval: 1h\n  chart: {spec: {chart: demo-%d, version: 1.0.0, sourceRef: {kind: HelmRepository, name: charts}}}\n",
				i,
				i,
			)
			if (*statementInput.tier1) && i == 0 {
				testutil.WriteFile(t, root, "producer/release.yaml", declaration.String())
			} else {
				releases.WriteString(declaration.String())
			}
		}
		testutil.WriteFile(t, root, "flux/releases.yaml", releases.String())
		testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			fmt.Sprintf(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result, namespace: apps}\ndata: {value: '${VALUE}-side-%d'}\n",
				side,
			),
		)
		(*statementInput.trees)[side] = Tree{
			Path:     filepath.Join(root, "flux"),
			RepoRoot: root,
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
}

func assertOpaqueInputInventory(t *testing.T, runs []Rendered, owner *sourcecache.Cache, label string) {
	t.Helper()
	for _, run := range runs {
		id := testutil.HelmReleaseID("apps", "producer-0")
		var reads store.ReadSet
		reads.Init(run.Store(), id, nil)
		controller := *run.hrc
		controller.Helm = helm.NewClient(owner)
		records, complete := controller.PreviousRender(
			t.Context(),
			run.Store().GetObject(id),
			&reads,
		)
		assert.Equal(t, complete, true)
		found := false
		if records != nil {
			for child := range records {
				found = found ||
					child == (manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "required-settings"})
			}
		}
		reads.Retire()
		assert.Equal(t, found, true)
	}
	t.Logf("%s accepted opaque inventory restored on both sides", label)
}

func opaqueInputCache(label, cacheDir string, warmCache *string) string {
	if label == "cold_0" {
		*warmCache = cacheDir
	} else if strings.HasPrefix(label, "warm_") {
		cacheDir = *warmCache
	}
	return cacheDir
}

func opaqueInputWork(t *testing.T, logs *bytes.Buffer) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for line := range bytes.Lines(logs.Bytes()) {
		var event struct {
			Message string `json:"msg"`
		}
		assert.NoError(t, json.Unmarshal(line, &event))
		switch event.Message {
		case "helm: render", "kustomize: build", "source: fetch complete":
			counts[event.Message]++
		}
	}
	return counts
}

func opaqueInputDownloadDelta(downloads *[3]atomic.Int64, before [3]int64) [3]int64 {
	var fetched [3]int64
	for i := range fetched {
		fetched[i] = downloads[i].Load() - before[i]
	}
	return fetched
}

func (i *testRenderTreesRequiredOpaqueInputCaseTailInputs) assertOpaqueInputColdWork(
	t *testing.T, counts map[string]int, fetched [3]int64,
) {
	t.Helper()
	want := [3]int64{1, 1, 1}
	renders := 3
	if *i.tier1 {
		want, renders = [3]int64{1, 0, 0}, 1
	}
	if (*(*i.testCaseInput)) == "generated" {
		want, renders = [3]int64{}, 0
	}
	assert.Equal(t, fetched, want)
	assert.Equal(t, counts["helm: render"], renders)
}
