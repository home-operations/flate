package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRenderTrees_MultipleValuesFromKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		keys    []string
		changed bool
	}{
		{"unchanged then changed", []string{"stable.yaml", "changed.yaml"}, true},
		{"changed then unchanged", []string{"changed.yaml", "stable.yaml"}, true},
		{"all unchanged", []string{"stable.yaml", "changed.yaml"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderTreesMultipleValuesFromKeysCase(t, &tc)
		},
		)
	}
}

func TestRenderTrees_ParentIntervalExternalChart(t *testing.T) {
	t.Parallel()
	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 2)
	for side, root := range roots {
		testutil.WriteFile(
			t,
			root,
			"flux/parent.yaml",
			testutil.KustomizationYAML(
				"parent",
				"apps",
				fmt.Sprintf(
					"  interval: %dm\n  path: ./apps/child\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
					side+1,
				),
			),
		)
		testutil.WriteFile(t, root, "apps/child/kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"apps/child/release.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: child, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n",
		)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/cm.yaml",
			fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {value: v%d}\n", side+1),
		)
		trees[side] = Tree{
			RepoRoot: root,
			Path:     filepath.Join(root, "flux"),
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
		}
	}
	changes, err := change.Detect(
		t.Context(),
		testDirectoryInput(t, roots[0]),
		testDirectoryInput(t, roots[1]),
		change.DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Diff(t, changes.Paths(), []string{"charts/demo/templates/cm.yaml", "flux/parent.yaml"})
	child := testutil.HelmReleaseID("apps", "child")
	parent := testutil.KustomizationID("apps", "parent")
	full := make([]Rendered, 2)
	for side, tree := range trees {
		runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 1)
		full[side] = runs[0]
	}
	delta := func(runs []Rendered) []byte {
		t.Helper()
		for side, run := range runs {
			assert.NoError(t, run.Err)
			assert.Equal(t, len(run.Result.Warnings), 0)
			assert.Equal(t, len(run.Result.ClusterWarnings), 0)
			assert.Equal(t, len(run.Result.Blocked), 0)
			docs := outputByParent(run.Result.Manifests)[child]
			assert.Equal(t, len(docs), 1)
			if len(docs) == 1 {
				assert.Diff(t, docs[0]["data"], map[string]any{"value": fmt.Sprintf("v%d", side+1)})
			}
		}
		projectedResults0, projectionErrors0 := Select(
			runs,
			nil,
			[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")},
			nil,
		)
		selected, selectedErr, err := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
		assert.NoError(t, err)
		comparison1, preparationErr1 := diff.Compare([]diff.Group{{Orig: diff.Snapshot{
			Subjects:     selected[0].Subjects,
			Manifests:    selected[0].Manifests,
			ProducerKeys: selected[0].ProducerKeys,
			Unavailable:  selected[0].Unavailable,
		}, Current: diff.Snapshot{
			Subjects:     selected[1].Subjects,
			Manifests:    selected[1].Manifests,
			ProducerKeys: selected[1].ProducerKeys,
			Unavailable:  selected[1].Unavailable,
		}, SelectedErr: selectedErr}}, diff.Options{Format: diff.FormatJSON})
		var comparisonOutput1 bytes.Buffer
		writeErr1 := comparison1.Write(&comparisonOutput1, nil)
		body, warnings, suppressed, err := comparisonOutput1.Bytes(), comparison1.Groups[0].Warnings, comparison1.Groups[0].Suppressed, errors.Join(
			preparationErr1,
			writeErr1,
		)
		assert.NoError(t, err)
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, len(suppressed), 0)
		return body
	}
	want := delta(full)
	var expected struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(want, &expected))
	assert.Equal(t, len(expected.Changes) > 0, true)
	assert.Equal(t, slices.ContainsFunc(expected.Changes, func(c diff.Change) bool {
		return c.Parent.NamedResource == child && c.Kind == "ConfigMap" && c.Name == "output" &&
			c.Status == diff.StatusChanged
	}), true)
	t.Logf("independent full delta: %s", want)
	var first [2][]byte
	for run := range 2 {
		for side := range trees {
			trees[side].ExternalChanges = changes
		}
		runs, err := renderTreesForTest(t.Context(), t, trees, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		assert.Equal(t, string(delta(runs)), string(want))
		for side, rendered := range runs {
			membershipResult1 := rendered.selection.Snapshot()
			needed, selected, dirty := membershipResult1.Reconcile(
				parent,
			), membershipResult1.Output(
				parent,
			), membershipResult1.Changed(
				parent,
			)
			assert.Equal(t, needed, true)
			assert.Equal(t, selected, true)
			t.Logf(
				"run %d side %d parent membership: needed=%t selected=%t dirty=%t",
				run,
				side,
				needed,
				selected,
				dirty,
			)
			membershipResult2 := rendered.selection.Snapshot()
			selected, dirty = membershipResult2.Output(child), membershipResult2.Changed(child)
			assert.Equal(t, selected, true)
			assert.Equal(t, dirty, true)
			assert.Equal(t, rendered.Store().GetObject(child) != nil, true)
			body, err := json.Marshal(rendered.Result.Manifests)
			assert.NoError(t, err)
			if run == 0 {
				first[side] = body
			} else {
				assert.Equal(t, string(body), string(first[side]))
			}
		}
	}
}

func TestRenderTrees_OptionalInputRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"directory_0300", "committed_0301"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			testRenderTreesOptionalInputRoutesCase(t, &route)
		},
		)
	}
}

func TestRenderTrees_ResourceSetGeneratedData(t *testing.T) {
	t.Parallel()

	baseDir, headDir := t.TempDir(), t.TempDir()
	for i, dir := range []string{baseDir, headDir} {
		writeCluster(t, dir, "unused", "same")
		testutil.WriteFile(t, dir, "apps/app-a/cm.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: z, namespace: flux-system}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>, namespace: flux-system}
      data: {key: v%d}
    - apiVersion: v1
      kind: Secret
      metadata: {name: secret-<< inputs.name >>, namespace: default}
      stringData: {key: same}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: a, namespace: flux-system}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>, namespace: flux-system}
      data: {key: losing-explicit}
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: cm-<< inputs.name >>}
      data: {key: losing-omitted}
`, i+1))
	}
	renderedTrees3, err := renderTreesForTest(t.Context(), t,
		[]Tree{
			{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
			{RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
		},
		Config{Concurrency: 2, Network: new(source.Network)},
	)
	base, head := renderedTrees3[0], renderedTrees3[1]
	assert.NoError(t, err)
	parent := testutil.KustomizationID("flux-system", "app-a")
	for i, side := range []Rendered{base, head} {
		assert.FatalIf(
			t,
			side.Result == nil || side.Err != nil,
			"side %d incomplete: result=%v err=%v",
			i,
			side.Result,
			side.Err,
		)
		assert.FatalIf(
			t,
			len(outputByParent(side.Result.Manifests)[testutil.KustomizationID("flux-system", "app-b")]) != 0 ||
				configMapNames(side.Result)["cfg-b"],
			"side %d rendered unchanged app-b: %v",
			i,
			side.Result.Manifests,
		)

		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "cm-one"}
		cm, ok := side.Store().Get[*manifest.ConfigMap](id)
		assert.FatalIf(t, !ok, "%v", "missing shared ConfigMap")
		assert.Diff(t, cm.Data, map[string]any{"key": fmt.Sprintf("v%d", i+1)})
		winner := manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "flux-system",
			Name:      "z",
		}
		assert.Equal(t, side.Store().EmissionOwner(id), winner)
		assert.Equal(t, len(side.Result.Warnings), 1)
		if len(side.Result.Warnings) == 1 {
			assert.Equal(t, side.Result.Warnings[0].Message, "conflicting producers; using "+winner.String())
		}
		identities := make([]string, 0, len(outputByParent(side.Result.Manifests)[parent]))
		for _, doc := range outputByParent(side.Result.Manifests)[parent] {
			metadata := manifest.DocMetadata(doc)
			name, namespace := metadata.Name, metadata.Namespace
			identities = append(identities, manifest.DocKind(doc)+"/"+namespace+"/"+name)
			switch manifest.DocKind(doc) {
			case manifest.KindConfigMap:
				assert.Diff(t, doc["data"], map[string]any{"key": fmt.Sprintf("v%d", i+1)})
			case manifest.KindSecret:
				assert.Diff(t, doc["stringData"], map[string]any{"key": "same"})
			}
		}
		slices.Sort(identities)
		assert.Diff(
			t,
			identities,
			[]string{
				"ConfigMap/flux-system/cm-one",
				"ResourceSet/flux-system/a",
				"ResourceSet/flux-system/z",
				"Secret/default/secret-one",
			},
		)
	}
	projectedResults1, projectionErrors1 := Select([]Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
	selected, selectedErr, _ := projectedResults1, projectionErrors1.Selected, projectionErrors1.Remaining
	comparison2, preparationErr2 := diff.Compare([]diff.Group{{Orig: diff.Snapshot{
		Subjects:     selected[0].Subjects,
		Manifests:    selected[0].Manifests,
		ProducerKeys: selected[0].ProducerKeys,
		Unavailable:  selected[0].Unavailable,
	}, Current: diff.Snapshot{
		Subjects:     selected[1].Subjects,
		Manifests:    selected[1].Manifests,
		ProducerKeys: selected[1].ProducerKeys,
		Unavailable:  selected[1].Unavailable,
	}, SelectedErr: selectedErr}}, diff.Options{Format: diff.FormatJSON})
	var comparisonOutput2 bytes.Buffer
	writeErr2 := comparison2.Write(&comparisonOutput2, nil)
	body, _, _, err := comparisonOutput2.Bytes(), comparison2.Groups[0].Warnings, comparison2.Groups[0].Suppressed, errors.Join(
		preparationErr2,
		writeErr2,
	)
	assert.NoError(t, err)
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(body, &output))
	generatedChanges := 0
	for _, c := range output.Changes {
		assert.FatalIf(t, c.Name == "cfg-b" || c.Name == "secret-one", "unchanged document in diff: %+v", c)
		if c.Kind == manifest.KindConfigMap && c.Namespace == "flux-system" && c.Name == "cm-one" {
			generatedChanges++
			assert.Equal(t, c.Parent.Name, parent.Name)
			assert.Equal(t, c.Parent.Namespace, parent.Namespace)
			assert.Equal(t, c.Status, diff.StatusChanged)
			assert.Diff(t, c.Old["data"], map[string]any{"key": "v1"})
			assert.Diff(t, c.New["data"], map[string]any{"key": "v2"})
		}
	}
	assert.Equal(t, generatedChanges, 1)
}

// writeHomeOpsCluster lays out a cluster-template-style tree under dir
// with NO .git: the Flux entry point is kubernetes/flux/cluster/ks.yaml
// (cluster-apps → ./kubernetes/apps, repo-ROOT-relative), and the app's
// ConfigMap carries val. This is konflate's extracted-tree shape — the
// caller renders it via Tree{RepoRoot: dir, Path: dir/kubernetes/flux/cluster}.
func writeHomeOpsCluster(t *testing.T, dir, val string) {
	t.Helper()
	testutil.WriteFile(
		t,
		dir,
		"kubernetes/flux/cluster/ks.yaml",
		testutil.KustomizationYAML(
			"cluster-apps",
			"flux-system",
			"  path: ./kubernetes/apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "kubernetes/apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, dir, "kubernetes/apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo, namespace: default}\ndata: {k: "+val+"}\n")
}

// TestRenderTrees_RepoRootRelativeNoGit is the konflate clusterPath
// regression at the SDK level: two extracted trees with NO .git, the Flux
// entry point a subdir (kubernetes/flux/cluster), and repo-root-relative
// spec.path (./kubernetes/apps). With per-side Tree{RepoRoot, Path} the
// change in kubernetes/apps/cm.yaml — outside the scanned entry-point dir —
// is detected and rendered, and diff.Changes reports exactly it. (Before
// the explicit-root model, the .git-less subdir collapsed the anchor and
// the change detection both failed: empty diff / "no changes detected".)
func TestRenderTrees_RepoRootRelativeNoGit(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	headDir := t.TempDir()
	writeHomeOpsCluster(t, baseDir, "v1")
	writeHomeOpsCluster(t, headDir, "v2") // only the ConfigMap data differs

	entry := func(root string) string { return root + "/kubernetes/flux/cluster" }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	renderedTrees4, err := renderTreesForTest(ctx, t,
		[]Tree{
			{RepoRoot: baseDir, Path: entry(baseDir), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
			{RepoRoot: headDir, Path: entry(headDir), ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
		},
		Config{Concurrency: 4},
	)
	base, head := renderedTrees4[0], renderedTrees4[1]
	assert.FatalIf(t, err != nil, "RenderTrees: %v", err)
	assert.FatalIf(
		t,
		base.Result == nil || head.Result == nil,
		"both sides must render: base=%v head=%v",
		base.Result,
		head.Result,
	)
	projectedResults2, projectionErrors2 := Select([]Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
	selected, selectedErr, _ := projectedResults2, projectionErrors2.Selected, projectionErrors2.Remaining
	comparison3, preparationErr3 := diff.Compare([]diff.Group{{Orig: diff.Snapshot{
		Subjects:     selected[0].Subjects,
		Manifests:    selected[0].Manifests,
		ProducerKeys: selected[0].ProducerKeys,
		Unavailable:  selected[0].Unavailable,
	}, Current: diff.Snapshot{
		Subjects:     selected[1].Subjects,
		Manifests:    selected[1].Manifests,
		ProducerKeys: selected[1].ProducerKeys,
		Unavailable:  selected[1].Unavailable,
	}, SelectedErr: selectedErr}}, diff.Options{Format: diff.FormatJSON})
	var comparisonOutput3 bytes.Buffer
	writeErr3 := comparison3.Write(&comparisonOutput3, nil)
	body, _, _, err := comparisonOutput3.Bytes(), comparison3.Groups[0].Warnings, comparison3.Groups[0].Suppressed, errors.Join(
		preparationErr3,
		writeErr3,
	)
	assert.FatalIf(t, err != nil, "%v", err)
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	{
		err := json.Unmarshal(body, &output)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	changes := output.Changes
	assert.FatalIf(
		t,
		len(changes) != 1,
		"want exactly 1 change (demo ConfigMap), got %d: %+v",
		len(changes),
		changes,
	)
	{
		c := changes[0]
		assert.ErrorIf(
			t,
			c.Name != "demo" || c.Kind != "ConfigMap" || c.Status != diff.StatusChanged,
			"change = %+v; want ConfigMap demo changed",
			c,
		)
	}
}

func testRenderTreesMultipleValuesFromKeysCase(
	t *testing.T,
	testCaseInput *struct {
		name string

		keys    []string
		changed bool
	},
) {
	t.Helper()

	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, 2)
	for side, root := range roots {
		value := "old"
		if side == 1 && testCaseInput.changed {
			value = "new"
		}
		testutil.WriteFile(
			t,
			root,
			"flux/values.yaml",
			fmt.Sprintf(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: values, namespace: apps}\ndata:\n  stable.yaml: 'stable: same'\n  changed.yaml: 'changed: %s'\n  unused: '%d'\n",
				value,
				side,
			),
		)
		release := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  chart: {spec: {chart: charts/demo, sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}}}\n  valuesFrom:\n"
		var releaseSb714 strings.Builder
		for _, key := range testCaseInput.keys {
			releaseSb714.WriteString("  - {kind: ConfigMap, name: values, valuesKey: " + key + "}\n")
		}
		release += releaseSb714.String()
		testutil.WriteFile(t, root, "flux/release.yaml", release)
		testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
		testutil.WriteFile(
			t,
			root,
			"charts/demo/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata:\n  changed: {{ .Values.changed | quote }}\n  stable: {{ .Values.stable | quote }}\n",
		)
		trees[side] = Tree{
			RepoRoot:        root,
			Path:            filepath.Join(root, "flux"),
			ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)},
			ExternalChanges: change.NewSet([]string{"flux/values.yaml"}),
		}
	}
	runs, err := renderTreesForTest(t.Context(), t, trees, Config{CacheDir: t.TempDir(), Concurrency: 2})
	assert.NoError(t, err)
	assert.Equal(t, len(runs), 2)
	app := testutil.HelmReleaseID("apps", "app")
	for side, run := range runs {
		assert.NoError(t, run.Err)
		assert.Equal(t, len(run.Result.Warnings), 0)
		assert.Equal(t, len(run.Result.ClusterWarnings), 0)
		assert.Equal(t, len(run.Result.Blocked), 0)
		membershipResult3 := run.selection.Snapshot()
		selected, dirty := membershipResult3.Output(app), membershipResult3.Changed(app)
		assert.Equal(t, selected, testCaseInput.changed)
		assert.Equal(t, dirty, testCaseInput.changed)
		docs := outputByParent(run.Result.Manifests)[app]
		if !testCaseInput.changed {
			assert.Equal(t, len(docs), 0)
			continue
		}
		assert.Equal(t, len(docs), 1)
		if len(docs) == 1 {
			value := "old"
			if side == 1 {
				value = "new"
			}
			assert.Diff(t, docs[0]["data"], map[string]any{"changed": value, "stable": "same"})
		}
	}
}

func testRenderTreesOptionalInputRoutesCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	before, after := t.TempDir(), t.TempDir()
	for _, root := range []string{before, after} {
		testutil.WriteFile(t, root, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
		testutil.WriteFile(
			t,
			root,
			"flux/app.yaml",
			testutil.KustomizationYAML(
				"app",
				"flux-system",
				"  path: ./apps/app\n  sourceRef: {kind: GitRepository, name: cluster}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"flux/stable.yaml",
			testutil.KustomizationYAML(
				"stable",
				"flux-system",
				"  path: ./apps/stable\n  sourceRef: {kind: GitRepository, name: cluster}\n  dependsOn: [{name: app}]\n",
			),
		)
		for _, name := range []string{"app", "stable"} {
			testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [output.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"apps/"+name+"/output.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: flux-system}\ndata: {value: '${VALUE:=old}'}\n",
			)
		}
	}
	owner := cachetest.New(t, "", sourcecache.New)
	var base string
	var repo *git.Repository
	options := &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
	}
	if (*testCaseInput) == "committed_0301" {
		repo = testutil.GitInit(t, after)
		base = testutil.GitCommitAll(t, repo, "base", options).String()
	}
	testutil.WriteFile(
		t,
		after,
		"flux/optional.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: new}\n",
	)
	var inputs *baseline.Result
	var err error
	if repo != nil {
		testutil.GitCommitAll(t, repo, "optional input appears", options)
		inputs, err = baseline.AutoResolve(
			t.Context(),
			baseline.Config{Network: networktest.New(t), Path: after, Base: base, Cache: owner},
		)
	} else {
		inputs, err = baseline.ResolvePaths(t.Context(), after, before)
	}
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		repo != nil && inputs.Changes == nil,
		"%v",
		"committed resolver returned no change set",
	)
	app := testutil.KustomizationID("flux-system", "app")
	stable := testutil.KustomizationID("flux-system", "stable")
	cluster := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "flux-system",
		Name:      "cluster",
	}
	fullApp := make([][]map[string]any, 2)
	for side, input := range []*store.SourceArtifact{inputs.BaseInput, inputs.Source} {
		renderedSingleton1, err := renderTreesForTest(t.Context(), t,
			[]Tree{
				{
					Path:     input.LocalPath,
					RepoRoot: input.LocalPath,
					Input:    input,
					SelfURLs: []string{"https://example.test/cluster.git"},
					ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				},
			},
			Config{Cache: owner, Concurrency: 2},
		)
		assert.FatalIf(t, len(renderedSingleton1) != 1, "missing render: %v", err)
		result := renderedSingleton1[0].Result
		assert.NoError(t, err)
		fullApp[side] = outputByParent(result.Manifests)[app]
		assert.FatalIf(t, len(fullApp[side]) != 1,
			"side %d full app documents = %d", side, len(fullApp[side]))

		want := "${VALUE:=old}"
		if side == 1 {
			want = "new"
		}
		assert.Diff(t, fullApp[side][0]["data"], map[string]any{"value": want})
	}
	testRenderTreesOptionalInputRoutesCaseTail(
		t,
		&testRenderTreesOptionalInputRoutesCaseTailInputs{
			owner:   &owner,
			inputs:  &inputs,
			app:     &app,
			cluster: &cluster,
			stable:  &stable,
			fullApp: &fullApp,
		},
	)
}

func testRenderTreesOptionalInputRoutesCaseTail(
	t *testing.T,
	tailCaseInput *testRenderTreesOptionalInputRoutesCaseTailInputs,
) {
	t.Helper()
	cfg := treeConfig{
		Cache:           (*tailCaseInput.owner),
		Concurrency:     2,
		BaseInput:       (*tailCaseInput.inputs).BaseInput,
		Input:           (*tailCaseInput.inputs).Source,
		ExternalChanges: (*tailCaseInput.inputs).Changes,
	}
	baseTree := Tree{
		RepoRoot: (*tailCaseInput.inputs).BaseInput.LocalPath,
		Path:     (*tailCaseInput.inputs).BaseScanPath,
		SelfURLs: []string{"https://example.test/cluster.git"},
	}
	headTree := Tree{
		RepoRoot: (*tailCaseInput.inputs).Source.LocalPath,
		Path:     (*tailCaseInput.inputs).ScanPath,
		SelfURLs: []string{"https://example.test/cluster.git"},
	}
	var first [2][]byte
	for run := range 2 {
		renderedTrees2, err := renderTreesForTest(t.Context(), t,
			[]Tree{
				{
					RepoRoot:        baseTree.RepoRoot,
					Path:            baseTree.Path,
					SelfURLs:        baseTree.SelfURLs,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
					Input:           cfg.BaseInput,
					BaseInput:       cfg.Input,
					ExternalChanges: cfg.ExternalChanges,
				},
				{
					RepoRoot:        headTree.RepoRoot,
					Path:            headTree.Path,
					SelfURLs:        headTree.SelfURLs,
					ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					Input:           cfg.Input,
					BaseInput:       cfg.BaseInput,
					ExternalChanges: cfg.ExternalChanges,
				},
			},
			cfg.Config,
		)
		base, head := renderedTrees2[0], renderedTrees2[1]
		assert.NoError(t, err)
		for side, rendered := range []Rendered{base, head} {
			assert.FatalIf(
				t,
				rendered.Result == nil || rendered.Orchestrator == nil,
				"side %d missing render result",
				side,
			)
			for _, id := range []manifest.NamedResource{(*tailCaseInput.app), (*tailCaseInput.cluster)} {
				info, _ := rendered.Store().GetStatus(id)
				assert.ErrorIf(
					t,
					info.Outcome() != store.OutcomePassed,
					"side %d required %s was not reconciled: %+v",
					side,
					id,
					info,
				)
			}
			_, dispatched := rendered.Store().GetStatus((*tailCaseInput.stable))
			assert.Equal(t, dispatched, false)
			byParent := outputByParent(rendered.Result.Manifests)
			assert.Equal(t, len(byParent[(*tailCaseInput.stable)]), 0)
			assert.Diff(t, byParent[(*tailCaseInput.app)], (*tailCaseInput.fullApp)[side])
			type producerBatch struct {
				Parent    manifest.NamedResource `json:"Parent"`
				Documents []map[string]any       `json:"Documents"`
			}
			var batches []producerBatch
			for _, batch := range rendered.Result.Manifests {
				var parent manifest.NamedResource
				if batch.Parent != nil {
					parent = batch.Parent.Named()
				}
				batches = append(batches, producerBatch{parent, batch.Documents})
			}
			body, err := json.Marshal(batches)
			assert.NoError(t, err)
			if run == 0 {
				first[side] = body
			} else {
				assert.Equal(t, string(body), string(first[side]))
			}
		}
	}
}

type testRenderTreesOptionalInputRoutesCaseTailInputs struct {
	owner **sourcecache.
		Cache

	inputs **baseline.
		Result

	app *manifest.
		NamedResource

	cluster *manifest.
		NamedResource

	stable *manifest.
		NamedResource

	fullApp *[][]map[string]any
}
