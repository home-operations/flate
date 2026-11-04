package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// Changed-only rendering must follow a nested child's effective namespace on
// both sides without selecting an unchanged sibling.
func TestRenderTrees_ChangedOnlyAndDiff(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target, namespace, query string
		remote                         bool
	}{
		{"bare", "", "", "", false},
		{"target", "targetNamespace: flux-system, ", "flux-system", "", false},
		{"markerless remote", "targetNamespace: flux-system, ", "flux-system", "", true},
		{"markerless remote without submodules", "targetNamespace: flux-system, ", "flux-system", "?submodules=false", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRenderTreesChangedOnlyAndDiffCase(t, &tc)
		},
		)
	}
}

func TestRenderTrees_ChangedOnlyAndDiff_SelectorRelabel(t *testing.T) {
	t.Parallel()

	roots := [2]string{t.TempDir(), t.TempDir()}
	trees := make([]Tree, len(roots))
	for side, root := range roots {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [set.yaml, provider-a.yaml, provider-b.yaml]\n")
		testutil.WriteFile(t, root, "set.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: matrix, namespace: apps}
spec:
  inputsFrom:
    - kind: ResourceSetInputProvider
      selector:
        matchLabels: {app: selected}
  resourcesTemplate: |
    apiVersion: v1
    kind: ConfigMap
    metadata: {name: 'cfg-<< inputs.name >>', namespace: apps}
    data: {value: '<< inputs.value >>'}
`)
		for _, name := range []string{"a", "b"} {
			label := "selected"
			if side == 1 && name == "a" {
				label = "other"
			}
			testutil.WriteFile(t, root, "provider-"+name+".yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: provider-%s
  namespace: apps
  labels: {app: %s}
spec:
  type: Static
  defaultValues: {name: %s, value: stable}
`, name, label, name))
		}
		trees[side] = Tree{RepoRoot: root, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeSide(side)}}
	}

	render := func(trees []Tree) []Rendered {
		t.Helper()
		runs, err := RenderTrees(t.Context(), trees, Config{CacheDir: t.TempDir(), Concurrency: 2})
		assert.NoError(t, err)
		assert.Equal(t, len(runs), len(trees))
		for _, run := range runs {
			assert.NoError(t, run.Err)
			{
				assert.FatalIf(t, run.Result == nil, "%v", "missing render result")
			}
			assert.Equal(t, len(run.Result.Blocked), 0)
			assert.Equal(t, len(run.Result.Failed), 0)
		}
		return runs
	}
	compare := func(runs []Rendered) []byte {
		t.Helper()
		selected, selectedErr, remainder := Select(runs, nil, []manifest.NamedResource{
			testutil.KustomizationID("", ""),
			{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet},
		}, nil)
		assert.NoError(t, selectedErr)
		assert.NoError(t, remainder)
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
		assert.NoError(t, err)
		return body
	}

	full := []Rendered{render(trees[:1])[0], render(trees[1:])[0]}
	for _, run := range full {
		assert.Equal(t, run.filter.Enabled(), false)
	}
	testRenderTreesChangedOnlyAndDiffSelectorRelabelTail(
		t,
		&testRenderTreesChangedOnlyAndDiffSelectorRelabelTailInputs{
			full:    &full,
			compare: &compare,
			render:  &render,
			trees:   &trees,
		},
	)
}

func testRenderTreesChangedOnlyAndDiffSelectorRelabelTail(
	t *testing.T,
	tailCaseInput *testRenderTreesChangedOnlyAndDiffSelectorRelabelTailInputs,
) {
	t.Helper()
	assert.Diff(t, configMapNames((*tailCaseInput.full)[0].Result), map[string]bool{"cfg-a": true, "cfg-b": true})
	assert.Diff(t, configMapNames((*tailCaseInput.full)[1].Result), map[string]bool{"cfg-b": true})
	want := (*tailCaseInput.compare)((*tailCaseInput.full))
	var first []byte
	for run := range 2 {
		paired := (*tailCaseInput.render)((*tailCaseInput.trees))
		for _, side := range paired {
			assert.Equal(t, side.filter.Enabled(), true)
			assert.Diff(t, side.cfg.ExternalChanges.Paths(), []string{"provider-a.yaml"})
		}
		assert.Diff(t, configMapNames(paired[0].Result), map[string]bool{"cfg-a": true, "cfg-b": true})
		assert.Diff(t, configMapNames(paired[1].Result), map[string]bool{"cfg-b": true})
		body := (*tailCaseInput.compare)(paired)
		assert.Diff(t, string(body), string(want))
		var output struct {
			Changes []diff.Change `json:"changes"`
		}
		assert.NoError(t, json.Unmarshal(body, &output))
		assert.Equal(t, len(output.Changes), 2)
		var deleted []string
		{
			changeValues := output.Changes
			for changeIndex := range changeValues {
				change := changeValues[changeIndex]

				if change.Kind == manifest.KindConfigMap {
					assert.Equal(t, change.Status, diff.StatusRemoved)
					deleted = append(deleted, change.Name)
				} else {
					assert.Equal(t, change.Kind, manifest.KindResourceSetInputProvider)
					assert.Equal(t, change.Name, "provider-a")
					assert.Equal(t, change.Status, diff.StatusChanged)
				}
			}
		}
		assert.Diff(t, deleted, []string{"cfg-a"})
		if run == 0 {
			first = body
		} else {
			assert.Diff(t, string(body), string(first))
		}
	}
}

func testRenderTreesChangedOnlyAndDiffCase(t *testing.T, testCaseInput *struct {
	name string

	target    string
	namespace string
	query     string
	remote    bool
},
) {
	t.Helper()

	baseDir := t.TempDir()
	headDir := t.TempDir()
	writeCluster(t, baseDir, "v1", "same")
	writeCluster(t, headDir, "v2", "same")
	remote := ""
	if testCaseInput.remote {
		options := &git.CommitOptions{
			Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)},
		}
		module := t.TempDir()
		testutil.WriteFile(t, module, "kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(
			t,
			module,
			"cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-submodule}\n",
		)
		pin := testutil.GitCommitAll(t, testutil.GitInit(t, module), "module", options)
		root := t.TempDir()
		resources := "resources: [child.yaml, lib]\n"
		if testCaseInput.query != "" {
			resources = "resources: [child.yaml]\n"
			module = filepath.Join(module, "unavailable")
		}
		testutil.WriteFile(t, root, "kustomization.yaml", resources)
		testutil.WriteFile(
			t,
			root,
			"child.yaml",
			testutil.KustomizationYAML(
				"consumer-a",
				"",
				"  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: render-values}]\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			".gitmodules",
			"[submodule \"lib\"]\n\tpath = lib\n\turl = file://"+module+"\n",
		)
		repo := testutil.GitInit(t, root)
		testutil.GitCommitAll(t, repo, "remote", options)
		idx, err := repo.Storer.Index()
		assert.NoError(t, err)
		idx.Entries = append(idx.Entries, &index.Entry{Name: "lib", Mode: filemode.Submodule, Hash: pin})
		assert.NoError(t, repo.Storer.SetIndex(idx))
		wt, err := repo.Worktree()
		assert.NoError(t, err)
		_, err = wt.Commit("submodule", options)
		assert.NoError(t, err)
		remote = "file://" + root + testCaseInput.query
	}
	for i, dir := range []string{baseDir, headDir} {
		testutil.WriteFile(
			t,
			dir,
			"ks-a.yaml",
			testutil.KustomizationYAML(
				"app-a",
				"flux-system",
				"  {path: ./apps/app-a, "+testCaseInput.target+"sourceRef: {kind: GitRepository, name: flux-system}}\n",
			),
		)
		if i == 1 && testCaseInput.target != "" {
			testutil.WriteFile(
				t,
				dir,
				"apps/app-a/kustomization.yaml",
				"namespace: dirty\nresources: [cm.yaml]\n",
			)
		} else if i == 1 {
			testutil.WriteFile(
				t,
				dir,
				"apps/app-a/kustomization.yaml",
				"resources: [cm.yaml]\ncommonAnnotations: {revision: head}\n",
			)
		}
		testutil.WriteFile(
			t,
			dir,
			"apps/app-a/cm.yaml",
			testutil.KustomizationYAML(
				"consumer-a",
				"",
				"  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: render-values}]\n",
			),
		)
		if testCaseInput.remote {
			control := fmt.Sprintf("resources: [%q]\n", remote)
			if i == 1 {
				control += "# head\n"
			}
			testutil.WriteFile(t, dir, "apps/app-a/kustomization.yaml", control)
			{
				{
					err := os.Remove(filepath.Join(dir, "apps/app-a/cm.yaml"))
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
		}
		testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [ks-a.yaml, ks-b.yaml, inputs]\n")
		testutil.WriteFile(t, dir, "inputs/kustomization.yaml", "resources: [values.yaml]\n")
		testutil.WriteFile(
			t,
			dir,
			"inputs/values.yaml",
			fmt.Sprintf(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: render-values, namespace: flux-system}\ndata: {VALUE: v%d}\n",
				i+1,
			),
		)
		testutil.WriteFile(t, dir, "consumer/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(
			t,
			dir,
			"consumer/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cfg-a}\ndata: {k: '${VALUE}'}\n",
		)
	}

	if testCaseInput.remote {
		assertFullRemoteTrees(t, []string{baseDir, headDir}, testCaseInput.query)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	renderedTrees1, err := RenderTrees(
		ctx,
		[]Tree{
			{RepoRoot: baseDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}},
			{RepoRoot: headDir, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}},
		},
		Config{
			WipeSecrets: true,
			Network:     new(source.Network),
			Concurrency: 2,
		},
	)
	base, head := renderedTrees1[0], renderedTrees1[1]
	{
		assert.FatalIf(
			t,
			(err != nil) != (testCaseInput.namespace == ""),
			"RenderTrees: %v, want blocked=%t",
			err,
			testCaseInput.namespace == "",
		)
	}
	{
		assert.FatalIf(
			t,
			base.Result == nil || head.Result == nil,
			"both sides must render: base=%v head=%v",
			base.Result,
			head.Result,
		)
	}

	assertChangedTreeChildren(t, []Rendered{base, head}, testCaseInput.namespace)

	consumer := testutil.KustomizationID(testCaseInput.namespace, "consumer-a")
	testRenderTreesChangedOnlyAndDiffCaseSegment(
		t,
		&testRenderTreesChangedOnlyAndDiffCaseSegmentInputs{
			testCaseInput: &testCaseInput,
			base:          &base,
			head:          &head,
			consumer:      &consumer,
		},
	)
	if testCaseInput.namespace == "" {
		return
	}

	for i, rendered := range []Rendered{base, head} {
		obj, ok := rendered.Store().Get[*manifest.Kustomization](consumer)
		{
			assert.FatalIf(t, !ok, "side %d lost effective consumer identity", i)
		}
		owners := testutil.Owners(rendered.Store().Productions(consumer, nil))
		{
			assert.FatalIf(
				t,
				!slices.Contains(owners, testutil.KustomizationID("flux-system", "app-a")),
				"side %d owners=%v",
				i,
				owners,
			)
		}
		ns := obj.Namespace
		assert.Equal(t, ns, "flux-system")
		docs := outputByParent(rendered.Result.Manifests)[consumer]
		{
			assert.FatalIf(t, len(docs) != 1, "side %d consumer documents = %d", i, len(docs))
		}
		assert.Equal(t, docs[0]["data"].(map[string]any)["k"], any(fmt.Sprintf("v%d", i+1)))
		_, ns = manifest.DocMetadata(docs[0])
		assert.Equal(t, ns, "")
	}

	selected, selectedErr, _ := Select([]Rendered{base, head}, err,
		[]manifest.NamedResource{testutil.KustomizationID("", ""), testutil.HelmReleaseID("", "")}, nil)
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
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	{
		{
			err := json.Unmarshal(body, &output)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	assert.Equal(t, len(output.Changes), 2)
	changes := slices.DeleteFunc(output.Changes, func(change diff.Change) bool {
		if change.Name == "render-values" {
			assert.Equal(t, change.Parent.NamedResource, manifest.NamedResource{})
			assert.Equal(t, change.Parent.Path, ".")
			assert.Equal(t, change.Status, diff.StatusChanged)
			return true
		}
		return false
	})
	{
		assert.FatalIf(
			t,
			len(changes) != 1,
			"want exactly 1 change (cfg-a), got %d: %+v",
			len(changes),
			changes,
		)
	}
	c := changes[0]
	{
		assert.ErrorIf(
			t,
			c.Name != "cfg-a" || c.Kind != "ConfigMap" || c.Status != diff.StatusChanged,
			"change = %+v; want ConfigMap cfg-a changed",
			c,
		)
	}
}

type testRenderTreesChangedOnlyAndDiffSelectorRelabelTailInputs struct {
	full    *[]Rendered
	compare *func(runs []Rendered) []byte
	render  *func(trees []Tree) []Rendered
	trees   *[]Tree
}

type testRenderTreesChangedOnlyAndDiffCaseSegmentInputs struct {
	testCaseInput **struct {
		name      string
		target    string
		namespace string
		query     string
		remote    bool
	}
	base     *Rendered
	head     *Rendered
	consumer *manifest.NamedResource
}

func testRenderTreesChangedOnlyAndDiffCaseSegment(
	t *testing.T,
	statementInput *testRenderTreesChangedOnlyAndDiffCaseSegmentInputs,
) {
	t.Helper()
	for i, side := range []Rendered{(*statementInput.base), (*statementInput.head)} {
		if (*statementInput.testCaseInput).remote {
			assert.Equal(t, configMapNames(side.Result)["from-submodule"], (*statementInput.testCaseInput).query == "")
		}
		if (*statementInput.testCaseInput).namespace == "" {
			missing := manifest.NamedResource{
				Group: "source.toolkit.fluxcd.io",
				Kind:  manifest.KindGitRepository,
				Name:  "flux-system",
			}
			if side.Err == nil || !slices.Contains(side.Result.Blocked[(*statementInput.consumer)], missing) ||
				configMapNames(side.Result)["cfg-a"] {
				t.Fatalf(
					"side %d bare child: error=%v blockers=%v manifests=%v",
					i,
					side.Err,
					side.Result.Blocked,
					side.Result.Manifests,
				)
			}
		} else {
			values := manifest.NamedResource{
				Kind:      manifest.KindConfigMap,
				Namespace: (*statementInput.testCaseInput).namespace,
				Name:      "render-values",
			}
			obj, ok := side.Store().Get[*manifest.ConfigMap](values)
			if side.Err != nil || len(side.Result.Blocked) != 0 || !ok ||
				obj.Data["VALUE"] != fmt.Sprintf("v%d", i+1) {
				t.Fatalf(
					"side %d producer %s: error=%v blockers=%v object=%v",
					i,
					values,
					side.Err,
					side.Result.Blocked,
					obj,
				)
			}
		}
		{
			assert.FatalIf(t, configMapNames(side.Result)["cfg-b"], "side %d rendered unchanged sibling", i)
		}
	}
}

func assertFullRemoteTrees(t *testing.T, roots []string, query string) {
	t.Helper()
	for side, root := range roots {
		var first []byte
		for run := range 2 {
			rendered, err := RenderTrees(
				t.Context(),
				[]Tree{
					{
						RepoRoot: root,
						Path:     root,
						ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
					},
				},
				Config{CacheDir: t.TempDir(), Concurrency: 2},
			)
			assert.NoError(t, err)
			result := rendered[0].Result
			assert.Equal(t, configMapNames(result)["from-submodule"], query == "")
			byParent := outputByParent(result.Manifests)
			consumer := testutil.KustomizationID("flux-system", "consumer-a")
			{
				assert.FatalIf(
					t,
					len(byParent[consumer]) != 1 || !configMapNames(result)["cfg-b"],
					"full remote reachability: %+v",
					result,
				)
			}
			assert.Equal(
				t,
				byParent[consumer][0]["data"].(map[string]any)["k"],
				any(fmt.Sprintf("v%d", side+1)),
			)
			body, err := json.Marshal(result.Manifests)
			assert.NoError(t, err)
			if run == 0 {
				first = body
			} else {
				assert.Equal(t, string(body), string(first))
			}
		}
	}
}

func assertChangedTreeChildren(t *testing.T, sides []Rendered, namespace string) {
	t.Helper()
	for _, side := range sides {
		children := 0
		for _, obj := range side.Store().ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
			if obj.Name == "consumer-a" {
				children++
				assert.Equal(t, obj.Namespace, namespace)
				assert.Equal(t, obj.SourceID().Namespace, namespace)
			}
		}
		assert.Equal(t, children, 1)
	}
}
