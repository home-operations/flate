package sourceref_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_RootPin(t *testing.T) {
	current, repo, older, head := sourceRefFixture(t)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	for _, hash := range []plumbing.Hash{older, head} {
		assert.NoError(t, wt.Checkout(&gogit.CheckoutOptions{Hash: hash}))
		testutil.WriteFile(
			t,
			current,
			"flux/entry.yaml",
			testutil.KustomizationYAML(
				"apps",
				"flux-system",
				"  interval: 10m\n  path: ./\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(t, current, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec:
  interval: 10m
  url: git://fixture.invalid/cluster
  ref: {tag: v1.0.0}
  ignore: '/ignored.txt'
`)
		testutil.WriteFile(t, current, "kustomization.yaml", "resources: [apps, flux/entry.yaml, flux/repo.yaml]\n")
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
		)
		if hash == older {
			ref, err := repo.Head()
			assert.NoError(t, err)
			assert.NoError(t, repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), ref.Hash()),
			))
		}
	}
	original := t.TempDir()
	testutil.CopyTree(t, current, original)
	transport := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(current, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	for _, dir := range []string{original, current} {
		full, _ := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			dir,
			"--concurrency",
			"2",
			"--cache-dir",
			t.TempDir(),
		)
		docs, err := testutil.DecodeDocs(strings.NewReader(full))
		assert.NoError(t, err)
		pinned := 0
		ids := map[string]int{}
		declarations := map[string]int{}
		e2EPinnedDiffRootPinLoop(t, struct {
			docs         *[]map[string]any
			ids          *map[string]int
			declarations *map[string]int
			pinned       *int
		}{docs: &docs, ids: &ids, declarations: &declarations, pinned: &pinned})
		assert.Equal(t, pinned, 1)
		assert.Diff(t, declarations, map[string]int{"": 1, "apps": 1})
		assert.Diff(
			t,
			ids,
			map[string]int{
				"ConfigMap/apps/hello":              1,
				"Kustomization/flux-system/apps":    2,
				"GitRepository/flux-system/cluster": 1,
			},
		)
	}
	for _, stage := range []string{"dirty-only", "tag-change"} {
		t.Run(stage, func(t *testing.T) {
			e2EPinnedDiffRootPinStage3(t, &stage, &current, struct {
				original  *string
				transport **sourceRefLoader
			}{original: &original, transport: &transport})
		})
	}
}

func e2EPinnedDiffRootPinLoop(t *testing.T, loopInputs struct {
	docs         *[]map[string]any
	ids          *map[string]int
	declarations *map[string]int
	pinned       *int
},
) {
	t.Helper()

	for _, doc := range *loopInputs.docs {
		name, namespace := manifest.DocMetadata(doc)
		(*loopInputs.ids)[manifest.DocKind(doc)+"/"+namespace+"/"+name]++
		if manifest.DocKind(doc) == manifest.KindKustomization {
			metadata := doc["metadata"].(map[string]any)
			labels, _ := metadata["labels"].(map[string]any)
			owner, _ := labels[manifest.FluxKustomizeDomain+"/name"].(string)
			(*loopInputs.declarations)[owner]++
			if owner != "" {
				assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/namespace"], "flux-system")
			}
			spec := doc["spec"].(map[string]any)
			assert.Equal(t, spec["path"], "./")
			assert.Diff(
				t,
				spec["sourceRef"],
				map[string]any{"kind": manifest.KindGitRepository, "name": "cluster", "namespace": "flux-system"},
			)
		}
		if manifest.DocKind(doc) != manifest.KindConfigMap || name != "hello" {
			continue
		}
		metadata := doc["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		if labels[manifest.FluxKustomizeDomain+"/name"] == "apps" {
			assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/namespace"], "flux-system")
			assert.Equal(t, doc["data"].(map[string]any)["value"], "v1.0.0")
			(*loopInputs.pinned)++
		}
	}
}

func e2EPinnedDiffRootPinStage(t *testing.T, stage *string, current *string, capturedInputs struct {
	original  *string
	transport **sourceRefLoader
},
) {
	t.Helper()

	if (*stage) == "tag-change" {
		harness.MutateFile(t, filepath.Join((*current), "flux/repo.yaml"), "tag: v1.0.0", "tag: v2.0.0")
	}
	for _, scope := range []string{"flux", "."} {
		t.Run(scope, func(t *testing.T) {
			e2EPinnedDiffRootPinStageScope(t, &current, &capturedInputs.original, struct {
				scope     *string
				stage     **string
				transport ***sourceRefLoader
			}{scope: &scope, stage: &stage, transport: &capturedInputs.transport})
		})
	}
}

func e2EPinnedDiffRootPinStage2(t *testing.T, stage *string, current *string, capturedInputs struct {
	original  *string
	transport **sourceRefLoader
},
) {
	t.Helper()

	e2EPinnedDiffRootPinStage(t, stage, current, struct {
		original  *string
		transport **sourceRefLoader
	}{original: capturedInputs.original, transport: capturedInputs.transport})
}

func e2EPinnedDiffRootPinStageScope(t *testing.T, current **string, original **string, capturedInputs struct {
	scope     *string
	stage     **string
	transport ***sourceRefLoader
},
) {
	t.Helper()

	for _, side := range []struct{ name, current, original, before, after string }{
		{"forward", (*(*current)), (*(*original)), "v1.0.0", "v2.0.0"},
		{"reverse", (*(*original)), (*(*current)), "v2.0.0", "v1.0.0"},
	} {
		t.Run(side.name, func(t *testing.T) { e2EPinnedDiffRootPinStageScopeName(t, &side, &capturedInputs) })
	}
}

func e2EPinnedDiffRootPinStage3(t *testing.T, stage *string, current *string, capturedInputs struct {
	original  *string
	transport **sourceRefLoader
},
) {
	t.Helper()

	e2EPinnedDiffRootPinStage2(t, stage, current, struct {
		original  *string
		transport **sourceRefLoader
	}{original: capturedInputs.original, transport: capturedInputs.transport})
}

func e2EPinnedDiffRootPinStageScopeName(t *testing.T, side *struct {
	name     string
	current  string
	original string
	before   string
	after    string
}, capturedInputs *struct {
	scope     *string
	stage     **string
	transport ***sourceRefLoader
},
) {
	t.Helper()

	args := []string{
		"diff",
		"all",
		"--path",
		filepath.Join(side.current, (*capturedInputs.scope)),
		"--path-orig",
		filepath.Join(side.original, (*capturedInputs.scope)),
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	}
	out, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
	if (*(*capturedInputs.stage)) == "dirty-only" {
		again, _ := harness.RequireCLIOK(t, append(args, "-o", "diff")...)
		assert.Equal(t, again, out)
		assert.Equal(t, out, "")
	}
	switch {
	case (*(*capturedInputs.stage)) == "dirty-only" && (*capturedInputs.scope) == ".":

		product, _ := harness.RequireCLIOK(t, append(args, "-o", "json")...)
		var result struct {
			Changes    []any `json:"Changes"`
			Suppressed []any `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte(product), &result))
		assert.Equal(t, len(result.Changes), 0)
		assert.Equal(t, len(result.Suppressed), 0)

	case (*(*capturedInputs.stage)) == "dirty-only":

		assert.FatalIf(t, out != "", "dirty content changed the root-pin diff:\n%s", out)

	default:

		assertSourceValueDiff(t, out, side.before, side.after)
	}
	args[0] = "build"
	out, _ = harness.RequireCLIOK(t, args...)
	switch {
	case (*(*capturedInputs.stage)) == "dirty-only" && (*capturedInputs.scope) == ".":

		docs, err := testutil.DecodeDocs(strings.NewReader(out))
		assert.NoError(t, err)
		ids := map[string]int{}
		for _, doc := range docs {
			name, namespace := manifest.DocMetadata(doc)
			ids[manifest.DocKind(doc)+"/"+namespace+"/"+name]++
			if manifest.DocKind(doc) == manifest.KindConfigMap {
				assert.Diff(t, doc["data"].(map[string]any)["value"], "v1.0.0")
				metadata := doc["metadata"].(map[string]any)
				labels, _ := metadata["labels"].(map[string]any)
				assert.Equal(t, labels[manifest.FluxKustomizeDomain+"/name"], "apps")
				assert.Equal(
					t,
					labels[manifest.FluxKustomizeDomain+"/namespace"],
					"flux-system",
				)
			}
		}
		assert.Diff(
			t,
			ids,
			map[string]int{
				"Kustomization/flux-system/apps":    1,
				"GitRepository/flux-system/cluster": 1,
			},
		)

	case (*(*capturedInputs.stage)) == "dirty-only":

		assert.FatalIf(t, out != "", "dirty content selected the pinned root:\n%s", out)

	case !strings.Contains(out, "value: "+side.after) || strings.Contains(out, "value: dirty"):

		t.Fatalf("tag change did not select the pinned root:\n%s", out)
	}
	if (*(*capturedInputs.transport)).calls.Load() != 0 {
		t.Fatalf("root pin accessed transport %d times", (*(*capturedInputs.transport)).calls.Load())
	}
}
