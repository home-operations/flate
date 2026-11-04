package scope_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SelectedKustomizeRootDiff(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		before, after                            string
		patch, sibling, plain, declaration, list bool
		want                                     map[string]string
	}{
		{name: "referenced patch", before: "resources: [deployment.yaml, cm.yaml]\npatches: [{path: patch.yaml}]\n", patch: true, want: map[string]string{"web": "changed"}},
		{name: "resource removed", before: "resources: [deployment.yaml, cm.yaml]\n", after: "resources: [deployment.yaml]\n", want: map[string]string{"config": "removed"}},
		{name: "resource added", before: "resources: [deployment.yaml]\n", after: "resources: [deployment.yaml, cm.yaml]\n", want: map[string]string{"config": "added"}},
		{name: "list member added", before: "resources: [deployment.yaml, cm.yaml]\n", list: true, want: map[string]string{"config": "added"}},
		{name: "list member removed", before: "resources: [deployment.yaml, cm.yaml]\n", list: true, want: map[string]string{"config": "removed"}},
		{name: "declaration removed", before: "resources: [deployment.yaml, child.yaml]\nnameSuffix: -dev\n", after: "resources: [deployment.yaml]\nnameSuffix: -dev\n", declaration: true, want: map[string]string{"child-dev": "removed"}},
		{name: "declaration added", before: "resources: [deployment.yaml]\nnameSuffix: -dev\n", after: "resources: [deployment.yaml, child.yaml]\nnameSuffix: -dev\n", declaration: true, want: map[string]string{"child-dev": "added"}},
		{name: "excluded sibling", before: "resources: [deployment.yaml, cm.yaml]\n", sibling: true},
		{name: "plain dependency", plain: true, sibling: true, want: map[string]string{"excluded": "changed"}},
	} {
		t.Run(tc.name, func(t *testing.T) { e2ESelectedKustomizeRootDiffName(t, &tc) })
	}
	for _, tc := range []struct {
		name          string
		before, after []string
	}{
		{name: "owner added", after: []string{"owner"}},
		{name: "owner removed", before: []string{"owner"}},
		{name: "owner renamed", before: []string{"old"}, after: []string{"new"}},
		{name: "owner winner changed", before: []string{"a", "b"}, after: []string{"b"}},
	} {
		t.Run(tc.name, func(t *testing.T) { e2ESelectedKustomizeRootDiffName2(t, &tc) })
	}
	for _, tc := range []struct {
		name   string
		owners [2]string
		nested bool
	}{
		{name: "owner added", owners: [2]string{"", "owner"}},
		{name: "owner removed", owners: [2]string{"owner", ""}},
		{name: "owner renamed", owners: [2]string{"old", "new"}},
		{name: "owner renamed nested", owners: [2]string{"old", "new"}, nested: true},
	} {
		t.Run("named missing child/"+tc.name, func(t *testing.T) { e2ESelectedKustomizeRootDiffCase(t, &tc) })
	}
}

func e2ESelectedKustomizeRootDiffName(t *testing.T, tc *struct {
	name        string
	before      string
	after       string
	patch       bool
	sibling     bool
	plain       bool
	declaration bool
	list        bool
	want        map[string]string
},
) {
	t.Helper()

	orig, current := t.TempDir(), t.TempDir()
	e2ESelectedKustomizeRootDiffNameLoop(t, struct {
		orig    *string
		current *string
		tc      **struct {
			name        string
			before      string
			after       string
			patch       bool
			sibling     bool
			plain       bool
			declaration bool
			list        bool
			want        map[string]string
		}
	}{orig: &orig, current: &current, tc: &tc})
	if tc.list {
		empty := "apiVersion: v1\nkind: ConfigMapList\nitems: []\n"
		member := "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: config}, data: {value: selected}}]\n"
		before, after := empty, member
		if tc.want["config"] == "removed" {
			before, after = member, empty
		}
		testutil.WriteFile(t, orig, "app/cm.yaml", before)
		testutil.WriteFile(t, current, "app/cm.yaml", after)
	}
	if tc.after != "" {
		testutil.WriteFile(t, current, "app/kustomization.yaml", tc.after)
	}
	if tc.patch {
		harness.MutateFile(t, filepath.Join(current, "app", "patch.yaml"), "web:1", "web:2")
	}
	if tc.sibling {
		harness.MutateFile(t, filepath.Join(current, "app", "unlisted.yaml"), "value: before", "value: after")
	}
	flags := []string{
		"--path",
		filepath.Join(current, "app"),
		"--path-orig",
		filepath.Join(orig, "app"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	requireCLIOKResult1 := harness.RequireCLIOK(t, append([]string{"diff", "all", "-o", "json"}, flags...)...)
	out, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
	if tc.declaration {
		t.Logf("paired declaration diff: %s", out)
	}
	var result struct {
		Changes []struct {
			Name   string         `json:"Name"`
			Status string         `json:"Status"`
			Parent map[string]any `json:"Parent"`
		} `json:"Changes"`
		Suppressed []any `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	assert.FatalIf(
		t,
		len(result.Changes) != len(tc.want) || len(result.Suppressed) != 0,
		"unexpected paired diff: %s",
		out,
	)
	parent := map[string]any{"kind": "", "name": "", "path": "app"}
	if tc.plain {
		parent = map[string]any{
			"group":     "kustomize.toolkit.fluxcd.io",
			"kind":      manifest.KindKustomization,
			"namespace": "apps",
			"name":      "owner",
			"path":      "app",
		}
	}
	for _, change := range result.Changes {
		assert.FatalIf(
			t,
			tc.want[change.Name] != change.Status,
			"change did not retain resource identity: %s",
			out,
		)
		assert.Diff(t, change.Parent, parent)
	}
	if tc.list {
		requireCLIOKResult2 := harness.RequireCLIOK(
			t,
			"diff",
			"all",
			"-o",
			"json",
			"--path",
			filepath.Join(orig, "app"),
			"--path-orig",
			filepath.Join(current, "app"),
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		)
		reversed, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
		forward := result.Changes[0]
		assert.NoError(t, json.Unmarshal([]byte(reversed), &result))
		assert.FatalIf(
			t,
			len(result.Changes) != 1 || len(result.Suppressed) != 0,
			"reversed member diff: %s",
			reversed,
		)
		back := result.Changes[0]
		want := "added"
		if forward.Status == "added" {
			want = "removed"
		}
		assert.FatalIf(
			t,
			back.Name != forward.Name || back.Status != want || cmp.Diff(back.Parent, forward.Parent) != "",
			"member pairing did not reverse: %s / %s",
			out,
			reversed,
		)
	}
	if tc.patch {
		requireCLIOKResult3 := harness.RequireCLIOK(
			t,
			append([]string{"diff", "images", "--include-removed", "-o", "json"}, flags...)...)
		images, _ := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
		var got []string
		assert.NoError(t, json.Unmarshal([]byte(images), &got))
		{
			difference := cmp.Diff([]string{"example.test/web:1", "example.test/web:2"}, got)
			assert.FatalIf(t, difference != "", "ownerless image diff (-want +got):\n%s", difference)
		}
	}
}

func e2ESelectedKustomizeRootDiffName2(t *testing.T, tc *struct {
	name   string
	before []string
	after  []string
},
) {
	t.Helper()

	roots := [2]string{t.TempDir(), t.TempDir()}
	owners := [2][]string{tc.before, tc.after}
	producerCounts := [2]int{max(1, len(tc.before)), max(1, len(tc.after))}
	for side, root := range roots {
		testutil.GitInit(t, root)
		resources := []string{"cm.yaml"}
		for _, name := range owners[side] {
			resources = append(resources, name+".yaml")
			testutil.WriteFile(
				t,
				root,
				"app/"+name+".yaml",
				testutil.KustomizationYAML(name, "apps", "  path: ./app\n"),
			)
		}
		testutil.WriteFile(
			t,
			root,
			"app/kustomization.yaml",
			"resources: ["+strings.Join(resources, ", ")+"]\n",
		)
		value := [2]string{"before", "after"}[side]
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root-output, namespace: apps}\ndata: {value: "+value+"}\n",
		)
		requireCLIOKResult4 := harness.RequireCLIOK(
			t,
			"build",
			"all",
			"--path",
			filepath.Join(root, "app"),
			"--concurrency",
			"2",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
		)
		out, _ := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
		docs, err := testutil.DecodeDocs(strings.NewReader(out))
		assert.NoError(t, err)
		outputs := 0
		for _, doc := range docs {
			metadata := manifest.DocMetadata(doc)
			name := metadata.Name
			if manifest.DocKind(doc) == manifest.KindConfigMap && name == "root-output" {
				outputs++
				assert.Equal(t, doc["data"].(map[string]any)["value"], any(value))
			}
		}
		if outputs != producerCounts[side] {
			t.Errorf("side %d root inventory count=%d, want %d: %s", side, outputs, producerCounts[side], out)
		}
	}
	for _, direction := range []struct {
		name      string
		orig, cur int
	}{
		{name: "forward", orig: 0, cur: 1},
		{name: "reverse", orig: 1, cur: 0},
	} {
		t.Run(direction.name, func(t *testing.T) {
			e2ESelectedKustomizeRootDiffName2Name(t, &roots, &direction, struct {
				producerCounts *[2]int
				owners         *[2][]string
			}{producerCounts: &producerCounts, owners: &owners})
		})
	}
}

func e2ESelectedKustomizeRootDiffNameLoop(t *testing.T, loopInputs struct {
	orig    *string
	current *string
	tc      **struct {
		name        string
		before      string
		after       string
		patch       bool
		sibling     bool
		plain       bool
		declaration bool
		list        bool
		want        map[string]string
	}
},
) {
	t.Helper()

	for _, root := range []string{(*loopInputs.orig), (*loopInputs.current)} {
		testutil.GitInit(t, root)
		if !(*loopInputs.tc).plain {
			testutil.WriteFile(t, root, "app/kustomization.yaml", (*loopInputs.tc).before)
		} else {
			testutil.WriteFile(
				t,
				root,
				"app/owner.yaml",
				testutil.KustomizationYAML("owner", "apps", "  path: ./app\n"),
			)
		}
		testutil.WriteFile(
			t,
			root,
			"app/deployment.yaml",
			"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: web, image: 'example.test/web:1'}]}}}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"app/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: config}\ndata: {value: selected}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"app/unlisted.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\ndata: {value: before}\n",
		)
		if (*loopInputs.tc).declaration {
			testutil.WriteFile(
				t,
				root,
				"app/child.yaml",
				testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"),
			)
			testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")
		}
		if (*loopInputs.tc).patch {
			testutil.WriteFile(
				t,
				root,
				"app/patch.yaml",
				"apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: web, image: 'example.test/web:1'}]}}}\n",
			)
		}
	}
}

func e2ESelectedKustomizeRootDiffCase(t *testing.T, tc *struct {
	name   string
	owners [2]string
	nested bool
},
) {
	t.Helper()

	for _, direction := range []struct {
		name      string
		orig, cur int
	}{
		{name: "forward", orig: 0, cur: 1},
		{name: "reverse", orig: 1, cur: 0},
	} {
		t.Run(direction.name, func(t *testing.T) {
			roots := [2]string{t.TempDir(), t.TempDir()}
			for side, root := range roots {
				testutil.GitInit(t, root)
				resources := []string{"missing.yaml"}
				if side == direction.orig {
					resources = []string{"child.yaml"}
					declarations := "app"
					if tc.nested {
						resources = []string{"driver.yaml"}
						declarations = "declarations"
						testutil.WriteFile(
							t,
							root,
							"app/driver.yaml",
							testutil.KustomizationYAML("driver", "apps", "  path: ./declarations\n"),
						)
						testutil.WriteFile(
							t,
							root,
							"declarations/kustomization.yaml",
							"resources: [child.yaml]\n",
						)
					}
					testutil.WriteFile(
						t,
						root,
						declarations+"/child.yaml",
						testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"),
					)
					testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
					testutil.WriteFile(
						t,
						root,
						"leaf/cm.yaml",
						"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: apps}\n",
					)
				}
				if owner := tc.owners[side]; owner != "" {
					resources = append(resources, "owner.yaml")
					testutil.WriteFile(
						t,
						root,
						"app/owner.yaml",
						testutil.KustomizationYAML(owner, "apps", "  path: ./app\n"),
					)
				}
				testutil.WriteFile(
					t,
					root,
					"app/kustomization.yaml",
					"resources: ["+strings.Join(resources, ", ")+"]\n",
				)
			}
			runCLIBuffersResult5 := harness.RunCLIBuffers(
				"diff",
				"ks",
				"child",
				"-o",
				"json",
				"--path",
				filepath.Join(roots[direction.cur], "app"),
				"--path-orig",
				filepath.Join(roots[direction.orig], "app"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			out, stderr, code := runCLIBuffersResult5.Stdout, runCLIBuffersResult5.Stderr, runCLIBuffersResult5.Code
			assert.FatalIf(
				t,
				code != 1 || !strings.Contains(stderr, "missing.yaml"),
				"named child hid its failed entry: code=%d out=%s stderr=%s",
				code,
				out,
				stderr,
			)
			var result struct {
				Changes    []any `json:"Changes"`
				Suppressed []struct {
					Side   string `json:"Side"`
					Reason string `json:"Reason"`
					Count  int    `json:"Count"`
				} `json:"Suppressed"`
			}
			assert.NoError(t, json.Unmarshal([]byte(out), &result))
			assert.FatalIf(t, len(result.Changes) != 0 || len(result.Suppressed) != 1 ||
				result.Suppressed[0].Side != "current" ||
				result.Suppressed[0].Count != 1 ||
				!strings.Contains(
					result.Suppressed[0].Reason,
					"missing.yaml",
				), "named child lost opposite inventory or failure correspondence: %s", out)
		})
	}
}

func e2ESelectedKustomizeRootDiffName2Name(t *testing.T, roots *[2]string, direction *struct {
	name string
	orig int
	cur  int
}, capturedInputs struct {
	producerCounts *[2]int
	owners         *[2][]string
},
) {
	t.Helper()

	args := []string{
		"diff",
		"all",
		"-o",
		"json",
		"--path",
		filepath.Join(roots[direction.cur], "app"),
		"--path-orig",
		filepath.Join(roots[direction.orig], "app"),
		"--concurrency",
		"2",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
	}
	requireCLIOKResult6 := harness.RequireCLIOK(t, args...)
	out, _ := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
	var result struct {
		Changes []struct {
			Kind string `json:"Kind"`
			Name string `json:"Name"`
			Old  *struct {
				Data map[string]string `json:"Data"`
			} `json:"Old"`
			New *struct {
				Data map[string]string `json:"Data"`
			} `json:"New"`
		} `json:"Changes"`
		Suppressed []struct {
			Side   string `json:"Side"`
			Count  int    `json:"Count"`
			Reason string `json:"Reason"`
		} `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	values := [2]string{"before", "after"}
	oldCount, newCount := 0, 0
	for _, change := range result.Changes {
		if change.Kind != manifest.KindConfigMap || change.Name != "root-output" {
			continue
		}
		if change.Old != nil {
			oldCount++
			assert.Equal(t, change.Old.Data["value"], values[direction.orig])
		}
		if change.New != nil {
			newCount++
			assert.Equal(t, change.New.Data["value"], values[direction.cur])
		}
	}
	assert.ErrorIf(
		t,
		oldCount != capturedInputs.producerCounts[direction.orig] || newCount != capturedInputs.producerCounts[direction.cur] ||
			len(result.Suppressed) != 0,
		"successful entry edit missing: %s",
		out,
	)
	failedRoot := roots[direction.cur]
	builder := filepath.Join(failedRoot, "app", "kustomization.yaml")
	body, err := os.ReadFile(builder)
	assert.NoError(t, err)
	testutil.WriteFile(
		t,
		failedRoot,
		"app/kustomization.yaml",
		strings.Replace(string(body), "resources: [", "resources: [missing.yaml, ", 1),
	)
	defer testutil.WriteFile(t, failedRoot, "app/kustomization.yaml", string(body))
	runCLIBuffersResult7 := harness.RunCLIBuffers(args...)
	out, stderr, code := runCLIBuffersResult7.Stdout, runCLIBuffersResult7.Stderr, runCLIBuffersResult7.Code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "missing.yaml"),
		"failed entry lost its error: code=%d out=%s stderr=%s",
		code,
		out,
		stderr,
	)
	result.Changes, result.Suppressed = nil, nil
	assert.NoError(t, json.Unmarshal([]byte(out), &result))
	withheld := 0
	for _, item := range result.Suppressed {
		withheld += item.Count
		assert.FatalIf(
			t,
			item.Side != "current" || !strings.Contains(item.Reason, "missing.yaml"),
			"failed representative lost local attribution: %s",
			out,
		)
	}
	wantWithheld := capturedInputs.producerCounts[direction.orig] * (1 + len(capturedInputs.owners[direction.orig]))
	assert.FatalIf(
		t,
		len(result.Changes) != 0 || withheld != wantWithheld,
		"failed opposite entry exposed false changes or lost inventory: %s",
		out,
	)
}
