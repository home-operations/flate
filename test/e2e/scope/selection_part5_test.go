package scope_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2EMalformedManifestScopeName(t *testing.T, tc *struct {
	name      string
	graph     bool
	owned     bool
	shared    bool
	wide      bool
	suspended bool
	changed   bool
	generated bool
	external  bool
},
) {
	t.Helper()

	dir := t.TempDir()
	repo := testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "monitoring/blackbox-exporter.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: blackbox-exporter, namespace: flux-system}
spec:
  path: ./monitoring/blackbox-exporter/app
  sourceRef: {kind: GitRepository, name: flux-system}
brokenPointer: [ unclosed
`)
	if tc.graph {
		testutil.WriteFile(t, dir, "monitoring/kustomization.yaml", "resources:\n  - blackbox-exporter.yaml\n")
	}
	testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources:\n  - configmap.yaml\n")
	testutil.WriteFile(t, dir, "healthy/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: sibling-output}
data: {value: rendered}
`)
	scan := filepath.Join(dir, "monitoring")
	if tc.owned {
		scan = filepath.Join(dir, "flux")
		testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources:\n  - ks.yaml\n")
		owner := testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  path: ./monitoring\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		)
		if tc.suspended {
			owner = strings.Replace(owner, "path: ./monitoring", "path: ./monitoring\n  suspend: true", 1)
		}
		if tc.generated {
			owner = `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: owners, namespace: flux-system}
spec:
  resourcesTemplate: |
    ` + strings.ReplaceAll(strings.TrimSpace(owner), "\n", "\n    ") + "\n"
		}
		if tc.external {
			owner = strings.Replace(owner, "name: flux-system}", "name: external}", 1) + `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: flux-system}
spec: {url: 'https://fixture.invalid/other'}
`
		}
		testutil.WriteFile(t, dir, "flux/ks.yaml", owner+"---\n"+
			testutil.KustomizationYAML("sibling", "flux-system", `  path: ./healthy
  sourceRef: {kind: GitRepository, name: flux-system}
`)+"---\n"+
			testutil.KustomizationYAML("dependent", "flux-system", `  path: ./healthy
  dependsOn: [{name: owner}]
  sourceRef: {kind: GitRepository, name: flux-system}
`))
	}
	if tc.shared {
		testutil.WriteFile(
			t,
			dir,
			"flux/peer.yaml",
			testutil.KustomizationYAML(
				"peer",
				"flux-system",
				"  path: ./monitoring\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
		testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [ks.yaml, peer.yaml]\n")
	}
	if tc.wide {
		testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux]\n")
		scan = dir
	}
	args := []string{"build", "all", "--path", scan, "--no-progress", "--concurrency", "2"}
	if tc.changed {
		testutil.GitCommitAll(
			t,
			repo,
			"init",
			&gogit.CommitOptions{
				Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
			},
		)
		original := t.TempDir()
		testutil.CopyTree(t, dir, original)
		harness.MutateFile(t, filepath.Join(dir, "healthy/configmap.yaml"), "value: rendered", "value: changed")
		args = append(args, "--path-orig", original)
	}
	if tc.external {
		original := t.TempDir()
		testutil.CopyTree(t, dir, original)
		args = []string{
			"diff",
			"all",
			"--path",
			scan,
			"--path-orig",
			original,
			"--no-progress",
			"--concurrency",
			"2",
		}
	}
	stdout, stderr, code := harness.RunCLIBuffers(args...)
	if tc.external {
		assert.FatalIf(
			t,
			code != 0 || stdout != "" || strings.Contains(stderr, "decode"),
			"external-source owner failed the diff: exit=%d\n%s\n%s",
			code,
			stdout,
			stderr,
		)
		return
	}
	if tc.suspended || tc.changed {
		assert.FatalIf(
			t,
			code != 0 || !strings.Contains(stdout, "sibling-output") || strings.Contains(stderr, "decode"),
			"unreconciled owner failed the run: exit=%d\n%s\n%s",
			code,
			stdout,
			stderr,
		)
		assert.FatalIf(t, tc.changed &&
			(!strings.Contains(stdout, "value: changed") || strings.Contains(stdout, "name: blackbox-exporter")), "changed sibling did not render alone:\n%s", stdout)
		return
	}
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, "blackbox-exporter.yaml"),
		"expected exit 1 naming malformed file; got %d:\n%s",
		code,
		stderr,
	)
	e2EMalformedManifestScopeNameLoop(t, struct {
		tc **struct {
			name      string
			graph     bool
			owned     bool
			shared    bool
			wide      bool
			suspended bool
			changed   bool
			generated bool
			external  bool
		}
		stderr *string
		stdout *string
		code   *int
		scan   *string
	}{tc: &tc, stderr: &stderr, stdout: &stdout, code: &code, scan: &scan})
}

func e2ESelectedKustomizeRootUnsuspendedChild(t *testing.T, root *string, flags *[]string) {
	t.Helper()

	testutil.WriteFile(t, (*root), "app/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"))
	testutil.WriteFile(t, (*root), "leaf/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, (*root), "leaf/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: leaf-output}\n")
	harness.MutateFile(
		t,
		filepath.Join((*root), "app/kustomization.yaml"),
		"resources: [release.yaml, repository.yaml, child.yaml]",
		"resources: [release.yaml, repository.yaml, child.yaml, root.yaml]",
	)
	testutil.WriteFile(
		t,
		(*root),
		"app/root.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root-output, namespace: apps}\ndata: {value: root-value}\n",
	)
	out, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, (*flags)...)...)
	again, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, (*flags)...)...)
	assert.Equal(t, again, out)
	docs, err := testutil.DecodeDocs(strings.NewReader(out))
	assert.NoError(t, err)
	assert.FatalIf(t, len(docs) != 5, "child and root inventory count=%d, want 5: %s", len(docs), out)
	assert.Diff(t, docs[0], map[string]any{
		"apiVersion": "v1",
		"kind":       manifest.KindConfigMap,
		"metadata": map[string]any{
			"name": "leaf-output",
			"labels": map[string]any{
				manifest.FluxKustomizeDomain + "/name":      "child-dev",
				manifest.FluxKustomizeDomain + "/namespace": "apps",
			},
		},
	})
	rootMaps := 0
	for _, doc := range docs[1:] {
		if manifest.DocKind(doc) != manifest.KindConfigMap {
			continue
		}
		rootMaps++
		assert.Diff(t, doc, map[string]any{
			"apiVersion": "v1",
			"kind":       manifest.KindConfigMap,
			"metadata":   map[string]any{"name": "root-output-dev", "namespace": "apps"},
			"data":       map[string]any{"value": "root-value"},
		})
	}
	assert.Equal(t, rootMaps, 1)
	named, _ := harness.RequireCLIOK(t, append([]string{"build", "ks", "child-dev"}, (*flags)...)...)
	assert.FatalIf(
		t,
		!strings.Contains(named, "name: leaf-output") || strings.Contains(named, "name: podinfo-dev"),
		"named child selection mixed root siblings with descendants: %s",
		named,
	)
}

func e2ESelectedKustomizeRootExcludedEntryDependency(t *testing.T, root *string, flags *[]string) {
	t.Helper()

	testutil.WriteFile(
		t,
		(*root),
		"app/kustomization.yaml",
		"resources: [child.yaml, settings.yaml]\nnameSuffix: -dev\n",
	)
	testutil.WriteFile(
		t,
		(*root),
		"app/settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: resolved}\n",
	)
	testutil.WriteFile(t, (*root), "app/child.yaml", testutil.KustomizationYAML("child", "apps", `  path: ./leaf
  postBuild:
    substituteFrom: [{kind: ConfigMap, name: settings-dev}]
`))
	testutil.WriteFile(t, (*root), "leaf/kustomization.yaml", "resources: [account.yaml]\n")
	testutil.WriteFile(
		t,
		(*root),
		"leaf/account.yaml",
		"apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: visible, annotations: {value: '${VALUE}'}}\n",
	)
	args := append([]string{"build", "all", "--skip-kinds", "ConfigMap"}, (*flags)...)
	out, _ := harness.RequireCLIOK(t, args...)
	warm, _ := harness.RequireCLIOK(t, args...)
	assert.FatalIf(t, out != warm, "%v", "excluded dependency output changed on a cache hit")
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	for _, doc := range docs {
		assert.FatalIf(
			t,
			manifest.DocKind(doc) == manifest.KindConfigMap,
			"excluded entry dependency leaked into output: %v",
			doc,
		)
	}
	assert.FatalIf(
		t,
		!strings.Contains(out, "kind: ServiceAccount\n") || !strings.Contains(out, "value: resolved") ||
			!strings.Contains(
				out,
				"kustomize.toolkit.fluxcd.io/name: child-dev",
			),
		"excluded entry values lost admission or visible descendant stamping: %s",
		out,
	)
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
		harness.MutateFile(t, filepath.Join(current, "app/patch.yaml"), "web:1", "web:2")
	}
	if tc.sibling {
		harness.MutateFile(t, filepath.Join(current, "app/unlisted.yaml"), "value: before", "value: after")
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
	out, _ := harness.RequireCLIOK(t, append([]string{"diff", "all", "-o", "json"}, flags...)...)
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
		reversed, _ := harness.RequireCLIOK(
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
		images, _ := harness.RequireCLIOK(
			t,
			append([]string{"diff", "images", "--include-removed", "-o", "json"}, flags...)...)
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
		out, _ := harness.RequireCLIOK(
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
		docs, err := testutil.DecodeDocs(strings.NewReader(out))
		assert.NoError(t, err)
		outputs := 0
		for _, doc := range docs {
			name, _ := manifest.DocMetadata(doc)
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

func e2EMalformedManifestScopeNameLoop(t *testing.T, loopInputs struct {
	tc **struct {
		name      string
		graph     bool
		owned     bool
		shared    bool
		wide      bool
		suspended bool
		changed   bool
		generated bool
		external  bool
	}
	stderr *string
	stdout *string
	code   *int
	scan   *string
},
) {
	t.Helper()

	if (*loopInputs.tc).owned {
		wantDecodes := 1
		if (*loopInputs.tc).shared {
			wantDecodes++
		}
		{
			got := strings.Count((*loopInputs.stderr), "did not find expected")
			assert.FatalIf(t, got != wantDecodes, "decode error reported %d times:\n%s", got, (*loopInputs.stderr))
		}
		assert.FatalIf(
			t,
			strings.Contains((*loopInputs.stderr), "flate error: loader: decode"),
			"owned decode error escaped resource attribution:\n%s",
			(*loopInputs.stderr),
		)
		assert.FatalIf(
			t,
			!strings.Contains((*loopInputs.stdout), "sibling-output") || !strings.Contains((*loopInputs.stderr), "flux-system/owner") ||
				!strings.Contains(
					(*loopInputs.stderr),
					"flux-system/dependent",
				),
			"expected sibling output and scoped owner/dependent failures:\n%s\n%s",
			(*loopInputs.stdout),
			(*loopInputs.stderr),
		)
		assert.FatalIf(
			t,
			(*loopInputs.tc).shared && !strings.Contains((*loopInputs.stderr), "flux-system/peer"),
			"second owner escaped the malformed file failure:\n%s",
			(*loopInputs.stderr),
		)
		(*loopInputs.stdout), (*loopInputs.stderr), (*loopInputs.code) = harness.RunCLIBuffers(
			"build",
			"ks",
			"sibling",
			"--path",
			(*loopInputs.scan),
			"--no-progress",
			"--concurrency",
			"2",
		)
		assert.FatalIf(
			t,
			(*loopInputs.code) != 0 || !strings.Contains((*loopInputs.stdout), "sibling-output"),
			"healthy named sibling failed: exit=%d\n%s\n%s",
			(*loopInputs.code),
			(*loopInputs.stdout),
			(*loopInputs.stderr),
		)
	} else {
		want := "entry monitoring/blackbox-exporter.yaml: document 1"
		if (*loopInputs.tc).graph {
			want = "kustomize build monitoring"
		}
		assert.FatalIf(
			t,
			!strings.Contains((*loopInputs.stderr), want) ||
				strings.Contains((*loopInputs.stderr), "Kustomization entry"),
			"unowned malformed file lost its owning error:\n%s",
			(*loopInputs.stderr),
		)
	}
}
