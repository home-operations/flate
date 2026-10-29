// Package substitution tests data reading, substitution and inherited patches.
package substitution

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_ParentPatchesPropagateToChildren(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName("main"))))
	_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/policy"}})
	assert.NoError(t, err)
	encode := func(value any) string {
		t.Helper()
		body, err := json.Marshal(value)
		assert.NoError(t, err)
		return string(body)
	}
	patch := func(kind, path string, value any) map[string]any {
		return map[string]any{"target": map[string]any{"kind": kind}, "patch": encode([]any{map[string]any{"op": "add", "path": path, "value": value}})}
	}
	policy := func(retries int) map[string]any {
		return map[string]any{
			"install":  map[string]any{"crds": "CreateReplace", "strategy": map[string]any{"name": "RemediateOnFailure"}, "remediation": map[string]any{"retries": retries}},
			"upgrade":  map[string]any{"crds": "CreateReplace", "remediation": map[string]any{"retries": retries, "strategy": "rollback", "remediateLastFailure": true}},
			"rollback": map[string]any{"cleanupOnFail": true, "timeout": "7m"},
		}
	}
	writeInstance := func(retries int) {
		t.Helper()
		hrPatch := map[string]any{"target": map[string]any{"kind": "HelmRelease"}, "patch": encode(map[string]any{"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease", "metadata": map[string]any{"name": "unused"}, "spec": policy(retries)})}
		childPatches := []any{hrPatch, patch("Kustomization", "/spec/patches/-", hrPatch)}
		rootPatches := []any{patch("ConfigMap", "/data/root", "root-policy")}
		for _, childPatch := range childPatches {
			rootPatches = append(rootPatches, patch("Kustomization", "/spec/patches/-", childPatch))
		}
		instance := map[string]any{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "FluxInstance", "metadata": map[string]any{"name": "flux", "namespace": "apps"}, "spec": map[string]any{
			"distribution": map[string]any{"version": "2.x"},
			"sync":         map[string]any{"name": "self", "kind": "GitRepository", "url": "https://example.test/policy", "ref": "refs/heads/main", "path": "./entry"},
			"kustomize":    map[string]any{"patches": []any{patch("Kustomization", "/spec/patches", rootPatches)}},
		}}
		testutil.WriteFile(t, root, "entry/instance.yaml", encode(instance))
	}
	writeInstance(2)
	for _, file := range []struct{ path, body string }{
		{"entry/kustomization.yaml", "resources: [instance.yaml, child.yaml, cm.yaml]\n"},
		{"entry/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: root-output, namespace: apps}\ndata: {}\n"},
		{"entry/child.yaml", testutil.KustomizationYAML("child", "apps", "  path: ./child\n  sourceRef: {kind: GitRepository, name: self}\n  patches: "+encode([]any{patch("ConfigMap", "/data/child", "authored"), patch("Kustomization", "/spec/patches/-", patch("Deployment", "/spec/replicas", 3))})+"\n")},
		{"child/kustomization.yaml", "resources: [grandchild.yaml, release.yaml, cm.yaml]\n"},
		{"child/grandchild.yaml", testutil.KustomizationYAML("grandchild", "apps", "  path: ./grandchild\n  sourceRef: {kind: GitRepository, name: self}\n  patches: "+encode([]any{patch("ConfigMap", "/data/grandchild", "authored")})+"\n")},
		{"child/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output, namespace: apps}\ndata: {}\n"},
		{"grandchild/kustomization.yaml", "resources: [release.yaml, cm.yaml, deploy.yaml]\n"},
		{"grandchild/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: grandchild-output, namespace: apps}\ndata: {}\n"},
		{"grandchild/deploy.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: workload, namespace: apps}\nspec:\n  replicas: 1\n  selector: {matchLabels: {app: workload}}\n  template:\n    metadata: {labels: {app: workload}}\n    spec: {containers: [{name: app, image: example.test/app:1}]}\n"},
		{"charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 1.0.0\n"},
		{"charts/app/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}-chart', namespace: apps}\n"},
	} {
		testutil.WriteFile(t, root, file.path, file.body)
	}
	for _, name := range []string{"child", "grandchild"} {
		testutil.WriteFile(t, root, name+"/release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: "+name+", namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/app\n      sourceRef: {kind: GitRepository, name: self, namespace: apps}\n")
	}
	testutil.GitCommitAll(t, repo, "initial policy", &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}})
	args := []string{"all", "--path", filepath.Join(root, "entry"), "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress"}
	before, _ := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
	writeInstance(5)
	after, _ := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
	again, _ := harness.RequireCLIOK(t, append([]string{"build"}, args...)...)
	assert.Equal(t, again, after)
	for side, output := range []string{before, after} {
		docs, err := testutil.DecodeDocs(strings.NewReader(output))
		assert.NoError(t, err)
		retries := 2
		if side == 1 {
			retries = 5
		}
		seen := map[string]bool{}
		for _, doc := range docs {
			name := doc["metadata"].(map[string]any)["name"].(string)
			if doc["kind"] == "HelmRelease" {
				spec := doc["spec"].(map[string]any)
				for key, want := range policy(retries) {
					assert.Equal(t, encode(spec[key]), encode(want))
				}
				seen[name] = true
			}
			if want, ok := map[string]map[string]any{"root-output": {"root": "root-policy"}, "child-output": {"child": "authored"}, "grandchild-output": {"grandchild": "authored"}}[name]; ok {
				assert.Diff(t, doc["data"], any(want))
				seen[name] = true
			}
			if doc["kind"] == "Deployment" {
				assert.Equal(t, encode(doc["spec"].(map[string]any)["replicas"]), "3")
				seen[name] = true
			}
		}
		assert.Diff(t, seen, map[string]bool{"child": true, "grandchild": true, "root-output": true, "child-output": true, "grandchild-output": true, "workload": true})
	}
	changed, _ := harness.RequireCLIOK(t, append(append([]string{"build"}, args...), "--base", "HEAD")...)
	assert.Equal(t, changed, after)
	var result struct {
		Changes []struct {
			Kind, Name string
			Old, New   map[string]any
		}
		Suppressed []any
	}
	output, _ := harness.RequireCLIOK(t, append(append([]string{"diff"}, args...), "--base", "HEAD", "-o", "json")...)
	assert.NoError(t, json.Unmarshal([]byte(output), &result))
	assert.Equal(t, len(result.Suppressed), 0)
	full, err := testutil.DecodeDocs(strings.NewReader(after))
	assert.NoError(t, err)
	releases := map[string]bool{}
	for _, change := range result.Changes {
		if change.Kind != "HelmRelease" {
			continue
		}
		releases[change.Name] = true
		for _, doc := range full {
			if doc["kind"] == change.Kind && doc["metadata"].(map[string]any)["name"] == change.Name {
				assert.Equal(t, encode(change.New), encode(doc))
			}
		}
	}
	assert.Diff(t, releases, map[string]bool{"child": true, "grandchild": true})
}

func TestE2E_SOPSValueWipedToPlaceholder(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "--concurrency", "2", "-o", "yaml")
	again := harness.RunCLIStdout(t, "build", "ks", "leaf", "-n", "flux-system",
		"--path", root, "--concurrency", "2", "-o", "yaml")
	if again != out {
		t.Fatal("SOPS substitution output changed between runs")
	}

	if !strings.Contains(out, "secret: ..PLACEHOLDER_MY_SECRET..") {
		t.Errorf("expected SOPS-derived MY_SECRET to substitute to its placeholder:\n%s", out)
	}
	if !strings.Contains(out, "value: from-cluster-settings") {
		t.Errorf("cleartext cluster-settings value should substitute normally:\n%s", out)
	}
}

func TestE2E_SubstituteDisabledAnnotation(t *testing.T) {
	src := harness.TestdataPath(t, "parent-patches")
	root := t.TempDir()
	testutil.CopyTree(t, src, root)

	out := harness.RunCLI(t, "test", "ks", "--path", root)

	leafLine := harness.MustExtractLine(t, out, "flux-system/leaf")
	if !strings.Contains(leafLine, "✓") {
		t.Errorf("leaf should pass — the script ConfigMap opts out of substitution; got: %s", leafLine)
	}
	if strings.Contains(out, "missing closing brace") {
		t.Errorf("substitute-disabled annotation should prevent the parse error:\n%s", out)
	}
}

func TestE2E_BootstrapGeneratorEnvAndFileSubstitution(t *testing.T) {
	root := t.TempDir()
	for _, file := range []struct{ path, body string }{
		{"kustomization.yaml", "namespace: flux-system\nresources: [bootstrap]\n"},
		{"bootstrap/kustomization.yaml", "resources: [ks.yaml]\ngeneratorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: bootstrap-values\n  envs: [../settings.env]\n  files: [FROM_FILE=../payload.txt]\n  literals: [FROM_LITERAL=literal-value]\n"},
		{"settings.env", "FROM_ENV=env-value\n"},
		{"payload.txt", "file-value"},
		{"bootstrap/ks.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: bootstrap-values}]\n")},
		{"apps/kustomization.yaml", "resources: [result.yaml]\n"},
		{"apps/result.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: result}
data:
  env: "${FROM_ENV}"
  file: "${FROM_FILE}"
  literal: "${FROM_LITERAL}"
`},
	} {
		testutil.WriteFile(t, root, file.path, file.body)
	}
	var previous string
	for _, concurrency := range []string{"2", "8"} {
		out := harness.RunCLIStdout(t, "build", "all", "--path", root, "--concurrency", concurrency, "--no-progress")
		for _, want := range []string{"env-value", "file-value", "literal-value"} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing substituted %q in %s", want, out)
			}
		}
		if previous != "" && out != previous {
			t.Fatal("generator substitution output differs across concurrency levels")
		}
		previous = out
	}
}

func TestE2E_SubstitutionPrecedence(t *testing.T) {
	root, external := t.TempDir(), t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n"))
	testutil.WriteFile(t, root, "flux/settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\ndata: {value: '${VALUE}'}\n")
	testutil.WriteFile(t, external, "settings.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: private-value}\n")
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			for range 2 {
				stdout, stderr := harness.RequireCLIOK(t, verb, "all", "--path", filepath.Join(root, "flux"), "--supply", filepath.Join(external, "settings.yaml"), "--concurrency=2", "--log-level=info")
				if strings.Count(stderr, "[SuppliedPrecedence]") != 1 || !strings.Contains(stderr, "ConfigMap/apps/settings: VALUE") || strings.Contains(stdout, "repository data takes precedence") || strings.Contains(stdout+stderr, "private-value") || strings.Contains(stderr, "level=WARN") {
					t.Fatalf("precedence destination: stdout=%s stderr=%s", stdout, stderr)
				}
				if verb == "build" && !strings.Contains(stdout, "value: repository") || verb == "test" && !strings.Contains(stdout, "passed") {
					t.Fatalf("precedence product: %s", stdout)
				}
			}
		})
	}
}

func TestE2E_UnappliedPackagePatches(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "unapplied", true: "applied"}[applied], func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			for _, file := range []struct{ path, body string }{
				{"cluster/app/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: app-cm
data:
  a: ${VAR_A}
  b: ${VAR_B}
`},
				{"cluster/app/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`},
				{"cluster/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: app
  namespace: flux-system
spec:
  interval: 5m
  path: ./cluster/app
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
  postBuild:
    substituteFrom:
      - kind: ConfigMap
        name: cluster-settings
`},
				{"cluster/vars/base.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  VAR_A: base-value
`},
				{"cluster/vars/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - base.yaml
patches:
  - path: patch.yaml
`},
				{"cluster/vars/patch.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-settings
  namespace: flux-system
data:
  VAR_B: patch-value
`},
			} {
				testutil.WriteFile(t, root, file.path, file.body)
			}
			if applied {
				testutil.WriteFile(t, root, "cluster/vars-ks.yaml", testutil.KustomizationYAML("vars", "flux-system", "  path: ./cluster/vars\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			}
			wantValues := []string{"a: base-value", "b: null"}
			if applied {
				wantValues[1] = "b: patch-value"
			}
			var previous string
			for range 2 {
				out := harness.RunCLIStdout(t, "build", "ks", "app", "--path", filepath.Join(root, "cluster"), "--concurrency", "2", "--no-progress")
				for _, want := range wantValues {
					if !strings.Contains(out, want) {
						t.Fatalf("missing %q in app output:\n%s", want, out)
					}
				}
				if previous != "" && out != previous {
					t.Fatal("equivalent renders differ")
				}
				previous = out
			}
		})
	}
}
