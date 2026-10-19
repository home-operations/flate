package substitution

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func writeDependencyCluster(t *testing.T, root, expression string) {
	t.Helper()
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system",
		"  path: ./app\n  dependsOn: [{name: '"+expression+"'}]\n  postBuild: {substitute: {PREFIX: 'prod-'}}\n"))
	testutil.WriteFile(t, root, "flux/config.yaml", testutil.KustomizationYAML("prod-config", "flux-system", "  path: ./config\n"))
	testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: []\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {value: before}\n")
}

func TestE2E_DependsOnDefaults_SuppliedValues(t *testing.T) {
	for _, expression := range []string{"${PREFIX:-}config", "${PREFIX:-dev-}config"} {
		t.Run(expression, func(t *testing.T) {
			root := t.TempDir()
			writeDependencyCluster(t, root, expression)
			var previous string
			for range 2 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
				if code != 0 || !strings.Contains(stdout, "name: consumer-output") {
					t.Fatalf("resolved dependency must render consumer: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				if previous != "" && stdout != previous {
					t.Fatalf("output differs between runs: first=%s second=%s", previous, stdout)
				}
				previous = stdout
			}
		})
	}
}

func TestE2E_DependsOnDefaults_ChangedOnly(t *testing.T) {
	root := t.TempDir()
	writeDependencyCluster(t, root, "${PREFIX:-}config")
	testutil.WriteFile(t, root, "flux/unrelated.yaml", testutil.KustomizationYAML("unrelated", "flux-system", "  path: ./unrelated\n"))
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated-output}\n")
	testutil.WriteFile(t, root, "notes.txt", "before\n")
	repo := testutil.GitInit(t, root)
	testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output}\ndata: {value: after}\n")
	testutil.WriteFile(t, root, "notes.txt", "unrelated dirty file\n")
	for _, mode := range []string{"build", "diff"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{mode, "all", "--path", filepath.Join(root, "flux"), "--base", "HEAD", "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress"}
			if mode == "diff" {
				args = append(args, "-o", "diff")
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			if code != 0 || !strings.Contains(stdout, "value: after") || strings.Contains(stdout, "unrelated-output") {
				t.Fatalf("changed consumer selection: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			if mode == "diff" && !strings.Contains(stdout, "value: before") {
				t.Fatalf("base side missing from diff: %s", stdout)
			}
		})
	}
}

func TestE2E_DependsOnDefaults_EmittedChild(t *testing.T) {
	for _, supplied := range []bool{false, true} {
		name := "default-only"
		target := "dev-config"
		if supplied {
			name, target = "discovered-supplier", "prod-config"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "flux-system", `  path: ./children
  patches:
  - target: {kind: Kustomization, name: child}
    patch: |-
      - op: add
        path: /spec/dependsOn
        value: [{name: '${PREFIX:-dev-}config'}]
`))
			testutil.WriteFile(t, root, "flux/config.yaml", testutil.KustomizationYAML(target, "flux-system", "  path: ./config\n"))
			testutil.WriteFile(t, root, "config/kustomization.yaml", "resources: []\n")
			if supplied {
				testutil.WriteFile(t, root, "flux/supplier.yaml", testutil.KustomizationYAML("supplier", "flux-system", "  path: ./settings\n  postBuild: {substitute: {PREFIX: 'prod-'}}\n"))
				testutil.WriteFile(t, root, "settings/kustomization.yaml", "resources: []\n")
			}
			testutil.WriteFile(t, root, "children/kustomization.yaml", "resources: [child.yaml]\n")
			testutil.WriteFile(t, root, "children/child.yaml", testutil.KustomizationYAML("child", "flux-system", "  path: ./app\n"))
			testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: emitted-output}\n")
			stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
			if code != 0 || !strings.Contains(stdout, "name: emitted-output") {
				t.Fatalf("emitted child must resolve dependency on first admission: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestE2E_DependsOnDefaults_EmptyNameFails(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./app\n  dependsOn: [{name: '${PREFIX:-}'}]\n"))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: should-not-render}\n")
	stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--concurrency", "2", "--cache-dir", t.TempDir(), "--no-progress")
	if code != 1 || stdout != "" || !strings.Contains(stderr, "required by flux-system/consumer") || !strings.Contains(stderr, "not found") {
		t.Fatalf("resolved-empty dependency must fail normally: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}
