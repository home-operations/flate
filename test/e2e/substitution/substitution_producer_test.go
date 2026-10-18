package substitution

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_SubstitutionProducerRendering(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		modes := []string{"full", "direct", "inherited"}
		if kind == "ConfigMap" {
			modes = append(modes, "env", "path-orig", "diff")
		}
		for _, mode := range modes {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				root := t.TempDir()
				testutil.WriteSubstitutionProducerCluster(t, root, kind)
				testutil.WriteFile(t, root, "flux/unrelated.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: unrelated, namespace: flux-system}
spec:
  path: ./unrelated
  sourceRef: {kind: GitRepository, name: flux-system}
`)
				testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [deployment.yaml]\n")
				testutil.WriteFile(t, root, "unrelated/deployment.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: unrelated-workload}\n")
				consumer, err := os.ReadFile(filepath.Join(root, "flux/consumer.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				postBuild := "  postBuild: {substituteFrom: [{kind: " + kind + ", name: settings}]}\n"
				if mode == "inherited" {
					testutil.WriteFile(t, root, "children/consumer.yaml", strings.Replace(string(consumer), postBuild, "", 1))
					testutil.WriteFile(t, root, "children/kustomization.yaml", "resources: [consumer.yaml]\n")
					testutil.WriteFile(t, root, "flux/consumer.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: parent, namespace: flux-system}
spec:
  path: ./children
  sourceRef: {kind: GitRepository, name: flux-system}
  dependsOn: [{name: producer}]
  postBuild: {substituteFrom: [{kind: `+kind+`, name: settings, optional: true}]}
  patches:
  - target: {kind: Kustomization}
    patch: |-
      apiVersion: kustomize.toolkit.fluxcd.io/v1
      kind: Kustomization
      metadata: {name: ignored}
      spec:
        postBuild: {substituteFrom: [{kind: `+kind+`, name: settings, optional: true}]}
`)
				} else {
					testutil.WriteFile(t, root, "flux/consumer.yaml", strings.Replace(string(consumer), postBuild,
						"  dependsOn: [{name: producer}]\n  postBuild: {substituteFrom: [{kind: "+kind+", name: settings, optional: true}]}\n", 1))
				}
				const deployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: output}
spec:
  selector: {matchLabels: {app: output}}
  template:
    metadata: {labels: {app: output}}
    spec:
      containers:
      - name: app
        image: example.invalid/app:${VERSION:=empty}-old
`
				if mode == "diff" {
					testutil.WriteFile(t, root, "app/cm.yaml", deployment)
				}
				var original string
				if mode == "path-orig" {
					original = harness.CopyTree(t, root)
				}
				repo := harness.GitInit(t, root)
				harness.GitCommitAll(t, repo)
				if mode == "diff" {
					testutil.WriteFile(t, root, "app/cm.yaml", strings.Replace(deployment, "-old", "-new", 1))
				} else {
					testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, labels: {changed: consumer}}\ndata: {version: '${VERSION:=empty}'}\n")
				}
				wt, err := repo.Worktree()
				if err != nil {
					t.Fatal(err)
				}
				status, err := wt.Status()
				if err != nil {
					t.Fatal(err)
				}
				if _, dirty := status["app/cm.yaml"]; len(status) != 1 || !dirty {
					t.Fatalf("consumer-only change: %v", status)
				}
				args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress"}
				switch mode {
				case "full":
				case "env":
					t.Setenv("FLATE_BASE", "HEAD")
				case "path-orig":
					args = append(args, "--path-orig", original)
				default:
					args = append(args, "--base", "HEAD")
				}
				if mode == "diff" {
					args[0] = "diff"
					args = append(args, "-o", "diff")
				}
				stdout, stderr, code := harness.RunCLIBuffers(args...)
				if mode != "full" && strings.Contains(stdout, "unrelated-workload") {
					t.Fatalf("unchanged workload retained: stdout=%s stderr=%s", stdout, stderr)
				}
				if mode == "diff" {
					if code != 0 || !strings.Contains(stdout, "image: example.invalid/app:4.6.2-old") || !strings.Contains(stdout, "image: example.invalid/app:4.6.2-new") {
						t.Fatalf("diff substitution: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
					}
				} else if code != 0 || !strings.Contains(stdout, "version: 4.6.2") || strings.Contains(stdout, "version: discovered") {
					t.Fatalf("rendered substitution: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			})
		}
	}
}

func TestE2E_SubstitutionProducerCycle(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionCycleCluster(t, root)
	const want = "\n  ✗  Kustomization.kustomize.toolkit.fluxcd.io  flux-system/a  dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/flux-system/b\n  ✗  Kustomization.kustomize.toolkit.fluxcd.io  flux-system/b  dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/flux-system/a\n\n  ✗ 2 failed\n"
	for _, workers := range []string{"1", "2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 15 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || stdout != "" || stderr != want {
					t.Fatalf("substitution cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
			}
		})
	}
}

func TestE2E_SubstitutionProducerTransformedNamespace(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		version := "4.6.2"
		if kind == "Secret" {
			version = "NC42LjI="
		}
		for _, mode := range []string{"full", "changed"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				root, original := t.TempDir(), t.TempDir()
				for _, dir := range []string{root, original} {
					for _, ks := range []struct{ name, path, postBuild string }{
						{"producer", "data", ""},
						{"consumer", "app", "  postBuild: {substituteFrom: [{kind: " + kind + ", name: settings}]}\n"},
					} {
						testutil.WriteFile(t, dir, "flux/"+ks.name+".yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: "+ks.name+", namespace: flux-system}\nspec:\n  path: ./"+ks.path+"\n"+ks.postBuild)
					}
					testutil.WriteFile(t, dir, "data/kustomization.yaml", "namespace: flux-system\nresources: [settings.yaml]\n")
					testutil.WriteFile(t, dir, "data/settings.yaml", "apiVersion: v1\nkind: "+kind+"\nmetadata: {name: settings, namespace: original}\ndata: {VERSION: "+version+"}\n")
					testutil.WriteFile(t, dir, "app/kustomization.yaml", "namespace: flux-system\nresources: [cm.yaml]\n")
					testutil.WriteFile(t, dir, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {version: '${VERSION:=empty}'}\n")
				}
				testutil.WriteFile(t, original, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\ndata: {version: old}\n")
				args := []string{"build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress"}
				if mode == "changed" {
					args = append(args, "--path-orig", original)
				}
				stdout, stderr, code := harness.RunCLIBuffers(args...)
				if code != 0 || !strings.Contains(stdout, "version: 4.6.2") {
					t.Fatalf("transformed substitution: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
			})
		}
	}
}

func TestE2E_SubstitutionCycleDownstream(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionCycleCluster(t, root)
	testutil.WriteFile(t, root, "flux/consumer.yaml", "apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: consumer, namespace: flux-system}\nspec:\n  path: ./app\n  dependsOn: [{name: a}]\n")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n")
	var want string
	for _, workers := range []string{"2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 15 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || stdout != "" || strings.Contains(stderr, "flux-system/consumer  dependency cycle:") ||
					!strings.Contains(stderr, "blocks flux-system/consumer") || !strings.Contains(stderr, "2 failed · 1 blocked") {
					t.Fatalf("downstream cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
				if want != "" && stderr != want {
					t.Fatalf("unstable cycle report: want=%q got=%q", want, stderr)
				}
				want = stderr
			}
		})
	}
}
