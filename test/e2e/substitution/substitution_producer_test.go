package substitution

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
				testutil.WriteFile(t, root, "flux/unrelated.yaml", testutil.KustomizationYAML("unrelated", "flux-system", "  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
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
					testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("parent", "flux-system", `  path: ./children
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
        postBuild: {substituteFrom: [{kind: `+kind+", name: settings, optional: true}]}\n"))
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
					original = t.TempDir()
					testutil.CopyTree(t, root, original)
				}
				repo := testutil.GitInit(t, root)
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
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
	const want = "\n  ✗  Kustomization  flux-system/a  dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/flux-system/b\n  ✗  Kustomization  flux-system/b  dependency cycle: blocked by Kustomization.kustomize.toolkit.fluxcd.io/flux-system/a\n\n  ✗ 2 failed\n"
	for _, workers := range []string{"1", "2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 2 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || stderr != want {
					t.Fatalf("substitution cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
				docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
				if err != nil || len(docs) != 2 {
					t.Fatalf("expected two root declarations: %s (decode: %v)", stdout, err)
				}
				for _, doc := range docs {
					name, namespace := manifest.DocMetadata(doc)
					if manifest.DocKind(doc) != "Kustomization" || namespace != "flux-system" || name != "a" && name != "b" {
						t.Fatalf("cycle emitted workload output: %s", stdout)
					}
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
						testutil.WriteFile(t, dir, "flux/"+ks.name+".yaml", testutil.KustomizationYAML(ks.name, "flux-system", "  path: ./"+ks.path+"\n"+ks.postBuild))
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
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [a.yaml, b.yaml, consumer.yaml]\n")
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "flux-system", "  path: ./app\n  dependsOn: [{name: a}]\n"))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "app/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n")
	var want string
	for _, workers := range []string{"2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 2 {
				stdout, stderr, code := harness.RunCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || strings.Contains(stderr, "flux-system/consumer  dependency cycle:") ||
					!strings.Contains(stderr, "blocks flux-system/consumer") || !strings.Contains(stderr, "2 failed · 1 blocked") {
					t.Fatalf("downstream cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
				docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
				if err != nil || len(docs) != 3 {
					t.Fatalf("expected three root declarations: %s (decode: %v)", stdout, err)
				}
				for _, doc := range docs {
					name, namespace := manifest.DocMetadata(doc)
					if manifest.DocKind(doc) != "Kustomization" || namespace != "flux-system" || name != "a" && name != "b" && name != "consumer" {
						t.Fatalf("blocked consumer emitted workload output: %s", stdout)
					}
				}
				if want != "" && stderr != want {
					t.Fatalf("unstable cycle report: want=%q got=%q", want, stderr)
				}
				want = stderr
			}
		})
	}
}

func TestE2E_DependencyCycleWarnings(t *testing.T) {
	root := t.TempDir()
	for _, pair := range []struct{ name, peer string }{{"a", "b"}, {"b", "a"}} {
		testutil.WriteFile(t, root, "flux/"+pair.name+".yaml", testutil.KustomizationYAML(pair.name, "flux-system", "  path: ./"+pair.name+"\n  dependsOn: [{name: "+pair.peer+"}]\n"))
	}
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			var previous string
			for range 2 {
				stdout, stderr, code := harness.RunCLIBuffers(verb, "all", "--path", root, "--concurrency=2")
				if code != 1 || strings.Count(stderr, "[DependencyCycle]") != 1 || !strings.Contains(stdout+stderr, "dependency cycle") || !strings.Contains(stdout+stderr, "2 failed") || strings.Contains(stdout, "warnings (") {
					t.Fatalf("cycle: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				if verb == "build" {
					docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
					if err != nil || len(docs) != 2 {
						t.Fatalf("expected two root declarations: %s (decode: %v)", stdout, err)
					}
					for _, doc := range docs {
						name, namespace := manifest.DocMetadata(doc)
						if manifest.DocKind(doc) != "Kustomization" || namespace != "flux-system" || name != "a" && name != "b" {
							t.Fatalf("cycle emitted workload output: %s", stdout)
						}
					}
				}
				if previous != "" && stderr != previous {
					t.Fatal("cycle warning changed")
				}
				previous = stderr
			}
		})
	}
}

func TestE2E_LiteralKustomizationPath(t *testing.T) {
	for _, mode := range []string{"full-build", "own-postBuild", "changed-only", "paired-diff", "reverse-diff"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			declaration := "root.yaml"
			selected := root
			if mode != "full-build" && mode != "own-postBuild" {
				declaration = "flux/root.yaml"
				selected = filepath.Join(root, "flux")
			}
			spec := "  path: ./apps/${ENV:=prod}\n  sourceRef: {kind: GitRepository, name: flux-system}\n"
			if mode == "own-postBuild" {
				spec += "  postBuild: {substitute: {ENV: stage}}\n"
			}
			testutil.WriteFile(t, root, declaration, testutil.KustomizationYAML("root", "apps", spec))
			testutil.WriteFile(t, root, "apps/${ENV:=prod}/kustomization.yaml", "resources: [cm.yaml]\n")
			const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: correct, namespace: apps}\ndata: {answer: %s}\n"
			before, after := "before", "literal"
			if mode == "reverse-diff" {
				before, after = after, before
			}
			if selected != root {
				testutil.WriteFile(t, root, "flux/unrelated.yaml", testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n"))
				testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated, namespace: apps}\n")
				testutil.WriteFile(t, root, "apps/${ENV:=prod}/cm.yaml", fmt.Sprintf(cm, before))
				repo := testutil.GitInit(t, root)
				testutil.GitCommitAll(t, repo, "init", &gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}})
			}
			testutil.WriteFile(t, root, "apps/${ENV:=prod}/cm.yaml", fmt.Sprintf(cm, after))
			args := []string{"build", "all", "--path", selected, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress"}
			if selected != root {
				args = append(args, "--base", "HEAD")
			}
			if strings.HasSuffix(mode, "diff") {
				args[0] = "diff"
				args = append(args, "-o", "diff")
			}
			var previous string
			for run := range 2 {
				stdout, stderr, code := harness.RunCLIBuffers(args...)
				if code != 0 || !strings.Contains(stdout, "correct") || !strings.Contains(stdout, "apps") || !strings.Contains(stdout, "literal") {
					t.Fatalf("literal path: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
				}
				if args[0] == "build" {
					docs, err := testutil.DecodeDocs(strings.NewReader(stdout))
					if err != nil {
						t.Fatal(err)
					}
					for _, doc := range docs {
						name, _ := manifest.DocMetadata(doc)
						if manifest.DocKind(doc) == "ConfigMap" && name == "unrelated" {
							t.Fatalf("unchanged sibling workload rendered: %s", stdout)
						}
					}
				} else if strings.Contains(stdout, "unrelated") {
					t.Fatalf("unchanged sibling changed: %s", stdout)
				}
				if args[0] == "build" && (!strings.Contains(stdout, "name: correct") || !strings.Contains(stdout, "namespace: apps") || !strings.Contains(stdout, "answer: literal")) {
					t.Fatalf("missing intended ConfigMap: stdout=%s stderr=%s", stdout, stderr)
				}
				if strings.HasSuffix(mode, "diff") && !strings.Contains(stdout, "before") {
					t.Fatalf("missing diff side: stdout=%s stderr=%s", stdout, stderr)
				}
				if run > 0 && stdout != previous {
					t.Fatalf("unstable output: first=%s second=%s", previous, stdout)
				}
				previous = stdout
			}
		})
	}
}
