package scope

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_CrossSourceOptionalInputRemoval(t *testing.T) {
	roots := testutil.CrossSourceOptionalInputs(t)
	flags := []string{"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "--log-level", "debug"}
	var rendered [2]string
	for side, root := range roots {
		full, _ := harness.RequireCLIOK(t, append([]string{"build", "all", "--path", filepath.Join(root, "flux")}, flags...)...)
		want := []string{"available", "fallback"}[side]
		if !strings.Contains(full, "value: "+want) || !strings.Contains(full, "sentinel: active") || strings.Contains(full, "name: optional") != (side == 0) {
			t.Fatalf("side %d full render lost consumer substitution or supplying input: %s", side, full)
		}
		rendered[side] = t.TempDir()
		testutil.GitInit(t, rendered[side])
		for _, name := range []string{"app", "inputs"} {
			directory := name
			if name == "app" {
				directory = "apps/app"
			}
			testutil.WriteFile(t, rendered[side], "flux/"+name+".yaml", testutil.KustomizationYAML(name, "flux-system", "  path: ./"+directory+"\n"))
			output := ""
			for doc := range strings.SplitSeq(full, "---\n") {
				if strings.Contains(doc, "kustomize.toolkit.fluxcd.io/name: "+name+"\n") {
					output += "---\n" + doc
				}
			}
			resources := "resources: []\n"
			if output != "" {
				resources = "resources: [output.yaml]\n"
				testutil.WriteFile(t, rendered[side], directory+"/output.yaml", output)
			}
			testutil.WriteFile(t, rendered[side], directory+"/kustomization.yaml", resources)
		}
	}
	oracleArgs := append([]string{"diff", "all", "--path", filepath.Join(rendered[1], "flux"), "--path-orig", filepath.Join(rendered[0], "flux"), "-o", "diff"}, flags...)
	oracle, _ := harness.RequireCLIOK(t, oracleArgs...)
	args := append([]string{"diff", "all", "--path", filepath.Join(roots[1], "flux"), "--path-orig", filepath.Join(roots[0], "flux"), "-o", "diff"}, flags...)
	var previous string
	for run := range 2 {
		scoped, stderr := harness.RequireCLIOK(t, args...)
		if scoped != oracle || !strings.Contains(scoped, "-  value: available") || !strings.Contains(scoped, "+  value: fallback") || !strings.Contains(scoped, "name: optional") {
			t.Fatalf("cross-source removal differs from full-render oracle: scoped=%s\nfull=%s\nstderr=%s", scoped, oracle, stderr)
		}
		selected := 0
		for line := range strings.SplitSeq(stderr, "\n") {
			if strings.Contains(line, "changed-only keep set") && strings.Contains(line, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/app") {
				selected++
			}
		}
		if selected != 2 {
			t.Fatalf("consumer must be selected on both sides, got %d: %s", selected, stderr)
		}
		if run == 1 && scoped != previous {
			t.Fatalf("cross-source removal output changed between runs: %s\n%s", previous, scoped)
		}
		previous = scoped
	}
}

func TestE2E_OptionalInputChangedOnly(t *testing.T) {
	for _, route := range []string{"directory", "committed"} {
		t.Run(route, func(t *testing.T) {
			root, before := t.TempDir(), t.TempDir()
			repo := writeOptionalInputCLICluster(t, root, true)
			options := &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}}
			base := testutil.GitCommitAll(t, repo, "base", options)
			testutil.CopyTree(t, root, before)
			testutil.WriteFile(t, root, "flux/optional.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: available}\n")
			baseline := []string{"--path-orig", filepath.Join(before, "flux")}
			if route == "committed" {
				testutil.GitCommitAll(t, repo, "input appears", options)
				baseline = []string{"--base", base.String()}
			}
			args := []string{"build", "all", "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "--log-level", "debug"}
			full, _ := harness.RequireCLIOK(t, args...)
			scopedArgs := append(args, baseline...)
			scoped, stderr := harness.RequireCLIOK(t, scopedArgs...)
			if scoped != full || !strings.Contains(scoped, "value: available") {
				t.Fatalf("scoped/full output mismatch: scoped=%s\nfull=%s\nstderr=%s", scoped, full, stderr)
			}
			for _, id := range []string{"Kustomization.kustomize.toolkit.fluxcd.io/flux-system/app", "GitRepository.source.toolkit.fluxcd.io/flux-system/cluster"} {
				found := false
				for line := range strings.SplitSeq(stderr, "\n") {
					found = found || strings.Contains(line, "changed-only keep set") && strings.Contains(line, id)
				}
				if !found {
					t.Errorf("selection diagnostic omitted %s: %s", id, stderr)
				}
			}
			second, _ := harness.RequireCLIOK(t, scopedArgs...)
			if second != scoped {
				t.Fatalf("scoped output changed between runs: %s\n%s", scoped, second)
			}
		})
	}
}

func TestE2E_OptionalInputDiffSides(t *testing.T) {
	for _, operation := range []string{"appear", "remove", "projected appear", "projected remove", "required removal"} {
		t.Run(operation, func(t *testing.T) {
			before, after := t.TempDir(), t.TempDir()
			for _, root := range []string{before, after} {
				writeOptionalInputCLICluster(t, root, operation != "required removal")
				if strings.HasPrefix(operation, "projected") {
					cluster, err := os.ReadFile(filepath.Join(root, "flux/cluster.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					testutil.WriteFile(t, root, "flux/cluster.yaml", string(cluster)+"  dependsOn: [{name: inputs}]\n")
					testutil.WriteFile(t, root, "flux/inputs.yaml", testutil.KustomizationYAML("inputs", "flux-system", "  path: ./inputs\n  targetNamespace: flux-system\n  sourceRef: {kind: GitRepository, name: cluster}\n"))
					testutil.WriteFile(t, root, "inputs/kustomization.yaml", "resources: []\n")
					testutil.WriteFile(t, root, "flux/other.yaml", testutil.KustomizationYAML("other", "other", "  path: ./apps/other\n  sourceRef: {kind: GitRepository, name: cluster, namespace: flux-system}\n  postBuild:\n    substitute: {SENTINEL: active}\n    substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]\n"))
					testutil.WriteFile(t, root, "apps/other/kustomization.yaml", "resources: [output.yaml]\n")
					testutil.WriteFile(t, root, "apps/other/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unaffected, namespace: other}\ndata: {value: '${VALUE:-fallback}'}\n")
				}
			}
			present := after
			oldValue, newValue := "fallback", "available"
			if !strings.HasSuffix(operation, "appear") {
				present, oldValue, newValue = before, "available", "fallback"
			}
			inputPath, namespace := "flux/optional.yaml", ", namespace: flux-system"
			if strings.HasPrefix(operation, "projected") {
				inputPath, namespace = "inputs/optional.yaml", ""
				testutil.WriteFile(t, present, "inputs/kustomization.yaml", "resources: [optional.yaml]\n")
			}
			testutil.WriteFile(t, present, inputPath, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional"+namespace+"}\ndata: {VALUE: available}\n")
			flags := []string{"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}
			args := append([]string{"diff", "all", "--path", filepath.Join(after, "flux"), "--path-orig", filepath.Join(before, "flux"), "-o", "diff"}, flags...)
			if operation == "required removal" {
				_, stderr, code := harness.RunCLIBuffers(args...)
				if code != 1 || !strings.Contains(stderr, "optional") {
					t.Fatalf("required removal exit=%d stderr=%s", code, stderr)
				}
				return
			}
			var rendered [2]string
			for i, root := range []string{before, after} {
				buildArgs := append([]string{"build", "all", "--path", filepath.Join(root, "flux")}, flags...)
				full, _ := harness.RequireCLIOK(t, buildArgs...)
				rendered[i] = t.TempDir()
				testutil.GitInit(t, rendered[i])
				testutil.WriteFile(t, rendered[i], "flux/app.yaml", testutil.KustomizationYAML("app", "flux-system", "  path: ./apps/app\n"))
				testutil.WriteFile(t, rendered[i], "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
				appOutput, inputOutput := "", ""
				for doc := range strings.SplitSeq(full, "---\n") {
					if strings.Contains(doc, "kustomize.toolkit.fluxcd.io/name: inputs") {
						inputOutput += "---\n" + doc
					} else if doc != "" {
						appOutput += "---\n" + doc
					}
				}
				testutil.WriteFile(t, rendered[i], "apps/app/output.yaml", appOutput)
				if strings.HasPrefix(operation, "projected") {
					testutil.WriteFile(t, rendered[i], "flux/inputs.yaml", testutil.KustomizationYAML("inputs", "flux-system", "  path: ./inputs\n"))
					testutil.WriteFile(t, rendered[i], "inputs/kustomization.yaml", "resources: []\n")
					if inputOutput != "" {
						testutil.WriteFile(t, rendered[i], "inputs/kustomization.yaml", "resources: [output.yaml]\n")
						testutil.WriteFile(t, rendered[i], "inputs/output.yaml", inputOutput)
					}
				}
				if root == after && !strings.Contains(full, "value: "+newValue) {
					t.Fatalf("current full build did not apply substitution/fallback: %s", full)
				}
			}
			fullArgs := append([]string{"diff", "all", "--path", filepath.Join(rendered[1], "flux"), "--path-orig", filepath.Join(rendered[0], "flux"), "-o", "diff"}, flags...)
			fullDiff, _ := harness.RequireCLIOK(t, fullArgs...)
			scoped, stderr := harness.RequireCLIOK(t, args...)
			if scoped != fullDiff || !strings.Contains(scoped, "-  value: "+oldValue) || !strings.Contains(scoped, "+  value: "+newValue) {
				t.Fatalf("paired diff mismatch: scoped=%s\nfull=%s\nstderr=%s", scoped, fullDiff, stderr)
			}
			second, _ := harness.RequireCLIOK(t, args...)
			if second != scoped {
				t.Fatalf("diff changed between runs: %s\n%s", scoped, second)
			}
		})
	}
}

func writeOptionalInputCLICluster(t *testing.T, root string, optional bool) *git.Repository {
	t.Helper()
	repo := testutil.GitInit(t, root)
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{"https://example.test/cluster.git"}}); err != nil {
		t.Fatal(err)
	}
	optionalValue := "true"
	if !optional {
		optionalValue = "false"
	}
	testutil.WriteFile(t, root, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: cluster, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  path: ./apps/app
  sourceRef: {kind: GitRepository, name: cluster}
  postBuild:
    substitute: {SENTINEL: active}
    substituteFrom: [{kind: ConfigMap, name: optional, optional: `+optionalValue+`}]
`)
	testutil.WriteFile(t, root, "apps/app/kustomization.yaml", "resources: [output.yaml]\n")
	testutil.WriteFile(t, root, "apps/app/output.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: rendered, namespace: flux-system}
data: {value: "${VALUE:-fallback}", sentinel: "${SENTINEL}"}
`)
	return repo
}
