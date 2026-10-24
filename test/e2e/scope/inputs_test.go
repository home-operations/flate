package scope

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func TestE2E_DiffKustomizeEscapedInputs(t *testing.T) {
	for _, tc := range []struct {
		name, overlay string
		mixed, patch  bool
	}{
		{name: "implicit shallow", overlay: "deploy/nested"},
		{name: "implicit deep", overlay: "deploy/deep/nested"},
		{name: "mixed", overlay: "deploy/deep/nested", mixed: true},
		{name: "explicit", overlay: "deploy"},
		{name: "patch only", overlay: "deploy", patch: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml]\n")
			testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "flux-system", "  path: ./deploy\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			if tc.mixed {
				testutil.WriteFile(t, root, "deploy/sibling.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\ndata: {value: unchanged}\n")
			}
			resource, err := filepath.Rel(tc.overlay, "shared")
			if err != nil {
				t.Fatal(err)
			}
			testutil.WriteFile(t, root, tc.overlay+"/kustomization.yaml", "resources: ["+filepath.ToSlash(resource)+"]\n")
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\n")
			cm := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: before}\n"
			testutil.WriteFile(t, root, "shared/cm.yaml", cm)
			if tc.patch {
				testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\n")
				testutil.WriteFile(t, root, "inputs/patch", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: before}\n")
			}
			testutil.WriteFile(t, root, "inputs/unrelated", "before")
			testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}})
			orig := t.TempDir()
			testutil.CopyTree(t, root, orig)
			before, stderr := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if !strings.Contains(before, "value: before") {
				t.Fatalf("base full build lost input: %s\n%s", before, stderr)
			}
			t.Logf("full build before:\n%s", before)
			baselines := [][]string{{"--base", "HEAD"}, {"--path-orig", filepath.Join(orig, "flux")}}
			harness.MutateFile(t, filepath.Join(root, "inputs/unrelated"), "before", "after")
			for _, baseline := range baselines {
				args := append([]string{"diff", "all", "-o", "diff", "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}, baseline...)
				if out, stderr := harness.RequireCLIOK(t, args...); out != "" {
					t.Fatalf("unrelated sibling selected the owner: %q\n%s", out, stderr)
				}
			}
			file := "shared/cm.yaml"
			if tc.patch {
				file = "inputs/patch"
			}
			harness.MutateFile(t, filepath.Join(root, file), "value: before", "value: after")
			after, stderr := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if !strings.Contains(after, "value: after") {
				t.Fatalf("candidate full build lost input: %s\n%s", after, stderr)
			}
			t.Logf("full build after:\n%s", after)
			for _, baseline := range baselines {
				t.Run(baseline[0], func(t *testing.T) {
					args := append([]string{"diff", "all", "-o", "diff", "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir()}, baseline...)
					out, stderr := harness.RequireCLIOK(t, args...)
					if strings.Count(out, "-  value: before") != 1 || strings.Count(out, "+  value: after") != 1 {
						t.Fatalf("changed-only diff missed escaped input: %q\n%s", out, stderr)
					}
					second, _ := harness.RequireCLIOK(t, args...)
					if second != out {
						t.Fatalf("diff output changed between runs: %q / %q", out, second)
					}
				})
			}
		})
	}
}

func TestE2E_DiffKustomizeInputLifecycle(t *testing.T) {
	for _, operation := range []string{"add", "delete", "dangling"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml]\n")
			testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "flux-system", "  path: ./apps\n"))
			body := "resources: [cm.yaml]\n"
			patch := "patches: [{path: ../inputs/patch}]\n"
			testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: plain}\n")
			patchBody := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: patched}\n"
			if operation != "add" {
				testutil.WriteFile(t, root, "inputs/patch", patchBody)
				body += patch
			}
			testutil.WriteFile(t, root, "apps/kustomization.yaml", body)
			orig := t.TempDir()
			testutil.CopyTree(t, root, orig)
			before, _ := harness.RequireCLIOK(t, "build", "all", "--path", orig, "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
			wantBefore, wantAfter := "patched", "plain"
			if operation == "add" {
				wantBefore, wantAfter = "plain", "patched"
				testutil.WriteFile(t, root, "inputs/patch", patchBody)
				testutil.WriteFile(t, root, "apps/kustomization.yaml", body+patch)
			} else {
				if err := os.Remove(filepath.Join(root, "inputs/patch")); err != nil {
					t.Fatal(err)
				}
				if operation == "delete" {
					testutil.WriteFile(t, root, "apps/kustomization.yaml", strings.TrimSuffix(body, patch))
				}
			}
			args := []string{"diff", "all", "-o", "diff", "--path", root, "--path-orig", orig, "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir()}
			out, stderr, code := harness.RunCLIBuffers(args...)
			if operation == "dangling" {
				if code != 1 || !strings.Contains(stderr, "patch") {
					t.Fatalf("dangling input lost its render error: code=%d stdout=%q stderr=%s", code, out, stderr)
				}
				return
			}
			after, _ := harness.RequireCLIOK(t, "build", "all", "--path", root, "--no-progress", "--concurrency", "2", "--cache-dir", t.TempDir())
			if code != 0 || !strings.Contains(before, "value: "+wantBefore) || !strings.Contains(after, "value: "+wantAfter) || !strings.Contains(out, wantBefore) || !strings.Contains(out, wantAfter) {
				t.Fatalf("valid %s input comparison failed: code=%d before=%q after=%q diff=%q stderr=%s", operation, code, before, after, out, stderr)
			}
		})
	}
}

func TestE2E_DiffSelectedKustomizeInputs(t *testing.T) {
	for _, input := range []string{"builder", "patch", "unrelated"} {
		t.Run(input, func(t *testing.T) {
			root, orig := t.TempDir(), t.TempDir()
			testutil.WriteFile(t, root, "kustomization.yaml", "resources: [cm.yaml]\nnameSuffix: -dev\npatches: [{path: patch.yaml}]\n")
			testutil.WriteFile(t, root, "cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: base}\n")
			testutil.WriteFile(t, root, "patch.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: before}\n")
			testutil.WriteFile(t, root, "unrelated.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\ndata: {value: before}\n")
			testutil.CopyTree(t, root, orig)
			switch input {
			case "builder":
				harness.MutateFile(t, filepath.Join(root, "kustomization.yaml"), "-dev", "-prod")
			case "patch":
				harness.MutateFile(t, filepath.Join(root, "patch.yaml"), "before", "after")
			case "unrelated":
				harness.MutateFile(t, filepath.Join(root, "unrelated.yaml"), "before", "after")
			}
			out, stderr := harness.RequireCLIOK(t, "diff", "all", "-o", "diff", "--path", root, "--path-orig", orig,
				"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
			if input == "unrelated" {
				if out != "" {
					t.Fatalf("unrelated file promoted a raw object: %s\n%s", out, stderr)
				}
				return
			}
			wants := []string{"input-dev", "before", "after"}
			if input == "builder" {
				wants = []string{"input-dev", "input-prod"}
			}
			for _, want := range wants {
				if !strings.Contains(out, want) {
					t.Fatalf("changed input missed %q: %s\n%s", want, out, stderr)
				}
			}
		})
	}
}
