package scope

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"regexp"
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

func TestE2E_DiffLocalChartInputs(t *testing.T) {
	for _, tc := range []struct {
		name, shape string
		committed   bool
	}{
		{name: "template committed", shape: "template", committed: true},
		{name: "template directories", shape: "template"},
		{name: "template added directories", shape: "template added"},
		{name: "template added committed", shape: "template added", committed: true},
		{name: "template deleted directories", shape: "template deleted"},
		{name: "template deleted committed", shape: "template deleted", committed: true},
		{name: "default values", shape: "values"},
		{name: "packaged subchart", shape: "archive"},
		{name: "root chart", shape: "root"},
		{name: "policy admits template", shape: "admit"},
		{name: "policy removes template", shape: "remove"},
		{name: "policy created", shape: "create"},
		{name: "policy deleted", shape: "delete"},
		{name: "excluded payload", shape: "excluded"},
		{name: "unchanged", shape: "unchanged"},
		{name: "remote chart excludes local payload", shape: "remote"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, orig := t.TempDir(), t.TempDir()
			repo := testutil.GitInit(t, root)
			location := "charts/demo"
			if tc.shape == "root" {
				location = "."
			}
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [hr.yaml, unrelated.yaml]\n")
			testutil.WriteFile(t, root, "flux/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  interval: 10m
  chart:
    spec:
      chart: `+location+`
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			if tc.shape == "remote" {
				testutil.WriteFile(t, root, "flux/hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {suspend: true, chartRef: {kind: OCIRepository, name: remote}}\n")
			}
			testutil.WriteFile(t, root, "flux/unrelated.yaml", testutil.KustomizationYAML("unrelated", "flux-system", "  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\ndata: {value: fixed}\n")
			template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-input, namespace: apps}\ndata: {value: {{ .Values.value | quote }}}\n"
			testutil.WriteFile(t, root, filepath.Join(location, "Chart.yaml"), "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
			testutil.WriteFile(t, root, filepath.Join(location, "values.yaml"), "value: before\n")
			testutil.WriteFile(t, root, filepath.Join(location, "templates/cm.yaml"), template)
			extra := strings.Replace(template, "name: chart-input,", "name: chart-input-extra,", 1)
			if tc.shape == "template deleted" {
				testutil.WriteFile(t, root, filepath.Join(location, "templates/extra.yaml"), extra)
			}
			policy := ".git/\nflux/\nunrelated/\nexcluded/\n.helmignore\n"
			if tc.shape == "admit" || tc.shape == "delete" {
				policy += "templates/cm.yaml\n"
			}
			if tc.shape != "create" {
				testutil.WriteFile(t, root, filepath.Join(location, ".helmignore"), policy)
			}
			testutil.WriteFile(t, root, filepath.Join(location, "excluded/unused"), "before")
			if tc.shape == "archive" {
				if err := os.Remove(filepath.Join(root, location, "templates/cm.yaml")); err != nil {
					t.Fatal(err)
				}
				writeChartInputArchive(t, root, location, "before")
			}
			options := &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}}
			base := testutil.GitCommitAll(t, repo, "base", options)
			testutil.CopyTree(t, root, orig)
			flags := []string{"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "--log-level", "debug"}
			before, _ := harness.RequireCLIOK(t, append([]string{"build", "all", "--path", filepath.Join(orig, "flux")}, flags...)...)
			switch tc.shape {
			case "template added":
				testutil.WriteFile(t, root, filepath.Join(location, "templates/extra.yaml"), extra)
			case "template deleted":
				if err := os.Remove(filepath.Join(root, location, "templates/extra.yaml")); err != nil {
					t.Fatal(err)
				}
			case "template", "root", "remote":
				testutil.WriteFile(t, root, filepath.Join(location, "templates/cm.yaml"), strings.Replace(template, ".Values.value | quote", "\"after\" | quote", 1))
			case "values":
				testutil.WriteFile(t, root, filepath.Join(location, "values.yaml"), "value: after\n")
			case "archive":
				writeChartInputArchive(t, root, location, "after")
			case "admit":
				testutil.WriteFile(t, root, filepath.Join(location, ".helmignore"), strings.ReplaceAll(policy, "templates/cm.yaml\n", ""))
			case "remove", "create":
				testutil.WriteFile(t, root, filepath.Join(location, ".helmignore"), policy+"templates/cm.yaml\n")
			case "delete":
				if err := os.Remove(filepath.Join(root, location, ".helmignore")); err != nil {
					t.Fatal(err)
				}
			case "excluded":
				testutil.WriteFile(t, root, filepath.Join(location, "excluded/unused"), "after")
			}
			baseline := []string{"--path-orig", filepath.Join(orig, "flux")}
			if tc.committed {
				testutil.GitCommitAll(t, repo, "change", options)
				baseline = []string{"--base", base.String()}
			}
			after, _ := harness.RequireCLIOK(t, append([]string{"build", "all", "--path", filepath.Join(root, "flux")}, flags...)...)
			args := append([]string{"diff", "all", "-o", "diff", "--path", filepath.Join(root, "flux")}, baseline...)
			args = append(args, flags...)
			out, stderr := harness.RequireCLIOK(t, args...)
			if tc.shape == "excluded" || tc.shape == "unchanged" || tc.shape == "remote" {
				if before != after || out != "" {
					t.Fatalf("precision control changed output: before=%s after=%s diff=%s\n%s", before, after, out, stderr)
				}
			} else {
				if before == after || !strings.Contains(out, "chart-input") {
					t.Fatalf("chart input missed full-render difference: before=%s after=%s diff=%s\n%s", before, after, out, stderr)
				}
				if tc.shape == "template" || tc.shape == "values" || tc.shape == "archive" || tc.shape == "root" {
					if !strings.Contains(out, "-  value: before") || !strings.Contains(out, "+  value: after") {
						t.Fatalf("chart input lost visible diff: %s\n%s", out, stderr)
					}
				}
				if tc.shape == "template added" || tc.shape == "template deleted" {
					prefix := "+"
					if tc.shape == "template deleted" {
						prefix = "-"
					}
					if strings.Count(out, "--- ") != 1 ||
						!strings.HasPrefix(out, "--- ConfigMap apps/chart-input-extra\n+++ ConfigMap apps/chart-input-extra\n") ||
						!strings.Contains(out, prefix+"  name: chart-input-extra\n") {
						t.Fatalf("chart file lifecycle changed more than its resource: %s\n%s", out, stderr)
					}
				}
				selections := 0
				for line := range strings.SplitSeq(stderr, "\n") {
					if !strings.Contains(line, "changed-only keep set") {
						continue
					}
					selections++
					for _, id := range []string{"HelmRelease.helm.toolkit.fluxcd.io/apps/demo", "GitRepository.source.toolkit.fluxcd.io/flux-system/flux-system"} {
						if !strings.Contains(line, id) {
							t.Errorf("diff-side selection omitted %s: %s", id, line)
						}
					}
					if strings.Contains(line, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/unrelated") {
						t.Errorf("chart read selected unrelated Kustomization: %s", line)
					}
				}
				if selections != 2 {
					t.Errorf("got %d diff-side selection diagnostics, want 2: %s", selections, stderr)
				}
			}
			second, _ := harness.RequireCLIOK(t, args...)
			if out != second {
				t.Fatalf("chart diff changed between equivalent runs: %s\n%s", out, second)
			}
		})
	}
}

func TestE2E_ChangedOnlyLocalChartInputs(t *testing.T) {
	elapsed := regexp.MustCompile(` +[0-9]+\.[0-9]+s\n$`)
	for _, committed := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			name := "directories"
			if committed {
				name = "committed"
			}
			if invalid {
				name += "/invalid"
			}
			t.Run(name, func(t *testing.T) {
				root, orig := t.TempDir(), t.TempDir()
				repo := testutil.GitInit(t, root)
				testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [hr.yaml, control.yaml, unrelated.yaml]\n")
				release := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"
				testutil.WriteFile(t, root, "flux/hr.yaml", release)
				testutil.WriteFile(t, root, "flux/control.yaml", strings.ReplaceAll(strings.ReplaceAll(release, "demo", "control"), "namespace: apps", "namespace: other"))
				testutil.WriteFile(t, root, "flux/unrelated.yaml", testutil.KustomizationYAML("unrelated", "other", "  path: ./unrelated\n"))
				testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
				testutil.WriteFile(t, root, "unrelated/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\n")
				template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: surviving, namespace: apps}\ndata: {value: unchanged}\n"
				for _, chart := range []string{"demo", "control"} {
					testutil.WriteFile(t, root, "charts/"+chart+"/Chart.yaml", "apiVersion: v2\nname: "+chart+"\nversion: 1.0.0\n")
					testutil.WriteFile(t, root, "charts/"+chart+"/templates/cm.yaml", strings.ReplaceAll(template, "surviving", chart+"-surviving"))
				}
				if invalid {
					testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "{{ fail \"surviving template is invalid\" }}\n")
				}
				testutil.WriteFile(t, root, "charts/demo/templates/extra.yaml", strings.ReplaceAll(template, "surviving", "deleted"))
				options := &git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)}}
				base := testutil.GitCommitAll(t, repo, "base", options)
				testutil.CopyTree(t, root, orig)
				if err := os.Remove(filepath.Join(root, "charts/demo/templates/extra.yaml")); err != nil {
					t.Fatal(err)
				}
				baseline := []string{"--path-orig", filepath.Join(orig, "flux")}
				if committed {
					testutil.GitCommitAll(t, repo, "delete template", options)
					baseline = []string{"--base", base.String()}
				}
				commands := []string{"build", "test", "get"}
				if invalid {
					commands = []string{"test"}
				}
				for _, command := range commands {
					t.Run(command, func(t *testing.T) {
						args := []string{command, "all"}
						if command == "get" {
							args = []string{command, "hr", "-o", "name"}
						}
						args = append(args, "--path", filepath.Join(root, "flux"), "--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir())
						args = append(args, baseline...)
						out, stderr, code := harness.RunCLIBuffers(args...)
						wantCode, want := 0, "apps/demo"
						if command == "get" {
							want = "demo\n"
						}
						if command == "test" && !invalid && !strings.Contains(out, "1 passed") {
							t.Errorf("selected release was not checked: %s", out)
						}
						if command == "build" {
							want = "name: demo-surviving"
						}
						if invalid {
							wantCode = 1
							if !strings.Contains(out+stderr, "surviving template is invalid") {
								t.Errorf("selected release failure was skipped: stdout=%q stderr=%s", out, stderr)
							}
						}
						if code != wantCode || !strings.Contains(out, want) {
							t.Errorf("deleted template lost its release: code=%d stdout=%q stderr=%s", code, out, stderr)
						}
						for _, excluded := range []string{"deleted", "unrelated", "control"} {
							if strings.Contains(out, excluded) {
								t.Errorf("selected unrelated or deleted output %q: %s", excluded, out)
							}
						}
						second, _, secondCode := harness.RunCLIBuffers(args...)
						if secondCode != code || elapsed.ReplaceAllString(second, "") != elapsed.ReplaceAllString(out, "") {
							t.Errorf("changed-only output differs between runs: %d/%q versus %d/%q", code, out, secondCode, second)
						}
					})
				}
			})
		}
	}
}

func writeChartInputArchive(t *testing.T, root, location, value string) {
	t.Helper()
	var data bytes.Buffer
	compressed := gzip.NewWriter(&data)
	tarball := tar.NewWriter(compressed)
	files := []struct{ name, body string }{
		{"child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 1.0.0\n"},
		{"child/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-input, namespace: apps}\ndata: {value: " + value + "}\n"},
	}
	for _, file := range files {
		if err := tarball.WriteHeader(&tar.Header{Name: file.name, Mode: 0o600, Size: int64(len(file.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tarball, file.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarball.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, filepath.Join(location, "charts/child-1.0.0.tgz"), data.String())
}
