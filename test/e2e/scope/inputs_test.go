package scope_test

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

	"github.com/home-operations/flate/internal/assert"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
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
			testutil.WriteFile(
				t,
				root,
				"flux/owner.yaml",
				testutil.KustomizationYAML(
					"owner",
					"flux-system",
					"  path: ./deploy\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
			if tc.mixed {
				testutil.WriteFile(
					t,
					root,
					"deploy/sibling.yaml",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\ndata: {value: unchanged}\n",
				)
			}
			resource, err := filepath.Rel(tc.overlay, "shared")
			assert.NoError(t, err)
			testutil.WriteFile(
				t,
				root,
				tc.overlay+"/kustomization.yaml",
				"resources: ["+filepath.ToSlash(resource)+"]\n",
			)
			testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources: [cm.yaml]\n")
			cm := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: before}\n"
			testutil.WriteFile(t, root, "shared/cm.yaml", cm)
			if tc.patch {
				testutil.WriteFile(
					t,
					root,
					"shared/kustomization.yaml",
					"resources: [cm.yaml]\npatches: [{path: ../inputs/patch}]\n",
				)
				testutil.WriteFile(
					t,
					root,
					"inputs/patch",
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: before}\n",
				)
			}
			testutil.WriteFile(t, root, "inputs/unrelated", "before")
			testutil.GitCommitAll(
				t,
				repo,
				"fixture",
				&git.CommitOptions{
					Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
				},
			)
			orig := t.TempDir()
			testutil.CopyTree(t, root, orig)
			requireCLIOKResult1 := harness.RequireCLIOK(
				t,
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			before, stderr := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
			assert.FatalIf(
				t,
				!strings.Contains(before, "value: before"),
				"base full build lost input: %s\n%s",
				before,
				stderr,
			)
			t.Logf("full build before:\n%s", before)
			baselines := [][]string{{"--base", "HEAD"}, {"--path-orig", filepath.Join(orig, "flux")}}
			harness.MutateFile(t, filepath.Join(root, "inputs", "unrelated"), "before", "after")
			for _, baseline := range baselines {
				args := append(
					[]string{
						"diff",
						"all",
						"-o",
						"diff",
						"--path",
						filepath.Join(root, "flux"),
						"--concurrency",
						"2",
						"--no-progress",
						"--cache-dir",
						t.TempDir(),
					},
					baseline...)
				{
					requireCLIOKResult2 := harness.RequireCLIOK(t, args...)
					out, requireCLIOKErr := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
					assert.FatalIf(t, out != "", "unrelated sibling selected the owner: %q\n%s", out, requireCLIOKErr)
				}
			}
			file := "shared/cm.yaml"
			if tc.patch {
				file = "inputs/patch"
			}
			harness.MutateFile(t, filepath.Join(root, file), "value: before", "value: after")
			requireCLIOKResult3 := harness.RequireCLIOK(
				t,
				"build",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			after, stderr := requireCLIOKResult3.Stdout, requireCLIOKResult3.Stderr
			assert.FatalIf(
				t,
				!strings.Contains(after, "value: after"),
				"candidate full build lost input: %s\n%s",
				after,
				stderr,
			)
			t.Logf("full build after:\n%s", after)
			for _, baseline := range baselines {
				t.Run(baseline[0], func(t *testing.T) {
					args := append(
						[]string{
							"diff",
							"all",
							"-o",
							"diff",
							"--path",
							filepath.Join(root, "flux"),
							"--concurrency",
							"2",
							"--no-progress",
							"--cache-dir",
							t.TempDir(),
						},
						baseline...)
					requireCLIOKResult4 := harness.RequireCLIOK(t, args...)
					out, stderr := requireCLIOKResult4.Stdout, requireCLIOKResult4.Stderr
					assert.FatalIf(
						t,
						strings.Count(out, "-  value: before") != 1 || strings.Count(out, "+  value: after") != 1,
						"changed-only diff missed escaped input: %q\n%s",
						out,
						stderr,
					)
					requireCLIOKResult5 := harness.RequireCLIOK(t, args...)
					second, _ := requireCLIOKResult5.Stdout, requireCLIOKResult5.Stderr
					assert.FatalIf(t, second != out, "diff output changed between runs: %q / %q", out, second)
				})
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiffKustomizeInputLifecycle(t *testing.T) {
	for _, operation := range []string{"add", "delete", "dangling"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml]\n")
			testutil.WriteFile(
				t,
				root,
				"flux/owner.yaml",
				testutil.KustomizationYAML("owner", "flux-system", "  path: ./apps\n"),
			)
			body := "resources: [cm.yaml]\n"
			patch := "patches: [{path: ../inputs/patch}]\n"
			testutil.WriteFile(
				t,
				root,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: plain}\n",
			)
			patchBody := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed-input}\ndata: {value: patched}\n"
			if operation != "add" {
				testutil.WriteFile(t, root, "inputs/patch", patchBody)
				body += patch
			}
			testutil.WriteFile(t, root, "apps/kustomization.yaml", body)
			orig := t.TempDir()
			testutil.CopyTree(t, root, orig)
			requireCLIOKResult6 := harness.RequireCLIOK(
				t,
				"build",
				"all",
				"--path",
				orig,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			)
			before, _ := requireCLIOKResult6.Stdout, requireCLIOKResult6.Stderr
			wantBefore, wantAfter := "patched", "plain"
			if operation == "add" {
				wantBefore, wantAfter = "plain", "patched"
				testutil.WriteFile(t, root, "inputs/patch", patchBody)
				testutil.WriteFile(t, root, "apps/kustomization.yaml", body+patch)
			} else {
				assert.NoError(t, os.Remove(filepath.Join(root, "inputs", "patch")))
				if operation == "delete" {
					testutil.WriteFile(t, root, "apps/kustomization.yaml", strings.TrimSuffix(body, patch))
				}
			}
			args := []string{
				"diff",
				"all",
				"-o",
				"diff",
				"--path",
				root,
				"--path-orig",
				orig,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			runCLIBuffersResult7 := harness.RunCLIBuffers(args...)
			out, stderr, code := runCLIBuffersResult7.Stdout, runCLIBuffersResult7.Stderr, runCLIBuffersResult7.Code
			if operation == "dangling" {
				assert.FatalIf(
					t,
					code != 1 || !strings.Contains(stderr, "patch"),
					"dangling input lost its render error: code=%d stdout=%q stderr=%s",
					code,
					out,
					stderr,
				)
				return
			}
			requireCLIOKResult8 := harness.RequireCLIOK(
				t,
				"build",
				"all",
				"--path",
				root,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			)
			after, _ := requireCLIOKResult8.Stdout, requireCLIOKResult8.Stderr
			assert.FatalIf(t, code != 0 || !strings.Contains(before, "value: "+wantBefore) ||
				!strings.Contains(after, "value: "+wantAfter) ||
				!strings.Contains(out, wantBefore) ||
				!strings.Contains(
					out,
					wantAfter,
				), "valid %s input comparison failed: code=%d before=%q after=%q diff=%q stderr=%s", operation, code, before, after, out, stderr)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_DiffSelectedKustomizeInputs(t *testing.T) {
	for _, input := range []string{"builder", "patch", "unrelated"} {
		t.Run(input, func(t *testing.T) {
			root, orig := t.TempDir(), t.TempDir()
			testutil.WriteFile(
				t,
				root,
				"kustomization.yaml",
				"resources: [cm.yaml]\nnameSuffix: -dev\npatches: [{path: patch.yaml}]\n",
			)
			testutil.WriteFile(
				t,
				root,
				"cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: base}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"patch.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: input}\ndata: {value: before}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"unrelated.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: excluded}\ndata: {value: before}\n",
			)
			testutil.CopyTree(t, root, orig)
			switch input {
			case "builder":
				harness.MutateFile(t, filepath.Join(root, "kustomization.yaml"), "-dev", "-prod")
			case "patch":
				harness.MutateFile(t, filepath.Join(root, "patch.yaml"), "before", "after")
			case "unrelated":
				harness.MutateFile(t, filepath.Join(root, "unrelated.yaml"), "before", "after")
			}
			requireCLIOKResult9 := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"-o",
				"diff",
				"--path",
				root,
				"--path-orig",
				orig,
				"--concurrency",
				"2",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
			)
			out, stderr := requireCLIOKResult9.Stdout, requireCLIOKResult9.Stderr
			if input == "unrelated" {
				assert.FatalIf(t, out != "", "unrelated file promoted a raw object: %s\n%s", out, stderr)
				return
			}
			wants := []string{"input-dev", "before", "after"}
			if input == "builder" {
				wants = []string{"input-dev", "input-prod"}
			}
			for _, want := range wants {
				assert.FatalIf(t, !strings.Contains(out, want), "changed input missed %q: %s\n%s", want, out, stderr)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
		t.Run(tc.name, func(t *testing.T) { e2EDiffLocalChartInputsName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ChangedOnlyLocalChartInputs(t *testing.T) {
	elapsed := regexp.MustCompile(` +\d+\.\d+s\n$`)
	for _, committed := range []bool{false, true} {
		for _, invalid := range []bool{false, true} {
			name := "directories"
			if committed {
				name = "committed"
			}
			if invalid {
				name += "/invalid"
			}
			t.Run(name, func(t *testing.T) { e2EChangedOnlyLocalChartInputsName(t, &invalid, &committed, &elapsed) })
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
		{
			"child/templates/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-input, namespace: apps}\ndata: {value: " + value + "}\n",
		},
	}
	for _, file := range files {
		assert.NoError(t, tarball.WriteHeader(
			&tar.Header{Name: file.name, Mode: 0o600, Size: int64(len(file.body))},
		))
		{
			_, err := io.WriteString(tarball, file.body)
			assert.FatalIf(t, err != nil, "%v", err)
		}
	}
	assert.NoError(t, tarball.Close())
	assert.NoError(t, compressed.Close())
	testutil.WriteFile(t, root, filepath.Join(location, "charts", "child-1.0.0.tgz"), data.String())
}

func e2EDiffLocalChartInputsName(t *testing.T, tc *struct {
	name      string
	shape     string
	committed bool
},
) {
	t.Helper()

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
		testutil.WriteFile(
			t,
			root,
			"flux/hr.yaml",
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo, namespace: apps}\nspec: {suspend: true, chartRef: {kind: OCIRepository, name: remote}}\n",
		)
	}
	testutil.WriteFile(
		t,
		root,
		"flux/unrelated.yaml",
		testutil.KustomizationYAML(
			"unrelated",
			"flux-system",
			"  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "unrelated/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"unrelated/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unrelated}\ndata: {value: fixed}\n",
	)
	template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chart-input, namespace: apps}\ndata: {value: {{ .Values.value | quote }}}\n"
	testutil.WriteFile(
		t,
		root,
		filepath.Join(location, "Chart.yaml"),
		"apiVersion: v2\nname: demo\nversion: 1.0.0\n",
	)
	testutil.WriteFile(t, root, filepath.Join(location, "values.yaml"), "value: before\n")
	testutil.WriteFile(t, root, filepath.Join(location, "templates", "cm.yaml"), template)
	extra := strings.Replace(template, "name: chart-input,", "name: chart-input-extra,", 1)
	if tc.shape == "template deleted" {
		testutil.WriteFile(t, root, filepath.Join(location, "templates", "extra.yaml"), extra)
	}
	policy := ".git/\nflux/\nunrelated/\nexcluded/\n.helmignore\n"
	if tc.shape == "admit" || tc.shape == "delete" {
		policy += "templates/cm.yaml\n"
	}
	if tc.shape != "create" {
		testutil.WriteFile(t, root, filepath.Join(location, ".helmignore"), policy)
	}
	testutil.WriteFile(t, root, filepath.Join(location, "excluded", "unused"), "before")
	if tc.shape == "archive" {
		assert.NoError(t, os.Remove(filepath.Join(root, location, "templates", "cm.yaml")))
		writeChartInputArchive(t, root, location, "before")
	}
	options := &git.CommitOptions{
		Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
	}
	base := testutil.GitCommitAll(t, repo, "base", options)
	testutil.CopyTree(t, root, orig)
	flags := []string{"--concurrency", "2", "--no-progress", "--cache-dir", t.TempDir(), "--log-level", "debug"}
	requireCLIOKResult10 := harness.RequireCLIOK(
		t,
		append([]string{"build", "all", "--path", filepath.Join(orig, "flux")}, flags...)...)
	before, _ := requireCLIOKResult10.Stdout, requireCLIOKResult10.Stderr
	switch tc.shape {
	case "template added":
		testutil.WriteFile(t, root, filepath.Join(location, "templates", "extra.yaml"), extra)
	case "template deleted":
		assert.NoError(t, os.Remove(filepath.Join(root, location, "templates", "extra.yaml")))
	case "template", "root", "remote":
		testutil.WriteFile(
			t,
			root,
			filepath.Join(location, "templates", "cm.yaml"),
			strings.Replace(template, ".Values.value | quote", "\"after\" | quote", 1),
		)
	case "values":
		testutil.WriteFile(t, root, filepath.Join(location, "values.yaml"), "value: after\n")
	case "archive":
		writeChartInputArchive(t, root, location, "after")
	case "admit":
		testutil.WriteFile(
			t,
			root,
			filepath.Join(location, ".helmignore"),
			strings.ReplaceAll(policy, "templates/cm.yaml\n", ""),
		)
	case "remove", "create":
		testutil.WriteFile(t, root, filepath.Join(location, ".helmignore"), policy+"templates/cm.yaml\n")
	case "delete":
		assert.NoError(t, os.Remove(filepath.Join(root, location, ".helmignore")))
	case "excluded":
		testutil.WriteFile(t, root, filepath.Join(location, "excluded", "unused"), "after")
	}
	baseline := []string{"--path-orig", filepath.Join(orig, "flux")}
	if tc.committed {
		testutil.GitCommitAll(t, repo, "change", options)
		baseline = []string{"--base", base.String()}
	}
	requireCLIOKResult11 := harness.RequireCLIOK(
		t,
		append([]string{"build", "all", "--path", filepath.Join(root, "flux")}, flags...)...)
	after, _ := requireCLIOKResult11.Stdout, requireCLIOKResult11.Stderr
	args := append([]string{"diff", "all", "-o", "diff", "--path", filepath.Join(root, "flux")}, baseline...)
	args = append(args, flags...)
	requireCLIOKResult12 := harness.RequireCLIOK(t, args...)
	out, stderr := requireCLIOKResult12.Stdout, requireCLIOKResult12.Stderr
	if tc.shape == "excluded" || tc.shape == "unchanged" || tc.shape == "remote" {
		assert.FatalIf(
			t,
			before != after || out != "",
			"precision control changed output: before=%s after=%s diff=%s\n%s",
			before,
			after,
			out,
			stderr,
		)
	} else {
		assert.FatalIf(
			t,
			before == after || !strings.Contains(out, "chart-input"),
			"chart input missed full-render difference: before=%s after=%s diff=%s\n%s",
			before,
			after,
			out,
			stderr,
		)
		if tc.shape == "template" || tc.shape == "values" || tc.shape == "archive" || tc.shape == "root" {
			assert.FatalIf(
				t,
				!strings.Contains(out, "-  value: before") || !strings.Contains(out, "+  value: after"),
				"chart input lost visible diff: %s\n%s",
				out,
				stderr,
			)
		}
		e2EDiffLocalChartInputsNameLoop2(t, struct {
			tc **struct {
				name      string
				shape     string
				committed bool
			}
			out    *string
			stderr *string
		}{tc: &tc, out: &out, stderr: &stderr})
		selections := 0
		e2EDiffLocalChartInputsNameLoop(t, struct {
			stderr     *string
			selections *int
		}{stderr: &stderr, selections: &selections})
		assert.ErrorIf(
			t,
			selections != 2,
			"got %d diff-side selection diagnostics, want 2: %s",
			selections,
			stderr,
		)
	}
	requireCLIOKResult13 := harness.RequireCLIOK(t, args...)
	second, _ := requireCLIOKResult13.Stdout, requireCLIOKResult13.Stderr
	assert.FatalIf(t, out != second, "chart diff changed between equivalent runs: %s\n%s", out, second)
}

func e2EDiffLocalChartInputsNameLoop(t *testing.T, loopInputs struct {
	stderr     *string
	selections *int
},
) {
	t.Helper()

	for line := range strings.SplitSeq((*loopInputs.stderr), "\n") {
		if !strings.Contains(line, "changed-only keep set") {
			continue
		}
		(*loopInputs.selections)++
		for _, id := range []string{"HelmRelease.helm.toolkit.fluxcd.io/apps/demo", "GitRepository.source.toolkit.fluxcd.io/flux-system/flux-system"} {
			assert.ErrorIf(t, !strings.Contains(line, id), "diff-side selection omitted %s: %s", id, line)
		}
		assert.ErrorIf(
			t,
			strings.Contains(line, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/unrelated"),
			"chart read selected unrelated Kustomization: %s",
			line,
		)
	}
}

func e2EDiffLocalChartInputsNameLoop2(t *testing.T, loopInputs struct {
	tc **struct {
		name      string
		shape     string
		committed bool
	}
	out    *string
	stderr *string
},
) {
	t.Helper()

	if (*loopInputs.tc).shape == "template added" || (*loopInputs.tc).shape == "template deleted" {
		prefix := "+"
		if (*loopInputs.tc).shape == "template deleted" {
			prefix = "-"
		}
		assert.FatalIf(t, strings.Count((*loopInputs.out), "--- ") != 1 ||
			!strings.HasPrefix(
				(*loopInputs.out),
				"--- ConfigMap apps/chart-input-extra\n+++ ConfigMap apps/chart-input-extra\n",
			) ||
			!strings.Contains(
				(*loopInputs.out),
				prefix+"  name: chart-input-extra\n",
			), "chart file lifecycle changed more than its resource: %s\n%s", (*loopInputs.out), (*loopInputs.stderr))
	}
}
