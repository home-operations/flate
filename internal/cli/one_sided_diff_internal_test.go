package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diff"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Diff_OneSidedFailureSuppressed(t *testing.T) {
	for _, side := range []string{"orig", "current"} {
		t.Run(side, func(t *testing.T) {
			orig, current := writeFixture(t), writeFixture(t)
			for _, root := range []string{orig, current} {
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/cluster.yaml]\n")
			}
			broken := current
			if side == "orig" {
				broken = orig
			}
			testutil.WriteFile(t, broken, "apps/kustomization.yaml", "resources:\n- missing.yaml\n")

			for _, format := range []string{"github", "json"} {
				t.Run(format, func(t *testing.T) {
					cli := runCLI(
						t,
						"diff",
						"ks",
						"--path",
						current,
						"--path-orig",
						orig,
						"-o",
						format,
						"--concurrency",
						"2",
					)
					stdout, stderr, code := cli.stdout, cli.stderr, cli.code
					assert.Equal(t, code, 1)
					if format == "github" {
						assert.Equal(t, stdout, "")
					} else {
						var result struct {
							Changes    []diff.Change      `json:"changes"`
							Suppressed []diff.Suppression `json:"suppressed"`
						}
						assert.NoError(t, json.Unmarshal([]byte(stdout), &result))
						assert.Equal(t, len(result.Changes), 0)
						assert.FatalIf(t, len(result.Suppressed) != 1, "lost suppression: %s", stdout)
						note := result.Suppressed[0]
						assert.Equal(t, string(note.Side), side)
						assert.Equal(t, note.Count, 1)
						assert.Equal(t, note.Parent.NamedResource, testutil.KustomizationID("flux-system", "apps"))
						assert.Equal(t, note.Parent.Path, "apps")
						assert.FatalIf(
							t,
							!strings.Contains(note.Reason, "missing.yaml"),
							"lost render failure: %s",
							stdout,
						)
					}
					assert.ErrorIf(
						t,
						strings.Count(stderr, "  ✗  "+side+" snapshot: Kustomization  flux-system/apps  ") != 1,
						"failure row must identify the %s snapshot:\n%s",
						side,
						stderr,
					)
					assert.ErrorIf(
						t,
						strings.Count(stderr, "1 object not rendered on the "+side+" side; diff suppressed (") != 1,
						"suppression must appear once on stderr: %s",
						stderr,
					)
					assert.ErrorIf(
						t,
						strings.Count(stderr, "✗ 1 failed") != 1 || strings.Contains(stderr, "flate error:"),
						"failure summary should be printed exactly once:\n%s",
						stderr,
					)
				})
			}
		})
	}
}

func runDiffOneSidedSkipSuppressedName(t *testing.T, tc *struct {
	name       string
	orig       string
	current    string
	other      bool
	json       bool
	wantSide   string
	wantRemove bool
},
) {
	t.Helper()

	write := func(source, message string) string {
		return runDiffOneSidedSkipSuppressedNameBody(source, message, &t, &tc)
	}
	orig := write(tc.orig, "before")
	current := write(tc.current, "after")
	format := "diff"
	if tc.json {
		format = "json"
	}
	cli := runCLI(
		t,
		"diff",
		"hr",
		"--path",
		current,
		"--path-orig",
		orig,
		"--allow-missing-kinds=Secret,ConfigMap",
		"--output",
		format,
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(t, code != 0, "diff hr exited %d: stderr=%s", code, stderr)
	cli2 := runCLI(
		t,
		"diff",
		"images",
		"--include-removed",
		"--path",
		current,
		"--path-orig",
		orig,
		"--allow-missing-kinds=Secret,ConfigMap",
		"--no-progress",
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	images, imageStderr, imageCode := cli2.stdout, cli2.stderr, cli2.code
	assert.Equal(t, imageCode, 0)
	if tc.wantRemove {
		assert.Equal(t, images, "ghcr.io/demo/private:v1\n")
	} else {
		assert.Equal(t, images, "")
	}
	runDiffOneSidedSkipSuppressedNameLoop(t, struct {
		tc **struct {
			name       string
			orig       string
			current    string
			other      bool
			json       bool
			wantSide   string
			wantRemove bool
		}
		imageStderr *string
		current     *string
		orig        *string
	}{tc: &tc, imageStderr: &imageStderr, current: &current, orig: &orig})
	if tc.json {
		var got struct {
			Changes    []diff.Change      `json:"Changes"`
			Suppressed []diff.Suppression `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte(stdout), &got))
		assert.Equal(t, len(got.Changes), 0)
		assert.FatalIf(t, len(got.Suppressed) != 1, "expected one suppression, got:\n%s", stdout)
		assert.Equal(t, got.Suppressed[0].Side, diff.SideCurrent)
		assert.Equal(t, got.Suppressed[0].Count, 2)
		assert.Equal(
			t,
			got.Suppressed[0].Parent,
			diff.Parent{NamedResource: testutil.HelmReleaseID("apps", "demo")},
		)
		assert.Equal(t, strings.Contains(got.Suppressed[0].Reason, "missing-ca"), true)
		assert.Equal(t, strings.HasPrefix(got.Suppressed[0].Reason, "skipped:"), false)
		return
	}
	switch {
	case tc.wantSide != "":

		prefix := "HelmRelease apps/demo: 2 objects not rendered on the " + tc.wantSide + " side; diff suppressed ("
		assert.ErrorIf(
			t,
			strings.Count(stderr, "[DiffSuppressed]") != 1 || strings.Count(stderr, prefix) != 1 ||
				!strings.Contains(
					stderr,
					"missing-ca",
				),
			"expected suppression with the missing Secret reason; got:\n%s",
			stderr,
		)
		assert.ErrorIf(
			t,
			strings.Contains(stdout, "diff suppressed") || strings.Contains(stdout, "ConfigMap apps/demo"),
			"skipped release must not appear as added or removed:\n%s",
			stdout,
		)

	case strings.Contains(stdout, "diff suppressed"):

		t.Errorf("a complete comparison must not be suppressed:\n%s", stdout)
	}
	assert.ErrorIf(
		t,
		tc.wantRemove && !strings.Contains(stdout, "-  greeting: hello"),
		"actual release removal must remain visible:\n%s",
		stdout,
	)
	assert.ErrorIf(t, tc.other &&
		(!strings.Contains(stdout, "-  greeting: before") || !strings.Contains(stdout, "+  greeting: after")), "unrelated change must remain visible:\n%s", stdout)
}

func runDiffOneSidedSkipSuppressedNameBody(source string, message string, t **testing.T, tc **struct {
	name       string
	orig       string
	current    string
	other      bool
	json       bool
	wantSide   string
	wantRemove bool
},
) string {
	(*t).Helper()
	path := writeFixture((*t))
	testutil.WriteFile((*t), path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
	testutil.WriteFile((*t), path, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile((*t), path, "charts/demo/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}', namespace: apps}
data:
  greeting: {{ .Values.greeting }}
---
apiVersion: v1
kind: Pod
metadata: {name: '{{ .Release.Name }}-second', namespace: apps}
spec:
  containers:
    - {name: app, image: ghcr.io/demo/private:v1}
`)
	chart := `  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
	if (*tc).other {
		testutil.WriteFile((*t), path, "flux/other.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: other, namespace: apps}
spec:
`+chart+`  values: {greeting: `+message+`}
`)
	}
	if source == "skipped" {
		testutil.WriteFile((*t), path, "flux/oci.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: private-chart, namespace: apps}
spec:
  url: oci://example.invalid/private/chart
  certSecretRef: {name: missing-ca}
`)
		chart = "  chartRef: {kind: OCIRepository, name: private-chart}\n"
	}
	if source != "absent" {
		testutil.WriteFile((*t), path, "flux/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
`+chart+"  values: {greeting: hello}\n")
	}
	resources := []string{"flux/cluster.yaml", "flux/source.yaml"}
	if (*tc).other {
		resources = append(resources, "flux/other.yaml")
	}
	if source == "skipped" {
		resources = append(resources, "flux/oci.yaml")
	}
	if source != "absent" {
		resources = append(resources, "flux/hr.yaml")
	}
	testutil.WriteFile((*t), path, "kustomization.yaml", "resources: ["+strings.Join(resources, ", ")+"]\n")
	return path
}

func runDiffOneSidedSkipSuppressedNameLoop(t *testing.T, loopInputs struct {
	tc **struct {
		name       string
		orig       string
		current    string
		other      bool
		json       bool
		wantSide   string
		wantRemove bool
	}
	imageStderr *string
	current     *string
	orig        *string
},
) {
	t.Helper()

	if (*loopInputs.tc).wantSide != "" || (*loopInputs.tc).json {
		side := (*loopInputs.tc).wantSide
		if side == "" {
			side = "current"
		}
		prefix := "HelmRelease apps/demo: 2 objects not rendered on the " + side + " side; diff suppressed ("
		assert.FatalIf(
			t,
			strings.Count((*loopInputs.imageStderr), "[DiffSuppressed]") != 1 || strings.Count((*loopInputs.imageStderr), prefix) != 1 ||
				strings.Contains((*loopInputs.imageStderr), "×2") ||
				!strings.Contains((*loopInputs.imageStderr), "missing-ca"),
			"image suppression missing: %s",
			(*loopInputs.imageStderr),
		)
		writeErr := errors.New("image disclosure write failed")
		cmd := New("test")
		cmd.SetContext(t.Context())
		cmd.SetArgs([]string{
			"diff", "images", "--path", (*loopInputs.current), "--path-orig", (*loopInputs.orig),
			"--allow-missing-kinds=Secret,ConfigMap", "--cache-dir", t.TempDir(), "--concurrency", "2",
		})
		cmd.SetOut(&bytes.Buffer{})
		cmd.SetErr(failingWriter{err: writeErr})
		{
			err := cmd.Execute()
			assert.FatalIf(t, !errors.Is(err, writeErr), "image disclosure writer error lost: %v", err)
		}
	}
}
