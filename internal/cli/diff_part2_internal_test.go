package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffKS_NamedExternalSourceDisclosure(t *testing.T) {
	current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/home-ops.git")
	for _, dir := range []string{current, orig} {
		testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [cluster.yaml, second.yaml, external.yaml]\n")
		for _, rel := range []string{"flux/cluster.yaml", "flux/second.yaml"} {
			data, err := os.ReadFile(filepath.Join(dir, rel))
			assert.NoError(t, err)
			testutil.WriteFile(t, dir, rel, strings.ReplaceAll(string(data), "gitops-system", "alpha"))
		}
		testutil.WriteFile(t, dir, "flux/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: external, namespace: beta}
spec: {url: "https://example.invalid/external.git", interval: 1h}
`+"---\n"+
			testutil.KustomizationYAML("external-apps", "beta", `  path: ./external
  sourceRef: {kind: GitRepository, name: external}
  interval: 1h
`))
	}
	for _, app := range []string{"apps", "second-apps"} {
		name := "hello"
		if app == "second-apps" {
			name = "second"
		}
		testutil.WriteFile(
			t,
			orig,
			app+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n",
		)
	}
	for _, tc := range []struct {
		name      string
		args      []string
		warn      bool
		code      int
		fieldDiff bool
	}{
		{name: "named inferred namespace", args: []string{"external-apps"}, warn: true, code: 1},
		{name: "named at error log level", args: []string{"external-apps", "--log-level", "error"}, warn: true, code: 1},
		{name: "unnamed", warn: true, fieldDiff: true},
		{name: "explicit namespace", args: []string{"external-apps", "--namespace", "beta"}, warn: true},
		{name: "excluded namespace", args: []string{"external-apps", "--namespace", "alpha"}, code: 1},
		{name: "local name", args: []string{"apps"}, fieldDiff: true},
		{name: "nonexistent name", args: []string{"missing"}, code: 1},
	} {
		t.Run(tc.name, func(t *testing.T) { runDiffKSNamedExternalSourceDisclosureName(t, &tc, &current, &orig) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Diff_ProducerDispositionAndPath(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		for _, mode := range []string{"both-failed", "both-suspended", "path-only", "path-content"} {
			t.Run(workers+"/"+mode, func(t *testing.T) { runDiffProducerDispositionAndPathCase(t, &mode, &workers) })
		}
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
	stdout, stderr, code := runCLI(
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
	assert.FatalIf(t, code != 0, "diff hr exited %d: stderr=%s", code, stderr)
	images, imageStderr, imageCode := runCLI(
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

func runDiffOmittedValuesName(t *testing.T, tc *struct {
	name        string
	kind        string
	verb        string
	resource    string
	namespace   string
	origRef     bool
	currentRef  bool
	change      bool
	wantOrig    bool
	wantCurrent bool
	wantCode    int
},
) {
	t.Helper()

	write := func(ref bool, interval, message string) string {
		t.Helper()
		path := writeFixture(t)
		testutil.WriteFile(t, path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
		testutil.WriteFile(t, path, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(t, path, "charts/demo/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: demo, namespace: apps}
data:
  greeting: {{ .Values.greeting | default "hello" }}
  message: {{ .Values.message }}
`)
		refs := ""
		if ref {
			refs = "  valuesFrom:\n    - kind: " + tc.kind + "\n      name: extra-values\n"
		}
		if tc.kind == "Secret" {
			testutil.WriteFile(t, path, "flux/vso.yaml", `apiVersion: secrets.hashicorp.com/v1beta1
kind: VaultStaticSecret
metadata: {name: extra-values, namespace: apps}
spec:
  destination: {name: extra-values, create: true}
`)
		}
		testutil.WriteFile(t, path, "flux/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  interval: `+interval+`
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`+refs+`  values: {message: `+message+`, unused: stale}
`)
		resources := []string{"flux/cluster.yaml", "flux/source.yaml", "flux/hr.yaml"}
		if tc.kind == "Secret" {
			resources = append(resources, "flux/vso.yaml")
		}
		testutil.WriteFile(t, path, "kustomization.yaml", "resources: ["+strings.Join(resources, ", ")+"]\n")
		return path
	}
	// Different intervals select both trees without changing chart output.
	orig := write(tc.origRef, "5m", "before")
	message := "before"
	if tc.change {
		message = "after"
	}
	current := write(tc.currentRef, "10m", message)
	cache := t.TempDir()
	runDiffOmittedValuesNameLoop(t, struct {
		tc **struct {
			name        string
			kind        string
			verb        string
			resource    string
			namespace   string
			origRef     bool
			currentRef  bool
			change      bool
			wantOrig    bool
			wantCurrent bool
			wantCode    int
		}
		current *string
		orig    *string
		cache   *string
	}{tc: &tc, current: &current, orig: &orig, cache: &cache})
}

func runDiffKSPathOrigRepositoryIdentityName(t *testing.T, tc *struct {
	name     string
	linked   bool
	nested   bool
	matching bool
},
) {
	t.Helper()
	const currentURL = "ssh://git@github.com/example/home-ops.git"
	const baselineURL = "https://example.invalid/upstream.git"

	current, orig := writeCustomSourceDiffFixture(t, currentURL)
	writeBaselineGit(t, current, currentURL)
	url := baselineURL
	if tc.matching {
		url = currentURL
	}
	cluster, err := os.ReadFile(filepath.Join(orig, "flux", "cluster.yaml"))
	assert.NoError(t, err)
	testutil.WriteFile(t, orig, "flux/cluster.yaml", strings.ReplaceAll(string(cluster), currentURL, url))
	if tc.linked {
		common := t.TempDir()
		writeBaselineGit(t, common, url)
		writeBaselineWorktree(t, orig, common)
	} else {
		writeBaselineGit(t, orig, url)
	}
	for _, app := range []string{"apps", "second-apps"} {
		name := "hello"
		if app == "second-apps" {
			name = "second"
		}
		testutil.WriteFile(
			t,
			orig,
			app+"/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n",
		)
	}
	for _, dir := range []string{current, orig} {
		testutil.WriteFile(
			t,
			dir,
			"flux/kustomization.yaml",
			"resources: [cluster.yaml, second.yaml, external.yaml]\n",
		)
		testutil.WriteFile(t, dir, "flux/external.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: unrelated, namespace: beta}
spec: {url: "https://example.invalid/unrelated.git", interval: 1h}
`+"---\n"+
			testutil.KustomizationYAML("external-apps", "beta", `  path: ./external
  sourceRef: {kind: GitRepository, name: unrelated}
  interval: 1h
`))
		testutil.WriteFile(t, dir, "external/kustomization.yaml", "resources:\n- cm.yaml\n")
		testutil.WriteFile(
			t,
			dir,
			"external/cm.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: must-not-render}\n",
		)
	}
	currentScan, origScan := current, orig
	if tc.nested {
		currentScan, origScan = filepath.Join(current, "flux"), filepath.Join(orig, "flux")
	}
	c := commonFlags{path: currentScan, pathOrig: origScan}
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err = resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(c.pathOrigSelfURLs) != 1 || c.pathOrigSelfURLs[0] != url,
		"baseline identity = %v, want [%s]; stopping before render to prevent an external fetch",
		c.pathOrigSelfURLs,
		url,
	)
	args := []string{
		"diff", "ks", "--path", currentScan, "--path-orig", origScan,
		"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff",
	}
	stdout, stderr, code := runCLI(t, args...)
	assert.FatalIf(t, code != 0, "diff exited %d: %s", code, stderr)
	for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
		{
			count := strings.Count(stdout, want)
			assert.ErrorIf(t, count != 2, "field diff %q occurred %d times, want 2:\n%s", want, count, stdout)
		}
	}
	for _, unwanted := range []string{"must-not-render", "+apiVersion:", "-apiVersion:", "suppressed"} {
		assert.ErrorIf(
			t,
			strings.Contains(stdout, unwanted),
			"unexpected %q in field diff:\n%s",
			unwanted,
			stdout,
		)
	}
	assert.ErrorIf(
		t,
		strings.Count(stderr, "external source") != 2 || strings.Contains(stderr, "home-kubernetes"),
		"want only unrelated external-source disclosures: %s",
		stderr,
	)
	if tc.name == "different origins" {
		secondOut, secondErr, secondCode := runCLI(t, args...)
		assert.ErrorIf(
			t,
			secondOut != stdout || secondErr != stderr || secondCode != code,
			"repeated diff changed output or exit code:\n%s\n%s",
			secondOut,
			secondErr,
		)
	}
}

func runDiffKSNamedExternalSourceDisclosureName(t *testing.T, tc *struct {
	name      string
	args      []string
	warn      bool
	code      int
	fieldDiff bool
}, current *string, orig *string,
) {
	t.Helper()

	args := append([]string{"diff", "ks"}, tc.args...)
	args = append(args, "--path", (*current), "--path-orig", (*orig), "--cache-dir", t.TempDir(),
		"--concurrency", "2", "-o", "diff")
	stdout, stderr, code := runCLI(t, args...)
	assert.FatalIf(t, code != tc.code, "diff exited %d, want %d: %s", code, tc.code, stderr)
	origWarning := strings.Index(stderr, "orig snapshot:")
	currentWarning := strings.Index(stderr, "current snapshot:")
	errorAt := strings.Index(stderr, "no Kustomization named")
	switch {
	case tc.warn:

		assert.ErrorIf(t, origWarning < 0 || currentWarning <= origWarning ||
			strings.Count(
				stderr,
				"Kustomization beta/external-apps: external source GitRepository.source.toolkit.fluxcd.io/beta/external",
			) != 2 ||
			strings.Count(
				stderr,
				"not rendered",
			) != 2, "want orig then current external-source disclosure: %s", stderr)
		assert.ErrorIf(
			t,
			tc.code != 0 && (errorAt < 0 || errorAt >= origWarning),
			"missing-name error must precede warnings: %s",
			stderr,
		)

	case strings.Contains(stderr, "external source"):

		t.Errorf("unrelated external source was disclosed: %s", stderr)
	}
	assert.ErrorIf(t, tc.code != 0 && errorAt < 0, "missing-name error absent: %s", stderr)
	switch {
	case tc.fieldDiff:

		for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
			assert.ErrorIf(t, !strings.Contains(stdout, want), "missing local field diff %q:\n%s", want, stdout)
		}

	case stdout != "":

		t.Errorf("external selection produced resource output:\n%s", stdout)
	}
}

func runDiffOmittedValuesNameLoop(t *testing.T, loopInputs struct {
	tc **struct {
		name        string
		kind        string
		verb        string
		resource    string
		namespace   string
		origRef     bool
		currentRef  bool
		change      bool
		wantOrig    bool
		wantCurrent bool
		wantCode    int
	}
	current *string
	orig    *string
	cache   *string
},
) {
	t.Helper()

	for _, output := range []string{"diff", "json"} {
		verb := "hr"
		if (*loopInputs.tc).verb != "" {
			verb = (*loopInputs.tc).verb
		}
		args := []string{
			"diff",
			verb,
			"--path",
			(*loopInputs.current),
			"--path-orig",
			(*loopInputs.orig),
			"--cache-dir",
			(*loopInputs.cache),
			"--concurrency",
			"2",
			"--output",
			output,
			"--no-progress",
			"--allow-missing-kinds=Secret,ConfigMap",
		}
		if (*loopInputs.tc).resource != "" {
			args = append(args, (*loopInputs.tc).resource)
		}
		if (*loopInputs.tc).namespace != "" {
			args = append(args, "--namespace", (*loopInputs.tc).namespace)
		}

		stdout, stderr, code := runCLI(t, args...)
		assert.Equal(t, code, (*loopInputs.tc).wantCode)
		assert.Equal(
			t,
			strings.Contains(stderr, "values not used by the chart"),
			(*loopInputs.tc).namespace == "" && (*loopInputs.tc).verb != "ks",
		)
		assert.Equal(t, strings.Contains(stdout, "flate warning:"), false)
		for _, side := range []struct {
			name string
			want bool
		}{{"orig", (*loopInputs.tc).wantOrig}, {"current", (*loopInputs.tc).wantCurrent}} {
			note := "HelmRelease apps/demo: required valuesFrom " + (*loopInputs.tc).kind + "/apps/extra-values omitted from offline render"
			assert.Equal(t, strings.Contains(stderr, side.name+" snapshot: "+note), side.want)
		}
		if (*loopInputs.tc).resource == "other" {
			assert.Equal(t, stdout, "")
			continue
		}
		runDiffOmittedValuesNameLoopLoop(t, struct {
			output     *string
			stdout     *string
			loopInputs *struct {
				tc **struct {
					name        string
					kind        string
					verb        string
					resource    string
					namespace   string
					origRef     bool
					currentRef  bool
					change      bool
					wantOrig    bool
					wantCurrent bool
					wantCode    int
				}
				current *string
				orig    *string
				cache   *string
			}
			code *int
		}{output: &output, stdout: &stdout, loopInputs: &loopInputs, code: &code})
	}
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

func runDiffOmittedValuesNameLoopLoop(t *testing.T, loopInputs struct {
	output     *string
	stdout     *string
	loopInputs *struct {
		tc **struct {
			name        string
			kind        string
			verb        string
			resource    string
			namespace   string
			origRef     bool
			currentRef  bool
			change      bool
			wantOrig    bool
			wantCurrent bool
			wantCode    int
		}
		current *string
		orig    *string
		cache   *string
	}
	code *int
},
) {
	t.Helper()

	switch {
	case (*loopInputs.output) == "json":

		var got struct {
			Changes    []map[string]any `json:"Changes"`
			Suppressed []map[string]any `json:"Suppressed"`
		}
		assert.NoError(t, json.Unmarshal([]byte((*loopInputs.stdout)), &got))
		assert.Equal(
			t,
			len(got.Changes) > 0,
			(*loopInputs.loopInputs.tc).change || (*loopInputs.loopInputs.tc).verb == "ks",
		)
		if (*loopInputs.loopInputs.tc).verb == "ks" {
			assert.FatalIf(t, len(got.Changes) != 1 || got.Changes[0]["kind"] != manifest.KindHelmRelease ||
				got.Changes[0]["name"] != "demo" ||
				len(got.Suppressed) != 0, "KS entry lost its applied HelmRelease declaration: %s", (*loopInputs.stdout))
			old := got.Changes[0]["old"].(map[string]any)["spec"].(map[string]any)
			current := got.Changes[0]["new"].(map[string]any)["spec"].(map[string]any)
			assert.Equal(t, old["interval"], any("5m"))
			assert.Equal(t, current["interval"], any("10m"))
			assert.Equal(t, old["valuesFrom"], nil)
			assert.Diff(
				t,
				current["valuesFrom"],
				[]any{map[string]any{"kind": "ConfigMap", "name": "extra-values"}},
			)
		}

	case (*loopInputs.loopInputs.tc).change:

		for _, want := range []string{"-  message: before", "+  message: after"} {
			assert.ErrorIf(
				t,
				!strings.Contains((*loopInputs.stdout), want),
				"available change %q missing: %s",
				want,
				(*loopInputs.stdout),
			)
		}

	case (*loopInputs.loopInputs.tc).verb == "ks":

		for _, want := range []string{"--- HelmRelease apps/demo", "-  interval: 5m", "+  interval: 10m", "+  valuesFrom:", "name: extra-values"} {
			assert.FatalIf(
				t,
				!strings.Contains((*loopInputs.stdout), want),
				"applied HelmRelease change lost %q: %s",
				want,
				(*loopInputs.stdout),
			)
		}
		assert.FatalIf(
			t,
			strings.Contains((*loopInputs.stdout), "ConfigMap apps/"),
			"KS selection included chart workload: %s",
			(*loopInputs.stdout),
		)

	case (*loopInputs.code) == 0:

		assert.Equal(t, (*loopInputs.stdout), "")
	}
}
