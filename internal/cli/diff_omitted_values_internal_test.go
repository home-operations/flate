package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

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

		cli := runCLI(t, args...)
		stdout, stderr, code := cli.stdout, cli.stderr, cli.code
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
