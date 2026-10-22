package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
)

func TestRun_Diff_OneSidedSkipSuppressed(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, tc := range []struct {
		name       string
		orig       string
		current    string
		other      bool
		json       bool
		wantSide   string
		wantRemove bool
	}{
		{name: "current skipped", orig: "local", current: "skipped", wantSide: "current"},
		{name: "orig skipped", orig: "skipped", current: "local", wantSide: "orig"},
		{name: "unrelated change", orig: "local", current: "skipped", other: true, wantSide: "current"},
		{name: "release removed", orig: "local", current: "absent", wantRemove: true},
		{name: "json", orig: "local", current: "skipped", json: true, wantSide: "current"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			write := func(source, message string) string {
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
metadata: {name: '{{ .Release.Name }}', namespace: apps}
data:
  greeting: {{ .Values.greeting }}
  image: ghcr.io/demo/private:v1
---
apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}-second', namespace: apps}
data:
  image: ghcr.io/demo/private:v1
`)
				chart := `  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
				if tc.other {
					testutil.WriteFile(t, path, "flux/other.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: other, namespace: apps}
spec:
`+chart+`  values: {greeting: `+message+`}
`)
				}
				if source == "skipped" {
					testutil.WriteFile(t, path, "flux/oci.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: private-chart, namespace: apps}
spec:
  url: oci://example.invalid/private/chart
  certSecretRef: {name: missing-ca}
`)
					chart = "  chartRef: {kind: OCIRepository, name: private-chart}\n"
				}
				if source != "absent" {
					testutil.WriteFile(t, path, "flux/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
`+chart+"  values: {greeting: hello}\n")
				}
				return path
			}
			orig := write(tc.orig, "before")
			current := write(tc.current, "after")
			format := "diff"
			if tc.json {
				format = "json"
			}
			stdout, stderr, code := runCLI(t, "diff", "hr", "--path", current, "--path-orig", orig,
				"--allow-missing-secrets", "--output", format, "--no-progress", "--cache-dir", t.TempDir(), "--concurrency", "2")
			if code != 0 {
				t.Fatalf("diff hr exited %d: stderr=%s", code, stderr)
			}
			images, imageStderr, imageCode := runCLI(t, "diff", "images", "--include-removed", "--path", current, "--path-orig", orig,
				"--allow-missing-secrets", "--no-progress", "--cache-dir", t.TempDir(), "--concurrency", "2")
			assert.Equal(t, imageCode, 0)
			if tc.wantRemove {
				assert.Equal(t, images, "ghcr.io/demo/private:v1\n")
			} else {
				assert.Equal(t, images, "")
			}
			if tc.wantSide != "" || tc.json {
				side := tc.wantSide
				if side == "" {
					side = "current"
				}
				prefix := "HelmRelease.helm.toolkit.fluxcd.io apps/demo: 2 objects not rendered on the " + side + " side; diff suppressed ("
				if strings.Count(imageStderr, prefix) != 1 || strings.Contains(imageStderr, "×2") || !strings.Contains(imageStderr, "missing-ca") {
					t.Fatalf("image suppression missing: %s", imageStderr)
				}
				writeErr := errors.New("image disclosure write failed")
				cmd := New("test")
				cmd.SetContext(t.Context())
				cmd.SetArgs([]string{"diff", "images", "--path", current, "--path-orig", orig,
					"--allow-missing-secrets", "--cache-dir", t.TempDir(), "--concurrency", "2"})
				cmd.SetOut(&bytes.Buffer{})
				cmd.SetErr(failingWriter{err: writeErr})
				if err := cmd.Execute(); !errors.Is(err, writeErr) {
					t.Fatalf("image disclosure writer error lost: %v", err)
				}
			}
			if tc.json {
				var got struct {
					Changes    []diff.Change
					Suppressed []diff.Suppression
				}
				if err := json.Unmarshal([]byte(stdout), &got); err != nil {
					t.Fatal(err)
				}
				assert.Equal(t, len(got.Changes), 0)
				if len(got.Suppressed) != 1 {
					t.Fatalf("expected one suppression, got:\n%s", stdout)
				}
				assert.Equal(t, got.Suppressed[0].Side, diff.SideCurrent)
				assert.Equal(t, got.Suppressed[0].Count, 2)
				assert.Equal(t, got.Suppressed[0].Parent, diff.Parent{NamedResource: testutil.HelmReleaseID("apps", "demo")})
				assert.Equal(t, strings.Contains(got.Suppressed[0].Reason, "missing-ca"), true)
				assert.Equal(t, strings.HasPrefix(got.Suppressed[0].Reason, "skipped:"), false)
				return
			}
			if tc.wantSide != "" {
				prefix := "# HelmRelease.helm.toolkit.fluxcd.io apps/demo: 2 objects not rendered on the " + tc.wantSide + " side; diff suppressed ("
				if !strings.HasPrefix(stdout, prefix) || !strings.Contains(stdout, "missing-ca") {
					t.Errorf("expected suppression with the missing Secret reason; got:\n%s", stdout)
				}
				if strings.Contains(stdout, "ConfigMap apps/demo") {
					t.Errorf("skipped release must not appear as added or removed:\n%s", stdout)
				}
			} else if strings.Contains(stdout, "diff suppressed") {
				t.Errorf("a complete comparison must not be suppressed:\n%s", stdout)
			}
			if tc.wantRemove && !strings.Contains(stdout, "-  greeting: hello") {
				t.Errorf("actual release removal must remain visible:\n%s", stdout)
			}
			if tc.other && (!strings.Contains(stdout, "-  greeting: before") || !strings.Contains(stdout, "+  greeting: after")) {
				t.Errorf("unrelated change must remain visible:\n%s", stdout)
			}
		})
	}
}

func TestRun_Diff_OneSidedSkipSuppressed_Kustomization(t *testing.T) {
	orig, current, failed := writeFixture(t), writeFixture(t), writeFixture(t)
	for _, path := range []string{orig, current, failed} {
		testutil.WriteFile(t, path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
		testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [cm.yaml, hr.yaml, child.yaml]\nnamePrefix: x-\n")
		testutil.WriteFile(t, path, "apps/child.yaml", testutil.KustomizationYAML("child", "flux-system", "  path: ./nested\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
		hr := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`
		testutil.WriteFile(t, path, "apps/hr.yaml", hr)
		testutil.WriteFile(t, path, "nested/hr.yaml", hr)
		testutil.WriteFile(t, path, "nested/kustomization.yaml", "resources: [hr.yaml]\nnamePrefix: y-\n")
		testutil.WriteFile(t, path, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
		testutil.WriteFile(t, path, "charts/demo/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}', namespace: apps}
`)
	}
	testutil.WriteFile(t, current, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: private, namespace: flux-system}
spec:
  url: oci://example.invalid/private/manifests
  certSecretRef: {name: missing-ca}
`+"---\n"+
		testutil.KustomizationYAML("apps", "flux-system", `  path: ./apps
  sourceRef: {kind: OCIRepository, name: private}
`))
	testutil.WriteFile(t, failed, "apps/kustomization.yaml", "resources: [cm.yaml, hr.yaml, child.yaml, missing.yaml]\nnamePrefix: x-\n")
	for _, tc := range []struct {
		name, side, namespace, orig, current string
		wantCode                             int
	}{
		{name: "current", side: "current", orig: orig, current: current},
		{name: "orig", side: "orig", orig: current, current: orig},
		{name: "current/apps", side: "current", namespace: "apps", orig: orig, current: current},
		{name: "orig/apps", side: "orig", namespace: "apps", orig: current, current: orig},
		{name: "current failed", side: "current", orig: orig, current: failed, wantCode: 1},
		{name: "current failed/apps", side: "current", namespace: "apps", orig: orig, current: failed},
		{name: "orig failed/apps", side: "orig", namespace: "apps", orig: failed, current: orig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, code := runCLI(t, "diff", "all", "--path", tc.current, "--path-orig", tc.orig,
				"--namespace", tc.namespace, "--allow-missing-secrets", "--output", "diff", "--no-progress", "--cache-dir", t.TempDir(), "--concurrency", "2")
			assert.Equal(t, code, tc.wantCode)
			parent := "# Kustomization.kustomize.toolkit.fluxcd.io flux-system/apps:"
			if tc.namespace != "" {
				parent = "# HelmRelease.helm.toolkit.fluxcd.io apps/x-demo:"
			}
			if !strings.Contains(stdout, parent) || !strings.Contains(stdout, "on the "+tc.side+" side; diff suppressed (") {
				t.Errorf("missing ownership suppression: stdout=%s stderr=%s", stdout, stderr)
			}
			for _, name := range []string{"x-demo", "y-demo"} {
				assert.Equal(t, strings.Contains(stdout, "# HelmRelease.helm.toolkit.fluxcd.io apps/"+name+":"), true)
			}
			assert.Equal(t, strings.Contains(stdout, "(skipped:"), false)
			if strings.Contains(stdout, "ConfigMap apps/") {
				t.Errorf("owned resources must not appear as wholesale changes:\n%s", stdout)
			}
		})
	}

	t.Run("moved HelmRelease", func(t *testing.T) {
		orig, current := writeFixture(t), writeFixture(t)
		for _, path := range []string{orig, current} {
			testutil.WriteFile(t, path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
			testutil.WriteFile(t, path, "flux/other.yaml", testutil.KustomizationYAML("other", "flux-system", "  path: ./other\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			dir, greeting := "apps", "before"
			if path == current {
				dir, greeting = "other", "after"
				testutil.WriteFile(t, path, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: private, namespace: flux-system}
spec:
  url: oci://example.invalid/private/manifests
  certSecretRef: {name: missing-ca}
`+"---\n"+
					testutil.KustomizationYAML("apps", "flux-system", `  path: ./apps
  sourceRef: {kind: OCIRepository, name: private}
`))
			}
			testutil.WriteFile(t, path, "other/kustomization.yaml", "resources: []\n")
			testutil.WriteFile(t, path, dir+"/kustomization.yaml", "resources: [hr.yaml]\n")
			testutil.WriteFile(t, path, dir+"/hr.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/demo
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  values: {greeting: `+greeting+`}
`)
			testutil.WriteFile(t, path, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
			testutil.WriteFile(t, path, "charts/demo/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: '{{ .Release.Name }}', namespace: apps}
data:
  greeting: {{ .Values.greeting }}
`)
		}
		stdout, stderr, code := runCLI(t, "diff", "all", "--path", current, "--path-orig", orig,
			"--allow-missing-secrets", "--output", "diff", "--no-progress", "--cache-dir", t.TempDir(), "--concurrency", "2")
		if code != 0 || !strings.Contains(stdout, "-  greeting: before") || !strings.Contains(stdout, "+  greeting: after") {
			t.Errorf("moved release change must remain visible: code=%d stdout=%s stderr=%s", code, stdout, stderr)
		}
		assert.Equal(t, strings.Contains(stdout, "# HelmRelease.helm.toolkit.fluxcd.io apps/demo:"), false)
	})

}

func TestRun_DiffJSON(t *testing.T) {
	orig, current := writeFixture(t), writeFixture(t)
	testutil.WriteFile(t, orig, "apps/kustomization.yaml", "resources: [cm.yaml, removed.yaml]\n")
	testutil.WriteFile(t, orig, "apps/removed.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed, namespace: apps}\n")
	testutil.WriteFile(t, current, "apps/kustomization.yaml", "resources: [cm.yaml, added.yaml]\n")
	testutil.WriteFile(t, current, "apps/added.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added, namespace: apps}\n")
	testutil.WriteFile(t, current, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n")
	for _, dir := range []string{orig, current} {
		failure := ""
		if dir == current {
			failure = "  postBuild: {substituteFrom: [{kind: ConfigMap, name: missing}]}\n"
		}
		testutil.WriteFile(t, dir, "flux/broken.yaml", testutil.KustomizationYAML("broken", "flux-system", "  interval: 10m\n  path: ./broken\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")+failure)
		testutil.WriteFile(t, dir, "broken/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, dir, "broken/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: withheld}\n")
	}
	stdout, stderr, code := runCLI(t, "diff", "all", "--path", current, "--path-orig", orig,
		"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json")
	assert.Equal(t, code, 1)
	assert.Equal(t, strings.Count(stderr, "flate error:"), 1)
	var got struct{ Changes, Suppressed []map[string]any }
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatal(err)
	}
	var rows []any
	for _, c := range got.Changes {
		rows = append(rows, []any{c["name"], c["status"], c["old"] != nil, c["new"] != nil})
	}
	assert.Diff(t, rows, []any{
		[]any{"added", "added", false, true},
		[]any{"hello", "changed", true, true},
		[]any{"removed", "removed", true, false},
	})
	if len(got.Changes) != 3 || len(got.Suppressed) != 1 {
		t.Fatalf("unexpected changes or suppressions: %s", stdout)
	}
	assert.Equal(t, got.Suppressed[0]["side"], any("current"))
	assert.Diff(t, slices.Sorted(maps.Keys(got.Changes[0])), []string{"apiVersion", "kind", "name", "namespace", "new", "newChart", "old", "oldChart", "parent", "status"})
	assert.Diff(t, slices.Sorted(maps.Keys(got.Changes[0]["parent"].(map[string]any))), []string{"group", "kind", "name", "namespace", "path"})
	assert.Diff(t, slices.Sorted(maps.Keys(got.Suppressed[0])), []string{"count", "parent", "reason", "side"})
}

func TestRun_DiffJSON_Empty(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	path := writeFixture(t)
	for _, verb := range []string{"ks", "hr", "all"} {
		t.Run(verb, func(t *testing.T) {
			stdout, stderr, code := runCLI(t, "diff", verb, "--path", path, "--path-orig", path,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json")
			assert.Equal(t, code, 0)
			assert.Equal(t, strings.HasPrefix(stderr, persistenceNotice), true)
			assert.Equal(t, strings.Count(stderr, "--path and --path-orig resolve to the same root"), 2)
			assert.Equal(t, strings.Count(stderr, "snapshot:"), 2)
			var compact bytes.Buffer
			if err := json.Compact(&compact, []byte(stdout)); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, compact.String(), `{"changes":[],"suppressed":[]}`)
		})
	}
}

func TestRun_Diff_OmittedValues(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		kind                  string
		verb, resource        string
		namespace             string
		origRef, currentRef   bool
		change                bool
		wantOrig, wantCurrent bool
		wantCode              int
	}{
		{name: "current omitted", kind: "ConfigMap", currentRef: true, wantCurrent: true},
		{name: "orig omitted", kind: "ConfigMap", origRef: true, wantOrig: true},
		{name: "both omitted", kind: "Secret", origRef: true, currentRef: true, wantOrig: true, wantCurrent: true},
		{name: "changed output", kind: "Secret", currentRef: true, change: true, wantCurrent: true},
		{name: "named release", kind: "ConfigMap", resource: "demo", currentRef: true, wantCurrent: true},
		{name: "other release", kind: "ConfigMap", resource: "other", currentRef: true, wantCode: 1},
		{name: "other namespace", kind: "ConfigMap", namespace: "flux-system", currentRef: true},
		{name: "other kind", kind: "ConfigMap", verb: "ks", currentRef: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			for _, output := range []string{"diff", "json"} {
				verb := "hr"
				if tc.verb != "" {
					verb = tc.verb
				}
				args := []string{"diff", verb, "--path", current, "--path-orig", orig,
					"--cache-dir", cache, "--concurrency", "2", "--output", output, "--no-progress", "--allow-missing-secrets"}
				if tc.resource != "" {
					args = append(args, tc.resource)
				}
				if tc.namespace != "" {
					args = append(args, "--namespace", tc.namespace)
				}

				stdout, stderr, code := runCLI(t, args...)
				assert.Equal(t, code, tc.wantCode)
				assert.Equal(t, strings.Contains(stderr, "values not used by the chart"), tc.namespace == "")
				assert.Equal(t, strings.Contains(stdout, "flate warning:"), false)
				for _, side := range []struct {
					name string
					want bool
				}{{"orig", tc.wantOrig}, {"current", tc.wantCurrent}} {
					_, block, found := strings.Cut(stderr, side.name+" snapshot:")
					block, _, _ = strings.Cut(block, "snapshot:")
					note := "HelmRelease.helm.toolkit.fluxcd.io apps/demo: required valuesFrom " + tc.kind + "/apps/extra-values omitted from offline render"
					assert.Equal(t, found && strings.Contains(block, note), side.want)
				}
				if tc.resource == "other" {
					assert.Equal(t, stdout, "")
					continue
				}
				if output == "json" {
					var got struct{ Changes, Suppressed []map[string]any }
					if err := json.Unmarshal([]byte(stdout), &got); err != nil {
						t.Fatal(err)
					}
					assert.Equal(t, len(got.Changes) > 0, tc.change)
				} else if tc.change {
					for _, want := range []string{"-  message: before", "+  message: after"} {
						if !strings.Contains(stdout, want) {
							t.Errorf("available change %q missing: %s", want, stdout)
						}
					}
				} else if code == 0 {
					assert.Equal(t, stdout, "")
				}
			}
		})
	}
}

func writeCustomSourceDiffFixture(t *testing.T, sourceURL string) (current, orig string) {
	t.Helper()
	current = t.TempDir()
	testutil.WriteFile(t, current, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: home-kubernetes, namespace: gitops-system}
spec:
  url: `+sourceURL+`
  interval: 1h
`+"---\n"+
		testutil.KustomizationYAML("apps", "gitops-system", `  path: ./apps
  sourceRef: {kind: GitRepository, name: home-kubernetes}
  interval: 1h
`))
	testutil.WriteFile(t, current, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, current, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hi}
`)
	testutil.WriteFile(t, current, "flux/second.yaml", testutil.KustomizationYAML("second-apps", "gitops-system", "  path: ./second-apps\n  sourceRef: {kind: GitRepository, name: home-kubernetes}\n  interval: 1h\n"))
	testutil.WriteFile(t, current, "second-apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, current, "second-apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: second, namespace: apps}
data: {greeting: hi}
`)
	orig = t.TempDir()
	testutil.CopyTree(t, current, orig)
	writeBaselineGit(t, current, "git@github.com:example/home-ops.git")
	return current, orig
}

func TestRun_DiffKS_PathOrigCustomSource(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, tc := range []struct {
		name    string
		changed bool
	}{
		{name: "identical"},
		{name: "changed", changed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/home-ops.git")
			if tc.changed {
				testutil.WriteFile(t, orig, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hola}
`)
			}
			stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			if tc.changed {
				assert.Equal(t, stderr, persistenceNotice)
			} else {
				assert.Equal(t, strings.HasPrefix(stderr, persistenceNotice), true)
				assert.Equal(t, strings.Count(stderr, "no changes detected"), 2)
				assert.Equal(t, strings.Count(stderr, "snapshot:"), 2)
			}
			if tc.changed {
				for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
					if !strings.Contains(stdout, want) {
						t.Errorf("expected a real field change %q, got:\n%s", want, stdout)
					}
				}
			} else if stdout != "" {
				t.Errorf("identical snapshots should have no diff:\n%s", stdout)
			}
		})
	}
}

func TestRun_DiffKS_ExternalSourceDisclosure(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/other-repo.git")
	testutil.WriteFile(t, orig, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hola}
`)
	for _, tc := range []struct {
		name string
		args []string
		warn bool
	}{
		{name: "default", warn: true},
		{name: "error log level", args: []string{"--log-level", "error"}, warn: true},
		{name: "other namespace", args: []string{"--namespace", "other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", filepath.Join(t.TempDir(), "cache"), "-o", "diff"}, tc.args...)
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			if stdout != "" {
				t.Errorf("external source must not render local manifests:\n%s", stdout)
			}
			if tc.warn {
				for _, want := range []string{"orig snapshot", "current snapshot", "Kustomization.kustomize.toolkit.fluxcd.io gitops-system/apps", "external source", "GitRepository.source.toolkit.fluxcd.io/gitops-system/home-kubernetes", "not rendered"} {
					if !strings.Contains(stderr, want) {
						t.Errorf("missing disclosure %q: %s", want, stderr)
					}
				}
			} else {
				assert.Equal(t, stderr, persistenceNotice)
			}
		})
	}
}

func TestRun_DiffKS_PathOrigRepositoryIdentity(t *testing.T) {
	const currentURL = "ssh://git@github.com/example/home-ops.git"
	const baselineURL = "https://example.invalid/upstream.git"
	for _, tc := range []struct {
		name     string
		linked   bool
		nested   bool
		matching bool
	}{
		{name: "different origins"},
		{name: "matching origins", matching: true},
		{name: "linked worktree", linked: true},
		{name: "nested scan", nested: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := writeCustomSourceDiffFixture(t, currentURL)
			writeBaselineGit(t, current, currentURL)
			url := baselineURL
			if tc.matching {
				url = currentURL
			}
			cluster, err := os.ReadFile(filepath.Join(orig, "flux", "cluster.yaml"))
			if err != nil {
				t.Fatal(err)
			}
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
				testutil.WriteFile(t, orig, app+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n")
			}
			for _, dir := range []string{current, orig} {
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
				testutil.WriteFile(t, dir, "external/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: must-not-render}\n")
			}
			currentScan, origScan := current, orig
			if tc.nested {
				currentScan, origScan = filepath.Join(current, "flux"), filepath.Join(orig, "flux")
			}
			c := commonFlags{path: currentScan, pathOrig: origScan}
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer func() {
				if err := cleanup(); err != nil {
					t.Errorf("baseline cleanup: %v", err)
				}
			}()
			if err != nil {
				t.Fatal(err)
			}
			if len(c.pathOrigSelfURLs) != 1 || c.pathOrigSelfURLs[0] != url {
				t.Fatalf("baseline identity = %v, want [%s]; stopping before render to prevent an external fetch", c.pathOrigSelfURLs, url)
			}
			args := []string{"diff", "ks", "--path", currentScan, "--path-orig", origScan,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff"}
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
				if count := strings.Count(stdout, want); count != 2 {
					t.Errorf("field diff %q occurred %d times, want 2:\n%s", want, count, stdout)
				}
			}
			for _, unwanted := range []string{"must-not-render", "+apiVersion:", "-apiVersion:", "suppressed"} {
				if strings.Contains(stdout, unwanted) {
					t.Errorf("unexpected %q in field diff:\n%s", unwanted, stdout)
				}
			}
			if strings.Count(stderr, "external source") != 2 || strings.Contains(stderr, "home-kubernetes") {
				t.Errorf("want only unrelated external-source disclosures: %s", stderr)
			}
			if tc.name == "different origins" {
				secondOut, secondErr, secondCode := runCLI(t, args...)
				if secondOut != stdout || secondErr != stderr || secondCode != code {
					t.Errorf("repeated diff changed output or exit code:\n%s\n%s", secondOut, secondErr)
				}
			}
		})
	}
}

func TestRun_DiffKS_PathOrigCurrentOnlySource(t *testing.T) {
	const currentURL = "ssh://git@github.com/example/home-ops.git"
	for _, tc := range []struct {
		name string
		urls []string
	}{
		{name: "baseline without remotes"},
		{name: "current-only URL", urls: []string{"https://example.invalid/upstream.git"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := writeCustomSourceDiffFixture(t, currentURL)
			writeBaselineGit(t, current, currentURL)
			writeBaselineGit(t, orig, tc.urls...)
			testutil.WriteFile(t, orig, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n")
			c := commonFlags{path: current, pathOrig: orig}
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer func() {
				if err := cleanup(); err != nil {
					t.Errorf("baseline cleanup: %v", err)
				}
			}()
			if err != nil {
				t.Fatal(err)
			}
			if len(c.pathOrigSelfURLs) != len(tc.urls) || (len(tc.urls) != 0 && c.pathOrigSelfURLs[0] != tc.urls[0]) {
				t.Fatalf("baseline borrowed current identity: %v", c.pathOrigSelfURLs)
			}
			stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
			if code != 0 {
				t.Fatalf("diff exited %d: %s", code, stderr)
			}
			if strings.Contains(stdout, "hola") {
				t.Errorf("external baseline rendered local manifests:\n%s", stdout)
			}
			if !strings.Contains(stderr, "Kustomization.kustomize.toolkit.fluxcd.io gitops-system/apps: external source") || strings.Contains(stderr, "current snapshot") {
				t.Errorf("want only baseline external-source disclosure: %s", stderr)
			}
		})
	}
}

func TestRun_DiffKS_NamedExternalSourceDisclosure(t *testing.T) {
	current, orig := writeCustomSourceDiffFixture(t, "ssh://git@github.com/example/home-ops.git")
	for _, dir := range []string{current, orig} {
		for _, rel := range []string{"flux/cluster.yaml", "flux/second.yaml"} {
			data, err := os.ReadFile(filepath.Join(dir, rel))
			if err != nil {
				t.Fatal(err)
			}
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
		testutil.WriteFile(t, orig, app+"/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\ndata: {greeting: hola}\n")
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
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"diff", "ks"}, tc.args...)
			args = append(args, "--path", current, "--path-orig", orig, "--cache-dir", t.TempDir(),
				"--concurrency", "2", "-o", "diff")
			stdout, stderr, code := runCLI(t, args...)
			if code != tc.code {
				t.Fatalf("diff exited %d, want %d: %s", code, tc.code, stderr)
			}
			origWarning := strings.Index(stderr, "orig snapshot:")
			currentWarning := strings.Index(stderr, "current snapshot:")
			errorAt := strings.Index(stderr, "no Kustomization named")
			if tc.warn {
				if origWarning < 0 || currentWarning <= origWarning || strings.Count(stderr, "Kustomization.kustomize.toolkit.fluxcd.io beta/external-apps: external source GitRepository.source.toolkit.fluxcd.io/beta/external") != 2 || strings.Count(stderr, "not rendered") != 2 {
					t.Errorf("want orig then current external-source disclosure: %s", stderr)
				}
				if tc.code != 0 && (errorAt < 0 || errorAt <= currentWarning) {
					t.Errorf("missing-name error must follow both warnings: %s", stderr)
				}
			} else if strings.Contains(stderr, "external source") {
				t.Errorf("unrelated external source was disclosed: %s", stderr)
			}
			if tc.code != 0 && errorAt < 0 {
				t.Errorf("missing-name error absent: %s", stderr)
			}
			if tc.fieldDiff {
				for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
					if !strings.Contains(stdout, want) {
						t.Errorf("missing local field diff %q:\n%s", want, stdout)
					}
				}
			} else if stdout != "" {
				t.Errorf("external selection produced resource output:\n%s", stdout)
			}
		})
	}
}

func TestRun_Diff_ProducerDispositionAndPath(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		for _, mode := range []string{"both-failed", "both-suspended", "path-only", "path-content"} {
			t.Run(workers+"/"+mode, func(t *testing.T) {
				orig, current := writeFixture(t), writeFixture(t)
				switch mode {
				case "both-failed":
					testutil.WriteFile(t, orig, "apps/kustomization.yaml", "resources: [cm.yaml, missing-before.yaml]\n")
					testutil.WriteFile(t, current, "apps/kustomization.yaml", "resources: [cm.yaml, missing-after.yaml]\n")
				case "both-suspended":
					appendScopeSpec(t, orig, "flux/cluster.yaml", "  suspend: true\n  postBuild: {substitute: {CONTROL: before}}\n")
					appendScopeSpec(t, current, "flux/cluster.yaml", "  suspend: true\n  postBuild: {substitute: {CONTROL: after}}\n")
				default:
					if err := os.Rename(filepath.Join(current, "apps"), filepath.Join(current, "moved")); err != nil {
						t.Fatal(err)
					}
					body, err := os.ReadFile(filepath.Join(current, "flux/cluster.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					testutil.WriteFile(t, current, "flux/cluster.yaml", strings.Replace(string(body), "./apps", "./moved", 1))
					if mode == "path-content" {
						testutil.WriteFile(t, current, "moved/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: changed}\n")
					}
				}
				stdout, stderr, code := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
					"--output", "json", "--cache-dir", t.TempDir(), "--concurrency", workers)
				wantCode := 0
				if mode == "both-failed" {
					wantCode = 1
				}
				if code != wantCode {
					t.Fatalf("exit=%d want=%d stderr=%s", code, wantCode, stderr)
				}
				var out struct {
					Changes    []diff.Change
					Suppressed []diff.Suppression
				}
				if err := json.Unmarshal([]byte(stdout), &out); err != nil {
					t.Fatalf("JSON %q: %v", stdout, err)
				}
				switch mode {
				case "both-failed", "both-suspended":
					if len(out.Changes) != 0 || len(out.Suppressed) != 1 || out.Suppressed[0].Side != "both" || out.Suppressed[0].Count != 0 {
						t.Fatalf("unavailable producer disclosure: %s", stdout)
					}
				case "path-only":
					if len(out.Changes) != 0 || len(out.Suppressed) != 0 {
						t.Fatalf("path-only move became a change: %s", stdout)
					}
				case "path-content":
					if len(out.Changes) != 1 || out.Changes[0].Status != diff.StatusChanged || out.Changes[0].Parent.Path != "moved" {
						t.Fatalf("content move did not pair with current path: %s", stdout)
					}
				}
			})
		}
	}
}
