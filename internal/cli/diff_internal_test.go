package cli

import (
	"bytes"
	"encoding/json"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diff"
	"github.com/home-operations/flate/internal/diffprep"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAppendSuppressionWarnings_Presentation(t *testing.T) {
	t.Parallel()
	duplicate := manifest.Warning{Category: manifest.WarnDuplicateDocument, Message: "shadowed"}
	suppressed := []diff.Suppression{
		{
			Parent: diff.Parent{NamedResource: testutil.HelmReleaseID("apps", "demo")},
			Side:   diff.SideCurrent,
			Count:  2,
			Reason: "missing source",
		},
	}
	want := manifest.Warning{
		Resource: suppressed[0].Parent.NamedResource, Category: manifest.WarnDiffSuppressed,
		Message: "2 objects not rendered on the current side; diff suppressed (missing source)",
	}
	assert.Diff(t, appendSuppressionWarnings(nil, suppressed), []manifest.Warning{want})
	assert.Diff(
		t,
		appendSuppressionWarnings([]manifest.Warning{duplicate}, suppressed),
		[]manifest.Warning{want, duplicate},
	)
	assert.Diff(t, appendSuppressionWarnings([]manifest.Warning{duplicate}, nil), []manifest.Warning{duplicate})
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
		t.Run(tc.name, func(t *testing.T) { runDiffOneSidedSkipSuppressedName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Diff_OneSidedSkipSuppressed_Kustomization(t *testing.T) {
	orig, current, failed := writeFixture(t), writeFixture(t), writeFixture(t)
	for _, path := range []string{orig, current, failed} {
		testutil.WriteFile(t, path, "kustomization.yaml", "resources: [flux/source.yaml, flux/cluster.yaml]\n")
		testutil.WriteFile(t, path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
		testutil.WriteFile(
			t,
			path,
			"apps/kustomization.yaml",
			"resources: [cm.yaml, hr.yaml, child.yaml]\nnamePrefix: x-\n",
		)
		testutil.WriteFile(
			t,
			path,
			"apps/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"flux-system",
				"  path: ./nested\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
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
	testutil.WriteFile(
		t,
		failed,
		"apps/kustomization.yaml",
		"resources: [cm.yaml, hr.yaml, child.yaml, missing.yaml]\nnamePrefix: x-\n",
	)
	for _, tc := range []struct {
		name, side, namespace, orig, current string
		wantCode                             int
	}{
		{name: "current", side: "current", orig: orig, current: current},
		{name: "orig", side: "orig", orig: current, current: orig},
		{name: "current/apps", side: "current", namespace: "apps", orig: orig, current: current},
		{name: "orig/apps", side: "orig", namespace: "apps", orig: current, current: orig},
		{name: "current failed", side: "current", orig: orig, current: failed, wantCode: 1},
		{name: "current failed/apps", side: "current", namespace: "apps", orig: orig, current: failed, wantCode: 1},
		{name: "orig failed/apps", side: "orig", namespace: "apps", orig: failed, current: orig, wantCode: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runCLIResult := runCLI(
				t,
				"diff",
				"all",
				"--path",
				tc.current,
				"--path-orig",
				tc.orig,
				"--namespace",
				tc.namespace,
				"--allow-missing-kinds=Secret,ConfigMap",
				"--output",
				"diff",
				"--no-progress",
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			)
			stdout, stderr, code := runCLIResult.stdout, runCLIResult.stderr, runCLIResult.code
			assert.Equal(t, code, tc.wantCode)
			parent := "Kustomization flux-system/apps:"
			if tc.namespace != "" {
				parent = "HelmRelease apps/x-demo:"
			}
			assert.ErrorIf(t, !strings.Contains(stderr, parent) ||
				!strings.Contains(
					stderr,
					"on the "+tc.side+" side; diff suppressed (",
				), "missing ownership suppression: stdout=%s stderr=%s", stdout, stderr)
			for _, name := range []string{"x-demo", "y-demo"} {
				assert.ErrorIf(
					t,
					!strings.Contains(stderr, "HelmRelease apps/"+name+":"),
					"missing suppression for %s: stdout=%s stderr=%s",
					name,
					stdout,
					stderr,
				)
			}
			assert.Equal(t, strings.Contains(stderr, "(skipped:"), false)
			assert.ErrorIf(
				t,
				strings.Contains(stdout, "ConfigMap apps/"),
				"owned resources must not appear as wholesale changes:\n%s",
				stdout,
			)
		})
	}

	t.Run("moved HelmRelease", func(t *testing.T) {
		orig, current := writeFixture(t), writeFixture(t)
		for _, path := range []string{orig, current} {
			testutil.WriteFile(
				t,
				path,
				"kustomization.yaml",
				"resources: [flux/source.yaml, flux/cluster.yaml, flux/other.yaml]\n",
			)
			testutil.WriteFile(t, path, "flux/source.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: https://example.test/cluster.git}
`)
			testutil.WriteFile(
				t,
				path,
				"flux/other.yaml",
				testutil.KustomizationYAML(
					"other",
					"flux-system",
					"  path: ./other\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				),
			)
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
		runCLIResult2 := runCLI(
			t,
			"diff",
			"all",
			"--path",
			current,
			"--path-orig",
			orig,
			"--allow-missing-kinds=Secret,ConfigMap",
			"--output",
			"diff",
			"--no-progress",
			"--cache-dir",
			t.TempDir(),
			"--concurrency",
			"2",
		)
		stdout, stderr, code := runCLIResult2.stdout, runCLIResult2.stderr, runCLIResult2.code
		assert.ErrorIf(t, code != 0 || !strings.Contains(stdout, "-  greeting: before") ||
			!strings.Contains(
				stdout,
				"+  greeting: after",
			), "moved release change must remain visible: code=%d stdout=%s stderr=%s", code, stdout, stderr)
		assert.Equal(t, strings.Contains(stdout, "# HelmRelease.helm.toolkit.fluxcd.io apps/demo:"), false)
	})
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffJSON(t *testing.T) {
	t.Run("borrows_payload", testDocumentsBorrowsPayload)
	orig, current := writeFixture(t), writeFixture(t)
	for _, root := range []string{orig, current} {
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux/cluster.yaml, flux/broken.yaml]\n")
	}
	testutil.WriteFile(t, orig, "apps/kustomization.yaml", "resources: [cm.yaml, removed.yaml]\n")
	testutil.WriteFile(
		t,
		orig,
		"apps/removed.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed, namespace: apps}\n",
	)
	testutil.WriteFile(t, current, "apps/kustomization.yaml", "resources: [cm.yaml, added.yaml]\n")
	testutil.WriteFile(
		t,
		current,
		"apps/added.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added, namespace: apps}\n",
	)
	testutil.WriteFile(
		t,
		current,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n",
	)
	for _, dir := range []string{orig, current} {
		failure := ""
		if dir == current {
			failure = "  postBuild: {substituteFrom: [{kind: ConfigMap, name: missing}]}\n"
		}
		testutil.WriteFile(
			t,
			dir,
			"flux/broken.yaml",
			testutil.KustomizationYAML(
				"broken",
				"flux-system",
				"  interval: 10m\n  path: ./broken\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			)+failure,
		)
		testutil.WriteFile(t, dir, "broken/kustomization.yaml", "resources: [cm.yaml]\n")
		testutil.WriteFile(t, dir, "broken/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: withheld}\n")
	}
	runCLIResult3 := runCLI(t, "diff", "all", "--path", current, "--path-orig", orig,
		"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json")
	stdout, stderr, code := runCLIResult3.stdout, runCLIResult3.stderr, runCLIResult3.code
	assert.Equal(t, code, 1)
	assert.Equal(t, strings.Count(stderr, "✗ 1 failed"), 1)
	var got struct {
		Changes    []map[string]any `json:"Changes"`
		Suppressed []map[string]any `json:"Suppressed"`
	}
	assert.NoError(t, json.Unmarshal([]byte(stdout), &got))
	var rows []any
	for _, c := range got.Changes {
		if rows == nil {
			rows = make([]any, 0, len(got.Changes))
		}
		rows = append(rows, []any{c["name"], c["status"], c["old"] != nil, c["new"] != nil})
	}
	assert.Diff(t, rows, []any{
		[]any{"broken", "changed", true, true},
		[]any{"added", "added", false, true},
		[]any{"hello", "changed", true, true},
		[]any{"removed", "removed", true, false},
	})
	assert.FatalIf(
		t,
		len(got.Changes) != 4 || len(got.Suppressed) != 1,
		"unexpected changes or suppressions: %s",
		stdout,
	)
	assert.Equal(t, got.Suppressed[0]["side"], any("current"))
	assert.Diff(
		t,
		slices.Sorted(maps.Keys(got.Changes[0])),
		[]string{"apiVersion", "kind", "name", "namespace", "new", "newChart", "old", "oldChart", "parent", "status"},
	)
	assert.Diff(t, got.Changes[0]["parent"], any(map[string]any{"kind": "", "name": "", "path": "."}))
	assert.Diff(t, slices.Sorted(maps.Keys(got.Suppressed[0])), []string{"count", "parent", "reason", "side"})
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_DiffJSON_Empty(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	path := writeFixture(t)
	for _, verb := range []string{"ks", "hr", "all"} {
		t.Run(verb, func(t *testing.T) {
			runCLIResult4 := runCLI(t, "diff", verb, "--path", path, "--path-orig", path,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json")
			stdout, stderr, code := runCLIResult4.stdout, runCLIResult4.stderr, runCLIResult4.code
			assert.Equal(t, code, 0)
			assert.Equal(t, strings.Contains(stderr, persistenceMessage), true)
			assert.Equal(t, strings.Count(stderr, "--path and --path-orig resolve to the same root"), 2)
			assert.Equal(t, strings.Count(stderr, "snapshot:"), 2)
			var compact bytes.Buffer
			assert.NoError(t, json.Compact(&compact, []byte(stdout)))
			assert.Equal(t, compact.String(), `{"changes":[],"suppressed":[]}`)
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
		t.Run(tc.name, func(t *testing.T) { runDiffOmittedValuesName(t, &tc) })
	}
}

func writeCustomSourceDiffFixture(t *testing.T, sourceURL string) diffFixture {
	t.Helper()
	identity := testutil.SelfSignedClientCert(t).Key
	signer, err := ssh.ParsePrivateKey([]byte(identity))
	assert.NoError(t, err)
	credential, err := json.Marshal(testutil.ObjectDocument(&manifest.Secret{
		Name: "source-auth", Namespace: "gitops-system", StringData: map[string]any{
			"identity": identity, "known_hosts": knownhosts.Line([]string{"github.com"}, signer.PublicKey()),
			"username": "fixture", "password": "fixture",
		},
	}))
	assert.NoError(t, err)
	var current string
	var orig string
	current = t.TempDir()
	testutil.WriteFile(t, current, "kustomization.yaml", "resources: [flux]\n")
	testutil.WriteFile(t, current, "flux/kustomization.yaml", "resources: [cluster.yaml, second.yaml]\n")
	testutil.WriteFile(t, current, "flux/cluster.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: home-kubernetes, namespace: gitops-system}
spec:
  url: `+sourceURL+`
  interval: 1h
  secretRef: {name: source-auth}
`+"---\n"+
		testutil.KustomizationYAML("apps", "gitops-system", `  path: ./apps
  sourceRef: {kind: GitRepository, name: home-kubernetes}
  interval: 1h
`)+"---\n"+string(credential)+"\n")
	testutil.WriteFile(t, current, "apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, current, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hi}
`)
	testutil.WriteFile(
		t,
		current,
		"flux/second.yaml",
		testutil.KustomizationYAML(
			"second-apps",
			"gitops-system",
			"  path: ./second-apps\n  sourceRef: {kind: GitRepository, name: home-kubernetes}\n  interval: 1h\n",
		),
	)
	testutil.WriteFile(t, current, "second-apps/kustomization.yaml", "resources:\n- cm.yaml\n")
	testutil.WriteFile(t, current, "second-apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: second, namespace: apps}
data: {greeting: hi}
`)
	orig = t.TempDir()
	testutil.CopyTree(t, current, orig)
	writeBaselineGit(t, current, "git@github.com:example/home-ops.git")
	return diffFixture{current: current, orig: orig}
}

//nolint:paralleltest // test changes the process environment or working directory.
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
			writeCustomSourceDiffFixtureResult := writeCustomSourceDiffFixture(
				t,
				"ssh://git@github.com/example/home-ops.git",
			)
			current, orig := writeCustomSourceDiffFixtureResult.current, writeCustomSourceDiffFixtureResult.orig
			if tc.changed {
				testutil.WriteFile(t, orig, "apps/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: hello, namespace: apps}
data: {greeting: hola}
`)
			}
			runCLIResult5 := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
			stdout, stderr, code := runCLIResult5.stdout, runCLIResult5.stderr, runCLIResult5.code
			assert.FatalIf(t, code != 0, "diff exited %d: %s", code, stderr)
			if tc.changed {
				assert.Equal(t, stderr, persistenceNotice)
			} else {
				assert.Equal(t, strings.Contains(stderr, persistenceMessage), true)
				assert.Equal(t, strings.Count(stderr, "no changes detected"), 2)
				assert.Equal(t, strings.Count(stderr, "snapshot:"), 2)
			}
			switch {
			case tc.changed:

				for _, want := range []string{"-  greeting: hola", "+  greeting: hi"} {
					assert.ErrorIf(
						t,
						!strings.Contains(stdout, want),
						"expected a real field change %q, got:\n%s",
						want,
						stdout,
					)
				}

			case stdout != "":

				t.Errorf("identical snapshots should have no diff:\n%s", stdout)
			}
		})
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_DiffKS_ExternalSourceDisclosure(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	writeCustomSourceDiffFixtureResult2 := writeCustomSourceDiffFixture(
		t,
		"ssh://git@github.com/example/other-repo.git",
	)
	current, orig := writeCustomSourceDiffFixtureResult2.current, writeCustomSourceDiffFixtureResult2.orig
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
			args := append([]string{
				"diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", filepath.Join(t.TempDir(), "cache"), "-o", "diff",
			}, tc.args...)
			runCLIResult6 := runCLI(t, args...)
			stdout, stderr, code := runCLIResult6.stdout, runCLIResult6.stderr, runCLIResult6.code
			assert.FatalIf(t, code != 0, "diff exited %d: %s", code, stderr)
			assert.ErrorIf(t, stdout != "", "external source must not render local manifests:\n%s", stdout)
			if tc.warn {
				for _, want := range []string{"orig snapshot", "current snapshot", "Kustomization gitops-system/apps", "external source", "GitRepository.source.toolkit.fluxcd.io/gitops-system/home-kubernetes", "not rendered"} {
					assert.ErrorIf(t, !strings.Contains(stderr, want), "missing disclosure %q: %s", want, stderr)
				}
			} else {
				assert.Equal(t, stderr, persistenceNotice)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_DiffKS_PathOrigRepositoryIdentity(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) { runDiffKSPathOrigRepositoryIdentityName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
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
			writeCustomSourceDiffFixtureResult3 := writeCustomSourceDiffFixture(t, currentURL)
			current, orig := writeCustomSourceDiffFixtureResult3.current, writeCustomSourceDiffFixtureResult3.orig
			writeBaselineGit(t, current, currentURL)
			writeBaselineGit(t, orig, tc.urls...)
			testutil.WriteFile(
				t,
				orig,
				"apps/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: hola}\n",
			)
			c := commonFlags{path: current, pathOrig: orig}
			if c.owner == nil {
				c.owner = cachetest.New(t, "", sourcecache.New)
			}
			err := resolveBaseline(t.Context(), &c, false)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				len(c.pathOrigSelfURLs) != len(tc.urls) || (len(tc.urls) != 0 && c.pathOrigSelfURLs[0] != tc.urls[0]),
				"baseline borrowed current identity: %v",
				c.pathOrigSelfURLs,
			)
			runCLIResult7 := runCLI(t, "diff", "ks", "--path", current, "--path-orig", orig,
				"--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "diff")
			stdout, stderr, code := runCLIResult7.stdout, runCLIResult7.stderr, runCLIResult7.code
			assert.FatalIf(t, code != 0, "diff exited %d: %s", code, stderr)
			assert.ErrorIf(
				t,
				strings.Contains(stdout, "hola"),
				"external baseline rendered local manifests:\n%s",
				stdout,
			)
			assert.ErrorIf(t, !strings.Contains(stderr, "Kustomization gitops-system/apps: external source") ||
				strings.Contains(
					stderr,
					"current snapshot",
				), "want only baseline external-source disclosure: %s", stderr)
		})
	}
}

func testDocumentsBorrowsPayload(t *testing.T) {
	t.Helper()
	payload := strings.Repeat("immutable payload", 1<<20)
	doc := map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "large"},
		"data": map[string]any{"payload": payload},
	}
	batches := []manifest.DocumentBatch{{Documents: []map[string]any{doc}}}
	projected := diffprep.Documents(batches)
	if reflect.ValueOf(projected[0].Manifest).Pointer() != reflect.ValueOf(doc).Pointer() {
		t.Fatal("projection copied the completed document")
	}
	borrowed := projected[0].Manifest["data"].(map[string]any)["payload"].(string)
	if unsafe.StringData(borrowed) != unsafe.StringData(payload) {
		t.Fatal("projection copied the immutable payload")
	}
	allocs := testing.AllocsPerRun(2, func() {
		if got := diffprep.Documents(batches); len(got) != 1 {
			t.Fatal("projection lost the document")
		}
	})
	if allocs != 1 {
		t.Fatalf("projection allocations = %v, want one document slice and zero payload copies or decodes", allocs)
	}
}

type diffFixture struct{ current, orig string }
