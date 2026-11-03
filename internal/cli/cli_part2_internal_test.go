package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

// TestBindCommon_DefaultValues sanity-checks the persistent flag set:
// every common flag binds with a known-good default.
func TestBindCommon_DefaultValues(t *testing.T) {
	t.Parallel()
	assert.Equal(t, buildOrchCfg(&commonFlags{}).SourceRetry, sourcecache.RetryConfig{
		Attempts: 3, MinWait: 200 * time.Millisecond, MaxWait: 3 * time.Second, Jitter: 0.1,
	})
	cmd := New("test")
	build, _, err := cmd.Find([]string{"build", "all"})
	assert.FatalIf(t, err != nil, "find build all: %v", err)
	for _, name := range []string{"path", "namespace", "output", "concurrency", "skip-kinds"} {
		assert.ErrorIf(t, build.Flags().Lookup(name) == nil, "expected common flag %q on `build all`", name)
	}
	var flags commonFlags
	bindCommon(pflag.NewFlagSet("test", pflag.ContinueOnError), &flags)
	assert.Diff(t, flags.skipKinds, []string{"Secret", "CustomResourceDefinition"})
	opts := buildOrchCfg(&flags).HelmOptions
	assert.Equal(t, opts.SkipCRDs, true)
	assert.Equal(t, opts.SkipTests, true)
	assert.Diff(t, opts.SkipKinds, []string{"Secret"})
}

func TestBindCommon_OnReconcilingSubcommands(t *testing.T) {
	t.Parallel()
	cmd := New("test")
	for _, tc := range []struct {
		path, use, short string
		aliases          []string
		maxArgs          int
	}{
		{path: "build", use: "build", short: "Render Flux objects to YAML (defaults to all)"},
		{path: "build all", use: "all", short: "Render all Flux producer output"},
		{path: "build ks", use: "ks [name]", short: "Render Kustomizations", aliases: []string{"kustomization", "kustomizations"}, maxArgs: 1},
		{path: "build hr", use: "hr [name]", short: "Render HelmReleases", aliases: []string{"helmrelease", "helmreleases"}, maxArgs: 1},
		{path: "test", use: "test", short: "Report Kustomization + HelmRelease reconcile status (defaults to all)"},
		{path: "test all", use: "all", short: "Validate every Kustomization, HelmRelease, and Flux source CR"},
		{path: "test ks", use: "ks [name]", short: "Validate Kustomizations", aliases: []string{"kustomization", "kustomizations"}, maxArgs: 1},
		{path: "test hr", use: "hr [name]", short: "Validate HelmReleases", aliases: []string{"helmrelease", "helmreleases"}, maxArgs: 1},
	} {
		sub, _, err := cmd.Find(strings.Fields(tc.path))
		assert.NoError(t, err)
		assert.Equal(t, sub.Use, tc.use)
		assert.Equal(t, sub.Short, tc.short)
		assert.Diff(t, sub.Aliases, tc.aliases)
		assert.FatalIf(t, sub.Args == nil, "%s: missing argument validator", tc.path)
		assert.NoError(t, sub.Args(sub, nil))
		{
			err := sub.Args(sub, []string{"one"})
			assert.ErrorIf(
				t,
				(err == nil) != (tc.maxArgs == 1),
				"%s: argument limit %d rejected/accepted one name: %v",
				tc.path,
				tc.maxArgs,
				err,
			)
		}
		{
			err := sub.Args(sub, []string{"one", "two"})
			assert.ErrorIf(t, err == nil, "%s: accepted two names", tc.path)
		}
	}
	for _, argv := range [][]string{
		{"build"},
		{"build", "ks"},
		{"build", "hr"},
		{"build", "all"},
		{"diff", "ks"},
		{"diff", "hr"},
		{"diff", "all"},
		{"diff", "images"},
		{"diff"},
		{"get"},
		{"get", "all"},
		{"get", "images"},
		{"get", "ks"},
		{"get", "hr"},
		{"test", "all"},
		{"test", "ks"},
		{"test", "hr"},
		{"test"},
	} {
		sub, _, err := cmd.Find(argv)
		assert.FatalIf(t, err != nil, "find %v: %v", argv, err)
		for _, binding := range []struct {
			name, typ, shorthand, def string
		}{
			{"kube-version", "string", "", helm.BundledKubeVersion()},
			{"api-versions", "string", "a", ""},
			{"enable-dns", "bool", "", "false"},
		} {
			flag := sub.Flags().Lookup(binding.name)
			assert.FatalIf(t, flag == nil, "%v: missing helm flag %s", argv, binding.name)
			assert.Equal(t, flag.Value.Type(), binding.typ)
			assert.Equal(t, flag.Shorthand, binding.shorthand)
			assert.Equal(t, flag.DefValue, binding.def)
			assert.ErrorIf(
				t,
				!strings.Contains(flag.Usage, envKey(binding.name)),
				"%v: missing environment usage for %s",
				argv,
				binding.name,
			)
		}
		for _, name := range []string{"path", "cache-dir"} {
			assert.ErrorIf(t, sub.Flags().Lookup(name) == nil, "%v: missing common flag %s", argv, name)
		}
		flag := sub.Flags().Lookup("disable-chart-digest-tracking")
		assert.FatalIf(
			t,
			flag == nil || flag.Value.Type() != "bool" || flag.NoOptDefVal != "true",
			"%v: missing boolean selection binding",
			argv,
		)
		assert.ErrorIf(
			t,
			!strings.Contains(flag.Usage, envKey("disable-chart-digest-tracking")),
			"%v: missing environment usage",
			argv,
		)
		assert.NoError(t, sub.ParseFlags([]string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--enable-dns"}))
		for name, want := range map[string]string{"kube-version": "1.31.2", "api-versions": "fixture.example/v1", "enable-dns": "true"} {
			assert.Equal(t, sub.Flags().Lookup(name).Value.String(), want)
		}
		fresh, _, err := New("test").Find(argv)
		assert.NoError(t, err)
		for _, name := range []string{"kube-version", "api-versions", "enable-dns"} {
			assert.Equal(t, fresh.Flags().Lookup(name).Value.String(), fresh.Flags().Lookup(name).DefValue)
		}
	}
	maintenance, _, err := cmd.Find([]string{"cache", "gc"})
	assert.NoError(t, err)
	assert.Equal(t, maintenance.Flags().Lookup("path") == nil, true)
	assert.Equal(t, maintenance.Flags().Lookup("cache-dir") != nil, true)
}

func TestCompareDocs_OrdersByIdentity(t *testing.T) {
	t.Parallel()
	mkDoc := func(apiVersion, kind, ns, name string) map[string]any {
		return map[string]any{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata":   map[string]any{"namespace": ns, "name": name},
		}
	}
	cases := []struct {
		name string
		a, b map[string]any
		want int
	}{
		{"kind", mkDoc("v1", "ConfigMap", "a", "x"), mkDoc("v1", "Secret", "a", "x"), -1},
		{"group", mkDoc("a.example/v1", "CM", "z", "x"), mkDoc("z.example/v1", "CM", "a", "x"), -1},
		{"core", mkDoc("v1", "CM", "z", "x"), mkDoc("a.example/v1", "CM", "a", "x"), -1},
		{"namespace", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "b", "x"), -1},
		{"name", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "a", "y"), -1},
		{"same", mkDoc("v1", "CM", "a", "x"), mkDoc("v1", "CM", "a", "x"), 0},
		{"version", mkDoc("a.example/v1", "CM", "a", "x"), mkDoc("a.example/v2", "CM", "a", "x"), 0},
		{"permissive group", mkDoc("a.example/v1/extra", "CM", "a", "x"), mkDoc("a.example/v2", "CM", "a", "x"), 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := compareDocs(tc.a, tc.b)
			assert.ErrorIf(
				t,
				(got < 0) != (tc.want < 0) || (got == 0) != (tc.want == 0),
				"compareDocs(%v, %v) = %d, want sign of %d",
				tc.a,
				tc.b,
				got,
				tc.want,
			)
		})
	}
	first := mkDoc("a.example/v1", "CM", "ns", "shared")
	second := mkDoc("a.example/v2", "CM", "ns", "shared")
	foreign := mkDoc("z.example/v1", "CM", "ns", "shared")
	core := mkDoc("v1", "CM", "ns", "shared")
	docs := []map[string]any{foreign, second, core, first}
	for range 2 {
		assert.Diff(t, emissionDocs(docs, &buildFlags{}), []map[string]any{core, second, first, foreign})
		assert.Diff(t, docs, []map[string]any{foreign, second, core, first})
	}
}

// TestEmissionDocs_OnlyKinds drops every non-CRD doc.
func TestEmissionDocs_OnlyKinds(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{
		{"kind": "ConfigMap"},
		{"kind": "CustomResourceDefinition"},
		{"kind": "Secret"},
		{"kind": "CustomResourceDefinition"},
	}
	out := emissionDocs(docs, &buildFlags{onlyKinds: []string{manifest.KindCustomResourceDefinition}})
	assert.ErrorIf(t, docs[2]["kind"] != "Secret", "emissionDocs mutated the input: %+v", docs)
	assert.ErrorIf(t, len(out) != 2, "expected 2 CRDs, got %d: %+v", len(out), out)
	for _, d := range out {
		assert.ErrorIf(t, d["kind"] != "CustomResourceDefinition", "non-CRD slipped through: %v", d)
	}
}

func TestEmissionDocs_EmptyOnNoCRDs(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{{"kind": "ConfigMap"}, {"kind": "Secret"}}
	{
		out := emissionDocs(
			docs,
			&buildFlags{onlyKinds: []string{manifest.KindCustomResourceDefinition}},
		)
		assert.ErrorIf(t, len(
			out,
		) != 0, "expected no docs for no-CRD input, got %+v", out)
	}
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

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
					stdout, stderr, code := runCLI(
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

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ConventionalIgnore(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(t, path, "kustomization.yaml", "resources: [flux/cluster.yaml]\n")
	testutil.WriteFile(
		t,
		path,
		"flux/extra.yaml",
		testutil.KustomizationYAML(
			"apps-extra",
			"flux-system",
			"  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, path, ".krmignore", "flux/extra.yaml\n")
	stdout, stderr, code := runCLI(t, "get", "ks", "--path", path, "-o", "name")
	assert.FatalIf(
		t,
		code != 0 || strings.Contains(stdout, "apps-extra") || !strings.Contains(stdout, "apps"),
		"conventional ignore: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestNamed_MatchingFailureFromOtherKind(t *testing.T) {
	root := writeFixture(t)
	testutil.WriteFile(
		t,
		root,
		"flux/ghost.yaml",
		testutil.KustomizationYAML(
			"ghost",
			"flux-system",
			"  path: ./missing-ghost\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	stdout, stderr, code := runCLI(
		t,
		"test",
		"hr",
		"ghost",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, `no HelmRelease named "ghost"`) || stdout != "",
		"other-kind diagnostic satisfied named roster: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetHRChartVersions(t *testing.T) {
	for _, tc := range []struct{ name, reference, chart, want, wantChart string }{
		{name: "digest", reference: "digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, semver: '>=1.2.0', tag: '1.2.3'", want: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantChart: "apps-chart/"},
		{name: "semver", reference: "semver: '>=1.2.0', tag: '1.2.3'", want: ">=1.2.0", wantChart: "apps-chart/"},
		{name: "tag", reference: "tag: '1.2.3'", want: "1.2.3", wantChart: "apps-chart/"},
		{name: "inline OCI", reference: "tag: '1.2.3'", chart: "chart: {kind: OCIRepository, spec: {url: 'oci://example.test/app', ref: {tag: '1.2.3'}}}", want: "1.2.3", wantChart: "apps-apps-app/"},
		{name: "own version", reference: "tag: '1.2.3'", chart: "chart: {spec: {chart: app, version: '9.9.9', sourceRef: {kind: HelmRepository, name: chart}}}", want: "9.9.9", wantChart: "apps-apps-app/app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			chart := tc.chart
			if chart == "" {
				chart = "chartRef: {kind: OCIRepository, name: chart}"
			}
			testutil.WriteFile(
				t,
				root,
				"flux.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec: {suspend: true, "+chart+"}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {suspend: true, url: 'oci://example.test/app', ref: {"+tc.reference+"}}\n",
			)
			for _, command := range []string{"get hr", "get all", "get"} {
				t.Run(command, func(t *testing.T) {
					args := append(
						strings.Fields(command),
						"--path",
						root,
						"--cache-dir",
						t.TempDir(),
						"--concurrency",
						"2",
						"-o",
						"json",
					)
					stdout, stderr, code := runCLI(t, args...)
					var rows []struct {
						Version   string `json:"version"`
						Chart     string `json:"chart"`
						SourceRef struct {
							Namespace string `json:"namespace"`
						} `json:"sourceRef"`
					}
					assert.FatalIf(t, code != 0 || json.Unmarshal([]byte(stdout), &rows) != nil || len(rows) != 1 ||
						rows[0].Version != tc.want ||
						rows[0].Chart != tc.wantChart ||
						rows[0].SourceRef.Namespace != "apps", "chart version: exit=%d stdout=%s stderr=%s want=%s chart=%s", code, stdout, stderr, tc.want, tc.wantChart)
				})
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Images_GetAndDiffFormatsMatch(t *testing.T) {
	current, orig := writeFixture(t), writeFixture(t)
	fixture := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: apps
  annotations:
    record: apiserver_request:burnrate1d
    address: 0.0.0.0:80
spec:
  selector: {matchLabels: {app: demo}}
  template:
    metadata: {labels: {app: demo}}
    spec:
      containers:
        - {name: z, image: ghcr.io/example/z:2}
        - name: a
          image: ghcr.io/example/a:1
          env:
            - {name: TOOL_IMAGE, value: ghcr.io/example/env:v1}
        - {name: untagged, image: nginx}
        - {name: duplicate, image: ghcr.io/example/z:2}
      initContainers:
        - {name: init, image: ghcr.io/example/init:1}
---
apiVersion: batch/v1
kind: CronJob
metadata: {name: job, namespace: apps}
spec:
  schedule: '0 * * * *'
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - {name: cron, image: ghcr.io/example/cron:1}
          initContainers:
            - {name: init, image: ghcr.io/example/init:1}
---
apiVersion: example.test/v1
kind: Workload
metadata:
  name: worker
  namespace: apps
  annotations:
    tag: 0.10.12@sha256:5338b99662e2e15048b4e14b49e17279d384970fdcb3c29169f509f806f49cbe
spec:
  imageName: ghcr.io/example/declared:v1
  worker:
    template:
      spec:
        containers:
          - {name: worker, image: ghcr.io/example/a:1}
        ephemeralContainers:
          - {name: debug, image: ghcr.io/example/debug:3}
`
	testutil.WriteFile(t, current, "apps/cm.yaml", fixture)
	cache := t.TempDir()
	want := []string{
		"ghcr.io/example/a:1",
		"ghcr.io/example/cron:1",
		"ghcr.io/example/debug:3",
		"ghcr.io/example/declared:v1",
		"ghcr.io/example/env:v1",
		"ghcr.io/example/init:1",
		"ghcr.io/example/z:2",
		"nginx",
	}
	for _, format := range []string{"name", "yaml", "json"} {
		t.Run(format, func(t *testing.T) {
			runImagesGetAndDiffFormatsMatchFormat(t, &current, &cache, struct {
				format *string
				orig   *string
				want   *[]string
			}{format: &format, orig: &orig, want: &want})
		})
	}
	noise, changed := writeFixture(t), writeFixture(t)
	testutil.WriteFile(t, noise, "apps/cm.yaml", strings.NewReplacer(
		"apiserver_request:burnrate1d", "apiserver_request:burnrate6h",
		"0.0.0.0:80", "127.0.0.1:9234",
	).Replace(fixture))
	testutil.WriteFile(t, changed, "apps/cm.yaml", strings.NewReplacer(
		"ghcr.io/example/a:1", "ghcr.io/example/a:2",
		"ghcr.io/example/declared:v1", "ghcr.io/example/declared:v2",
		"ghcr.io/example/env:v1", "ghcr.io/example/env:v2",
	).Replace(fixture))
	t.Run("noise/get", func(t *testing.T) { runImagesGetAndDiffFormatsMatchNoiseGet(t, &noise, &cache, &want) })
	for _, tc := range []struct {
		name           string
		current, orig  string
		includeRemoved bool
		want           string
	}{
		{name: "noise/additions", current: noise, orig: current},
		{name: "noise/including removals", current: noise, orig: current, includeRemoved: true},
		{name: "real/additions", current: changed, orig: current, want: "ghcr.io/example/a:2\nghcr.io/example/declared:v2\nghcr.io/example/env:v2\n"},
		{name: "real/including removals", current: changed, orig: current, includeRemoved: true, want: "ghcr.io/example/a:1\nghcr.io/example/a:2\nghcr.io/example/declared:v1\nghcr.io/example/declared:v2\nghcr.io/example/env:v1\nghcr.io/example/env:v2\n"},
		{name: "reversed/additions", current: current, orig: changed, want: "ghcr.io/example/a:1\nghcr.io/example/declared:v1\nghcr.io/example/env:v1\n"},
		{name: "reversed/including removals", current: current, orig: changed, includeRemoved: true, want: "ghcr.io/example/a:1\nghcr.io/example/a:2\nghcr.io/example/declared:v1\nghcr.io/example/declared:v2\nghcr.io/example/env:v1\nghcr.io/example/env:v2\n"},
	} {
		t.Run(tc.name, func(t *testing.T) { runImagesGetAndDiffFormatsMatchName(t, &tc, &cache) })
	}
	t.Run("empty/default", func(t *testing.T) { runImagesGetAndDiffFormatsMatchEmptyDefault(t, &orig, &cache) })
	t.Run("unchanged/json", func(t *testing.T) { runImagesGetAndDiffFormatsMatchUnchangedJSON(t, &current, &cache) })
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Build_PartialRenderAndDocumentWriteFailure(t *testing.T) {
	root := writeMultiNamespaceFixture(t)
	testutil.WriteFile(
		t,
		root,
		"apps-a/stray.yaml",
		testutil.KustomizationYAML(
			"stray",
			"alpha",
			"  path: ./missing-stray\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "apps-b/kustomization.yaml", "resources: [missing.yaml]\n")
	args := []string{"build", "ks", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json"}
	full, stderr, code := runCLI(t, args...)
	assert.Equal(t, code, 1)
	assert.FatalIf(
		t,
		!strings.Contains(stderr, "apps-b") || !strings.Contains(full, "apps-a"),
		"fixture did not preserve the healthy render: stdout=%s stderr=%s",
		full,
		stderr,
	)
	limit := len(full) - 1
	writeErr := errors.New("document write failed")
	writer := &outputFailWriter{remaining: limit, err: writeErr}
	var errOut bytes.Buffer
	cmd := New("test")
	cmd.SetArgs(args)
	cmd.SetOut(writer)
	cmd.SetErr(&errOut)
	got := cmd.ExecuteContext(t.Context())
	{
		_, ok := errors.AsType[*orchestrator.FailuresError](got)
		assert.FatalIf(t, !ok || !errors.Is(got, writeErr), "render or writer error lost: %v", got)
	}
	{
		_, ok := errors.AsType[reportedError](got)
		assert.FatalIf(t, !ok, "completed diagnostic was not marked reported: %v", got)
	}
	assert.Equal(t, writer.buf.String(), full[:limit])
	assert.Equal(t, strings.Count(errOut.String(), writeErr.Error()), 1)
	assert.Equal(t, strings.Count(errOut.String(), "[OrphanedResource]"), 0)
	writer = &outputFailWriter{remaining: limit, err: writeErr}
	errOut.Reset()
	assert.Equal(t, Run(args, writer, &errOut), 1)
	assert.Equal(t, writer.buf.String(), full[:limit])
	assert.Equal(t, strings.Count(errOut.String(), writeErr.Error()), 1)
	assert.Equal(t, strings.Count(errOut.String(), "[OrphanedResource]"), 0)
}

type outputFailWriter struct {
	buf       bytes.Buffer
	remaining int
	err       error
}

func (w *outputFailWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.remaining)
	_, _ = w.buf.Write(p[:n])
	w.remaining -= n
	if n < len(p) {
		return n, w.err
	}
	return n, nil
}
