package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/selection"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
)

// TestBindCommon_CacheDirFlag pins that --cache-dir is wired on every
// commonFlags subcommand and ends up on commonFlags.cacheDir for the
// downstream buildOrchCfg → orchestrator.Config.CacheDir handoff.
func TestBindCommon_CacheDirFlag(t *testing.T) {
	t.Parallel()
	var f commonFlags
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	bindCommon(fs, &f)
	{
		err := fs.Parse([]string{"--cache-dir", "/tmp/explicit"})
		assert.FatalIf(t, err != nil, "Parse: %v", err)
	}
	assert.ErrorIf(t, f.cacheDir != "/tmp/explicit", "cacheDir = %q, want /tmp/explicit", f.cacheDir)
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestCacheDir_FlagAndEnvPopulateRoot(t *testing.T) {
	for _, tc := range []struct {
		name string
		via  func(t *testing.T, root string) []string
	}{
		{
			name: "flag",
			via:  func(_ *testing.T, root string) []string { return []string{"--cache-dir", root} },
		},
		{
			name: "env",
			via: func(t *testing.T, root string) []string {
				t.Helper()
				t.Setenv("FLATE_CACHE_DIR", root)
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFixture(t)
			selectedRoot := filepath.Join(t.TempDir(), "cache")
			args := append([]string{"build", "all", "--path", path}, tc.via(t, selectedRoot)...)
			cli := runCLI(t, args...)
			stderr, code := cli.stderr, cli.code
			assert.FatalIf(t, code != 0, "build all (%s) exited %d: %s", tc.name, code, stderr)
			entries, err := os.ReadDir(selectedRoot)
			assert.FatalIf(t, err != nil, "read cache root %q: %v", selectedRoot, err)
			assert.ErrorIf(
				t,
				len(entries) == 0,
				"cache root %q is empty after build — --cache-dir / FLATE_CACHE_DIR did not flow through",
				selectedRoot,
			)
		})
	}
}

func TestCacheDir_EmptyUsesMemory(t *testing.T) {
	for _, setting := range []string{"unset", "empty-env", "empty-flag", "empty-flag-over-env"} {
		t.Run(setting, func(t *testing.T) {
			fixture := writeFixture(t)
			fallback := t.TempDir()
			for _, key := range []string{"HOME", "XDG_CACHE_HOME", "TMPDIR", "TMP", "TEMP", "LOCALAPPDATA"} {
				t.Setenv(key, fallback)
			}
			t.Setenv("FLATE_CACHE_DIR", "")
			if setting == "unset" {
				assert.NoError(t, os.Unsetenv("FLATE_CACHE_DIR"))
			}
			args := []string{"build", "all", "--path", fixture, "--concurrency", "2"}
			if strings.HasPrefix(setting, "empty-flag") {
				args = append(args, "--cache-dir=")
			}
			if setting == "empty-flag-over-env" {
				t.Setenv("FLATE_CACHE_DIR", filepath.Join(fallback, "explicit"))
			}
			cli := runCLI(t, args...)
			stdout, stderr, code := cli.stdout, cli.stderr, cli.code
			assert.FatalIf(
				t,
				code != 0 || !strings.Contains(stdout, "name: hello"),
				"memory render: exit=%d stdout=%s stderr=%s",
				code,
				stdout,
				stderr,
			)
			for _, name := range []string{"flate", "flate-cache", "explicit", ".cache/flate", "Library/Caches/flate"} {
				{
					_, err := os.Stat(filepath.Join(fallback, name))
					assert.FatalIf(t, !errors.Is(err, os.ErrNotExist), "empty root created cache %s: %v", name, err)
				}
			}
			cfg := buildOrchCfg(&commonFlags{})
			assert.FatalIf(t, cfg.CacheDir != "", "empty cache setting resolved to %q", cfg.CacheDir)
		})
	}
}

func TestOutputValue_Validates(t *testing.T) {
	t.Parallel()
	var out string
	v := &outputValue{target: &out, allowed: []output.Format{output.FormatYAML, output.FormatJSON}}

	{
		err := v.Set("json")
		assert.FatalIf(t, err != nil, "json should be accepted: %v", err)
	}
	assert.ErrorIf(t, out != "json", "Set should update target, got %q", out)

	err := v.Set("name")
	assert.FatalIf(t, err == nil, "%v", "name should be rejected")
	assert.ErrorIf(
		t,
		!strings.Contains(err.Error(), "must be one of: yaml, json"),
		"error should name the accepted set: %q",
		err,
	)
}

func writeBaselineGit(t *testing.T, root string, urls ...string) {
	t.Helper()
	{
		_, err := git.PlainInit(root, false)
		assert.FatalIf(t, err != nil && !errors.Is(err, git.ErrRepositoryAlreadyExists), "%v", err)
	}
	var cfg strings.Builder
	for i, url := range urls {
		fmt.Fprintf(&cfg, "[remote \"remote-%d\"]\n  url = %s\n", i, url)
	}
	testutil.WriteFile(t, root, ".git/config", cfg.String())
}

func writeBaselineWorktree(t *testing.T, root, common string) {
	t.Helper()
	gitDir := filepath.Join(common, ".git", "worktrees", "baseline")
	testutil.WriteFile(t, root, ".git", "gitdir: "+gitDir+"\n")
	testutil.WriteFile(t, gitDir, "gitdir", filepath.Join(root, ".git")+"\n")
	testutil.WriteFile(t, gitDir, "commondir", "../..\n")
	testutil.WriteFile(t, gitDir, "HEAD", "ref: refs/heads/main\n")
}

func TestResolveBaseline_ExplicitIdentity(t *testing.T) {
	t.Parallel()
	const currentURL = "https://example.invalid/fork.git"
	const baselineURL = "https://example.invalid/upstream.git"
	for _, tc := range []struct {
		name                      string
		currentGit, brokenCurrent bool
		baseline                  string
		want                      []string
	}{
		{name: "different origins", currentGit: true, baseline: "different", want: []string{baselineURL}},
		{name: "matching origins", currentGit: true, baseline: "matching", want: []string{currentURL}},
		{name: "snapshot fallback", currentGit: true, want: []string{currentURL}},
		{name: "no repositories"},
		{name: "no remotes", currentGit: true, baseline: "empty"},
		{name: "baseline only", baseline: "different", want: []string{baselineURL}},
		{name: "optional current open", brokenCurrent: true, baseline: "different", want: []string{baselineURL}},
		{name: "linked worktree", currentGit: true, baseline: "linked", want: []string{baselineURL}},
		{name: "nested scan", currentGit: true, baseline: "nested", want: []string{baselineURL}},
		{name: "sorted remotes", currentGit: true, baseline: "sorted", want: []string{currentURL, baselineURL}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current, orig := t.TempDir(), t.TempDir()
			switch {
			case tc.currentGit:

				writeBaselineGit(t, current, currentURL)

			case tc.brokenCurrent:

				testutil.WriteFile(t, current, ".git", "gitdir: missing\n")
			}
			scan := orig
			switch tc.baseline {
			case "different":
				writeBaselineGit(t, orig, baselineURL)
			case "matching":
				writeBaselineGit(t, orig, currentURL)
			case "empty":
				writeBaselineGit(t, orig)
			case "linked":
				common := t.TempDir()
				writeBaselineGit(t, common, baselineURL)
				writeBaselineWorktree(t, orig, common)
			case "nested":
				writeBaselineGit(t, orig, baselineURL)
				testutil.WriteFile(t, orig, "nested/scan/manifest.yaml", "")
				scan = filepath.Join(orig, "nested", "scan")
			case "sorted":
				writeBaselineGit(t, orig, currentURL, baselineURL)
			}
			path := current
			if tc.baseline == "nested" {
				for _, p := range []*string{&path, &scan} {
					link := filepath.Join(t.TempDir(), "scan")
					assert.Equal(t, os.Symlink(*p, link), nil)
					*p = link
				}
			}
			c := commonFlags{path: path, pathOrig: scan, pathOrigSelfURLs: []string{"stale"}}
			if c.owner == nil {
				c.owner = cachetest.New(t, "", sourcecache.New)
			}
			err := resolveBaseline(t.Context(), &c, false)
			assert.NoError(t, err)
			assert.Diff(t, c.pathOrigSelfURLs, tc.want)
			assert.Equal(t, c.pathRoot, current)
			assert.Equal(t, c.pathOrigRoot, orig)
			assert.Equal(t, [2]string{inputTrees(&c)[1].RepoRoot, inputTrees(&c)[0].RepoRoot}, [2]string{current, orig})
		})
	}
}

func TestResolveBaseline_FullTree(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, ".git/config", "[invalid\n")
	c := commonFlags{
		path: root, pathRoot: "stale",
		pathOrigRoot: "stale", pathOrigSelfURLs: []string{"stale"},
	}
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err := resolveBaseline(t.Context(), &c, false)
	assert.Equal(t, err, nil)
	assert.Equal(t, c.pathRoot, "")
	assert.Equal(t, c.pathOrigRoot, "")
	assert.Equal(t, c.pathOrigSelfURLs == nil, true)
	testutil.WriteFile(t, root, ".git/config", "[core]\nrepositoryformatversion = 0\n")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		AllowEmptyCommits: true,
		Author:            &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	})
	c.base = "HEAD"
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err = resolveBaseline(t.Context(), &c, false)
	assert.Equal(t, err, nil)
	assert.Equal(t, c.pathRoot, root)
	assert.Equal(t, c.baseInput.LocalPath, filepath.Join(root, ".flate-base", c.baseInput.Revision))
	assert.Equal(t, c.pathOrig, c.baseInput.LocalPath)
	assert.Equal(t, c.pathOrigRoot, c.baseInput.LocalPath)
	assert.FatalIf(t, c.baseInput.FS == nil, "%v", "baseline content unavailable through its virtual label")
	{
		_, err := os.Stat(c.baseInput.LocalPath)
		assert.FatalIf(t, !errors.Is(err, os.ErrNotExist), "baseline label materialized: %v", err)
	}
}

//nolint:paralleltest // test changes process-wide logging.
func TestResolveBaseline_DebugRevision(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "file.yaml", "fixture")
	pin := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	})
	var logOutput bytes.Buffer
	logger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(logger) })
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c := commonFlags{path: root, base: "HEAD", concurrency: 2}
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err := resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	assert.Equal(t, c.baseInput.Revision, pin.String())
	var record map[string]any
	assert.NoError(t, json.Unmarshal(logOutput.Bytes(), &record))
	delete(record, "time")
	assert.Diff(t, record, map[string]any{
		"level": "DEBUG", "msg": "cli: baseline", "source": "explicit --base=HEAD",
		"rev": pin.String()[:7], "path_orig": c.pathOrig,
	})
}

func TestResolveBaseline_SelectedRepositoryErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		file    string
		data    string
		current bool
		want    string
	}{
		{name: "baseline git metadata", file: ".git", data: "invalid gitdir\n", want: "baseline tree"},
		{name: "missing linked gitdir", file: ".git", data: "gitdir: missing\n", want: "baseline tree"},
		{name: "baseline config", file: ".git/config", data: "[invalid", want: "baseline tree"},
		{name: "baseline stat", want: "baseline tree"},
		{name: "current fallback config", file: ".git/config", data: "[invalid", current: true, want: "working tree"},
		{name: "current fallback open", file: ".git", data: "gitdir: missing\n", current: true, want: "working tree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			current, orig := t.TempDir(), t.TempDir()
			selected := orig
			if tc.current {
				selected = current
			}
			if tc.file == ".git/config" {
				writeBaselineGit(t, selected, "https://example.invalid/selected.git")
			}
			if !tc.current {
				assert.Equal(t, os.Symlink(".git", filepath.Join(current, ".git")), nil)
			}
			if tc.file == "" {
				assert.Equal(t, os.Symlink(".git", filepath.Join(selected, ".git")), nil)
			} else {
				testutil.WriteFile(t, selected, tc.file, tc.data)
			}
			c := commonFlags{path: current, pathOrig: orig, pathOrigSelfURLs: []string{"stale"}}
			if c.owner == nil {
				c.owner = cachetest.New(t, "", sourcecache.New)
			}
			err := resolveBaseline(t.Context(), &c, false)
			assert.FatalIf(
				t,
				err == nil || !strings.Contains(err.Error(), tc.want) || !errors.Is(err, manifest.ErrInput) ||
					!errors.Is(err, manifest.ErrFlux),
				"error = %v, want wrapped %q error",
				err,
				tc.want,
			)
			assert.Equal(t, tc.data != "gitdir: missing\n" || errors.Is(err, git.ErrRepositoryNotExists), true)
			assert.Equal(t, len(c.pathOrigSelfURLs), 0)
		})
	}
}

func TestBindCommon_ChartDigestTrackingOverride(t *testing.T) {
	const key = "FLATE_DISABLE_CHART_DIGEST_TRACKING"
	for _, tc := range []struct {
		name                     string
		args                     []string
		env                      string
		setEnv, disable, invalid bool
	}{
		{name: "absent"},
		{name: "bare", args: []string{"--disable-chart-digest-tracking"}, disable: true},
		{name: "explicit true", args: []string{"--disable-chart-digest-tracking=true"}, disable: true},
		{name: "explicit false", args: []string{"--disable-chart-digest-tracking=false"}},
		{name: "env true", env: "true", setEnv: true, disable: true},
		{name: "env false", env: "false", setEnv: true},
		{name: "flag false wins", args: []string{"--disable-chart-digest-tracking=false"}, env: "true", setEnv: true},
		{name: "flag true wins", args: []string{"--disable-chart-digest-tracking=true"}, env: "false", setEnv: true, disable: true},
		{name: "flag overrides invalid env", args: []string{"--disable-chart-digest-tracking=false"}, env: "invalid", setEnv: true},
		{name: "invalid env", env: "invalid", setEnv: true, invalid: true},
		{name: "invalid flag", args: []string{"--disable-chart-digest-tracking=invalid"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.env)
			if !tc.setEnv {
				assert.NoError(t, os.Unsetenv(key))
			}
			root := New("test")
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			var f commonFlags
			var got orchestrator.Config
			ran := false
			cmd := &cobra.Command{Use: "probe", RunE: func(*cobra.Command, []string) error {
				ran = true
				got = buildOrchCfg(&f)
				return nil
			}}
			bindCommon(cmd.Flags(), &f)
			root.AddCommand(cmd)
			root.SetArgs(append([]string{"probe"}, tc.args...))
			err := root.ExecuteContext(t.Context())
			if tc.invalid {
				assert.FatalIf(
					t,
					err == nil || ran || tc.setEnv && !strings.Contains(err.Error(), key),
					"invalid selection result: err=%v ran=%t",
					err,
					ran,
				)
				return
			}
			assert.FatalIf(t, err != nil || !ran, "command result: err=%v ran=%t", err, ran)
			assert.Equal(t, got.HelmOptions.DisableChartDigestTracking, tc.disable)
		})
	}
}

func TestResolveBaseline_AdmittedIdentity(t *testing.T) {
	t.Parallel()
	const url = "ssh://git@github.com/example/home-ops.git"
	fixture := writeCustomSourceDiffFixture(t, url)
	current, original := fixture.current, fixture.orig
	writeBaselineGit(t, current, url)
	writeBaselineGit(t, original, url)
	testutil.WriteFile(
		t,
		original,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: before}\n",
	)
	currentScan, originalScan := filepath.Join(current, "flux"), filepath.Join(original, "flux")
	currentLink, originalLink := filepath.Join(t.TempDir(), "current"), filepath.Join(t.TempDir(), "original")
	assert.NoError(t, os.Symlink(currentScan, currentLink))
	assert.NoError(t, os.Symlink(originalScan, originalLink))
	c := commonFlags{path: currentLink, pathOrig: originalLink, cacheDir: t.TempDir(), concurrency: 2}
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err := resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	cfg := buildOrchCfg(&c)
	assert.Equal(t, inputTrees(&c)[1].Input, c.input)
	assert.Equal(t, inputTrees(&c)[0].Input, c.baseInput)
	assert.Equal(t, inputTrees(&c)[1].Path, currentScan)
	assert.Equal(t, c.pathOrig, originalScan)
	assert.Equal(t, inputTrees(&c)[1].RepoRoot, current)
	assert.Equal(t, inputTrees(&c)[0].RepoRoot, original)
	for _, input := range []*gittree.FS{c.input.FS.(*gittree.FS), c.baseInput.FS.(*gittree.FS)} {
		location := input.Location()
		assert.Diff(t, location.URLs, []string{url})
		assert.NoError(t, os.Remove(filepath.Join(location.RepositoryRoot, ".git", "config")))
	}
	runs, err := renderTreesForTest(t.Context(), t, inputTrees(&c), cfg)
	base, head := runs[0], runs[1]
	assert.NoError(t, err)
	id := testutil.KustomizationID("gitops-system", "apps")
	for i, side := range []orchestrator.Rendered{base, head} {
		docs := outputByParent(side.Result.Manifests)[id]
		assert.FatalIf(
			t,
			len(docs) != 1 || docs[0]["data"].(map[string]any)["greeting"] != []string{"before", "hi"}[i],
			"side %d lost admitted source identity: %v",
			i,
			side.Result.Manifests,
		)
	}
}

//nolint:paralleltest // test replaces process-wide stderr.
func TestRunSelected_ProgressLifecycle(t *testing.T) {
	for _, name := range []string{"success", "render-warning", "partial-render", "fatal-bootstrap", "fatal-construction"} {
		t.Run(name, func(t *testing.T) { runSelectedProgressLifecycleName(t, &name) })
	}
}

func TestBindCommon_HelmOptions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
		want helm.Options
	}{
		{name: "defaults", want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{"CustomResourceDefinition", "Secret"}}},
		{name: "CRD only", args: []string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--enable-dns", "--skip-kinds=CustomResourceDefinition"}, want: helm.Options{KubeVersion: "1.31.2", APIVersions: "fixture.example/v1", EnableDNS: true, SkipTests: true, SkipCRDs: true, SkipKinds: []string{"CustomResourceDefinition"}}},
		{name: "mixed", args: []string{"--skip-kinds=Secret,CustomResourceDefinition,ConfigMap"}, want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{"ConfigMap", "CustomResourceDefinition", "Secret"}}},
		{name: "duplicates", args: []string{"--skip-kinds=Secret,CustomResourceDefinition,ConfigMap,Secret,CustomResourceDefinition"}, want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{"ConfigMap", "CustomResourceDefinition", "Secret"}}},
		{name: "explicit false and empty", args: []string{"--enable-dns=false", "--skip-kinds="}, want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipKinds: []string{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var f commonFlags
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			bindCommon(fs, &f)
			assert.NoError(t, fs.Parse(tc.args))
			retained, want := f.skipKinds, slices.Clone(f.skipKinds)
			for range 2 {
				assert.Diff(t, buildOrchCfg(&f).HelmOptions, tc.want)
				assert.Diff(t, f.skipKinds, want)
				assert.Diff(t, retained, want)
			}
		})
	}
	assert.Equal(t, buildOrchCfg(&commonFlags{}).HelmOptions.SkipTests, true)
	assert.Diff(t, buildOrchCfg(&commonFlags{}).HelmOptions.SkipKinds, []string(nil))
}

func TestRunSelected_WarningIdentity(t *testing.T) {
	t.Parallel()
	base := manifest.Warning{
		Category: manifest.WarnStaleValues,
		Resource: testutil.HelmReleaseID("apps", "demo"),
		Message:  "advisory",
		Detail:   []string{"one"},
		Count:    3,
	}
	detail, count, category, resource, message := base, base, base, base, base
	detail.Detail = []string{"two"}
	count.Count = 4
	category.Category = manifest.WarnOmittedValues
	resource.Resource.Name = "other"
	message.Message = "different"
	want := []manifest.Warning{base, detail, count, category, resource, message}
	side := &selection.Result{
		Warnings:  []manifest.Warning{base, detail, count, category, resource, message},
		Unrelated: []manifest.Warning{base},
		External:  []manifest.Warning{detail},
	}
	before := slices.Clone(side.Warnings)
	for i := range before {
		before[i].Detail = slices.Clone(before[i].Detail)
	}
	assert.Diff(t, selectedWarnings(side), want)
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		err := finishSelected(
			&selectedRequest{
				roots:  []manifest.NamedResource{{Kind: manifest.KindHelmRelease}},
				pair:   true,
				stderr: &stderr,
			},
			&selectedInvocation{
				groups: []selectedGroup{
					{cluster: ".", sides: []*selection.Result{side, {Warnings: []manifest.Warning{base}}}},
				},
			},
			&selectedPayload{},
		)
		assert.NoError(t, err)
		expected := output.Report{Groups: []output.ReportGroup{{Cluster: "."}}}
		for _, warning := range want {
			expected.Warnings = append(expected.Warnings, output.Warning{Warning: warning, Context: "orig snapshot"})
		}
		expected.Warnings = append(expected.Warnings, output.Warning{Warning: base, Context: "current snapshot"})
		expected.Prepare(nil)
		var rendered bytes.Buffer
		encoder := output.Encoder{Writer: &rendered}
		assert.NoError(t, encoder.Report(&expected, encoder))
		assert.Equal(t, stderr.String(), rendered.String())
		assert.Equal(t, strings.Count(stderr.String(), "warnings (7)"), 1)
		if run == 0 {
			first = stderr.String()
		} else {
			assert.Equal(t, stderr.String(), first)
		}
	}
	assert.Diff(t, side.Warnings, before)
	assert.Diff(t, side.Unrelated, []manifest.Warning{base})
	assert.Diff(t, side.External, []manifest.Warning{detail})
}

func TestCommonFlags_Roots(t *testing.T) {
	t.Parallel()
	flags := commonFlags{namespace: "apps"}
	assert.Diff(
		t,
		flags.roots(
			"selected",
			manifest.KindKustomization,
			manifest.KindGitRepository,
			manifest.KindConfigMap,
			"Unknown",
		),
		[]manifest.NamedResource{
			{
				Group:     manifest.FluxKustomizeDomain,
				Kind:      manifest.KindKustomization,
				Namespace: "apps",
				Name:      "selected",
			},
			{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "selected"},
			{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "selected"},
			{Kind: "Unknown", Namespace: "apps", Name: "selected"},
		},
	)
}
