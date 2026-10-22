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
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// TestBindCommon_CacheDirFlag pins that --cache-dir is wired on every
// commonFlags subcommand and ends up on commonFlags.cacheDir for the
// downstream buildOrchCfg → orchestrator.Config.CacheDir handoff.
func TestBindCommon_CacheDirFlag(t *testing.T) {
	t.Parallel()
	var f commonFlags
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	bindCommon(fs, &f)
	if err := fs.Parse([]string{"--cache-dir", "/tmp/explicit"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.cacheDir != "/tmp/explicit" {
		t.Errorf("cacheDir = %q, want /tmp/explicit", f.cacheDir)
	}
}

// TestBindCommon_ForceGenericProviderFlag pins the whole
// --force-generic-provider handoff: flag → commonFlags →
// orchestrator.Config. Config is what puts ForceGeneric on the git/OCI/
// bucket fetchers, so dropping that assignment would silently disable the
// flag with every other test still green.
func TestBindCommon_ForceGenericProviderFlag(t *testing.T) {
	t.Parallel()
	parse := func(t *testing.T, args ...string) orchestrator.Config {
		t.Helper()
		var f commonFlags
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		bindCommon(fs, &f)
		if err := fs.Parse(args); err != nil {
			t.Fatalf("Parse: %v", err)
		}
		return buildOrchCfg(f)
	}
	if cfg := parse(t); cfg.ForceGenericProvider {
		t.Errorf("default: ForceGenericProvider = true, want false")
	}
	if cfg := parse(t, "--force-generic-provider"); !cfg.ForceGenericProvider {
		t.Errorf("--force-generic-provider: ForceGenericProvider = false, want true")
	}
}

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
			_, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("build all (%s) exited %d: %s", tc.name, code, stderr)
			}
			entries, err := os.ReadDir(selectedRoot)
			if err != nil {
				t.Fatalf("read cache root %q: %v", selectedRoot, err)
			}
			if len(entries) == 0 {
				t.Errorf("cache root %q is empty after build — --cache-dir / FLATE_CACHE_DIR did not flow through", selectedRoot)
			}
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
				if err := os.Unsetenv("FLATE_CACHE_DIR"); err != nil {
					t.Fatal(err)
				}
			}
			args := []string{"build", "all", "--path", fixture, "--concurrency", "2"}
			if strings.HasPrefix(setting, "empty-flag") {
				args = append(args, "--cache-dir=")
			}
			if setting == "empty-flag-over-env" {
				t.Setenv("FLATE_CACHE_DIR", filepath.Join(fallback, "explicit"))
			}
			stdout, stderr, code := runCLI(t, args...)
			if code != 0 || !strings.Contains(stdout, "name: hello") {
				t.Fatalf("memory render: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
			for _, name := range []string{"flate", "flate-cache", "explicit", ".cache/flate", "Library/Caches/flate"} {
				if _, err := os.Stat(filepath.Join(fallback, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("empty root created cache %s: %v", name, err)
				}
			}
			cfg := buildOrchCfg(commonFlags{})
			if cfg.CacheDir != "" {
				t.Fatalf("empty cache setting resolved to %q", cfg.CacheDir)
			}
		})
	}
}

func TestOutputValue_Validates(t *testing.T) {
	t.Parallel()
	var out string
	v := &outputValue{target: &out, allowed: []output.Format{output.FormatYAML, output.FormatJSON}}

	if err := v.Set("json"); err != nil {
		t.Fatalf("json should be accepted: %v", err)
	}
	if out != "json" {
		t.Errorf("Set should update target, got %q", out)
	}

	err := v.Set("name")
	if err == nil {
		t.Fatal("name should be rejected")
	}
	if !strings.Contains(err.Error(), "must be one of: yaml, json") {
		t.Errorf("error should name the accepted set: %q", err)
	}
}

func writeBaselineGit(t *testing.T, root string, urls ...string) {
	t.Helper()
	if _, err := git.PlainInit(root, false); err != nil && !errors.Is(err, git.ErrRepositoryAlreadyExists) {
		t.Fatal(err)
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
			if tc.currentGit {
				writeBaselineGit(t, current, currentURL)
			} else if tc.brokenCurrent {
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
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer func() {
				if err := cleanup(); err != nil {
					t.Errorf("baseline cleanup: %v", err)
				}
			}()
			if err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, c.pathOrigSelfURLs, tc.want)
			assert.Equal(t, c.pathRoot, current)
			assert.Equal(t, c.pathOrigRoot, orig)
			cfg := buildOrchCfg(c)
			assert.Equal(t, [2]string{cfg.RepoRoot, cfg.PathOrig}, [2]string{current, orig})
		})
	}
}

func TestResolveBaseline_FullTree(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, ".git/config", "[invalid\n")
	c := commonFlags{path: root, pathRoot: "stale",
		pathOrigRoot: "stale", pathOrigSelfURLs: []string{"stale"}}
	noopCleanup, err := resolveBaseline(t.Context(), &c, false)
	defer func() {
		if err := noopCleanup(); err != nil {
			t.Errorf("baseline cleanup: %v", err)
		}
	}()
	assert.Equal(t, err, nil)
	assert.Equal(t, c.pathRoot, "")
	assert.Equal(t, c.pathOrigRoot, "")
	assert.Equal(t, c.pathOrigSelfURLs == nil, true)
	testutil.WriteFile(t, root, ".git/config", "[core]\nrepositoryformatversion = 0\n")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{AllowEmptyCommits: true,
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
	c.base = "HEAD"
	cleanup, err := resolveBaseline(t.Context(), &c, false)
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("baseline cleanup: %v", err)
		}
	}()
	assert.Equal(t, err, nil)
	assert.Equal(t, c.pathRoot, root)
	assert.Equal(t, c.baseInput.LocalPath, filepath.Join(root, ".flate-base", c.baseInput.Revision))
	assert.Equal(t, c.pathOrig, c.baseInput.LocalPath)
	assert.Equal(t, c.pathOrigRoot, c.baseInput.LocalPath)
	if c.baseInput.FS == nil {
		t.Fatal("baseline content unavailable through its virtual label")
	}
	if _, err := os.Stat(c.baseInput.LocalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("baseline label materialized: %v", err)
	}
}

func TestResolveBaseline_DebugRevision(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "file.yaml", "fixture")
	pin := testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	})
	var output bytes.Buffer
	logger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(logger) })
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c := commonFlags{path: root, base: "HEAD", concurrency: 2}
	cleanup, err := resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cleanup()) })
	assert.Equal(t, c.baseInput.Revision, pin.String())
	var record map[string]any
	assert.NoError(t, json.Unmarshal(output.Bytes(), &record))
	delete(record, "time")
	assert.Diff(t, record, map[string]any{
		"level": "DEBUG", "msg": "baseline", "source": "explicit --base=HEAD",
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
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer func() {
				if err := cleanup(); err != nil {
					t.Errorf("baseline cleanup: %v", err)
				}
			}()
			if err == nil || !strings.Contains(err.Error(), tc.want) || !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
				t.Fatalf("error = %v, want wrapped %q error", err, tc.want)
			}
			assert.Equal(t, tc.data != "gitdir: missing\n" || errors.Is(err, git.ErrRepositoryNotExists), true)
			assert.Equal(t, len(c.pathOrigSelfURLs), 0)
		})
	}
}

func TestBindCommon_ChartDigestTrackingOverride(t *testing.T) {
	const key = "FLATE_DISABLE_CHART_DIGEST_TRACKING"
	for _, tc := range []struct {
		name                           string
		args                           []string
		env                            string
		setEnv, auto, disable, invalid bool
	}{
		{name: "absent", auto: true},
		{name: "bare", args: []string{"--disable-chart-digest-tracking"}, disable: true},
		{name: "explicit true", args: []string{"--disable-chart-digest-tracking=true"}, disable: true},
		{name: "explicit false", args: []string{"--disable-chart-digest-tracking=false"}},
		{name: "env true", env: "true", setEnv: true, disable: true},
		{name: "env false", env: "false", setEnv: true},
		{name: "flag false wins", args: []string{"--disable-chart-digest-tracking=false"}, env: "true", setEnv: true},
		{name: "flag true wins", args: []string{"--disable-chart-digest-tracking=true"}, env: "false", setEnv: true, disable: true},
		{name: "flag overrides invalid env", args: []string{"--disable-chart-digest-tracking=false"}, env: "invalid", setEnv: true},
		{name: "invalid env", env: "invalid", setEnv: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(key, tc.env)
			if !tc.setEnv {
				if err := os.Unsetenv(key); err != nil {
					t.Fatal(err)
				}
			}
			root := New("test")
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			var f commonFlags
			var got orchestrator.Config
			ran := false
			cmd := &cobra.Command{Use: "probe", RunE: func(*cobra.Command, []string) error {
				ran = true
				got = buildOrchCfg(f)
				return nil
			}}
			bindCommon(cmd.Flags(), &f)
			root.AddCommand(cmd)
			root.SetArgs(append([]string{"probe"}, tc.args...))
			err := root.ExecuteContext(t.Context())
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), key) || ran {
					t.Fatalf("invalid environment result: err=%v ran=%t", err, ran)
				}
				return
			}
			if err != nil || !ran {
				t.Fatalf("command result: err=%v ran=%t", err, ran)
			}
			assert.Equal(t, got.DisableChartDigestTracking == nil, tc.auto)
			if !tc.auto {
				assert.Equal(t, *got.DisableChartDigestTracking, tc.disable)
			}
		})
	}
	got := buildOrchCfg(commonFlags{})
	assert.Equal(t, got.DisableChartDigestTracking == nil, true)
	got = buildOrchCfg(commonFlags{disableChartDigestTracking: new(true)})
	assert.Equal(t, *got.DisableChartDigestTracking, true)
}

func TestResolveBaseline_AdmittedIdentity(t *testing.T) {
	const url = "ssh://git@github.com/example/home-ops.git"
	current, original := writeCustomSourceDiffFixture(t, url)
	writeBaselineGit(t, current, url)
	writeBaselineGit(t, original, url)
	testutil.WriteFile(t, original, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {greeting: before}\n")
	currentScan, originalScan := filepath.Join(current, "flux"), filepath.Join(original, "flux")
	currentLink, originalLink := filepath.Join(t.TempDir(), "current"), filepath.Join(t.TempDir(), "original")
	assert.NoError(t, os.Symlink(currentScan, currentLink))
	assert.NoError(t, os.Symlink(originalScan, originalLink))
	c := commonFlags{path: currentLink, pathOrig: originalLink, cacheDir: t.TempDir(), concurrency: 2}
	cleanup, err := resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cleanup()) })
	cfg := buildOrchCfg(c)
	assert.Equal(t, cfg.Input, c.input)
	assert.Equal(t, cfg.BaseInput, c.baseInput)
	assert.Equal(t, cfg.Path, currentScan)
	assert.Equal(t, c.pathOrig, originalScan)
	assert.Equal(t, cfg.RepoRoot, current)
	assert.Equal(t, cfg.PathOrig, original)
	for _, input := range []*gittree.FS{c.input.FS.(*gittree.FS), c.baseInput.FS.(*gittree.FS)} {
		location := input.Location()
		assert.Diff(t, location.URLs, []string{url})
		assert.NoError(t, os.Remove(filepath.Join(location.RepositoryRoot, ".git", "config")))
	}
	base, head, err := orchestrator.RenderTrees(t.Context(),
		orchestrator.Tree{RepoRoot: c.pathOrigRoot, Path: c.pathOrig, SelfURLs: c.pathOrigSelfURLs},
		orchestrator.Tree{RepoRoot: c.pathRoot, Path: c.path, SelfURLs: c.selfURLs}, cfg)
	assert.NoError(t, err)
	id := testutil.KustomizationID("gitops-system", "apps")
	for i, side := range []orchestrator.Rendered{base, head} {
		docs := side.Result.Manifests[id]
		if len(docs) != 1 || docs[0]["data"].(map[string]any)["greeting"] != []string{"before", "hi"}[i] {
			t.Fatalf("side %d lost admitted source identity: %v", i, side.Result.Manifests)
		}
	}
}

func TestRunOrchestratorCfg_ProgressLifecycle(t *testing.T) {
	for _, name := range []string{"success", "render-warning", "partial-render", "fatal-bootstrap", "fatal-construction"} {
		t.Run(name, func(t *testing.T) {
			oldBar, oldLogger, original := barSink, slog.Default(), os.Stderr
			var o *orchestrator.Orchestrator
			var raw *os.File
			defer func() {
				if o != nil {
					if err := o.Stop(); err != nil {
						t.Error(err)
					}
				}
				captured := os.Stderr
				os.Stderr = original
				if captured != original && captured != raw {
					_ = captured.Close()
				}
				if raw != nil {
					if err := raw.Close(); err != nil {
						t.Error(err)
					}
				}
				barSink = oldBar
				slog.SetDefault(oldLogger)
			}()
			var out progressOutput
			barSink = &barWriter{out: &out}
			slog.SetDefault(slog.New(slog.NewTextHandler(barSink, &slog.HandlerOptions{Level: slog.LevelDebug})))
			path := filepath.Join(t.TempDir(), "absent")
			switch name {
			case "fatal-construction":
				path = ""
			case "fatal-bootstrap":
			default:
				path = writeFixture(t)
			}
			if name == "partial-render" {
				testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
			}
			expected := original
			if name == "render-warning" {
				testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [cm.yaml]\ncommonLabels: {progress-warning: test}\n")
				f, err := os.CreateTemp(t.TempDir(), "stderr")
				if err != nil {
					t.Fatal(err)
				}
				raw = f
				os.Stderr = raw
				expected = raw
			}
			run := runOrchestratorCfg(t.Context(), orchestrator.Config{Path: path, RepoRoot: path, Concurrency: 2, WipeSecrets: true})
			o = run.Orchestrator
			result, err := run.Result, run.Err
			switch name {
			case "fatal-construction":
				if err == nil || o != nil || result != nil {
					t.Fatalf("fatal run returned orchestrator=%v result=%v err=%v", o, result, err)
				}
			case "fatal-bootstrap":
				if err == nil || o != nil {
					t.Fatalf("fatal run exposed orchestrator=%v, err=%v", o, err)
				}
				assert.Diff(t, result, &orchestrator.Result{})
			case "partial-render":
				if err == nil || o == nil || result == nil || len(result.Failed) == 0 {
					t.Fatalf("partial run lost state: %+v", run)
				}
			default:
				assert.NoError(t, err)
				if o == nil || result == nil {
					t.Fatal("successful render returned nil")
				}
			}
			if name == "render-warning" {
				const warning = "# Warning: 'commonLabels' is deprecated."
				if got := out.String(); !strings.Contains(got, warning) {
					t.Errorf("render warning missing from sink: %q", got)
				}
				got, err := os.ReadFile(raw.Name())
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(got), warning) {
					t.Errorf("render warning leaked to raw stderr: %q", got)
				}
			}
			if os.Stderr != expected {
				t.Error("shared helper left stderr captured")
			}
			if progressProgram(t, barSink) != nil {
				t.Error("shared helper left progress attached")
			}
		})
	}
}

func TestBindCommon_HelmOptions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		args []string
		want helm.Options
	}{
		{name: "defaults", want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{"Secret"}}},
		{name: "CRD only", args: []string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--is-upgrade", "--no-hooks", "-s", "templates/main.yaml", "--enable-dns", "--skip-kinds=CustomResourceDefinition"}, want: helm.Options{KubeVersion: "1.31.2", APIVersions: "fixture.example/v1", IsUpgrade: true, NoHooks: true, ShowOnly: []string{"templates/main.yaml"}, EnableDNS: true, SkipTests: true, SkipCRDs: true, SkipKinds: []string{}}},
		{name: "mixed", args: []string{"--skip-kinds=Secret,CustomResourceDefinition,ConfigMap"}, want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{"Secret", "ConfigMap"}}},
		{name: "explicit false and empty", args: []string{"--is-upgrade=false", "--no-hooks=false", "--enable-dns=false", "--show-only=", "--skip-kinds="}, want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, ShowOnly: []string{}, SkipKinds: []string{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var f commonFlags
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			bindCommon(fs, &f)
			assert.NoError(t, fs.Parse(tc.args))
			retained, want := f.skipKinds, slices.Clone(f.skipKinds)
			for range 2 {
				assert.Diff(t, buildOrchCfg(f).HelmOptions, tc.want)
				assert.Diff(t, f.skipKinds, want)
				assert.Diff(t, retained, want)
			}
		})
	}
	assert.Equal(t, buildOrchCfg(commonFlags{}).HelmOptions.SkipTests, true)
}

func TestRunSelected_WarningIdentity(t *testing.T) {
	base := manifest.Warning{Category: manifest.WarnStaleValues, Resource: testutil.HelmReleaseID("apps", "demo"), Message: "advisory", Detail: []string{"one"}, Count: 3}
	detail, count, category, resource, message := base, base, base, base, base
	detail.Detail = []string{"two"}
	count.Count = 4
	category.Category = manifest.WarnOmittedValues
	resource.Resource.Name = "other"
	message.Message = "different"
	want := []manifest.Warning{base, detail, count, category, resource, message}
	side := &selection.Selection{Warnings: []manifest.Warning{base, detail, count, category, resource, message}, Unrelated: []manifest.Warning{base}, External: []manifest.Warning{detail}}
	before := slices.Clone(side.Warnings)
	for i := range before {
		before[i].Detail = slices.Clone(before[i].Detail)
	}
	assert.Diff(t, selectedWarnings(side), want)
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		err := finishSelected(selectedRequest{roots: []manifest.NamedResource{{Kind: manifest.KindHelmRelease}}, pair: true, stderr: &stderr, presentation: presentData},
			&selectedInvocation{sides: []*selection.Selection{side, {Warnings: []manifest.Warning{base}}}}, selectedPayload{})
		if err != nil {
			t.Fatal(err)
		}
		var expected bytes.Buffer
		expected.WriteString("orig snapshot:")
		assert.NoError(t, (output.Encoder{Writer: &expected}).Report(output.Model{Warnings: want}))
		expected.WriteString("current snapshot:")
		assert.NoError(t, (output.Encoder{Writer: &expected}).Report(output.Model{Warnings: []manifest.Warning{base}}))
		assert.Equal(t, stderr.String(), expected.String())
		if run == 0 {
			first = stderr.String()
		} else {
			assert.Equal(t, stderr.String(), first)
		}
	}
	assert.Diff(t, side.Warnings, before)
	assert.Diff(t, side.Unrelated, []manifest.Warning{base})
	assert.Diff(t, side.External, []manifest.Warning{detail})
	if allocs := testing.AllocsPerRun(1, func() { selectedWarnings(&selection.Selection{}) }); allocs != 0 {
		t.Fatalf("empty diagnostics allocated: %v", allocs)
	}
}

func TestRunSelected_RemainderAndPayloadErrors(t *testing.T) {
	for _, mode := range []presentation{presentBuild, presentData, presentTest, presentDiff} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			if mode != presentTest {
				t.Parallel()
			}
			o, err := orchestrator.New(t.Context(), orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir(), Concurrency: 2})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := o.Stop(); err != nil {
					t.Error(err)
				}
			})
			id := testutil.KustomizationID("apps", "selected")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{id: {Status: store.StatusFailed, Message: "selected render failed"}}}
			resourceErr := &orchestrator.FailuresError{Message: "aggregate render failure"}
			closeErr, writeErr := errors.New("shared cleanup failed"), errors.New("payload write failed")
			runs := []orchestrator.Rendered{{Orchestrator: o, Result: res, Err: resourceErr}, {Orchestrator: o, Result: &orchestrator.Result{}}}
			roots := []manifest.NamedResource{id}
			sides, selectedErr, remainder := orchestrator.Select(runs, errors.Join(resourceErr, fmt.Errorf("cache close: %w", closeErr)), roots, nil)
			if len(sides) != 2 || selectedErr == nil {
				t.Fatalf("completed run selection: %v", selectedErr)
			}
			var stdout, stderr bytes.Buffer
			_, payloadErr := failingWriter{err: writeErr}.Write([]byte("partial"))
			payload := selectedPayload{err: payloadErr}
			if mode == presentTest {
				payload.roster = &output.TestReport{Cases: []output.Case{{ID: id, Outcome: output.OutcomePassed}}, Passed: 1, Matched: 1}
			}
			got := finishSelected(selectedRequest{roots: roots, stdout: &stdout, stderr: &stderr, presentation: mode},
				&selectedInvocation{sides: sides, err: selectedErr, remainder: remainder}, payload)
			if !errors.Is(got, closeErr) || !errors.Is(got, writeErr) || !errors.Is(got, selectedErr) {
				t.Fatalf("lost selected, cleanup or payload error: %v", got)
			}
			if _, reported := errors.AsType[reportedError](got); !reported {
				if err := (output.Encoder{Writer: &stderr}).Errors(got); err != nil {
					t.Fatal(err)
				}
			}
			for _, message := range []string{"selected render failed", closeErr.Error(), writeErr.Error()} {
				assert.Equal(t, strings.Count(stderr.String(), message), 1)
			}
		})
	}
}

func TestRenderKinds_Capabilities(t *testing.T) {
	t.Parallel()
	var want []string
	for info := range manifest.Kinds() {
		if info.Render {
			want = append(want, info.Kind)
		}
	}
	assert.Diff(t, renderKinds, want)
}

func TestRun_DiagnosticOnlyComparison(t *testing.T) {
	root := writeFixture(t)
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "work.txt", "before")
	pin := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("work.txt")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "work.txt", "after")
	testutil.GitCommitAll(t, repo, "head", &git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}})
	oid := entry.Hash.String()
	assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
	testutil.WriteFile(t, root, ".git/index", "invalid index")
	for _, command := range [][]string{{"diff", "all"}, {"diff", "images"}, {"test", "ks", "named"}, {"build", "all"}, {"get", "all"}} {
		t.Run(strings.Join(command, "/"), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			args := append(slices.Clone(command), "--path", root, "--base", pin.String(), "--concurrency", "2")
			code := Run(args, &stdout, &stderr)
			if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "Git status unavailable") || !strings.Contains(stderr.String(), "missing objects") || strings.Contains(stderr.String(), `no Kustomization named`) {
				t.Fatalf("diagnostic-only %v = exit %d, stdout %q, stderr %q", command, code, stdout.String(), stderr.String())
			}
		})
	}
	for _, mode := range []presentation{presentBuild, presentData, presentTest, presentDiff} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			rootID := testutil.KustomizationID("", "")
			if mode == presentTest {
				rootID.Name = "named"
			}
			err := runSelected(t.Context(), selectedRequest{
				flags: commonFlags{path: root, base: pin.String(), concurrency: 2},
				roots: []manifest.NamedResource{rootID},
				pair:  mode == presentDiff, stdout: &stdout, stderr: &stderr, presentation: mode,
			}, func(*selectedInvocation) selectedPayload { called = true; return selectedPayload{} })
			if err == nil || called || !strings.Contains(stderr.String(), "Git status unavailable") {
				t.Fatalf("incomplete input invoked product=%v: %v, %s", called, err, stderr.String())
			}
		})
	}
}

func TestRunOrchestratorCfg_BootstrapWarningOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	warning := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "before failure", Count: 1}
	cfg := orchestrator.Config{Path: root, Input: &store.SourceArtifact{LocalPath: root, Warnings: []manifest.Warning{warning}}, Concurrency: 2}
	run := runOrchestratorCfg(t.Context(), cfg)
	if run.Orchestrator != nil || run.Result == nil || !errors.Is(run.Err, manifest.ErrInput) || len(run.Result.Manifests)+len(run.Result.Failed) != 0 {
		t.Fatalf("failed bootstrap exposed content: %+v", run)
	}
	assert.Diff(t, run.Result.Warnings, []manifest.Warning{warning})
}

func TestFinishSelected_JoinsWriterErrors(t *testing.T) {
	t.Parallel()
	for _, mode := range []presentation{presentBuild, presentData, presentTest, presentDiff} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			t.Parallel()
			productErr, reportErr, renderErr := errors.New("product closed"), errors.New("report closed"), errors.New("render failed")
			side := &selection.Selection{Warnings: []manifest.Warning{{Category: manifest.WarnEmptyScan, Message: "advisory"}}}
			inv := &selectedInvocation{sides: []*selection.Selection{side, side}, err: renderErr, remainder: renderErr}
			payload := selectedPayload{err: productErr, warnings: []manifest.Warning{{Category: manifest.WarnDiffSuppressed, Message: "payload warning"}}}
			if mode == presentTest {
				payload.err = nil
				payload.roster = &output.TestReport{}
			}
			writer := &countFailWriter{err: reportErr}
			err := finishSelected(selectedRequest{roots: []manifest.NamedResource{{Kind: manifest.KindKustomization}}, stdout: failingWriter{err: productErr}, stderr: writer, presentation: mode, pair: true}, inv, payload)
			for _, want := range []error{productErr, reportErr, renderErr} {
				if !errors.Is(err, want) {
					t.Fatalf("lost %v: %v", want, err)
				}
			}
			if writer.writes < 5 {
				t.Fatalf("report stopped after %d writes", writer.writes)
			}
		})
	}
}

type countFailWriter struct {
	writes int
	err    error
}

func (w *countFailWriter) Write([]byte) (int, error) { w.writes++; return 0, w.err }
