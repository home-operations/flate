package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// TestBindCommon_CacheDirFlag pins that --cache-dir is wired on every
// commonFlags subcommand and ends up on commonFlags.cacheDir for the
// downstream buildOrchCfg → orchestrator.Config.CacheDir handoff.
func TestBindCommon_CacheDirFlag(t *testing.T) {
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
	parse := func(t *testing.T, args ...string) orchestrator.Config {
		t.Helper()
		var f commonFlags
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		bindCommon(fs, &f)
		if err := fs.Parse(args); err != nil {
			t.Fatalf("Parse: %v", err)
		}
		return buildOrchCfg(f, helmFlags{})
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
			cfg := buildOrchCfg(commonFlags{}, helmFlags{})
			if cfg.CacheDir != "" {
				t.Fatalf("empty cache setting resolved to %q", cfg.CacheDir)
			}
		})
	}
}

func TestOutputValue_Validates(t *testing.T) {
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
			cfg := buildOrchCfg(c, helmFlags{})
			assert.Equal(t, [2]string{cfg.RepoRoot, cfg.PathOrig}, [2]string{current, orig})
		})
	}
}

func TestResolveBaseline_FullTree(t *testing.T) {
	root := t.TempDir()
	repo, err := git.PlainInit(root, false)
	if err != nil {
		t.Fatal(err)
	}
	wt, err := repo.Worktree()
	if err != nil {
		t.Fatal(err)
	}
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
	_, err = wt.Commit("fixture", &git.CommitOptions{AllowEmptyCommits: true,
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
	assert.Equal(t, err, nil)
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

func TestResolveBaseline_SelectedRepositoryErrors(t *testing.T) {
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

func TestHelmFlags_ChartDigestTrackingOverride(t *testing.T) {
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
			var h helmFlags
			var got orchestrator.Config
			ran := false
			cmd := &cobra.Command{Use: "probe", RunE: func(*cobra.Command, []string) error {
				ran = true
				got = buildOrchCfg(commonFlags{}, h)
				return nil
			}}
			bindHelmFlags(cmd.Flags(), &h)
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
	got := buildOrchCfg(commonFlags{}, helmFlags{})
	assert.Equal(t, got.DisableChartDigestTracking == nil, true)
	got = buildOrchCfg(commonFlags{}, helmFlags{disableChartDigestTracking: new(true)})
	assert.Equal(t, *got.DisableChartDigestTracking, true)
}
