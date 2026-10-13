package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/internal/testutil"
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

// TestCacheDir_FlagAndEnvPopulateRoot runs build all twice — once via
// --cache-dir and once via FLATE_CACHE_DIR — against fresh tempdirs
// and asserts each one ends up non-empty. The kustomize stage cache
// writes into <cacheDir>/stage even for the inline-ConfigMap fixture,
// so a successful build populates the dir if the flag flowed through
// to orchestrator.Config.CacheDir.
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
				t.Setenv("FLATE_CACHE_DIR", root)
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFixture(t)
			cacheRoot := filepath.Join(t.TempDir(), "cache")
			args := append([]string{"build", "all", "--path", path}, tc.via(t, cacheRoot)...)
			_, stderr, code := runCLI(t, args...)
			if code != 0 {
				t.Fatalf("build all (%s) exited %d: %s", tc.name, code, stderr)
			}
			entries, err := os.ReadDir(cacheRoot)
			if err != nil {
				t.Fatalf("read cache root %q: %v", cacheRoot, err)
			}
			if len(entries) == 0 {
				t.Errorf("cache root %q is empty after build — --cache-dir / FLATE_CACHE_DIR did not flow through", cacheRoot)
			}
		})
	}
}

func TestOutputValue_Validates(t *testing.T) {
	var out string
	v := &outputValue{target: &out, allowed: []format.Output{format.OutputYAML, format.OutputJSON}}

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

func TestProfileValue_Validates(t *testing.T) {
	var mode string
	v := &profileValue{target: &mode}

	for _, ok := range []string{"cpu", "mem", "block", "mutex", "trace", ""} {
		mode = "sentinel"
		if err := v.Set(ok); err != nil {
			t.Errorf("%q should be accepted: %v", ok, err)
		}
		if mode != ok {
			t.Errorf("Set(%q) should update target, got %q", ok, mode)
		}
	}

	err := v.Set("heap")
	if err == nil {
		t.Fatal("invalid mode should be rejected at parse time")
	}
	if !strings.Contains(err.Error(), "must be one of: cpu, mem, block, mutex, trace") {
		t.Errorf("error should name the accepted set: %q", err)
	}
}

func writeBaselineGit(t *testing.T, root string, urls ...string) {
	t.Helper()
	var cfg strings.Builder
	cfg.WriteString("[core]\n  repositoryformatversion = 0\n")
	for i, url := range urls {
		fmt.Fprintf(&cfg, "[remote \"remote-%d\"]\n  url = %s\n", i, url)
	}
	testutil.WriteFile(t, root, ".git/config", cfg.String())
	testutil.WriteFile(t, root, ".git/HEAD", "ref: refs/heads/main\n")
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
		name       string
		currentGit bool
		baseline   string
		want       []string
	}{
		{name: "different origins", currentGit: true, baseline: "different", want: []string{baselineURL}},
		{name: "matching origins", currentGit: true, baseline: "matching", want: []string{currentURL}},
		{name: "snapshot fallback", currentGit: true, want: []string{currentURL}},
		{name: "no repositories"},
		{name: "no remotes", currentGit: true, baseline: "empty"},
		{name: "baseline only", baseline: "different", want: []string{baselineURL}},
		{name: "linked worktree", currentGit: true, baseline: "linked", want: []string{baselineURL}},
		{name: "nested scan", currentGit: true, baseline: "nested", want: []string{baselineURL}},
		{name: "sorted remotes", currentGit: true, baseline: "sorted", want: []string{currentURL, baselineURL}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := t.TempDir(), t.TempDir()
			for _, dir := range []string{current, orig} {
				if got := repoRootOf(dir); got != dir {
					t.Fatalf("fixture inherited a parent repository: repoRootOf(%q) = %q", dir, got)
				}
			}
			if tc.currentGit {
				writeBaselineGit(t, current, currentURL)
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
			c := commonFlags{path: current, pathOrig: scan, pathOrigSelfURLs: []string{"stale"}}
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer cleanup()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, c.pathOrigSelfURLs); diff != "" {
				t.Errorf("baseline identity (-want +got):\n%s", diff)
			}
			if got := c.baselineRoot(); got != orig {
				t.Errorf("baselineRoot = %q, want %q", got, orig)
			}
		})
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
		{name: "baseline git metadata", file: ".git", data: "invalid gitdir\n", want: "open baseline tree"},
		{name: "missing linked gitdir", file: ".git", data: "gitdir: missing\n", want: "open baseline tree"},
		{name: "baseline config", file: ".git/config", data: "[invalid", want: "open baseline tree"},
		{name: "baseline stat", want: "stat baseline tree git metadata"},
		{name: "current fallback config", file: ".git/config", data: "[invalid", current: true, want: "open working tree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, orig := t.TempDir(), t.TempDir()
			writeBaselineGit(t, current, "https://example.invalid/fork.git")
			selected := orig
			if tc.current {
				selected = current
			} else if tc.file == ".git/config" {
				writeBaselineGit(t, orig, "https://example.invalid/upstream.git")
			}
			if tc.file == "" {
				if err := os.Symlink(".git", filepath.Join(orig, ".git")); err != nil {
					t.Fatal(err)
				}
			} else {
				testutil.WriteFile(t, selected, tc.file, tc.data)
			}
			c := commonFlags{path: current, pathOrig: orig, pathOrigSelfURLs: []string{"stale"}}
			cleanup, err := resolveBaseline(t.Context(), &c, false)
			defer cleanup()
			if err == nil || !strings.Contains(err.Error(), tc.want) || errors.Unwrap(err) == nil {
				t.Fatalf("error = %v, want wrapped %q error", err, tc.want)
			}
			if len(c.pathOrigSelfURLs) != 0 {
				t.Errorf("selected repository failure retained URLs: %v", c.pathOrigSelfURLs)
			}
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
