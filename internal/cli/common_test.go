package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
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

func TestScopedNamespaces_ExplicitNamespaceWins(t *testing.T) {
	c := &commonFlags{namespace: "media"}
	got := c.scopedNamespaces(&change.Filter{})
	if _, ok := got["media"]; !ok || len(got) != 1 {
		t.Errorf("explicit -n media not honored: %v", got)
	}
}

func TestScopedNamespaces_PathOrigAutoScopesToKeepSet(t *testing.T) {
	c := &commonFlags{namespace: ""}
	mediaHR := &manifest.HelmRelease{Name: "x", Namespace: "media"}
	netHR := &manifest.HelmRelease{Name: "y", Namespace: "networking"}
	f := change.NewFilter(
		change.NewSet([]string{"media.yaml", "networking.yaml"}),
		map[manifest.NamedResource]string{
			mediaHR.Named(): "media.yaml",
			netHR.Named():   "networking.yaml",
		},
		"",
		testutil.EmptyLister(),
	)
	got := c.scopedNamespaces(f)
	for _, want := range []string{"media", "networking"} {
		if _, ok := got[want]; !ok {
			t.Errorf("auto-scope missing %q: got=%v", want, got)
		}
	}
}

func TestScopedNamespaces_NoFilterMeansAll(t *testing.T) {
	c := &commonFlags{namespace: ""}
	// Disabled filter (Changes == nil) → no scope (all namespaces).
	if got := c.scopedNamespaces(&change.Filter{}); got != nil {
		t.Errorf("expected nil (all-namespaces), got %v", got)
	}
}

func TestIncludeNamespace_ClusterScopedAlwaysIncluded(t *testing.T) {
	c := &commonFlags{namespace: "media"}
	if !c.includeNamespace(&change.Filter{}, "") {
		t.Error("cluster-scoped (empty) namespace must always pass")
	}
}

func TestIncludeNamespace_RespectsExplicitFilter(t *testing.T) {
	c := &commonFlags{namespace: "media"}
	if !c.includeNamespace(&change.Filter{}, "media") {
		t.Error("matching namespace must pass")
	}
	if c.includeNamespace(&change.Filter{}, "default") {
		t.Error("non-matching namespace must fail")
	}
}

func TestScopedRunError_FiltersOutsideNamespace(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		{Kind: manifest.KindKustomization, Namespace: "media", Name: "plex"}: {
			Status:  store.StatusFailed,
			Message: "media failed",
		},
		{Kind: manifest.KindKustomization, Namespace: "default", Name: "other"}: {
			Status:  store.StatusFailed,
			Message: "default failed",
		},
	}}

	got := scopedRunError(o, res, &commonFlags{namespace: "media"}, aggregateScopedFailures(res.Failed, nil))
	if got == nil {
		t.Fatal("expected scoped failure")
	}
	if !strings.Contains(got.Error(), "media failed") {
		t.Errorf("scoped error missing media failure: %v", got)
	}
	if strings.Contains(got.Error(), "default failed") {
		t.Errorf("scoped error included default failure: %v", got)
	}
}

func TestScopedRunError_ReturnsNilWhenOnlyOutsideNamespaceFailed(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		{Kind: manifest.KindKustomization, Namespace: "default", Name: "other"}: {
			Status:  store.StatusFailed,
			Message: "default failed",
		},
	}}

	if got := scopedRunError(o, res, &commonFlags{namespace: "media"}, aggregateScopedFailures(res.Failed, nil)); got != nil {
		t.Fatalf("scopedRunError = %v, want nil", got)
	}
}

func TestScopedRunError_PreservesUnattributedRunError(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		{Kind: manifest.KindKustomization, Namespace: "default", Name: "other"}: {
			Status:  store.StatusFailed,
			Message: "default failed",
		},
	}}
	panicErr := errors.New("1 task(s) panicked without per-resource attribution; check logs")
	runErr := errors.Join(aggregateScopedFailures(res.Failed, nil), panicErr)

	got := scopedRunError(o, res, &commonFlags{namespace: "media"}, runErr)
	if got == nil {
		t.Fatal("expected unattributed error to be preserved")
	}
	if !errors.Is(got, panicErr) {
		t.Errorf("scoped error did not preserve panic error identity: %v", got)
	}
	if strings.Contains(got.Error(), "default failed") {
		t.Errorf("scoped error included hidden resource failure: %v", got)
	}
}

func TestScopedRunError_CancellationStillFiltersHiddenFailures(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		{Kind: manifest.KindKustomization, Namespace: "default", Name: "other"}: {
			Status:  store.StatusFailed,
			Message: "default failed",
		},
	}}
	runErr := errors.Join(aggregateScopedFailures(res.Failed, nil), context.Canceled)

	got := scopedRunError(o, res, &commonFlags{namespace: "media"}, runErr)
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("scopedRunError should preserve cancellation identity, got %v", got)
	}
	if strings.Contains(got.Error(), "default failed") {
		t.Errorf("canceled scoped error included hidden resource failure: %v", got)
	}
}

func TestScopedRunError_JoinsScopedAndUnattributed(t *testing.T) {
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New orchestrator: %v", err)
	}
	res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{
		{Kind: manifest.KindKustomization, Namespace: "media", Name: "plex"}: {
			Status:  store.StatusFailed,
			Message: "media failed",
		},
		{Kind: manifest.KindKustomization, Namespace: "default", Name: "other"}: {
			Status:  store.StatusFailed,
			Message: "default failed",
		},
	}}
	panicErr := errors.New("1 task(s) panicked without per-resource attribution; check logs")
	runErr := errors.Join(aggregateScopedFailures(res.Failed, nil), panicErr)

	got := scopedRunError(o, res, &commonFlags{namespace: "media"}, runErr)
	if got == nil {
		t.Fatal("expected scoped failure")
	}
	if !strings.Contains(got.Error(), "media failed") {
		t.Errorf("scoped error missing visible failure: %v", got)
	}
	if !errors.Is(got, panicErr) {
		t.Errorf("scoped error did not preserve panic error identity: %v", got)
	}
	if strings.Contains(got.Error(), "default failed") {
		t.Errorf("scoped error included hidden resource failure: %v", got)
	}
}

// TestOutputValue_Validates pins the -o enum flag: a value in the accepted
// set updates the target; anything else is rejected (naming the set), which
// is how pflag/cobra surface the error at parse time.
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

// TestAggregateScopedFailures_FoldsBlocked pins the concise machine error: it
// enumerates only primary (root-cause) failures and tallies the blocked cascade
// as a count, so get/diff print a root-cause summary rather than a wall of every
// cascade victim. It is typed so scopedRunError can recognize and re-scope it.
func TestAggregateScopedFailures_FoldsBlocked(t *testing.T) {
	root := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster-apps"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "media", Name: "plex"}
	failed := map[manifest.NamedResource]store.StatusInfo{
		root:  {Status: store.StatusFailed, Message: "kustomize build boom"},
		child: {Status: store.StatusFailed, Message: "dependencies failed: cluster-apps"},
	}
	blocked := map[manifest.NamedResource][]manifest.NamedResource{child: {root}}

	err := aggregateScopedFailures(failed, blocked)
	msg := err.Error()
	if !strings.Contains(msg, "cluster-apps") || !strings.Contains(msg, "kustomize build boom") {
		t.Errorf("primary failure should be enumerated: %q", msg)
	}
	if strings.Contains(msg, "media/plex") || strings.Contains(msg, "dependencies failed") {
		t.Errorf("blocked cascade victim should be folded, not enumerated: %q", msg)
	}
	if !strings.Contains(msg, "1 blocked") {
		t.Errorf("blocked count should be summarized: %q", msg)
	}
	if _, ok := errors.AsType[*orchestrator.FailuresError](err); !ok {
		t.Errorf("aggregate must be a *orchestrator.FailuresError, got %T", err)
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
		name                             string
		args                             []string
		env                              string
		setEnv, detect, disable, invalid bool
	}{
		{name: "absent", detect: true},
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
			assert.Equal(t, got.DetectChartDigestTracking, tc.detect)
			assert.Equal(t, got.HelmOptions.DisableChartDigestTracking, tc.disable)
			assert.Equal(t, h.digestTrackingFlag.Changed, !tc.detect)
		})
	}
	got := buildOrchCfg(commonFlags{}, helmFlags{})
	assert.Equal(t, got.DetectChartDigestTracking, true)
	assert.Equal(t, got.HelmOptions.DisableChartDigestTracking, false)
	got = buildOrchCfg(commonFlags{}, helmFlags{disableChartDigestTracking: true})
	assert.Equal(t, got.HelmOptions.DisableChartDigestTracking, true)
}

func TestHelmFlags_ChartDigestTrackingBindings(t *testing.T) {
	root := New("test")
	for _, verb := range []string{"build", "get", "diff", "test"} {
		kinds := []string{"ks", "hr", "all"}
		if verb == "get" || verb == "diff" {
			kinds = append(kinds, "images")
		}
		for _, kind := range kinds {
			t.Run(verb+" "+kind, func(t *testing.T) {
				cmd, _, err := root.Find([]string{verb, kind})
				if err != nil {
					t.Fatal(err)
				}
				flag := cmd.Flags().Lookup("disable-chart-digest-tracking")
				if flag == nil || flag.Value.Type() != "bool" || flag.NoOptDefVal != "true" {
					t.Fatal("missing boolean override binding")
				}
				if !strings.Contains(flag.Usage, envKey("disable-chart-digest-tracking")) {
					t.Fatal("missing environment usage")
				}
			})
		}
	}
}
