package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
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
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
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
			if c.owner == nil {
				c.owner = cachetest.New(t, "", sourcecache.New)
			}
			err := resolveBaseline(t.Context(), &c, false)
			if err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, c.pathOrigSelfURLs, tc.want)
			assert.Equal(t, c.pathRoot, current)
			assert.Equal(t, c.pathOrigRoot, orig)
			assert.Equal(t, [2]string{inputTrees(c)[1].RepoRoot, inputTrees(c)[0].RepoRoot}, [2]string{current, orig})
		})
	}
}

func TestResolveBaseline_FullTree(t *testing.T) {
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, ".git/config", "[invalid\n")
	c := commonFlags{path: root, pathRoot: "stale",
		pathOrigRoot: "stale", pathOrigSelfURLs: []string{"stale"}}
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err := resolveBaseline(t.Context(), &c, false)
	assert.Equal(t, err, nil)
	assert.Equal(t, c.pathRoot, "")
	assert.Equal(t, c.pathOrigRoot, "")
	assert.Equal(t, c.pathOrigSelfURLs == nil, true)
	testutil.WriteFile(t, root, ".git/config", "[core]\nrepositoryformatversion = 0\n")
	testutil.GitCommitAll(t, repo, "fixture", &git.CommitOptions{AllowEmptyCommits: true,
		Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}})
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
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err := resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
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
			if c.owner == nil {
				c.owner = cachetest.New(t, "", sourcecache.New)
			}
			err := resolveBaseline(t.Context(), &c, false)
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
				if err == nil || ran || tc.setEnv && !strings.Contains(err.Error(), key) {
					t.Fatalf("invalid selection result: err=%v ran=%t", err, ran)
				}
				return
			}
			if err != nil || !ran {
				t.Fatalf("command result: err=%v ran=%t", err, ran)
			}
			assert.Equal(t, got.HelmOptions.DisableChartDigestTracking, tc.disable)
		})
	}
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
	if c.owner == nil {
		c.owner = cachetest.New(t, "", sourcecache.New)
	}
	err := resolveBaseline(t.Context(), &c, false)
	assert.NoError(t, err)
	cfg := buildOrchCfg(c)
	assert.Equal(t, inputTrees(c)[1].Input, c.input)
	assert.Equal(t, inputTrees(c)[0].Input, c.baseInput)
	assert.Equal(t, inputTrees(c)[1].Path, currentScan)
	assert.Equal(t, c.pathOrig, originalScan)
	assert.Equal(t, inputTrees(c)[1].RepoRoot, current)
	assert.Equal(t, inputTrees(c)[0].RepoRoot, original)
	for _, input := range []*gittree.FS{c.input.FS.(*gittree.FS), c.baseInput.FS.(*gittree.FS)} {
		location := input.Location()
		assert.Diff(t, location.URLs, []string{url})
		assert.NoError(t, os.Remove(filepath.Join(location.RepositoryRoot, ".git", "config")))
	}
	runs, err := orchestrator.RenderTrees(t.Context(), inputTrees(c), cfg)
	base, head := runs[0], runs[1]
	assert.NoError(t, err)
	id := testutil.KustomizationID("gitops-system", "apps")
	for i, side := range []orchestrator.Rendered{base, head} {
		docs := outputByParent(side.Result.Manifests)[id]
		if len(docs) != 1 || docs[0]["data"].(map[string]any)["greeting"] != []string{"before", "hi"}[i] {
			t.Fatalf("side %d lost admitted source identity: %v", i, side.Result.Manifests)
		}
	}
}

func TestRunSelected_ProgressLifecycle(t *testing.T) {
	for _, name := range []string{"success", "render-warning", "partial-render", "fatal-bootstrap", "fatal-construction"} {
		t.Run(name, func(t *testing.T) {
			oldBar, oldLogger, original := barSink, slog.Default(), os.Stderr
			var raw *os.File
			defer func() {
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
			flags := commonFlags{path: path, concurrency: 2}
			var selected *selectedInvocation
			err := runSelected(t.Context(), selectedRequest{
				flags: flags, roots: flags.roots("", runnableKinds...), stdout: io.Discard, stderr: io.Discard,
			}, func(inv *selectedInvocation) selectedPayload {
				selected = inv
				return selectedPayload{}
			})
			switch name {
			case "fatal-construction", "fatal-bootstrap":
				if err == nil || selected != nil {
					t.Fatalf("fatal run exposed selection=%v, err=%v", selected, err)
				}
			case "partial-render":
				if err == nil || selected == nil || len(selected.groups) != 1 || len(selected.groups[0].sides[0].Failed) == 0 {
					t.Fatalf("partial run lost selection: %+v", selected)
				}
			default:
				assert.NoError(t, err)
				if selected == nil || len(selected.groups) != 1 || len(selected.groups[0].sides[0].Objects) == 0 {
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
		{name: "CRD only", args: []string{"--kube-version=1.31.2", "-a", "fixture.example/v1", "--enable-dns", "--skip-kinds=CustomResourceDefinition"}, want: helm.Options{KubeVersion: "1.31.2", APIVersions: "fixture.example/v1", EnableDNS: true, SkipTests: true, SkipCRDs: true, SkipKinds: []string{}}},
		{name: "mixed", args: []string{"--skip-kinds=Secret,CustomResourceDefinition,ConfigMap"}, want: helm.Options{KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{"Secret", "ConfigMap"}}},
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
		err := finishSelected(selectedRequest{roots: []manifest.NamedResource{{Kind: manifest.KindHelmRelease}}, pair: true, stderr: &stderr},
			&selectedInvocation{groups: []selectedGroup{{cluster: ".", sides: []*selection.Selection{side, {Warnings: []manifest.Warning{base}}}}}}, selectedPayload{})
		if err != nil {
			t.Fatal(err)
		}
		expected := output.Report{Groups: []output.ReportGroup{{Cluster: "."}}}
		for _, warning := range want {
			expected.Warnings = append(expected.Warnings, output.Warning{Warning: warning, Context: "orig snapshot"})
		}
		expected.Warnings = append(expected.Warnings, output.Warning{Warning: base, Context: "current snapshot"})
		expected.Prepare(nil)
		var rendered bytes.Buffer
		encoder := output.Encoder{Writer: &rendered}
		assert.NoError(t, encoder.Report(expected, encoder))
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
	if allocs := testing.AllocsPerRun(1, func() { selectedWarnings(&selection.Selection{}) }); allocs != 0 {
		t.Fatalf("empty diagnostics allocated: %v", allocs)
	}
}

func TestFinishSelected_SuppliedAdvisoryUnion(t *testing.T) {
	t.Parallel()
	ordinary := manifest.Warning{Resource: testutil.HelmReleaseID("apps", "demo"), Category: manifest.WarnStaleValues, Message: "ordinary", Detail: []string{"kept"}, Count: 3}
	other := ordinary
	other.Message, other.Detail = "another ordinary", []string{"another", "kept"}
	inv := &selectedInvocation{}
	for _, cluster := range []string{"alpha", "omega"} {
		advisories := []manifest.Warning{
			{Category: manifest.WarnSuppliedPrecedence, Message: "repository wins", Detail: []string{cluster, "common", "first", "last"}, Count: 1},
		}
		inv.groups = append(inv.groups, selectedGroup{cluster: cluster, sides: []*selection.Selection{
			{Warnings: []manifest.Warning{ordinary, other}},
			{Warnings: []manifest.Warning{ordinary, other}, ClusterWarnings: advisories},
		}})
	}
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		assert.NoError(t, finishSelected(selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr}, inv, selectedPayload{}))
		text := stderr.String()
		for _, token := range []string{"warnings (10)"} {
			assert.Equal(t, strings.Count(text, token), 1)
		}
		assert.Equal(t, strings.Count(text, "[SuppliedPrecedence]"), 2)
		assert.Equal(t, strings.Count(text, "[StaleValues]"), 8)
		for _, cluster := range []string{"alpha", "omega"} {
			for _, row := range []string{
				"[SuppliedPrecedence] " + cluster + ": repository wins\n      " + cluster + ", common, first, last\n",
			} {
				assert.Equal(t, strings.Count(text, row), 1)
			}
			for _, context := range []string{"orig snapshot", "current snapshot"} {
				for _, warning := range []manifest.Warning{ordinary, other} {
					row := "[StaleValues] " + context + ": HelmRelease " + cluster + ": apps/demo: " + warning.Message + " (×3)\n      " + strings.Join(warning.Detail, ", ") + "\n"
					assert.Equal(t, strings.Count(text, row), 1)
				}
			}
		}
		if strings.Contains(text, "Cluster:") || strings.Contains(text, output.GlyphFail) {
			t.Fatalf("advisories changed report framing or verdict: %s", text)
		}
		if run == 1 {
			assert.Equal(t, text, first)
		}
		first = text
		for _, group := range inv.groups {
			for _, side := range group.sides {
				assert.Diff(t, side.Warnings, []manifest.Warning{ordinary, other})
			}
		}
	}
	fatal := errors.New("fatal render failure")
	var stderr bytes.Buffer
	err := finishSelected(selectedRequest{pair: true, stdout: io.Discard, stderr: &stderr}, inv, selectedPayload{err: fatal})
	if !errors.Is(err, fatal) || !strings.Contains(stderr.String(), "another ordinary (×3)") {
		t.Fatalf("warnings changed fatal result or lost messages: %v\n%s", err, stderr.String())
	}
}

func TestRunSelected_RemainderAndPayloadErrors(t *testing.T) {
	for _, test := range []bool{false, true} {
		t.Run(fmt.Sprint(test), func(t *testing.T) {
			if !test {
				t.Parallel()
			}
			o, err := completedOrchestrator(t)
			if err != nil {
				t.Fatal(err)
			}
			id := testutil.KustomizationID("apps", "selected")
			o.Store().AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
			res := &orchestrator.Result{Failed: map[manifest.NamedResource]store.StatusInfo{id: {Status: store.StatusFailed, Message: "selected render failed"}}}
			resourceErr := &orchestrator.FailuresError{Message: "aggregate render failure"}
			closeErr, writeErr := errors.New("shared cleanup failed"), errors.New("payload write failed")
			runs := []orchestrator.Rendered{{Orchestrator: o, Result: res, Err: resourceErr}}
			roots := []manifest.NamedResource{id}
			sides, selectedErr, remainder := orchestrator.Select(runs, errors.Join(fmt.Errorf("head tree: %w", errors.Join(resourceErr, context.Canceled)), fmt.Errorf("cache close: %w", closeErr)), roots, nil)
			if len(sides) != 1 || selectedErr == nil {
				t.Fatalf("completed run selection: %v", selectedErr)
			}
			var stdout, stderr bytes.Buffer
			_, payloadErr := failingWriter{err: writeErr}.Write([]byte("partial"))
			payload := selectedPayload{err: payloadErr}
			if test {
				payload.rosters = []output.TestReport{{Cases: []output.Case{{ID: id, Outcome: output.OutcomePassed}}, Passed: 1, Matched: 1}}
			}
			got := finishSelected(selectedRequest{roots: roots, stdout: &stdout, stderr: &stderr, test: test},
				&selectedInvocation{groups: []selectedGroup{{cluster: ".", sides: sides, err: selectedErr, remainder: remainder}}}, payload)
			if !errors.Is(got, closeErr) || !errors.Is(got, writeErr) || !errors.Is(got, context.Canceled) || !errors.Is(got, selectedErr) {
				t.Fatalf("lost selected, cleanup or payload error: %v", got)
			}
			if _, reported := errors.AsType[reportedError](got); !reported {
				if err := (output.Encoder{Writer: &stderr}).Errors(got); err != nil {
					t.Fatal(err)
				}
			}
			for _, message := range []string{"selected render failed", closeErr.Error(), writeErr.Error(), context.Canceled.Error()} {
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
			if code != 1 || stdout.Len() != 0 || strings.Count(stderr.String(), "Git status unavailable") != 1 || strings.Count(stderr.String(), "missing objects") != 1 || strings.Contains(stderr.String(), `no Kustomization named`) {
				t.Fatalf("diagnostic-only %v = exit %d, stdout %q, stderr %q", command, code, stdout.String(), stderr.String())
			}
		})
	}
	for _, tc := range []struct {
		name       string
		test, pair bool
	}{{name: "data"}, {name: "test", test: true}, {name: "diff", pair: true}} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			called := false
			rootID := testutil.KustomizationID("", "")
			if tc.test {
				rootID.Name = "named"
			}
			err := runSelected(t.Context(), selectedRequest{
				flags: commonFlags{path: root, base: pin.String(), concurrency: 2},
				roots: []manifest.NamedResource{rootID},
				pair:  tc.pair, stdout: &stdout, stderr: &stderr, test: tc.test,
			}, func(*selectedInvocation) selectedPayload { called = true; return selectedPayload{} })
			if err == nil || called || !strings.Contains(stderr.String(), "Git status unavailable") {
				t.Fatalf("incomplete input invoked product=%v: %v, %s", called, err, stderr.String())
			}
		})
	}
}

func TestRenderTrees_InputWarningOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	warning := manifest.Warning{Category: manifest.WarnBaselineStatusUnavailable, Message: "before failure", Count: 1}
	runs, err := orchestrator.RenderTrees(t.Context(), []orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root, Input: &store.SourceArtifact{LocalPath: root, Warnings: []manifest.Warning{warning}}}}, orchestrator.Config{Concurrency: 2})
	if len(runs) != 1 {
		t.Fatalf("missing diagnostic run: %v", err)
	}
	run := runs[0]
	if run.Orchestrator != nil || run.Result == nil || !errors.Is(run.Err, manifest.ErrInput) || len(run.Result.Manifests)+len(run.Result.Failed) != 0 {
		t.Fatalf("failed bootstrap exposed content: %+v", run)
	}
	assert.Diff(t, run.Result.Warnings, []manifest.Warning{warning})
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprintf("unavailable_%t", unavailable), func(t *testing.T) {
			t.Parallel()
			baseWarning, headWarning := warning, warning
			baseWarning.Message, headWarning.Message = "base before failure", "head before failure"
			base := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(filepath.Join(root, "missing")), Warnings: []manifest.Warning{baseWarning}}
			head := &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root), Warnings: []manifest.Warning{headWarning}}
			wantErr := os.ErrNotExist
			if unavailable {
				base.FS = nil
				wantErr = manifest.ErrInput
			}
			runs, err := orchestrator.RenderTrees(t.Context(), []orchestrator.Tree{
				{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Path: root, Input: base, BaseInput: head, ExternalChanges: change.NewSet(nil)},
				{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: root, Input: head, BaseInput: base, ExternalChanges: change.NewSet(nil)},
			}, orchestrator.Config{Concurrency: 2, HeadOnly: true})
			if !errors.Is(err, wantErr) {
				t.Fatalf("baseline discovery error lost: %v", err)
			}
			var warnings []manifest.Warning
			for _, run := range runs {
				if run.Orchestrator != nil || run.Result == nil || len(run.Result.Manifests)+len(run.Result.Failed) != 0 {
					t.Fatalf("failed preparation exposed content: %+v", run)
				}
				for _, w := range run.Result.Warnings {
					if w.Category == manifest.WarnBaselineStatusUnavailable {
						warnings = append(warnings, w)
					}
				}
			}
			assert.Diff(t, warnings, []manifest.Warning{baseWarning, headWarning})
			roots := []manifest.NamedResource{testutil.KustomizationID("", "")}
			sides, selectedErr, remainder := orchestrator.Select(runs, err, roots, nil)
			if !errors.Is(selectedErr, wantErr) || !errors.Is(remainder, wantErr) || len(sides) != len(runs) {
				t.Fatalf("preparation projection = %+v %v %v", sides, selectedErr, remainder)
			}
			var stderr bytes.Buffer
			finished := finishSelected(selectedRequest{roots: roots, stderr: &stderr},
				&selectedInvocation{groups: []selectedGroup{{cluster: ".", sides: sides, err: selectedErr, remainder: remainder}}}, selectedPayload{})
			if _, reported := errors.AsType[reportedError](finished); !reported {
				assert.NoError(t, (output.Encoder{Writer: &stderr}).Errors(finished))
			}
			for _, w := range []manifest.Warning{baseWarning, headWarning} {
				assert.Equal(t, strings.Count(stderr.String(), w.Message), 1)
			}
			if !unavailable {
				cause, ok := errors.AsType[*os.PathError](err)
				if !ok || strings.Count(stderr.String(), cause.Error()) != 1 {
					t.Fatalf("baseline discovery cause was duplicated: %v\n%s", err, stderr.String())
				}
			}
		})
	}
}

func TestFinishSelected_JoinsWriterErrors(t *testing.T) {
	t.Parallel()
	for _, test := range []bool{false, true} {
		t.Run(fmt.Sprint(test), func(t *testing.T) {
			t.Parallel()
			productErr, reportErr, renderErr := errors.New("product closed"), errors.New("report closed"), errors.New("render failed")
			side := &selection.Selection{Warnings: []manifest.Warning{{Category: manifest.WarnEmptyScan, Message: "advisory"}}}
			inv := &selectedInvocation{groups: []selectedGroup{{cluster: ".", sides: []*selection.Selection{side, side}, err: renderErr, remainder: renderErr}}}
			payload := selectedPayload{err: productErr, warnings: [][]manifest.Warning{{{Category: manifest.WarnDiffSuppressed, Message: "payload warning"}}}}
			if test {
				payload.err = nil
				payload.rosters = []output.TestReport{{}}
			}
			writer := &countFailWriter{err: reportErr}
			err := finishSelected(selectedRequest{roots: []manifest.NamedResource{{Kind: manifest.KindKustomization}}, stdout: failingWriter{err: productErr}, stderr: writer, test: test, pair: true}, inv, payload)
			for _, want := range []error{productErr, reportErr, renderErr} {
				if !errors.Is(err, want) {
					t.Fatalf("lost %v: %v", want, err)
				}
			}
			if writer.writes < 2 {
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

func outputByParent(batches []manifest.DocumentBatch) map[manifest.NamedResource][]map[string]any {
	out := make(map[manifest.NamedResource][]map[string]any, len(batches))
	for _, batch := range batches {
		if batch.Parent != nil {
			out[batch.Parent.Named()] = batch.Documents
		}
	}
	return out
}

func completedOrchestrator(tb testing.TB) (*orchestrator.Orchestrator, error) {
	tb.Helper()
	runs, err := orchestrator.RenderTrees(tb.Context(), []orchestrator.Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: tb.TempDir()}}, orchestrator.Config{Concurrency: 2})
	if err != nil {
		return nil, err
	}
	if len(runs) != 1 || runs[0].Orchestrator == nil {
		tb.Fatal("missing completed store")
	}
	return runs[0].Orchestrator, nil
}

func TestRun_ClusterReport_KindCollisionInventory(t *testing.T) {
	t.Parallel()
	widget := manifest.NamedResource{Group: "one.example", Kind: "Widget", Namespace: "ns", Name: "same"}
	other := widget
	other.Group = "two.example"
	gadget := manifest.NamedResource{Group: "one.example", Kind: "Gadget", Namespace: "ns", Name: "root"}
	victims := make([]manifest.NamedResource, 7)
	for i := range victims {
		victims[i] = manifest.NamedResource{Kind: "Leaf", Namespace: "ns", Name: fmt.Sprintf("leaf%d", i)}
	}
	victims[6] = manifest.NamedResource{Group: "hidden.example", Kind: "Gadget", Name: "hidden"}
	warning := manifest.Warning{Resource: other, Category: "Inventory", Count: 3,
		Message: "Widget.one.example/ns/opaque stays unchanged", Detail: []string{"Gadget.hidden.example/ns/reference"}}
	alpha := &selection.Selection{Failed: map[manifest.NamedResource]store.StatusInfo{
		widget: {Status: store.StatusFailed, Message: "widget failed"}, gadget: {Status: store.StatusFailed, Message: "gadget failed"}},
		Causes: map[manifest.NamedResource][]manifest.NamedResource{widget: nil, gadget: victims}}
	omega := &selection.Selection{Warnings: []manifest.Warning{warning}}
	var first string
	for run := range 2 {
		var stderr bytes.Buffer
		assert.NoError(t, finishSelected(selectedRequest{stderr: &stderr, stdout: io.Discard},
			&selectedInvocation{groups: []selectedGroup{{cluster: "alpha", sides: []*selection.Selection{alpha}}, {cluster: "omega", sides: []*selection.Selection{omega}}}}, selectedPayload{}))
		out := stderr.String()
		for _, want := range []string{"Widget.one.example  alpha: ns/same", "Widget.two.example omega: ns/same: " + warning.Message,
			"Gadget  alpha: ns/root", "alpha: ns/leaf5 +1 more", warning.Detail[0], "(×3)"} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q: %s", want, out)
			}
		}
		if strings.Contains(out, "Gadget.one.example") || strings.Contains(out, "hidden.example  ") || strings.Contains(out, "Cluster:") {
			t.Fatalf("hidden subjects changed the inventory: %s", out)
		}
		if run == 0 {
			first = out
		} else {
			assert.Equal(t, out, first)
		}
	}
	assert.Diff(t, omega.Warnings, []manifest.Warning{warning})
}

func TestFinishSelected_FailureContext(t *testing.T) {
	t.Parallel()
	for _, pair := range []bool{false, true} {
		t.Run(fmt.Sprintf("pair_%t", pair), func(t *testing.T) {
			t.Parallel()
			root := testutil.KustomizationID("ns", "root")
			missing := testutil.KustomizationID("ns", "missing")
			leaf := testutil.KustomizationID("ns", "leaf")
			dependent := testutil.KustomizationID("ns", "dependent")
			labels := []string{"single"}
			if pair {
				labels = []string{"orig", "current"}
			}
			var sides []*selection.Selection
			var rows []string
			for _, label := range labels {
				sides = append(sides, &selection.Selection{
					Failed:  map[manifest.NamedResource]store.StatusInfo{root: {Status: store.StatusFailed, Message: label + " failure\nunchanged details"}},
					Blocked: map[manifest.NamedResource][]manifest.NamedResource{leaf: {root}, dependent: {missing}},
					Causes:  map[manifest.NamedResource][]manifest.NamedResource{root: {leaf}, missing: {dependent}},
				})
				prefix := ""
				if pair {
					prefix = label + " snapshot: "
				}
				rows = append(rows, "  ✗  "+prefix+"Kustomization  ns/root  "+label+" failure\n"+
					"         unchanged details\n"+
					"         "+prefix+"blocks ns/leaf\n"+
					"  ✗  "+prefix+"Kustomization  ns/missing  not found\n"+
					"         "+prefix+"required by ns/dependent")
			}
			var stderr bytes.Buffer
			err := finishSelected(selectedRequest{pair: pair, stdout: io.Discard, stderr: &stderr},
				&selectedInvocation{groups: []selectedGroup{{cluster: ".", sides: sides, err: errors.New("render failed")}}}, selectedPayload{})
			if _, reported := errors.AsType[reportedError](err); !reported {
				t.Fatalf("failure was not marked reported: %v", err)
			}
			want := "\n" + strings.Join(rows, "\n\n") + fmt.Sprintf("\n\n  ✗ %d failed · %d blocked\n", 2*len(sides), 2*len(sides))
			assert.Equal(t, stderr.String(), want)
		})
	}
}

func TestFinishSelected_ReportOrder(t *testing.T) {
	t.Parallel()
	id := testutil.KustomizationID("ns", "same")
	var combined bytes.Buffer
	stdout, stderr := &recordingStream{all: &combined}, &recordingStream{all: &combined}
	failure := store.StatusInfo{Status: store.StatusFailed, Message: "primary cause"}
	side := &selection.Selection{Failed: map[manifest.NamedResource]store.StatusInfo{id: failure},
		Causes:   map[manifest.NamedResource][]manifest.NamedResource{id: nil},
		Warnings: []manifest.Warning{{Resource: id, Message: "selected warning"}}}
	payload := selectedPayload{rosters: []output.TestReport{{Cases: []output.Case{{ID: id, Outcome: output.OutcomeFailed, Reason: failure.Message}}, Failed: 1},
		{Cases: []output.Case{{ID: id, Outcome: output.OutcomePassed}}, Passed: 1}}, write: func() error {
		_, err := io.WriteString(stdout, "payload\n")
		return err
	}}
	err := finishSelected(selectedRequest{roots: []manifest.NamedResource{id}, stdout: stdout, stderr: stderr, test: true},
		&selectedInvocation{groups: []selectedGroup{{cluster: "alpha", sides: []*selection.Selection{side}, err: errors.New("selected failed")}, {cluster: "omega", sides: []*selection.Selection{{}}}}}, payload)
	if err == nil {
		t.Fatal("test failure became successful")
	}
	out := combined.String()
	last := -1
	for _, token := range []string{"payload", "primary cause", "omega: ns/same", "selected warning", "alpha: ✗ 0 passed", "omega: ✓ 1 passed", "Invocation totals", "✗ 1 passed · 1 failed"} {
		at := strings.Index(out, token)
		if at <= last {
			t.Fatalf("%q out of order: %s", token, out)
		}
		last = at
	}
	assert.Equal(t, strings.Count(out, "primary cause"), 1)
	assert.Equal(t, strings.Contains(stdout.String(), "warning"), false)
	assert.Equal(t, strings.Contains(stderr.String(), "Invocation totals"), false)
}

type recordingStream struct {
	bytes.Buffer
	all *bytes.Buffer
}

func (w *recordingStream) Write(p []byte) (int, error) {
	_, _ = w.all.Write(p)
	return w.Buffer.Write(p)
}

func (w *recordingStream) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

func TestFinishSelected_UnpresentedCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("unpresented cause")
	var stderr bytes.Buffer
	err := finishSelected(selectedRequest{stdout: io.Discard, stderr: &stderr},
		&selectedInvocation{groups: []selectedGroup{{cluster: "alpha", err: cause}}}, selectedPayload{})
	if !errors.Is(err, cause) {
		t.Fatalf("lost cause: %v", err)
	}
	if _, reported := errors.AsType[reportedError](err); reported || stderr.Len() != 0 {
		t.Fatalf("unwritten diagnostics marked reported: %v, %s", err, stderr.String())
	}
}

func TestFinishSelected_PhaseWriterErrors(t *testing.T) {
	t.Parallel()
	for _, phase := range []struct {
		name, token string
		test        bool
	}{
		{name: "payload", token: "phase payload"},
		{name: "warning", token: "phase advisory"},
		{name: "summary", token: "alpha: ✗ 1 failed"},
		{name: "test verdict", token: "Invocation totals", test: true},
	} {
		t.Run(phase.name, func(t *testing.T) {
			t.Parallel()
			writeErr, renderErr := errors.New("phase write failed"), errors.New("render failed")
			writer := &phaseFailWriter{token: phase.token, err: writeErr}
			id := testutil.KustomizationID("ns", "root")
			side := &selection.Selection{
				Failed:   map[manifest.NamedResource]store.StatusInfo{id: {Status: store.StatusFailed, Message: "phase primary cause"}},
				Causes:   map[manifest.NamedResource][]manifest.NamedResource{id: nil},
				Warnings: []manifest.Warning{{Resource: id, Message: "phase advisory"}},
			}
			inv := &selectedInvocation{groups: []selectedGroup{
				{cluster: "alpha", sides: []*selection.Selection{side}, err: renderErr},
				{cluster: "omega", sides: []*selection.Selection{{}}},
			}}
			payload := selectedPayload{write: func() error {
				_, err := io.WriteString(writer, "phase payload\n")
				return err
			}}
			if phase.test {
				payload.rosters = []output.TestReport{{Failed: 1}, {Passed: 1}}
			}
			err := finishSelected(selectedRequest{stdout: writer, stderr: writer}, inv, payload)
			if !writer.failed || !errors.Is(err, writeErr) || !errors.Is(err, renderErr) {
				t.Fatalf("%s lost a write or render failure: failed=%v err=%v", phase.name, writer.failed, err)
			}
			assert.Equal(t, strings.Count(writer.String(), "phase primary cause"), 1)
			if phase.name == "summary" || phase.test {
				assert.Equal(t, strings.Count(writer.String(), "phase payload"), 1)
				assert.Equal(t, strings.Count(writer.String(), "phase advisory"), 1)
			}
		})
	}
}

type phaseFailWriter struct {
	bytes.Buffer
	token  string
	err    error
	failed bool
}

func (w *phaseFailWriter) Write(p []byte) (int, error) {
	if !w.failed && bytes.Contains(p, []byte(w.token)) {
		w.failed = true
		return 0, w.err
	}
	return w.Buffer.Write(p)
}

func (w *phaseFailWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }

func TestBindCommon_AllowMissingKinds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		args, want []string
	}{
		{name: "default"},
		{name: "Secret", args: []string{"--allow-missing-kinds=Secret"}, want: []string{manifest.KindSecret}},
		{name: "ConfigMap", args: []string{"--allow-missing-kinds=ConfigMap"}, want: []string{manifest.KindConfigMap}},
		{name: "CRD", args: []string{"--allow-missing-kinds=CustomResourceDefinition"}, want: []string{manifest.KindCustomResourceDefinition}},
		{name: "repeated CSV and duplicates", args: []string{"--allow-missing-kinds=Secret,ConfigMap", "--allow-missing-kinds=Secret,CustomResourceDefinition"}, want: []string{manifest.KindSecret, manifest.KindConfigMap, manifest.KindSecret, manifest.KindCustomResourceDefinition}},
		{name: "explicit empty", args: []string{"--allow-missing-kinds="}, want: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var flags commonFlags
			fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
			bindCommon(fs, &flags)
			assert.NoError(t, fs.Parse(tc.args))
			assert.NoError(t, flags.loadInputs(t.Context()))
			assert.Diff(t, buildOrchCfg(flags).AllowMissingKinds, tc.want)
		})
	}
}

func TestRun_AllowMissingKindsValidation(t *testing.T) {
	root := substitutionFixture(t, "", "literal")
	for _, arg := range []string{"secret", "Unknown", "true", "Secret,,ConfigMap", "Secret,"} {
		t.Run(arg, func(t *testing.T) {
			_, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--supply", filepath.Join(root, "absent.yaml"), "--allow-missing-kinds="+arg)
			if code != 1 || !strings.Contains(diagnostic, "--allow-missing-kinds") || strings.Contains(diagnostic, "--supply") {
				t.Fatalf("validation after input I/O: exit=%d %s", code, diagnostic)
			}
		})
	}
	_, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--allow-missing-kinds")
	if code != 1 || !strings.Contains(diagnostic, "needs an argument") {
		t.Fatalf("bare selection accepted: exit=%d %s", code, diagnostic)
	}
}

func TestEnv_AllowMissingKindsPrecedence(t *testing.T) {
	root := substitutionFixture(t, "", "literal")
	for _, tc := range []struct {
		name, environment string
		flags             []string
		fail              bool
	}{
		{name: "CSV", environment: "Secret,ConfigMap,CustomResourceDefinition"},
		{name: "invalid", environment: "secret", fail: true},
		{name: "CLI replaces environment", environment: "invalid", flags: []string{"--allow-missing-kinds=Secret"}},
		{name: "empty replaces environment", environment: "invalid", flags: []string{"--allow-missing-kinds="}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("FLATE_ALLOW_MISSING_KINDS", tc.environment)
			args := append([]string{"build", "all", "--path", root, "--concurrency=2"}, tc.flags...)
			_, diagnostic, code := runCLI(t, args...)
			if (code != 0) != tc.fail {
				t.Fatalf("exit=%d %s", code, diagnostic)
			}
		})
	}
}

func TestRun_FlagInventory(t *testing.T) {
	names := map[string]bool{}
	for _, route := range []string{"", "build all", "get ks", "get images", "test all", "diff all", "diff images", "cache gc", "completion fish"} {
		args := append(strings.Fields(route), "--help")
		help, diagnostic, code := runCLI(t, args...)
		if code != 0 {
			t.Fatalf("help %q: %s", route, diagnostic)
		}
		for line := range strings.SplitSeq(help, "\n") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			token := fields[0]
			if len(fields) > 1 && strings.HasSuffix(token, ",") {
				token = fields[1]
			}
			if after, ok := strings.CutPrefix(token, "--"); ok {
				names[after] = true
			}
		}
		if strings.Contains(help, "--base diff") {
			t.Fatalf("incorrect base metavar: %s", help)
		}
	}
	if len(names) != 30 {
		t.Fatalf("distinct flags=%d: %v", len(names), slices.Sorted(maps.Keys(names)))
	}
}

func TestRun_ProviderCredentialPolicy(t *testing.T) {
	root := substitutionFixture(t, "", "local")
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, source.yaml, remote.yaml]\n")
	testutil.WriteFile(t, root, "source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: cloud, namespace: apps}\nspec: {provider: aws, url: 'oci://fixture.invalid/image'}\n")
	testutil.WriteFile(t, root, "remote.yaml", testutil.KustomizationYAML("remote", "apps", "  path: ./remote\n  sourceRef: {kind: OCIRepository, name: cloud}\n"))
	registry := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registry, `{"auths":{}}`)
	for _, tc := range []struct {
		name, selection string
		want            int
	}{
		{name: "default", want: 1}, {name: "ConfigMap", selection: "ConfigMap", want: 1}, {name: "Secret", selection: "Secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, diagnostic, code := runCLI(t, "build", "all", "--path", root, "--registry-config", registry, "--allow-missing-kinds="+tc.selection, "--concurrency=2")
			if code != tc.want || !strings.Contains(out, "value: local") {
				t.Fatalf("provider blocked unrelated output: exit=%d %s %s", code, out, diagnostic)
			}
			if code == 0 && !strings.Contains(diagnostic, "SourceCredentialsUnavailable") {
				t.Fatalf("missing source warning: %s", diagnostic)
			}
		})
	}
}
