package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"

	"github.com/home-operations/flate/internal/format"
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
		emptyLister{},
	)
	got := c.scopedNamespaces(f)
	for _, want := range []string{"media", "networking"} {
		if _, ok := got[want]; !ok {
			t.Errorf("auto-scope missing %q: got=%v", want, got)
		}
	}
}

// emptyLister satisfies change.ObjectLister with empty results — used
// for filter resolution tests where transitive deps aren't exercised.
type emptyLister struct{}

func (emptyLister) GetObject(manifest.NamedResource) manifest.BaseManifest { return nil }
func (emptyLister) ListObjects(string) []manifest.BaseManifest             { return nil }

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

	got := scopedRunError(o, res, &commonFlags{namespace: "media"}, aggregateScopedFailures(res.Failed))
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

	if got := scopedRunError(o, res, &commonFlags{namespace: "media"}, aggregateScopedFailures(res.Failed)); got != nil {
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
	runErr := errors.Join(aggregateScopedFailures(res.Failed), panicErr)

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
	runErr := errors.Join(aggregateScopedFailures(res.Failed), context.Canceled)

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
	runErr := errors.Join(aggregateScopedFailures(res.Failed), panicErr)

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

// TestRequireOutput_AcceptsDefaultTableAndAllowed pins the
// short-circuit shape: "table" always passes (subcommand coerces via
// outputOrDefault), and anything in the allowed set passes.
func TestRequireOutput_AcceptsDefaultTableAndAllowed(t *testing.T) {
	for _, out := range []string{"table", "yaml", "json"} {
		c := &commonFlags{output: out}
		if err := c.requireOutput(format.OutputYAML, format.OutputJSON); err != nil {
			t.Errorf("output=%q: unexpected error %v", out, err)
		}
	}
}

// TestRequireOutput_RejectsUnsupported guards the diff/drift fix:
// `get all -o name` and `get images -o diff` used to silently coerce
// into a different format — now they fail loud. Empty allowed-set
// (test's pattern) must reject every non-default `-o`.
func TestRequireOutput_RejectsUnsupported(t *testing.T) {
	cases := []struct {
		output  string
		allowed []format.Output
		wantSub string
	}{
		{output: "name", allowed: []format.Output{format.OutputYAML, format.OutputJSON}, wantSub: `"name"`},
		{output: "yaml", allowed: nil, wantSub: "want one of: table"},
		{output: "json", allowed: []format.Output{format.OutputName}, wantSub: "table, name"},
	}
	for _, tc := range cases {
		c := &commonFlags{output: tc.output}
		err := c.requireOutput(tc.allowed...)
		if err == nil {
			t.Errorf("output=%q allowed=%v: expected error, got nil", tc.output, tc.allowed)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("output=%q: error %q missing substring %q", tc.output, err.Error(), tc.wantSub)
		}
	}
}
