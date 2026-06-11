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
	var fe *orchestrator.FailuresError
	if !errors.As(err, &fe) {
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
