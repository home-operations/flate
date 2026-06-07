package kustomize

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

// stripSops is a minimal stand-in for source.ApplyIgnore (which pkg/kustomize
// can't import — cycle): it deletes a root .sops.yaml, the file flux's default
// sourceignore strips that triggers the bo0tzz failure.
func stripSops(root string) error {
	err := os.Remove(filepath.Join(root, ".sops.yaml"))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func TestStage_SourceIgnoreFilter(t *testing.T) {
	src := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(src, ".sops.yaml"), "creation_rules: []\n")
	testutil.WriteFileAt(t, filepath.Join(src, "configmap.yaml"),
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: keep}\n")

	t.Run("applyIgnore=true strips the file", func(t *testing.T) {
		cache := newPreflightCache(t)
		var calls atomic.Int32
		cache.SetSourceIgnoreFilter(func(root string) error { calls.Add(1); return stripSops(root) })

		staged, err := cache.Stage(context.Background(), src, "fp-filtered", WithSourceIgnore())
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		if _, err := os.Stat(filepath.Join(staged, ".sops.yaml")); !os.IsNotExist(err) {
			t.Errorf(".sops.yaml should be filtered from the stage (stat err = %v)", err)
		}
		if _, err := os.Stat(filepath.Join(staged, "configmap.yaml")); err != nil {
			t.Errorf("real resource must survive the filter: %v", err)
		}
		// Warm hit (same fingerprint) must not re-run the filter.
		if _, err := cache.Stage(context.Background(), src, "fp-filtered", WithSourceIgnore()); err != nil {
			t.Fatalf("Stage (warm): %v", err)
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("filter should run once per materialization, ran %d times", n)
		}
	})

	t.Run("applyIgnore=false leaves the file", func(t *testing.T) {
		cache := newPreflightCache(t)
		cache.SetSourceIgnoreFilter(func(root string) error { return stripSops(root) })
		staged, err := cache.Stage(context.Background(), src, "fp-unfiltered")
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		if _, err := os.Stat(filepath.Join(staged, ".sops.yaml")); err != nil {
			t.Errorf("filter must not run when applyIgnore=false: %v", err)
		}
	})

	t.Run("nil filter is a no-op", func(t *testing.T) {
		cache := newPreflightCache(t) // no SetSourceIgnoreFilter
		staged, err := cache.Stage(context.Background(), src, "fp-nofilter", WithSourceIgnore())
		if err != nil {
			t.Fatalf("Stage: %v", err)
		}
		if _, err := os.Stat(filepath.Join(staged, ".sops.yaml")); err != nil {
			t.Errorf("no filter wired ⇒ nothing stripped: %v", err)
		}
	})
}

// TestRenderFlux_SourceIgnoreStripsSopsConfig is the bo0tzz reproduction: a
// working-tree source with path "." and NO kustomization.yaml, plus a root
// .sops.yaml (a SOPS config, not a resource). flux's Generator auto-scans the
// dir; without filtering it tries to decode .sops.yaml and fails with
// "missing Resource metadata". WithSourceIgnore strips it first.
func TestRenderFlux_SourceIgnoreStripsSopsConfig(t *testing.T) {
	newSrc := func(t *testing.T) string {
		src := t.TempDir()
		testutil.WriteFileAt(t, filepath.Join(src, ".sops.yaml"),
			"creation_rules:\n  - path_regex: .*.yaml\n    encrypted_regex: ^(data|stringData)$\n")
		testutil.WriteFileAt(t, filepath.Join(src, "configmap.yaml"),
			"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app\n")
		return src
	}
	spec := map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "flux-system", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	}

	t.Run("with filter: builds, .sops.yaml ignored", func(t *testing.T) {
		cache, err := NewStagingCache(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("NewStagingCache: %v", err)
		}
		t.Cleanup(func() { _ = cache.Close() })
		cache.SetSourceIgnoreFilter(func(root string) error { return stripSops(root) })

		out, err := RenderFlux(context.Background(), cache, newSrc(t), "wt-fp", ".", spec, WithSourceIgnore())
		if err != nil {
			t.Fatalf("RenderFlux should succeed once .sops.yaml is filtered: %v", err)
		}
		if !strings.Contains(string(out), "name: app") {
			t.Errorf("expected the ConfigMap in output:\n%s", out)
		}
	})

	t.Run("without filter: reproduces the decode failure", func(t *testing.T) {
		cache, err := NewStagingCache(t.TempDir(), 0)
		if err != nil {
			t.Fatalf("NewStagingCache: %v", err)
		}
		t.Cleanup(func() { _ = cache.Close() })
		cache.SetSourceIgnoreFilter(func(root string) error { return stripSops(root) })

		// No WithSourceIgnore ⇒ .sops.yaml reaches the Generator.
		_, err = RenderFlux(context.Background(), cache, newSrc(t), "wt-fp-raw", ".", spec)
		if err == nil {
			t.Fatal("expected a kustomize decode failure on the unfiltered .sops.yaml")
		}
		if !strings.Contains(err.Error(), ".sops.yaml") {
			t.Errorf("error should name .sops.yaml; got %q", err)
		}
	})
}
