package helm

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestLoadChart_CoalescesConcurrentFirstLoad verifies that N parallel
// LoadChart calls for the same chart issue exactly one loader.Load.
// Without the per-path keylock, every concurrent first-loader saw an
// empty cache and parsed the tgz independently — wasted CPU and (more
// importantly) divergent *chart.Chart pointers between callers.
//
// We can't directly intercept loader.Load, but we can verify the
// post-condition: every caller ends up with the SAME *chart.Chart
// pointer (cache wins) rather than independently-parsed copies.
func TestLoadChart_CoalescesConcurrentFirstLoad(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: test
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "k: v\n")
	testutil.WriteFile(t, dir, "mychart/templates/_helpers.tpl", "")

	cli, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	gr := &manifest.GitRepository{Name: "chart-repo", Namespace: "flux-system"}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	cli.SetSourceResolver(NewStoreSourceResolver(st))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}

	const goroutines = 16
	var (
		wg        sync.WaitGroup
		pointers  = make(chan *chart.Chart, goroutines)
		startGate sync.WaitGroup
		errs      atomic.Int32
	)
	startGate.Add(1)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			startGate.Wait()
			res, err := cli.LoadChart(context.Background(), hr)
			if err != nil {
				errs.Add(1)
				t.Errorf("LoadChart: %v", err)
				return
			}
			pointers <- res.Chart
		}()
	}
	startGate.Done()
	wg.Wait()
	close(pointers)

	if errs.Load() > 0 {
		t.Fatalf("%d goroutines errored", errs.Load())
	}

	var canonical *chart.Chart
	count := 0
	for p := range pointers {
		if canonical == nil {
			canonical = p
		} else if p != canonical {
			t.Errorf("got divergent *chart.Chart pointers across goroutines — first-load coalesce broken")
		}
		count++
	}
	if count != goroutines {
		t.Errorf("collected %d pointers, want %d", count, goroutines)
	}
}

// TestLoadChart_InvalidatesOnFileMtimeChange pins the chart-cache
// mtime+size invalidation: when a chart file is overwritten under
// the same path (mutable OCI tag re-pushed via writeAtomic), the
// next LoadChart must re-parse rather than serve the stale
// in-memory *chart.Chart. Without the fix the cache key is just
// the path, and an overwrite is invisible.
func TestLoadChart_InvalidatesOnFileMtimeChange(t *testing.T) {
	dir := t.TempDir()
	cli, err := NewClient(t.TempDir(), t.TempDir())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	gr := &manifest.GitRepository{Name: "chart-repo", Namespace: "flux-system"}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
	})
	cli.SetSourceResolver(NewStoreSourceResolver(st))

	hr := &manifest.HelmRelease{
		Name: "demo", Namespace: "default",
		Chart: manifest.HelmChart{
			Name:          "mychart",
			RepoName:      "chart-repo",
			RepoNamespace: "flux-system",
			RepoKind:      manifest.KindGitRepository,
		},
	}

	// First version of the chart.
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: first
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "k: v1\n")

	first, err := cli.LoadChart(context.Background(), hr)
	if err != nil {
		t.Fatalf("first LoadChart: %v", err)
	}

	// Overwrite Chart.yaml — flate's chartCacheFingerprint stats
	// Chart.yaml for directory charts (directory mtime is unreliable),
	// so this is the file the cache key actually tracks. New content
	// changes both size and mtime; the future-mtime stamp guards
	// against coarse-granularity filesystems where same-second
	// rewrites can collide.
	mustWriteFile := func(rel, body string) {
		t.Helper()
		path := filepath.Join(dir, rel)
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		future := time.Now().Add(2 * time.Hour)
		if err := os.Chtimes(path, future, future); err != nil {
			t.Fatal(err)
		}
	}
	mustWriteFile("mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: second-version-different-size-and-mtime
`)

	second, err := cli.LoadChart(context.Background(), hr)
	if err != nil {
		t.Fatalf("second LoadChart: %v", err)
	}
	if second.Chart == first.Chart {
		t.Error("cache served stale chart after on-disk overwrite; mtime/size invalidation failed")
	}
}
