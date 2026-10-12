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
	"helm.sh/helm/v4/pkg/chart/v2/loader"
	chartutil "helm.sh/helm/v4/pkg/chart/v2/util"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

// TestLoadChart_CoalescesConcurrentFirstLoad verifies that N parallel
// LoadChart calls for the same chart issue exactly one loader.Load.
// Without the per-path keylock, every concurrent first-loader saw an
// empty cache and parsed the tgz independently — wasted CPU.
//
// The post-condition is that every caller's `*chart.Chart` aliases the
// same underlying Templates/Raw/Files (which loader.Load produced
// once), even though each caller receives a fresh outer pointer (the
// per-render clone defended against ProcessDependencies mutation). We
// assert the alias via Templates pointer-identity — Templates is one
// of the immutable slices preserved across cloneChartForRender.
func TestLoadChart_CoalescesConcurrentFirstLoad(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: test
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "k: v\n")
	testutil.WriteFile(t, dir, "mychart/templates/_helpers.tpl", "")

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	st := store.New()
	gr := &manifest.GitRepository{Name: "chart-repo", Namespace: "flux-system"}
	st.AddObject(gr)
	st.SetArtifact(gr.Named(), &store.SourceArtifact{
		Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
		FS: os.DirFS(dir), Key: "immutable-chart-fixture",
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
		wg.Go(func() {
			startGate.Wait()
			res, err := cli.LoadChart(context.Background(), hr)
			if err != nil {
				errs.Add(1)
				t.Errorf("LoadChart: %v", err)
				return
			}
			pointers <- res.Chart
		})
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
			continue
		}
		// Each goroutine receives its own outer *chart.Chart (the
		// per-render clone), but the Templates slice header — one of
		// the immutable, aliased fields — must point at the same
		// backing array as the canonical's. A different backing array
		// would mean loader.Load fired more than once.
		if len(p.Templates) != len(canonical.Templates) {
			t.Errorf("Templates length divergence — first-load coalesce broken (got %d, want %d)",
				len(p.Templates), len(canonical.Templates))
			continue
		}
		if len(p.Templates) > 0 && p.Templates[0] != canonical.Templates[0] {
			t.Errorf("Templates[0] pointer divergence — first-load coalesce broken")
		}
		count++
	}
	if count != goroutines-1 { // canonical is the first one
		t.Errorf("collected %d aliased pointers, want %d", count, goroutines-1)
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
	ch := minimalChart()
	ch.Metadata.Description = "first"
	saved, err := chartutil.Save(ch, dir)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(dir, httpChartFilename)
	if err := os.Rename(saved, archive); err != nil {
		t.Fatal(err)
	}
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	st := store.New()
	ref := &manifest.OCIRepository{Name: "chart-repo", Namespace: "flux-system"}
	st.AddObject(ref)
	st.SetArtifact(ref.Named(), &store.SourceArtifact{Kind: manifest.KindOCIRepository, LocalPath: dir})
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	hr := newHR()
	hr.Chart.RepoKind = manifest.KindOCIRepository
	first, err := cli.LoadChart(t.Context(), hr)
	if err != nil {
		t.Fatal(err)
	}
	if len(cli.chartCache) != 1 {
		t.Fatal("archive chart was not cached")
	}
	ch.Metadata.Description = "second-version-different-size-and-mtime"
	saved, err = chartutil.Save(ch, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(saved, archive); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Hour)
	if err := os.Chtimes(archive, future, future); err != nil {
		t.Fatal(err)
	}
	second, err := cli.LoadChart(t.Context(), hr)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, first.Chart.Metadata.Description, "first")
	assert.Equal(t, second.Chart.Metadata.Description, ch.Metadata.Description)
	loaded, err := loader.Load(archive)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, second.Chart.Metadata.Description, loaded.Metadata.Description)
}

// TestLoadChart_PerCallCloneIsolatesValuesAndDependencies pins that
// each LoadChart call returns a fresh outer *chart.Chart whose
// Values map and Metadata.Dependencies slice are independent of the
// cached canonical AND of every other concurrent caller. Without
// the clone, helm's ProcessDependencies (called by Install.Run)
// mutates Chart.Values and per-Dependency Enabled flags on the
// shared pointer, racing across reconciles.
func TestLoadChart_PerCallCloneIsolatesValuesAndDependencies(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
description: with deps
dependencies:
  - name: dep-a
    version: 0.0.0
    repository: ""
    condition: dep-a.enabled
  - name: dep-b
    version: 0.0.0
    repository: ""
    condition: dep-b.enabled
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "k: shared\n")
	testutil.WriteFile(t, dir, "mychart/templates/_helpers.tpl", "")

	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatal(err)
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

	a, err := cli.LoadChart(context.Background(), hr)
	if err != nil {
		t.Fatal(err)
	}
	b, err := cli.LoadChart(context.Background(), hr)
	if err != nil {
		t.Fatal(err)
	}

	if a.Chart == b.Chart {
		t.Fatal("expected distinct outer *chart.Chart pointers across calls")
	}
	// Mutate a's Values + Dependencies; b must be untouched.
	a.Chart.Values["k"] = "MUTATED-A"
	if len(a.Chart.Metadata.Dependencies) > 0 {
		a.Chart.Metadata.Dependencies[0].Enabled = true
	}

	if got := b.Chart.Values["k"]; got != "shared" {
		t.Errorf("b.Values aliased a's Values map (got %q, want %q)", got, "shared")
	}
	if len(b.Chart.Metadata.Dependencies) > 0 && b.Chart.Metadata.Dependencies[0].Enabled {
		t.Errorf("b.Metadata.Dependencies[0] aliased a's pointer; Enabled leaked")
	}
}
