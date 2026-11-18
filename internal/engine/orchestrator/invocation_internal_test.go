package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/engine/source"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestInvocation_LazySelectionLifetime(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"completed", "resource_failure", "preparation_failure", "discovery_failure", "canceled"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			testInvocationLazySelectionLifetime(t, stage)
		})
	}
}

func testInvocationLazySelectionLifetime(t *testing.T, stage string) {
	t.Helper()
	root, cacheDir := t.TempDir(), t.TempDir()
	invocation, err := NewInvocation(t.Context(), Config{Concurrency: 2, CacheDir: cacheDir})
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, invocation.Close()) })
	files := map[string]string{
		"flux.yaml": testutil.KustomizationYAML("owner", "apps",
			"  path: ./app\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"),
		"app/kustomization.yaml": "resources: [cm.yaml]\n",
		"app/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result}\n",
		"lazy.txt":               strings.Repeat("retained source\n", 4096),
	}
	if stage == "resource_failure" {
		delete(files, "app/cm.yaml")
	}
	var archive bytes.Buffer
	testutil.WriteTar(t, &archive, files, 0o644)
	input, _, identity, err := invocation.Config.Cache.Acquire(t.Context(),
		sourcecache.ArchiveKey{Content: stage}, &sourcecache.Policy{},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, writeErr := w.Write(archive.Bytes())
			return sourcecache.ProducerResult{}, writeErr
		})
	assert.NoError(t, err)
	artifact := &store.SourceArtifact{FS: input, LocalPath: root, LocalRoot: root, Content: identity}
	tree := Tree{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: artifact, Path: root}
	if stage == "preparation_failure" {
		tree.Input = nil
		tree.Path = ""
	}
	if stage == "discovery_failure" {
		tree.Path = filepath.Join(root, "absent")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if stage == "canceled" {
		cancel()
	}
	err = invocation.Render(ctx, []Tree{tree})
	if stage == "completed" {
		assert.NoError(t, err)
	} else {
		assert.FatalIf(t, err == nil, "missing %s error", stage)
	}
	if stage == "canceled" {
		assert.Equal(t, errors.Is(err, context.Canceled), true)
	}
	assert.Equal(t, errors.Is(invocation.err, err), true)
	data, readErr := fs.ReadFile(input, "lazy.txt")
	assert.NoError(t, readErr)
	assert.Equal(t, string(data), files["lazy.txt"])
	if stage == "completed" || stage == "resource_failure" {
		assert.Equal(t, len(invocation.Results), 1)
		retained, retainedErr := fs.ReadFile(invocation.results[0].cfg.Input.FS, "lazy.txt")
		assert.NoError(t, retainedErr)
		assert.Equal(t, string(retained), files["lazy.txt"])
		projected, projection := Select(invocation.results, err,
			[]manifest.NamedResource{testutil.KustomizationID("apps", "owner")}, nil)
		assert.Equal(t, len(projected), 1)
		assert.Equal(t, len(projected[0].Objects) > 0, true)
		if stage == "completed" {
			assert.NoError(t, projection.Selected)
			assert.Equal(t, len(projected[0].Manifests) > 0, true)
		}
	}
	invocation.Results = nil
	assert.NoError(t, invocation.Close())
	assert.NoError(t, invocation.Close())
	_, readErr = fs.ReadFile(input, "lazy.txt")
	assert.Equal(t, errors.Is(readErr, fs.ErrClosed), true)
	res, sweepErr := sourcecache.Sweep(t.Context(), cacheDir, sourcecache.SweepOpts{})
	assert.NoError(t, sweepErr)
	assert.Equal(t, len(res.Errors), 0)
}

func TestInvocation_ValidationAndPartialAllocation(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"identity", "supply", "cache"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			trees := []Tree{{ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}}}
			cfg := Config{Concurrency: 2}
			switch name {
			case "identity":
				trees = append(trees, trees[0])
			case "supply":
				cfg.Supply = []store.Declaration{{Doc: map[string]any{"kind": "ConfigMap"}}}
			case "cache":
				cfg.CacheDir = filepath.Join(t.TempDir(), "file")
				testutil.WriteFileAt(t, cfg.CacheDir, "not a directory")
			}
			invocation, err := RenderTrees(t.Context(), trees, cfg)
			assert.FatalIf(t, err == nil, "missing %s error", name)
			if name == "cache" {
				assert.FatalIf(t, invocation == nil || invocation.Config.Network == nil,
					"allocated network lost on cache initialization failure: %v", err)
				assert.NoError(t, invocation.Close())
				assert.NoError(t, invocation.Close())
			} else {
				assert.Equal(t, invocation == nil, true)
			}
		})
	}
}

func TestInvocation_InjectedResources(t *testing.T) {
	t.Parallel()
	for _, ownership := range []string{"borrowed", "transferred"} {
		t.Run(ownership, func(t *testing.T) {
			t.Parallel()
			cacheDir := t.TempDir()
			cache, err := sourcecache.New(t.Context(), cacheDir)
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, cache.Close()) })
			network := new(source.Network)
			t.Cleanup(network.Close)
			invocation, err := RenderTrees(t.Context(), []Tree{{
				ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Path: t.TempDir(),
			}}, Config{Concurrency: 2, Cache: cache, Network: network})
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, invocation.Close()) })
			assert.Equal(t, invocation.releaseCache == nil, true)
			assert.Equal(t, invocation.releaseNetwork == nil, true)
			if ownership == "transferred" {
				invocation.releaseCache, invocation.releaseNetwork = cache.Close, network.Close
			}
			assert.NoError(t, invocation.Close())
			res, err := sourcecache.Sweep(t.Context(), cacheDir, sourcecache.SweepOpts{})
			if ownership == "borrowed" {
				assert.FatalIf(t, err == nil && len(res.Errors) == 0, "borrower released caller cache")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(res.Errors), 0)
			}
			assert.NoError(t, cache.Close())
			res, err = sourcecache.Sweep(t.Context(), cacheDir, sourcecache.SweepOpts{})
			assert.NoError(t, err)
			assert.Equal(t, len(res.Errors), 0)
		})
	}
}
