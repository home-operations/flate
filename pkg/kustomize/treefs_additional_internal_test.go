package kustomize

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func recordTestRender(t *testing.T, cache *TreeCache, req RenderRequest, receipt *manifest.RenderReceipt) {
	t.Helper()
	assert.FatalIf(t, receipt == nil, "%v", "first render returned no receipt")
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: req.Spec.Named(), Receipt: receipt}))
	_, stored, hit := readTestRenderFrame(t, cache, receipt.Value().(renderFrame).header.Key)
	assert.FatalIf(t, !hit, "%v", "accepted render was not persisted")
	replayed, replayReceipt, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.FatalIf(t, replayReceipt == nil, "%v", "unchanged input returned no receipt")
	frame := replayReceipt.Value().(renderFrame)
	assert.FatalIf(t, frame.raw == "", "%v", "unchanged input did not hit the accepted render cache")
	assert.Diff(t, replayed, stored)
}

func TestRender_CacheMissesOnDirectoryMembership(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{"app/a.yaml": cmYAML("a")})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: "app", Spec: demoSpec}
	first, receipt, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.FatalIf(t, err != nil || !hasRenderField(first, "name", "a"), "first = %q, %v", first, err)
	recordTestRender(t, cache, req, receipt)
	testutil.WriteFile(t, root, "app/b.yaml", cmYAML("b"))
	second, _, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.FatalIf(
		t,
		err != nil || !hasRenderField(second, "name", "b"),
		"a file added to an auto-generated directory replayed a stale render: %q, %v",
		second,
		err,
	)
}

func TestTreeFS_LocalConstraints(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	testutil.WriteFile(t, root, "folder/value", "inside")
	testutil.WriteFile(t, parent, "outside", "outside")
	testutil.WriteFile(t, parent, "root-sibling/value", "sibling")
	for name, target := range map[string]string{
		"file-link": "folder/value", "directory-link": "folder", "dangling": "missing", "escaping-link": "../outside",
	} {
		assert.NoError(t, os.Symlink(target, filepath.Join(root, name)))
	}
	artifact := localSource(t, root)

	for _, tc := range []struct {
		name, path string
		constraint bool
	}{
		{name: "ordinary read", path: filepath.Join(root, "folder/value")},
		{name: "parent escape", path: root + "/../outside", constraint: true},
		{name: "outside absolute", path: filepath.Join(parent, "outside"), constraint: true},
		{name: "sibling prefix", path: filepath.Join(parent, "root-sibling/value"), constraint: true},
		{name: "file link", path: filepath.Join(root, "file-link")},
		{name: "directory link", path: filepath.Join(root, "directory-link/value")},
		{name: "dangling", path: filepath.Join(root, "dangling")},
		{name: "escaping link", path: filepath.Join(root, "escaping-link")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lower, _, err := source.OperationView(t.Context(), artifact.FS)
			assert.NoError(t, err)
			adapter := newTreeFS(lower, root, nil)
			data, err := adapter.ReadFile(tc.path)
			if tc.constraint {
				{
					_, ok := errors.AsType[*fluxfilesys.ConstraintError](err)
					assert.FatalIf(t, !ok, "lexical escape error = %v; want ConstraintError", err)
				}
			} else if tc.name == "ordinary read" {
				assert.FatalIf(t, err != nil || string(data) != "inside", "ordinary read = %q, %v", data, err)
			} else if !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("denied link read = %q, %v", data, err)
			}
		})
	}
}

func TestRender_CacheInvalidatesTypeTransitions(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{"app/a.yaml": cmYAML("a")})
	artifact := localSource(t, root)
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: artifact, SubPath: "app", Spec: demoSpec}
	_, receipt, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	recordTestRender(t, cache, req, receipt)
	assert.NoError(t, os.RemoveAll(filepath.Join(root, "app")))
	testutil.WriteFile(t, root, "app", "file")
	{
		_, _, err := testutil.RenderMaps(
			Render(t.Context(), cache, req, sourcecache.RetryConfig{}),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "directory-to-file transition replayed a successful frame: %v", err)
	}
	assert.NoError(t, os.Remove(filepath.Join(root, "app")))
	assert.NoError(t, os.Symlink(".", filepath.Join(root, "app")))
	{
		_, _, err := testutil.RenderMaps(
			Render(t.Context(), cache, req, sourcecache.RetryConfig{}),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "directory link replayed a successful frame: %v", err)
	}
}

func TestRender_CacheMissingComponentAppearance(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": "resources: [cm.yaml]\n",
		"cm.yaml":            cmYAML("base"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), SubPath: ".", Spec: parsedKS(t, ksDoc(map[string]any{
		"components": []any{"./missing"}, "ignoreMissingComponents": true,
	}))}
	_, receipt, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	recordTestRender(t, cache, req, receipt)
	testutil.WriteFile(
		t,
		root,
		"missing/kustomization.yaml",
		"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [cm.yaml]\n",
	)
	testutil.WriteFile(t, root, "missing/cm.yaml", cmYAML("arrived"))
	out, _, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
	assert.FatalIf(
		t,
		err != nil || !hasRenderField(out, "name", "arrived"),
		"missing component arrival replayed stale output: %s, %v",
		out,
		err,
	)
}

func TestRender_ArtifactWithoutPayloadRejected(t *testing.T) {
	t.Parallel()
	label := testutil.WriteTree(t, map[string]string{"cm.yaml": cmYAML("host")})
	for _, key := range []string{"", "pin"} {
		t.Run("key="+key, func(t *testing.T) {
			t.Parallel()
			source := &store.SourceArtifact{LocalPath: label, Key: key}
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					newTestTreeCache(t, ""),
					RenderRequest{Source: source, SubPath: ".", Spec: demoSpec},
					sourcecache.RetryConfig{},
				),
			)
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || hasRenderField(out, "name", "host"),
				"render = %q, %v",
				out,
				err,
			)
		})
	}
}
