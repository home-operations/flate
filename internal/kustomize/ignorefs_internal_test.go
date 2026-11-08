package kustomize

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/go-git/go-git/v5/plumbing"

	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// allowlistIgnore is the mono-repo .sourceignore shape from #936: exclude
// everything at the root, re-include the manifest directories.
const allowlistIgnore = "# Exclude all\n/*\n# Include manifest directories\n!/clusters/\n!/charts/x/\n"

func TestIgnoreFS_HidesExcludedPaths(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		".sourceignore":         allowlistIgnore,
		"clusters/dev/cm.yaml":  cmYAML("dev"),
		"bundles/common/a.yaml": cmYAML("a"),
		"charts/x/Chart.yaml":   "name: x\n",
		"charts/y/Chart.yaml":   "name: y\n",
		"README.md":             "hi\n",
	})
	ig := testIgnoreTreeFS(t, root)
	abs := func(rel string) string { return filepath.Join(root, rel) }

	cases := []struct {
		rel     string
		visible bool
	}{
		{"clusters", true},
		{"clusters/dev", true},
		{"clusters/dev/cm.yaml", true},
		{"bundles", false},
		{"bundles/common", false},
		{"bundles/common/a.yaml", false},
		{"README.md", false},
		// charts/ matches /* but a deeper `!` keeps charts/x/**, so the parent
		// still exists in the artifact while its excluded sibling does not.
		{"charts", true},
		{"charts/x", true},
		{"charts/x/Chart.yaml", true},
		{"charts/y", false},
		{"charts/y/Chart.yaml", false},
	}
	for _, tc := range cases {
		t.Run(tc.rel, func(t *testing.T) {
			t.Parallel()
			testIgnoreFSHidesExcludedPathsCase(
				t,
				&testIgnoreFSHidesExcludedPathsCaseInputs{root: &root, ig: &ig, abs: &abs, tc: &tc},
			)
		},
		)
	}

	// The root itself is never hidden, and its listing drops hidden children.
	assert.FatalIf(t, !ig.IsDir(root), "%s", "root must stay visible")
	entries, err := ig.ReadDir(root)
	assert.FatalIf(t, err != nil, "ReadDir root: %v", err)
	slices.Sort(entries)
	{
		want := []string{"charts", "clusters"}
		assert.ErrorIf(t, !slices.Equal(entries, want), "ReadDir root = %v, want %v", entries, want)
	}

	var walked []string
	{
		err := ig.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				rel, _ := filepath.Rel(root, p)
				walked = append(walked, filepath.ToSlash(rel))
			}
			return nil
		})
		assert.FatalIf(t, err != nil, "walk: %v", err)
	}
	slices.Sort(walked)
	{
		want := []string{"charts/x/Chart.yaml", "clusters/dev/cm.yaml"}
		assert.ErrorIf(t, !slices.Equal(walked, want), "Walk files = %v, want %v", walked, want)
	}
}

// TestRender_SourceignoreAllowlist is the regression for #936: a root
// .sourceignore allowlist that leaves a directory out of the artifact must make
// a working-tree render fail the way Flux does, whether the directory is the
// Kustomization's spec.path or a base an in-tree kustomization.yaml pulls in.
// A fetched artifact (applyIgnore=false) is left alone: its fetcher already
// filtered it.
func TestRender_SourceignoreAllowlist(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		".sourceignore":                       allowlistIgnore,
		"bundles/common/kustomization.yaml":   existingKustomization("cm.yaml"),
		"bundles/common/cm.yaml":              cmYAML("common"),
		"clusters/dev/app/kustomization.yaml": existingKustomization("../../../bundles/common"),
		"clusters/dev/ok/cm.yaml":             cmYAML("ok"),
	})
	ctx := context.Background()
	spec := func(path string) map[string]any {
		return ksDoc(map[string]any{"path": path})
	}

	t.Run("spec.path excluded", func(t *testing.T) {
		t.Parallel()
		_, _, err := testutil.RenderMaps(
			Render(
				ctx,
				newTestTreeCache(t, ""),
				RenderRequest{
					Source:      localSource(t, root),
					Label:       root,
					ApplyIgnore: true,
					SubPath:     "bundles/common",
					Spec:        parsedKS(t, spec("./bundles/common")),
				},
				sourcecache.RetryConfig{},
			),
		)
		assert.FatalIf(
			t,
			!errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "not a directory"),
			"expected the excluded spec.path to be reported missing, got %v",
			err,
		)
	})

	t.Run("kustomization base excluded", func(t *testing.T) {
		t.Parallel()
		_, _, err := testutil.RenderMaps(
			Render(
				ctx,
				newTestTreeCache(t, ""),
				RenderRequest{
					Source:      localSource(t, root),
					Label:       root,
					ApplyIgnore: true,
					SubPath:     "clusters/dev/app",
					Spec:        parsedKS(t, spec("./clusters/dev/app")),
				},
				sourcecache.RetryConfig{},
			),
		)
		assert.FatalIf(
			t,
			err == nil || !strings.Contains(err.Error(), "bundles/common"),
			"expected the build to fail resolving the excluded base, got %v",
			err,
		)
	})

	t.Run("allowlisted path renders", func(t *testing.T) {
		t.Parallel()
		out, _, err := testutil.RenderMaps(
			Render(
				ctx,
				newTestTreeCache(t, ""),
				RenderRequest{
					Source:      localSource(t, root),
					Label:       root,
					ApplyIgnore: true,
					SubPath:     "clusters/dev/ok",
					Spec:        parsedKS(t, spec("./clusters/dev/ok")),
				},
				sourcecache.RetryConfig{},
			),
		)
		assert.FatalIf(t, err != nil, "render: %v", err)
		assert.FatalIf(t, !hasRenderField(out, "name", "ok"), "expected rendered ConfigMap, got:\n%s", out)
	})

	t.Run("fetched artifact not refiltered", func(t *testing.T) {
		t.Parallel()
		out, _, err := testutil.RenderMaps(
			Render(
				ctx,
				newTestTreeCache(t, ""),
				RenderRequest{
					Source:      localSource(t, root),
					Label:       root,
					ApplyIgnore: false,
					SubPath:     "bundles/common",
					Spec:        parsedKS(t, spec("./bundles/common")),
				},
				sourcecache.RetryConfig{},
			),
		)
		assert.FatalIf(t, err != nil, "render: %v", err)
		assert.FatalIf(t, !hasRenderField(out, "name", "common"), "expected rendered ConfigMap, got:\n%s", out)
	})
}

// TestRender_SourceignoreCacheReplaysFilteredView pins that a cached render
// of a working-tree source validates against the filtered disk view it was
// recorded through: a probe of a hidden path is recorded as absent and must
// still read as absent on replay, or every such render would miss forever.
func TestRender_SourceignoreCacheReplaysFilteredView(t *testing.T) {
	t.Parallel()
	for _, explicit := range []bool{false, true} {
		name := "file patterns"
		if explicit {
			name = "pre-shaped explicit patterns"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testRenderSourceignoreCacheReplaysFilteredViewCase(
				t,
				&explicit,
			)
		},
		)
	}
}

func TestRender_SourceignoreConfigMapGenerator(t *testing.T) {
	t.Parallel()
	const image = "\x89PNG\r\n\x1a\n"
	cases := []struct {
		name    string
		ignore  *string
		extra   string
		shaped  bool
		wantErr bool
	}{
		{name: "user patterns preserve image", ignore: new("*.tmp\n")},
		{name: "no sourceignore excludes image", wantErr: true},
		{name: "empty sourceignore excludes image", ignore: new(""), wantErr: true},
		{name: "pre-shaped explicit patterns preserve image", extra: "*.tmp\n", shaped: true},
		{name: "pre-shaped defaults exclude image", shaped: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := map[string]string{
				"app/icons/logo.png": image,
				"app/kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
configMapGenerator:
- name: icons
  files:
  - logo.png=icons/logo.png
`,
			}
			if tc.ignore != nil {
				files[".sourceignore"] = *tc.ignore
			}
			root := testutil.WriteTree(t, files)
			artifact := localSource(t, root)
			if tc.shaped {
				input, _, err := gittree.New(
					t.Context(),
					nil,
					plumbing.ZeroHash,
					gittree.ViewOptions{Mode: gittree.Source, Current: artifact.FS, Ignore: tc.extra},
				)
				assert.NoError(t, err)
				artifact.FS = input
			}
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					newTestTreeCache(t, ""),
					RenderRequest{
						Source:      artifact,
						Label:       root,
						ApplyIgnore: true,
						SubPath:     "app",
						Spec:        parsedKS(t, ksDoc(map[string]any{"path": "./app"})),
					},
					sourcecache.RetryConfig{},
				),
			)
			if tc.wantErr {
				assert.FatalIf(
					t,
					err == nil || !strings.Contains(err.Error(), "logo.png"),
					"expected excluded image to fail the build, got %v",
					err,
				)
				return
			}
			assert.FatalIf(t, err != nil, "render: %v", err)
			assert.FatalIf(t, !hasRenderField(out, "kind", "ConfigMap") ||
				!hasRenderField(
					out,
					"logo.png",
					base64.StdEncoding.EncodeToString([]byte(image)),
				), "expected image bytes in generated ConfigMap, got: %v", out)
		})
	}
}

type testIgnoreFSHidesExcludedPathsCaseInputs struct {
	root *string
	ig   *filesys.FileSystem
	abs  *func(rel string) string
	tc   *struct {
		rel     string
		visible bool
	}
}

func testIgnoreFSHidesExcludedPathsCase(t *testing.T, testCaseInput *testIgnoreFSHidesExcludedPathsCaseInputs) {
	t.Helper()
	p := (*testCaseInput.abs)(testCaseInput.tc.rel)
	{
		got := (*testCaseInput.ig).Exists(p)
		assert.ErrorIf(t, got != testCaseInput.tc.visible, "Exists = %v, want %v", got, testCaseInput.tc.visible)
	}
	isDir := testTreeFS(t, (*testCaseInput.root)).IsDir(p)
	{
		got := (*testCaseInput.ig).IsDir(p)
		assert.ErrorIf(
			t,
			got != (testCaseInput.tc.visible && isDir),
			"IsDir = %v, want %v",
			got,
			testCaseInput.tc.visible && isDir,
		)
	}
	_, _, err := (*testCaseInput.ig).CleanedAbs(p)
	assert.ErrorIf(
		t,
		(err == nil) != testCaseInput.tc.visible,
		"CleanedAbs err = %v, want visible=%v",
		err,
		testCaseInput.tc.visible,
	)
	if !isDir {
		_, err := (*testCaseInput.ig).ReadFile(p)
		assert.ErrorIf(
			t,
			(err == nil) != testCaseInput.tc.visible,
			"ReadFile err = %v, want visible=%v",
			err,
			testCaseInput.tc.visible,
		)
		assert.ErrorIf(
			t,
			!testCaseInput.tc.visible && !errors.Is(err, os.ErrNotExist),
			"hidden ReadFile error should be not-exist, got %v",
			err,
		)
	}
}

func testRenderSourceignoreCacheReplaysFilteredViewCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	root := testutil.WriteTree(t, map[string]string{
		".sourceignore": "b.yaml\n",
		"a.yaml":        cmYAML("a"),
		"b.yaml":        cmYAML("b"),
	})
	if *testCaseInput {
		{
			err := os.WriteFile(
				filepath.Join(root, ".sourceignore"),
				[]byte("# file policy empty\n"),
				0o600,
			)
			assert.NoError(t, err)
		}
	}
	admit := func() *store.SourceArtifact {
		artifact := localSource(t, root)
		if *testCaseInput {
			input, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Source, Current: artifact.FS, Ignore: "b.yaml\n"},
			)
			assert.NoError(t, err)
			artifact.FS = input
		}
		return artifact
	}
	cache := newTestTreeCache(t, t.TempDir())
	ctx := context.Background()

	out1, receipt, err := testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{Source: admit(), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	assert.FatalIf(t, err != nil, "first render: %v", err)
	{
		recordRenderErr := cache.RecordRender(ctx, RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt})
		assert.NoError(t, recordRenderErr)
	}
	assert.FatalIf(t, hasRenderField(out1, "name", "b"), "b.yaml should be excluded; got %s", out1)
	req := RenderRequest{Source: admit(), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec}
	input, err := cache.sourceRootFor(ctx, req.Source, root)
	assert.NoError(t, err)
	key := renderKey(&req, root, input.ignore)
	snap, _, ok := readTestRenderFrame(t, cache, key)
	assert.FatalIf(t, !ok, "%s", "owner did not retain filtered render")
	publishTestFrame(
		t,
		cache.owner,
		sourcecache.BlobKey{Content: key, Render: sourcecache.RenderPending},
		testRenderFrame(t, new(snap), testRenderPayload(t, "SENTINEL")),
	)
	out2, _, err := testutil.RenderMaps(
		Render(
			ctx,
			cache,
			RenderRequest{Source: admit(), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec},
			sourcecache.RetryConfig{},
		),
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!hasRenderField(out2, "witness", "SENTINEL"),
		"filtered view missed its validated frame: %s",
		out2,
	)
}
