package kustomize

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// allowlistIgnore is the mono-repo .sourceignore shape from #936: exclude
// everything at the root, re-include the manifest directories.
const allowlistIgnore = "# Exclude all\n/*\n# Include manifest directories\n!/clusters/\n!/charts/x/\n"

func TestIgnoreFS_HidesExcludedPaths(t *testing.T) {
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
			p := abs(tc.rel)
			if got := ig.Exists(p); got != tc.visible {
				t.Errorf("Exists = %v, want %v", got, tc.visible)
			}
			isDir := testTreeFS(t, root).IsDir(p)
			if got := ig.IsDir(p); got != (tc.visible && isDir) {
				t.Errorf("IsDir = %v, want %v", got, tc.visible && isDir)
			}
			_, _, err := ig.CleanedAbs(p)
			if (err == nil) != tc.visible {
				t.Errorf("CleanedAbs err = %v, want visible=%v", err, tc.visible)
			}
			if !isDir {
				_, err := ig.ReadFile(p)
				if (err == nil) != tc.visible {
					t.Errorf("ReadFile err = %v, want visible=%v", err, tc.visible)
				}
				if !tc.visible && !errors.Is(err, os.ErrNotExist) {
					t.Errorf("hidden ReadFile error should be not-exist, got %v", err)
				}
			}
		})
	}

	// The root itself is never hidden, and its listing drops hidden children.
	if !ig.IsDir(root) {
		t.Fatal("root must stay visible")
	}
	entries, err := ig.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir root: %v", err)
	}
	slices.Sort(entries)
	if want := []string{"charts", "clusters"}; !slices.Equal(entries, want) {
		t.Errorf("ReadDir root = %v, want %v", entries, want)
	}

	var walked []string
	if err := ig.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(root, p)
			walked = append(walked, filepath.ToSlash(rel))
		}
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	slices.Sort(walked)
	if want := []string{"charts/x/Chart.yaml", "clusters/dev/cm.yaml"}; !slices.Equal(walked, want) {
		t.Errorf("Walk files = %v, want %v", walked, want)
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
		_, err := Render(ctx, newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: "bundles/common", Spec: parsedKS(t, spec("./bundles/common"))}, sourcecache.RetryConfig{})
		if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "not a directory") {
			t.Fatalf("expected the excluded spec.path to be reported missing, got %v", err)
		}
	})

	t.Run("kustomization base excluded", func(t *testing.T) {
		t.Parallel()
		_, err := Render(ctx, newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: "clusters/dev/app", Spec: parsedKS(t, spec("./clusters/dev/app"))}, sourcecache.RetryConfig{})
		if err == nil || !strings.Contains(err.Error(), "bundles/common") {
			t.Fatalf("expected the build to fail resolving the excluded base, got %v", err)
		}
	})

	t.Run("allowlisted path renders", func(t *testing.T) {
		t.Parallel()
		out, err := Render(ctx, newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: "clusters/dev/ok", Spec: parsedKS(t, spec("./clusters/dev/ok"))}, sourcecache.RetryConfig{})
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if !hasRenderField(out, "name", "ok") {
			t.Fatalf("expected rendered ConfigMap, got:\n%s", out)
		}
	})

	t.Run("fetched artifact not refiltered", func(t *testing.T) {
		t.Parallel()
		out, err := Render(ctx, newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "bundles/common", Spec: parsedKS(t, spec("./bundles/common"))}, sourcecache.RetryConfig{})
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if !hasRenderField(out, "name", "common") {
			t.Fatalf("expected rendered ConfigMap, got:\n%s", out)
		}
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
			root := testutil.WriteTree(t, map[string]string{
				".sourceignore": "b.yaml\n",
				"a.yaml":        cmYAML("a"),
				"b.yaml":        cmYAML("b"),
			})
			if explicit {
				if err := os.WriteFile(filepath.Join(root, ".sourceignore"), []byte("# file policy empty\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			admit := func() *store.SourceArtifact {
				artifact := localSource(t, root)
				if explicit {
					input, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: artifact.FS, Ignore: "b.yaml\n"})
					if err != nil {
						t.Fatal(err)
					}
					artifact.FS = input
				}
				return artifact
			}
			cache := newTestTreeCache(t, t.TempDir())
			ctx := context.Background()

			out1, err := Render(ctx, cache, RenderRequest{Source: admit(), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("first render: %v", err)
			}
			if hasRenderField(out1, "name", "b") {
				t.Fatalf("b.yaml should be excluded; got %s", out1)
			}
			req := RenderRequest{Source: admit(), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec}
			input, err := cache.sourceRootFor(ctx, req.Source, root)
			if err != nil {
				t.Fatal(err)
			}
			key := renderKey(req, root, input.ignore)
			snap, _, ok := readTestRenderFrame(t, cache, key)
			eligible := renderPersistenceEligible(t)
			if ok != eligible {
				t.Fatalf("persisted render = %v, want %v", ok, eligible)
			}
			if eligible {
				publishTestFrame(t, cache.owner, sourcecache.BlobKey{Content: key, PersistOnly: true}, testRenderFrame(t, snap, testRenderPayload(t, "SENTINEL")))
			}
			out2, err := Render(ctx, cache, RenderRequest{Source: admit(), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("second render: %v", err)
			}
			if eligible && !hasRenderField(out2, "witness", "SENTINEL") {
				t.Errorf("expected a cache hit against the filtered view; got a re-render:\n%s", out2)
			}
			if !eligible && !reflect.DeepEqual(out2, out1) {
				t.Errorf("ineligible owner changed filtered output: %s", out2)
			}
		})
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
				input, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: artifact.FS, Ignore: tc.extra})
				if err != nil {
					t.Fatal(err)
				}
				artifact.FS = input
			}
			out, err := Render(t.Context(), newTestTreeCache(t, ""), RenderRequest{Source: artifact, Label: root, ApplyIgnore: true, SubPath: "app", Spec: parsedKS(t, ksDoc(map[string]any{"path": "./app"}))}, sourcecache.RetryConfig{})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "logo.png") {
					t.Fatalf("expected excluded image to fail the build, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if !hasRenderField(out, "kind", "ConfigMap") || !hasRenderField(out, "logo.png", base64.StdEncoding.EncodeToString([]byte(image))) {
				t.Fatalf("expected image bytes in generated ConfigMap, got: %v", out)
			}
		})
	}
}
