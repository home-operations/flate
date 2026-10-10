package kustomize

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/tree"
)

func TestRenderFlux_SubPathSymlinkRetargetInvalidates(t *testing.T) {
	root := writeTree(t, map[string]string{
		"a/kustomization.yaml": "resources:\n  - cm.yaml\n",
		"a/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n",
		"b/kustomization.yaml": "resources:\n  - cm.yaml\n",
		"b/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
	})
	link := filepath.Join(root, "apps")
	if err := os.Symlink("a", link); err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()
	cache := NewTreeCache()
	cache.SetRenderCache(cacheDir, 1<<30)
	req := RenderRequest{Label: root, SubPath: "apps", Spec: demoRawSpec}
	first, err := RenderFlux(t.Context(), cache, req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first), "name: a") {
		t.Fatalf("first render: %s", first)
	}
	dr, err := cache.diskRootFor(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := cache.render.get(renderKey(demoRawSpec, dr.root, "apps", "")); !ok {
		t.Fatal("first render did not populate the cache")
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("b", link); err != nil {
		t.Fatal(err)
	}
	cache = NewTreeCache()
	cache.SetRenderCache(cacheDir, 1<<30)
	got, err := RenderFlux(t.Context(), cache, req)
	if err != nil {
		t.Fatal(err)
	}
	want, err := RenderFlux(t.Context(), NewTreeCache(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("retargeted render differs from fresh render:\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestRenderFlux_SymlinkedKustomization(t *testing.T) {
	const original = "resources:\n  - cm.yaml\n"
	files := map[string]string{
		"settings": original,
		"cm.yaml":  "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\n",
	}
	for _, origin := range []string{"disk", "memory"} {
		t.Run(origin, func(t *testing.T) {
			root := writeTree(t, files)
			if err := os.Symlink("settings", filepath.Join(root, "kustomization.yaml")); err != nil {
				t.Fatal(err)
			}
			spec := map[string]any{"spec": map[string]any{"namePrefix": "merged-"}}
			req := RenderRequest{Label: root, SubPath: ".", Spec: spec}
			source := os.DirFS(root)
			if origin == "memory" {
				b := tree.NewBuilder(memoryTree(t, files))
				if err := b.AddSymlink("kustomization.yaml", "settings"); err != nil {
					t.Fatal(err)
				}
				lower, err := b.Build()
				if err != nil {
					t.Fatal(err)
				}
				req.Source = treeRef(lower, "pin")
				req.Label = "/virtual"
				source = lower
			}
			cache := NewTreeCache()
			cache.SetRenderCache(t.TempDir(), 1<<30)
			for range 2 {
				out, err := RenderFlux(t.Context(), cache, req)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(out), "name: merged-demo") {
					t.Fatalf("missing generated prefix: %s", out)
				}
			}
			got, err := fs.ReadFile(source, "settings")
			if err != nil || string(got) != original {
				t.Fatalf("source target changed: %q, %v", got, err)
			}
			target, err := fs.ReadLink(source, "kustomization.yaml")
			if err != nil || target != "settings" {
				t.Fatalf("source link changed: %q, %v", target, err)
			}
		})
	}
}
