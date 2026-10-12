package kustomize

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var demoRawSpec = map[string]any{
	"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
	"kind":       "Kustomization",
	"metadata":   map[string]any{"name": "demo", "namespace": "flux-system"},
	"spec":       map[string]any{"path": "./"},
}

// TestRenderFlux_CacheHitMissInvalidate is the end-to-end soundness gate for the
// cross-run render cache: a populated entry is a real hit (returns the stored
// bytes, krusty skipped — proven by tampering the cached output), and editing a
// recorded input invalidates it so the re-render reflects the change.
func TestRenderFlux_CacheHitMissInvalidate(t *testing.T) {
	root := writeTree(t, map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\n",
	})
	cache := NewTreeCache()
	cache.SetRenderCache(t.TempDir(), 1<<30)
	ctx := context.Background()

	out1, err := RenderFlux(ctx, cache, RenderRequest{Label: root, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	if !strings.Contains(string(out1), "k: v1") {
		t.Fatalf("first render missing data; got %s", out1)
	}

	// Prove the next call is a real HIT (returns stored bytes, skips krusty) by
	// tampering the cached output and confirming it comes back verbatim.
	dr, err := cache.diskRootFor(root)
	if err != nil {
		t.Fatal(err)
	}
	key := renderKey(demoRawSpec, dr.root, ".", "")
	snap, _, ok := cache.render.get(key)
	if !ok {
		t.Fatal("first render did not populate the cache")
	}
	cache.render.put(key, snap, []byte("SENTINEL\n"))
	out2, err := RenderFlux(ctx, cache, RenderRequest{Label: root, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if string(out2) != "SENTINEL\n" {
		t.Errorf("expected a cache hit returning the stored bytes; got a re-render:\n%s", out2)
	}

	// Editing a recorded input must invalidate: the cache misses, krusty re-runs,
	// and the fresh output reflects the change (never the stale sentinel).
	if err := os.WriteFile(filepath.Join(root, "cm.yaml"),
		[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out3, err := RenderFlux(ctx, cache, RenderRequest{Label: root, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("post-edit render: %v", err)
	}
	if string(out3) == "SENTINEL\n" {
		t.Fatal("edit did not invalidate the cache (stale hit — unsound)")
	}
	if !strings.Contains(string(out3), "k: v2") {
		t.Errorf("re-render should reflect the edit; got %s", out3)
	}
}

// TestRenderFlux_SourceignoreContentInvalidates is the regression for the
// adversarially-found stale hit: .sourceignore is loaded off-disk (outside the
// recording overlay), so an in-place edit that changes which resources an
// auto-generated kustomization includes must still invalidate the cache. The
// ignore matcher is memoized per root for the life of a TreeCache (one run), so
// the edit is made between two runs sharing the on-disk render cache.
func TestRenderFlux_SourceignoreContentInvalidates(t *testing.T) {
	root := writeTree(t, map[string]string{
		"a.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n",
		"b.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
		".sourceignore": "# excludes nothing yet\n",
	})
	cacheDir := t.TempDir()
	cache := NewTreeCache()
	cache.SetRenderCache(cacheDir, 1<<30)
	ctx := context.Background()

	out1, err := RenderFlux(ctx, cache, RenderRequest{Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	if !strings.Contains(string(out1), "name: a") || !strings.Contains(string(out1), "name: b") {
		t.Fatalf("first render should include both a and b; got %s", out1)
	}

	// Edit .sourceignore IN PLACE to exclude b.yaml — filenames and every .yaml's
	// content are unchanged, so only the off-overlay .sourceignore bytes differ.
	if err := os.WriteFile(filepath.Join(root, ".sourceignore"), []byte("b.yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cache = NewTreeCache()
	cache.SetRenderCache(cacheDir, 1<<30)
	out2, err := RenderFlux(ctx, cache, RenderRequest{Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if strings.Contains(string(out2), "name: b") {
		t.Error("STALE HIT: a .sourceignore edit excluding b.yaml did not invalidate the cache")
	}
	if !strings.Contains(string(out2), "name: a") {
		t.Errorf("re-render should still include a; got %s", out2)
	}
}

// TestRenderFlux_DisabledCacheUnchanged pins that a TreeCache with no render
// cache renders identically to one with the cache — the cache only memoizes, it
// never changes output.
func TestRenderFlux_DisabledCacheUnchanged(t *testing.T) {
	files := map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\n",
	}
	ctx := context.Background()

	rootA := writeTree(t, files)
	plain := NewTreeCache()
	outPlain, err := RenderFlux(ctx, plain, RenderRequest{Label: rootA, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("plain render: %v", err)
	}

	rootB := writeTree(t, files)
	cached := NewTreeCache()
	cached.SetRenderCache(t.TempDir(), 1<<30)
	outCached, err := RenderFlux(ctx, cached, RenderRequest{Label: rootB, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec})
	if err != nil {
		t.Fatalf("cached render: %v", err)
	}
	if string(outPlain) != string(outCached) {
		t.Errorf("render cache changed output:\nplain:\n%s\ncached:\n%s", outPlain, outCached)
	}
}

// TestEncodeDecodeFrame_RoundTrip pins the kustomize-specific value codec: the
// read-set snapshot + rendered output survive framing intact. The frame is the
// plain (un-gzipped) bytes the shared diskcache.Store compresses; gzip is the
// Store's job, so this test never touches it.
func TestEncodeDecodeFrame_RoundTrip(t *testing.T) {
	snap := readSetSnapshot{
		Files: []fileEntry{{Path: "kustomization.yaml", Hash: "h1"}, {Path: "a.yaml", Hash: "h2"}},
		Dirs:  []fileEntry{{Path: ".", Hash: "dirhash"}},
		Nodes: []nodeEntry{{Path: "comp", Kind: kindAbsent}},
	}
	output := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\n")

	frame, err := encodeFrame(snap, output)
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}
	gotSnap, gotOut, err := decodeFrame(frame)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if !bytes.Equal(gotOut, output) {
		t.Errorf("output mismatch:\nwant %q\ngot  %q", output, gotOut)
	}
	if !reflect.DeepEqual(gotSnap, snap) {
		t.Errorf("snapshot mismatch:\nwant %+v\ngot  %+v", snap, gotSnap)
	}
}

// TestDecodeFrame_RejectsMalformed pins the bounds checks: a frame shorter than
// the 4-byte header, or one whose header claims more snapshot bytes than exist,
// is a decode error (→ a cache miss, never a panic or torn read).
func TestDecodeFrame_RejectsMalformed(t *testing.T) {
	if _, _, err := decodeFrame([]byte{0, 0}); err == nil {
		t.Error("a frame shorter than the 4-byte header must error")
	}
	// Header claims 10 snapshot bytes but only 1 follows.
	if _, _, err := decodeFrame([]byte{0, 0, 0, 10, 'x'}); err == nil {
		t.Error("an out-of-range header length must error")
	}
}

func TestRenderFlux_RemoteNotCached(t *testing.T) {
	var version atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote-v%d}\n", version.Load())
	}))
	t.Cleanup(srv.Close)
	root := writeTree(t, map[string]string{"kustomization.yaml": existingKustomization(srv.URL + "/cm.yaml")})
	cacheDir := t.TempDir()
	for _, v := range []int64{1, 2} {
		version.Store(v)
		cache := NewTreeCache()
		cache.SetRenderCache(cacheDir, 1<<20)
		out, err := RenderFlux(t.Context(), cache, RenderRequest{Label: root, SubPath: ".", Spec: demoRawSpec})
		if err != nil || !strings.Contains(string(out), fmt.Sprintf("name: remote-v%d", v)) {
			t.Fatalf("remote version %d: %s, %v", v, out, err)
		}
	}
}

func TestRenderKey_VersionedSalt(t *testing.T) {
	files := map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")}
	root := writeTree(t, files)
	oldKey := manifest.Fingerprint(struct {
		Spec       map[string]any
		SourceRoot string
		SubPath    string
		Ignore     string
	}{demoRawSpec, root, ".", ""})
	want := manifest.Fingerprint(struct {
		Spec       map[string]any
		SourceRoot string
		SubPath    string
		Ignore     string
		Version    string
	}{demoRawSpec, root, ".", "", "render-fs-v3"})
	if got := renderKey(demoRawSpec, root, ".", ""); got != want {
		t.Errorf("render key salt changed: got %s, want %s", got, want)
	}
	for _, origin := range []string{"disk", "artifact"} {
		t.Run(origin, func(t *testing.T) {
			cacheDir := t.TempDir()
			cache := NewTreeCache()
			cache.SetRenderCache(cacheDir, 1<<20)
			req := RenderRequest{Label: root, SubPath: ".", Spec: demoRawSpec}
			key := oldKey
			if origin == "artifact" {
				req.Source = &store.SourceArtifact{Key: "pin", FS: memoryTree(t, files)}
				key = manifest.Fingerprint([]string{oldKey, "pin", "render-fs-v2"})
			}
			cache.render.put(key, readSetSnapshot{}, []byte("STALE"))
			cache = NewTreeCache()
			cache.SetRenderCache(cacheDir, 1<<20)
			out, err := RenderFlux(t.Context(), cache, req)
			if err != nil || !strings.Contains(string(out), "name: fresh") {
				t.Fatalf("persisted old key reused: %s, %v", out, err)
			}
		})
	}
}
