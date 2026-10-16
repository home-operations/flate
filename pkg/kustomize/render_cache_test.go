package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

var demoRawSpec = map[string]any{
	"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
	"kind":       "Kustomization",
	"metadata":   map[string]any{"name": "demo", "namespace": "flux-system"},
	"spec":       map[string]any{"path": "./"},
}

func renderPersistenceEligible(t testing.TB) bool {
	t.Helper()
	info, ok := debug.ReadBuildInfo()
	eligible := false
	if ok {
		const module = "github.com/home-operations/flate"
		eligible = info.Main.Path == module && info.Main.Version != "" && info.Main.Version != "(devel)"
		for _, dep := range info.Deps {
			if dep.Path == module && dep.Version != "" && dep.Version != "(devel)" {
				eligible = true
			}
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				eligible = true
			}
		}
		for _, dep := range info.Deps {
			if dep.Replace != nil && dep.Replace.Version == "" {
				eligible = false
			}
		}
		for _, setting := range info.Settings {
			if setting.Key == "vcs.modified" && setting.Value == "true" {
				eligible = false
			}
		}
	}
	return eligible
}

func readTestRenderFrame(t testing.TB, cache *TreeCache, key string) (readSetSnapshot, []byte, bool) {
	t.Helper()
	r, _, err := cache.owner.Blob(t.Context(), sourcecache.BlobKey{ID: key, PersistOnly: true}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if errors.Is(err, errdef.ErrNotFound) {
		return readSetSnapshot{}, nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err := errors.Join(err, r.Close()); err != nil {
		t.Fatal(err)
	}
	snap, out, err := decodeFrame(raw)
	if err != nil {
		t.Fatal(err)
	}
	return snap, out, true
}

func publishTestFrame(t testing.TB, owner *sourcecache.Cache, key sourcecache.BlobKey, raw []byte) {
	t.Helper()
	r, _, err := owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
		_, err := w.Write(raw)
		return ocispec.Descriptor{}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func testRenderFrame(t testing.TB, snap readSetSnapshot, output []byte) []byte {
	t.Helper()
	frame, err := encodeFrame(snap, output)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// TestRenderFlux_CacheHitMissInvalidate is the end-to-end soundness gate for the
// cross-run render cache: a populated entry is a real hit (returns the stored
// bytes, krusty skipped — proven by tampering the cached output), and editing a
// recorded input invalidates it so the re-render reflects the change.
func TestRenderFlux_CacheHitMissInvalidate(t *testing.T) {
	t.Logf("render persistence eligible=%v", renderPersistenceEligible(t))
	root := writeTree(t, map[string]string{
		"selected/kustomization.yaml": "resources: [../child]\n",
		"child/kustomization.yaml":    "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../cm.yaml\ngenerators: [../plugins]\npatches:\n  - path: ../patch.yaml\n",
		"plugins/kustomization.yaml":  "resources: [g.yaml]\n",
		"plugins/g.yaml":              "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [../value.txt]\n",
		"value.txt":                   "v1",
		"patch.yaml":                  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {patched: v1}\n",
		"cm.yaml":                     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\n",
	})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	ctx := t.Context()

	out1, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "selected", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	if !strings.Contains(string(out1), "k: v1") {
		t.Fatalf("first render missing data; got %s", out1)
	}

	// Prove the next call is a real HIT (returns stored bytes, skips krusty) by
	// tampering the cached output and confirming it comes back verbatim.
	key := renderKey(demoRawSpec, root, "selected", "")
	snap, _, ok := readTestRenderFrame(t, cache, key)
	eligible := renderPersistenceEligible(t)
	if ok != eligible {
		t.Fatalf("persisted render = %v, want %v", ok, eligible)
	}
	if eligible {
		publishTestFrame(t, cache.owner, sourcecache.BlobKey{ID: key, PersistOnly: true}, testRenderFrame(t, snap, []byte("SENTINEL\n")))
	}
	cache.Close()
	if err := cache.owner.Close(); err != nil {
		t.Fatal(err)
	}
	cache = newTestTreeCache(t, cacheDir)
	out2, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "selected", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if eligible && string(out2) != "SENTINEL\n" {
		t.Errorf("expected a cache hit returning stored bytes; got %s", out2)
	}
	if !eligible && !bytes.Equal(out2, out1) {
		t.Errorf("ineligible owner changed output: %s", out2)
	}

	// Editing a recorded input must invalidate: the cache misses, krusty re-runs,
	// and the fresh output reflects the change (never the stale sentinel).
	if err := os.WriteFile(filepath.Join(root, "cm.yaml"),
		[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out3, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "selected", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("post-edit render: %v", err)
	}
	if string(out3) == "SENTINEL\n" {
		t.Fatal("edit did not invalidate the cache (stale hit — unsound)")
	}
	if !strings.Contains(string(out3), "k: v2") {
		t.Errorf("re-render should reflect the edit; got %s", out3)
	}
	testutil.WriteFile(t, root, "patch.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {patched: v2}\n")
	out4, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil || !strings.Contains(string(out4), "patched: v2") {
		t.Fatalf("consumed patch edit replayed stale output: %s, %v", out4, err)
	}
	testutil.WriteFile(t, root, "child/kustomization.yaml", "namePrefix: child-\nresources: [../cm.yaml]\npatches: [{path: ../patch.yaml}]\n")
	out4, err = RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil || !strings.Contains(string(out4), "name: child-demo") {
		t.Fatalf("outside-subpath control edit replayed stale output: %s, %v", out4, err)
	}
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [../cm.yaml]\ngenerators: [../plugins]\n")
	testutil.WriteFile(t, root, "value.txt", "v2")
	out4, err = RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil || !strings.Contains(string(out4), "value.txt: v2") {
		t.Fatalf("local plugin directory lost its source read set: %s, %v", out4, err)
	}
	cache.Close()
	if err := cache.owner.Close(); err != nil {
		t.Fatal(err)
	}
	cache = newTestTreeCache(t, cacheDir)
	_, persisted, hit := readTestRenderFrame(t, cache, key)
	if hit != eligible || hit && !bytes.Equal(persisted, out4) {
		t.Fatalf("replacement after reopen = %s, %v; eligible = %v", persisted, hit, eligible)
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
	cache := newTestTreeCache(t, cacheDir)
	ctx := context.Background()

	out1, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
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

	cache = newTestTreeCache(t, cacheDir)
	out2, err := RenderFlux(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
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

// TestRenderFlux_MemoryOwnerUnchanged pins output parity across backing choices.
func TestRenderFlux_MemoryOwnerUnchanged(t *testing.T) {
	files := map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\n",
	}
	ctx := context.Background()

	rootA := writeTree(t, files)
	plain := newTestTreeCache(t, "")
	outPlain, err := RenderFlux(ctx, plain, RenderRequest{Source: localSource(t, rootA), Label: rootA, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("plain render: %v", err)
	}

	rootB := writeTree(t, files)
	cached := newTestTreeCache(t, t.TempDir())
	outCached, err := RenderFlux(ctx, cached, RenderRequest{Source: localSource(t, rootB), Label: rootB, ApplyIgnore: false, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("cached render: %v", err)
	}
	if string(outPlain) != string(outCached) {
		t.Errorf("render cache changed output:\nplain:\n%s\ncached:\n%s", outPlain, outCached)
	}
}

// TestEncodeDecodeFrame_RoundTrip pins the kustomize-specific value codec: the
// read-set snapshot + rendered output survive framing intact. The frame is the
// plain bytes handed to the shared Blob owner.
func TestEncodeDecodeFrame_RoundTrip(t *testing.T) {
	snap := readSetSnapshot{
		Files: []fileEntry{{Path: "kustomization.yaml", Hash: "h1"}, {Path: "a.yaml", Hash: "h2"}},
		Dirs:  []fileEntry{{Path: ".", Hash: "dirhash"}},
		Nodes: []nodeEntry{{Path: "comp", Kind: kindAbsent}},
	}
	for _, output := range []string{"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\n", ""} {
		t.Run(fmt.Sprintf("output-bytes-%d", len(output)), func(t *testing.T) {
			frame, err := encodeFrame(snap, []byte(output))
			if err != nil {
				t.Fatalf("encodeFrame: %v", err)
			}
			gotSnap, gotOut, err := decodeFrame(frame)
			if err != nil {
				t.Fatalf("decodeFrame: %v", err)
			}
			if string(gotOut) != output {
				t.Errorf("output mismatch: want %q, got %q", output, gotOut)
			}
			if !reflect.DeepEqual(gotSnap, snap) {
				t.Errorf("snapshot mismatch: want %+v, got %+v", snap, gotSnap)
			}
		})
	}
}

// TestDecodeFrame_RejectsMalformed pins the bounds checks: a frame shorter than
// the 4-byte header, or one whose header claims more snapshot bytes than exist,
// is a decode error (→ a cache miss, never a panic or torn read).
func TestDecodeFrame_RejectsMalformed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"short header", []byte{0, 0}},
		{"truncated snapshot", []byte{0, 0, 0, 10, 'x'}},
		{"invalid snapshot JSON", []byte{0, 0, 0, 1, 'x'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := decodeFrame(tc.frame); err == nil {
				t.Fatal("malformed frame decoded successfully")
			}
		})
	}
}

func TestRenderFlux_RemoteNotCached(t *testing.T) {
	t.Logf("render persistence eligible=%v", renderPersistenceEligible(t))
	for _, origin := range []string{"http", "patch", "generator", "plugin", "plugin-directory", "git", "nested-mount"} {
		t.Run(origin, func(t *testing.T) {
			var version atomic.Int64
			remoteBody := func() string {
				name := fmt.Sprintf("remote-v%d", version.Load())
				switch origin {
				case "patch":
					return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: local}\ndata: {marker: " + name + "}\n"
				case "generator", "plugin-directory":
					return name
				case "plugin":
					return "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: " + name + "}\nliterals: [marker=" + name + "]\n"
				default:
					return cmYAML(name)
				}
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, remoteBody())
			}))
			t.Cleanup(srv.Close)
			const gitURL = "https://example.invalid/o/r.git"
			ref := srv.URL + "/remote.yaml"
			remoteFiles := map[string]string{}
			if origin == "git" || origin == "nested-mount" {
				ref = gitURL + "//remote?ref=main"
				remoteFiles["remote/kustomization.yaml"] = "resources: [cm.yaml]\n"
			}
			kustomization := "resources: [local.yaml, " + fmt.Sprintf("%q", ref) + "]\n"
			if origin == "nested-mount" {
				remoteFiles["outer/nested/kustomization.yaml"] = kustomization
				remoteFiles["outer/nested/local.yaml"] = cmYAML("local")
				remoteFiles["outer/kustomization.yaml"] = "resources: [nested]\n"
				kustomization = fmt.Sprintf("resources: [%q]\n", gitURL+"//outer?ref=main")
			}
			files := map[string]string{"local.yaml": cmYAML("local")}
			switch origin {
			case "patch":
				kustomization = "resources: [local.yaml]\npatches: [{path: " + ref + "}]\n"
			case "generator":
				kustomization = "configMapGenerator: [{name: generated, files: [" + ref + "]}]\n"
			case "plugin":
				kustomization = "generators: [" + ref + "]\n"
			case "plugin-directory":
				kustomization = "generators: [plugins]\n"
				files["plugins/kustomization.yaml"] = "resources: [g.yaml]\n"
				files["plugins/g.yaml"] = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [" + ref + "]\n"
			}
			files["kustomization.yaml"] = kustomization
			root := writeTree(t, files)
			cacheDir := t.TempDir()
			for _, v := range []int64{1, 2} {
				version.Store(v)
				remoteFiles["remote/cm.yaml"] = cmYAML(fmt.Sprintf("remote-v%d", v))
				cache := newTestTreeCache(t, cacheDir)
				remote := memoryTree(t, remoteFiles)
				cache.SetGitBaseFetcher(func(_ context.Context, req GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					if req.URL != gitURL {
						t.Errorf("unexpected git URL %q", req.URL)
					}
					return &store.SourceArtifact{FS: remote, Key: fmt.Sprintf("pin-%d", v)}, nil
				})
				out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
				if err != nil || !strings.Contains(string(out), fmt.Sprintf("remote-v%d", v)) {
					t.Fatalf("remote version %d: %s, %v", v, out, err)
				}
				if _, _, hit := readTestRenderFrame(t, cache, renderKey(demoRawSpec, root, ".", "")); hit {
					t.Fatal("remote render persisted without a local read-set identity")
				}
			}
		})
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
		Version    string
	}{demoRawSpec, root, ".", "", "render-fs-v4"})
	if renderKey(demoRawSpec, root, ".", "") == oldKey {
		t.Fatal("preceding render-key domain still active")
	}
	for _, origin := range []string{"disk", "artifact"} {
		t.Run(origin, func(t *testing.T) {
			cacheDir := t.TempDir()
			cache := newTestTreeCache(t, cacheDir)
			req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoRawSpec}
			key := oldKey
			if origin == "artifact" {
				req.Source = &store.SourceArtifact{Key: "pin", FS: memoryTree(t, files)}
				key = manifest.Fingerprint([]string{oldKey, "pin"})
			}
			publishTestFrame(t, cache.owner, sourcecache.BlobKey{ID: key, PersistOnly: true}, testRenderFrame(t, readSetSnapshot{}, []byte("STALE")))
			cache = newTestTreeCache(t, cacheDir)
			out, err := RenderFlux(t.Context(), cache, req, sourcecache.RetryConfig{})
			if err != nil || !strings.Contains(string(out), "name: fresh") {
				t.Fatalf("persisted old key reused: %s, %v", out, err)
			}
		})
	}
}

func TestRenderFlux_CacheIdentityIsolated(t *testing.T) {
	root := writeTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("fresh"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoRawSpec}
	key := renderKey(demoRawSpec, root, ".", "")
	publishTestFrame(t, cache.owner, sourcecache.BlobKey{ID: key}, testRenderFrame(t, readSetSnapshot{}, []byte("ORDINARY\n")))
	out, err := RenderFlux(t.Context(), cache, req, sourcecache.RetryConfig{})
	if err != nil || !strings.Contains(string(out), "name: fresh") {
		t.Fatalf("ordinary raw identity reused cached output: %s, %v", out, err)
	}
	_, stored, hit := readTestRenderFrame(t, cache, key)
	if eligible := renderPersistenceEligible(t); hit != eligible || hit && !bytes.Equal(stored, out) {
		t.Fatalf("completed render = %s, %v; eligible = %v", stored, hit, eligible)
	}
}

func TestRenderFlux_CacheFrames(t *testing.T) {
	eligible := renderPersistenceEligible(t)
	for _, tc := range []struct {
		name      string
		snapshot  readSetSnapshot
		malformed bool
		empty     bool
		immutable bool
	}{
		{name: "malformed", malformed: true},
		{name: "mismatched", snapshot: readSetSnapshot{Files: []fileEntry{{Path: "cm.yaml", Hash: "mismatch"}}}},
		{name: "empty", empty: true},
		{name: "immutable", immutable: true, snapshot: readSetSnapshot{Files: []fileEntry{{Path: "unavailable", Hash: "mismatch"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml"),
				"cm.yaml":            cmYAML("fresh"),
			})
			cacheDir := t.TempDir()
			cache := newTestTreeCache(t, cacheDir)
			source := localSource(t, root)
			key := renderKey(demoRawSpec, root, ".", "")
			if tc.immutable {
				source.Key = "immutable-pin"
				key = manifest.Fingerprint([]string{key, source.Key})
			}
			frame := testRenderFrame(t, tc.snapshot, []byte("STALE\n"))
			if tc.malformed {
				frame = []byte{0, 0, 0, 1, 'x'}
			} else if tc.empty {
				frame = testRenderFrame(t, tc.snapshot, nil)
			}
			publishTestFrame(t, cache.owner, sourcecache.BlobKey{ID: key, PersistOnly: true}, frame)
			out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: source, Label: root, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if eligible && tc.immutable {
				if string(out) != "STALE\n" {
					t.Fatalf("immutable pin replayed mutable validation: %s", out)
				}
			} else if eligible && tc.empty {
				if len(out) != 0 {
					t.Fatalf("complete empty frame missed: %s", out)
				}
			} else if !strings.Contains(string(out), "name: fresh") {
				t.Fatalf("candidate prevented a fresh render: %s", out)
			}
			cache.Close()
			if err := cache.owner.Close(); err != nil {
				t.Fatal(err)
			}
			cache = newTestTreeCache(t, cacheDir)
			_, stored, hit := readTestRenderFrame(t, cache, key)
			if hit != eligible || hit && !bytes.Equal(stored, out) {
				t.Fatalf("completed frame = %s, %v; eligible = %v", stored, hit, eligible)
			}
		})
	}
}

type pausedRenderFS struct {
	fs.FS
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *pausedRenderFS) ReadFile(name string) ([]byte, error) {
	if name == "cm.yaml" {
		p.once.Do(func() { close(p.entered); <-p.release })
	}
	return fs.ReadFile(p.FS, name)
}

func TestRenderFlux_CacheAcquisitionOverlap(t *testing.T) {
	for _, validate := range []bool{false, true} {
		t.Run(fmt.Sprintf("validation_%t", validate), func(t *testing.T) {
			eligible := renderPersistenceEligible(t)
			root := writeTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")})
			cache := newTestTreeCache(t, t.TempDir())
			key := sourcecache.BlobKey{ID: renderKey(demoRawSpec, root, ".", ""), PersistOnly: true}
			frame := testRenderFrame(t, readSetSnapshot{Files: []fileEntry{{Path: "cm.yaml", Hash: manifest.SHA256Hex([]byte(cmYAML("fresh")))}}}, []byte("VALIDATED\n"))
			if validate {
				publishTestFrame(t, cache.owner, key, frame)
			}
			lower := &pausedRenderFS{FS: os.DirFS(root), entered: make(chan struct{}), release: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			released := false
			defer func() {
				if !released {
					close(lower.release)
				}
				wg.Wait()
			}()
			rendered := make(chan error, 1)
			wg.Go(func() {
				out, err := RenderFlux(ctx, cache, RenderRequest{Source: &store.SourceArtifact{FS: lower}, Label: root, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
				if err == nil && validate && eligible && string(out) != "VALIDATED\n" {
					err = fmt.Errorf("validation did not return its acquired frame: %q", out)
				}
				if err == nil && (!validate || !eligible) && !strings.Contains(string(out), "name: fresh") {
					err = fmt.Errorf("real render = %q", out)
				}
				rendered <- err
			})
			select {
			case <-lower.entered:
			case <-ctx.Done():
				t.Fatal("render did not reach input-read barrier")
			}
			progress := make(chan error, 1)
			wg.Go(func() {
				progress <- func() error {
					r, _, err := cache.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
					if r != nil {
						err = errors.Join(err, r.Close())
					}
					if validate && eligible && err != nil {
						return err
					}
					if (!validate || !eligible) && !errors.Is(err, errdef.ErrNotFound) {
						return fmt.Errorf("lookup before completed publication: %w", err)
					}
					r, _, err = cache.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
						_, err := w.Write(frame)
						return ocispec.Descriptor{}, err
					})
					if err != nil {
						return err
					}
					if err := r.Close(); err != nil {
						return err
					}
					r, _, err = cache.owner.Blob(ctx, sourcecache.BlobKey{ID: "ordinary-source"}, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w io.Writer) (ocispec.Descriptor, error) {
						_, err := io.WriteString(w, "source")
						return ocispec.Descriptor{}, err
					})
					if err != nil {
						return err
					}
					return r.Close()
				}()
			})
			select {
			case err := <-progress:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("same-frame lookup/publication or source acquisition blocked behind input read")
			}
			// Release the filesystem only after all acquisitions completed.
			close(lower.release)
			released = true
			select {
			case err := <-rendered:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("render did not finish")
			}
		})
	}
}

func TestRenderFlux_CacheStorageFailures(t *testing.T) {
	if !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	for _, fault := range []string{"lookup", "publication"} {
		t.Run(fault, func(t *testing.T) {
			root := writeTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")})
			cacheDir := t.TempDir()
			cache := newTestTreeCache(t, cacheDir)
			req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoRawSpec}
			if fault == "lookup" {
				if _, err := RenderFlux(t.Context(), cache, req, sourcecache.RetryConfig{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.RemoveAll(cacheDir); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				out, err := RenderFlux(t.Context(), cache, req, sourcecache.RetryConfig{})
				if len(out) != 0 || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("%s fault returned output %q, error %v", fault, out, err)
				}
			}
		})
	}
}
