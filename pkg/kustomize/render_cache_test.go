package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxapis "github.com/fluxcd/pkg/apis/kustomize"
	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	"github.com/opencontainers/go-digest"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

var demoSpec = &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}

func renderPersistenceEligible(tb testing.TB) bool {
	tb.Helper()
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

func readTestRenderFrame(tb testing.TB, cache *TreeCache, key string) (readSetSnapshot, []map[string]any, bool) {
	tb.Helper()
	r, _, _, err := cache.owner.Blob(tb.Context(), sourcecache.BlobKey{Content: key, PersistOnly: true}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if errors.Is(err, errdef.ErrNotFound) {
		return readSetSnapshot{}, nil, false
	}
	if err != nil {
		tb.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err := errors.Join(err, r.Close()); err != nil {
		tb.Fatal(err)
	}
	snap, out, err := decodeFrame(raw)
	if err != nil {
		tb.Fatal(err)
	}
	docs, err := manifest.DecodeRender(out)
	if err != nil {
		tb.Fatal(err)
	}
	return snap, docs, true
}

func publishTestFrame(tb testing.TB, owner *sourcecache.Cache, key sourcecache.BlobKey, raw []byte) {
	tb.Helper()
	r, _, _, err := owner.Blob(tb.Context(), key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		_, err := w.Write(raw)
		return sourcecache.ProducerResult{}, err
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := r.Close(); err != nil {
		tb.Fatal(err)
	}
}

func testRenderFrame(tb testing.TB, snap readSetSnapshot, output []byte) []byte {
	tb.Helper()
	frame, err := encodeFrame(snap, output)
	if err != nil {
		tb.Fatal(err)
	}
	return frame
}

func TestRender_CacheHitMissInvalidate(t *testing.T) {
	t.Parallel()
	t.Run("empty stream", func(t *testing.T) {
		t.Parallel()
		root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": "resources: []\n"})
		storage := t.TempDir()
		req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
		for range 2 {
			c := newTestTreeCache(t, storage)
			docs, err := Render(t.Context(), c, req, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Equal(t, len(docs), 0)
			_, stored, hit := readTestRenderFrame(t, c, renderKey(req, root, nil))
			assert.Equal(t, hit, renderPersistenceEligible(t))
			assert.Equal(t, len(stored), 0)
		}
	})
	t.Logf("render persistence eligible=%v", renderPersistenceEligible(t))
	root := testutil.WriteTree(t, map[string]string{
		"selected/kustomization.yaml": "resources: [../child]\n",
		"child/kustomization.yaml":    "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - ../cm.yaml\ngenerators: [../plugins]\npatches:\n  - path: ../patch.yaml\n",
		"plugins/kustomization.yaml":  "resources: [g.yaml]\n",
		"plugins/g.yaml":              "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [../value.txt]\n",
		"value.txt":                   "v1",
		"patch.yaml":                  "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {patched: v1}\n",
		"cm.yaml":                     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\nextra: {sequence: [{value: original}]}\n",
	})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	ctx := t.Context()

	out1, err := Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	if !hasRenderField(out1, "k", "v1") {
		t.Fatalf("first render missing data; got %s", out1)
	}

	want := make([]map[string]any, len(out1))
	for i, doc := range out1 {
		want[i] = manifest.DeepCopyMap(doc)
	}
	for _, doc := range out1 {
		if name, _ := manifest.DocMetadata(doc); name == "demo" {
			doc["extra"].(map[string]any)["sequence"].([]any)[0].(map[string]any)["value"] = "mutated"
		}
	}
	out1[0]["metadata"].(map[string]any)["name"] = "mutated"
	out1, err = Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.Diff(t, out1, want)

	key := renderKey(RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "selected"}, root, nil)
	snap, _, ok := readTestRenderFrame(t, cache, key)
	eligible := renderPersistenceEligible(t)
	if ok != eligible {
		t.Fatalf("persisted render = %v, want %v", ok, eligible)
	}
	if eligible {
		publishTestFrame(t, cache.owner, sourcecache.BlobKey{Content: key, PersistOnly: true}, testRenderFrame(t, snap, testRenderPayload(t, "SENTINEL")))
	}
	cache.Close()
	if err := cache.owner.Close(); err != nil {
		t.Fatal(err)
	}
	cache = newTestTreeCache(t, cacheDir)
	out2, err := Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if eligible && !hasRenderField(out2, "witness", "SENTINEL") {
		t.Errorf("expected a cache hit returning stored bytes; got %s", out2)
	}
	if !eligible && !reflect.DeepEqual(out2, out1) {
		t.Errorf("ineligible owner changed output: %s", out2)
	}

	// Editing a recorded input must invalidate: the cache misses, krusty re-runs,
	// and the fresh output reflects the change (never the stale sentinel).
	if err := os.WriteFile(filepath.Join(root, "cm.yaml"),
		[]byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v2\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out3, err := Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: false, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("post-edit render: %v", err)
	}
	if hasRenderField(out3, "witness", "SENTINEL") {
		t.Fatal("edit did not invalidate the cache (stale hit — unsound)")
	}
	if !hasRenderField(out3, "k", "v2") {
		t.Errorf("re-render should reflect the edit; got %s", out3)
	}
	testutil.WriteFile(t, root, "patch.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {patched: v2}\n")
	out4, err := Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out4, "patched", "v2") {
		t.Fatalf("consumed patch edit replayed stale output: %s, %v", out4, err)
	}
	testutil.WriteFile(t, root, "child/kustomization.yaml", "namePrefix: child-\nresources: [../cm.yaml]\npatches: [{path: ../patch.yaml}]\n")
	out4, err = Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out4, "name", "child-demo") {
		t.Fatalf("outside-subpath control edit replayed stale output: %s, %v", out4, err)
	}
	testutil.WriteFile(t, root, "child/kustomization.yaml", "resources: [../cm.yaml]\ngenerators: [../plugins]\n")
	testutil.WriteFile(t, root, "value.txt", "v2")
	out4, err = Render(ctx, cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out4, "value.txt", "v2") {
		t.Fatalf("local plugin directory lost its source read set: %s, %v", out4, err)
	}
	cache.Close()
	if err := cache.owner.Close(); err != nil {
		t.Fatal(err)
	}
	cache = newTestTreeCache(t, cacheDir)
	_, persisted, hit := readTestRenderFrame(t, cache, key)
	if hit != eligible || hit && !reflect.DeepEqual(persisted, out4) {
		t.Fatalf("replacement after reopen = %s, %v; eligible = %v", persisted, hit, eligible)
	}
	spec := demoSpec.Clone()
	for _, tc := range []struct {
		name, want string
		edit       func()
	}{
		{name: "prefix", want: "name: typed-demo", edit: func() { spec.NamePrefix = "typed-" }},
		{name: "namespace", want: "namespace: typed", edit: func() { spec.TargetNamespace = "typed" }},
		{name: "patch", want: "from-spec: typed", edit: func() {
			spec.Patches = []fluxapis.Patch{{Patch: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {from-spec: typed}\n"}}
		}},
	} {
		tc.edit()
		req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: "selected", Spec: spec}
		out, err := Render(ctx, cache, req, sourcecache.RetryConfig{})
		if err != nil || !hasRenderField(out, strings.SplitN(tc.want, ": ", 2)[0], strings.SplitN(tc.want, ": ", 2)[1]) || reflect.DeepEqual(out, out4) {
			t.Fatalf("typed edit returned stale output: %s, %v", out, err)
		}
		repeated, err := Render(ctx, cache, req, sourcecache.RetryConfig{})
		if err != nil || !reflect.DeepEqual(out, repeated) {
			t.Fatalf("typed render is not deterministic: %s, %v", repeated, err)
		}
		out4 = out
	}

}

// TestRender_SourceignoreContentInvalidates verifies policy content and path
// changes invalidate persisted renders after re-admission.
func TestRender_SourceignoreContentInvalidates(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"a.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n",
		"b.yaml":        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
		".sourceignore": "# excludes nothing yet\n",
	})
	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	ctx := t.Context()
	req := RenderRequest{Source: localSource(t, root), Label: root, ApplyIgnore: true, SubPath: ".", Spec: demoSpec}
	input, err := cache.sourceRootFor(ctx, req.Source, root)
	if err != nil {
		t.Fatal(err)
	}
	key := renderKey(req, root, input.ignore)

	out1, err := Render(ctx, cache, req, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("first render: %v", err)
	}
	if !hasRenderField(out1, "name", "a") || !hasRenderField(out1, "name", "b") {
		t.Fatalf("first render should include both a and b; got %s", out1)
	}

	if err := os.WriteFile(filepath.Join(root, ".sourceignore"), []byte("b.yaml\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cache = newTestTreeCache(t, cacheDir)
	req.Source = localSource(t, root)
	input, err = cache.sourceRootFor(ctx, req.Source, root)
	if err != nil {
		t.Fatal(err)
	}
	changedKey := renderKey(req, root, input.ignore)
	if changedKey == key {
		t.Fatal("re-admitted ignore bytes reused the render key")
	}
	out2, err := Render(ctx, cache, req, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("second render: %v", err)
	}
	if hasRenderField(out2, "name", "b") {
		t.Error("STALE HIT: a .sourceignore edit excluding b.yaml did not invalidate the cache")
	}
	if !hasRenderField(out2, "name", "a") {
		t.Errorf("re-render should still include a; got %s", out2)
	}
	if err := os.Remove(filepath.Join(root, ".sourceignore")); err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, root, "app/.sourceignore", "b.yaml\n")
	cache = newTestTreeCache(t, cacheDir)
	req.Source = localSource(t, root)
	input, err = cache.sourceRootFor(ctx, req.Source, root)
	if err != nil {
		t.Fatal(err)
	}
	if renderKey(req, root, input.ignore) == changedKey {
		t.Fatal("moved ignore file reused the render key")
	}
	out3, err := Render(ctx, cache, req, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out3, "name", "a") || !hasRenderField(out3, "name", "b") {
		t.Fatalf("nested policy reused root policy membership: %s, %v", out3, err)
	}
}

// TestRender_MemoryOwnerUnchanged pins output parity across backing choices.
func TestRender_MemoryOwnerUnchanged(t *testing.T) {
	t.Parallel()
	files := map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - cm.yaml\n",
		"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\ndata:\n  k: v1\n",
	}
	ctx := context.Background()

	rootA := testutil.WriteTree(t, files)
	plain := newTestTreeCache(t, "")
	outPlain, err := Render(ctx, plain, RenderRequest{Source: localSource(t, rootA), Label: rootA, ApplyIgnore: false, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("plain render: %v", err)
	}

	rootB := testutil.WriteTree(t, files)
	cached := newTestTreeCache(t, t.TempDir())
	outCached, err := Render(ctx, cached, RenderRequest{Source: localSource(t, rootB), Label: rootB, ApplyIgnore: false, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("cached render: %v", err)
	}
	if !reflect.DeepEqual(outPlain, outCached) {
		t.Errorf("render cache changed output:\nplain:\n%s\ncached:\n%s", outPlain, outCached)
	}
}

// TestEncodeDecodeFrame_RoundTrip pins the kustomize-specific value codec: the
// read-set snapshot + rendered output survive framing intact. The frame is the
// plain bytes handed to the shared Blob owner.
func TestEncodeDecodeFrame_RoundTrip(t *testing.T) {
	t.Parallel()
	snap := readSetSnapshot{
		Files: []fileEntry{{Path: "kustomization.yaml", Hash: "h1"}, {Path: "a.yaml", Hash: "h2"}},
		Dirs:  []fileEntry{{Path: ".", Hash: "dirhash"}},
		Nodes: []nodeEntry{{Path: "comp", Kind: kindAbsent}},
	}
	for _, output := range []string{"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo\n", ""} {
		t.Run(fmt.Sprintf("output-bytes-%d", len(output)), func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"short header", []byte{0, 0}},
		{"truncated snapshot", []byte{0, 0, 0, 10, 'x'}},
		{"invalid snapshot JSON", []byte{0, 0, 0, 1, 'x'}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := decodeFrame(tc.frame); err == nil {
				t.Fatal("malformed frame decoded successfully")
			}
		})
	}
}

func TestRender_RemoteCacheDependencies(t *testing.T) {
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted sentinel assertions executed=%v", eligible)
	for _, immutable := range []bool{false, true} {
		t.Run(fmt.Sprintf("immutable_%t", immutable), func(t *testing.T) {
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
					var trace remoteHTTPTrace
					srv := trace.serve(t, remoteBody)
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
						kustomization = "configMapGenerator: [{name: generated, files: [chosen=" + ref + "]}]\n"
					case "plugin":
						kustomization = "generators: [" + ref + "]\n"
					case "plugin-directory":
						kustomization = "generators: [plugins]\n"
						files["plugins/kustomization.yaml"] = "resources: [nested]\n"
						files["plugins/nested/kustomization.yaml"] = "resources: [../g.yaml]\n"
						files["plugins/g.yaml"] = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [" + ref + "]\n"
					}
					files["kustomization.yaml"] = kustomization
					root := testutil.WriteTree(t, files)
					cacheDir := t.TempDir()

					newCache := func(storage string) *TreeCache {
						c := newTestTreeCache(t, storage)
						v := version.Load()
						remoteFiles["remote/cm.yaml"] = cmYAML(fmt.Sprintf("remote-v%d", v))
						remote := memoryTree(t, remoteFiles)
						c.SetGitBaseFetcher(func(_ context.Context, req GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
							if req.URL != gitURL {
								t.Errorf("unexpected git URL %q", req.URL)
							}
							return &store.SourceArtifact{FS: remote, Key: "family", Revision: fmt.Sprint(v)}, nil
						})

						return c
					}
					request := func() RenderRequest {
						source := localSource(t, root)
						if immutable {
							source.Key, source.Revision = "local-family", "local-pin"
						}
						return RenderRequest{Source: source, Label: root, SubPath: "."}
					}
					version.Store(1)
					initial := len(trace.snapshot())
					cache := newCache(cacheDir)
					out1, err := Render(t.Context(), cache, request(), sourcecache.RetryConfig{})
					if err != nil || !hasRenderField(out1, "", "remote-v1") {
						t.Fatalf("initial render: %s, %v", out1, err)
					}
					inputRec := newReadSet(root)
					inputRec.immutable = immutable
					view := newTreeFS(request().Source.FS, root, inputRec)
					if err := preflightRemoteResources(t.Context(), cache, view, root); err != nil {
						t.Fatal(err)
					}
					recorded := inputRec.snapshot()
					if len(recorded.Remotes) == 0 || immutable && (len(recorded.Files) != 0 || len(recorded.Dirs) != 0 || len(recorded.Nodes) != 0) {
						t.Fatalf("incomplete remote recorder: %+v", recorded)
					}
					if valid, err := recorded.stillValid(t.Context(), cache, view, immutable); err != nil || !valid {
						t.Fatalf("unchanged remote identity: %t, %v", valid, err)
					}
					if origin == "generator" && len(trace.snapshot())-initial != 1 {
						t.Fatalf("bootstrap, render and validation did not share acquisition: %+v", trace.snapshot()[initial:])
					}
					key := renderKey(request(), root, nil)
					snap, stored, hit := readTestRenderFrame(t, cache, key)
					if hit != eligible || hit && (!reflect.DeepEqual(stored, out1) || len(snap.Remotes) == 0 || immutable && (len(snap.Files) != 0 || len(snap.Dirs) != 0 || len(snap.Nodes) != 0)) {
						t.Fatalf("published=%t eligible=%t snapshot=%+v output=%s", hit, eligible, snap, stored)
					}
					fresh := newCache("")
					want, err := Render(t.Context(), fresh, request(), sourcecache.RetryConfig{})
					if err != nil || !reflect.DeepEqual(want, out1) || hit && !reflect.DeepEqual(want, stored) {
						t.Fatalf("fresh/cached bytes differ: %s, %s, %v", want, stored, err)
					}
					closeTestTreeCache(t, fresh)
					if eligible {
						publishTestFrame(t, cache.owner, sourcecache.BlobKey{Content: key, PersistOnly: true}, testRenderFrame(t, snap, testRenderPayload(t, "SENTINEL")))
					}
					closeTestTreeCache(t, cache)
					before := len(trace.snapshot())
					cache = newCache(cacheDir)
					if valid, err := recorded.stillValid(t.Context(), cache, &treeFS{lower: request().Source.FS}, immutable); err != nil || !valid {
						t.Fatalf("reopened unchanged remote identity: %t, %v", valid, err)
					}
					out2, err := Render(t.Context(), cache, request(), sourcecache.RetryConfig{})
					if err != nil || eligible && !hasRenderField(out2, "witness", "SENTINEL") || !eligible && !reflect.DeepEqual(out2, out1) {
						t.Fatalf("unchanged fresh-owner render: %s, %v", out2, err)
					}
					if origin != "git" && origin != "nested-mount" {
						got := trace.snapshot()[before:]
						if origin == "generator" {
							t.Logf("bootstrap-primed persistence eligible=%t sentinel_reused=%t requests=%d", eligible, hasRenderField(out2, "witness", "SENTINEL"), len(got))
						}
						t.Logf("unchanged render: requests=%+v accepted_connections=%d", got, trace.connections.Load())
						if len(got) != 1 || got[0].Method != http.MethodGet || got[0].ETag == "" || got[0].Modified == "" || got[0].Status != http.StatusNotModified || got[0].Bytes != 0 {
							t.Errorf("unchanged render must make one conditional GET with zero body bytes: %+v", got)
						}
					}
					closeTestTreeCache(t, cache)
					version.Store(2)
					before = len(trace.snapshot())
					cache = newCache(cacheDir)
					if valid, err := recorded.stillValid(t.Context(), cache, &treeFS{lower: request().Source.FS}, immutable); err != nil || valid {
						t.Fatalf("changed remote identity: %t, %v", valid, err)
					}
					out3, err := Render(t.Context(), cache, request(), sourcecache.RetryConfig{})
					if err != nil || !hasRenderField(out3, "", "remote-v2") || reflect.DeepEqual(out3, out1) {
						t.Fatalf("changed remote render: %s, %v", out3, err)
					}
					if origin != "git" && origin != "nested-mount" {
						got := trace.snapshot()[before:]
						t.Logf("changed render and preflight: requests=%+v accepted_connections=%d", got, trace.connections.Load())
						if len(got) != 1 || got[0].Status != http.StatusOK || got[0].Bytes == 0 {
							t.Errorf("changed render must acquire one response including subsequent preflight: %+v", got)
						}
					}
					closeTestTreeCache(t, cache)
				})
			}
		})
	}
}

func closeTestTreeCache(t *testing.T, c *TreeCache) {
	t.Helper()
	c.Close()
	c.remoteInputs.Close()
	if err := c.owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRenderKey_ContentInputs(t *testing.T) {
	t.Parallel()
	req := RenderRequest{Source: &store.SourceArtifact{Key: "family"}, SubPath: ".", Spec: demoSpec}
	key := renderKey(req, "/source", nil)
	for _, tc := range []struct {
		name   string
		change func(*RenderRequest)
		root   string
		ignore any
		spec   map[string]any
		typed  map[string]any
		same   bool
	}{
		{name: "substitution", change: func(r *RenderRequest) {
			r.Spec.PostBuild = &kustomizev1.PostBuild{Substitute: map[string]string{"VALUE": "typed"}}
		}},
		{name: "spec", change: func(r *RenderRequest) { r.Spec.NamePrefix = "other-" }},
		{name: "owner name", change: func(r *RenderRequest) { r.Spec.Name = "other" }},
		{name: "owner namespace", change: func(r *RenderRequest) { r.Spec.Namespace = "other" }},
		{name: "explicit empty namespace", same: true, spec: map[string]any{"targetNamespace": ""}},
		{name: "explicit empty prefix", same: true, spec: map[string]any{"namePrefix": ""}},
		{name: "explicit empty suffix", same: true, spec: map[string]any{"nameSuffix": ""}},
		{name: "null fields", same: true, spec: map[string]any{"components": nil}},
		{name: "null whole fields", same: true, spec: map[string]any{"patches": nil, "images": nil, "components": nil, "buildMetadata": nil, "commonMetadata": nil, "targetNamespace": nil, "namePrefix": nil, "nameSuffix": nil}},
		{name: "empty lists", same: true, spec: map[string]any{"patches": []any{}, "images": []any{}, "components": []any{}, "buildMetadata": []any{}}},
		{name: "null image elements", spec: map[string]any{"images": []any{nil}}, typed: map[string]any{"images": []any{map[string]any{"name": ""}}}},
		{name: "null component elements", spec: map[string]any{"components": []any{nil}}, typed: map[string]any{"components": []any{""}}},
		{name: "null build metadata elements", spec: map[string]any{"buildMetadata": []any{nil}}, typed: map[string]any{"buildMetadata": []any{""}}},
		{name: "null patch elements", spec: map[string]any{"patches": []any{nil}}, typed: map[string]any{"patches": []any{map[string]any{"patch": ""}}}},
		{name: "null common labels", spec: map[string]any{"commonMetadata": map[string]any{"labels": map[string]any{"app": nil}}}, typed: map[string]any{"commonMetadata": map[string]any{"labels": map[string]any{"app": ""}}}},
		{name: "null common annotations", spec: map[string]any{"commonMetadata": map[string]any{"annotations": map[string]any{"note": nil}}}, typed: map[string]any{"commonMetadata": map[string]any{"annotations": map[string]any{"note": ""}}}},
		{name: "strategic merge patches", same: true, spec: map[string]any{"patchesStrategicMerge": []any{map[string]any{"metadata": map[string]any{"name": "app"}}}}},
		{name: "JSON 6902 patches", same: true, spec: map[string]any{"patchesJson6902": []any{map[string]any{"patch": []any{map[string]any{"op": "remove", "path": "/data/old"}}}}}},
		{name: "root", root: "/other"},
		{name: "subpath", change: func(r *RenderRequest) { r.SubPath = "app" }},
		{name: "ignore selection", change: func(r *RenderRequest) { r.ApplyIgnore = true }},
		{name: "ignore bytes", ignore: []byte("*.yaml")},
		{name: "acquisition", change: func(r *RenderRequest) { r.Source.Key = "other-family" }},
		{name: "revision", change: func(r *RenderRequest) { r.Source.Revision = "revision" }},
		{name: "digest", change: func(r *RenderRequest) { r.Source.Digest = "digest" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := req
			changed.Spec = req.Spec.Clone()
			source := *req.Source
			changed.Source = &source
			if tc.spec != nil {
				doc := renderDoc(t, changed.Spec)
				maps.Copy(doc["spec"].(map[string]any), tc.spec)
				changed.Spec = parsedKS(t, doc)
			}
			if tc.change != nil {
				tc.change(&changed)
			}
			root := "/source"
			if tc.root != "" {
				root = tc.root
			}
			got := renderKey(changed, root, tc.ignore)
			if got == "" || (got == key) != tc.same {
				t.Fatalf("render key equality = %t, want %t", got == key, tc.same)
			}
			if tc.typed != nil {
				typed := req
				doc := renderDoc(t, req.Spec)
				maps.Copy(doc["spec"].(map[string]any), tc.typed)
				typed.Spec = parsedKS(t, doc)
				if got != renderKey(typed, root, tc.ignore) {
					t.Fatal("null input differs from its typed zero value")
				}
			}
		})
	}
	for _, field := range []string{"targetNamespace", "namePrefix", "nameSuffix"} {
		t.Run("typed override/"+field, func(t *testing.T) {
			t.Parallel()
			changed := req
			changed.Spec = req.Spec.Clone()
			doc := renderDoc(t, changed.Spec)
			doc["spec"].(map[string]any)[field] = nil
			changed.Spec = parsedKS(t, doc)
			plain := req
			plain.Spec = req.Spec.Clone()
			switch field {
			case "targetNamespace":
				changed.Spec.TargetNamespace, plain.Spec.TargetNamespace = "apps", "apps"
			case "namePrefix":
				changed.Spec.NamePrefix, plain.Spec.NamePrefix = "pre-", "pre-"
			case "nameSuffix":
				changed.Spec.NameSuffix, plain.Spec.NameSuffix = "-post", "-post"
			}
			if renderKey(changed, "/source", nil) != renderKey(plain, "/source", nil) {
				t.Fatal("authored null changed typed content")
			}
		})
	}

	stamped := req
	stamped.Spec = req.Spec.Clone()
	stamped.Spec.Labels = map[string]string{"owner": "parent"}
	stamped.Spec.Annotations = map[string]string{"stamp": "changed"}
	if got := renderKey(stamped, "/source", nil); got != key {
		t.Fatal("emission metadata changed the render key")
	}
	if got := renderKey(req, "/source", nil); got != key {
		t.Fatal("unchanged render input changed the key")
	}
}

func TestRender_CacheIdentityIsolated(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("fresh"),
	})
	cache := newTestTreeCache(t, t.TempDir())
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	key := renderKey(RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "."}, root, nil)
	publishTestFrame(t, cache.owner, sourcecache.BlobKey{Content: struct{ Render string }{key}}, testRenderFrame(t, readSetSnapshot{}, []byte("ORDINARY\n")))
	out, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(out, "name", "fresh") {
		t.Fatalf("ordinary raw identity reused cached output: %s, %v", out, err)
	}
	_, stored, hit := readTestRenderFrame(t, cache, key)
	if eligible := renderPersistenceEligible(t); hit != eligible || hit && !reflect.DeepEqual(stored, out) {
		t.Fatalf("completed render = %s, %v; eligible = %v", stored, hit, eligible)
	}
	bare := req
	bare.Spec = nil
	if renderKey(bare, root, nil) == renderKey(req, root, nil) {
		t.Fatal("bare and owned cache identities collide")
	}
	first, err := Render(t.Context(), cache, bare, sourcecache.RetryConfig{})
	if err != nil || hasRenderField(first, manifest.FluxKustomizeDomain+"/name", "demo") || !hasRenderField(first, "name", "fresh") {
		t.Fatalf("bare render acquired owner metadata: %s, %v", first, err)
	}
	warm, err := Render(t.Context(), cache, bare, sourcecache.RetryConfig{})
	if err != nil || !reflect.DeepEqual(first, warm) {
		t.Fatalf("bare replay changed: %s, %v", warm, err)
	}
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("changed"))
	updated, err := Render(t.Context(), cache, bare, sourcecache.RetryConfig{})
	if err != nil || !hasRenderField(updated, "name", "changed") {
		t.Fatalf("bare cache missed input invalidation: %s, %v", updated, err)
	}
	req.Spec = req.Spec.Clone()
	req.Spec.NamePrefix = "owned-"
	owned, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
	if err != nil || hasRenderField(owned, manifest.FluxKustomizeDomain+"/name", "demo") || !hasRenderField(owned, "name", "owned-changed") {
		t.Fatalf("owned render reused bare output: %s, %v", owned, err)
	}

}

func TestRender_CacheFrames(t *testing.T) {
	t.Parallel()
	eligible := renderPersistenceEligible(t)
	t.Logf("render persistence eligible=%v", eligible)
	for _, tc := range []struct {
		name      string
		snapshot  readSetSnapshot
		malformed bool
		corrupt   bool
		invalid   bool
		empty     bool
		immutable bool
	}{
		{name: "malformed", malformed: true},
		{name: "validated corrupt payload", corrupt: true},
		{name: "corrupt payload invalid source", corrupt: true, invalid: true},
		{name: "mismatched", snapshot: readSetSnapshot{Files: []fileEntry{{Path: "cm.yaml", Hash: "mismatch"}}}},
		{name: "empty", empty: true},
		{name: "immutable", immutable: true, snapshot: readSetSnapshot{Files: []fileEntry{{Path: "unavailable", Hash: "mismatch"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("cm.yaml"),
				"cm.yaml":            cmYAML("fresh"),
			})
			if tc.invalid {
				testutil.WriteFile(t, root, "cm.yaml", "apiVersion: [\n")
			}
			cacheDir := t.TempDir()
			cache := newTestTreeCache(t, cacheDir)
			source := localSource(t, root)
			if tc.immutable {
				source.Key = "immutable-pin"
				source.Revision, source.Digest = "revision", "digest"
			}
			req := RenderRequest{Source: source, Label: root, SubPath: ".", Spec: demoSpec}
			var fresh []map[string]any
			var freshErr error
			if tc.corrupt {
				fresh, freshErr = Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{})
			}
			key := renderKey(req, root, nil)
			frame := testRenderFrame(t, tc.snapshot, testRenderPayload(t, "STALE"))
			if tc.malformed {
				frame = []byte{0, 0, 0, 1, 'x'}
			} else if tc.corrupt || tc.name == "mismatched" {
				frame = testRenderFrame(t, tc.snapshot, []byte("corrupt"))
			} else if tc.empty {
				frame = testRenderFrame(t, tc.snapshot, testRenderEncoding(t, nil))
			}
			publishTestFrame(t, cache.owner, sourcecache.BlobKey{Content: key, PersistOnly: true}, frame)
			out, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
			if tc.invalid {
				if err == nil || freshErr == nil || err.Error() != freshErr.Error() || len(out) != 0 {
					t.Fatalf("fresh render failure: %v, %v; want %v", out, err, freshErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.corrupt {
				assert.NoError(t, freshErr)
				assert.Diff(t, out, fresh)
				again, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
				assert.NoError(t, err)
				assert.Diff(t, again, fresh)
			}
			if eligible && tc.immutable {
				if !hasRenderField(out, "witness", "STALE") {
					t.Fatalf("immutable pin replayed mutable validation: %s", out)
				}
			} else if eligible && tc.empty {
				if len(out) != 0 {
					t.Fatalf("complete empty frame missed: %s", out)
				}
			} else if !hasRenderField(out, "name", "fresh") {
				t.Fatalf("candidate prevented a fresh render: %s", out)
			}
			cache.Close()
			if err := cache.owner.Close(); err != nil {
				t.Fatal(err)
			}
			cache = newTestTreeCache(t, cacheDir)
			_, stored, hit := readTestRenderFrame(t, cache, key)
			if hit != eligible || hit && !reflect.DeepEqual(stored, out) {
				t.Fatalf("completed frame = %s, %v; eligible = %v", stored, hit, eligible)
			}
			if tc.corrupt {
				again, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
				assert.NoError(t, err)
				assert.Diff(t, again, fresh)
				if eligible {
					t.Log("verified corrupt payload recovery, replacement, subsequent and reopened hits")
				}
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

func TestRender_CacheAcquisitionOverlap(t *testing.T) {
	for _, validate := range []bool{false, true} {
		t.Run(fmt.Sprintf("validation_%t", validate), func(t *testing.T) {
			eligible := renderPersistenceEligible(t)
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")})
			cache := newTestTreeCache(t, t.TempDir())
			key := sourcecache.BlobKey{Content: renderKey(RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "."}, root, nil), PersistOnly: true}
			frame := testRenderFrame(t, readSetSnapshot{Files: []fileEntry{{Path: "cm.yaml", Hash: manifest.SHA256Hex([]byte(cmYAML("fresh")))}}}, testRenderPayload(t, "VALIDATED"))
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
				out, err := Render(ctx, cache, RenderRequest{Source: &store.SourceArtifact{FS: lower}, Label: root, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
				if err == nil && validate && eligible && !hasRenderField(out, "witness", "VALIDATED") {
					err = fmt.Errorf("validation did not return its acquired frame: %q", out)
				}
				if err == nil && (!validate || !eligible) && !hasRenderField(out, "name", "fresh") {
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
					r, _, _, err := cache.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
					if r != nil {
						err = errors.Join(err, r.Close())
					}
					if validate && eligible && err != nil {
						return err
					}
					if (!validate || !eligible) && !errors.Is(err, errdef.ErrNotFound) {
						return fmt.Errorf("lookup before completed publication: %w", err)
					}
					r, _, _, err = cache.owner.Blob(ctx, key, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := w.Write(frame)
						return sourcecache.ProducerResult{}, err
					})
					if err != nil {
						return err
					}
					if err := r.Close(); err != nil {
						return err
					}
					r, _, _, err = cache.owner.Blob(ctx, sourcecache.BlobKey{Content: struct{ Name string }{"ordinary-source"}}, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						_, err := io.WriteString(w, "source")
						return sourcecache.ProducerResult{}, err
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

func TestRender_CacheStorageFailures(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lookup", "read", "publication"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			if !renderPersistenceEligible(t) {
				t.Skip("storage faults require an eligible build")
			}
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")})

			cacheDir := t.TempDir()
			cache := newTestTreeCache(t, cacheDir)
			req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
			if fault == "lookup" || fault == "read" {
				if _, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{}); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "read" {
				key := sourcecache.BlobKey{Content: renderKey(req, root, nil), PersistOnly: true}
				reader, _, _, err := cache.owner.Blob(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
				assert.NoError(t, err)
				data, err := io.ReadAll(reader)
				assert.NoError(t, errors.Join(err, reader.Close()))
				sum := digest.FromBytes(data)
				blob := filepath.Join(cacheDir, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
				assert.NoError(t, os.Remove(blob))
				assert.NoError(t, os.Mkdir(blob, 0o750))
			} else {
				assert.NoError(t, os.RemoveAll(cacheDir))
			}
			for range 2 {
				out, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
				if fault == "read" {
					if pathErr, ok := errors.AsType[*fs.PathError](err); !ok || pathErr.Op != "read" {
						t.Fatalf("read fault did not precede frame decoding: %v", err)
					}
				}
				if len(out) != 0 || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, errdef.ErrNotFound) {
					t.Fatalf("%s fault returned output %q, error %v", fault, out, err)
				}
			}
		})
	}
}

func TestRender_RemoteCacheReachability(t *testing.T) {
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted reachability assertions executed=%v; validator assertions always execute", eligible)
	t.Run("empty-and-canceled", func(t *testing.T) {
		valid, err := (readSetSnapshot{}).stillValid(t.Context(), nil, &treeFS{}, false)
		if err != nil || !valid {
			t.Fatalf("empty snapshot did work: %t, %v", valid, err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		valid, err = (readSetSnapshot{Files: []fileEntry{{Path: "unread", Hash: "hash"}}}).stillValid(ctx, nil, &treeFS{}, false)
		if valid || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation did not precede replay: %t, %v", valid, err)
		}
	})
	t.Run("cancellation-during-local-mismatch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		lower := &pausedRenderFS{FS: memoryTree(t, map[string]string{"cm.yaml": cmYAML("local")}), entered: make(chan struct{}), release: make(chan struct{})}
		done := make(chan error, 1)
		go func() {
			_, err := (readSetSnapshot{Files: []fileEntry{{Path: "cm.yaml", Hash: "mismatch"}}}).stillValid(ctx, nil, &treeFS{lower: lower}, false)
			done <- err
		}()
		<-lower.entered
		cancel()
		close(lower.release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("local mismatch swallowed cancellation: %v", err)
		}
	})
	for _, scenario := range []string{"local-removes", "nested-changes", "parent-removes", "reachable-fails", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			var version, hits atomic.Int32
			var unavailable, blocked atomic.Bool
			entered := make(chan struct{}, 1)
			var trace remoteHTTPTrace
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record := remoteHTTPRequest{Method: r.Method, Path: r.URL.Path, ETag: r.Header.Get("If-None-Match"), Modified: r.Header.Get("If-Modified-Since"), Status: http.StatusOK}
				defer func() {
					trace.mu.Lock()
					trace.requests = append(trace.requests, record)
					trace.mu.Unlock()
				}()
				hits.Add(1)
				if blocked.Load() {
					entered <- struct{}{}
					<-r.Context().Done()
					return
				}
				if unavailable.Load() {
					record.Status, record.Bytes = http.StatusNotFound, len("unavailable\n")
					http.Error(w, "unavailable", http.StatusNotFound)
					return
				}
				record.Bytes, _ = io.WriteString(w, cmYAML(fmt.Sprintf("child-v%d", version.Load())))
			}))
			srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					trace.connections.Add(1)
				}
			}
			srv.Start()
			t.Cleanup(func() {
				srv.Close()
				t.Logf("reachability: requests=%+v accepted_connections=%d", trace.snapshot(), trace.connections.Load())
			})
			const gitURL = "https://example.invalid/o/parent.git"
			control := "resources: ['" + gitURL + "?ref=main']\n"
			if scenario == "local-removes" {
				control = "resources: ['" + srv.URL + "/child.yaml']\n"
			}
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": control, "local.yaml": cmYAML("local")})
			cacheDir := t.TempDir()
			parentChanged := false
			newCache := func() *TreeCache {
				c := newTestTreeCache(t, cacheDir)
				files := map[string]string{"kustomization.yaml": "resources: ['" + srv.URL + "/child.yaml']\n"}
				revision := "1"
				if parentChanged {
					files = map[string]string{"kustomization.yaml": "resources: [local.yaml]\n", "local.yaml": cmYAML("parent-v2")}
					revision = "2"
				}
				remote := memoryTree(t, files)
				c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					return &store.SourceArtifact{FS: remote, Key: "stable-parent", Revision: revision}, nil
				})
				return c
			}
			request := func() RenderRequest {
				return RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
			}
			version.Store(1)
			c := newCache()
			out, err := Render(t.Context(), c, request(), sourcecache.RetryConfig{})
			if err != nil || !hasRenderField(out, "name", "child-v1") {
				t.Fatalf("initial render: %s, %v", out, err)
			}
			rec := newReadSet(root)
			view := newTreeFS(request().Source.FS, root, rec)
			if err := preflightRemoteResources(t.Context(), c, view, root); err != nil {
				t.Fatal(err)
			}
			snap, _, err := decodeFrame(testRenderFrame(t, rec.snapshot(), testRenderEncoding(t, out)))
			if err != nil {
				t.Fatal(err)
			}
			if valid, err := snap.stillValid(t.Context(), c, &treeFS{lower: request().Source.FS}, false); err != nil || !valid {
				t.Fatalf("unchanged recorded graph: valid=%t, %v", valid, err)
			}
			key := renderKey(request(), root, nil)
			if persisted, _, hit := readTestRenderFrame(t, c, key); hit {
				snap = persisted
				publishTestFrame(t, c.owner, sourcecache.BlobKey{Content: key, PersistOnly: true}, testRenderFrame(t, snap, testRenderPayload(t, "SENTINEL")))
			}
			closeTestTreeCache(t, c)
			hits.Store(0)
			switch scenario {
			case "local-removes":
				testutil.WriteFile(t, root, "kustomization.yaml", "resources: [local.yaml]\n")
				unavailable.Store(true)
			case "nested-changes":
				version.Store(2)
			case "parent-removes":
				parentChanged = true
				unavailable.Store(true)
			case "reachable-fails":
				unavailable.Store(true)
			case "cancellation":
				blocked.Store(true)
			}
			c = newCache()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "cancellation" {
				done := make(chan error, 1)
				go func() {
					valid, err := snap.stillValid(ctx, c, &treeFS{lower: request().Source.FS}, false)
					if valid {
						err = errors.New("canceled validation certified a hit")
					}
					done <- err
				}()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("validation did not reach the remote")
				}
				cancel()
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("cancellation error: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("validation did not promptly observe cancellation")
				}
			} else {
				valid, err := snap.stillValid(ctx, c, &treeFS{lower: request().Source.FS}, false)
				if err != nil || valid {
					t.Fatalf("changed/unavailable graph: valid=%t, %v", valid, err)
				}
			}
			if (scenario == "local-removes" || scenario == "parent-removes") && hits.Load() != 0 {
				t.Fatalf("removed edge fetched %d times during validation", hits.Load())
			}
			out, err = Render(ctx, c, request(), sourcecache.RetryConfig{})
			switch scenario {
			case "reachable-fails":
				if err == nil || len(out) != 0 {
					t.Fatalf("unavailable reachable input returned %q, %v", out, err)
				}
			case "cancellation":
				if !errors.Is(err, context.Canceled) || len(out) != 0 {
					t.Fatalf("canceled render returned %q, %v", out, err)
				}
			default:
				want := map[string]string{"local-removes": "name: local", "nested-changes": "name: child-v2", "parent-removes": "name: parent-v2"}[scenario]
				if err != nil || !hasRenderField(out, strings.SplitN(want, ": ", 2)[0], strings.SplitN(want, ": ", 2)[1]) {
					t.Fatalf("current graph: %s, %v; want %q", out, err, want)
				}
			}
			if (scenario == "local-removes" || scenario == "parent-removes") && hits.Load() != 0 {
				t.Fatalf("removed edge fetched %d times after fresh rendering", hits.Load())
			}
			closeTestTreeCache(t, c)
		})
	}
}

func TestRender_RemoteCacheLocalAttribution(t *testing.T) {
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted attribution assertions executed=%v; recorded snapshot assertions always execute", eligible)
	for _, scenario := range []string{"after-git", "rewritten-plugin", "source-link"} {
		t.Run(scenario, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "remote data")
			}))
			t.Cleanup(srv.Close)
			const gitURL = "https://example.invalid/o/repo.git"
			files := map[string]string{
				"kustomization.yaml": "resources: ['" + gitURL + "?ref=main', local.yaml]\n",
				"local.yaml":         cmYAML("local-v1"),
			}
			changed, value, want := "local.yaml", cmYAML("local-v2"), "name: local-v2"
			if scenario == "rewritten-plugin" {
				files["kustomization.yaml"] = "resources: ['" + gitURL + "?ref=main']\ngenerators: [plugins]\n"
				files["plugins/kustomization.yaml"] = "resources: [nested]\n"
				files["plugins/nested/kustomization.yaml"] = "resources: [../g.yaml]\n"
				files["plugins/g.yaml"] = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [local.txt, 'remote=" + srv.URL + "/value']\n"
				files["local.txt"] = "v1"
				changed, value, want = "local.txt", "v2", "local.txt: v2"
			}
			if scenario == "source-link" {
				files["kustomization.yaml"] = "resources: ['" + gitURL + "?ref=main', link.yaml]\n"
				files["other.yaml"] = cmYAML("other")
				want = "name: other"
			}
			root := testutil.WriteTree(t, files)
			var linked fs.FS
			if scenario == "source-link" {
				links := tree.NewBuilder(memoryTree(t, files))
				if err := links.AddSymlink("link.yaml", "local.yaml"); err != nil {
					t.Fatal(err)
				}
				var err error
				linked, err = links.Build()
				if err != nil {
					t.Fatal(err)
				}
			}
			remote := memoryTree(t, map[string]string{"kustomization.yaml": "resources: [cm.yaml]\n", "cm.yaml": cmYAML("git")})
			cacheDir := t.TempDir()
			newCache := func() *TreeCache {
				c := newTestTreeCache(t, cacheDir)
				c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					return &store.SourceArtifact{FS: remote, Key: "family", Revision: "1"}, nil
				})
				return c
			}
			request := func() RenderRequest {
				artifact := localSource(t, root)
				if linked != nil {
					artifact.FS = linked
				}
				return RenderRequest{Source: artifact, Label: root, SubPath: ".", Spec: demoSpec}
			}
			c := newCache()
			rec := newReadSet(root)
			view := newTreeFS(request().Source.FS, root, rec)
			if err := preflightRemoteResources(t.Context(), c, view, root); err != nil {
				t.Fatal(err)
			}
			_, err := buildTestTreeFS(view)
			if err != nil {
				t.Fatal(err)
			}
			snap := rec.snapshot()
			if valid, err := snap.stillValid(t.Context(), c, &treeFS{lower: request().Source.FS}, false); err != nil || !valid {
				t.Fatalf("unchanged snapshot: %+v, %v, %v", snap, valid, err)
			}
			observed := changed
			if scenario == "source-link" {
				observed = "link.yaml"
			}
			if !slices.ContainsFunc(snap.Files, func(f fileEntry) bool { return f.Path == observed }) {
				t.Fatalf("source input %q absent: %+v", observed, snap)
			}
			for _, f := range snap.Files {
				if strings.Contains(f.Path, remoteResourcePrefix) {
					t.Fatalf("overlay coordinate recorded as source: %+v", f)
				}
			}
			for name, body := range files {
				got, err := os.ReadFile(filepath.Join(root, name))
				if err != nil || string(got) != body {
					t.Fatalf("source %q mutated: %q, %v", name, got, err)
				}
			}
			out, err := Render(t.Context(), c, request(), sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("first render: %s, %v", out, err)
			}
			key := renderKey(request(), root, nil)
			if persisted, _, hit := readTestRenderFrame(t, c, key); hit {
				publishTestFrame(t, c.owner, sourcecache.BlobKey{Content: key, PersistOnly: true}, testRenderFrame(t, persisted, testRenderPayload(t, "SENTINEL")))
			}
			closeTestTreeCache(t, c)
			if scenario == "source-link" {
				links := tree.NewBuilder(linked)
				if err := links.AddSymlink("link.yaml", "other.yaml"); err != nil {
					t.Fatal(err)
				}
				linked, err = links.Build()
				if err != nil {
					t.Fatal(err)
				}
			} else {
				testutil.WriteFile(t, root, changed, value)
			}
			c = newCache()
			if valid, err := snap.stillValid(t.Context(), c, &treeFS{lower: request().Source.FS}, false); err != nil || valid {
				t.Fatalf("changed source validated: %t, %v", valid, err)
			}
			out, err = Render(t.Context(), c, request(), sourcecache.RetryConfig{})
			if err != nil || !hasRenderField(out, strings.SplitN(want, ": ", 2)[0], strings.SplitN(want, ": ", 2)[1]) {
				t.Fatalf("source edit: %s, %v; want %q", out, err, want)
			}
			closeTestTreeCache(t, c)
		})
	}
	t.Run("source-link-to-injected-file", func(t *testing.T) {
		const body = "remote bytes"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, body)
		}))
		t.Cleanup(srv.Close)
		for _, open := range []bool{false, true} {
			t.Run(fmt.Sprintf("open_%t", open), func(t *testing.T) {
				c := newTestTreeCache(t, "")
				const root = "/source"
				rec := newReadSet(root)
				view := newTreeFS(memoryTree(t, nil), root, rec)
				walk := inputWalk{cache: c, fs: view}
				if _, err := walk.acquire(t.Context(), remoteInput{Operation: remoteHTTP, URL: srv.URL}, walkScope{}); err != nil {
					t.Fatal(err)
				}
				generated, err := walk.mount([]byte(body))
				if err != nil {
					t.Fatal(err)
				}
				links := tree.NewBuilder(nil)
				if err := links.AddSymlink("link", filepath.Base(generated)); err != nil {
					t.Fatal(err)
				}
				lower, err := links.Build()
				if err != nil {
					t.Fatal(err)
				}
				view.lower = lower
				view.upper = tree.NewBuilder(lower)
				if _, err := walk.mount([]byte(body)); err != nil {
					t.Fatal(err)
				}
				if open {
					f, err := view.Open(root + "/link")
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(f)
					if err = errors.Join(err, f.Close()); err != nil || string(data) != body {
						t.Fatalf("read: %q, %v", data, err)
					}
				} else if data, err := view.ReadFile(root + "/link"); err != nil || string(data) != body {
					t.Fatalf("read: %q, %v", data, err)
				}
				snap := rec.snapshot()
				if len(snap.Files) != 1 || snap.Files[0].Path != "link" || snap.Files[0].Hash == "" {
					t.Fatalf("authored source link lost: %+v", snap)
				}
				if validReadSet(t, readSetSnapshot{Files: snap.Files}, lower) {
					t.Fatal("lower source cannot certify bytes obtained through an injected target")
				}
				changed := tree.NewBuilder(lower)
				if err := changed.AddFile("local", []byte("new bytes"), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := changed.AddSymlink("link", "local"); err != nil {
					t.Fatal(err)
				}
				current, err := changed.Build()
				if err != nil {
					t.Fatal(err)
				}
				if validReadSet(t, readSetSnapshot{Files: snap.Files}, current) {
					t.Fatal("retargeted source link replayed injected bytes")
				}
			})
		}
	})
	t.Run("nested-snapshot-observations", func(t *testing.T) {
		root := testutil.WriteTree(t, map[string]string{"local.txt": "source", "view-more-data/file": "prefix", "listed/one": "one", "walked/one": "one", "node": "file"})
		lower := os.DirFS(root)
		rec := newReadSet(root)
		view := newTreeFS(lower, root, rec)
		snapshot, err := view.upper.Build()
		if err != nil {
			t.Fatal(err)
		}
		view.upper = tree.NewBuilder(snapshot)
		if err := view.upper.Mount("view", snapshot); err != nil {
			t.Fatal(err)
		}
		view.origins = append(view.origins, mountOrigin{path: "view", source: "."})
		c := newTestTreeCache(t, "")
		c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			return &store.SourceArtifact{FS: memoryTree(t, map[string]string{"foreign": "remote"}), Key: "git-family", Revision: "1"}, nil
		})
		walk := inputWalk{cache: c, fs: view}
		gitPath, err := walk.fetchGitBase(t.Context(), kustomizeinput.GitBase{RepoURL: "https://example.invalid/o/repo.git"}, walkScope{root: filepath.Join(root, "view")})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err = view.upper.Build()
		if err != nil {
			t.Fatal(err)
		}
		view.upper = tree.NewBuilder(snapshot)
		if err := view.upper.Mount("view-more", snapshot); err != nil {
			t.Fatal(err)
		}
		view.origins = append(view.origins, mountOrigin{path: "view-more", source: "."})
		alias := filepath.Join(root, "view-more", "view")
		originalDir := filepath.Join(root, "view", gitPath)
		nestedDir := filepath.Join(alias, gitPath)
		original := walk.location(controlLocation{file: filepath.Join(originalDir, "foreign"), root: originalDir, pluginRoot: filepath.Join(root, "view")})
		nested := walk.location(controlLocation{file: filepath.Join(nestedDir, "foreign"), root: nestedDir, pluginRoot: alias})
		if original != nested || original.file == "" {
			t.Fatalf("nested remote location lost canonical coordinates: %+v, %+v", original, nested)
		}
		if _, err := view.ReadFile(filepath.Join(alias, "local.txt")); err != nil {
			t.Fatal(err)
		}
		f, err := view.Open(filepath.Join(alias, "local.txt"))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := view.ReadFile(filepath.Join(root, "view-more-data/file")); err != nil {
			t.Fatal(err)
		}
		if _, err := view.ReadFile(filepath.Join(alias, gitPath, "foreign")); err != nil {
			t.Fatal(err)
		}
		if err := view.WriteFile(filepath.Join(alias, "generated"), []byte("generated")); err != nil {
			t.Fatal(err)
		}
		if _, err := view.ReadFile(filepath.Join(alias, "generated")); err != nil {
			t.Fatal(err)
		}
		if !view.Exists(filepath.Join(alias, "node")) || view.IsDir(filepath.Join(alias, "node")) {
			t.Fatal("aliased source node was not a file")
		}
		if _, err := view.ReadDir(alias); err != nil {
			t.Fatal(err)
		}
		if _, err := view.ReadDir(filepath.Join(alias, "listed")); err != nil {
			t.Fatal(err)
		}
		if err := view.Walk(filepath.Join(alias, "walked"), func(_ string, _ os.FileInfo, err error) error { return err }); err != nil {
			t.Fatal(err)
		}
		snap := rec.snapshot()
		if len(snap.Remotes) != 1 || len(snap.Files) != 2 || snap.Files[0].Path != "local.txt" || snap.Files[1].Path != "view-more-data/file" {
			t.Fatalf("nested snapshot attribution: %+v", snap)
		}
		if valid, err := snap.stillValid(t.Context(), c, &treeFS{lower: lower}, false); err != nil || !valid {
			t.Fatalf("snapshot contains injected directory members: %+v, %t, %v", snap, valid, err)
		}
		for _, dir := range []string{"listed", "walked"} {
			t.Run(dir+"-membership", func(t *testing.T) {
				index := slices.IndexFunc(snap.Dirs, func(f fileEntry) bool { return f.Path == dir })
				if index < 0 {
					t.Fatalf("directory %q was not observed: %+v", dir, snap)
				}
				testutil.WriteFile(t, root, dir+"/two", "two")
				if validReadSet(t, readSetSnapshot{Dirs: snap.Dirs[index : index+1]}, lower) {
					t.Fatal("aliased directory membership change was missed")
				}
			})
		}
		if err := os.Remove(filepath.Join(root, "node")); err != nil {
			t.Fatal(err)
		}
		testutil.WriteFile(t, root, "node/child", "child")
		if validReadSet(t, readSetSnapshot{Nodes: snap.Nodes}, lower) {
			t.Fatal("aliased node transition was missed")
		}
	})
}

func TestRender_RemoteCacheIsolation(t *testing.T) {
	eligible := renderPersistenceEligible(t)
	t.Logf("persisted isolation assertions executed=%v; concurrent snapshot assertions always execute", eligible)
	var shared, distinct atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/shared" {
			shared.Add(1)
			entered <- struct{}{}
			<-release
		} else {
			distinct.Add(1)
		}
		_, _ = io.WriteString(w, "contents")
	}))
	t.Cleanup(srv.Close)
	c := newTestTreeCache(t, t.TempDir())
	type result struct {
		request RenderRequest
		snap    readSetSnapshot
		output  []map[string]any
		err     error
	}
	results := make(chan result, 2)
	requests := make([]RenderRequest, 2)
	for i := range requests {
		name := fmt.Sprintf("local-%d", i)
		root := testutil.WriteTree(t, map[string]string{
			"kustomization.yaml": "resources: [" + name + ".yaml]\nconfigMapGenerator: [{name: generated, files: ['first=" + srv.URL + "/shared', 'second=" + srv.URL + "/shared', 'distinct=" + srv.URL + "/" + name + "']}]\n",
			name + ".yaml":       cmYAML(name),
		})
		requests[i] = RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	for _, request := range requests {
		wg.Go(func() {
			rec := newReadSet(request.Label)
			view := newTreeFS(request.Source.FS, request.Label, rec)
			err := preflightRemoteResources(t.Context(), c, view, request.Label)
			if err != nil {
				results <- result{err: err}
				return
			}
			if _, err := buildTestTreeFS(view); err != nil {
				results <- result{err: err}
				return
			}
			out, err := Render(t.Context(), c, request, sourcecache.RetryConfig{})
			results <- result{request: request, snap: rec.snapshot(), output: out, err: err}
		})
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent renders did not reach shared acquisition")
	}
	close(release)
	released = true
	for range requests {
		got := <-results
		if got.err != nil || !hasRenderField(got.output, "first", "contents") {
			t.Fatalf("concurrent render: %s, %v", got.output, got.err)
		}
		snap := got.snap
		if persisted, _, hit := readTestRenderFrame(t, c, renderKey(got.request, got.request.Label, nil)); hit {
			snap = persisted
		}
		if len(snap.Remotes) != 2 || snap.Remotes[0].Input.URL >= snap.Remotes[1].Input.URL {
			t.Fatalf("remote descriptors not private/deduplicated/sorted: %+v", snap)
		}
		control, err := fs.ReadFile(got.request.Source.FS, "kustomization.yaml")
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range snap.Remotes {
			if !bytes.Contains(control, []byte(entry.Input.URL)) {
				t.Fatalf("another render's dependency leaked: %+v", entry)
			}
		}
		for _, file := range snap.Files {
			if _, err := fs.Stat(got.request.Source.FS, file.Path); err != nil {
				t.Fatalf("another source's read leaked: %+v, %v", file, err)
			}
		}
		if valid, err := snap.stillValid(t.Context(), c, &treeFS{lower: got.request.Source.FS}, false); err != nil || !valid {
			t.Fatalf("private snapshot invalid: %+v, %t, %v", snap, valid, err)
		}
	}
	if shared.Load() != 1 || distinct.Load() != 2 {
		t.Fatalf("shared requests=%d, distinct=%d", shared.Load(), distinct.Load())
	}
}

func buildTestTreeFS(view *treeFS) ([]byte, error) {
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	rm, err := fluxkustomize.Build(view, view.root)
	if err != nil {
		return nil, err
	}
	return rm.AsYaml()
}

func hasRenderField(value any, key, want string) bool {
	switch value := value.(type) {
	case string:
		return key == "" && strings.Contains(value, want)
	case []map[string]any:
		for _, doc := range value {
			if hasRenderField(doc, key, want) {
				return true
			}
		}
	case map[string]any:
		if value[key] == want {
			return true
		}
		if items, ok := value[key].([]any); ok {
			for _, item := range items {
				if item == want {
					return true
				}
			}
		}
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if hasRenderField(item, key, want) {
				return true
			}
		}
	}
	return false
}

func testRenderEncoding(tb testing.TB, docs []map[string]any) []byte {
	tb.Helper()
	payload, err := manifest.EncodeRender(docs)
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}

func testRenderPayload(tb testing.TB, witness string) []byte {
	tb.Helper()
	return testRenderEncoding(tb, []map[string]any{{"witness": witness}})
}
