package kustomize

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// memFSWith uses the same private filesystem boundary as a render.
func memFSWith(t *testing.T, files map[string]string) *treeFS {
	t.Helper()
	fsys := newTreeFS(os.DirFS(t.TempDir()), "/", nil)
	for p, body := range files {
		if err := fsys.WriteFile(p, []byte(body)); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}
	return fsys
}

// remoteFiles lists the pre-fetched remote-resource files preflight wrote into
// dir (the .flate-remote-*.yaml form), so a test can assert one landed.
func remoteFiles(t *testing.T, fsys filesys.FileSystem, dir string) []string {
	t.Helper()
	names, err := fsys.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, remoteResourcePrefix) {
			out = append(out, n)
		}
	}
	return out
}

// TestPreflightRemoteResources_RewritesSuccessfulFetch confirms the happy
// path: a kustomization listing an HTTP resource → flate fetches it → the
// kustomization gets the URL replaced with the local file → the fetched body
// lives next to the kustomization in the fs. The point is to make
// kustomize.Build see local files only so it never invokes the git fallback.
func TestPreflightRemoteResources_RewritesSuccessfulFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/foo.yaml\n"})
	if err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	body, err := fsys.ReadFile("kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("URL still present after preflight:\n%s", body)
	}
	if !strings.Contains(string(body), remoteResourcePrefix) {
		t.Errorf("expected rewritten path with %s prefix:\n%s", remoteResourcePrefix, body)
	}

	fetched := remoteFiles(t, fsys, ".")
	if len(fetched) != 1 {
		t.Fatalf("expected one fetched file, got %v", fetched)
	}
	cached, err := fsys.ReadFile(fetched[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cached), "kind: ConfigMap") {
		t.Errorf("fetched body lost: %q", cached)
	}
}

// TestPreflightRemoteResources_PropagatesFailure locks the contract: a URL
// fetch failure returns an error to the caller — the KS controller surfaces it
// as a real reconcile failure rather than silently tombstoning. The error
// wraps the URL and the underlying status so `flate test` shows the user what's
// actually broken in their repo.
func TestPreflightRemoteResources_PropagatesFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/missing.yaml\n"})
	err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
	if err == nil {
		t.Fatal("expected error on 404; got nil (preflight would silently swallow the failure)")
	}
	if !strings.Contains(err.Error(), srv.URL) {
		t.Errorf("error should name the URL; got %q", err)
	}
	if !strings.Contains(err.Error(), "HTTP 404") {
		t.Errorf("error should name the status code; got %q", err)
	}
}

// TestPreflightRemoteResources_IgnoresLocalEntries guards the no-op path: a
// kustomization with only local resources must be untouched.
func TestPreflightRemoteResources_IgnoresLocalEntries(t *testing.T) {
	t.Parallel()
	body := "resources:\n  - ./local.yaml\n  - ../shared/cm.yaml\n"
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": body})
	if err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	got, _ := fsys.ReadFile("kustomization.yaml")
	if string(got) != body {
		t.Errorf("local-only kustomization was modified:\nwant %q\ngot  %q", body, got)
	}
}

func TestPreflightRemoteResources_WalksNestedKustomizations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: nested}\n"))
	}))
	t.Cleanup(srv.Close)

	const parent = "resources:\n  - ./components/x\n"
	fsys := memFSWith(t, map[string]string{
		"kustomization.yaml":              parent,
		"components/x/kustomization.yaml": "resources:\n  - " + srv.URL + "/nested.yaml\n",
	})
	err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if body, _ := fsys.ReadFile("kustomization.yaml"); string(body) != parent {
		t.Fatalf("unchanged parent operand was rewritten: %q", body)
	}
	body, _ := fsys.ReadFile("components/x/kustomization.yaml")
	if strings.Contains(string(body), srv.URL) {
		t.Errorf("nested URL not rewritten:\n%s", body)
	}
}

// TestRenderFlux_RemotePreflightDoesNotMutateSource proves the in-memory render
// never touches the user's working tree: after a render that pre-fetches a
// remote resource, the on-disk source kustomization is byte-for-byte unchanged
// and no fetched files were written to disk.
func TestRenderFlux_RemotePreflightDoesNotMutateSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	child := "resources:\n  - " + srv.URL + "/remote.yaml\n"
	src := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml":       "resources:\n  - ./child\n",
		"child/kustomization.yaml": child,
	})

	_, err := RenderFlux(context.Background(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, src), Label: src, ApplyIgnore: false, SubPath: ".", Spec: parsedKS(t, map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	})}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(src, "child", "kustomization.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != child {
		t.Fatalf("source nested kustomization was mutated:\nwant %q\ngot  %q", child, got)
	}
	matches, _ := filepath.Glob(filepath.Join(src, "child", remoteResourcePrefix+"*.yaml"))
	if len(matches) != 0 {
		t.Fatalf("remote preflight wrote fetched files into source tree: %v", matches)
	}
}

// TestPreflightRemoteResources_HonorsAlternateFilenames sanity-checks the
// filename matcher: kustomize accepts kustomization.yml and Kustomization in
// addition to kustomization.yaml.
func TestPreflightRemoteResources_HonorsAlternateFilenames(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\n"))
	}))
	t.Cleanup(srv.Close)

	for _, name := range []string{"kustomization.yml", "Kustomization"} {
		t.Run(name, func(t *testing.T) {
			fsys := memFSWith(t, map[string]string{name: "resources:\n  - " + srv.URL + "/x.yaml\n"})
			if err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
				t.Fatalf("preflight: %v", err)
			}
			body, _ := fsys.ReadFile(name)
			if strings.Contains(string(body), srv.URL) {
				t.Errorf("%s URL not rewritten:\n%s", name, body)
			}
		})
	}
}

// TestPreflightRemoteResources_FetchesEachURLOnce pins the dedup contract: a
// URL referenced from N kustomization files (parent + nested child + sibling
// overlay …) must hit the network exactly once per run — the fix for the
// m00nwtchr report where the same broken URL emitted 6 WARNs per `flate test
// all`.
func TestPreflightRemoteResources_FetchesEachURLOnce(t *testing.T) {
	var hits int32
	mu := &sync.Mutex{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: shared}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{
		"kustomization.yaml":   "resources: [a, b, c]\n",
		"a/kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n",
		"b/kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n",
		"c/kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n",
	})

	cache := newTestTreeCache(t, "")
	if err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 network call, got %d", hits)
	}
	if got := remoteFiles(t, fsys, "."); len(got) != 1 {
		t.Errorf("expected one immutable shared overlay file, got %v", got)
	}

	// A second preflight against the same cache (production runs one preflight
	// per RenderFlux) must NOT re-fetch.
	fsys2 := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n"})
	if err := preflightRemoteResources(context.Background(), cache, fsys2, "."); err != nil {
		t.Fatalf("second preflight: %v", err)
	}
	if hits != 1 {
		t.Errorf("expected 1 network call across both runs, got %d", hits)
	}
}

// TestHttpGetURL_RejectsOversizedBody pins the OOM guard: an endpoint returning
// more than remoteFetchMaxBytes must fail fast rather than reading the whole
// body into memory.
func TestHttpGetURL_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		body := remoteBody{Reader: remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), close: func() error { return nil }}
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})
	_, err := c.httpGetURL(t.Context(), "https://example.invalid/big.yaml", sourcecache.ProducerInput{Writer: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized-body error=%v", err)
	}
}

func TestPreflightRemoteResources_PreservesCommentsAndOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("kind: ConfigMap\n"))
	}))
	t.Cleanup(srv.Close)

	original := `# Owner comment that MUST survive.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
# why we have this list
resources:
  # remote shared fragment
  - ` + srv.URL + `/x.yaml
  - ./local.yaml  # keep this comment
namespace: my-app
`
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": original})
	if err := preflightRemoteResources(t.Context(), newTestTreeCache(t, ""), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}

	got, err := fsys.ReadFile("kustomization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)

	for _, expect := range []string{
		"# Owner comment that MUST survive.",
		"# why we have this list",
		"# remote shared fragment",
		"# keep this comment",
	} {
		if !strings.Contains(out, expect) {
			t.Errorf("comment dropped on rewrite: %q\nfile is:\n%s", expect, out)
		}
	}
	if !strings.Contains(out, "- ./local.yaml") {
		t.Errorf("local resource path mangled:\n%s", out)
	}
	if !strings.Contains(out, remoteResourcePrefix) {
		t.Errorf("URL entry not rewritten to local file:\n%s", out)
	}
	if strings.Contains(out, srv.URL) {
		t.Errorf("URL still present after rewrite:\n%s", out)
	}
	if idxRes, idxNs := strings.Index(out, "\nresources:"), strings.Index(out, "\nnamespace:"); idxRes < 0 || idxNs < 0 || idxRes >= idxNs {
		t.Errorf("key ordering not preserved (resources should precede namespace):\n%s", out)
	}
}

// TestHttpGetURL_AcceptsBodyAtCap is the boundary check: a body exactly at the
// cap must succeed. Guards against off-by-one in the LimitReader+overflow
// pattern.
func TestHttpGetURL_AcceptsBodyAtCap(t *testing.T) {
	t.Parallel()
	c := newTestTreeCache(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		reader := io.LimitReader(remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), remoteFetchMaxBytes)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(reader), Header: make(http.Header)}, nil
	})
	desc, err := c.httpGetURL(t.Context(), "https://example.invalid/atcap.yaml", sourcecache.ProducerInput{Writer: io.Discard})
	if err != nil || desc.Size != remoteFetchMaxBytes {
		t.Fatalf("body exactly at cap: size=%d, error=%v", desc.Size, err)
	}
}

func TestPreflight_UppercaseAndMixedCaseScheme(t *testing.T) {
	for _, scheme := range []string{"HTTP", "HtTp", "HTTPS", "hTtPs"} {
		t.Run(scheme, func(t *testing.T) {
			var hits atomic.Int64
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				_, _ = w.Write([]byte(cmYAML("remote")))
			})
			var srv *httptest.Server
			if strings.EqualFold(scheme, "https") {
				srv = httptest.NewTLSServer(handler)
			} else {
				srv = httptest.NewServer(handler)
			}
			t.Cleanup(srv.Close)
			url := scheme + srv.URL[strings.Index(srv.URL, ":"):] + "/cm.yaml"
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization(url)})
			cache := newTestTreeCache(t, "")
			cache.httpClient = srv.Client()
			out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}, sourcecache.RetryConfig{})
			if err != nil || !strings.Contains(string(out), "name: remote") {
				t.Fatalf("render: %s, %v", out, err)
			}
			if _, err := cache.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}, nil); err != nil {
				t.Fatal(err)
			}
			if got := hits.Load(); got != 1 {
				t.Fatalf("server hits = %d, want 1 shared preflight fetch", got)
			}
		})
	}
}

func TestPreflightRemoteResources_ReachableInputs(t *testing.T) {
	const base = "resources: [cm.yaml]\n"
	const patch = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {marker: patched}\n"
	const target = "target: {kind: ConfigMap, name: demo}\n"
	const replacement = "source: {kind: ConfigMap, name: demo, fieldPath: metadata.name}\ntargets:\n- select: {kind: ConfigMap, name: demo}\n  fieldPaths: [data.marker]\n"
	const plugin = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: [local.txt, $URL/file.txt]\n"
	cases := []struct {
		name, control, subpath, want, wantErr string
		files, replies, gitFiles              map[string]string
	}{
		{name: "resource", control: "resources: [$URL/resource.yaml]\n", replies: map[string]string{"/resource.yaml": cmYAML("remote")}, want: "name: remote"},
		{name: "base", control: "bases: [$URL/resource.yaml]\n", replies: map[string]string{"/resource.yaml": cmYAML("remote")}, want: "name: remote"},
		{name: "patch", control: base + "patches: [{path: $URL/patch.yaml}]\n", replies: map[string]string{"/patch.yaml": patch}, want: "marker: patched"},
		{name: "json-patch", control: base + "patchesJson6902:\n- " + target + "  path: $URL/patch.json\n", replies: map[string]string{"/patch.json": `[{"op":"replace","path":"/data/marker","value":"patched"}]`}, want: "marker: patched"},
		{name: "strategic-patch", control: base + "patchesStrategicMerge: [$URL/patch.yaml]\n", replies: map[string]string{"/patch.yaml": patch}, want: "marker: patched"},
		{name: "replacement", control: base + "replacements: [{path: $URL/replacement.yaml}]\n", replies: map[string]string{"/replacement.yaml": replacement}, want: "marker: demo"},
		{name: "configuration", control: base + "configurations: [$URL/config.yaml]\n", replies: map[string]string{"/config.yaml": "nameReference: []\n"}, want: "name: demo"},
		{name: "crd", control: base + "crds: [$URL/crd.json]\n", replies: map[string]string{"/crd.json": "{}"}, want: "name: demo"},
		{name: "openapi", control: base + "openapi: {path: $URL/schema.json}\n", replies: map[string]string{"/schema.json": `{"swagger":"2.0","definitions":{}}`}, want: "name: demo"},
		{name: "inferred-key", control: base + "configMapGenerator: [{name: generated, files: [$URL/file.txt]}]\n", replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "explicit-key", control: base + "configMapGenerator: [{name: generated, files: [chosen=$URL/file.txt]}]\n", replies: map[string]string{"/file.txt": "contents"}, want: "chosen: contents"},
		{name: "invalid-key-grammar", control: base + "configMapGenerator: [{name: generated, files: ['key=$URL/file.txt=extra']}]\n", wantErr: "key name or file path contains '='"},
		{name: "invalid-empty-key", control: base + "configMapGenerator: [{name: generated, files: ['=$URL/file.txt']}]\n", wantErr: "missing key name"},
		{name: "configmap-envs", control: base + "configMapGenerator: [{name: generated, envs: [$URL/env]}]\n", replies: map[string]string{"/env": "marker=env-value\n"}, want: "marker: env-value"},
		{name: "configmap-env", control: base + "configMapGenerator: [{name: generated, env: $URL/env}]\n", replies: map[string]string{"/env": "marker=env-value\n"}, want: "marker: env-value"},
		{name: "secret-files", control: base + "secretGenerator: [{name: generated, files: [$URL/file.txt]}]\n", replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: Y29udGVudHM="},
		{name: "secret-envs", control: base + "secretGenerator: [{name: generated, envs: [$URL/env]}]\n", replies: map[string]string{"/env": "marker=secret\n"}, want: "marker: c2VjcmV0"},
		{name: "secret-env", control: base + "secretGenerator: [{name: generated, env: $URL/env}]\n", replies: map[string]string{"/env": "marker=secret\n"}, want: "marker: c2VjcmV0"},
		{name: "case-shadowed-resource", control: "resources: [child]\n", files: map[string]string{"child/kustomization.yaml": "Resources: [../cm.yaml]\nresources: [$URL/resource.yaml]\n"}, replies: map[string]string{"/resource.yaml": cmYAML("remote")}, want: "name: remote"},
		{name: "case-shadowed-data", control: "resources: [child]\n", files: map[string]string{"child/kustomization.yaml": "resources: [../cm.yaml]\nResources: [$URL/data]\n"}, want: "name: demo"},
		{name: "case-insensitive-control", control: "resources: [child]\n", files: map[string]string{"child/kustomization.yaml": "Resources: [$URL/resource.yaml]\n"}, replies: map[string]string{"/resource.yaml": cmYAML("remote")}, want: "name: remote"},
		{name: "local-parent-alternate-control", control: "resources: [child]\n", files: map[string]string{"child/Kustomization": "resources: [$URL/resource.yaml]\n"}, replies: map[string]string{"/resource.yaml": cmYAML("remote")}, want: "name: remote"},
		{name: "outside-component", subpath: "selected", control: "resources: [cm.yaml]\ncomponents: [../component]\n", files: map[string]string{"component/kustomization.yml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches: [{path: $URL/patch.yaml}]\n"}, replies: map[string]string{"/patch.yaml": patch}, want: "marker: patched"},
		{name: "generated-component", subpath: "selected", control: base, files: map[string]string{"component/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\npatches: [{path: $URL/patch.yaml}]\n"}, replies: map[string]string{"/patch.yaml": patch}, want: "marker: patched"},
		{name: "coalesced-resource-patch", control: "resources: [$URL/shared.yaml]\npatches: [{path: $URL/shared.yaml}]\n", replies: map[string]string{"/shared.yaml": cmYAML("remote")}, want: "name: remote"},
		{name: "fetched-git-input", control: "resources: ['https://example.invalid/o/r.git//selected?ref=main']\n", gitFiles: map[string]string{"selected/kustomization.yaml": base + "components: [../component]\n", "selected/cm.yaml": cmYAML("demo"), "component/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nconfigMapGenerator: [{name: generated, files: [$URL/file.txt]}]\n"}, replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "plugin-git-directory", control: base + "generators: ['https://example.invalid/o/r.git//plugins?ref=main']\n", gitFiles: map[string]string{"plugins/kustomization.yaml": "resources: [g.yaml]\n", "plugins/g.yaml": plugin}, replies: map[string]string{"/file.txt": "contents"}, want: "local.txt: owner"},
		{name: "plugin-file", control: base + "generators: [conf/g.yaml]\n", files: map[string]string{"conf/g.yaml": plugin}, replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "plugin-inline-multidoc", control: base + "generators:\n- |\n  " + strings.ReplaceAll(plugin+"---\napiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: other}\nenvs: [$URL/env]\n", "\n", "\n  "), replies: map[string]string{"/file.txt": "contents", "/env": "marker=secret\n"}, want: "marker: c2VjcmV0"},
		{name: "git-looking-file", control: base + "patches: [{path: $URL/repo.git//patch.yaml}]\n", replies: map[string]string{"/repo.git//patch.yaml": patch}, want: "marker: patched"},
		{name: "plugin-inline", control: base + "generators:\n- |\n" + "  " + strings.ReplaceAll(plugin, "\n", "\n  "), replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "plugin-directory-owner", control: base + "generators: [plugins]\n", files: map[string]string{"plugins/kustomization.yaml": "resources: [conf/g.yaml]\n", "plugins/conf/g.yaml": plugin}, replies: map[string]string{"/file.txt": "contents"}, want: "local.txt: owner"},
		{name: "plugin-multidoc", control: base + "generators: [$URL/g.yaml]\n", replies: map[string]string{"/g.yaml": plugin + "---\napiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: other}\nenvs: [$URL/env]\n", "/file.txt": "contents", "/env": "marker=secret\n"}, want: "marker: c2VjcmV0"},
		{name: "plugin-secret-files", control: base + "generators: [g.yaml]\n", files: map[string]string{"g.yaml": "apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: generated}\nfiles: [$URL/file.txt]\n"}, replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: Y29udGVudHM="},
		{name: "plugin-case-insensitive-files", control: base + "generators: [g.yaml]\n", files: map[string]string{"g.yaml": strings.ReplaceAll(plugin, "files:", "Files:")}, replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "plugin-local-directory", control: base + "generators: [plugins]\n", files: map[string]string{"plugins/kustomization.yaml": "resources: [g.yaml]\n", "plugins/g.yaml": strings.ReplaceAll(plugin, ", $URL/file.txt", "")}, want: "local.txt: owner"},
		{name: "plugin-directory-two-owners", control: "resources: [a, b]\n", files: map[string]string{"a/kustomization.yaml": "namePrefix: a-\ngenerators: [../plugins]\n", "b/kustomization.yaml": "namePrefix: b-\ngenerators: [../plugins]\n", "plugins/kustomization.yaml": "resources: [nested]\n", "plugins/nested/kustomization.yaml": "resources: [../g.yaml]\n", "plugins/g.yaml": plugin, "a/local.txt": "owner-a", "b/local.txt": "owner-b"}, replies: map[string]string{"/file.txt": "contents"}, want: "local.txt: owner-b"},
		{name: "plugin-patch", control: base + "transformers: [p.yaml]\n", files: map[string]string{"p.yaml": "apiVersion: builtin\nkind: PatchTransformer\nmetadata: {name: patch}\npath: $URL/patch.yaml\n"}, replies: map[string]string{"/patch.yaml": patch}, want: "marker: patched"},
		{name: "plugin-json", control: base + "transformers: [p.yaml]\n", files: map[string]string{"p.yaml": "apiVersion: builtin\nkind: PatchJson6902Transformer\nmetadata: {name: patch}\n" + target + "path: $URL/patch.json\n"}, replies: map[string]string{"/patch.json": `[{"op":"replace","path":"/data/marker","value":"patched"}]`}, want: "marker: patched"},
		{name: "plugin-strategic", control: base + "transformers: [p.yaml]\n", files: map[string]string{"p.yaml": "apiVersion: builtin\nkind: PatchStrategicMergeTransformer\nmetadata: {name: patch}\npaths: [$URL/patch.yaml]\n"}, replies: map[string]string{"/patch.yaml": patch}, want: "marker: patched"},
		{name: "plugin-replacement-validator", control: base + "validators: [p.yaml]\n", files: map[string]string{"p.yaml": "apiVersion: builtin\nkind: ReplacementTransformer\nmetadata: {name: replace}\nreplacements: [{path: $URL/replacement.yaml}]\n"}, replies: map[string]string{"/replacement.yaml": strings.ReplaceAll(replacement, "metadata.name", "data.marker")}, want: "marker: initial"},
		{name: "plugin-valueadd", control: base + "transformers: [p.yaml]\n", files: map[string]string{"p.yaml": "apiVersion: builtin\nkind: ValueAddTransformer\nmetadata: {name: add}\nvalue: injected\ntargetFilePath: $URL/targets.yaml\n"}, replies: map[string]string{"/targets.yaml": "targets: [{fieldPath: metadata/namespace}]\n"}, want: "namespace: injected"},
		{name: "directory-resource-and-plugin-role", control: "resources: [plugins]\ngenerators: [plugins]\n", files: map[string]string{"plugins/kustomization.yaml": "resources: [g.yaml]\n", "plugins/g.yaml": plugin}, replies: map[string]string{"/file.txt": "contents"}, want: "$URL/file.txt"},
		{name: "resource-and-plugin-role", control: "resources: [conf/g.yaml]\ngenerators: [conf/g.yaml]\n", files: map[string]string{"conf/g.yaml": plugin}, replies: map[string]string{"/file.txt": "contents"}, want: "$URL/file.txt"},
		{name: "two-plugin-owners", control: "resources: [a, b]\n", files: map[string]string{"a/kustomization.yaml": "namePrefix: a-\ngenerators: [../g.yaml]\n", "b/kustomization.yaml": "namePrefix: b-\ngenerators: [../g.yaml]\n", "g.yaml": plugin, "a/local.txt": "owner-a", "b/local.txt": "owner-b"}, replies: map[string]string{"/file.txt": "contents"}, want: "local.txt: owner-b"},
		{name: "inline-plugin-strategic-data", control: base + "transformers: [p.yaml]\n", files: map[string]string{"p.yaml": "apiVersion: builtin\nkind: PatchStrategicMergeTransformer\nmetadata: {name: patch}\npaths:\n- |\n  " + strings.ReplaceAll(strings.ReplaceAll(patch, "patched", "$URL/data"), "\n", "\n  ")}, want: "marker: $URL/data"},
		{name: "replacement-field-data", control: base + "replacements: [{source: {kind: ConfigMap, name: demo, fieldPath: metadata.name}, targets: [{select: {kind: ConfigMap}, fieldPaths: ['data.[$URL/data]'], options: {create: true}}]}]\n", want: "$URL/data: demo"},
		{name: "unreachable-sibling", control: base, files: map[string]string{"unused/kustomization.yaml": "resources: [$URL/unreachable]\n"}, want: "name: demo"},
		{name: "literal-and-inline-data", control: base + "configMapGenerator: [{name: literal, literals: [website=$URL/data]}]\npatchesStrategicMerge:\n- |\n  apiVersion: v1\n  kind: ConfigMap\n  metadata: {name: demo}\n  data: {marker: $URL/data}\n", want: "website: $URL/data"},
		{name: "unknown-plugin-data", control: base + "generators: [g.yaml]\n", files: map[string]string{"g.yaml": "apiVersion: example.org/v1\nkind: ConfigMapGenerator\nmetadata: {name: external}\nfiles: [$URL/data]\n"}, wantErr: "external plugins disabled"},
		{name: "malformed-plugin", control: base + "generators: [g.yaml]\n", files: map[string]string{"g.yaml": "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: [\n"}, wantErr: "kustomize build "},
		{name: "malformed-remote-plugin", control: base + "generators: [$URL/g.yaml]\n", replies: map[string]string{"/g.yaml": "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: [\n"}, wantErr: "preflight:"},
		{name: "plugin-directory-cycle", control: base + "generators: [plugins]\n", files: map[string]string{"plugins/kustomization.yaml": "resources: [..]\n"}, wantErr: "cycle"},
		{name: "malformed-root-control", subpath: "broken", control: "resources: [\n", wantErr: "flux kustomize generator: error converting YAML to JSON:"},
		{name: "malformed-reachable-control", control: "resources: [child]\n", files: map[string]string{"child/kustomization.yaml": "resources: [\n"}, wantErr: "kustomize build "},
		{name: "cycle", control: "resources: [child]\n", files: map[string]string{"child/kustomization.yaml": "resources: [..]\n"}, wantErr: "cycle"},
		{name: "fetch-failure", control: base + "patches: [{path: $URL/missing}]\n", replies: map[string]string{"/missing": "404"}, wantErr: ".patches[0].path"},
		{name: "aliased-operand", control: base + "configMapGenerator:\n- name: first\n  files: &files [$URL/file.txt]\n- name: second\n  files: *files\n", replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var marked, bypass atomic.Int32
			var url string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Flate-Preflight-Test") == "yes" {
					marked.Add(1)
				} else {
					bypass.Add(1)
				}
				body, ok := tc.replies[r.URL.Path]
				if !ok || body == "404" {
					http.NotFound(w, r)
					return
				}
				_, _ = io.WriteString(w, strings.ReplaceAll(body, "$URL", url))
			}))
			t.Cleanup(srv.Close)
			url = srv.URL
			files := map[string]string{
				filepath.Join(tc.subpath, "kustomization.yaml"): tc.control,
				filepath.Join(tc.subpath, "cm.yaml"):            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {marker: initial}\n",
				filepath.Join(tc.subpath, "local.txt"):          "owner",
			}
			maps.Copy(files, tc.files)
			for name, body := range files {
				files[name] = strings.ReplaceAll(body, "$URL", url)
			}
			root := testutil.WriteTree(t, files)
			cache := newTestTreeCache(t, "")
			if tc.gitFiles != nil {
				gitFiles := make(map[string]string, len(tc.gitFiles))
				for name, body := range tc.gitFiles {
					gitFiles[name] = strings.ReplaceAll(body, "$URL", url)
				}
				withFakeGitBase(t, cache, fakeWorktree(t, gitFiles), nil)
			}
			client := srv.Client()
			transport := client.Transport
			cache.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
				r = r.Clone(r.Context())
				r.Header.Set("X-Flate-Preflight-Test", "yes")
				return transport.RoundTrip(r)
			})}
			spec := demoSpec
			if tc.name == "generated-component" {
				spec = demoSpec.Clone()
				spec.Components = []string{"../component"}
			}
			before := manifest.Fingerprint(spec)
			out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: tc.subpath, Spec: spec}, sourcecache.RetryConfig{})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error=%v, want %q", err, tc.wantErr)
				}
				if tc.name == "plugin-directory-cycle" && !strings.Contains(err.Error(), remoteResourcePrefix) {
					t.Fatalf("cycle diagnostic lost private view: %v", err)
				}
			} else if err != nil || !strings.Contains(string(out), strings.ReplaceAll(tc.want, "$URL", url)) {
				t.Fatalf("render: %s, %v; want %q", out, err, tc.want)
			}
			if strings.Contains(tc.name, "two-owners") && !strings.Contains(string(out), "local.txt: owner-a") {
				t.Fatalf("first owner's relative operand was lost: %s", out)
			}
			if marked.Load() != int32(len(tc.replies)) || bypass.Load() != 0 {
				t.Fatalf("marked=%d want=%d, bypass=%d", marked.Load(), len(tc.replies), bypass.Load())
			}
			if tc.name == "git-looking-file" {
				rec := newReadSet(root)
				view := newTreeFS(localSource(t, root).FS, root, rec)
				if err := preflightRemoteResources(t.Context(), cache, view, root); err != nil {
					t.Fatal(err)
				}
				snap, _, err := decodeFrame(testRenderFrame(t, rec.snapshot(), out))
				if err != nil || len(snap.Remotes) != 1 || snap.Remotes[0].Input.Operation != remoteHTTP {
					t.Fatalf("admitted file operation: %+v, %v", snap, err)
				}
				replay := newTestTreeCache(t, "")
				replay.httpClient = cache.httpClient
				replay.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					t.Error("file replay reclassified the admitted HTTP operation")
					return nil, manifest.ErrInput
				})
				if valid, err := snap.stillValid(t.Context(), replay, view, false); err != nil || !valid {
					t.Fatalf("file frame replay: %t, %v", valid, err)
				}
				if marked.Load() != 2 || bypass.Load() != 0 {
					t.Fatalf("HTTP initial/replay: marked=%d bypass=%d", marked.Load(), bypass.Load())
				}
			}
			if manifest.Fingerprint(spec) != before {
				t.Fatal("stored Flux specification mutated")
			}
			for name, body := range files {
				got, err := os.ReadFile(filepath.Join(root, name))
				if err != nil || string(got) != body {
					t.Fatalf("source mutation %s: %q, %v", name, got, err)
				}
			}
			if matches, _ := filepath.Glob(filepath.Join(root, remoteResourcePrefix+"*")); len(matches) != 0 {
				t.Fatalf("overlay escaped to disk: %v", matches)
			}
		})
	}
}

func TestHttpGetURL_BodyCloseErrors(t *testing.T) {
	t.Parallel()
	readErr, closeErr := errors.New("body read"), errors.New("body close")
	for _, outcome := range []string{"success", "created", "last-success", "status", "read", "limit"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			c := newTestTreeCache(t, "")
			closes := 0
			c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
				var reader io.Reader = strings.NewReader("complete")
				status := http.StatusOK
				switch outcome {
				case "created":
					status = http.StatusCreated
				case "last-success":
					status = 299
				case "status":
					status = http.StatusNotFound
				case "read":
					reader = remoteRead(func([]byte) (int, error) { return 0, readErr })
				case "limit":
					reader = remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil })
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: remoteBody{Reader: reader, close: func() error { closes++; return closeErr }}}, nil
			})
			_, err := c.httpGetURL(t.Context(), "https://example.invalid/body", sourcecache.ProducerInput{Writer: io.Discard})
			if !errors.Is(err, closeErr) || closes != 1 || strings.Count(err.Error(), closeErr.Error()) != 1 {
				t.Fatalf("body cleanup: %v closes=%d", err, closes)
			}
			if outcome == "read" && !errors.Is(err, readErr) || outcome == "status" && fetcherr.Classes(err)&fetcherr.Client == 0 || outcome == "limit" && !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("operation identity: %v", err)
			}
			if outcome == "status" {
				status, ok := errors.AsType[*fetcherr.ResponseError](err)
				if !ok || status.StatusCode != http.StatusNotFound || status.Op != http.MethodGet {
					t.Fatalf("response facts=%+v error=%v", status, err)
				}
			}
			if (outcome == "status" || outcome == "read" || outcome == "limit") && !strings.HasSuffix(err.Error(), "\n"+closeErr.Error()) {
				t.Fatalf("operation must precede cleanup: %v", err)
			}
		})
	}
}

func TestPreflightRemoteResources_RemoteObservations(t *testing.T) {
	t.Run("root-after-nested", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = io.WriteString(w, "remote data")
		}))
		t.Cleanup(srv.Close)
		url := srv.URL + "/repo.git//data"
		const gitURL = "https://example.invalid/owner/repo.git"
		remote := memoryTree(t, map[string]string{"kustomization.yaml": "configMapGenerator: [{name: nested, files: ['chosen=" + url + "']}]\n"})
		c := newTestTreeCache(t, "")
		gitCalls := 0
		c.SetGitBaseFetcher(func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			gitCalls++
			if request.URL != gitURL || request.Ref != "main" {
				t.Fatalf("Git request=%+v", request)
			}
			return &store.SourceArtifact{FS: remote, Key: "family", Revision: "revision"}, nil
		})
		rec := newReadSet("/src")
		input := newTreeFS(memoryTree(t, map[string]string{"kustomization.yaml": "resources: ['" + gitURL + "?ref=main']\nconfigMapGenerator: [{name: root, files: ['chosen=" + url + "', 'other=" + url + "']}]\n"}), "/src", rec)
		err := preflightRemoteResources(t.Context(), c, input, "/src")
		if err != nil {
			t.Fatalf("preflight error=%v", err)
		}
		snap := rec.snapshot()
		if len(snap.Remotes) != 2 || snap.Remotes[0].Input != (remoteInput{Operation: remoteHTTP, URL: url}) || snap.Remotes[1].Input != (remoteInput{Operation: remoteGit, URL: gitURL, Ref: "main"}) {
			t.Fatalf("remote observations=%+v", snap.Remotes)
		}
		wantParents := []remoteInput{{Operation: remoteGit, URL: gitURL, Ref: "main"}}
		if diff := cmp.Diff(wantParents, snap.Remotes[0].Parents); diff != "" || len(snap.Remotes[1].Parents) != 0 {
			t.Fatalf("root sighting erased nested parents (-want +got):\n%s; Git parents=%+v", diff, snap.Remotes[1].Parents)
		}
		frame, err := encodeFrame(snap, nil)
		if err != nil {
			t.Fatal(err)
		}
		decoded, _, err := decodeFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range decoded.Remotes {
			acquired, identity, err := entry.Input.acquire(t.Context(), c, input)
			if err != nil || identity != entry.Identity {
				t.Fatalf("replayed identity=%+v observation=%+v error=%v", identity, entry, err)
			}
			if entry.Input.Operation == remoteHTTP {
				body, err := fs.ReadFile(acquired, "content")
				if err != nil || string(body) != "remote data" {
					t.Fatalf("HTTP body=%q error=%v", body, err)
				}
			}
		}
		if hits.Load() != 1 || gitCalls != 2 {
			t.Fatalf("HTTP requests=%d Git acquisitions=%d", hits.Load(), gitCalls)
		}
	})
	for _, scenario := range []string{"nested-git", "http-plugin", "shared-mount", "shared-plugin", "equal-content-plugins", "same-repository-subpaths"} {
		t.Run(scenario, func(t *testing.T) {
			var mu sync.Mutex
			calls := make(map[string]int)
			record := func(url string) {
				mu.Lock()
				defer mu.Unlock()
				calls[url]++
			}
			var changed atomic.Int32
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(srv.URL + r.URL.Path)
				body := "remote data"
				if strings.HasPrefix(r.URL.Path, "/plugin-") {
					body = "apiVersion: builtin\nkind: ConfigMapGenerator\nmetadata: {name: generated}\nfiles: ['chosen=" + srv.URL + "/a-child']\n"
					if changed.Load() == 2 && r.URL.Path == "/plugin-one" || changed.Load() == 3 && r.URL.Path == "/plugin-two" {
						body += "behavior: merge\n"
					}
				}
				_, _ = io.WriteString(w, body)
			}))
			t.Cleanup(srv.Close)
			a := remoteInput{Operation: remoteGit, URL: "https://example.invalid/a-parent.git", Ref: "main"}
			b := remoteInput{Operation: remoteGit, URL: "https://example.invalid/z-parent.git", Ref: "main"}
			shared := remoteInput{Operation: remoteGit, URL: "https://example.invalid/m-shared.git", Ref: "main"}
			child := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/a-child"}
			plugin := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/plugin-one"}
			otherPlugin := remoteInput{Operation: remoteHTTP, URL: srv.URL + "/plugin-two"}
			leaf := "configMapGenerator: [{name: nested, files: ['chosen=" + child.URL + "']}]\n"
			root := "resources: ['" + a.URL + "//subdir?ref=main']\n"
			repositories := map[string]map[string]string{}
			want := map[remoteInput][]remoteInput{a: nil}
			switch scenario {
			case "nested-git":
				repositories[a.URL] = map[string]string{"subdir/kustomization.yaml": "resources: ['" + b.URL + "//subdir?ref=main']\n"}
				repositories[b.URL] = map[string]string{"subdir/kustomization.yaml": leaf}
				want[b], want[child] = []remoteInput{a}, []remoteInput{a, b}
			case "http-plugin":
				repositories[a.URL] = map[string]string{"subdir/kustomization.yaml": "generators: ['" + plugin.URL + "']\n"}
				want[plugin], want[child] = []remoteInput{a}, []remoteInput{a, plugin}
			case "shared-mount":
				root = "resources: ['" + a.URL + "//subdir?ref=main', '" + b.URL + "//subdir?ref=main']\n"
				for _, parent := range []remoteInput{a, b} {
					repositories[parent.URL] = map[string]string{"subdir/kustomization.yaml": "resources: ['" + shared.URL + "//subdir?ref=main']\n"}
				}
				repositories[shared.URL] = map[string]string{
					"subdir/kustomization.yaml": "resources: [../inner]\n",
					"inner/kustomization.yaml":  leaf,
				}
				want[b], want[shared], want[child] = nil, []remoteInput{a, b}, []remoteInput{a, b, shared}
			case "shared-plugin":
				root = "generators: ['" + a.URL + "//plugins?ref=main', '" + b.URL + "//plugins?ref=main']\n"
				for _, parent := range []remoteInput{a, b} {
					repositories[parent.URL] = map[string]string{"plugins/kustomization.yaml": "resources: ['" + plugin.URL + "']\n"}
				}
				want[b], want[plugin], want[child] = nil, []remoteInput{a, b}, []remoteInput{a, b, plugin}
			case "equal-content-plugins":
				root = "generators: ['" + plugin.URL + "', '" + otherPlugin.URL + "']\n"
				want = map[remoteInput][]remoteInput{plugin: nil, otherPlugin: nil, child: {plugin, otherPlugin}}
			case "same-repository-subpaths":
				repositories[a.URL] = map[string]string{"subdir/kustomization.yaml": "resources: ['" + a.URL + "//leaf?ref=main']\n", "leaf/kustomization.yaml": leaf}
				want[child] = []remoteInput{a}
			}
			newCache := func() *TreeCache {
				c := newTestTreeCache(t, "")
				c.SetGitBaseFetcher(func(_ context.Context, request GitBaseRequest, _ sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					record(request.URL)
					files, ok := repositories[request.URL]
					if !ok || request.Ref != "main" {
						t.Fatalf("Git request=%+v", request)
					}
					revision := "1"
					if changed.Load() == 1 && request.URL == b.URL {
						revision = "2"
					}
					return &store.SourceArtifact{FS: memoryTree(t, files), Key: request.URL, Revision: revision}, nil
				})
				return c
			}
			c := newCache()
			rec := newReadSet("/src")
			input := newTreeFS(memoryTree(t, map[string]string{"kustomization.yaml": root}), "/src", rec)
			if err := preflightRemoteResources(t.Context(), c, input, "/src"); err != nil {
				t.Fatal(err)
			}
			snap := rec.snapshot()
			if len(snap.Remotes) != len(want) {
				t.Fatalf("observations=%+v; want=%+v", snap.Remotes, want)
			}
			for _, entry := range snap.Remotes {
				parents, ok := want[entry.Input]
				slices.SortFunc(parents, compareRemoteInputs)
				if !ok || !cmp.Equal(parents, entry.Parents) {
					t.Fatalf("parents of %+v (-want +got):\n%s", entry.Input, cmp.Diff(parents, entry.Parents))
				}
			}
			mu.Lock()
			for descriptor := range want {
				if calls[descriptor.URL] != 1 {
					t.Errorf("capture acquisitions for %s=%d, want 1", descriptor.URL, calls[descriptor.URL])
				}
			}
			mu.Unlock()
			frame, err := encodeFrame(snap, nil)
			if err != nil {
				t.Fatal(err)
			}
			snap, _, err = decodeFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			closeTestTreeCache(t, c)
			mu.Lock()
			clear(calls)
			mu.Unlock()
			c = newCache()
			if valid, err := snap.stillValid(t.Context(), c, input, true); err != nil || !valid {
				t.Fatalf("unchanged graph: valid=%t error=%v", valid, err)
			}
			mu.Lock()
			for descriptor := range want {
				if calls[descriptor.URL] != 1 {
					t.Errorf("replay acquisitions for %s=%d, want 1", descriptor.URL, calls[descriptor.URL])
				}
			}
			mu.Unlock()
			closeTestTreeCache(t, c)
			var changes []int32
			switch scenario {
			case "shared-mount", "shared-plugin":
				changes = []int32{1}
			case "equal-content-plugins":
				changes = []int32{2, 3}
			}
			for _, change := range changes {
				changed.Store(change)
				mu.Lock()
				clear(calls)
				mu.Unlock()
				c = newCache()
				if valid, err := snap.stillValid(t.Context(), c, input, true); err != nil || valid {
					t.Fatalf("changed parent %d: valid=%t error=%v", change, valid, err)
				}
				mu.Lock()
				if calls[child.URL] != 0 || scenario == "shared-mount" && calls[shared.URL] != 0 {
					t.Errorf("changed parent %d acquired descendants: %v", change, calls)
				}
				mu.Unlock()
				closeTestTreeCache(t, c)
			}
		})
	}
}

func TestHttpGetURL_RequestLifetime(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"headers", "body", "caller-deadline", "previous", "close"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := newTestTreeCache(t, "")
				started := time.Now()
				ctx := t.Context()
				bound := 5 * time.Second
				if stage == "caller-deadline" {
					var cancel context.CancelFunc
					bound = time.Second
					ctx, cancel = context.WithTimeout(ctx, bound)
					defer cancel()
				}
				if stage == "previous" {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				var requestCtx context.Context
				closes := 0
				c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					if stage == "previous" {
						t.Error("request issued after Previous rejected cancellation")
					}
					requestCtx = r.Context()
					deadline, ok := requestCtx.Deadline()
					if !ok || deadline.Sub(started) != bound {
						t.Fatalf("request deadline = %v, present = %t, want %v", deadline.Sub(started), ok, bound)
					}
					if stage == "headers" {
						<-requestCtx.Done()
						return nil, requestCtx.Err()
					}
					var reader io.Reader = remoteRead(func([]byte) (int, error) {
						<-requestCtx.Done()
						return 0, requestCtx.Err()
					})
					if stage == "close" {
						reader = strings.NewReader("complete")
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: remoteBody{Reader: reader, close: func() error {
						closes++
						if stage == "close" && requestCtx.Err() != nil {
							t.Errorf("request context canceled before body close: %v", requestCtx.Err())
						}
						return nil
					}}}, nil
				})
				_, err := c.httpGetURL(ctx, "https://example.invalid/resource", sourcecache.ProducerInput{Writer: io.Discard})
				switch stage {
				case "close":
					if err != nil || closes != 1 || !errors.Is(requestCtx.Err(), context.Canceled) {
						t.Fatalf("completed lifetime: err=%v closes=%d context=%v", err, closes, requestCtx.Err())
					}
				case "previous":
					if !errors.Is(err, context.Canceled) || requestCtx != nil {
						t.Fatalf("Previous cancellation: err=%v request=%v", err, requestCtx)
					}
				default:
					if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) != bound {
						t.Fatalf("request lifetime: err=%v elapsed=%v want=%v", err, time.Since(started), bound)
					}
					if stage != "headers" && closes != 1 {
						t.Fatalf("expired body closes=%d", closes)
					}
				}
			})
		})
	}
	t.Run("network-body-cancellation", func(t *testing.T) {
		t.Parallel()
		entered, canceled := make(chan struct{}), make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthy" {
				_, _ = io.WriteString(w, "healthy")
				return
			}
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(canceled)
		}))
		defer srv.Close()
		var network source.Network
		defer network.Close()
		c := newTestTreeCache(t, "")
		c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			resp, err := network.RoundTrip(r)
			if err != nil || r.URL.Path == "/healthy" {
				return resp, err
			}
			body := resp.Body
			enteredBody := sync.OnceFunc(func() { close(entered) })
			resp.Body = remoteBody{Reader: remoteRead(func(p []byte) (int, error) {
				enteredBody()
				return body.Read(p)
			}), close: body.Close}
			return resp, nil
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := c.httpGetURL(ctx, srv.URL+"/stalled", sourcecache.ProducerInput{Writer: io.Discard})
			done <- err
		}()
		<-entered
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("wrapped Network body cancellation: %v", err)
		}
		<-canceled
		var body strings.Builder
		if _, err := c.httpGetURL(t.Context(), srv.URL+"/healthy", sourcecache.ProducerInput{Writer: &body}); err != nil || body.String() != "healthy" {
			t.Fatalf("Network after caller cancellation: %q %v", body.String(), err)
		}
	})
}
