package kustomize

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

// memFSWith uses the same private filesystem boundary as a render.
func memFSWith(t *testing.T, files map[string]string) filesys.FileSystem {
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
	if _, err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
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
	_, err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
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
	body := "resources:\n  - ./local.yaml\n  - ../shared/cm.yaml\n"
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": body})
	if _, err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
	}
	got, _ := fsys.ReadFile("kustomization.yaml")
	if string(got) != body {
		t.Errorf("local-only kustomization was modified:\nwant %q\ngot  %q", body, got)
	}
}

// TestPreflightRemoteResources_WalksNestedKustomizations covers the recursive
// case: a Components / overlay layout where the URL resource hides inside a
// subdir's kustomization.yaml.
func TestPreflightRemoteResources_WalksNestedKustomizations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: nested}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{
		"kustomization.yaml":              "resources:\n  - ./components/x\n",
		"components/x/kustomization.yaml": "resources:\n  - " + srv.URL + "/nested.yaml\n",
	})
	if _, err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
		t.Fatalf("preflight: %v", err)
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
	src := writeTree(t, map[string]string{
		"kustomization.yaml":       "resources:\n  - ./child\n",
		"child/kustomization.yaml": child,
	})

	_, err := RenderFlux(context.Background(), newTestTreeCache(t, ""), RenderRequest{Source: localSource(t, src), Label: src, ApplyIgnore: false, SubPath: ".", Spec: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
		"spec":       map[string]any{"path": "."},
	}}, sourcecache.RetryConfig{})
	if err != nil {
		t.Fatalf("RenderFlux: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(src, "child", "kustomization.yaml")) //nolint:gosec // src is t.TempDir
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
			if _, err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, "."); err != nil {
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
	if _, err := preflightRemoteResources(context.Background(), cache, fsys, "."); err != nil {
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
	if _, err := preflightRemoteResources(context.Background(), cache, fsys2, "."); err != nil {
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
	c := newTestTreeCache(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		body := remoteBody{Reader: remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), close: func() error { return nil }}
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})
	_, err := c.httpGetURL(t.Context(), "https://example.invalid/big.yaml", io.Discard)
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
	if _, err := preflightRemoteResources(t.Context(), newTestTreeCache(t, ""), fsys, "."); err != nil {
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
	c := newTestTreeCache(t, "")
	c.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		reader := io.LimitReader(remoteRead(func(p []byte) (int, error) { clear(p); return len(p), nil }), remoteFetchMaxBytes)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(reader), Header: make(http.Header)}, nil
	})
	desc, err := c.httpGetURL(t.Context(), "https://example.invalid/atcap.yaml", io.Discard)
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
			root := writeTree(t, map[string]string{"kustomization.yaml": existingKustomization(url)})
			cache := newTestTreeCache(t, "")
			cache.httpClient = srv.Client()
			out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoRawSpec}, sourcecache.RetryConfig{})
			if err != nil || !strings.Contains(string(out), "name: remote") {
				t.Fatalf("render: %s, %v", out, err)
			}
			if _, err := cache.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}); err != nil {
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
		{name: "plugin-inline", control: base + "generators:\n- |\n" + "  " + strings.ReplaceAll(plugin, "\n", "\n  "), replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "plugin-directory-owner", control: base + "generators: [plugins]\n", files: map[string]string{"plugins/kustomization.yaml": "resources: [conf/g.yaml]\n", "plugins/conf/g.yaml": plugin}, replies: map[string]string{"/file.txt": "contents"}, want: "local.txt: owner"},
		{name: "plugin-multidoc", control: base + "generators: [$URL/g.yaml]\n", replies: map[string]string{"/g.yaml": plugin + "---\napiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: other}\nenvs: [$URL/env]\n", "/file.txt": "contents", "/env": "marker=secret\n"}, want: "marker: c2VjcmV0"},
		{name: "plugin-secret-files", control: base + "generators: [g.yaml]\n", files: map[string]string{"g.yaml": "apiVersion: builtin\nkind: SecretGenerator\nmetadata: {name: generated}\nfiles: [$URL/file.txt]\n"}, replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: Y29udGVudHM="},
		{name: "plugin-case-insensitive-files", control: base + "generators: [g.yaml]\n", files: map[string]string{"g.yaml": strings.ReplaceAll(plugin, "files:", "Files:")}, replies: map[string]string{"/file.txt": "contents"}, want: "file.txt: contents"},
		{name: "plugin-local-directory", control: base + "generators: [plugins]\n", files: map[string]string{"plugins/kustomization.yaml": "resources: [g.yaml]\n", "plugins/g.yaml": strings.ReplaceAll(plugin, ", $URL/file.txt", "")}, want: "local.txt: owner"},
		{name: "plugin-directory-two-owners", control: "resources: [a, b]\n", files: map[string]string{"a/kustomization.yaml": "namePrefix: a-\ngenerators: [../plugins]\n", "b/kustomization.yaml": "namePrefix: b-\ngenerators: [../plugins]\n", "plugins/kustomization.yaml": "resources: [g.yaml]\n", "plugins/g.yaml": plugin, "a/local.txt": "owner-a", "b/local.txt": "owner-b"}, replies: map[string]string{"/file.txt": "contents"}, want: "local.txt: owner-b"},
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
		{name: "disabled-helm-data", control: base + "helmCharts: [{name: chart, valuesFile: $URL/data}]\n", wantErr: "must specify --enable-helm"},
		{name: "invalid-key-grammar", control: base + "configMapGenerator: [{name: invalid, files: ['chosen=$URL/file?q=v']}]\n", wantErr: "contains '='"},
		{name: "invalid-empty-key", control: base + "configMapGenerator: [{name: invalid, files: ['=$URL/file']}]\n", wantErr: "missing key name"},
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
			root := writeTree(t, files)
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
			spec := demoRawSpec
			if tc.name == "generated-component" {
				spec = map[string]any{"spec": map[string]any{"components": []any{"../component"}}}
			}
			before := manifest.Fingerprint(spec)
			out, err := RenderFlux(t.Context(), cache, RenderRequest{Source: localSource(t, root), Label: root, SubPath: tc.subpath, Spec: spec}, sourcecache.RetryConfig{})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error=%v, want %q", err, tc.wantErr)
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
	readErr, closeErr := errors.New("body read"), errors.New("body close")
	for _, outcome := range []string{"success", "created", "last-success", "status", "read", "limit"} {
		t.Run(outcome, func(t *testing.T) {
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
			_, err := c.httpGetURL(t.Context(), "https://example.invalid/body", io.Discard)
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
