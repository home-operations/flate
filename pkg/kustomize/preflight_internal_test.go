package kustomize

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

// memFSWith uses the same private filesystem boundary as a render.
func memFSWith(t *testing.T, files map[string]string) *treeFS {
	t.Helper()
	fsys := newTreeFS(os.DirFS(t.TempDir()), "/", nil)
	for p, body := range files {
		{
			err := fsys.WriteFile(p, []byte(body))
			assert.FatalIf(t, err != nil, "write %s: %v", p, err)
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
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/foo.yaml\n"})
	{
		err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}

	body, err := fsys.ReadFile("kustomization.yaml")
	assert.NoError(t, err)
	assert.ErrorIf(t, strings.Contains(string(body), srv.URL), "URL still present after preflight:\n%s", body)
	assert.ErrorIf(
		t,
		!strings.Contains(string(body), remoteResourcePrefix),
		"expected rewritten path with %s prefix:\n%s",
		remoteResourcePrefix,
		body,
	)

	fetched := remoteFiles(t, fsys, ".")
	assert.FatalIf(t, len(fetched) != 1, "expected one fetched file, got %v", fetched)
	cached, err := fsys.ReadFile(fetched[0])
	assert.NoError(t, err)
	assert.ErrorIf(t, !strings.Contains(string(cached), "kind: ConfigMap"), "fetched body lost: %q", cached)
}

func TestPreflightRemoteResources_PropagatesFailure(t *testing.T) {
	t.Parallel()
	for _, requiredFile := range []bool{false, true} {
		t.Run(fmt.Sprintf("required-file=%t", requiredFile), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			}))
			t.Cleanup(srv.Close)
			url := srv.URL + "/missing.yaml"
			body := "resources: ['" + url + "']\n"
			if requiredFile {
				body = "configMapGenerator: [{name: required, files: ['chosen=" + url + "']}]\n"
			}
			fsys := memFSWith(t, map[string]string{"kustomization.yaml": body})
			fsys.rec = newReadSet("/")
			err := preflightRemoteResources(t.Context(), newTestTreeCache(t, ""), fsys, ".")
			assert.FatalIf(
				t,
				err == nil || !strings.Contains(err.Error(), url) || !strings.Contains(err.Error(), "HTTP 404"),
				"404 diagnostics=%v",
				err,
			)
			if requiredFile {
				response, ok := errors.AsType[*fetcherr.ResponseError](err)
				assert.FatalIf(
					t,
					!ok || response.StatusCode != http.StatusNotFound,
					"required file lost acquisition error: %v",
					err,
				)
			}
			observation := fsys.rec.remotes[remoteInput{Operation: remoteHTTP, URL: url}]
			assert.FatalIf(
				t,
				observation.identity != (remoteIdentity{HTTPStatus: http.StatusNotFound}),
				"failed input observation=%+v",
				observation.identity,
			)
		})
	}
}

// TestPreflightRemoteResources_IgnoresLocalEntries guards the no-op path: a
// kustomization with only local resources must be untouched.
func TestPreflightRemoteResources_IgnoresLocalEntries(t *testing.T) {
	t.Parallel()
	body := "resources:\n  - ./local.yaml\n  - ../shared/cm.yaml\n"
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": body})
	{
		err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}
	got, _ := fsys.ReadFile("kustomization.yaml")
	assert.ErrorIf(t, string(got) != body, "local-only kustomization was modified:\nwant %q\ngot  %q", body, got)
}

func TestPreflightRemoteResources_WalksNestedKustomizations(t *testing.T) {
	t.Parallel()
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
	assert.FatalIf(t, err != nil, "preflight: %v", err)
	{
		body, _ := fsys.ReadFile("kustomization.yaml")
		assert.FatalIf(t, string(body) != parent, "unchanged parent operand was rewritten: %q", body)
	}
	body, _ := fsys.ReadFile("components/x/kustomization.yaml")
	assert.ErrorIf(t, strings.Contains(string(body), srv.URL), "nested URL not rewritten:\n%s", body)
}

// TestRender_RemotePreflightDoesNotMutateSource proves the in-memory render
// never touches the user's working tree: after a render that pre-fetches a
// remote resource, the on-disk source kustomization is byte-for-byte unchanged
// and no fetched files were written to disk.
func TestRender_RemotePreflightDoesNotMutateSource(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: remote}\n"))
	}))
	t.Cleanup(srv.Close)

	child := "resources:\n  - " + srv.URL + "/remote.yaml\n"
	src := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml":       "resources:\n  - ./child\n",
		"child/kustomization.yaml": child,
	})

	_, _, err := testutil.RenderMaps(
		Render(
			context.Background(),
			newTestTreeCache(t, ""),
			RenderRequest{
				Source:      localSource(t, src),
				Label:       src,
				ApplyIgnore: false,
				SubPath:     ".",
				Spec: parsedKS(t, map[string]any{
					"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
					"kind":       "Kustomization",
					"metadata":   map[string]any{"name": "apps", "namespace": "flux-system"},
					"spec":       map[string]any{"path": "."},
				}),
			},
			sourcecache.RetryConfig{},
		),
	)
	assert.FatalIf(t, err != nil, "Render: %v", err)

	got, err := os.ReadFile(filepath.Join(src, "child", "kustomization.yaml"))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		string(got) != child,
		"source nested kustomization was mutated:\nwant %q\ngot  %q",
		child,
		got,
	)
	matches, _ := filepath.Glob(filepath.Join(src, "child", remoteResourcePrefix+"*.yaml"))
	assert.FatalIf(t, len(matches) != 0, "remote preflight wrote fetched files into source tree: %v", matches)
}

// TestPreflightRemoteResources_HonorsAlternateFilenames sanity-checks the
// filename matcher: kustomize accepts kustomization.yml and Kustomization in
// addition to kustomization.yaml.
func TestPreflightRemoteResources_HonorsAlternateFilenames(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("apiVersion: v1\nkind: ConfigMap\n"))
	}))
	t.Cleanup(srv.Close)

	for _, name := range []string{"kustomization.yml", "Kustomization"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fsys := memFSWith(t, map[string]string{name: "resources:\n  - " + srv.URL + "/x.yaml\n"})
			{
				err := preflightRemoteResources(context.Background(), newTestTreeCache(t, ""), fsys, ".")
				assert.FatalIf(t, err != nil, "preflight: %v", err)
			}
			body, _ := fsys.ReadFile(name)
			assert.ErrorIf(t, strings.Contains(string(body), srv.URL), "%s URL not rewritten:\n%s", name, body)
		})
	}
}

// TestPreflightRemoteResources_FetchesEachURLOnce pins the dedup contract: a
// URL referenced from N kustomization files (parent + nested child + sibling
// overlay …) must hit the network exactly once per run — the fix for the
// m00nwtchr report where the same broken URL emitted 6 WARNs per `flate test
// all`.
func TestPreflightRemoteResources_FetchesEachURLOnce(t *testing.T) {
	t.Parallel()
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
	{
		err := preflightRemoteResources(context.Background(), cache, fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}
	assert.ErrorIf(t, hits != 1, "expected 1 network call, got %d", hits)
	{
		got := remoteFiles(t, fsys, ".")
		assert.ErrorIf(t, len(got) != 1, "expected one immutable shared overlay file, got %v", got)
	}

	// A second preflight against the same cache (production runs one preflight
	// per Render) must NOT re-fetch.
	fsys2 := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/shared.yaml\n"})
	{
		err := preflightRemoteResources(context.Background(), cache, fsys2, ".")
		assert.FatalIf(t, err != nil, "second preflight: %v", err)
	}
	assert.ErrorIf(t, hits != 1, "expected 1 network call across both runs, got %d", hits)
}

func TestPreflightRemoteResources_PreservesCommentsAndOrder(t *testing.T) {
	t.Parallel()
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
	{
		err := preflightRemoteResources(t.Context(), newTestTreeCache(t, ""), fsys, ".")
		assert.FatalIf(t, err != nil, "preflight: %v", err)
	}

	got, err := fsys.ReadFile("kustomization.yaml")
	assert.NoError(t, err)
	out := string(got)

	for _, expect := range []string{
		"# Owner comment that MUST survive.",
		"# why we have this list",
		"# remote shared fragment",
		"# keep this comment",
	} {
		assert.ErrorIf(
			t,
			!strings.Contains(out, expect),
			"comment dropped on rewrite: %q\nfile is:\n%s",
			expect,
			out,
		)
	}
	assert.ErrorIf(t, !strings.Contains(out, "- ./local.yaml"), "local resource path mangled:\n%s", out)
	assert.ErrorIf(
		t,
		!strings.Contains(out, remoteResourcePrefix),
		"URL entry not rewritten to local file:\n%s",
		out,
	)
	assert.ErrorIf(t, strings.Contains(out, srv.URL), "URL still present after rewrite:\n%s", out)
	{
		idxRes, idxNs := strings.Index(
			out,
			"\nresources:",
		), strings.Index(
			out,
			"\nnamespace:",
		)
		assert.ErrorIf(t, idxRes < 0 || idxNs < 0 ||
			idxRes >= idxNs, "key ordering not preserved (resources should precede namespace):\n%s", out)
	}
}

// TestHttpGetURL_AcceptsBodyAtCap is the boundary check: a body exactly at the
// cap must succeed. Guards against off-by-one in the LimitReader+overflow
// pattern.

//nolint:paralleltest // test replaces process-wide dependency configuration.
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
			//nolint:gocritic // The replacement scheme must retain the URL colon.
			url := scheme + srv.URL[strings.Index(srv.URL, ":"):] + "/cm.yaml"
			root := testutil.WriteTree(t, map[string]string{"kustomization.yaml": existingKustomization(url)})
			cache := newTestTreeCache(t, "")
			original := http.DefaultTransport
			http.DefaultTransport = srv.Client().Transport //nolint:reassign // The serial fixture must trust its TLS server.
			t.Cleanup(func() {
				http.DefaultTransport = original //nolint:reassign // Restore the process transport after the fixture.
			})
			out, _, err := testutil.RenderMaps(
				Render(
					t.Context(),
					cache,
					RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec},
					sourcecache.RetryConfig{},
				),
			)
			assert.FatalIf(t, err != nil || !hasRenderField(out, "name", "remote"), "render: %s, %v", out, err)
			{
				_, err := cache.remoteInputs.Acquire(t.Context(), url, sourcecache.RetryConfig{}, nil)
				assert.NoError(t, err)
			}
			{
				got := hits.Load()
				assert.FatalIf(t, got != 1, "server hits = %d, want 1 shared preflight fetch", got)
			}
		})
	}
}
