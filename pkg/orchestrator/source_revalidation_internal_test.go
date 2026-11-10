package orchestrator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

type sourceRevalidationHTTP struct {
	files                      map[string][]byte
	lists, bodies, validations atomic.Int32
}

func (s *sourceRevalidationHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("list-type") {
		s.lists.Add(1)
		fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
		for _, name := range []string{"cm.yaml", "kustomization.yaml"} {
			fmt.Fprintf(
				w,
				"<Contents><Key>%s</Key><Size>%d</Size><ETag>stable</ETag></Contents>",
				name,
				len(s.files["/data/"+name]),
			)
		}
		fmt.Fprint(w, "</ListBucketResult>")
		return
	}
	body, known := s.files[r.URL.Path]
	if !known {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("ETag", `"stable"`)
	if r.Header.Get("If-None-Match") == `"stable"` {
		s.validations.Add(1)
		w.WriteHeader(http.StatusNotModified)
		return
	}
	s.bodies.Add(1)
	http.ServeContent(w, r, r.URL.Path, time.Unix(0, 0), bytes.NewReader(body))
}

//nolint:paralleltest // RenderTrees changes the process-wide egress policy.
func TestRenderTrees_SourceRevalidation(t *testing.T) {
	for _, protocol := range []string{"bucket", "helm"} {
		t.Run(protocol, func(t *testing.T) {
			testSourceRevalidation(t, protocol)
		})
	}
}

func testSourceRevalidation(t *testing.T, protocol string) {
	t.Helper()

	remote := sourceRevalidationFixture(t, protocol)
	server := httptest.NewServer(remote)
	t.Cleanup(server.Close)
	tree, parents := sourceRevalidationTree(t, protocol, server.URL)
	var expected []byte
	for independent := range 2 {
		owner := cachetest.New(t, "", sourcecache.New)
		for _, regime := range []string{"initial", "stale-unchanged"} {
			before := remote.bodies.Load()
			lists, validations := remote.lists.Load(), remote.validations.Load()
			runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{Cache: owner, Concurrency: 2})
			assert.NoError(t, err)
			assert.Equal(t, len(runs), 1)
			result := runs[0].Result
			assert.Equal(t, len(result.Failed), 0)
			assert.Equal(t, len(result.Blocked), 0)
			assert.Equal(t, len(result.Warnings), 0)
			output := outputByParent(result.Manifests)
			for _, parent := range parents {
				assert.Equal(t, len(output[parent]), 1)
				assert.Diff(t, output[parent][0]["data"], map[string]any{"payload": "stable"})
			}
			body, err := json.Marshal(result.Manifests)
			assert.NoError(t, err)
			if expected == nil {
				expected = body
			}
			assert.Equal(t, string(body), string(expected))
			if regime == "initial" {
				assert.Equal(t, remote.bodies.Load()-before, int32(2))
			} else {
				assert.Equal(t, remote.bodies.Load()-before, int32(0))
				if protocol == "bucket" {
					assert.Equal(t, remote.lists.Load()-lists, int32(1))
				} else {
					assert.Equal(t, remote.validations.Load()-validations >= 2, true)
				}
			}
			t.Logf("cache=%d regime=%s complete-output-bytes=%d", independent, regime, len(body))
		}
	}
}

func sourceRevalidationFixture(t *testing.T, protocol string) *sourceRevalidationHTTP {
	t.Helper()
	if protocol == "bucket" {
		return &sourceRevalidationHTTP{files: map[string][]byte{
			"/data/kustomization.yaml": []byte("resources: [cm.yaml]\n"),
			"/data/cm.yaml": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n" +
				"data: {payload: stable}\n"),
		}}
	}
	chart := testutil.TarGz(t, map[string]string{
		"demo/Chart.yaml": "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
		"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\n" +
			"metadata: {name: {{ .Release.Name }}, namespace: {{ .Release.Namespace }}}\n" +
			"data: {payload: stable}\n",
	}, 0o644, gzip.DefaultCompression)
	index := fmt.Sprintf(
		"apiVersion: v1\nentries:\n  demo:\n    - name: demo\n"+
			"      version: 1.0.0\n      digest: %s\n      urls: [demo.tgz]\n",
		digest.FromBytes(chart).Encoded(),
	)
	return &sourceRevalidationHTTP{files: map[string][]byte{"/index.yaml": []byte(index), "/demo.tgz": chart}}
}

func sourceRevalidationTree(t *testing.T, protocol, url string) (Tree, []manifest.NamedResource) {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [source.yaml, first.yaml, second.yaml]\n")
	source := fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\n"+
		"metadata: {name: remote, namespace: apps}\nspec: {url: %s, interval: 0s}\n", url)
	if protocol == "bucket" {
		source = fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: Bucket\n"+
			"metadata: {name: remote, namespace: apps}\n"+
			"spec: {bucketName: data, endpoint: %s, insecure: true, region: us-east-1, interval: 0s}\n", url)
	}
	testutil.WriteFile(t, root, "source.yaml", source)
	parents := make([]manifest.NamedResource, 0, 2)
	for _, name := range []string{"first", "second"} {
		body := fmt.Sprintf("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\n"+
			"metadata: {name: %[1]s, namespace: apps}\nspec:\n  interval: 0s\n  releaseName: %[1]s\n"+
			"  chart:\n    spec:\n      chart: demo\n      version: 1.0.0\n      interval: 0s\n"+
			"      sourceRef: {kind: HelmRepository, name: remote}\n", name)
		id := testutil.HelmReleaseID("apps", name)
		if protocol == "bucket" {
			body = testutil.KustomizationYAML(name, "apps", "  interval: 0s\n  path: ./\n"+
				"  targetNamespace: "+name+"\n  sourceRef: {kind: Bucket, name: remote}\n")
			id = testutil.KustomizationID("apps", name)
		}
		testutil.WriteFile(t, root, name+".yaml", body)
		parents = append(parents, id)
	}
	return Tree{ID: manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, Path: root, RepoRoot: root}, parents
}
