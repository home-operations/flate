package orchestrator

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/bucket"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type sourceRevalidationHTTP struct {
	files                      map[string][]byte
	lists, bodies, validations atomic.Int32
}

func (s *sourceRevalidationHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("list-type") {
		s.lists.Add(1)
		fmt.Fprint(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
		for _, path := range slices.Sorted(maps.Keys(s.files)) {
			body := s.files[path]
			fmt.Fprintf(
				w,
				"<Contents><Key>%s</Key><Size>%d</Size><ETag>%s</ETag></Contents>",
				strings.TrimPrefix(path, "/data/"),
				len(body),
				digest.FromBytes(body).Encoded(),
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
	etag := `"stable"`
	if strings.HasPrefix(r.URL.Path, "/data/") {
		etag = `"` + digest.FromBytes(body).Encoded() + `"`
	}
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
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
	declaration := fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\n"+
		"metadata: {name: remote, namespace: apps}\nspec: {url: %s, interval: 0s}\n", url)
	if protocol == "bucket" {
		declaration = fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: Bucket\n"+
			"metadata: {name: remote, namespace: apps}\n"+
			"spec: {bucketName: data, endpoint: %s, insecure: true, region: us-east-1, interval: 0s}\n", url)
	}
	testutil.WriteFile(t, root, "source.yaml", declaration)
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

//nolint:paralleltest // RenderTrees changes the process-wide egress policy.
func TestRenderTrees_SourceRevalidation_ChangedOnly(t *testing.T) {
	snapshots := sourceRevalidationSnapshots(t)
	for _, direction := range []string{"addition", "removal"} {
		t.Run(direction, func(t *testing.T) {
			testSourceRevalidationChangedOnly(t, snapshots, direction)
		})
	}
}

func testSourceRevalidationChangedOnly(t *testing.T, snapshots []Tree, direction string) {
	t.Helper()
	parents := []manifest.NamedResource{
		testutil.KustomizationID("apps", "first"),
		testutil.KustomizationID("apps", "second"),
	}
	stable := testutil.KustomizationID("apps", "stable")
	trees := slices.Clone(snapshots)
	counts := []int{1, 2}
	status := diff.StatusAdded
	if direction == "removal" {
		slices.Reverse(trees)
		slices.Reverse(counts)
		status = diff.StatusRemoved
	}
	full := make([]Rendered, len(trees))
	owner := cachetest.New(t, "", sourcecache.New)
	cfg := Config{Cache: owner, Concurrency: 2, RecordWork: true}
	for side := range trees {
		trees[side].ID.Side = manifest.TreeSide(side)
		runs, err := renderTreesForTest(t.Context(), t, trees[side:side+1], cfg)
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 1)
		full[side] = runs[0]
		assert.Equal(t, full[side].selection == nil, true)
		assert.Equal(t, len(outputByParent(full[side].Result.Manifests)[stable]), 1)
		assert.Equal(t, workCounts(full[side].Result.Work, stable, manifest.WorkKustomize),
			manifest.WorkCounts{Attempts: 1, Completed: 1})
		for _, parent := range parents {
			assert.Equal(t, len(outputByParent(full[side].Result.Manifests)[parent]), counts[side])
		}
	}
	wantSelected, wantDiff := sourceRevalidationProjection(t, full, parents)
	var output struct {
		Changes []diff.Change `json:"changes"`
	}
	assert.NoError(t, json.Unmarshal(wantDiff, &output))
	assert.Equal(t, len(output.Changes), 2)
	for i := range output.Changes {
		change := &output.Changes[i]
		assert.Equal(t, change.Kind, manifest.KindConfigMap)
		assert.Equal(t, change.Name, "added")
		assert.Equal(t, change.Status, status)
	}
	for repeat := range 2 {
		runs, err := renderTreesForTest(t.Context(), t, trees, cfg)
		assert.NoError(t, err)
		assert.Equal(t, len(runs), 2)
		for side, run := range runs {
			assert.Equal(t, run.selection != nil, true)
			assert.Equal(t, len(outputByParent(run.Result.Manifests)[stable]), 0)
			assert.Equal(t, workCounts(run.Result.Work, stable, manifest.WorkKustomize), manifest.WorkCounts{})
			assert.Diff(t, visibleWitnessOutput(run.Result), witnessOutput(full[side].Result, parents))
			for _, parent := range parents {
				assert.Equal(t, run.selection.Snapshot().Reconcile(parent), true)
			}
		}
		selected, body := sourceRevalidationProjection(t, runs, parents)
		assert.Equal(t, selected, wantSelected)
		assert.Equal(t, string(body), string(wantDiff))
		t.Logf("direction=%s run=%d cache=acquired-immutable-snapshots concurrency=2 selected-bytes=%d diff-bytes=%d",
			direction, repeat, len(selected), len(body))
	}
}

func sourceRevalidationSnapshots(t *testing.T) []Tree {
	t.Helper()
	remote := sourceRevalidationFixture(t, "bucket")
	remote.files["/data/flux/kustomization.yaml"] = []byte("resources: [first.yaml, second.yaml, stable.yaml]\n")
	for _, name := range []string{"first", "second", "stable"} {
		path := "./"
		if name == "stable" {
			path = "./stable"
		}
		body := testutil.KustomizationYAML(name, "apps",
			"  interval: 0s\n  path: "+path+"\n  targetNamespace: "+name+
				"\n  sourceRef: {kind: GitRepository, name: remote}\n")
		remote.files["/data/flux/"+name+".yaml"] = []byte(body)
	}
	remote.files["/data/stable/kustomization.yaml"] = []byte("resources: [cm.yaml]\n")
	remote.files["/data/stable/cm.yaml"] = []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unchanged}\n")
	server := httptest.NewServer(remote)
	t.Cleanup(server.Close)
	owner := cachetest.New(t, "", sourcecache.New)
	network := new(source.Network)
	t.Cleanup(network.Close)
	fetcher := &bucket.Fetcher{Cache: owner, Network: network}
	object := &manifest.Bucket{Name: "remote", Namespace: "apps"}
	object.BucketName, object.Endpoint = "data", server.URL
	object.Insecure, object.Region = true, "us-east-1"
	st := store.New()
	st.AddObject(object)
	var reads store.ReadSet
	reads.Init(st, object.Named(), nil)
	defer reads.Close()
	trees := make([]Tree, 0, 2)
	for side := range 2 {
		if side == 1 {
			remote.files["/data/kustomization.yaml"] = []byte("resources: [cm.yaml, added.yaml]\n")
			remote.files["/data/added.yaml"] = []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: added}\n")
		}
		before := remote.bodies.Load()
		artifact, err := fetchtest.Fetch(t.Context(), fetcher.Fetch, object, &reads, &sourcecache.Policy{}, nil)
		assert.NoError(t, err)
		assert.Equal(t, remote.bodies.Load()-before, int32(len(remote.files)))
		root := t.TempDir()
		artifact.LocalPath, artifact.LocalRoot = root, root
		trees = append(trees, Tree{
			ID:    manifest.TreeID{Cluster: "cluster", Side: manifest.TreeSide(side)},
			Input: artifact, RepoRoot: root, Path: filepath.Join(root, "flux"),
		})
		t.Logf("snapshot=%d cache=%s revision=%s", side,
			[]string{"cold", "stale-changed"}[side], artifact.Revision)
	}
	assert.Equal(t, trees[0].Input.Revision != trees[1].Input.Revision, true)
	for side, tree := range trees {
		body, err := fs.ReadFile(tree.Input.FS, "kustomization.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(body), []string{"resources: [cm.yaml]\n", "resources: [cm.yaml, added.yaml]\n"}[side])
	}
	return trees
}

//nolint:nonamedreturns // Result names distinguish consumer bytes from diff bytes.
func sourceRevalidationProjection(
	t *testing.T, runs []Rendered, parents []manifest.NamedResource,
) (consumerBytes string, diffBytes []byte) {
	t.Helper()
	for _, run := range runs {
		assert.NoError(t, run.Err)
		assert.Equal(t, len(run.Result.Failed), 0)
		assert.Equal(t, len(run.Result.Blocked), 0)
		assert.Equal(t, len(run.Result.Warnings), 0)
	}
	selected, projectionErrors := Select(runs, nil, []manifest.NamedResource{testutil.KustomizationID("apps", "")}, nil)
	assert.NoError(t, projectionErrors.Selected)
	assert.NoError(t, projectionErrors.Remaining)
	manifests := make([][]manifest.DocumentBatch, 0, len(selected))
	snapshots := make([]diff.Snapshot, 0, len(selected))
	for _, result := range selected {
		var batches []manifest.DocumentBatch
		for _, batch := range result.Manifests {
			if slices.Contains(parents, batch.Parent.Named()) {
				batches = append(batches, batch)
			}
		}
		manifests = append(manifests, batches)
		snapshots = append(snapshots, diff.Snapshot{
			Subjects: result.Subjects, Manifests: result.Manifests,
			ProducerKeys: result.ProducerKeys, Unavailable: result.Unavailable,
		})
	}
	body, err := json.Marshal(manifests)
	assert.NoError(t, err)
	changes, warnings, _, err := diff.Compare(snapshots[0], snapshots[1], nil, diff.Options{Format: diff.FormatJSON})
	assert.NoError(t, err)
	assert.Equal(t, len(warnings), 0)
	return string(body), changes
}
