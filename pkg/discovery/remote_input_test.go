package discovery_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRun_RemoteInputFailure(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); fmt.Fprint(w, "VALUE=remote\n") }))
	defer server.Close()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"))
	testutil.WriteFile(t, root, "orphan.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: orphan, namespace: apps}\ndata: {VALUE: orphan}\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", fmt.Sprintf("configMapGenerator:\n- name: remote\n  envs: [%s]\n", server.URL))
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{Path: root, RepoRoot: root, Store: st})
	if !errors.Is(err, manifest.ErrInput) || result != nil {
		t.Fatalf("fatal finalization published success: result=%+v err=%v", result, err)
	}
	assert.Equal(t, requests.Load(), int32(0))
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "remote"}
	assert.Equal(t, len(st.Productions(id, nil)), 0)
	id.Name = "orphan"
	if st.GetObject(id) != nil {
		t.Fatal("fatal finalization promoted an orphan")
	}
}

func TestRun_BuiltRemoteGenerator(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, "selected-remote-value")
	}))
	defer server.Close()
	owner := cachetest.New(t, "", cache.New)
	network := new(source.Network)
	t.Cleanup(network.Close)
	tasks := task.NewBounded(2)
	inputs := source.NewHTTPInputs(owner, network, tasks)
	t.Cleanup(inputs.Close)
	trees := kustomize.NewTreeCache(owner, &inputs, tasks)
	t.Cleanup(trees.Close)
	const root = "/virtual/remote-generator"
	input := fstest.MapFS{"kustomization.yaml": {Data: []byte(fmt.Sprintf("configMapGenerator: [{name: generated, files: [chosen=%s]}]\n", server.URL))}}
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{Input: &store.SourceArtifact{LocalPath: root, FS: input}, Path: root, Store: st, Trees: trees, RemoteInputs: &inputs})
	if err != nil {
		t.Fatal(err)
	}
	var id manifest.NamedResource
	for candidate, record := range result.Index.Records {
		if strings.HasPrefix(candidate.Name, "generated-") && record.Parent == result.Binding.Producer {
			id = candidate
		}
	}
	if id.Name == "" || st.GetObject(id) != nil {
		t.Fatal("prepared generator missing ownership or published early")
	}
	if _, raw := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated"}); raw {
		t.Fatal("raw generator declaration resurrected")
	}
	assert.Equal(t, requests.Load(), int32(1))
	record, _ := result.Index.Lookup(id)
	if !slices.Contains(record.Inputs, "kustomization.yaml") || len(result.Index.Readers) == 0 {
		t.Fatalf("remote generator lost declaration input or acquired render ownership: %+v", record)
	}
}

func TestRun_MarkerlessRemoteOwnership(t *testing.T) {
	t.Parallel()
	var probes, acquisitions atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	owner := cachetest.New(t, "", cache.New)
	network := new(source.Network)
	t.Cleanup(network.Close)
	tasks := task.NewBounded(2)
	inputs := source.NewHTTPInputs(owner, network, tasks)
	t.Cleanup(inputs.Close)
	trees := kustomize.NewTreeCache(owner, &inputs, tasks)
	t.Cleanup(trees.Close)
	trees.SetGitBaseFetcher(func(_ context.Context, request kustomize.GitBaseRequest, _ cache.RetryConfig) (*store.SourceArtifact, error) {
		acquisitions.Add(1)
		if request.URL != server.URL+"/org/repo" || request.Ref != "" || request.Timeout != 27*time.Second || !request.Submodules {
			return nil, fmt.Errorf("remote request lost upstream defaults: %+v", request)
		}
		return &store.SourceArtifact{Key: "remote-ownership", Content: "remote-ownership-generation", Revision: "fixed", FS: fstest.MapFS{
			"base/kustomization.yaml": {Data: []byte("resources: [bundle.yaml]\n")},
			"base/bundle.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: admitted, namespace: apps}\ndata: {value: remote}\n")},
		}}, nil
	})
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [owner.yaml, "+server.URL+"/org/repo/base]\n")
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "namespace: apps\nresources: [release.yaml, repository.yaml]\n")
	testutil.WriteFile(t, root, "apps/release.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: remote}\nspec: {chart: {spec: {chart: demo, sourceRef: {kind: HelmRepository, name: remote}}}}\n")
	testutil.WriteFile(t, root, "apps/repository.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: {name: remote}\nspec: {url: https://fixture.invalid/charts}\n")
	testutil.WriteFile(t, root, "unowned/sibling.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: unowned, namespace: apps}\n")
	st := store.New()
	result, err := discovery.Run(t.Context(), discovery.Config{Trees: trees, Path: root + "/flux", RepoRoot: root, Store: st, RemoteInputs: &inputs})
	assert.NoError(t, err)
	release := testutil.HelmReleaseID("apps", "remote")
	if _, indexed := result.Index.Lookup(release); !indexed || st.GetObject(release) != nil {
		t.Fatalf("owned remote declaration lost index-only state: indexed=%v object=%v", indexed, st.GetObject(release))
	}
	repository := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: "apps", Name: "remote"}
	if _, indexed := result.Index.Lookup(repository); !indexed || st.GetObject(repository) != nil {
		t.Fatalf("owned source lost index-only scope: indexed=%v object=%v", indexed, st.GetObject(repository))
	}
	alias := st.GetArtifact(manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "flux-system"}, store.ArtifactContent)
	if alias == nil || alias.(*store.SourceArtifact).LocalPath != root {
		t.Fatal("remote graph lost bootstrap source classification")
	}
	assert.Diff(t, ownersForTest(result.Index, "apps/kustomization.yaml"), []manifest.NamedResource{testutil.KustomizationID("flux-system", "owner")})
	record, ok := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "admitted"})
	if !ok || record.Parent != result.Binding.Producer || st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "unowned"}) != nil {
		t.Fatal("markerless output or scoped sibling admission changed")
	}
	if probes.Load() != 1 || acquisitions.Load() == 0 {
		t.Fatalf("markerless admission: HTTP=%d Git=%d", probes.Load(), acquisitions.Load())
	}
}
