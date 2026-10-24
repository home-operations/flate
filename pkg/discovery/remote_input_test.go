package discovery_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

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
	result, err := discovery.Run(t.Context(), discovery.Config{Trees: testTrees(t), Path: root, RepoRoot: root, Store: st})
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
	if len(result.Documents) != 1 {
		t.Fatalf("generated output missing: %v", result.Documents)
	}
	id := manifest.DocIdentity(result.Documents[0])
	generated, ok := st.GetObject(id).(*manifest.ConfigMap)
	if !ok || !strings.HasPrefix(id.Name, "generated-") || generated.Data["chosen"] != "selected-remote-value" {
		t.Fatalf("remote generator lost built identity/content: %s %+v", id, generated)
	}
	if _, raw := result.Index.Lookup(manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "generated"}); raw {
		t.Fatal("raw generator declaration resurrected")
	}
	assert.Equal(t, requests.Load(), int32(1))
	record, _ := result.Index.Lookup(id)
	if !slices.Contains(record.Inputs, "kustomization.yaml") || len(result.Index.Readers) != 0 {
		t.Fatalf("remote generator lost declaration input or acquired render ownership: %+v", record)
	}
}
