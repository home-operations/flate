package discovery_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
