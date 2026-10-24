package orchestrator

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestBootstrap_RemoteGeneratorSharedAcquisition(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, "VALUE=stable\n")
	}))
	defer server.Close()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", fmt.Sprintf(`namespace: apps
configMapGenerator:
- name: env
  envs: [%[1]s]
- name: file
  files: [payload=%[1]s]
secretGenerator:
- name: secret
  envs: [%[1]s]
generatorOptions:
  disableNameSuffixHash: true
`, server.URL))
	o, err := New(t.Context(), Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2, SourceRetry: sourcecache.RetryConfig{Attempts: 2}})
	assert.NoError(t, err)
	defer func() { assert.NoError(t, o.Stop()) }()
	assert.NoError(t, o.Bootstrap(t.Context()))
	for name, want := range map[string]map[string]any{
		"env": {"VALUE": "stable"}, "file": {"payload": "VALUE=stable\n"},
	} {
		obj, ok := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name})
		if !ok {
			t.Fatalf("bootstrap ConfigMap %s missing", name)
		}
		assert.Diff(t, obj.Data, want)
	}
	secret, ok := o.store.Get[*manifest.Secret](manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "secret"})
	if !ok {
		t.Fatal("bootstrap Secret missing")
	}
	assert.Diff(t, secret.Data, map[string]any{"VALUE": base64.StdEncoding.EncodeToString([]byte("stable"))})
	ks, ok := o.store.Get[*manifest.Kustomization](testutil.KustomizationID("apps", "owner"))
	if !ok {
		t.Fatal("bootstrap owner missing")
	}
	req := kustomize.RenderRequest{Source: o.store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact), SubPath: "apps", Spec: ks}
	first, err := kustomize.RenderFlux(t.Context(), o.ksc.Trees, req, o.cfg.SourceRetry)
	assert.NoError(t, err)
	second, err := kustomize.RenderFlux(t.Context(), o.ksc.Trees, req, o.cfg.SourceRetry)
	assert.NoError(t, err)
	if len(first) == 0 || !reflect.DeepEqual(first, second) {
		t.Fatalf("rendered documents differ: first=%v second=%v", first, second)
	}
	assert.Equal(t, requests.Load(), int32(1))
}
