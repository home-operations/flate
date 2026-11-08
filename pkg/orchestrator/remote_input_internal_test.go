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
	"github.com/home-operations/flate/internal/kustomize"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/values"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_RemoteGeneratorSharedAcquisition(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, "VALUE=stable\n")
	}))
	defer server.Close()
	root := t.TempDir()
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML("owner", "apps", "  path: ./apps\n  interval: 5m\n"),
	)
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
	o, err := newTestOrchestrator(
		t,
		Tree{Path: root, RepoRoot: root},
		Config{CacheDir: t.TempDir(), Concurrency: 2, SourceRetry: sourcecache.RetryConfig{Attempts: 2}},
	)
	assert.NoError(t, err)
	defer func() { o.stop() }()
	assert.NoError(t, o.bootstrap(t.Context()))
	reconcileEntryForTest(t, o)
	for name, want := range map[string]map[string]any{
		"env": {"VALUE": "stable"}, "file": {"payload": "VALUE=stable\n"},
	} {
		obj, ok := values.NewStoreProvider(o.store).Data(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: name}).(*manifest.ConfigMap)
		assert.FatalIf(t, !ok, "bootstrap ConfigMap %s missing", name)
		assert.Diff(t, obj.Data, want)
	}
	secret, ok := values.NewStoreProvider(o.store).Data(manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "secret"}).(*manifest.Secret)
	assert.FatalIf(t, !ok, "%v", "bootstrap Secret missing")
	assert.Diff(t, secret.Data, map[string]any{"VALUE": base64.StdEncoding.EncodeToString([]byte("stable"))})
	ks, ok := o.store.Get[*manifest.Kustomization](testutil.KustomizationID("apps", "owner"))
	assert.FatalIf(t, !ok, "%v", "bootstrap owner missing")
	req := kustomize.RenderRequest{
		Source:  o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
		SubPath: "apps",
		Spec:    ks,
	}
	first, _, err := testutil.RenderMaps(kustomize.Render(t.Context(), o.ksc.Trees, req, o.cfg.SourceRetry))
	assert.NoError(t, err)
	second, _, err := testutil.RenderMaps(kustomize.Render(t.Context(), o.ksc.Trees, req, o.cfg.SourceRetry))
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		len(first) == 0 || !reflect.DeepEqual(first, second),
		"rendered documents differ: first=%v second=%v",
		first,
		second,
	)
	assert.Equal(t, requests.Load(), int32(1))
	assert.FatalIf(
		t,
		len(o.store.ListObjects("", manifest.KindConfigMap))+len(o.store.ListObjects("", manifest.KindSecret)) != 0,
		"%v",
		"bootstrap generator lookup entered canonical inventory",
	)
}
