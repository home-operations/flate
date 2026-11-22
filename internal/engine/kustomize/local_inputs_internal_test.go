package kustomize

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestObserveLocalInputs_DirectHTTPResource(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(w, cmYAML("remote"))
	}))
	t.Cleanup(server.Close)
	req := RenderRequest{
		Source: &store.SourceArtifact{Content: "source", FS: fstest.MapFS{
			"app/kustomization.yaml": {
				Data: []byte(existingKustomization("../shared/local.yaml", server.URL+"/cm.yaml")),
			},
			"shared/local.yaml": {Data: []byte(cmYAML("local"))},
		}},
		SubPath: "app",
	}
	req.LocalInputs = ObserveLocalInputs(t.Context(), &req)
	assert.Equal(t, hits.Load(), int32(0))
	assert.Equal(t, req.LocalInputs.Complete, true)
	assert.Diff(t, req.LocalInputs.Files, []string{"app/kustomization.yaml", "shared/local.yaml"})

	out, _, err := testutil.RenderMaps(Render(t.Context(), newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(out, "name", "local"), true)
	assert.Equal(t, hasRenderField(out, "name", "remote"), true)
	assert.Equal(t, hits.Load(), int32(1))
}

func TestObserveLocalInputs_RemoteFilesAndLocalPlugins(t *testing.T) {
	t.Parallel()
	req := RenderRequest{
		Source: &store.SourceArtifact{Content: "source", FS: fstest.MapFS{
			"app/kustomization.yaml": {Data: []byte(`
resources: [../shared/local.yaml, "https://example.invalid/org/repo.git//base"]
generators: [../plugins]
patches:
  - path: https://example.invalid/patch.yaml
    target: {kind: ConfigMap}
`)},
			"plugins/kustomization.yaml": {Data: []byte("resources: [generator.yaml]")},
			"plugins/generator.yaml": {Data: []byte(`
apiVersion: builtin
kind: ConfigMapGenerator
metadata: {name: generated}
files: [../shared/value.txt]
`)},
			"shared/local.yaml":       {Data: []byte(cmYAML("local"))},
			"shared/value.txt":        {Data: []byte("read")},
			"shared/unread-value.txt": {Data: []byte("unread")},
		}},
		SubPath: "app",
	}
	inputs := ObserveLocalInputs(t.Context(), &req)
	assert.Equal(t, inputs.Complete, true)
	assert.Diff(t, inputs.Roots, []string{"app"})
	assert.Diff(t, inputs.Files, []string{
		"app/kustomization.yaml", "plugins/generator.yaml", "plugins/kustomization.yaml",
		"shared/local.yaml", "shared/value.txt",
	})
}

type countedControls struct {
	fstest.MapFS

	reads atomic.Int32
}

func (f *countedControls) ReadFile(name string) ([]byte, error) {
	if strings.HasSuffix(name, "/kustomization.yaml") {
		f.reads.Add(1)
	}
	return f.MapFS.ReadFile(name)
}

func TestPrepareFilesAndPreflight_CaptureMatch(t *testing.T) {
	t.Parallel()
	spec := &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: "app"}
	for _, tc := range []struct {
		name    string
		label   string
		subPath string
		spec    *manifest.Kustomization
		content string
	}{
		{name: "matching", subPath: "app", spec: spec.Clone(), content: "source"},
		{name: "changed mount", label: "/other", subPath: "app", spec: spec, content: "source"},
		{name: "changed subpath", subPath: "other", spec: spec, content: "source"},
		{name: "changed declaration", subPath: "app", content: "source"},
		{name: "mutable source", subPath: "app", spec: spec},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			control := []byte(existingKustomization())
			files := &countedControls{MapFS: fstest.MapFS{
				"app/kustomization.yaml":   {Data: control},
				"other/kustomization.yaml": {Data: control},
			}}
			req := RenderRequest{
				Source: &store.SourceArtifact{Content: tc.content, FS: files}, Spec: spec, SubPath: "app",
			}
			req.LocalInputs = ObserveLocalInputs(t.Context(), &req)
			assert.Equal(t, req.LocalInputs.Complete, true)
			req.Label, req.SubPath, req.Spec = tc.label, tc.subPath, tc.spec
			root, err := renderRoot(&req)
			assert.NoError(t, err)
			files.reads.Store(0)
			mem := newTreeFS(files, root, nil)
			got, err := prepareFilesAndPreflight(t.Context(), &req, mem, nil)
			assert.NoError(t, err)
			assert.Equal(t, files.reads.Load() == 0, tc.name == "matching")
			assert.Equal(t, got.dir, mem.mount(tc.subPath))
		})
	}
}

func TestRender_ReusesLocalInputsAcrossRemoteRewrites(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/value" {
			_, _ = io.WriteString(w, "remote-value")
			return
		}
		_, _ = io.WriteString(w, cmYAML("remote"))
	}))
	t.Cleanup(server.Close)
	req := RenderRequest{
		Source: &store.SourceArtifact{Content: "source", FS: fstest.MapFS{
			"kustomization.yaml": {Data: []byte(existingKustomization(server.URL+"/resource.yaml") + `
generators:
  - |
    apiVersion: builtin
    kind: ConfigMapGenerator
    metadata: {name: generated}
    files: [value=` + server.URL + `/value]
`)},
		}},
		SubPath: ".",
	}
	ctx := t.Context()
	plain, _, err := testutil.RenderMaps(Render(ctx, newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(plain, "name", "remote"), true)
	assert.Equal(t, hasRenderField(plain, "value", "remote-value"), true)

	req.LocalInputs = ObserveLocalInputs(ctx, &req)
	assert.Equal(t, req.LocalInputs.Complete, true)
	control := req.LocalInputs.controls[req.LocalInputs.generated.path]
	for range 2 {
		out, _, renderErr := testutil.RenderMaps(Render(ctx, newTestTreeCache(t, ""), req, sourcecache.RetryConfig{}))
		assert.NoError(t, renderErr)
		assert.Diff(t, out, plain)
		body, controlErr := control.doc.String()
		assert.NoError(t, controlErr)
		assert.Equal(t, strings.Contains(body, server.URL+"/resource.yaml"), true)
		assert.Equal(t, strings.Contains(body, server.URL+"/value"), true)
		for _, ref := range control.refs {
			assert.Equal(t, ref.Node == nil, true)
			for _, plugin := range ref.Resources {
				pluginBody, pluginErr := plugin.AsYAML()
				assert.NoError(t, pluginErr)
				assert.Equal(t, strings.Contains(string(pluginBody), server.URL+"/value"), true)
			}
		}
	}
}
