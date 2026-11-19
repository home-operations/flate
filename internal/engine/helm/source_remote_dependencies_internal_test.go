package helm

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/source"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/helmchart"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepareChart_RemoteDependency(t *testing.T) {
	t.Parallel()
	archive := chartArchive(t, fstest.MapFS{
		"Chart.yaml":        {Data: []byte("apiVersion: v2\nname: child\nversion: 1.0.0\n")},
		"templates/cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: child-marker\n")},
	})
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		username, password, ok := r.BasicAuth()
		if !ok || username != "user" || password != "password" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/index.yaml":
			_, _ = io.WriteString(
				w,
				"apiVersion: v1\nentries:\n  child:\n  - apiVersion: v2\n    name: child\n    version: 1.0.0\n    urls: [child.tgz]\n",
			)
		case "/child.tgz":
			_, _ = w.Write(archive)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"mychart/Chart.yaml",
		fmt.Sprintf(
			"apiVersion: v2\nname: parent\nversion: 1.0.0\ndependencies:\n- name: child\n  version: 1.0.0\n  repository: %s\n",
			server.URL,
		),
	)
	owner := cachetest.New(t, "", cache.New)
	network := new(source.Network)
	t.Cleanup(network.Close)
	client := NewClient(owner, helmchart.New(nil, helmchart.Config{Cache: owner, Network: network, Concurrency: 2}))
	hr := newHR()
	repository := &manifest.HelmRepository{
		Name:      "declared",
		Namespace: hr.ChartSource().Namespace,
		URL:       server.URL,
		SecretRef: &manifest.LocalObjectReference{Name: "auth"},
	}
	st := store.New()
	st.AddObject(repository)
	st.AddObject(
		&manifest.Secret{
			Name:       "auth",
			Namespace:  repository.Namespace,
			StringData: map[string]any{"username": "user", "password": "password"},
		},
	)
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: os.DirFS(root)})
	var reads store.ReadSet
	reads.Init(st, hr.Named(), nil)
	defer reads.Close()
	var preparation Preparation
	input := RenderOptions{Sources: NewSources(&reads, nil), Preparation: &preparation}
	fetch, err := client.PrepareChart(t.Context(), hr, &input)
	assert.NoError(t, err)
	assert.Equal(t, fetch != nil, true)
	_, complete := client.PreviousRender(t.Context(), hr, nil, input)
	assert.Equal(t, complete, false)
	assert.Equal(t, requests.Load(), int64(0))
	pending, err := fetch(t.Context())
	assert.NoError(t, err)
	artifact, err := pending.Fetch(t.Context(), nil)
	assert.NoError(t, err)
	assert.NoError(t, preparation.AcceptChartDependency(t.Context(), artifact))
	if pending.Close != nil {
		pending.Close()
	}
	artifact.FS = nil
	next, err := client.PrepareChart(t.Context(), hr, &input)
	assert.NoError(t, err)
	assert.Equal(t, next == nil, true)
	docs, _, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, hasRenderField(docs, "name", "child-marker"), true)
	assert.Equal(t, requests.Load(), int64(2))
}
