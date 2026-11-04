package kustomize

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/api/krusty"

	"github.com/home-operations/flate/internal/assert"
	kustomizestate "github.com/home-operations/flate/internal/kustomize"
)

func TestPreflightRemoteResources_LargeBodyMatchesUpstream(t *testing.T) {
	t.Parallel()
	body := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: large}\n" + strings.Repeat(" ", 64<<20) + "\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	fsys := memFSWith(t, map[string]string{"kustomization.yaml": "resources:\n  - " + srv.URL + "/large.yaml\n"})
	kustomizestate.BuildMutex.Lock()
	upstream, err := krusty.MakeKustomizer(krusty.MakeDefaultOptions()).Run(fsys, ".")
	kustomizestate.BuildMutex.Unlock()
	{
		assert.FatalIf(t, err != nil, "upstream large resource: %v", err)
	}
	out, err := upstream.AsYaml()
	{
		assert.FatalIf(
			t,
			err != nil || !strings.Contains(string(out), "name: large"),
			"upstream resource output: %q error=%v",
			out,
			err,
		)
	}
	t.Logf("upstream Kustomize accepted %d-byte valid HTTP resource", len(body))
	{
		preflightRemoteResourcesErr := preflightRemoteResources(t.Context(), newTestTreeCache(t, ""), fsys, ".")
		assert.FatalIf(
			t,
			preflightRemoteResourcesErr != nil,
			"flate large resource after upstream success: %v",
			preflightRemoteResourcesErr,
		)
	}
	files := remoteFiles(t, fsys, ".")
	{
		assert.FatalIf(t, len(files) != 1, "large remote files: %v", files)
	}
	got, err := fsys.ReadFile(files[0])
	{
		assert.FatalIf(
			t,
			err != nil || string(got) != body,
			"large resource: size=%d error=%v, want %d bytes",
			len(got),
			err,
			len(body),
		)
	}
}
