package testfixture

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func TestFixture_RegistryReplay(t *testing.T) {
	t.Parallel()
	dir := fixtureSource(t, "reset")
	for _, repo := range []string{"repo", "baseline"} {
		testutil.WriteFile(t, dir, repo+"/source", "oci://reach.invalid/demo")
	}
	testutil.WriteFile(t, dir, "charts/reach/Chart.yaml", "apiVersion: v2\nname: reach\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "charts/reach/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: witness}\n")
	s, err := load(dir)
	assert.NoError(t, err)
	s.Cases[0].Setup = []operation{{Registry: &registry{
		Chart: "charts/reach", Files: []string{"repo/source", "baseline/source"}, Sentinel: "oci://reach.invalid/demo",
	}}}
	data, err := json.Marshal(s)
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "scenario.json", string(data))
	f := materialize(t, dir, "sequence")
	var first string
	for range 2 {
		f.Restore(t)
		body, readErr := os.ReadFile(filepath.Join(f.Repo, "source"))
		assert.NoError(t, readErr)
		url := string(body)
		if first == "" {
			first = url
		}
		assert.Equal(t, url, first)
		baseline, readErr := os.ReadFile(f.path(t, "baseline/source"))
		assert.NoError(t, readErr)
		assert.Equal(t, string(baseline), url)
		endpoint := strings.Replace(url, "oci://", "https://", 1)
		endpoint = strings.TrimSuffix(endpoint, "/demo") + "/v2/demo/manifests/0.1.0"
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint, http.NoBody)
		assert.NoError(t, requestErr)
		transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
		t.Cleanup(transport.CloseIdleConnections)
		client := &http.Client{Transport: transport}
		response, requestErr := client.Do(request)
		assert.NoError(t, requestErr)
		assert.Equal(t, response.StatusCode, http.StatusOK)
		manifest, readErr := io.ReadAll(response.Body)
		assert.NoError(t, readErr)
		assert.NoError(t, response.Body.Close())
		var image struct {
			Layers []struct {
				MediaType string `json:"mediaType"`
			} `json:"layers"`
		}
		assert.NoError(t, json.Unmarshal(manifest, &image))
		assert.Equal(t, len(image.Layers), 2)
		assert.Equal(t, image.Layers[1].MediaType, "application/vnd.cncf.helm.chart.content.v1.tar+gzip")
	}
}
