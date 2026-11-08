package helm

import (
	"testing"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestTemplate_KubeVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: capabilities
data:
  version: {{ .Capabilities.KubeVersion.Version | quote }}
`)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	for _, tc := range []struct {
		name, version, want string
		fail                bool
	}{
		{name: "default", want: chartcommon.DefaultCapabilities.KubeVersion.Version},
		{name: "override", version: "1.30.2", want: "v1.30.2"},
		{name: "invalid", version: "invalid", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, _, err := testutil.RenderMaps(
				cli.Template(
					t.Context(),
					newHR(),
					nil,
					RenderOptions{
						KubeVersion: tc.version,
						Sources:     localChartResolver(t, "chart-repo", "flux-system", dir),
					},
				),
			)
			if tc.fail {
				assert.FatalIf(t, err == nil, "%s", "invalid override accepted")
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, docs[0]["data"].(map[string]any)["version"], any(tc.want))
		})
	}
}
