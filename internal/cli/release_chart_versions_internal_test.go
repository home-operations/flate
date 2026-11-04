package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetHRChartVersions(t *testing.T) {
	for _, tc := range []struct{ name, reference, chart, want, wantChart string }{
		{name: "digest", reference: "digest: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, semver: '>=1.2.0', tag: '1.2.3'", want: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantChart: "apps-chart/"},
		{name: "semver", reference: "semver: '>=1.2.0', tag: '1.2.3'", want: ">=1.2.0", wantChart: "apps-chart/"},
		{name: "tag", reference: "tag: '1.2.3'", want: "1.2.3", wantChart: "apps-chart/"},
		{name: "inline OCI", reference: "tag: '1.2.3'", chart: "chart: {kind: OCIRepository, spec: {url: 'oci://example.test/app', ref: {tag: '1.2.3'}}}", want: "1.2.3", wantChart: "apps-apps-app/"},
		{name: "own version", reference: "tag: '1.2.3'", chart: "chart: {spec: {chart: app, version: '9.9.9', sourceRef: {kind: HelmRepository, name: chart}}}", want: "9.9.9", wantChart: "apps-apps-app/app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			chart := tc.chart
			if chart == "" {
				chart = "chartRef: {kind: OCIRepository, name: chart}"
			}
			testutil.WriteFile(
				t,
				root,
				"flux.yaml",
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: app, namespace: apps}\nspec: {suspend: true, "+chart+"}\n---\napiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {suspend: true, url: 'oci://example.test/app', ref: {"+tc.reference+"}}\n",
			)
			for _, command := range []string{"get hr", "get all", "get"} {
				t.Run(command, func(t *testing.T) {
					args := append(
						strings.Fields(command),
						"--path",
						root,
						"--cache-dir",
						t.TempDir(),
						"--concurrency",
						"2",
						"-o",
						"json",
					)
					stdout, stderr, code := runCLI(t, args...)
					var rows []struct {
						Version   string `json:"version"`
						Chart     string `json:"chart"`
						SourceRef struct {
							Namespace string `json:"namespace"`
						} `json:"sourceRef"`
					}
					assert.FatalIf(t, code != 0 || json.Unmarshal([]byte(stdout), &rows) != nil || len(rows) != 1 ||
						rows[0].Version != tc.want ||
						rows[0].Chart != tc.wantChart ||
						rows[0].SourceRef.Namespace != "apps", "chart version: exit=%d stdout=%s stderr=%s want=%s chart=%s", code, stdout, stderr, tc.want, tc.wantChart)
				})
			}
		})
	}
}
