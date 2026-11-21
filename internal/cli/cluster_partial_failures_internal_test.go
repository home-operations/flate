package cli

import (
	"strings"
	"testing"

	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/home-operations/flate/internal/cli/output"
	"github.com/home-operations/flate/internal/testutil/assert"
)

func runClusterGroupsPartialFailureCase(t *testing.T, command, root, format *string) {
	t.Helper()

	args := append(
		strings.Fields((*command)),
		"--path",
		(*root),
		"-o",
		(*format),
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	cluster := runClusterInputs(t, args...)
	out, diagnostics, status := cluster.stdout, cluster.stderr, cluster.code
	assert.FatalIf(t, status != 1 || !strings.Contains(diagnostics, "alpha: flux-system/missing") ||
		strings.Contains(
			diagnostics,
			"Cluster:",
		), "lost qualified failure: exit=%d stdout=%s stderr=%s", status, out, diagnostics)
	if (*command) == "get images" {
		if (*format) == "table" {
			assert.Equal(t, out, "CLUSTER    IMAGE\nomega      example.test/app:second\n")
		}
		if (*format) == "name" {
			assert.Equal(t, out, "omega: example.test/app:second\n")
		}
		if (*format) == "json" || (*format) == "yaml" {
			var images []output.ImageGroup
			assert.NoError(t, yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&images))
			assert.Equal(t, len(images), 2)
			assert.Equal(t, images[0].Cluster, "alpha")
			assert.Equal(t, len(images[0].Images), 0)
			assert.Equal(t, images[1].Cluster, "omega")
			assert.Diff(t, images[1].Images, []string{"example.test/app:second"})
		}
		return
	}
	switch *format {
	case "table":
		assert.FatalIf(t, strings.Count(out, "CLUSTER") != 2 || !strings.Contains(out, "omega") ||
			!strings.Contains(out, "Kustomization/shared") ||
			!strings.Contains(out, "HelmRelease/shared"), "partial table lost healthy resources: %s", out)
	case "name":
		assert.Equal(
			t,
			out,
			"alpha:Kustomization/cluster\nalpha:Kustomization/shared\n"+
				"omega:Kustomization/cluster\nomega:Kustomization/shared\nomega:HelmRelease/shared\n",
		)
	default:
		groups := decodeGetDocuments(t, out, (*format), []string{"alpha", "omega"})
		assert.Equal(t, len(groups[0].Documents), 2)
		assert.Equal(t, len(groups[1].Documents), 3)
		for _, group := range groups {
			assert.Equal(t, group.Documents[0]["kind"], any("Kustomization"))
			assert.Equal(t, group.Documents[0]["name"], any("cluster"))
			assert.Equal(t, group.Documents[1]["kind"], any("Kustomization"))
			assert.Equal(t, group.Documents[1]["name"], any("shared"))
		}
		assert.Equal(t, groups[1].Documents[2]["kind"], any("HelmRelease"))
		assert.Equal(t, groups[1].Documents[2]["name"], any("shared"))
	}
}
