package cli

import (
	"strings"
	"testing"

	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
)

func runClusterGroupsPartialFailureCase(t *testing.T, command *string, root *string, format *string) {
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
	out, diagnostics, status := runClusterInputs(t, args...)
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
			"alpha:Kustomization/shared\nomega:Kustomization/shared\nomega:HelmRelease/shared\n",
		)
	default:
		groups := decodeGetDocuments(t, out, (*format), []string{"alpha", "omega"})
		assert.Equal(t, len(groups[0].Documents), 1)
		assert.Equal(t, groups[0].Documents[0]["kind"], any("Kustomization"))
		assert.Equal(t, len(groups[1].Documents), 2)
		assert.Equal(t, groups[1].Documents[0]["kind"], any("Kustomization"))
		assert.Equal(t, groups[1].Documents[1]["kind"], any("HelmRelease"))
		assert.Equal(t, groups[1].Documents[1]["name"], any("shared"))
	}
}
