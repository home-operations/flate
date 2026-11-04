package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SyntheticDiagnosticsAcrossCommands(t *testing.T) {
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }),
	)
	t.Cleanup(server.Close)
	fixture := func() string {
		root := t.TempDir()
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(t, root, "release.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: apps}
spec:
  interval: 5m
  chart:
    kind: OCIRepository
    spec:
      insecure: true
      url: oci://%s/charts/app
      ref: {tag: 1.0.0}
      secretRef: {name: auth}
`, strings.TrimPrefix(server.URL, "https://")))
		return root
	}
	current, original := fixture(), fixture()
	testutil.WriteFile(t, original, "kustomization.yaml", "resources: []\n")
	assert.NoError(t, os.Remove(filepath.Join(original, "release.yaml")))
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	for _, verb := range []string{"build", "diff", "get", "test"} {
		t.Run(verb, func(t *testing.T) {
			args := []string{
				verb,
				"all",
				"--path",
				current,
				"--allow-missing-kinds",
				"Secret",
				"--registry-config",
				config,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			}
			if verb == "diff" {
				args = append(args, "--path-orig", original, "-o", "diff")
			}
			if verb == "build" || verb == "get" {
				args = append(args, "-o", "json")
			}
			out, diagnostic, code := runCLI(t, args...)
			assert.FatalIf(t, code != 0 || strings.Contains(diagnostic, "OCIRepository") ||
				!strings.Contains(
					diagnostic,
					"HelmRelease",
				), "%s diagnostic: code=%d stdout=%s stderr=%s", verb, code, out, diagnostic)
			if verb == "build" || verb == "get" {
				var documents []map[string]any
				assert.NoError(t, json.Unmarshal([]byte(out), &documents))
				for _, document := range documents {
					assert.FatalIf(
						t,
						document["kind"] == manifest.KindOCIRepository,
						"%s exposed generated source: %s",
						verb,
						out,
					)
				}
			}
		})
	}
}
