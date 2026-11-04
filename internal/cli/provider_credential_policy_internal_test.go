package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ProviderCredentialPolicy(t *testing.T) {
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }),
	)
	t.Cleanup(server.Close)
	root := substitutionFixture(t, "", "local")
	testutil.WriteFile(t, root, "kustomization.yaml", "resources: [flux.yaml, source.yaml, remote.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"source.yaml",
		fmt.Sprintf(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: cloud, namespace: apps}\nspec: {provider: aws, insecure: true, url: 'oci://%s/image'}\n",
			strings.TrimPrefix(server.URL, "https://"),
		),
	)
	testutil.WriteFile(
		t,
		root,
		"remote.yaml",
		testutil.KustomizationYAML(
			"remote",
			"apps",
			"  path: ./remote\n  sourceRef: {kind: OCIRepository, name: cloud}\n",
		),
	)
	registry := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registry, `{"auths":{}}`)
	for _, tc := range []struct {
		name, selection string
		want            int
	}{
		{name: "default", want: 1}, {name: "ConfigMap", selection: "ConfigMap", want: 1}, {name: "Secret", selection: "Secret"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, diagnostic, code := runCLI(
				t,
				"build",
				"all",
				"--path",
				root,
				"--registry-config",
				registry,
				"--allow-missing-kinds="+tc.selection,
				"--concurrency=2",
			)
			assert.FatalIf(
				t,
				code != tc.want || !strings.Contains(out, "value: local"),
				"provider blocked unrelated output: exit=%d %s %s",
				code,
				out,
				diagnostic,
			)
			assert.FatalIf(
				t,
				code == 0 && !strings.Contains(diagnostic, "SourceCredentialsUnavailable"),
				"missing source warning: %s",
				diagnostic,
			)
		})
	}
}
