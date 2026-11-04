package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func runGetKSName(t *testing.T, tc *struct {
	name     string
	path     string
	wantPath string
},
) {
	t.Helper()

	root := t.TempDir()
	pathField := ""
	if tc.path != "" {
		pathField = "  path: " + tc.path + "\n"
	}
	files := map[string]string{
		"kustomization.yaml": "resources: [cluster.yaml]\n",
		"cluster.yaml": testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  interval: 10m\n"+pathField+"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
		"apps/kustomization.yaml": "resources: [cm.yaml]\n",
		"apps/cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\n",
	}
	for name, content := range files {
		testutil.WriteFile(t, root, name, content)
	}
	for _, command := range []string{"get ks", "get", "get all"} {
		t.Run(command, func(t *testing.T) {
			runGetKSNameCommand(t, &command, &root, struct {
				tc **struct {
					name     string
					path     string
					wantPath string
				}
				files *map[string]string
			}{tc: &tc, files: &files})
		})
	}
}

func runGetKSNameCommand(t *testing.T, command *string, root *string, capturedInputs struct {
	tc **struct {
		name     string
		path     string
		wantPath string
	}
	files *map[string]string
},
) {
	t.Helper()

	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			args := append(
				strings.Fields((*command)),
				"--path",
				(*root),
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
				"-o",
				format,
			)
			stdout, stderr, code := runCLI(t, args...)
			assert.FatalIf(t, code != 0, "exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			if format == "table" {
				name, width := "apps", 8
				if (*command) != "get ks" {
					name, width = "Kustomization/apps", 22
				}
				want := fmt.Sprintf(
					"NAMESPACE      %-*sPATH\nflux-system    %-*s%s\n",
					width,
					"NAME",
					width,
					name,
					(*capturedInputs.tc).wantPath,
				)
				assert.Equal(t, stdout, want)
			} else {
				groups := decodeGetDocuments(t, stdout, format, []string{""})
				assert.Diff(t, groups[0].Documents, []map[string]any{
					{
						"kind":      "Kustomization",
						"name":      "apps",
						"namespace": "flux-system",
						"path":      (*capturedInputs.tc).path,
						"sourceRef": map[string]any{
							"kind":      "GitRepository",
							"name":      "flux-system",
							"namespace": "flux-system",
						},
						"prune": false,
						"wait":  false,
					},
				})
			}
			for name, want := range *capturedInputs.files {
				got, err := os.ReadFile(filepath.Join((*root), name))
				assert.NoError(t, err)
				assert.Equal(t, string(got), want)
			}
		})
	}
}
