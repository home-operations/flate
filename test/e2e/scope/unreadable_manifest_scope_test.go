package scope_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_UnreadableManifestScope(t *testing.T) {
	for _, owned := range []bool{true, false} {
		name := "unowned nested census"
		if owned {
			name = "owned with independent sibling"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.GitInit(t, dir)
			testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux]\n")
			testutil.WriteFile(t, dir, "flux/kustomization.yaml", "resources: [drivers.yaml]\n")
			drivers := testutil.KustomizationYAML(
				"sibling",
				"flux-system",
				"  path: ./healthy\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			)
			if owned {
				drivers += "---\n" + testutil.KustomizationYAML(
					"owner",
					"flux-system",
					"  path: ./incidental/nested\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
				)
			}
			testutil.WriteFile(t, dir, "flux/drivers.yaml", drivers)
			testutil.WriteFile(t, dir, "healthy/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(
				t,
				dir,
				"healthy/cm.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: independent-output}\n",
			)
			builder := "resources: [unreadable.yaml]\n"
			if !owned {
				builder = "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n" + builder
			}
			testutil.WriteFile(t, dir, "incidental/nested/kustomization.yaml", builder)
			assert.NoError(t, os.Symlink("missing.yaml", filepath.Join(dir, "incidental/nested/unreadable.yaml")))
			args := []string{
				"build",
				"all",
				"--path",
				dir,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			want := 0
			if owned {
				want = 1
			}
			assert.FatalIf(
				t,
				code != want || !strings.Contains(stdout, "independent-output"),
				"exit=%d want=%d\n%s\n%s",
				code,
				want,
				stdout,
				stderr,
			)
			switch {
			case owned:

				assert.FatalIf(
					t,
					!strings.Contains(stderr, "flux-system/owner") || !strings.Contains(stderr, "unreadable.yaml"),
					"file failure lost its owner: %s",
					stderr,
				)

			case strings.Contains(stderr, "unreadable.yaml"):

				t.Fatalf("incidental failure acquired authority: %s", stderr)
			}
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}
