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
func TestE2E_UnownedMalformedEntryAuthority(t *testing.T) {
	for _, unreadable := range []bool{false, true} {
		name := "malformed"
		if unreadable {
			name = "unreadable"
		}
		t.Run(name, func(t *testing.T) { e2EUnownedMalformedEntryAuthorityName(t, &unreadable) })
	}
}

func e2EUnownedMalformedEntryAuthorityName(t *testing.T, unreadable *bool) {
	t.Helper()

	dir := t.TempDir()
	testutil.GitInit(t, dir)
	testutil.WriteFile(t, dir, "kustomization.yaml", "resources: [flux/ks.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"flux/ks.yaml",
		testutil.KustomizationYAML(
			"owner",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, dir, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		dir,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: healthy-output}\n",
	)
	testutil.WriteFile(t, dir, "pkgdir/bad.yaml", "broken: [\n")
	if *unreadable {
		assert.NoError(t, os.Remove(filepath.Join(dir, "pkgdir/bad.yaml")))
		assert.NoError(t, os.Symlink("missing.yaml", filepath.Join(dir, "pkgdir/bad.yaml")))
		testutil.WriteFile(
			t,
			dir,
			"pkgdir/kustomization.yaml",
			"apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\nresources: [bad.yaml]\n",
		)
	}
	for _, tc := range []struct {
		name, path string
		failed     bool
	}{
		{name: "incidental nested file", path: dir},
		{name: "explicit package entry", path: filepath.Join(dir, "pkgdir"), failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{
				"build",
				"all",
				"--path",
				tc.path,
				"--no-progress",
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
			}
			stdout, stderr, code := harness.RunCLIBuffers(args...)
			switch {
			case tc.failed:

				want := "pkgdir/bad.yaml"
				if *unreadable {
					want = "bad.yaml"
				}
				assert.FatalIf(t, code != 1 || !strings.Contains(stderr, want) ||
					strings.Contains(stderr, "Kustomization entry") ||
					!(*unreadable) && !strings.Contains(stderr, "document 1") ||
					strings.Contains(
						stderr,
						"flux-system/owner",
					), "explicit entry lost its input failure: exit=%d\n%s\n%s", code, stdout, stderr)

			case code != 0 || !strings.Contains(stdout, "healthy-output") || strings.Contains(stderr, "bad.yaml"):

				t.Fatalf("incidental file failed independent authority: exit=%d\n%s\n%s", code, stdout, stderr)
			}
			nextOut, nextErr, nextCode := harness.RunCLIBuffers(args...)
			assert.Equal(t, nextCode, code)
			assert.Equal(t, nextOut, stdout)
			assert.Equal(t, nextErr, stderr)
		})
	}
}
