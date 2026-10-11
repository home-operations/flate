package e2e

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_SubstitutionProducerRendering(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteSubstitutionProducerCluster(t, root, kind)
			stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress")
			if code != 0 || !strings.Contains(stdout, "version: 4.6.2") || strings.Contains(stdout, "version: discovered") {
				t.Fatalf("rendered substitution: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
			}
		})
	}
}

func TestE2E_SubstitutionProducerCycle(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionCycleCluster(t, root)
	const want = "\n  ✗  Kustomization  flux-system/a  dependency cycle: blocked by Kustomization/flux-system/b\n  ✗  Kustomization  flux-system/b  dependency cycle: blocked by Kustomization/flux-system/a\n\n  ✗ 2 failed\n"
	for _, workers := range []string{"1", "2", "8"} {
		t.Run("concurrency_"+workers, func(t *testing.T) {
			for range 15 {
				stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress")
				if code != 1 || stdout != "" || stderr != want {
					t.Fatalf("substitution cycle: exit=%d stdout=%q stderr=%q", code, stdout, stderr)
				}
			}
		})
	}
}
