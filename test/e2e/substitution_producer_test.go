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

func TestE2E_SubstitutionProducerFailure(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionProducerCluster(t, root, "ConfigMap")
	testutil.WriteFile(t, root, "data/kustomization.yaml", "namespace: flux-system\nresources: [missing.yaml]\ngeneratorOptions: {disableNameSuffixHash: true}\nconfigMapGenerator:\n- name: settings\n  literals: [VERSION=4.6.2]\n")
	stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress")
	if code != 1 || strings.Contains(stdout, "name: output") || !strings.Contains(stderr, "producer") || !strings.Contains(stderr, "consumer") {
		t.Fatalf("failed producer consumer: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}

func TestE2E_SubstitutionProducerCycle(t *testing.T) {
	root := t.TempDir()
	testutil.WriteSubstitutionCycleCluster(t, root)
	stdout, stderr, code := runCLIBuffers("build", "all", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress")
	if code != 1 || stdout != "" || !strings.Contains(stderr, "dependency cycle") || !strings.Contains(stderr, "flux-system/a") || !strings.Contains(stderr, "flux-system/b") || strings.Contains(stderr, "0 failure(s)") {
		t.Fatalf("substitution cycle: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	}
}
