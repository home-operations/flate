package scope

import (
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
	"regexp"
	"strings"
	"testing"
)

func TestE2E_ExcludedSiblingsDeterministic(t *testing.T) {
	root, cache := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, root, "flux.yaml", testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n")
	for _, suffix := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		testutil.WriteFile(t, root, "apps/stray-"+suffix+".yaml", testutil.KustomizationYAML("stray-"+suffix, "flux-system", "  path: ./missing-"+suffix+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
	}
	clock := regexp.MustCompile(`   \d+(?:\.\d+s|m\d+s)\n$`)
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			var wantOut, wantErr string
			for run := range 2 {
				stdout, stderr := harness.RequireCLIOK(t, verb, "all", "--path", root, "--cache-dir", cache, "--no-progress", "--concurrency=2")
				if strings.Contains(stderr, "[OrphanedResource]") || strings.Contains(stdout, "stray-") || strings.Contains(stderr, "stray-") || strings.Contains(stdout, "warnings") || strings.Contains(stdout, "notes (") || strings.Contains(stderr, "level=WARN") {
					t.Fatalf("orphan destination: stdout=%s stderr=%s", stdout, stderr)
				}
				if verb == "build" && !strings.Contains(stdout, "name: rendered") || verb == "test" && (!strings.Contains(stdout, "passed") || strings.Contains(stdout, "failed")) {
					t.Fatalf("orphan product changed: %s", stdout)
				}
				stdout = clock.ReplaceAllString(stdout, "   <elapsed>\n")
				if run == 0 {
					wantOut, wantErr = stdout, stderr
				} else if stdout != wantOut || stderr != wantErr {
					t.Fatalf("unstable orphan output: stdout=%s stderr=%s", stdout, stderr)
				}
			}
		})
	}
}
