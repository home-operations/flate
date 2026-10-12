package e2e

import (
	stdcmp "cmp"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/testutil"
)

func TestE2E_OrphanNotesDeterministic(t *testing.T) {
	root, cache := t.TempDir(), t.TempDir()
	testutil.WriteFile(t, root, "flux.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
`)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n")
	for _, suffix := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		testutil.WriteFile(t, root, "apps/stray-"+suffix+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: stray-`+suffix+`, namespace: flux-system}
spec:
  path: ./missing-`+suffix+`
  sourceRef: {kind: GitRepository, name: flux-system}
`)
	}
	// The verdict's elapsed clock depends on runtime, rather than resource order.
	clock := regexp.MustCompile(`   \d+(?:\.\d+s|m\d+s)\n$`)
	var want string
	for _, concurrency := range []int{2, 8} {
		t.Run(fmt.Sprintf("concurrency%d", concurrency), func(t *testing.T) {
			for run := range 6 {
				stdout, _ := requireCLIOK(t, "test", "--path", root, "--cache-dir", cache,
					"--no-progress", "--concurrency", fmt.Sprint(concurrency))
				if strings.Count(stdout, "resource orphaned id=") != 8 {
					t.Fatalf("want eight orphan notes:\n%s", stdout)
				}
				got := clock.ReplaceAllString(stdout, "   <elapsed>\n")
				want = stdcmp.Or(want, got)
				if diff := cmp.Diff(want, got); diff != "" {
					t.Fatalf("run %d stdout differs (-want +got):\n%s", run, diff)
				}
			}
		})
	}
}
