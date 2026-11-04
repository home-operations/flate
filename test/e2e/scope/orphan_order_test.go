package scope_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ExcludedSiblingsDeterministic(t *testing.T) {
	root, cache := t.TempDir(), t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux.yaml",
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, root, "apps/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: rendered}\n")
	for _, suffix := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		testutil.WriteFile(
			t,
			root,
			"apps/stray-"+suffix+".yaml",
			testutil.KustomizationYAML(
				"stray-"+suffix,
				"flux-system",
				"  path: ./missing-"+suffix+"\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
			),
		)
	}
	clock := regexp.MustCompile(`   \d+(?:\.\d+s|m\d+s)\n$`)
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			firstOut, firstErr := harness.RequireCLIOK(
				t,
				verb,
				"all",
				"--path",
				root,
				"--cache-dir",
				cache,
				"--no-progress",
				"--concurrency=2",
			)
			secondOut, secondErr := harness.RequireCLIOK(
				t,
				verb,
				"all",
				"--path",
				root,
				"--cache-dir",
				cache,
				"--no-progress",
				"--concurrency=2",
			)
			for _, result := range []struct{ stdout, stderr string }{{firstOut, firstErr}, {secondOut, secondErr}} {
				stdout, stderr := result.stdout, result.stderr
				assert.FatalIf(
					t,
					strings.Contains(stderr, "[OrphanedResource]") || strings.Contains(stdout, "stray-") ||
						strings.Contains(stderr, "stray-") ||
						strings.Contains(stdout, "warnings") ||
						strings.Contains(stdout, "notes (") ||
						strings.Contains(stderr, "level=WARN"),
					"orphan destination: stdout=%s stderr=%s",
					stdout,
					stderr,
				)
				assert.FatalIf(t, verb == "build" && !strings.Contains(stdout, "name: rendered") ||
					verb == "test" &&
						(!strings.Contains(stdout, "passed") || strings.Contains(stdout, "failed")), "orphan product changed: %s", stdout)
			}
			assert.FatalIf(t, clock.ReplaceAllString(
				firstOut,
				"   <elapsed>\n",
			) != clock.ReplaceAllString(
				secondOut,
				"   <elapsed>\n",
			) ||
				firstErr != secondErr, "unstable orphan output: first stdout=%s stderr=%s; second stdout=%s stderr=%s", firstOut, firstErr, secondOut, secondErr)
		})
	}
}
