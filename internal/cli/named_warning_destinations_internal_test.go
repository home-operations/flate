package cli

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_NamedScope_UnrelatedNamespaceWarnings(t *testing.T) {
	for _, verb := range []string{"build", "test"} {
		t.Run(verb, func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			body, err := os.ReadFile(filepath.Join(root, "cluster/unrelated.yaml"))
			assert.NoError(t, err)
			testutil.WriteFile(
				t,
				root,
				"cluster/unrelated.yaml",
				strings.Replace(string(body), "namespace: flux-system", "namespace: other", 1),
			)
			out, stderr, code := runCLI(
				t,
				verb,
				"hr",
				"selected",
				"--path",
				filepath.Join(root, "cluster"),
				"-n",
				"apps",
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			)
			assert.Equal(t, code, 0)
			assert.FatalIf(
				t,
				strings.Contains(out+stderr, "unrelated") || strings.Contains(out+stderr, "outside"),
				"excluded namespace diagnostics leaked: %s%s",
				out,
				stderr,
			)
		})
	}
}

//nolint:paralleltest // test changes the process environment or working directory.
func TestRun_NamedScope_TestWarningDestinations(t *testing.T) {
	preservePersistenceNoticeState(t)
	renderPersistenceDisabledReason = persistenceReason
	for _, fatal := range []bool{false, true} {
		t.Run(strconv.FormatBool(fatal), func(t *testing.T) {
			root := writeNamedScopeFixture(t)
			wantCode := 0
			if fatal {
				appendScopeSpec(t, root, "cluster/hr.yaml", "  dependsOn: [{name: fatal-dependency}]\n")
				wantCode = 1
			}
			out, stderr, code := runCLI(t, "test", "hr", "selected", "--path", filepath.Join(root, "cluster"),
				"--cache-dir", t.TempDir(), "--concurrency", "2")
			assert.Equal(t, code, wantCode)
			assert.Equal(
				t,
				strings.Contains(out, "warnings") || strings.Contains(out, "unrelated") ||
					strings.Contains(out, "outside"),
				false,
			)
			assert.Equal(t, strings.Count(stderr, "warnings (1)"), 1)
			assert.Equal(t, strings.Count(stderr, "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/unrelated"), 0)
			assert.Equal(t, strings.Contains(stderr, "outside"), false)
			if fatal {
				assert.Equal(t, strings.Count(stderr, "apps/fatal-dependency"), 1)
				assert.Equal(t, strings.Contains(stderr, "unrelated"), false)
				assert.Equal(t, strings.Contains(stderr, "warnings"), true)
				assert.Equal(t, strings.Count(stderr, "flate error:"), 0)
			} else {
				assert.Equal(t, strings.Contains(stderr, persistenceMessage), true)
			}
		})
	}
}
