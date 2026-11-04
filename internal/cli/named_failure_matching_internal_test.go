package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_TestNamed_MatchingFailureFromOtherKind(t *testing.T) {
	root := writeFixture(t)
	testutil.WriteFile(
		t,
		root,
		"flux/ghost.yaml",
		testutil.KustomizationYAML(
			"ghost",
			"flux-system",
			"  path: ./missing-ghost\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	cli := runCLI(
		t,
		"test",
		"hr",
		"ghost",
		"--path",
		root,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 1 || !strings.Contains(stderr, `no HelmRelease named "ghost"`) || stdout != "",
		"other-kind diagnostic satisfied named roster: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}
