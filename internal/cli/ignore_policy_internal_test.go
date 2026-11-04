package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ConventionalIgnore(t *testing.T) {
	path := writeFixture(t)
	testutil.WriteFile(t, path, "kustomization.yaml", "resources: [flux/cluster.yaml]\n")
	testutil.WriteFile(
		t,
		path,
		"flux/extra.yaml",
		testutil.KustomizationYAML(
			"apps-extra",
			"flux-system",
			"  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	testutil.WriteFile(t, path, ".krmignore", "flux/extra.yaml\n")
	cli := runCLI(t, "get", "ks", "--path", path, "-o", "name")
	stdout, stderr, code := cli.stdout, cli.stderr, cli.code
	assert.FatalIf(
		t,
		code != 0 || strings.Contains(stdout, "apps-extra") || !strings.Contains(stdout, "apps"),
		"conventional ignore: exit=%d stdout=%s stderr=%s",
		code,
		stdout,
		stderr,
	)
}
