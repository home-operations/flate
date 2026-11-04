package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/orchestrator"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Build_PartialRenderAndDocumentWriteFailure(t *testing.T) {
	root := writeMultiNamespaceFixture(t)
	testutil.WriteFile(
		t,
		root,
		"apps-a/stray.yaml",
		testutil.KustomizationYAML(
			"stray",
			"alpha",
			"  path: ./missing-stray\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "apps-b/kustomization.yaml", "resources: [missing.yaml]\n")
	args := []string{"build", "ks", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", "2", "-o", "json"}
	full, stderr, code := runCLI(t, args...)
	assert.Equal(t, code, 1)
	assert.FatalIf(
		t,
		!strings.Contains(stderr, "apps-b") || !strings.Contains(full, "apps-a"),
		"fixture did not preserve the healthy render: stdout=%s stderr=%s",
		full,
		stderr,
	)
	limit := len(full) - 1
	writeErr := errors.New("document write failed")
	writer := &outputFailWriter{remaining: limit, err: writeErr}
	var errOut bytes.Buffer
	cmd := New("test")
	cmd.SetArgs(args)
	cmd.SetOut(writer)
	cmd.SetErr(&errOut)
	got := cmd.ExecuteContext(t.Context())
	{
		_, ok := errors.AsType[*orchestrator.FailuresError](got)
		assert.FatalIf(t, !ok || !errors.Is(got, writeErr), "render or writer error lost: %v", got)
	}
	{
		_, ok := errors.AsType[reportedError](got)
		assert.FatalIf(t, !ok, "completed diagnostic was not marked reported: %v", got)
	}
	assert.Equal(t, writer.buf.String(), full[:limit])
	assert.Equal(t, strings.Count(errOut.String(), writeErr.Error()), 1)
	assert.Equal(t, strings.Count(errOut.String(), "[OrphanedResource]"), 0)
	writer = &outputFailWriter{remaining: limit, err: writeErr}
	errOut.Reset()
	assert.Equal(t, Run(args, writer, &errOut), 1)
	assert.Equal(t, writer.buf.String(), full[:limit])
	assert.Equal(t, strings.Count(errOut.String(), writeErr.Error()), 1)
	assert.Equal(t, strings.Count(errOut.String(), "[OrphanedResource]"), 0)
}
