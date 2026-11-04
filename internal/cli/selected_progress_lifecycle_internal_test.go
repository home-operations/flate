package cli

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func runSelectedProgressLifecycleName(t *testing.T, name *string) {
	t.Helper()

	oldBar, oldLogger, original := barSink, slog.Default(), os.Stderr
	var raw *os.File
	defer func() {
		captured := os.Stderr
		os.Stderr = original //nolint:reassign // The fixture restores process stderr after testing dependency-log capture.
		if captured != original && captured != raw {
			_ = captured.Close()
		}
		if raw != nil {
			{
				err := raw.Close()
				assert.ErrorIf(t, err != nil, "%v", err)
			}
		}
		barSink = oldBar
		slog.SetDefault(oldLogger)
	}()
	var out progressOutput
	barSink = &barWriter{out: &out}
	slog.SetDefault(slog.New(slog.NewTextHandler(barSink, &slog.HandlerOptions{Level: slog.LevelDebug})))
	path := filepath.Join(t.TempDir(), "absent")
	switch *name {
	case "fatal-construction":
		path = ""
	case "fatal-bootstrap":
	default:
		path = writeFixture(t)
	}
	if (*name) == "partial-render" {
		testutil.WriteFile(t, path, "apps/kustomization.yaml", "resources: [missing.yaml]\n")
	}
	expected := original
	if (*name) == "render-warning" {
		testutil.WriteFile(
			t,
			path,
			"apps/kustomization.yaml",
			"resources: [cm.yaml]\ncommonLabels: {progress-warning: test}\n",
		)
		f, err := os.CreateTemp(t.TempDir(), "stderr")
		assert.NoError(t, err)
		raw = f
		os.Stderr = raw //nolint:reassign // The fixture redirects process stderr to exercise dependency-log capture.
		expected = raw
	}
	flags := commonFlags{path: path, concurrency: 2}
	var selected *selectedInvocation
	err := runSelected(t.Context(), selectedRequest{
		flags: flags, roots: flags.roots("", runnableKinds...), stdout: io.Discard, stderr: io.Discard,
	}, func(inv *selectedInvocation) selectedPayload {
		selected = inv
		return selectedPayload{}
	})
	switch *name {
	case "fatal-construction", "fatal-bootstrap":
		assert.FatalIf(
			t,
			err == nil || selected != nil,
			"fatal run exposed selection=%v, err=%v",
			selected,
			err,
		)
	case "partial-render":
		assert.FatalIf(t, err == nil || selected == nil || len(selected.groups) != 1 ||
			len(selected.groups[0].sides[0].Failed) == 0, "partial run lost selection: %+v", selected)
	default:
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			selected == nil || len(selected.groups) != 1 || len(selected.groups[0].sides[0].Objects) == 0,
			"%v",
			"successful render returned nil",
		)
	}
	if (*name) == "render-warning" {
		const warning = "# Warning: 'commonLabels' is deprecated."
		{
			got := out.String()
			assert.ErrorIf(t, !strings.Contains(got, warning), "render warning missing from sink: %q", got)
		}
		got, err := os.ReadFile(raw.Name())
		assert.NoError(t, err)
		assert.ErrorIf(
			t,
			strings.Contains(string(got), warning),
			"render warning leaked to raw stderr: %q",
			got,
		)
	}
	assert.ErrorIf(t, os.Stderr != expected, "%v", "shared helper left stderr captured")
	assert.ErrorIf(t, progressProgram(t, barSink) != nil, "%v", "shared helper left progress attached")
}
