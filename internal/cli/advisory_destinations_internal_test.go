package cli

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func runAdvisoryDestinationsCase(t *testing.T, command *[]string, k8s *string) {
	t.Helper()
	const advisory = "could not be read offline"

	var firstOut, firstErr string
	for run := range 2 {
		args := append(slices.Clone((*command)), "--path", (*k8s), "--concurrency", "2")
		if (*command)[0] != "test" {
			args = append(args, "-o", "json")
		}
		stdout, stderr, code := runCLI(t, args...)
		assert.Equal(t, code, 0)
		diagnostic, data := stderr, stdout
		assert.Equal(t, strings.Count(diagnostic, advisory), 1)
		assert.Equal(t, strings.Count(diagnostic, "[UnresolvedSubstitution]"), 1)
		assert.Equal(t, strings.Contains(data, advisory), false)
		normalized := stdout
		if (*command)[0] == "test" {
			normalized = regexp.MustCompile(`   [0-9]+\.[0-9]+s\n$`).ReplaceAllString(stdout, "")
		} else {
			assert.FatalIf(t, !json.Valid([]byte(stdout)), "invalid payload: %s", stdout)
		}
		if run == 0 {
			firstOut, firstErr = normalized, stderr
		} else {
			assert.Equal(t, normalized, firstOut)
			assert.Equal(t, stderr, firstErr)
		}
	}
	for _, level := range []string{"debug", "info", "warn", "error"} {
		args := append(slices.Clone((*command)), "--path", (*k8s), "--concurrency=2", "--log-level", level)
		stdout, stderr, code := runCLI(t, args...)
		assert.FatalIf(t, code != 0 || strings.Count(stderr, "[UnresolvedSubstitution]") != 1 ||
			strings.Count(stderr, advisory) != 1 ||
			strings.Contains(stdout, advisory) ||
			strings.Contains(
				stderr,
				"level=WARN",
			), "%s destination: exit=%d stdout=%s stderr=%s", level, code, stdout, stderr)
	}
}
