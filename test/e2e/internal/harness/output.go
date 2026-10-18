package harness

import (
	"strings"
	"testing"
)

// MustExtractLine finds the status line containing needle or fails the test.
func MustExtractLine(t *testing.T, haystack, needle string) string {
	t.Helper()
	return mustFindLine(t, haystack, needle, lineMatcher{
		kind: "status",
		match: func(line string) bool {
			return strings.Contains(line, "✓") || strings.Contains(line, "✗")
		},
	})
}

// MustExtractSkipLine finds the skipped line containing needle or fails the test.
func MustExtractSkipLine(t *testing.T, haystack, needle string) string {
	t.Helper()
	return mustFindLine(t, haystack, needle, lineMatcher{
		kind: "skipped",
		match: func(line string) bool {
			return strings.Contains(line, "‒")
		},
	})
}

type lineMatcher struct {
	kind  string
	match func(string) bool
}

func mustFindLine(t *testing.T, haystack, needle string, matcher lineMatcher) string {
	t.Helper()
	for line := range strings.SplitSeq(haystack, "\n") {
		if strings.Contains(line, needle) && matcher.match(line) {
			return line
		}
	}
	t.Fatalf("%s line for %q not found in:\n%s", matcher.kind, needle, haystack)
	return ""
}
