package cli

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_FlagInventory(t *testing.T) {
	names := map[string]bool{}
	for _, route := range []string{"", "build all", "get ks", "get images", "test all", "diff all", "diff images", "cache gc", "completion fish"} {
		args := append(strings.Fields(route), "--help")
		help, diagnostic, code := runCLI(t, args...)
		assert.FatalIf(t, code != 0, "help %q: %s", route, diagnostic)
		for line := range strings.SplitSeq(help, "\n") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			token := fields[0]
			if len(fields) > 1 && strings.HasSuffix(token, ",") {
				token = fields[1]
			}
			if after, ok := strings.CutPrefix(token, "--"); ok {
				names[after] = true
			}
		}
		assert.FatalIf(t, strings.Contains(help, "--base diff"), "incorrect base metavar: %s", help)
	}
	if len(names) != 30 {
		t.Fatalf("distinct flags=%d: %v", len(names), slices.Sorted(maps.Keys(names)))
	}
}
