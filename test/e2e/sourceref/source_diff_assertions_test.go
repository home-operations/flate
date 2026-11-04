package sourceref_test

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func assertSourceValueDiff(t *testing.T, out, before, after string) {
	t.Helper()
	assert.FatalIf(t, strings.Count(out, "-  value: "+before) != 1 || strings.Count(out, "+  value: "+after) != 1 ||
		strings.Contains(
			out,
			"+apiVersion:",
		) || strings.Contains(out, "+kind: ConfigMap"), "expected exact field removal/addition for %s -> %s:\n%s", before, after, out)
}
