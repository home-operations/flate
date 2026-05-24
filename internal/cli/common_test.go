package cli

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/format"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestScopedNamespaces_ExplicitNamespaceWins(t *testing.T) {
	c := &commonFlags{namespace: "media"}
	got := c.scopedNamespaces(&change.Filter{})
	if _, ok := got["media"]; !ok || len(got) != 1 {
		t.Errorf("explicit -n media not honored: %v", got)
	}
}

func TestScopedNamespaces_PathOrigAutoScopesToKeepSet(t *testing.T) {
	c := &commonFlags{namespace: ""}
	mediaHR := &manifest.HelmRelease{Name: "x", Namespace: "media"}
	netHR := &manifest.HelmRelease{Name: "y", Namespace: "networking"}
	f := change.NewFilter(
		change.NewSet([]string{"media.yaml", "networking.yaml"}),
		map[manifest.NamedResource]string{
			mediaHR.Named(): "media.yaml",
			netHR.Named():   "networking.yaml",
		},
		"",
		emptyLister{},
	)
	got := c.scopedNamespaces(f)
	for _, want := range []string{"media", "networking"} {
		if _, ok := got[want]; !ok {
			t.Errorf("auto-scope missing %q: got=%v", want, got)
		}
	}
}

// emptyLister satisfies change.ObjectLister with empty results — used
// for filter resolution tests where transitive deps aren't exercised.
type emptyLister struct{}

func (emptyLister) GetObject(manifest.NamedResource) manifest.BaseManifest { return nil }
func (emptyLister) ListObjects(string) []manifest.BaseManifest             { return nil }

func TestScopedNamespaces_NoFilterMeansAll(t *testing.T) {
	c := &commonFlags{namespace: ""}
	// Disabled filter (Changes == nil) → no scope (all namespaces).
	if got := c.scopedNamespaces(&change.Filter{}); got != nil {
		t.Errorf("expected nil (all-namespaces), got %v", got)
	}
}

func TestIncludeNamespace_ClusterScopedAlwaysIncluded(t *testing.T) {
	c := &commonFlags{namespace: "media"}
	if !c.includeNamespace(&change.Filter{}, "") {
		t.Error("cluster-scoped (empty) namespace must always pass")
	}
}

func TestIncludeNamespace_RespectsExplicitFilter(t *testing.T) {
	c := &commonFlags{namespace: "media"}
	if !c.includeNamespace(&change.Filter{}, "media") {
		t.Error("matching namespace must pass")
	}
	if c.includeNamespace(&change.Filter{}, "default") {
		t.Error("non-matching namespace must fail")
	}
}

// TestRequireOutput_AcceptsDefaultTableAndAllowed pins the
// short-circuit shape: "table" always passes (subcommand coerces via
// outputOrDefault), and anything in the allowed set passes.
func TestRequireOutput_AcceptsDefaultTableAndAllowed(t *testing.T) {
	for _, out := range []string{"table", "yaml", "json"} {
		c := &commonFlags{output: out}
		if err := c.requireOutput(format.OutputYAML, format.OutputJSON); err != nil {
			t.Errorf("output=%q: unexpected error %v", out, err)
		}
	}
}

// TestRequireOutput_RejectsUnsupported guards the diff/drift fix:
// `get all -o name` and `get images -o diff` used to silently coerce
// into a different format — now they fail loud. Empty allowed-set
// (test's pattern) must reject every non-default `-o`.
func TestRequireOutput_RejectsUnsupported(t *testing.T) {
	cases := []struct {
		output  string
		allowed []format.Output
		wantSub string
	}{
		{output: "name", allowed: []format.Output{format.OutputYAML, format.OutputJSON}, wantSub: `"name"`},
		{output: "yaml", allowed: nil, wantSub: "want one of: table"},
		{output: "json", allowed: []format.Output{format.OutputName}, wantSub: "table, name"},
	}
	for _, tc := range cases {
		c := &commonFlags{output: tc.output}
		err := c.requireOutput(tc.allowed...)
		if err == nil {
			t.Errorf("output=%q allowed=%v: expected error, got nil", tc.output, tc.allowed)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("output=%q: error %q missing substring %q", tc.output, err.Error(), tc.wantSub)
		}
	}
}
