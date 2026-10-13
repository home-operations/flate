package diff

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

var (
	hrParent = Parent{Kind: "HelmRelease", Namespace: "monitoring", Name: "grafana-operator"}
	hrID     = manifest.NamedResource{Kind: "HelmRelease", Namespace: "monitoring", Name: "grafana-operator"}
	ksParent = Parent{Kind: "Kustomization", Namespace: "flux-system", Name: "apps", Path: "apps"}
)

func hrDocs(n int) []Doc {
	out := make([]Doc, 0, n)
	for i := range n {
		out = append(out, cmDoc(hrParent, "cm-"+string(rune('a'+i)), "monitoring", "v", ""))
	}
	return out
}

func TestSuppression_String(t *testing.T) {
	s := Suppression{Parent: hrParent, Side: SideCurrent, Count: 11, Reason: "chart source\n  not ready"}
	want := "HelmRelease monitoring/grafana-operator: 11 objects not rendered on the current side; diff suppressed (chart source not ready)"
	if got := s.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got := (Suppression{Parent: ksParent, Side: SideOrig, Count: 1, Reason: "x"}).String(); !strings.Contains(got, "1 object not") {
		t.Errorf("singular noun expected, got %q", got)
	}
}

func TestSuppression_StringBothAndEmpty(t *testing.T) {
	cases := []struct {
		note Suppression
		want string
	}{
		{Suppression{Parent: hrParent, Side: SideBoth, Count: 1, Reason: "failed on both sides: orig: old;\n current: new"},
			"HelmRelease monitoring/grafana-operator: 1 object withheld; diff suppressed (failed on both sides: orig: old; current: new)"},
		{Suppression{Parent: hrParent, Side: SideBoth, Reason: "suspended on orig; suspended on current"},
			"HelmRelease monitoring/grafana-operator: 0 objects withheld; diff suppressed (suspended on orig; suspended on current)"},
		{Suppression{Parent: hrParent, Side: SideCurrent, Reason: "suspended on current"},
			"HelmRelease monitoring/grafana-operator: 0 objects not rendered on the current side; diff suppressed (suspended on current)"},
	}
	for _, tc := range cases {
		if got := tc.note.String(); got != tc.want {
			t.Errorf("String() = %q; want %q", got, tc.want)
		}
	}
}

// Suppression headers must remain visible even when no changes render.
func TestRenderDocs_SuppressionHeader(t *testing.T) {
	l := []Doc{cmDoc(ksParent, "keep", "apps", "old", "")}
	r := []Doc{cmDoc(ksParent, "keep", "apps", "new", "")}
	suppressed := []Suppression{{Parent: hrParent, Side: SideCurrent, Count: 2, Reason: "chart not ready"}}
	note := suppressed[0].String()

	for _, tc := range []struct {
		format Format
		prefix string
	}{
		{FormatHuman, "⚠ "},
		{FormatGitHub, "! "},
		{FormatGitLab, "# "},
		{FormatGitea, "! "},
		{FormatBrief, "⚠ "},
		{FormatDiff, "# "},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			out, err := RenderDocs(l, r, Options{Format: tc.format, Suppressed: suppressed})
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			s := string(out)
			if !strings.HasPrefix(s, tc.prefix+note+"\n") {
				t.Errorf("output should lead with the note %q; got:\n%s", tc.prefix+note, s)
			}
			if strings.TrimSpace(strings.TrimPrefix(s, tc.prefix+note)) == "" {
				t.Errorf("unrelated change should still render after the note:\n%s", s)
			}
		})
	}

	t.Run("html", func(t *testing.T) {
		out, err := RenderDocs(l, r, Options{Format: FormatHTML, Suppressed: suppressed})
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if !strings.Contains(string(out), `class="suppressed"`) || !strings.Contains(string(out), "not rendered on the current side") {
			t.Errorf("html should carry the suppression banner")
		}
	})

	t.Run("suppression alone still emits", func(t *testing.T) {
		for _, f := range []Format{FormatHuman, FormatGitHub, FormatDiff, FormatHTML} {
			out, err := RenderDocs(nil, nil, Options{Format: f, Suppressed: suppressed})
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if !strings.Contains(string(out), "not rendered on the current side") {
				t.Errorf("%s: an otherwise-empty diff must still disclose the suppression; got %q", f, out)
			}
		}
	})
}
