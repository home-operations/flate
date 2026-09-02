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

func TestSuppressFailed(t *testing.T) {
	other := cmDoc(ksParent, "keep", "apps", "v", "")
	reason := "chart source HelmChart/monitoring/grafana not ready:\n  digest mismatch"
	cases := []struct {
		name                    string
		left, right             []Doc
		leftFailed, rightFailed map[manifest.NamedResource]string
		wantLeft, wantRight     int
		wantSuppressed          []Suppression
	}{
		{
			name:        "current side failed: orig docs withheld",
			left:        append(hrDocs(3), other),
			right:       []Doc{other},
			rightFailed: map[manifest.NamedResource]string{hrID: reason},
			wantLeft:    1, wantRight: 1,
			wantSuppressed: []Suppression{{Parent: hrParent, Side: SideCurrent, Count: 3, Reason: reason}},
		},
		{
			name:       "orig side failed: current docs withheld",
			left:       []Doc{other},
			right:      append(hrDocs(2), other),
			leftFailed: map[manifest.NamedResource]string{hrID: reason},
			wantLeft:   1, wantRight: 1,
			wantSuppressed: []Suppression{{Parent: hrParent, Side: SideOrig, Count: 2, Reason: reason}},
		},
		{
			name:        "stale artifact on the failing side is dropped too, not counted",
			left:        hrDocs(3),
			right:       hrDocs(1),
			rightFailed: map[manifest.NamedResource]string{hrID: reason},
			wantLeft:    0, wantRight: 0,
			wantSuppressed: []Suppression{{Parent: hrParent, Side: SideCurrent, Count: 3, Reason: reason}},
		},
		{
			name:        "failed on both sides: nothing to suppress",
			left:        hrDocs(1),
			right:       hrDocs(1),
			leftFailed:  map[manifest.NamedResource]string{hrID: reason},
			rightFailed: map[manifest.NamedResource]string{hrID: reason},
			wantLeft:    1, wantRight: 1,
		},
		{
			name:        "failed but absent on the healthy side: no note",
			left:        []Doc{other},
			right:       []Doc{other},
			rightFailed: map[manifest.NamedResource]string{hrID: reason},
			wantLeft:    1, wantRight: 1,
		},
		{
			name:     "no failures: passthrough",
			left:     hrDocs(2),
			right:    hrDocs(2),
			wantLeft: 2, wantRight: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, r, s := SuppressFailed(tc.left, tc.right, tc.leftFailed, tc.rightFailed)
			if len(l) != tc.wantLeft || len(r) != tc.wantRight {
				t.Errorf("kept left=%d right=%d, want %d/%d", len(l), len(r), tc.wantLeft, tc.wantRight)
			}
			if len(s) != len(tc.wantSuppressed) {
				t.Fatalf("suppressed = %+v, want %+v", s, tc.wantSuppressed)
			}
			for i := range s {
				if s[i] != tc.wantSuppressed[i] {
					t.Errorf("suppressed[%d] = %+v, want %+v", i, s[i], tc.wantSuppressed[i])
				}
			}
		})
	}
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

// TestRenderDocs_SuppressionHeader pins that every format leads with the
// suppression note, that the withheld objects are absent from the body, and
// that a suppression alone (empty diff) still produces output.
func TestRenderDocs_SuppressionHeader(t *testing.T) {
	left := append(hrDocs(2), cmDoc(ksParent, "keep", "apps", "old", ""))
	right := []Doc{cmDoc(ksParent, "keep", "apps", "new", "")}
	l, r, suppressed := SuppressFailed(left, right, nil, map[manifest.NamedResource]string{hrID: "chart not ready"})
	if len(suppressed) != 1 {
		t.Fatalf("expected one suppression, got %+v", suppressed)
	}
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
			if strings.Contains(s, "cm-a") {
				t.Errorf("withheld object leaked into the body:\n%s", s)
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
		if strings.Contains(string(out), "cm-a") {
			t.Errorf("withheld object leaked into the html body")
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
