package diff

import (
	"cmp"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

var (
	hrParent = Parent{NamedResource: testutil.HelmReleaseID("monitoring", "grafana-operator")}
	hrID     = testutil.HelmReleaseID("monitoring", "grafana-operator")
	ksParent = Parent{NamedResource: testutil.KustomizationID("flux-system", "apps"), Path: "apps"}
)

func hrDocs(n int) []Doc {
	out := make([]Doc, 0, n)
	for i := range n {
		out = append(out, cmDoc(hrParent, manifest.NamedResource{Name: "cm-" + string(rune('a'+i)), Namespace: "monitoring"}, "v", ""))
	}
	return out
}

func TestSuppression_String(t *testing.T) {
	t.Parallel()
	s := Suppression{Parent: hrParent, Side: SideCurrent, Count: 11, Reason: "chart source\n  not ready"}
	want := "HelmRelease.helm.toolkit.fluxcd.io monitoring/grafana-operator: 11 objects not rendered on the current side; diff suppressed (chart source not ready)"
	if got := s.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got := (Suppression{Parent: ksParent, Side: SideOrig, Count: 1, Reason: "x"}).String(); !strings.Contains(got, "1 object not") {
		t.Errorf("singular noun expected, got %q", got)
	}
	for _, tc := range []struct {
		name   string
		parent Parent
		want   string
	}{
		{"core", Parent{Kind: "ConfigMap", Namespace: "ns", Name: "shared"}, "ConfigMap ns/shared: 1 object withheld; diff suppressed (reason)"},
		{"foreign group", Parent{Group: "example.test", Kind: "ConfigMap", Namespace: "ns", Name: "shared"}, "ConfigMap.example.test ns/shared: 1 object withheld; diff suppressed (reason)"},
		{"cluster", Parent{Group: "example.test", Kind: "Widget", Name: "shared", Path: "ignored/path"}, "Widget.example.test shared: 1 object withheld; diff suppressed (reason)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := (Suppression{Parent: tc.parent, Side: SideBoth, Count: 1, Reason: "reason"}).String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
		})
	}

}

func TestSuppression_StringBothAndEmpty(t *testing.T) {
	t.Parallel()
	cases := []struct {
		note Suppression
		want string
	}{
		{Suppression{Parent: hrParent, Side: SideBoth, Count: 1, Reason: "failed on both sides: orig: old;\n current: new"},
			"HelmRelease.helm.toolkit.fluxcd.io monitoring/grafana-operator: 1 object withheld; diff suppressed (failed on both sides: orig: old; current: new)"},
		{Suppression{Parent: hrParent, Side: SideBoth, Reason: "suspended on orig; suspended on current"},
			"HelmRelease.helm.toolkit.fluxcd.io monitoring/grafana-operator: 0 objects withheld; diff suppressed (suspended on orig; suspended on current)"},
		{Suppression{Parent: hrParent, Side: SideCurrent, Reason: "suspended on current"},
			"HelmRelease.helm.toolkit.fluxcd.io monitoring/grafana-operator: 0 objects not rendered on the current side; diff suppressed (suspended on current)"},
	}
	for _, tc := range cases {
		if got := tc.note.String(); got != tc.want {
			t.Errorf("String() = %q; want %q", got, tc.want)
		}
	}
}

// Suppression headers must remain visible even when no changes render.
func TestRenderDocs_SuppressionHeader(t *testing.T) {
	t.Parallel()
	parent := ksParent
	parent.Path = "apps/<path>"
	l := []Doc{cmDoc(parent, manifest.NamedResource{Name: "keep", Namespace: "apps"}, "old", ""), cmDoc(parent, manifest.NamedResource{Name: "other", Namespace: "apps"}, "before", "")}
	r := []Doc{cmDoc(parent, manifest.NamedResource{Name: "keep", Namespace: "apps"}, "new", ""), cmDoc(parent, manifest.NamedResource{Name: "other", Namespace: "apps"}, "after", "")}
	suppressed := []Suppression{
		{Parent: hrParent, Side: SideCurrent, Count: 2, Reason: "chart not ready"},
		{Parent: ksParent, Side: SideOrig, Count: 1, Reason: "suspended on orig"},
	}
	note := suppressed[0].String()
	const humanBody = `
data.k  (v1/ConfigMap/apps/keep)
  ± value change
    - old
    + new

data.k  (v1/ConfigMap/apps/other)
  ± value change
    - before
    + after

`

	for _, tc := range []struct {
		format Format
		prefix string
		body   string
	}{
		{"", "⚠ ", humanBody},
		{FormatHuman, "⚠ ", humanBody},
		{FormatGitHub, "! ", `
@@ data.k @@
# v1/ConfigMap/apps/keep
! ± value change
- old
+ new

@@ data.k @@
# v1/ConfigMap/apps/other
! ± value change
- before
+ after

`},
		{FormatGitLab, "# ", `
= data.k
= v1/ConfigMap/apps/keep
# ± value change
- old
+ new

= data.k
= v1/ConfigMap/apps/other
# ± value change
- before
+ after

`},
		{FormatGitea, "! ", `
@@ data.k @@
= v1/ConfigMap/apps/keep
! ± value change
- old
+ new

@@ data.k @@
= v1/ConfigMap/apps/other
! ± value change
- before
+ after

`},
		{FormatBrief, "⚠ ", "two changes detected between from, two documents and to, two documents\n\n"},
		{FormatDiff, "# ", `--- ConfigMap apps/keep (apps/<path>)
+++ ConfigMap apps/keep (apps/<path>)
@@ -1,6 +1,6 @@
 apiVersion: v1
 data:
-  k: old
+  k: new
 kind: ConfigMap
 metadata:
   name: keep

--- ConfigMap apps/other (apps/<path>)
+++ ConfigMap apps/other (apps/<path>)
@@ -1,6 +1,6 @@
 apiVersion: v1
 data:
-  k: before
+  k: after
 kind: ConfigMap
 metadata:
   name: other
`},
	} {
		t.Run(cmp.Or(string(tc.format), "default"), func(t *testing.T) {
			t.Parallel()
			out, err := RenderDocs(l, r, Options{Format: tc.format, Suppressed: suppressed})
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			header := tc.prefix + note + "\n" + tc.prefix + suppressed[1].String() + "\n"
			body, err := RenderDocs(l, r, Options{Format: tc.format})
			if err != nil {
				t.Fatalf("body: %v", err)
			}
			if string(body) != tc.body {
				t.Errorf("body = %q, want %q", body, tc.body)
			}
			if want := header + "\n" + tc.body; string(out) != want {
				t.Errorf("output = %q, want %q", out, want)
			}
			out, err = RenderDocs(nil, nil, Options{Format: tc.format, Suppressed: suppressed})
			if err != nil || string(out) != header {
				t.Errorf("notes only = %q, %v; want %q, nil", out, err, header)
			}
			out, err = RenderDocs(nil, nil, Options{Format: tc.format})
			if err != nil || len(out) != 0 {
				t.Errorf("empty = %q, %v; want empty bytes, nil", out, err)
			}
		})
	}

	t.Run("html", func(t *testing.T) {
		t.Parallel()
		out, err := RenderDocs(l, r, Options{Format: FormatHTML, Suppressed: suppressed})
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if !strings.Contains(string(out), `class="suppressed"`) || !strings.Contains(string(out), "not rendered on the current side") {
			t.Errorf("html should carry the suppression banner")
		}
	})

	t.Run("suppression alone still emits", func(t *testing.T) {
		t.Parallel()
		out, err := RenderDocs(nil, nil, Options{Format: FormatHTML, Suppressed: suppressed})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(out), "not rendered on the current side") {
			t.Errorf("an otherwise-empty HTML diff must still disclose the suppression; got %q", out)
		}
	})
}
