package output

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEncoderReport_WarningsSection(t *testing.T) {
	t.Parallel()
	hr := testutil.HelmReleaseID("default", "app")
	warns := []manifest.Warning{
		{Resource: hr, Category: manifest.WarnStaleValues, Message: "values not defined by the chart schema", Detail: []string{"image.tagg", "oldKey"}},
		{Category: manifest.WarnEmptyScan, Message: "no Flux objects found"},
		{Message: "SDK warning"},
	}
	m := Model{Warnings: warns}
	if m.Empty() {
		t.Fatal("a model with warnings is not empty")
	}
	var b bytes.Buffer
	if err := (Encoder{Writer: &b}).Report(m); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"warnings (3)", "HelmRelease.helm.toolkit.fluxcd.io default/app: values not defined", "image.tagg, oldKey", "[StaleValues]", "[EmptyScan]", "no Flux objects found", "    SDK warning\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("warnings section missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "[]") {
		t.Fatalf("empty category emitted brackets: %s", out)
	}
	if strings.Contains(out, "failed") {
		t.Errorf("a warnings-only run must not render a failure verdict:\n%s", out)
	}
}

func TestEncoderReport_WarningCount(t *testing.T) {
	t.Parallel()
	warns := []manifest.Warning{{Category: manifest.WarnPathConfig, Message: "same root", Count: 4}}
	var b bytes.Buffer
	_ = (Encoder{Writer: &b}).Report(Model{Warnings: warns})
	if out := b.String(); !strings.Contains(out, "×4") {
		t.Errorf("warning repeat count not rendered:\n%s", out)
	}
}

func TestEncoderReport_PrimaryMessageIsMultiLine(t *testing.T) {
	t.Parallel()
	m := Model{
		Primary: []Primary{{ID: testutil.KustomizationID("flux-system", "root"), Msg: " may not add resource with an already registered id: X \n\t\n" +
			"duplicate resource(s) produced by multiple accumulated paths:\n  - ./one\n  - ./two ", Blocks: []manifest.NamedResource{testutil.KustomizationID("flux-system", "leaf"), testutil.KustomizationID("flux-system", "mid")}}},
		Missing: []Missing{{ID: testutil.KustomizationID("flux-system", "missing"), RequiredBy: []manifest.NamedResource{testutil.KustomizationID("flux-system", "dependent")}}},
		Blocked: 3,
	}
	for _, color := range []bool{false, true} {
		t.Run(fmt.Sprint(color), func(t *testing.T) {
			t.Parallel()
			want := "\n  " + Fail(GlyphFail, color) + "  " + Dim("Kustomization.kustomize.toolkit.fluxcd.io", color) + "  flux-system/root  may not add resource with an already registered id: X\n" +
				"         duplicate resource(s) produced by multiple accumulated paths:\n         - ./one\n         - ./two" +
				"\n         " + Dim("blocks flux-system/leaf, flux-system/mid", color) +
				"\n  " + Fail(GlyphFail, color) + "  " + Dim("Kustomization.kustomize.toolkit.fluxcd.io", color) + "  flux-system/missing  " + Fail("not found", color) +
				"\n         " + Dim("required by flux-system/dependent", color) +
				"\n\n  " + Fail(GlyphFail, color) + " " + Fail("2 failed", color) + " · " + Dim("3 blocked", color) + "\n"
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Color: color}).Report(m); err != nil {
				t.Fatal(err)
			}
			if got := b.String(); got != want {
				t.Fatalf("report = %q, want %q", got, want)
			}
		})
	}
}

func TestSummarize_KindOnCollision(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ids  []manifest.NamedResource // sorted, as Build delivers them
		want string
	}{
		{"same name, different kind", []manifest.NamedResource{testutil.HelmReleaseID("flux-system", "acars"), testutil.KustomizationID("flux-system", "acars")}, "HelmRelease.helm.toolkit.fluxcd.io flux-system/acars, Kustomization.kustomize.toolkit.fluxcd.io flux-system/acars"},
		{"same name, different group", []manifest.NamedResource{{Group: "example.test", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "acars"}, testutil.KustomizationID("flux-system", "acars")}, "Kustomization.example.test flux-system/acars, Kustomization.kustomize.toolkit.fluxcd.io flux-system/acars"},
		{"same name, different namespace", []manifest.NamedResource{testutil.HelmReleaseID("default", "acars"), testutil.KustomizationID("flux-system", "acars")}, "default/acars, flux-system/acars"},
		{"collision hidden by the cap", []manifest.NamedResource{testutil.HelmReleaseID("flux-system", "zzz"), testutil.KustomizationID("flux-system", "a"), testutil.KustomizationID("flux-system", "b"), testutil.KustomizationID("flux-system", "c"), testutil.KustomizationID("flux-system", "d"), testutil.KustomizationID("flux-system", "e"), testutil.KustomizationID("flux-system", "zzz")}, "HelmRelease.helm.toolkit.fluxcd.io flux-system/zzz, flux-system/a, flux-system/b, flux-system/c, flux-system/d, flux-system/e +1 more"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := summarize(tc.ids); got != tc.want {
				t.Errorf("summarize = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDocument(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		blocks []string
		want   string
	}{
		{"empty", []string{"", ""}, ""},
		{"none", nil, ""},
		{"single", []string{"a"}, "\na\n"},
		{"two", []string{"a", "b"}, "\na\n\nb\n"},
		{"drops empties between", []string{"a", "", "b"}, "\na\n\nb\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Document(tc.blocks...); got != tc.want {
				t.Errorf("Document(%q) = %q, want %q", tc.blocks, got, tc.want)
			}
		})
	}
}

func TestVerdict(t *testing.T) {
	t.Parallel()
	pass := func(s string, _ bool) string { return s }
	c := func(n int, label string) Count { return Count{N: n, Label: label, Paint: pass} }

	if got := Verdict(false, pass("✓", false), 0); got != "" {
		t.Errorf("no counts = %q, want empty", got)
	}
	if got := Verdict(false, pass("✓", false), 0, c(0, "passed")); got != "  ✓ 0 passed" {
		t.Errorf("lead-at-zero = %q", got)
	}
	if got := Verdict(false, pass("✓", false), 0,
		c(2, "passed"), c(0, "skipped"), c(3, "failed")); got != "  ✓ 2 passed · 3 failed" {
		t.Errorf("zero non-lead dropped = %q", got)
	}
	if got := Verdict(false, pass("✗", false), 0, c(2, "failed"), c(1, "blocked")); got != "  ✗ 2 failed · 1 blocked" {
		t.Errorf("distinct blocked count = %q", got)
	}
	if got := Verdict(false, pass("✗", false), 1500*time.Millisecond, c(1, "failed")); got != "  ✗ 1 failed   1.5s" {
		t.Errorf("elapsed = %q", got)
	}
}

func TestEncoderErrors_NonResourceDiagnostics(t *testing.T) {
	t.Parallel()
	closeErr := errors.New("cache close failed")
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{name: "none"},
		{name: "wrapped close", err: fmt.Errorf("close: %w", closeErr), want: "flate error: close: cache close failed\n"},
		{name: "joined", err: errors.Join(closeErr, errors.New("cancelled")), want: "flate error: cache close failed\ncancelled\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b}).Errors(tc.err); err != nil {
				t.Fatal(err)
			}
			if got := b.String(); got != tc.want {
				t.Fatalf("Errors = %q, want %q", got, tc.want)
			}
		})
	}
	writeErr := errors.New("write failed")
	if err := (Encoder{Writer: errorWriter{writeErr}}).Errors(closeErr); !errors.Is(err, writeErr) {
		t.Fatalf("writer error = %v, want %v", err, writeErr)
	}
	if err := (Encoder{Writer: errorWriter{writeErr}}).Report(Model{Warnings: []manifest.Warning{{Message: "advisory"}}}); !errors.Is(err, writeErr) {
		t.Fatalf("report writer error = %v, want %v", err, writeErr)
	}
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
