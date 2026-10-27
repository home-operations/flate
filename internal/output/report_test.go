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
	m := Model{}
	var b bytes.Buffer
	if err := writeModel(Encoder{Writer: &b}, m, warns...); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"warnings (3)", "HelmRelease default/app: values not defined", "image.tagg, oldKey", "[StaleValues]", "[EmptyScan]", "no Flux objects found", "    SDK warning\n"} {
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
	_ = writeModel(Encoder{Writer: &b}, Model{}, warns...)
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
	for _, context := range []string{"", "current snapshot"} {
		t.Run("context_"+context, func(t *testing.T) {
			t.Parallel()
			m := m
			m.Context = context
			prefix := ""
			if context != "" {
				prefix = context + ": "
			}
			for _, color := range []bool{false, true} {
				t.Run(fmt.Sprint(color), func(t *testing.T) {
					t.Parallel()
					want := "\n  " + Fail(GlyphFail, color) + "  " + Dim(prefix+"Kustomization", color) + "  flux-system/root  may not add resource with an already registered id: X\n" +
						"         duplicate resource(s) produced by multiple accumulated paths:\n         - ./one\n         - ./two" +
						"\n         " + Dim(prefix+"blocks flux-system/leaf, flux-system/mid", color) +
						"\n  " + Fail(GlyphFail, color) + "  " + Dim(prefix+"Kustomization", color) + "  flux-system/missing  " + Fail("not found", color) +
						"\n         " + Dim(prefix+"required by flux-system/dependent", color) +
						"\n\n  " + Fail(GlyphFail, color) + " " + Fail("2 failed", color) + " · " + Dim("3 blocked", color) + "\n"
					var b bytes.Buffer
					if err := writeModel(Encoder{Writer: &b, Color: color}, m); err != nil {
						t.Fatal(err)
					}
					if got := b.String(); got != want {
						t.Fatalf("report = %q, want %q", got, want)
					}
				})
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
		{"same name, different kind", []manifest.NamedResource{testutil.HelmReleaseID("flux-system", "acars"), testutil.KustomizationID("flux-system", "acars")}, "HelmRelease flux-system/acars, Kustomization flux-system/acars"},
		{"same name, different group", []manifest.NamedResource{{Group: "example.test", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "acars"}, testutil.KustomizationID("flux-system", "acars")}, "Kustomization.example.test flux-system/acars, Kustomization.kustomize.toolkit.fluxcd.io flux-system/acars"},
		{"same name, different namespace", []manifest.NamedResource{testutil.HelmReleaseID("default", "acars"), testutil.KustomizationID("flux-system", "acars")}, "default/acars, flux-system/acars"},
		{"collision hidden by the cap", []manifest.NamedResource{testutil.HelmReleaseID("flux-system", "zzz"), testutil.KustomizationID("flux-system", "a"), testutil.KustomizationID("flux-system", "b"), testutil.KustomizationID("flux-system", "c"), testutil.KustomizationID("flux-system", "d"), testutil.KustomizationID("flux-system", "e"), testutil.KustomizationID("flux-system", "zzz")}, "flux-system/zzz, flux-system/a, flux-system/b, flux-system/c, flux-system/d, flux-system/e +1 more"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			report := Report{Groups: []ReportGroup{{Models: []Model{{Primary: []Primary{{Blocks: tc.ids}}}}}}}
			names := report.Prepare(nil)
			if got := summarize(tc.ids, names[0]); got != tc.want {
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
	if err := writeModel(Encoder{Writer: errorWriter{writeErr}}, Model{}, manifest.Warning{Message: "advisory"}); !errors.Is(err, writeErr) {
		t.Fatalf("report writer error = %v, want %v", err, writeErr)
	}
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

func writeModel(e Encoder, m Model, warnings ...manifest.Warning) error {
	r := Report{Groups: []ReportGroup{{Models: []Model{m}}}}
	for _, w := range warnings {
		r.Warnings = append(r.Warnings, Warning{Warning: w})
	}
	r.Prepare(nil)
	return e.Report(r, e)
}

func TestEncoderReport_LocalAgentInformation(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	warning := manifest.Warning{Resource: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}, Category: manifest.WarnLocalSSHAgent, Message: "authenticated with the local SSH agent", Count: 1}
	if err := writeModel(Encoder{Writer: &output}, Model{}, warning); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "information (1)") || !strings.Contains(text, "[LocalSSHAgent]") || strings.Contains(text, "warnings (") || strings.Contains(text, "failed") {
		t.Fatalf("agent information = %q", text)
	}
}

func TestEncoderReport_AdvisoryNames(t *testing.T) {
	t.Parallel()
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
	other := source
	other.Group = "custom.example"
	agent := manifest.Warning{Resource: source, Category: manifest.WarnLocalSSHAgent, Message: "authenticated with the local SSH agent", Count: 1}
	ordinary := manifest.Warning{Resource: source, Category: manifest.WarnPathConfig, Message: "ordinary", Count: 3}
	collision := ordinary
	collision.Resource = other
	for _, tc := range []struct {
		name     string
		groups   []ReportGroup
		warnings []Warning
		want     []string
		absent   []string
	}{
		{
			name:     "two retained clusters",
			groups:   []ReportGroup{{Cluster: "alpha"}, {Cluster: "omega"}},
			warnings: []Warning{{Warning: ordinary}, {Warning: agent}, {Warning: ordinary, Group: 1}, {Warning: agent, Group: 1}},
			want:     []string{"warnings (2)", "information (2)", "GitRepository alpha: apps/source: ordinary (×3)", "GitRepository omega: apps/source: ordinary (×3)", "GitRepository alpha: apps/source: authenticated", "GitRepository omega: apps/source: authenticated"},
			absent:   []string{"GitRepository.source.toolkit.fluxcd.io", "Cluster:", GlyphFail},
		},
		{
			name:     "one retained cluster",
			groups:   []ReportGroup{{Cluster: "omega"}},
			warnings: []Warning{{Warning: ordinary}, {Warning: agent}},
			want:     []string{"warnings (1)", "information (1)", "GitRepository apps/source: ordinary (×3)", "GitRepository apps/source: authenticated"},
			absent:   []string{"omega:", "failed", "Cluster:"},
		},
		{
			name:     "kind collision",
			groups:   []ReportGroup{{Cluster: "alpha"}, {Cluster: "omega"}},
			warnings: []Warning{{Warning: agent}, {Warning: collision, Group: 1}},
			want:     []string{"warnings (1)", "information (1)", "GitRepository.source.toolkit.fluxcd.io alpha: apps/source: authenticated", "GitRepository.custom.example omega: apps/source: ordinary (×3)"},
			absent:   []string{"Cluster:", GlyphFail},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			report := Report{Groups: tc.groups, Warnings: tc.warnings}
			report.Prepare(nil)
			var b bytes.Buffer
			encoder := Encoder{Writer: &b}
			if err := encoder.Report(report, encoder); err != nil {
				t.Fatal(err)
			}
			for _, want := range tc.want {
				if strings.Count(b.String(), want) != 1 {
					t.Errorf("missing or duplicate %q: %s", want, b.String())
				}
			}
			for _, absent := range tc.absent {
				if strings.Contains(b.String(), absent) {
					t.Errorf("unexpected %q: %s", absent, b.String())
				}
			}
		})
	}
}
