package output

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
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
		{Resource: hr, Category: manifest.WarnDiffSuppressed, Message: "1 object not rendered on the current side; diff suppressed (secret default/auth not found)"},
	}
	m := Model{}
	var b bytes.Buffer
	if err := writeModel(Encoder{Writer: &b}, m, warns...); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"warnings (4)", "HelmRelease default/app: values not defined", "image.tagg, oldKey", "[StaleValues]", "[EmptyScan]", "no Flux objects found", "    SDK warning\n"} {
		if !strings.Contains(out, want) {
			t.Errorf("warnings section missing %q:\n%s", want, out)
		}
	}
	if strings.Count(out, "HelmRelease default/app: 1 object") != 1 || strings.Count(out, "secret default/auth not found") != 1 {
		t.Fatalf("warning repeated identity or cause: %s", out)
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

func TestEncoderReport_AdvisoryNames(t *testing.T) {
	t.Parallel()
	source := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "source"}
	other := source
	other.Group = "custom.example"
	agent := manifest.Warning{Resource: source, Category: manifest.WarnSkippedSubmodule, Message: "submodule omitted", Count: 1}
	ordinary := manifest.Warning{Resource: source, Category: manifest.WarnPathConfig, Message: "ordinary", Count: 3}
	collision := ordinary
	collision.Resource = other
	global := ordinary
	global.Resource = manifest.NamedResource{}
	global.Detail = []string{"kept"}
	clusterAgent := agent
	clusterAgent.Resource = manifest.NamedResource{}
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
			want:     []string{"warnings (4)", "GitRepository alpha: apps/source: ordinary (×3)", "GitRepository omega: apps/source: ordinary (×3)", "GitRepository alpha: apps/source: submodule omitted", "GitRepository omega: apps/source: submodule omitted"},
			absent:   []string{"GitRepository.source.toolkit.fluxcd.io", "Cluster:", GlyphFail},
		},
		{
			name:     "one retained cluster",
			groups:   []ReportGroup{{Cluster: "omega"}},
			warnings: []Warning{{Warning: ordinary}, {Warning: agent}},
			want:     []string{"warnings (2)", "GitRepository apps/source: ordinary (×3)", "GitRepository apps/source: submodule omitted"},
			absent:   []string{"omega:", "failed", "Cluster:"},
		},
		{
			name:     "kind collision",
			groups:   []ReportGroup{{Cluster: "alpha"}, {Cluster: "omega"}},
			warnings: []Warning{{Warning: agent}, {Warning: collision, Group: 1}},
			want:     []string{"warnings (2)", "GitRepository.source.toolkit.fluxcd.io alpha: apps/source: submodule omitted", "GitRepository.custom.example omega: apps/source: ordinary (×3)"},
			absent:   []string{"Cluster:", GlyphFail},
		},
		{
			name:     "cluster globals and invocation notice",
			groups:   []ReportGroup{{Cluster: "alpha"}, {Cluster: "omega"}},
			warnings: []Warning{{Warning: global}, {Warning: clusterAgent}, {Warning: global, Group: 1}, {Warning: clusterAgent, Group: 1}, {Warning: manifest.Warning{Category: "Notice", Message: "invocation"}, Group: -1}},
			want:     []string{"alpha: ordinary (×3)", "omega: ordinary (×3)", "alpha: submodule omitted", "omega: submodule omitted", "[Notice] invocation"},
			absent:   []string{"orig snapshot", "current snapshot", "alpha: invocation", "omega: invocation"},
		},
		{
			name:     "one cluster global",
			groups:   []ReportGroup{{Cluster: "omega"}},
			warnings: []Warning{{Warning: global}, {Warning: clusterAgent}},
			want:     []string{"[PathConfig] ordinary (×3)", "[SkippedSubmodule] submodule omitted"},
			absent:   []string{"omega:", "snapshot"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := make([]manifest.Warning, len(tc.warnings))
			for i, warning := range tc.warnings {
				before[i] = warning.Warning
				before[i].Detail = slices.Clone(warning.Warning.Detail)
			}
			report := Report{Groups: tc.groups, Warnings: tc.warnings}
			report.Prepare(nil)
			for i, warning := range report.Warnings {
				assert.Diff(t, warning.Warning, before[i])
			}
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

func TestReport_SyntheticSubjectsRemainDistinct(t *testing.T) {
	t.Parallel()
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	first := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	second := first.WithNamespace("other")
	ids := []manifest.NamedResource{first.Named(), second.Named()}
	subjects := map[manifest.NamedResource]manifest.DiagnosticIdentity{}
	for _, obj := range []manifest.BaseManifest{first, second} {
		subjects[obj.Named()] = manifest.DiagnosticFor(obj.Named(), obj)
	}
	entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "clusters/test", manifest.NamedResource{})
	subjects[entry.Named()] = manifest.DiagnosticFor(entry.Named(), entry)
	report := Report{Groups: []ReportGroup{{Subjects: subjects, Models: []Model{{Primary: []Primary{{ID: ids[0], Msg: "first failed"}, {ID: ids[1], Msg: "second failed"}, {ID: entry.Named(), Msg: "root failed"}}}}}}}
	report.Prepare(nil)
	var out bytes.Buffer
	encoder := Encoder{Writer: &out}
	if err := encoder.Report(report, encoder); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if strings.Contains(text, "OCIRepository") || strings.Contains(text, "Kustomization entry") || !strings.Contains(text, `path "clusters/test"`) || strings.Count(text, "HelmChart") < 2 || !strings.Contains(text, "3 failed") {
		t.Fatalf("raw causes collapsed or leaked: %s", text)
	}
}

func TestReport_SnapshotSpecificAttribution(t *testing.T) {
	t.Parallel()
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "sources"}
	synthetic := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	raw := synthetic.Named()
	subjects := map[manifest.NamedResource]manifest.DiagnosticIdentity{raw: manifest.DiagnosticFor(raw, synthetic)}
	report := Report{Groups: []ReportGroup{{Subjects: subjects, Models: []Model{{Context: "orig snapshot", Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{}, Primary: []Primary{{ID: raw, Msg: "ordinary source failed"}}}, {Context: "current snapshot", Subjects: subjects, Primary: []Primary{{ID: raw, Msg: "synthetic source failed"}}}}}}}
	report.Prepare(nil)
	var out bytes.Buffer
	encoder := Encoder{Writer: &out}
	if err := encoder.Report(report, encoder); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "OCIRepository") || !strings.Contains(text, "HelmChart") || !strings.Contains(text, "ordinary source failed") || !strings.Contains(text, "synthetic source failed") {
		t.Fatalf("snapshot provenance crossed sides: %s", text)
	}
}

func TestReport_OverrideCollisions(t *testing.T) {
	t.Parallel()
	raw := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "source"}
	a := manifest.NamedResource{Group: "a.example", Kind: "Widget", Namespace: "apps", Name: "a"}
	b := manifest.NamedResource{Group: "b.example", Kind: "Widget", Namespace: "apps", Name: "b"}
	c := manifest.NamedResource{Group: "c.example", Kind: "Widget", Namespace: "apps", Name: "c"}
	capture := func(resource manifest.NamedResource) map[manifest.NamedResource]manifest.DiagnosticIdentity {
		return map[manifest.NamedResource]manifest.DiagnosticIdentity{raw: {ID: raw, Resource: resource}}
	}
	model := func(context string, subjects map[manifest.NamedResource]manifest.DiagnosticIdentity) Model {
		return Model{Context: context, Subjects: subjects, Primary: []Primary{{ID: raw, Msg: "failed"}}}
	}
	labels := func(prefix string, entries ...Subject) *Names {
		n := &Names{Prefix: prefix, Subjects: make(map[manifest.NamedResource]Subject, len(entries))}
		for _, subject := range entries {
			n.Subjects[subject.ID] = subject
		}
		return n
	}
	alphaRaw := labels("alpha: ", Subject{ID: raw, Kind: "OCIRepository", Name: "alpha: apps/source"})
	alphaA := labels("alpha: ", Subject{ID: raw, Kind: "Widget.a.example", Name: "alpha: apps/a"})
	alphaB := labels("alpha: ", Subject{ID: raw, Kind: "Widget", Name: "alpha: apps/b"})
	betaB := labels("beta: ", Subject{ID: b, Kind: "Widget.b.example", Name: "beta: apps/b"})
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	synthetic := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	chartRaw := synthetic.Named()
	otherChart := manifest.NamedResource{Group: "custom.example", Kind: manifest.KindHelmChart, Namespace: "apps", Name: "other"}
	for _, tc := range []struct {
		name         string
		report       Report
		extra        [][]manifest.NamedResource
		wantNames    []Names
		wantModels   [][]*Names
		wantWarnings []Subject
		wantText     string
	}{
		{
			name: "model introduces collision and restores raw contexts",
			report: Report{Groups: []ReportGroup{
				{Cluster: "alpha", Models: []Model{model("baseline", nil), model("empty", capture(raw)), model("captured", capture(a)), model("restored", map[manifest.NamedResource]manifest.DiagnosticIdentity{})}},
				{Cluster: "beta"},
			}},
			extra:      [][]manifest.NamedResource{nil, {b}},
			wantNames:  []Names{*alphaRaw, *labels("beta: ", Subject{ID: b, Kind: "Widget", Name: "beta: apps/b"})},
			wantModels: [][]*Names{{nil, alphaRaw, alphaA, alphaRaw}, nil},
			wantText: "\n  ✗  baseline: OCIRepository  alpha: apps/source  failed\n\n" +
				"  ✗  empty: OCIRepository  alpha: apps/source  failed\n\n" +
				"  ✗  captured: Widget.a.example  alpha: apps/a  failed\n\n" +
				"  ✗  restored: OCIRepository  alpha: apps/source  failed\n\n" +
				"  alpha: ✗ 4 failed\n\n  beta: ✓ 0 failed\n",
		},
		{
			name: "model removes final contribution with duplicate raw IDs",
			report: Report{Groups: []ReportGroup{
				{Cluster: "alpha", Subjects: capture(a), Models: []Model{model("baseline", nil), model("empty", map[manifest.NamedResource]manifest.DiagnosticIdentity{}), model("captured", capture(b)), model("restored", capture(a))}},
				{Cluster: "beta"},
			}},
			extra:      [][]manifest.NamedResource{{raw, raw}, {b}},
			wantNames:  []Names{*alphaA, *betaB},
			wantModels: [][]*Names{{nil, alphaRaw, alphaB, alphaA}, nil},
			wantText: "\n  ✗  baseline: Widget.a.example  alpha: apps/a  failed\n\n" +
				"  ✗  empty: OCIRepository  alpha: apps/source  failed\n\n" +
				"  ✗  captured: Widget  alpha: apps/b  failed\n\n" +
				"  ✗  restored: Widget.a.example  alpha: apps/a  failed\n\n" +
				"  alpha: ✗ 4 failed\n\n  beta: ✓ 0 failed\n",
		},
		{
			name: "model removes singleton contribution",
			report: Report{Groups: []ReportGroup{
				{Cluster: "alpha", Subjects: capture(a), Models: []Model{model("captured", capture(b))}},
				{Cluster: "beta"},
			}},
			extra:      [][]manifest.NamedResource{nil, {b}},
			wantNames:  []Names{*alphaA, *betaB},
			wantModels: [][]*Names{{alphaB}, nil},
			wantText: "\n  ✗  captured: Widget  alpha: apps/b  failed\n\n" +
				"  alpha: ✗ 1 failed\n\n  beta: ✓ 0 failed\n",
		},
		{
			name: "another group retains the removed contributor",
			report: Report{Groups: []ReportGroup{
				{Cluster: "alpha", Subjects: capture(a), Models: []Model{model("captured", capture(b))}},
				{Cluster: "beta"}, {Cluster: "gamma"},
			}},
			extra:      [][]manifest.NamedResource{nil, {b}, {a}},
			wantNames:  []Names{*alphaA, *betaB, *labels("gamma: ", Subject{ID: a, Kind: "Widget.a.example", Name: "gamma: apps/a"})},
			wantModels: [][]*Names{{labels("alpha: ", Subject{ID: raw, Kind: "Widget.b.example", Name: "alpha: apps/b"})}, nil, nil},
			wantText: "\n  ✗  captured: Widget.b.example  alpha: apps/b  failed\n\n" +
				"  alpha: ✗ 1 failed\n\n  beta: ✓ 0 failed\n\n  gamma: ✓ 0 failed\n",
		},
		{
			name: "interleaved singleton warnings reset other captures and restore each group",
			report: Report{
				Groups: []ReportGroup{{Cluster: "alpha", Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{b: {ID: b, Resource: a}}}, {Cluster: "beta"}},
				Warnings: []Warning{
					{Warning: manifest.Warning{Resource: raw, Subject: manifest.DiagnosticIdentity{ID: raw, Resource: b}, Category: "Captured", Message: "first", Detail: []string{"kept"}, Related: []manifest.NamedResource{b}, Count: 2}},
					{Group: 1, Warning: manifest.Warning{Resource: b, Subject: manifest.DiagnosticIdentity{ID: b, Resource: a}, Message: "second"}},
					{Warning: manifest.Warning{Resource: raw, Subject: manifest.DiagnosticIdentity{ID: raw, Resource: a, Path: "warning/a"}, Message: "third"}},
					{Group: 1, Warning: manifest.Warning{Resource: b, Subject: manifest.DiagnosticIdentity{ID: b, Resource: c}, Message: "fourth"}},
					{Warning: manifest.Warning{Resource: raw, Message: "ordinary"}},
					{Warning: manifest.Warning{Resource: b, Subject: manifest.DiagnosticIdentity{ID: b, Resource: a}, Message: "matching"}},
					{Group: 1, Warning: manifest.Warning{Message: "cluster"}},
					{Group: -1, Warning: manifest.Warning{Message: "invocation"}},
				},
			},
			extra:      [][]manifest.NamedResource{{raw, b, b}, {b}},
			wantNames:  []Names{*labels("alpha: ", Subject{ID: raw, Kind: "OCIRepository", Name: "alpha: apps/source"}, Subject{ID: b, Kind: "Widget.a.example", Name: "alpha: apps/a"}), *betaB},
			wantModels: [][]*Names{nil, nil},
			wantWarnings: []Subject{
				{ID: raw, Kind: "Widget", Name: "alpha: apps/b"},
				{ID: b, Kind: "Widget", Name: "beta: apps/a"},
				{ID: raw, Kind: "Widget.a.example", Name: `alpha: apps/a (entry path "warning/a")`},
				{ID: b, Kind: "Widget.c.example", Name: "beta: apps/c"},
				{ID: raw, Kind: "OCIRepository", Name: "alpha: apps/source"},
				{ID: b, Kind: "Widget.a.example", Name: "alpha: apps/a"}, {}, {},
			},
			wantText: "\n  ⚠ warnings (8)\n    [Captured] Widget alpha: apps/b: first (×2)\n      kept\n" +
				"    Widget beta: apps/a: second\n" +
				"    Widget.a.example alpha: apps/a (entry path \"warning/a\"): third\n" +
				"    Widget.c.example beta: apps/c: fourth\n" +
				"    OCIRepository alpha: apps/source: ordinary\n" +
				"    Widget.a.example alpha: apps/a: matching\n    beta: cluster\n    invocation\n\n" +
				"  alpha: ✓ 0 failed\n\n  beta: ✓ 0 failed\n",
		},
		{
			name: "captured HelmChart warning introduces collision without changing beta",
			report: Report{
				Groups:   []ReportGroup{{Cluster: "alpha"}, {Cluster: "beta"}},
				Warnings: []Warning{{Warning: manifest.Warning{Resource: chartRaw, Subject: manifest.DiagnosticFor(chartRaw, synthetic), Message: "captured"}}},
			},
			extra: [][]manifest.NamedResource{nil, {otherChart}},
			wantNames: []Names{
				*labels("alpha: ", Subject{ID: chartRaw, Kind: "OCIRepository", Name: "alpha: apps/chart"}),
				*labels("beta: ", Subject{ID: otherChart, Kind: "HelmChart", Name: "beta: apps/other"}),
			},
			wantModels:   [][]*Names{nil, nil},
			wantWarnings: []Subject{{ID: chartRaw, Kind: "HelmChart.source.toolkit.fluxcd.io", Name: "alpha: apps/chart"}},
			wantText: "\n  ⚠ warnings (1)\n    HelmChart.source.toolkit.fluxcd.io alpha: apps/chart: captured\n\n" +
				"  alpha: ✓ 0 failed\n\n  beta: ✓ 0 failed\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clone := func() Report {
				r := tc.report
				r.Groups = slices.Clone(r.Groups)
				for i := range r.Groups {
					r.Groups[i].Subjects = maps.Clone(r.Groups[i].Subjects)
					r.Groups[i].Models = slices.Clone(r.Groups[i].Models)
					for j := range r.Groups[i].Models {
						r.Groups[i].Models[j].Subjects = maps.Clone(r.Groups[i].Models[j].Subjects)
					}
				}
				r.Warnings = slices.Clone(r.Warnings)
				for i := range r.Warnings {
					r.Warnings[i].Warning.Detail = slices.Clone(r.Warnings[i].Warning.Detail)
					r.Warnings[i].Warning.Related = slices.Clone(r.Warnings[i].Warning.Related)
				}
				return r
			}
			want := clone()
			for i := range want.Groups {
				want.Groups[i].Names = tc.wantNames[i]
				for j := range want.Groups[i].Models {
					want.Groups[i].Models[j].Names = tc.wantModels[i][j]
				}
			}
			for i := range want.Warnings {
				want.Warnings[i].Subject = tc.wantWarnings[i]
				if want.Warnings[i].Group >= 0 && want.Warnings[i].Warning.Resource == (manifest.NamedResource{}) {
					want.Warnings[i].prefix = tc.wantNames[want.Warnings[i].Group].Prefix
				}
			}
			first, second := clone(), clone()
			for _, report := range []*Report{&first, &second} {
				assert.Diff(t, report.Prepare(tc.extra), tc.wantNames)
				if diff := cmp.Diff(want, *report, cmp.AllowUnexported(Warning{})); diff != "" {
					t.Errorf("prepared report (-want +got):\n%s", diff)
				}
				var out bytes.Buffer
				encoder := Encoder{Writer: &out}
				assert.NoError(t, encoder.Report(*report, encoder))
				assert.Equal(t, out.String(), tc.wantText)
			}
			if diff := cmp.Diff(first, second, cmp.AllowUnexported(Warning{})); diff != "" {
				t.Errorf("preparation is not deterministic (-first +second):\n%s", diff)
			}
		})
	}
}
