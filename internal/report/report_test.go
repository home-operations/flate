package report

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

func ks(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: name}
}

func failed(ids ...manifest.NamedResource) map[manifest.NamedResource]store.StatusInfo {
	m := map[manifest.NamedResource]store.StatusInfo{}
	for _, id := range ids {
		m[id] = store.StatusInfo{Status: store.StatusFailed, Message: id.Name + " boom"}
	}
	return m
}

// TestBuild_GroupsCascadeUnderRoots pins the core collapse: a real (primary)
// failure and a missing dependency each surface once, with the resources that
// cascaded from them folded into Blocks / RequiredBy — never on their own.
func TestBuild_GroupsCascadeUnderRoots(t *testing.T) {
	root, mid, leaf := ks("root"), ks("mid"), ks("leaf") // leaf → mid → root (primary)
	missing := ks("missing")
	dependent := ks("dependent") // dependent → missing (not found)

	f := failed(root, mid, leaf, dependent) // missing is absent from the store
	blocked := map[manifest.NamedResource][]manifest.NamedResource{
		mid:       {root},
		leaf:      {mid},
		dependent: {missing},
	}

	m := Build(&orchestrator.Result{
		Failed: f, Blocked: blocked,
		Causes: map[manifest.NamedResource][]manifest.NamedResource{
			root: {leaf, mid}, missing: {dependent},
		},
	})

	if len(m.Primary) != 1 || m.Primary[0].ID != root {
		t.Fatalf("Primary = %+v, want only root", m.Primary)
	}
	// root's cascade is mid and leaf (transitively), folded under it and sorted.
	if got := m.Primary[0].Blocks; len(got) != 2 || got[0] != leaf || got[1] != mid {
		t.Errorf("root.Blocks = %+v, want sorted [leaf mid]", got)
	}
	if len(m.Missing) != 1 || m.Missing[0].ID != missing {
		t.Fatalf("Missing = %+v, want only 'missing'", m.Missing)
	}
	if got := m.Missing[0].RequiredBy; len(got) != 1 || got[0] != dependent {
		t.Errorf("missing.RequiredBy = %+v, want [dependent]", got)
	}

	var b bytes.Buffer
	if err := m.Write(&b, false); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"root", "blocks", "missing", "not found", "required by", "2 failed", "3 blocked"} {
		if !strings.Contains(out, want) {
			t.Errorf("rendered report missing %q:\n%s", want, out)
		}
	}
	// Cascaded resources never get their own failure line.
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "✗") && (strings.Contains(line, "leaf") || strings.Contains(line, "dependent")) {
			t.Errorf("cascaded resource must not get its own failure line: %q", line)
		}
	}
}

// TestBuild_PrimaryOnly covers a lone real failure with nothing depending on it.
func TestBuild_PrimaryOnly(t *testing.T) {
	a := ks("a")
	m := Build(&orchestrator.Result{Failed: failed(a), Causes: map[manifest.NamedResource][]manifest.NamedResource{a: nil}})
	if len(m.Primary) != 1 || len(m.Missing) != 0 || len(m.Primary[0].Blocks) != 0 {
		t.Fatalf("want one primary with no blocks, got %+v", m)
	}
	if m.Empty() {
		t.Error("a model with a failure is not empty")
	}
}

// TestBuild_Empty covers a clean run with nothing to show.
func TestBuild_Empty(t *testing.T) {
	if !Build(&orchestrator.Result{}).Empty() {
		t.Error("a clean run must produce an empty model")
	}
}

// TestWrite_WarningsSection: a clean run carrying only render advisories renders
// a "warnings" section (attributed + detail) and NO red "✗ 0 failed" verdict.
func TestWrite_WarningsSection(t *testing.T) {
	hr := manifest.NamedResource{Kind: "HelmRelease", Namespace: "default", Name: "app"}
	warns := []manifest.Warning{
		{Resource: hr, Category: manifest.WarnStaleValues, Message: "values not defined by the chart schema", Detail: []string{"image.tagg", "oldKey"}},
		{Category: manifest.WarnEmptyScan, Message: "no Flux objects found"}, // global
	}
	m := Build(&orchestrator.Result{Unrelated: warns})
	if m.Empty() {
		t.Fatal("a model with warnings is not empty")
	}
	var b bytes.Buffer
	if err := m.Write(&b, false); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"warnings (2)", "HelmRelease default/app: values not defined", "image.tagg, oldKey", "no Flux objects found"} {
		if !strings.Contains(out, want) {
			t.Errorf("warnings section missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "failed") {
		t.Errorf("a warnings-only run must not render a failure verdict:\n%s", out)
	}
}

// TestWrite_WarningCount: a deduped warning renders its repeat count.
func TestWrite_WarningCount(t *testing.T) {
	warns := []manifest.Warning{{Category: manifest.WarnPathConfig, Message: "same root", Count: 4}}
	var b bytes.Buffer
	_ = Build(&orchestrator.Result{Unrelated: warns}).Write(&b, false)
	if out := b.String(); !strings.Contains(out, "×4") {
		t.Errorf("warning repeat count not rendered:\n%s", out)
	}
}

// TestWrite_PrimaryMessageIsMultiLine pins that a multi-line primary message (a
// kustomize duplicate-id diagnostic) renders every line — the producer
// attribution must survive intact rather than being truncated to the first
// line, which is exactly the detail that tells the user what to fix.
func TestWrite_PrimaryMessageIsMultiLine(t *testing.T) {
	a := ks("a")
	f := map[manifest.NamedResource]store.StatusInfo{
		a: {Status: store.StatusFailed, Message: "may not add resource with an already registered id: X\n" +
			"duplicate resource(s) produced by multiple accumulated paths:\n  - ./one\n  - ./two"},
	}
	var b bytes.Buffer
	if err := Build(&orchestrator.Result{Failed: f, Causes: map[manifest.NamedResource][]manifest.NamedResource{a: nil}}).Write(&b, false); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"already registered id: X", "produced by multiple accumulated paths", "- ./one", "- ./two"} {
		if !strings.Contains(out, want) {
			t.Errorf("multi-line primary message missing %q:\n%s", want, out)
		}
	}
}

// TestBuild_BlockedCountsDistinctResources pins that a resource blocked by two
// independent roots (a failed parent AND a missing dependency) counts once in
// the verdict, even though it folds under both roots' lists.
func TestBuild_BlockedCountsDistinctResources(t *testing.T) {
	root, missing, dual := ks("root"), ks("missing"), ks("dual")
	f := failed(root, dual) // missing is absent from the store
	blocked := map[manifest.NamedResource][]manifest.NamedResource{
		dual: {root, missing}, // blocked by a failed parent AND a missing dep
	}

	m := Build(&orchestrator.Result{
		Failed: f, Blocked: blocked,
		Causes: map[manifest.NamedResource][]manifest.NamedResource{
			root: {dual}, missing: {dual},
		},
	})
	if m.Blocked != 1 {
		t.Errorf("Blocked = %d, want 1 distinct blocked resource", m.Blocked)
	}
	if len(m.Primary) != 1 || len(m.Primary[0].Blocks) != 1 {
		t.Errorf("root should fold dual once: %+v", m.Primary)
	}
	if len(m.Missing) != 1 || len(m.Missing[0].RequiredBy) != 1 {
		t.Errorf("missing should fold dual once: %+v", m.Missing)
	}

	var b bytes.Buffer
	_ = m.Write(&b, false)
	if out := b.String(); !strings.Contains(out, "1 blocked") {
		t.Errorf("verdict should count the dual-rooted resource once:\n%s", out)
	}
}

func TestSummarize_KindOnCollision(t *testing.T) {
	hr := func(namespace, name string) manifest.NamedResource {
		return manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: namespace, Name: name}
	}
	tests := []struct {
		name string
		ids  []manifest.NamedResource // sorted, as Build delivers them
		want string
	}{
		{"same name, different kind", []manifest.NamedResource{hr("flux-system", "acars"), ks("acars")}, "HelmRelease flux-system/acars, Kustomization flux-system/acars"},
		{"same name, different namespace", []manifest.NamedResource{hr("default", "acars"), ks("acars")}, "default/acars, flux-system/acars"},
		{"collision hidden by the cap", []manifest.NamedResource{hr("flux-system", "zzz"), ks("a"), ks("b"), ks("c"), ks("d"), ks("e"), ks("zzz")}, "HelmRelease flux-system/zzz, flux-system/a, flux-system/b, flux-system/c, flux-system/d, flux-system/e +1 more"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := summarize(tc.ids); got != tc.want {
				t.Errorf("summarize = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDocument pins the one spacing rule: a leading blank line, one blank line
// between non-empty blocks, a trailing newline, and "" when all blocks are empty.
func TestDocument(t *testing.T) {
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
			if got := Document(tc.blocks...); got != tc.want {
				t.Errorf("Document(%q) = %q, want %q", tc.blocks, got, tc.want)
			}
		})
	}
}

// TestVerdict pins the shared verdict grammar: a lead glyph, the lead count
// always shown (even at zero), later counts only when non-zero, joined by " · ",
// and a trailing elapsed clock only when > 0.
func TestVerdict(t *testing.T) {
	pass := func(s string, _ bool) string { return s }
	c := func(n int, label string) Count { return Count{N: n, Label: label, Paint: pass} }

	if got := Verdict(false, "✓", pass, 0); got != "" {
		t.Errorf("no counts = %q, want empty", got)
	}
	if got := Verdict(false, "✓", pass, 0, c(0, "passed")); got != "  ✓ 0 passed" {
		t.Errorf("lead-at-zero = %q", got)
	}
	if got := Verdict(false, "✓", pass, 0,
		c(2, "passed"), c(0, "skipped"), c(3, "failed")); got != "  ✓ 2 passed · 3 failed" {
		t.Errorf("zero non-lead dropped = %q", got)
	}
	if got := Verdict(false, "✗", pass, 1500*time.Millisecond, c(1, "failed")); got != "  ✗ 1 failed   1.5s" {
		t.Errorf("elapsed = %q", got)
	}
}

func TestBuild_PreparedCycleRoot(t *testing.T) {
	root, peer, leaf := ks("root"), ks("peer"), ks("leaf")
	result := &orchestrator.Result{
		Failed: map[manifest.NamedResource]store.StatusInfo{
			root: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + peer.String()},
			peer: {Status: store.StatusFailed, Message: "dependency cycle: blocked by " + root.String()},
			leaf: {Status: store.StatusFailed, Message: "dependencies failed"},
		},
		Blocked: map[manifest.NamedResource][]manifest.NamedResource{
			root: {peer}, peer: {root}, leaf: {root},
		},
	}
	o, err := orchestrator.New(orchestrator.Config{Path: t.TempDir(), CacheDir: t.TempDir(), Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Stop)
	selected, selectedErr, _ := orchestrator.Select([]orchestrator.Rendered{{Orchestrator: o, Result: result}}, &orchestrator.FailuresError{Message: "raw failures"},
		[]manifest.NamedResource{{Kind: manifest.KindKustomization}}, nil)
	m := Build(selected[0])
	if selectedErr == nil || !strings.Contains(selectedErr.Error(), fmt.Sprintf("(+%d blocked", m.Blocked)) {
		t.Fatalf("selected error and report counts disagree: report=%+v error=%v", m, selectedErr)
	}
	if len(m.Primary) != 2 || m.Blocked != 1 {
		t.Fatalf("want 2 primary cycles and 1 distinct blocked child, got %+v", m)
	}
	var b bytes.Buffer
	if err := m.Write(&b, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"dependency cycle: blocked by " + peer.String(), "blocks flux-system/leaf", "2 failed · 1 blocked"} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("prepared report missing %q:\n%s", want, b.String())
		}
	}
	if len(result.Blocked) != 3 || result.Failed[leaf].Message != "dependencies failed" {
		t.Fatal("Build mutated the selected failure snapshot")
	}
}

func TestWriteErrors_NonResourceDiagnostics(t *testing.T) {
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
			var b bytes.Buffer
			if err := WriteErrors(&b, tc.err); err != nil {
				t.Fatal(err)
			}
			if got := b.String(); got != tc.want {
				t.Fatalf("WriteErrors = %q, want %q", got, tc.want)
			}
		})
	}
	writeErr := errors.New("write failed")
	if err := WriteErrors(errorWriter{writeErr}, closeErr); !errors.Is(err, writeErr) {
		t.Fatalf("writer error = %v, want %v", err, writeErr)
	}
}

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
