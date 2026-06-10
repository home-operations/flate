package cli

import (
	"regexp"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

// visibleLen counts the runes a frame actually occupies on screen — ANSI color
// codes are zero-width and must not count against the terminal budget.
func visibleLen(s string) int { return len([]rune(ansiRE.ReplaceAllString(s, ""))) }

func barID(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

// inflightModel builds a model with names tracked as in-flight (Pending).
func inflightModel(color bool, names ...string) barModel {
	m := newBarModel(color)
	for _, n := range names {
		m.track(barID(n), store.StatusInfo{Status: store.StatusPending})
	}
	return m
}

// TestView_FitsWidth: the frame never exceeds the terminal width at any size, so
// Bubble Tea's inline renderer never wraps (a wrap would break the sticky line).
func TestView_FitsWidth(t *testing.T) {
	for _, width := range []int{1, 3, 8, 20, 40, 80, 200} {
		for _, color := range []bool{false, true} {
			m := inflightModel(color, "alpha", "bravo", "charlie", "delta", "echo")
			m.done, m.total, m.width = 42, 86, width
			got := m.View().Content
			if vl := visibleLen(got); vl > width {
				t.Errorf("width=%d color=%v: visible len %d > width (%q)", width, color, vl, got)
			}
		}
	}
}

// TestView_PlainNoColor: with color off the frame carries no escape codes and
// shows the counts + in-flight names.
func TestView_PlainNoColor(t *testing.T) {
	m := inflightModel(false, "plex")
	m.done, m.total, m.width = 3, 10, 80
	got := m.View().Content
	if strings.Contains(got, "\x1b") {
		t.Errorf("color=false leaked an escape code: %q", got)
	}
	for _, want := range []string{"[3/10]", "plex"} {
		if !strings.Contains(got, want) {
			t.Errorf("frame %q missing %q", got, want)
		}
	}
}

// TestView_ColorInvisibleToWidth: the same content fits the same width whether
// colorized or not — codes don't consume columns.
func TestView_ColorInvisibleToWidth(t *testing.T) {
	plain := inflightModel(false, "plex")
	plain.done, plain.total, plain.width = 3, 10, 40
	color := inflightModel(true, "plex")
	color.done, color.total, color.width = 3, 10, 40

	pc, cc := plain.View().Content, color.View().Content
	if visibleLen(pc) != visibleLen(cc) {
		t.Errorf("visible lengths differ: plain=%d color=%d", visibleLen(pc), visibleLen(cc))
	}
	if !strings.Contains(cc, "\x1b") {
		t.Errorf("color frame carries no ANSI styling: %q", cc)
	}
}

// TestModel_FinishVanishes: finishMsg sets the bar to render an empty frame
// (clearing the line so it vanishes) and returns the quit command.
func TestModel_FinishVanishes(t *testing.T) {
	m := inflightModel(false, "a")
	m.done, m.total = 1, 1
	updated, cmd := m.Update(finishMsg{})
	if c := updated.View().Content; c != "" {
		t.Errorf("finished view should be empty, got %q", c)
	}
	if cmd == nil {
		t.Fatal("finishMsg should return a quit command")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Errorf("finishMsg should quit; got cmd msg %T", cmd())
	}
}

// TestModel_InitTicks: Init kicks off the spinner animation.
func TestModel_InitTicks(t *testing.T) {
	if newBarModel(false).Init() == nil {
		t.Error("Init should return the spinner tick command")
	}
}

// TestModel_WindowSize: a resize updates the width used for truncation.
func TestModel_WindowSize(t *testing.T) {
	m := newBarModel(false)
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 120})
	if w := updated.(barModel).width; w != 120 {
		t.Errorf("width = %d, want 120", w)
	}
}

func TestSummarizeInflight(t *testing.T) {
	cases := []struct {
		names []string
		max   int
		want  string
	}{
		{nil, 2, ""},
		{[]string{"a"}, 2, "a"},
		{[]string{"a", "b"}, 2, "a, b"},
		{[]string{"a", "b", "c"}, 2, "a, b +1"},
		{[]string{"a", "b", "c", "d", "e"}, 2, "a, b +3"},
	}
	for _, c := range cases {
		if got := summarizeInflight(c.names, c.max); got != c.want {
			t.Errorf("summarizeInflight(%v, %d) = %q, want %q", c.names, c.max, got, c.want)
		}
	}
}

// TestModel_Counts drives track through realistic events: counters track unique
// terminal ids; Ready, Failed, and skipped all count as "done loading".
func TestModel_Counts(t *testing.T) {
	m := newBarModel(false)
	a, b, s := barID("a"), barID("b"), barID("suspended")
	m.track(a, store.StatusInfo{Status: store.StatusPending})
	m.track(b, store.StatusInfo{Status: store.StatusPending})
	m.track(a, store.StatusInfo{Status: store.StatusReady})
	m.track(b, store.StatusInfo{Status: store.StatusFailed, Message: "boom: chart not found"})
	m.track(s, store.StatusInfo{Status: store.StatusReady, Message: store.MsgSuspended})

	if m.done != 3 || m.total != 3 {
		t.Errorf("counts: done=%d total=%d; want 3/3", m.done, m.total)
	}
	if got := m.inflightLabels(); len(got) != 0 {
		t.Errorf("all resources terminal but still in-flight: %v", got)
	}
}

// TestModel_DedupAndInflight: an idempotent terminal re-write doesn't
// double-count, and a resource drops out of the in-flight set the moment it
// reaches a terminal status.
func TestModel_DedupAndInflight(t *testing.T) {
	m := newBarModel(false)
	a := barID("a")
	m.track(a, store.StatusInfo{Status: store.StatusPending})
	if got := m.inflightLabels(); len(got) != 1 || got[0] != "a" {
		t.Fatalf("in-flight after Pending = %v, want [a]", got)
	}
	m.track(a, store.StatusInfo{Status: store.StatusReady})
	m.track(a, store.StatusInfo{Status: store.StatusReady}) // duplicate
	if m.done != 1 {
		t.Errorf("duplicate Ready double-counted: done=%d, want 1", m.done)
	}
	if got := m.inflightLabels(); len(got) != 0 {
		t.Errorf("terminal resource still in-flight: %v", got)
	}
}

// TestModel_ExcludesSyntheticIDs: the synthetic bootstrap GitRepository and
// internally-synthesized HelmCharts appear in no `flate test`/`build` report, so
// the bar must neither count them nor list them in-flight — otherwise its
// [done/total] overshoots every report (the 175-vs-174 gap).
func TestModel_ExcludesSyntheticIDs(t *testing.T) {
	m := newBarModel(false)
	boot := manifest.BootstrapSourceID
	chart := manifest.NamedResource{Kind: manifest.KindHelmChart, Namespace: "ns", Name: "repo-app-abc1234"}
	declared := barID("declared")

	for _, id := range []manifest.NamedResource{boot, chart, declared} {
		m.track(id, store.StatusInfo{Status: store.StatusPending})
	}
	for _, id := range []manifest.NamedResource{boot, chart, declared} {
		m.track(id, store.StatusInfo{Status: store.StatusReady})
	}
	if m.done != 1 || m.total != 1 {
		t.Errorf("synthetic ids leaked into counts: done=%d total=%d; want 1/1", m.done, m.total)
	}

	// Excluded ids never enter the in-flight set, even while Pending.
	m2 := newBarModel(false)
	m2.track(boot, store.StatusInfo{Status: store.StatusPending})
	m2.track(chart, store.StatusInfo{Status: store.StatusPending})
	if got := m2.inflightLabels(); len(got) != 0 {
		t.Errorf("synthetic ids listed in-flight: %v", got)
	}
}
