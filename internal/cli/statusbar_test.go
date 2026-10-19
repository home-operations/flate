package cli

import (
	"regexp"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var ansiRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

// visibleLen counts the runes a frame actually occupies on screen — ANSI color
// codes are zero-width and must not count against the terminal budget.
func visibleLen(s string) int { return len([]rune(ansiRE.ReplaceAllString(s, ""))) }

func inflightModel(color bool, names ...string) barModel {
	m := newBarModel(color)
	for _, n := range names {
		m.events.publish(testutil.KustomizationID("ns", n), store.StatusPending, time.Now())
	}
	m.drainEvents()
	return m
}

// TestView_FitsWidth: the frame never exceeds the terminal width at any size, so
// Bubble Tea's inline renderer never wraps (a wrap would break the sticky line).
func TestView_FitsWidth(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	if newBarModel(false).Init() == nil {
		t.Error("Init should return the spinner tick command")
	}
}

// TestModel_WindowSize: a resize updates the width used for truncation.
func TestModel_WindowSize(t *testing.T) {
	t.Parallel()
	m := newBarModel(false)
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 120})
	if w := updated.(barModel).width; w != 120 {
		t.Errorf("width = %d, want 120", w)
	}
}

func TestSummarizeInflight(t *testing.T) {
	t.Parallel()
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

func TestModel_Counts(t *testing.T) {
	t.Parallel()
	m := newBarModel(false)
	a, b, s := testutil.KustomizationID("ns", "a"), testutil.KustomizationID("ns", "b"), testutil.KustomizationID("ns", "suspended")
	m.events.publish(a, store.StatusPending, time.Now())
	m.events.publish(b, store.StatusPending, time.Now())
	m.events.publish(a, store.StatusReady, time.Now())
	m.events.publish(b, store.StatusFailed, time.Now())
	m.events.publish(s, store.StatusReady, time.Now())

	m.drainEvents()
	if m.done != 3 || m.total != 3 {
		t.Errorf("counts: done=%d total=%d; want 3/3", m.done, m.total)
	}
	m.drainEvents()
	if got := m.inflightLabels(); len(got) != 0 {
		t.Errorf("all resources terminal but still in-flight: %v", got)
	}
}

func TestModel_DedupAndInflight(t *testing.T) {
	t.Parallel()
	m := newBarModel(false)
	a := testutil.KustomizationID("ns", "a")
	first := time.Unix(10, 0)
	m.events.publish(a, store.StatusPending, first)
	updated, _ := m.Update(m.spinner.Tick())
	m = updated.(barModel)
	assert.Diff(t, m.inflightLabels(), []string{"a"})
	m.events.publish(a, store.StatusPending, first.Add(-time.Second))
	m.events.publish(a, store.StatusReady, first.Add(time.Second))
	m.events.publish(a, store.StatusReady, first.Add(2*time.Second))
	updated, _ = m.Update(m.spinner.Tick())
	m = updated.(barModel)
	assert.Equal(t, m.first[a], first.Add(-time.Second))
	assert.Equal(t, m.total, 1)
	assert.Equal(t, m.done, 1)
	assert.Equal(t, len(m.inflight), 0)
	if m.events.head.Load() != nil {
		t.Fatal("spinner left pending statuses")
	}
}

func TestModel_InflightLabelsFirstSeenOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		names []string
	}{
		{"later first", []string{"alpha", "charlie", "bravo"}},
		{"later last", []string{"bravo", "charlie", "alpha"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newBarModel(false)
			first := time.Unix(1, 0)
			for _, name := range tc.names {
				observedAt := first
				if name == "alpha" {
					observedAt = first.Add(time.Second)
				}
				m.events.publish(testutil.KustomizationID("ns", name), store.StatusPending, observedAt)
			}
			m.drainEvents()
			assert.Diff(t, m.inflightLabels(), []string{"bravo", "charlie", "alpha"})
		})
	}
}

func TestModel_ExcludesSyntheticIDs(t *testing.T) {
	t.Parallel()
	m := newBarModel(false)
	boot := manifest.BootstrapSourceID
	chart := manifest.NamedResource{Group: "source.toolkit.fluxcd.io", Kind: manifest.KindHelmChart, Namespace: "ns", Name: "repo-app-abc1234"}
	declared := []manifest.NamedResource{
		testutil.KustomizationID("ns", "declared"),
		{Group: "foreign.example", Kind: chart.Kind, Namespace: chart.Namespace, Name: chart.Name},
		{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "ns", Name: "ordinary"},
		{Group: boot.Group, Kind: boot.Kind, Namespace: "other", Name: boot.Name},
	}
	ids := append([]manifest.NamedResource{boot, chart}, declared...)

	for _, id := range ids {
		m.events.publish(id, store.StatusPending, time.Now())
	}
	m.drainEvents()
	if got := m.inflightLabels(); len(got) != len(declared) {
		t.Fatalf("in-flight labels = %v, want %d declared resources", got, len(declared))
	}
	for _, id := range ids {
		m.events.publish(id, store.StatusReady, time.Now())
	}
	m.drainEvents()
	if m.done != len(declared) || m.total != len(declared) {
		t.Errorf("live counts: done=%d total=%d; want %d/%d", m.done, m.total, len(declared), len(declared))
	}

	m2 := newBarModel(false)
	m2.events.publish(boot, store.StatusPending, time.Now())
	m2.events.publish(chart, store.StatusPending, time.Now())
	m2.drainEvents()
	if got := m2.inflightLabels(); len(got) != 0 {
		t.Errorf("synthetic ids listed in-flight: %v", got)
	}
}

func TestModel_DelayedElapsed(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		m := newBarModel(false)
		time.Sleep(progressDelay)
		m.drainEvents()
		time.Sleep(250 * time.Millisecond)
		if got := m.View().Content; !strings.Contains(got, "0.5s") {
			t.Fatalf("elapsed excludes delay: %q", got)
		}
	})
}
