// Package report turns a reconcile's failures into a compact, styled end-of-run
// summary: the real (primary) errors shown once, every cascaded failure folded
// under the root that caused it, and any deferred log lines in a quiet footer.
// It exists so a single missing or failed dependency reads as one root cause
// with a list of what it blocked — not a wall of nested, repeated
// "dependencies failed: A: dependencies failed: B: …" chains.
//
// The model is built from orchestrator data (Result.Failed + Result.Blocked) and
// is pure/deterministic; rendering is gated on a color bool so it is plain and
// testable off a TTY. Styling reuses internal/style, the vocabulary the live
// status bar and `flate test` report already speak.
package report

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// blockedSample bounds how many blocked/requiring ids are named inline per root
// before collapsing the rest to "+N more", so a root that blocks a whole cluster
// stays one readable line.
const blockedSample = 6

// Primary is a resource that failed on its own — a real render/template/build
// error. Blocks lists the resources whose failure cascaded from it (sorted).
type Primary struct {
	ID     manifest.NamedResource
	Msg    string
	Blocks []manifest.NamedResource
}

// Missing is a dependency that never rendered (absent from the store) yet was
// required by one or more resources — the other kind of root cause. RequiredBy
// lists the resources blocked on it (sorted).
type Missing struct {
	ID         manifest.NamedResource
	RequiredBy []manifest.NamedResource
}

// Note is one deferred log line for the footer, collapsed by identical text with
// a repeat count.
type Note struct {
	Text  string
	Count int
}

// Model is the partitioned, deterministic view the renderer consumes.
type Model struct {
	Primary []Primary
	Missing []Missing
	Notes   []Note
	// Blocked is the count of distinct derived (cascaded) failures. It is the
	// verdict's "N blocked" figure — distinct resources, not the sum of the
	// per-root Blocks/RequiredBy lists, which can overlap when one resource
	// traces to more than one root (e.g. a failed parent AND a missing dep).
	Blocked int
}

// Empty reports whether there is nothing to render.
func (m Model) Empty() bool {
	return len(m.Primary) == 0 && len(m.Missing) == 0 && len(m.Notes) == 0
}

// Build partitions failures into primary errors and root-grouped cascades.
// failed is Result.Failed (sentinel-trimmed messages); blocked is Result.Blocked
// (a failed id → the immediate deps that blocked it, empty for primaries); notes
// are the deferred log lines for the footer.
//
// A blocked resource is folded into the root(s) it traces to: walking its
// blockers until reaching a primary failure (a failed id that is not itself
// blocked) or a missing id (absent from failed). Cascaded resources never get
// their own line — they appear only in a root's Blocks / RequiredBy list.
func Build(failed map[manifest.NamedResource]store.StatusInfo, blocked map[manifest.NamedResource][]manifest.NamedResource, notes []Note) Model {
	byRoot := Roots(blocked)

	var primary []Primary
	var missing []Missing
	for id, info := range failed {
		if _, derived := blocked[id]; derived {
			continue // cascaded — surfaced under its root, not on its own
		}
		primary = append(primary, Primary{ID: id, Msg: info.Message, Blocks: sortedIDs(byRoot[id])})
	}
	for root, reqs := range byRoot {
		if _, isFailed := failed[root]; isFailed {
			continue // root is a primary failure, already listed above
		}
		missing = append(missing, Missing{ID: root, RequiredBy: sortedIDs(reqs)})
	}

	slices.SortFunc(primary, func(a, b Primary) int { return a.ID.Compare(b.ID) })
	slices.SortFunc(missing, func(a, b Missing) int { return a.ID.Compare(b.ID) })
	return Model{Primary: primary, Missing: missing, Notes: notes, Blocked: len(blocked)}
}

// Roots inverts a blocked graph into root cause → the resources that trace to
// it. blocked maps each derived failure to the immediate dependencies that
// blocked it; a root is any id reached by walking those blockers that is not
// itself blocked (a primary failure, or a dependency missing from the graph).
// Shared by Build and the test runner so both fold a cascade under the same
// root with identical walk semantics.
func Roots(blocked map[manifest.NamedResource][]manifest.NamedResource) map[manifest.NamedResource][]manifest.NamedResource {
	byRoot := map[manifest.NamedResource][]manifest.NamedResource{}
	for id := range blocked {
		for _, r := range rootsOf(id, blocked) {
			byRoot[r] = append(byRoot[r], id)
		}
	}
	return byRoot
}

// rootsOf resolves the root cause(s) of a blocked id by walking its blockers to
// a primary failure or a missing id. Cycle-safe via a visited set.
func rootsOf(id manifest.NamedResource, blocked map[manifest.NamedResource][]manifest.NamedResource) []manifest.NamedResource {
	var out []manifest.NamedResource
	seen := map[manifest.NamedResource]bool{id: true}
	var walk func(manifest.NamedResource)
	walk = func(n manifest.NamedResource) {
		for _, dep := range blocked[n] {
			if seen[dep] {
				continue
			}
			seen[dep] = true
			if _, derived := blocked[dep]; derived {
				walk(dep) // dep is itself cascaded — keep climbing
			} else {
				out = append(out, dep) // primary failure or missing id — a root
			}
		}
	}
	walk(id)
	return out
}

func sortedIDs(ids []manifest.NamedResource) []manifest.NamedResource {
	out := slices.Clone(ids)
	slices.SortFunc(out, manifest.NamedResource.Compare)
	return slices.CompactFunc(out, func(a, b manifest.NamedResource) bool { return a.Compare(b) == 0 })
}

// Write renders the model to w. color gates ANSI styling; elapsed, when > 0,
// closes the verdict line.
func (m Model) Write(w io.Writer, color bool, elapsed time.Duration) error {
	var b strings.Builder
	b.WriteByte('\n')

	for _, p := range m.Primary {
		fmt.Fprintf(&b, "  %s  %s  %s",
			style.Fail(style.GlyphFail, color),
			style.Dim(p.ID.Kind, color),
			p.ID.NamespacedName())
		writeMessage(&b, p.Msg)
		if len(p.Blocks) > 0 {
			fmt.Fprintf(&b, "\n%s%s", msgIndent, style.Dim("blocks "+summarize(p.Blocks), color))
		}
		b.WriteByte('\n')
	}

	for _, mr := range m.Missing {
		fmt.Fprintf(&b, "  %s  %s  %s  %s\n      %s\n",
			style.Fail(style.GlyphFail, color),
			style.Dim(mr.ID.Kind, color),
			mr.ID.NamespacedName(),
			style.Fail("not found", color),
			style.Dim("required by "+summarize(mr.RequiredBy), color))
	}

	if len(m.Notes) > 0 {
		fmt.Fprintf(&b, "\n  %s\n", style.Dim(fmt.Sprintf("notes (%d)", noteTotal(m.Notes)), color))
		for _, n := range m.Notes {
			line := n.Text
			if n.Count > 1 {
				line = fmt.Sprintf("%s (×%d)", line, n.Count)
			}
			fmt.Fprintf(&b, "    %s\n", style.Dim(line, color))
		}
	}

	// Verdict: failed = primary + missing roots; blocked = distinct cascaded
	// resources (m.Blocked, not the sum of per-root lists, which can overlap).
	failedCount := len(m.Primary) + len(m.Missing)
	b.WriteString("\n  " + style.Fail(style.GlyphFail, color) + " " +
		style.Fail(fmt.Sprintf("%d failed", failedCount), color))
	if m.Blocked > 0 {
		b.WriteString(" · " + style.Dim(fmt.Sprintf("%d blocked", m.Blocked), color))
	}
	if elapsed > 0 {
		b.WriteString("   " + style.Dim(style.Elapsed(elapsed), color))
	}
	b.WriteByte('\n')

	_, err := io.WriteString(w, b.String())
	return err
}

// summarize renders an id list as "a, b, c +N more", capped at blockedSample.
func summarize(ids []manifest.NamedResource) string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		names = append(names, id.NamespacedName())
	}
	if len(names) <= blockedSample {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s +%d more", strings.Join(names[:blockedSample], ", "), len(names)-blockedSample)
}

// msgIndent aligns a primary failure's continuation lines (and its "blocks …"
// line) under the message column.
const msgIndent = "         "

// writeMessage appends a primary failure's message to its resource header line:
// the first line follows inline; any continuation lines print indented beneath.
// Multi-line build/template diagnostics — notably the duplicate-id producer
// attribution from pkg/kustomize — survive intact instead of being truncated to
// their first line.
func writeMessage(b *strings.Builder, msg string) {
	lines := MessageLines(msg)
	if len(lines) == 0 {
		return
	}
	b.WriteString("  " + lines[0])
	for _, line := range lines[1:] {
		b.WriteString("\n" + msgIndent + line)
	}
}

// MessageLines splits a stored failure message into trimmed, non-empty lines.
// Shared with the test runner so a multi-line build/template diagnostic renders
// the same way on both surfaces.
func MessageLines(s string) []string {
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func noteTotal(notes []Note) int {
	n := 0
	for _, note := range notes {
		n += cmp.Or(note.Count, 1)
	}
	return n
}
