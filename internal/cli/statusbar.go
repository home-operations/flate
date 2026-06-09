package cli

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/term"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// statusBar is the live, single-line progress indicator flate paints to
// stderr during a run, replacing a wall of per-resource lines with one sticky
// frame that repaints in place:
//
//	⠹ [42/86] plex, factorio +3  12.3s
//
// A background ticker advances a braille spinner ~10×/s; the store's status
// events drive the counters and the in-flight set. Only failures scroll into
// permanent history (red ✗ lines above the bar) — successes and skips stay in
// the counts, so the terminal reads as steady forward motion instead of spam.
// A final summary replaces the bar when the run ends.
//
// The bar paints exclusively through a stderrRouter, which also carries slog
// output, so log records and failure lines slot in cleanly above the bar
// without ever corrupting it. stdout is untouched.
type statusBar struct {
	r         *stderrRouter
	color     bool
	startedAt time.Time

	mu sync.Mutex
	// first records each id's first-seen time — drives in-flight ordering
	// and per-resource elapsed on failure lines.
	first    map[manifest.NamedResource]time.Time
	inflight map[manifest.NamedResource]struct{}
	// printed records the last terminal status counted per id, so an
	// idempotent re-write doesn't double-count and a genuine flip recounts.
	printed map[manifest.NamedResource]store.Status
	done    int // resources that reached a terminal status
	failed  int
	skipped int
	total   int // every id seen so far (a live lower bound, grows with renders)

	stop chan struct{}
	wg   sync.WaitGroup
}

func newStatusBar(r *stderrRouter) *statusBar {
	return &statusBar{
		r:         r,
		color:     colorEnabled(),
		startedAt: time.Now(),
		first:     map[manifest.NamedResource]time.Time{},
		inflight:  map[manifest.NamedResource]struct{}{},
		printed:   map[manifest.NamedResource]store.Status{},
	}
}

// attach subscribes the bar to s's status events. Call before the run starts;
// the returned unsubscribe must fire when it ends (before finish).
func (b *statusBar) attach(s *store.Store) store.Unsubscribe {
	return s.OnStatus(b.onStatus, false)
}

func (b *statusBar) onStatus(id manifest.NamedResource, info store.StatusInfo) {
	now := time.Now()
	b.mu.Lock()
	if _, seen := b.first[id]; !seen {
		b.first[id] = now
		b.total++
		if info.Status == store.StatusPending {
			b.inflight[id] = struct{}{}
		}
	}
	if info.Status != store.StatusReady && info.Status != store.StatusFailed {
		b.mu.Unlock()
		return
	}
	if b.printed[id] == info.Status {
		b.mu.Unlock()
		return
	}
	b.printed[id] = info.Status
	delete(b.inflight, id)
	b.done++
	var failLine string
	switch {
	case info.Status == store.StatusFailed:
		b.failed++
		failLine = fmt.Sprintf("%s %s (%s) — %s",
			b.paint("✗", ansiRed), id, fmtElapsed(now.Sub(b.first[id])),
			progressDetail(info.Message))
	case store.IsSkipped(info), store.IsSuspended(info), store.IsUnchanged(info):
		b.skipped++
	}
	b.mu.Unlock()
	// Failures are the one terminal event worth keeping in scroll-back; the
	// router slots the line above the bar. Best-effort: a stderr write error
	// mid-run is unactionable.
	if failLine != "" {
		_, _ = io.WriteString(b.r, failLine+"\n")
	}
}

// start launches the spinner ticker. It paints ~10×/s until stop is closed.
func (b *statusBar) start() {
	b.stop = make(chan struct{})
	b.wg.Go(func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for spin := 0; ; spin++ {
			select {
			case <-b.stop:
				return
			case <-t.C:
				b.repaint(spin)
			}
		}
	})
}

// finish stops the ticker, erases the bar, and prints the run summary in its
// place. Safe to call once after the attach unsubscribe has fired.
func (b *statusBar) finish() {
	if b.stop != nil {
		close(b.stop)
		b.wg.Wait()
	}
	b.r.Stop()
	b.mu.Lock()
	summary := b.summary(time.Since(b.startedAt))
	b.mu.Unlock()
	_, _ = io.WriteString(b.r, summary+"\n")
}

// repaint snapshots the live counts under the lock and paints one frame.
func (b *statusBar) repaint(spin int) {
	b.mu.Lock()
	done, total := b.done, b.total
	labels := b.inflightLabels()
	b.mu.Unlock()
	glyph := string(spinnerFrames[spin%len(spinnerFrames)])
	frame := renderFrame(glyph, done, total, labels,
		time.Since(b.startedAt), b.r.width(), b.color)
	b.r.Paint(frame)
}

// inflightLabels returns the in-flight resource names ordered by first-seen
// (id-tiebroken) so the bar's name list is stable frame to frame. Caller holds
// b.mu.
func (b *statusBar) inflightLabels() []string {
	ids := make([]manifest.NamedResource, 0, len(b.inflight))
	for id := range b.inflight {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ti, tj := b.first[ids[i]], b.first[ids[j]]
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return ids[i].Compare(ids[j]) < 0
	})
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = id.Name
	}
	return names
}

// summary is the one-line report the bar leaves behind. Caller holds b.mu.
func (b *statusBar) summary(elapsed time.Duration) string {
	parts := []string{fmt.Sprintf("%s %d rendered", b.paint("✓", ansiGreen), b.done-b.failed-b.skipped)}
	if b.failed > 0 {
		parts = append(parts, fmt.Sprintf("%s %d failed", b.paint("✗", ansiRed), b.failed))
	}
	if b.skipped > 0 {
		parts = append(parts, fmt.Sprintf("%s %d skipped", b.paint("–", ansiDim), b.skipped))
	}
	return fmt.Sprintf("flate: %s in %s", strings.Join(parts, ", "), fmtElapsed(elapsed))
}

// paint wraps s in an ANSI color when color is enabled, else returns it bare.
func (b *statusBar) paint(s, code string) string {
	if b.color {
		return code + s + ansiReset
	}
	return s
}

// maxInflightNames caps how many in-flight resource names the bar lists before
// collapsing the rest into a "+N" tail.
const maxInflightNames = 2

// spinnerFrames is the classic braille spinner cycle.
var spinnerFrames = []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

// ANSI SGR codes. Width math never counts these — they are zero visible runes.
const (
	ansiReset = "\x1b[0m"
	ansiBold  = "\x1b[1m"
	ansiDim   = "\x1b[2m"
	ansiRed   = "\x1b[31m"
	ansiGreen = "\x1b[32m"
	ansiCyan  = "\x1b[36m"
)

// renderFrame composes one status-bar line and truncates it to width without
// counting ANSI codes against the budget. Pure: deterministic in its inputs,
// so the layout, truncation, and "+N" collapse are all unit-testable.
//
//	⠹ [42/86] plex, factorio +3  12.3s
func renderFrame(spin string, done, total int, inflight []string, elapsed time.Duration, width int, color bool) string {
	segs := []segment{
		{spin, ansiCyan},
		{" ", ""},
		{fmt.Sprintf("[%d/%d]", done, total), ansiBold},
	}
	if names := summarizeInflight(inflight, maxInflightNames); names != "" {
		segs = append(segs, segment{" ", ""}, segment{names, ansiDim})
	}
	segs = append(segs, segment{"  ", ""}, segment{fmtElapsed(elapsed), ansiDim})
	return assemble(segs, width, color)
}

// segment is a run of text with an optional color; assemble emits the code
// only when color is on, and never counts it toward the width budget.
type segment struct {
	text string
	code string
}

// assemble concatenates segments, truncating with an ellipsis the moment the
// visible width would exceed width. Color codes wrap each kept segment but are
// invisible to the width count.
func assemble(segs []segment, width int, color bool) string {
	if width < 1 {
		width = 1
	}
	var b strings.Builder
	used := 0
	for _, s := range segs {
		if s.text == "" {
			continue
		}
		runes := []rune(s.text)
		if used+len(runes) <= width {
			writeSegment(&b, s.text, s.code, color)
			used += len(runes)
			continue
		}
		// Overflow: keep what fits, reserving one column for the ellipsis.
		if remain := width - used; remain >= 2 {
			writeSegment(&b, string(runes[:remain-1])+"…", s.code, color)
		} else if remain == 1 {
			writeSegment(&b, "…", s.code, color)
		}
		break
	}
	return b.String()
}

func writeSegment(b *strings.Builder, text, code string, color bool) {
	if color && code != "" {
		b.WriteString(code)
		b.WriteString(text)
		b.WriteString(ansiReset)
		return
	}
	b.WriteString(text)
}

// summarizeInflight renders up to limit names joined by ", ", collapsing any
// remainder into a "+N" tail. Empty in → empty out.
func summarizeInflight(names []string, limit int) string {
	switch {
	case len(names) == 0:
		return ""
	case len(names) <= limit:
		return strings.Join(names, ", ")
	default:
		return fmt.Sprintf("%s +%d", strings.Join(names[:limit], ", "), len(names)-limit)
	}
}

// fmtElapsed renders a duration compactly: tenths of a second under a minute,
// m+ss above it.
func fmtElapsed(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dm%02ds", int(d/time.Minute), int(d%time.Minute/time.Second))
}

// colorEnabled reports whether ANSI color is appropriate, honoring the
// NO_COLOR convention and dumb/absent terminals.
func colorEnabled() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	switch os.Getenv("TERM") {
	case "", "dumb":
		return false
	default:
		return true
	}
}

// terminalWidth reports w's column count when it is a sized terminal, else 80.
func terminalWidth(w io.Writer) int {
	if f, ok := w.(*os.File); ok {
		if cols, _, err := term.GetSize(int(f.Fd())); err == nil && cols > 0 {
			return cols
		}
	}
	return 80
}
