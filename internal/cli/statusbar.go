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

	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// statusBar is the live, single-line progress indicator flate paints to
// stderr during a run — one sticky frame that repaints in place:
//
//	⠹ [42/86] plex, factorio +3  12.3s
//
// A background ticker advances a braille spinner ~10×/s; the store's status
// events drive the counters and the in-flight set. The bar is purely a
// loading indicator: it prints nothing permanent and is erased without a
// trace when the run ends (finish). Failures aren't surfaced here — flate's
// own end-of-run report prints them once the bar is gone.
//
// The bar paints exclusively through a stderrRouter, which also carries slog
// output, so log records slot in cleanly above the bar without ever
// corrupting it. stdout is untouched.
type statusBar struct {
	r         *stderrRouter
	color     bool
	startedAt time.Time

	mu sync.Mutex
	// first records each id's first-seen time — drives in-flight ordering.
	first    map[manifest.NamedResource]time.Time
	inflight map[manifest.NamedResource]struct{}
	// printed records the last terminal status counted per id, so an
	// idempotent re-write doesn't double-count and a genuine flip recounts.
	printed map[manifest.NamedResource]store.Status
	done    int // declared resources that reached a terminal status
	// total counts every declared id seen so far — a live lower bound that grows
	// as discovery and renders surface more resources. Synthetic/internal ids
	// (the bootstrap source, synthesized HelmCharts) are excluded via barExcluded
	// so the denominator matches flate's own report rather than overshooting it.
	total int

	stop chan struct{}
	wg   sync.WaitGroup
}

func newStatusBar(r *stderrRouter) *statusBar {
	return &statusBar{
		r:         r,
		color:     style.ColorEnabled(r.w),
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

// barExcluded reports ids the bar must neither count nor name: the synthetic
// bootstrap GitRepository and any HelmChart (always internally synthesized for a
// HelmRepository-backed HelmRelease — see helmrelease.materializeHelmChartSource).
// Neither is a resource the user declared, and neither appears in flate's own
// report: `flate test` skips the bootstrap id and its kind set omits HelmChart.
// Counting them made the bar's [done/total] overshoot every report (e.g. 175 vs
// 174 when only the bootstrap leaks; +1 per HelmRepository-backed HelmRelease on
// top). Excluding the same two id-classes aligns the bar's population exactly
// with `flate test all`'s "collected N".
func barExcluded(id manifest.NamedResource) bool {
	return id == manifest.BootstrapSourceID || id.Kind == manifest.KindHelmChart
}

func (b *statusBar) onStatus(id manifest.NamedResource, info store.StatusInfo) {
	if barExcluded(id) {
		return
	}
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, seen := b.first[id]; !seen {
		b.first[id] = now
		b.total++
		if info.Status == store.StatusPending {
			b.inflight[id] = struct{}{}
		}
	}
	if info.Status != store.StatusReady && info.Status != store.StatusFailed {
		return
	}
	if b.printed[id] == info.Status {
		return
	}
	// Any terminal status (Ready, Failed, or a skipped/suspended/unchanged
	// Ready) means the resource is done loading: count it and drop it from the
	// in-flight set. The bar emits nothing here — it's a pure loading
	// indicator; failures surface via flate's end-of-run report after the bar
	// is erased.
	b.printed[id] = info.Status
	delete(b.inflight, id)
	b.done++
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

// finish stops the ticker and erases the bar, leaving no trace — the bar
// exists only during the loading phase. Safe to call once after the attach
// unsubscribe has fired.
func (b *statusBar) finish() {
	if b.stop != nil {
		close(b.stop)
		b.wg.Wait()
	}
	b.r.Stop()
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

// maxInflightNames caps how many in-flight resource names the bar lists before
// collapsing the rest into a "+N" tail.
const maxInflightNames = 2

// spinnerFrames is the classic braille spinner cycle.
var spinnerFrames = []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")

// renderFrame composes one status-bar line, styled via internal/style and
// truncated to width by visible columns (style.Truncate is ANSI- and wide-rune-
// aware, so escape codes never count against the budget). Pure: deterministic in
// its inputs, so the layout, truncation, and "+N" collapse are unit-testable.
//
//	⠹ [42/86] plex, factorio +3  12.3s
func renderFrame(spin string, done, total int, inflight []string, elapsed time.Duration, width int, color bool) string {
	var b strings.Builder
	b.WriteString(style.Cyan(spin, color))
	b.WriteByte(' ')
	b.WriteString(style.Bold(fmt.Sprintf("[%d/%d]", done, total), color))
	if names := summarizeInflight(inflight, maxInflightNames); names != "" {
		b.WriteByte(' ')
		b.WriteString(style.Dim(names, color))
	}
	b.WriteString("  ")
	b.WriteString(style.Dim(fmtElapsed(elapsed), color))
	return style.Truncate(b.String(), width)
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

// terminalWidth reports w's column count when it is a sized terminal, else 80.
func terminalWidth(w io.Writer) int {
	if f, ok := w.(*os.File); ok {
		if cols, _, err := term.GetSize(int(f.Fd())); err == nil && cols > 0 {
			return cols
		}
	}
	return 80
}
