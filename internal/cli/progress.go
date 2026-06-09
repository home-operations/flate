package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// progressWriter is the stderr sink live progress lines go to. Set by the
// root command's PersistentPreRunE when stderr is a terminal and
// --no-progress is unset; nil disables progress entirely (pipes, CI, the
// in-process e2e harness). stdout is never written to — the rendered
// output stays byte-deterministic.
var progressWriter io.Writer

// writerIsTTY reports whether w is a character device (an interactive
// terminal). Buffers and pipes — CI, redirections, the e2e harness's
// bytes.Buffer — are not, so progress stays off there without a flag.
func writerIsTTY(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// progressReporter prints one line per resource to stderr the moment it
// reaches a terminal status, so a long first run (cold source fetches, big
// renders) reads as live work instead of silence until the buffered render
// is emitted. Lines look like:
//
//	[12/86] ✓ Kustomization/flux-system/apps (1.2s)
//	[13/86] ✗ HelmRelease/media/plex (30s) — dependency not found
//	[14/86] – Kustomization/games/factorio (0s) (suspended)
//
// done counts resources that reached a terminal status; the denominator is
// every resource seen so far (including still-Pending ones), so it grows as
// renders emit children — a live lower bound, not a fixed plan.
type progressReporter struct {
	w io.Writer

	mu sync.Mutex
	// first records each id's first status event (its Pending arrival) so
	// the terminal line can show a per-resource elapsed time.
	first map[manifest.NamedResource]time.Time
	// printed records the last terminal status printed per id: idempotent
	// terminal re-writes are suppressed, while a genuine flip (a Refire
	// resurrection ending differently) prints a fresh line.
	printed map[manifest.NamedResource]store.Status
}

func newProgressReporter(w io.Writer) *progressReporter {
	return &progressReporter{
		w:       w,
		first:   map[manifest.NamedResource]time.Time{},
		printed: map[manifest.NamedResource]store.Status{},
	}
}

// attach subscribes the reporter to s's status events. Call before the
// orchestrator runs (the store may still be empty); the returned
// unsubscribe must be called once the run ends.
func (p *progressReporter) attach(s *store.Store) store.Unsubscribe {
	return s.OnStatus(p.onStatus, false)
}

func (p *progressReporter) onStatus(id manifest.NamedResource, info store.StatusInfo) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	started, seen := p.first[id]
	if !seen {
		p.first[id] = now
		started = now
	}
	if info.Status != store.StatusReady && info.Status != store.StatusFailed {
		return
	}
	if p.printed[id] == info.Status {
		return
	}
	p.printed[id] = info.Status
	mark, detail := "✓", ""
	switch {
	case info.Status == store.StatusFailed:
		mark, detail = "✗", " — "+progressDetail(info.Message)
	case store.IsSkipped(info), store.IsSuspended(info), store.IsUnchanged(info):
		mark, detail = "–", " ("+progressDetail(info.Message)+")"
	}
	// A stderr write failure is unactionable mid-run; progress is best-effort.
	_, _ = fmt.Fprintf(p.w, "[%d/%d] %s %s (%s)%s\n",
		len(p.printed), len(p.first), mark, id,
		now.Sub(started).Round(10*time.Millisecond), detail)
}

// progressDetail reduces a status message to a one-line hint: first line
// only, capped at 120 runes. Full failure detail belongs to the final
// report, not the live ticker.
func progressDetail(msg string) string {
	if i := strings.IndexByte(msg, '\n'); i >= 0 {
		msg = msg[:i]
	}
	if r := []rune(msg); len(r) > 120 {
		return string(r[:119]) + "…"
	}
	return msg
}
