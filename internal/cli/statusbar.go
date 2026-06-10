package cli

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"

	"github.com/home-operations/flate/internal/style"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// barModel is the live, single-line progress indicator flate paints to stderr
// during a run — a Bubble Tea model rendered inline:
//
//	⠹ [42/86] plex, factorio +3  12.3s
//
// Bubble Tea owns the render loop, the braille spinner (bubbles/spinner), resize,
// and printing log lines above the frame; the store's status events feed it via
// Program.Send. The bar is purely a loading indicator: it prints nothing
// permanent and clears itself (an empty View on finishMsg) when the run ends.
// Failures aren't surfaced here — flate's end-of-run report prints them once the
// bar is gone.
//
// Update runs on Bubble Tea's single goroutine, so the model needs no mutex even
// though statusMsg crosses from the orchestrator's reconcile goroutines.
type barModel struct {
	spinner   spinner.Model
	color     bool
	width     int
	startedAt time.Time
	finished  bool // set on finishMsg → View renders empty so the bar vanishes

	// first records each id's first-seen time — drives in-flight ordering.
	first    map[manifest.NamedResource]time.Time
	inflight map[manifest.NamedResource]struct{}
	// printed records the last terminal status counted per id, so an idempotent
	// re-write doesn't double-count and a genuine flip recounts.
	printed map[manifest.NamedResource]store.Status
	done    int // declared resources that reached a terminal status
	// total counts every declared id seen so far — a live lower bound that grows
	// as discovery and renders surface more resources. Synthetic/internal ids
	// (the bootstrap source, synthesized HelmCharts) are excluded via barExcluded
	// so the denominator matches flate's own report rather than overshooting it.
	total int
}

// statusMsg carries one store status event into the bar's Update loop.
type statusMsg struct {
	id   manifest.NamedResource
	info store.StatusInfo
}

// finishMsg clears the frame and quits the program — the bar leaves no trace.
type finishMsg struct{}

func newBarModel(color bool) barModel {
	return barModel{
		spinner:   spinner.New(spinner.WithSpinner(spinner.MiniDot)),
		color:     color,
		width:     80,
		startedAt: time.Now(),
		first:     map[manifest.NamedResource]time.Time{},
		inflight:  map[manifest.NamedResource]struct{}{},
		printed:   map[manifest.NamedResource]store.Status{},
	}
}

// Init starts the spinner animation; subsequent TickMsgs re-render the frame
// (which also refreshes the elapsed clock).
func (m barModel) Init() tea.Cmd { return m.spinner.Tick }

func (m barModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		if msg.Width > 0 {
			m.width = msg.Width
		}
		return m, nil
	case statusMsg:
		m.track(msg.id, msg.info)
		return m, nil
	case finishMsg:
		m.finished = true
		return m, tea.Quit
	}
	return m, nil
}

// track applies one status event: the declared-only counting + in-flight set
// behind the [done/total] gauge. A first sighting grows total (and joins the
// in-flight set while Pending); any terminal status (Ready / Failed / a
// skipped-or-suspended Ready) counts toward done once and drops the id from
// in-flight. barExcluded drops synthetic/internal ids so the gauge mirrors the
// report.
func (m *barModel) track(id manifest.NamedResource, info store.StatusInfo) {
	if barExcluded(id) {
		return
	}
	if _, seen := m.first[id]; !seen {
		m.first[id] = time.Now()
		m.total++
		if info.Status == store.StatusPending {
			m.inflight[id] = struct{}{}
		}
	}
	if info.Status != store.StatusReady && info.Status != store.StatusFailed {
		return
	}
	if m.printed[id] == info.Status {
		return
	}
	m.printed[id] = info.Status
	delete(m.inflight, id)
	m.done++
}

// View composes one frame, styled via internal/style and truncated to the
// terminal width (ANSI- and wide-rune-aware). An empty frame after finishMsg
// clears the line so the bar vanishes without a trace.
func (m barModel) View() tea.View {
	if m.finished {
		return tea.NewView("")
	}
	line := style.Cyan(m.spinner.View(), m.color) + " " +
		style.Bold(fmt.Sprintf("[%d/%d]", m.done, m.total), m.color)
	if names := summarizeInflight(m.inflightLabels(), maxInflightNames); names != "" {
		line += " " + style.Dim(names, m.color)
	}
	line += "  " + style.Dim(style.Elapsed(time.Since(m.startedAt)), m.color)
	return tea.NewView(style.Truncate(line, m.width))
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

// inflightLabels returns the in-flight resource names ordered by first-seen
// (id-tiebroken) so the bar's name list is stable frame to frame.
func (m barModel) inflightLabels() []string {
	ids := make([]manifest.NamedResource, 0, len(m.inflight))
	for id := range m.inflight {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ti, tj := m.first[ids[i]], m.first[ids[j]]
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
