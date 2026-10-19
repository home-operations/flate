package cli

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"

	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// barModel is the live, single-line progress indicator flate paints to stderr
// during a run — a Bubble Tea model rendered inline:
//
//	⠹ [42/86] plex, factorio +3  12.3s
//
// Bubble Tea owns the render loop, the braille spinner (bubbles/spinner), resize,
// and printing log lines above the frame; store events are drained on ticks. The bar is purely a loading indicator: it prints nothing
// permanent and clears itself (an empty View on finishMsg) when the run ends.
// Failures aren't surfaced here — flate's end-of-run report prints them once the
// bar is gone.
//
// Update owns the model maps; store callbacks only publish pending events.
type barModel struct {
	spinner   spinner.Model
	color     bool
	width     int
	startedAt time.Time
	finished  bool // set on finishMsg → View renders empty so the bar vanishes
	events    *progressEvents

	// first records each id's first-seen time — drives in-flight ordering.
	first    map[manifest.NamedResource]time.Time
	inflight map[manifest.NamedResource]struct{}
	// printed records the last terminal status counted per id, so an idempotent
	// re-write doesn't double-count and a genuine flip recounts.
	printed map[manifest.NamedResource]store.Status
	done    int // declared resources that reached a terminal status
	total   int
}

type progressEvent struct {
	id         manifest.NamedResource
	status     store.Status
	observedAt time.Time
	next       *progressEvent
}

type progressEvents struct {
	head atomic.Pointer[progressEvent]
}

func (e *progressEvents) publish(id manifest.NamedResource, status store.Status, observedAt time.Time) {
	event := &progressEvent{id: id, status: status, observedAt: observedAt}
	for {
		head := e.head.Load()
		event.next = head
		if e.head.CompareAndSwap(head, event) {
			return
		}
	}
}

func (e *progressEvents) take() *progressEvent {
	// Only the consumer owns detached links; a failed publisher CAS never reads them.
	head := e.head.Swap(nil)
	var first *progressEvent
	for head != nil {
		next := head.next
		head.next = first
		first, head = head, next
	}
	return first
}

// finishMsg clears the frame and quits the program — the bar leaves no trace.
type finishMsg struct{}

func newBarModel(color bool) barModel {
	return barModel{
		spinner:   spinner.New(spinner.WithSpinner(spinner.MiniDot)),
		color:     color,
		width:     80,
		startedAt: time.Now(),
		events:    &progressEvents{},
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
		m.drainEvents()
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		if msg.Width > 0 {
			m.width = msg.Width
		}
		return m, nil
	case finishMsg:
		m.finished = true
		return m, tea.Quit
	}
	return m, nil
}

func (m *barModel) drainEvents() {
	for event := m.events.take(); event != nil; event = event.next {
		m.track(event.id, event.status, event.observedAt)
	}
}

func (m *barModel) track(id manifest.NamedResource, status store.Status, observedAt time.Time) {
	if id.ReportHidden() {
		return
	}
	if first, seen := m.first[id]; !seen {
		m.first[id] = observedAt
		m.total++
		if status == store.StatusPending {
			m.inflight[id] = struct{}{}
		}
	} else if observedAt.Before(first) {
		m.first[id] = observedAt
	}
	if status != store.StatusReady && status != store.StatusFailed {
		return
	}
	if m.printed[id] == status {
		return
	}
	m.printed[id] = status
	delete(m.inflight, id)
	m.done++
}

// View composes one frame, styled via internal/output and truncated to the
// terminal width (ANSI- and wide-rune-aware). An empty frame after finishMsg
// clears the line so the bar vanishes without a trace.
func (m barModel) View() tea.View {
	if m.finished {
		return tea.NewView("")
	}
	line := output.Cyan(m.spinner.View(), m.color) + " " +
		output.Bold(fmt.Sprintf("[%d/%d]", m.done, m.total), m.color)
	if names := summarizeInflight(m.inflightLabels(), maxInflightNames); names != "" {
		line += " " + output.Dim(names, m.color)
	}
	line += "  " + output.Dim(output.Elapsed(time.Since(m.startedAt)), m.color)
	return tea.NewView(output.Truncate(line, m.width))
}

// inflightLabels returns the in-flight resource names ordered by first-seen
// (id-tiebroken) so the bar's name list is stable frame to frame.
func (m barModel) inflightLabels() []string {
	ids := make([]manifest.NamedResource, 0, len(m.inflight))
	for id := range m.inflight {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b manifest.NamedResource) int {
		return cmp.Or(m.first[a].Compare(m.first[b]), a.Compare(b))
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
