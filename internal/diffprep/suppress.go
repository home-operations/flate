package diffprep

import (
	"fmt"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Side identifies an unavailable comparison side.
type Side string

// Comparison sides use the CLI orig/current vocabulary.
const (
	SideOrig    Side = "orig"
	SideCurrent Side = "current"
	SideBoth    Side = "both"
)

// Snapshot contains immutable selected batches and producer availability.
type Snapshot struct {
	Manifests   []manifest.DocumentBatch
	Unavailable map[manifest.NamedResource]store.StatusInfo
}

// Suppression discloses withheld producer output.
type Suppression struct {
	Parent Parent
	Side   Side
	Count  int
	Reason string
}

// String renders the suppression as a human-readable sentence.
func (s Suppression) String() string {
	noun := "objects"
	if s.Count == 1 {
		noun = "object"
	}
	label := s.Parent.KindLabel() + " " + s.Parent.NamespacedName()
	reason := strings.Join(strings.Fields(s.Reason), " ")
	if s.Side == SideBoth {
		return fmt.Sprintf("%s: %d %s withheld; diff suppressed (%s)",
			label, s.Count, noun, reason)
	}
	return fmt.Sprintf("%s: %d %s not rendered on the %s side; diff suppressed (%s)",
		label, s.Count, noun, s.Side, reason)
}

type suppressionPlan struct {
	note          Suppression
	discloseEmpty bool
}

func selectedSuppressionPlans(left, right map[manifest.NamedResource]store.StatusInfo) map[manifest.NamedResource]suppressionPlan {
	plans := make(map[manifest.NamedResource]suppressionPlan, len(left)+len(right))
	add := func(id manifest.NamedResource) {
		if plan, ok := planSuppression(left[id], right[id]); ok {
			plan.note.Parent = Parent{NamedResource: id}
			plans[id] = plan
		}
	}
	for id := range left {
		add(id)
	}
	for id := range right {
		if _, ok := left[id]; !ok {
			add(id)
		}
	}
	return plans
}

func planSuppression(left, right store.StatusInfo) (suppressionPlan, bool) {
	lo, ro := left.Outcome(), right.Outcome()
	lf := lo == store.OutcomeFailed || lo == store.OutcomeBlocked
	rf := ro == store.OutcomeFailed || ro == store.OutcomeBlocked
	ls, rs := lo == store.OutcomeSuspended, ro == store.OutcomeSuspended
	lu, ru := left.IsUnavailable(), right.IsUnavailable()
	plan := suppressionPlan{}
	switch {
	case lf && rf:
		plan.note.Side = SideBoth
		plan.note.Reason = "failed on both sides: orig: " + left.String() + "; current: " + right.String()
		plan.discloseEmpty = true
	case ls && rs:
		plan.note.Side = SideBoth
		plan.note.Reason = "suspended on orig; suspended on current"
		plan.discloseEmpty = true
	case ls && ru || rs && lu:
		plan.note.Side = SideBoth
		plan.note.Reason = unavailableReason(left, SideOrig) + "; " + unavailableReason(right, SideCurrent)
		plan.discloseEmpty = true
	case lu && !ru:
		plan.note.Side = SideOrig
		plan.note.Reason = left.String()
		if ls {
			plan.note.Reason = "suspended on orig"
			plan.discloseEmpty = true
		}
	case ru && !lu:
		plan.note.Side = SideCurrent
		plan.note.Reason = right.String()
		if rs {
			plan.note.Reason = "suspended on current"
			plan.discloseEmpty = true
		}
	default:
		return suppressionPlan{}, false
	}
	return plan, true
}

func unavailableReason(info store.StatusInfo, side Side) string {
	if info.Outcome() == store.OutcomeSuspended {
		return "suspended on " + string(side)
	}
	return string(side) + ": " + info.String()
}

// Suppress withholds unavailable producer batches on both comparison sides.
// Producer identity excludes paths; counts measure withheld document entries.
func Suppress(orig, current Snapshot) ([]manifest.DocumentBatch, []manifest.DocumentBatch, []Suppression) {
	plans := selectedSuppressionPlans(orig.Unavailable, current.Unavailable)
	keep := func(snapshot Snapshot, side Side) []manifest.DocumentBatch {
		out := make([]manifest.DocumentBatch, 0, len(snapshot.Manifests))
		for _, batch := range snapshot.Manifests {
			if batch.Parent == nil {
				out = append(out, batch)
				continue
			}
			id := batch.Parent.Named()
			plan, ok := plans[id]
			if !ok {
				out = append(out, batch)
				continue
			}
			plan.note.Parent = objectParent(batch.Parent)
			if plan.note.Side == SideBoth || plan.note.Side != side {
				plan.note.Count += len(batch.Documents)
			}
			plans[id] = plan
		}
		return out
	}
	l, r := keep(orig, SideOrig), keep(current, SideCurrent)
	suppressed := slices.Grow([]Suppression(nil), len(plans))
	for _, plan := range plans {
		if plan.note.Count > 0 || plan.discloseEmpty {
			suppressed = append(suppressed, plan.note)
		}
	}
	slices.SortFunc(suppressed, func(a, b Suppression) int {
		return a.Parent.Compare(b.Parent.NamedResource)
	})
	return l, r, suppressed
}
