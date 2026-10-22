package diff

import (
	"fmt"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Side names the side of a comparison. Left is the orig (base, --path-orig)
// tree and right the current (head, --path) tree. Suppression also uses "both"
// when output is withheld for unavailable producers on both sides.
type Side string

// Side values, in the vocabulary the CLI uses for --path-orig / --path.
const (
	SideOrig    Side = "orig"
	SideCurrent Side = "current"
	SideBoth    Side = "both"
)

// Suppression records a producer whose rendered objects were withheld because
// rendering failed, was soft-skipped, or was suspended.
type Suppression struct {
	Parent Parent `json:"parent"`
	Side   Side   `json:"side"`   // orig, current, or both unavailable sides
	Count  int    `json:"count"`  // withheld healthy-side entries, or all entries for both
	Reason string `json:"reason"` // the render failure, skip, or suspension reason
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
		plan.note.Reason = "failed on both sides: orig: " + left.Message + "; current: " + right.Message
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
		plan.note.Reason = left.Message
		if ls {
			plan.note.Reason = "suspended on orig"
			plan.discloseEmpty = true
		}
	case ru && !lu:
		plan.note.Side = SideCurrent
		plan.note.Reason = right.Message
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
	return string(side) + ": " + info.Message
}

// Producer identity excludes paths; counts measure withheld document entries.
func suppress(orig, current Snapshot) ([]manifest.BaseManifest, []manifest.BaseManifest, []Suppression) {
	plans := selectedSuppressionPlans(orig.Unavailable, current.Unavailable)
	keep := func(snapshot Snapshot, side Side) []manifest.BaseManifest {
		out := make([]manifest.BaseManifest, 0, len(snapshot.Objects))
		for _, obj := range snapshot.Objects {
			id := obj.Named()
			plan, ok := plans[id]
			if !ok {
				out = append(out, obj)
				continue
			}
			plan.note.Parent = objectParent(obj)
			if plan.note.Side == SideBoth || plan.note.Side != side {
				plan.note.Count += len(snapshot.Manifests[id])
			}
			plans[id] = plan
		}
		return out
	}
	l, r := keep(orig, SideOrig), keep(current, SideCurrent)
	var suppressed []Suppression
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
