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
	Subjects    map[manifest.NamedResource]manifest.DiagnosticIdentity
	Manifests   []manifest.DocumentBatch
	Unavailable map[manifest.NamedResource]store.StatusInfo
	// ProducerKeys maps actual producers to their paired comparison identities.
	ProducerKeys map[manifest.NamedResource]manifest.NamedResource
}

// Suppression discloses withheld producer output.
type Suppression struct {
	Parent  Parent
	Subject manifest.DiagnosticIdentity
	Side    Side
	Count   int
	Reason  string
}

// String renders the suppression as a human-readable sentence.
func (s Suppression) String() string {
	noun := "objects"
	if s.Count == 1 {
		noun = "object"
	}
	reason := strings.Join(strings.Fields(s.Reason), " ")
	if s.Side == SideBoth {
		return fmt.Sprintf("%d %s withheld; diff suppressed (%s)", s.Count, noun, reason)
	}
	return fmt.Sprintf("%d %s not rendered on the %s side; diff suppressed (%s)", s.Count, noun, s.Side, reason)
}

type suppressionPlan struct {
	parent        Parent
	side          Side
	count         int
	reason        string
	discloseEmpty bool
}

func selectedSuppressionPlans(left, right Snapshot) map[manifest.NamedResource]suppressionPlan {
	plans := make(map[manifest.NamedResource]suppressionPlan, len(left.Unavailable)+len(right.Unavailable))
	add := func(id manifest.NamedResource) {
		if plan, ok := planSuppression(left.Unavailable[id], right.Unavailable[id]); ok {
			plan.parent = Parent{NamedResource: id}
			plans[id] = plan
		}
	}
	for id := range left.Unavailable {
		add(id)
	}
	for id := range right.Unavailable {
		if _, ok := left.Unavailable[id]; !ok {
			add(id)
		}
	}
	return plans
}

func (s Snapshot) subject(key manifest.NamedResource) manifest.DiagnosticIdentity {
	if subject, ok := s.Subjects[key]; ok {
		return subject
	}
	var subject manifest.DiagnosticIdentity
	for id, comparison := range s.ProducerKeys {
		if comparison == key && (subject.ID == (manifest.NamedResource{}) || id.Compare(subject.ID) < 0) {
			subject = manifest.DiagnosticFor(id, nil)
			if captured, ok := s.Subjects[id]; ok {
				subject = captured
			}
		}
	}
	return subject
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
		plan.side = SideBoth
		plan.reason = "failed on both sides: orig: " + left.String() + "; current: " + right.String()
		plan.discloseEmpty = true
	case ls && rs:
		plan.side = SideBoth
		plan.reason = "suspended on orig; suspended on current"
		plan.discloseEmpty = true
	case ls && ru || rs && lu:
		plan.side = SideBoth
		plan.reason = unavailableReason(left, SideOrig) + "; " + unavailableReason(right, SideCurrent)
		plan.discloseEmpty = true
	case lu && !ru:
		plan.side = SideOrig
		plan.reason = left.String()
		if ls {
			plan.reason = "suspended on orig"
			plan.discloseEmpty = true
		}
	case ru && !lu:
		plan.side = SideCurrent
		plan.reason = right.String()
		if rs {
			plan.reason = "suspended on current"
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
	plans := selectedSuppressionPlans(orig, current)
	keep := func(snapshot Snapshot, side Side) []manifest.DocumentBatch {
		out := make([]manifest.DocumentBatch, 0, len(snapshot.Manifests))
		for _, batch := range snapshot.Manifests {
			if batch.Parent == nil {
				out = append(out, batch)
				continue
			}
			id := batch.Parent.Named()
			key := id
			if comparison, ok := snapshot.ProducerKeys[id]; ok {
				key = comparison
			}
			plan, ok := plans[key]
			if !ok {
				out = append(out, batch)
				continue
			}
			plan.parent = objectParent(batch.Parent)
			if plan.side == SideBoth || plan.side != side {
				plan.count += len(batch.Documents)
			}
			plans[key] = plan
		}
		return out
	}
	l, r := keep(orig, SideOrig), keep(current, SideCurrent)
	suppressed := slices.Grow([]Suppression(nil), len(plans))
	for key, plan := range plans {
		if plan.count == 0 && !plan.discloseEmpty {
			continue
		}
		note := Suppression{Parent: plan.parent, Side: plan.side, Count: plan.count, Reason: plan.reason}
		if plan.side == SideOrig {
			note.Subject = orig.subject(key)
		} else {
			note.Subject = current.subject(key)
			if plan.side == SideBoth && note.Subject.ID == (manifest.NamedResource{}) {
				note.Subject = orig.subject(key)
			}
		}
		suppressed = append(suppressed, note)
	}
	slices.SortFunc(suppressed, func(a, b Suppression) int {
		return a.Parent.Compare(b.Parent.NamedResource)
	})
	return l, r, suppressed
}
