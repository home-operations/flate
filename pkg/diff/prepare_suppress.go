package diff

import (
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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

func suppress(orig, current Snapshot) suppressionResult {
	plans := selectedSuppressionPlans(orig, current)
	l, r := orig.keep(plans, SideOrig), current.keep(plans, SideCurrent)
	suppressed := slices.Grow([]Suppression(nil), len(plans))
	for key := range plans {
		plan := plans[key]
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
		if _, separate := plans[note.Subject.ID]; separate && note.Subject.ID != key {
			note.Subject = manifest.DiagnosticIdentity{}
		}
		suppressed = append(suppressed, note)
	}
	slices.SortFunc(suppressed, func(a, b Suppression) int {
		return a.Parent.Compare(b.Parent.NamedResource)
	})
	return suppressionResult{Orig: l, Current: r, Suppressed: suppressed}
}

func (s Snapshot) keep(plans map[manifest.NamedResource]suppressionPlan, side Side) []manifest.DocumentBatch {
	out := make([]manifest.DocumentBatch, 0, len(s.Manifests))
	for _, batch := range s.Manifests {
		if batch.Parent == nil {
			out = append(out, batch)
			continue
		}
		id := batch.Parent.Named()
		key := id
		if comparison, ok := s.ProducerKeys[id]; ok {
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

func prepareSnapshots(orig, current Snapshot, opts *Options) (prepared, []Suppression) {
	retained := suppress(orig, current)
	options := *opts
	options.subjects = [2]map[manifest.NamedResource]manifest.DiagnosticIdentity{orig.Subjects, current.Subjects}
	options.producerKeys = [2]map[manifest.NamedResource]manifest.NamedResource{
		orig.pairingKeys(current.Manifests), current.pairingKeys(orig.Manifests),
	}
	return prepare(documents(retained.Orig), documents(retained.Current), &options), retained.Suppressed
}

// A producer that also emits on the other side MUST pair under its own identity.
func (s Snapshot) pairingKeys(other []manifest.DocumentBatch) map[manifest.NamedResource]manifest.NamedResource {
	keys := s.ProducerKeys
	for producer := range s.ProducerKeys {
		if slices.ContainsFunc(other, func(batch manifest.DocumentBatch) bool {
			return batch.Parent != nil && batch.Parent.Named() == producer
		}) {
			keys = maps.Clone(keys)
			delete(keys, producer)
		}
	}
	return keys
}

type suppressionResult struct {
	Orig, Current []manifest.DocumentBatch
	Suppressed    []Suppression
}
