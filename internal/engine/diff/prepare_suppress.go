package diff

import (
	"errors"
	"fmt"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type suppressionPlan struct {
	parent        Parent
	side          side
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
	id, found := s.Bindings.Producer(key)
	if !found {
		return manifest.DiagnosticIdentity{}
	}
	if subject, ok := s.Subjects[id]; ok {
		return subject
	}
	return manifest.DiagnosticFor(id, nil)
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
		plan.side = sideBoth
		plan.reason = "failed on both sides: orig: " + left.String() + "; current: " + right.String()
		plan.discloseEmpty = true
	case ls && rs:
		plan.side = sideBoth
		plan.reason = "suspended on orig; suspended on current"
		plan.discloseEmpty = true
	case ls && ru || rs && lu:
		plan.side = sideBoth
		plan.reason = unavailableReason(left, sideOrig) + "; " + unavailableReason(right, sideCurrent)
		plan.discloseEmpty = true
	case lu && !ru:
		plan.side = sideOrig
		plan.reason = left.String()
		if ls {
			plan.reason = "suspended on orig"
			plan.discloseEmpty = true
		}
	case ru && !lu:
		plan.side = sideCurrent
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

func unavailableReason(info store.StatusInfo, side side) string {
	if info.Outcome() == store.OutcomeSuspended {
		return "suspended on " + string(side)
	}
	return string(side) + ": " + info.String()
}

func suppress(orig, current Snapshot) suppressionResult {
	plans := selectedSuppressionPlans(orig, current)
	l, r := orig.keep(plans, sideOrig), current.keep(plans, sideCurrent)
	suppressed := slices.Grow([]Suppression(nil), len(plans))
	for key := range plans {
		plan := plans[key]
		if plan.count == 0 && !plan.discloseEmpty {
			continue
		}
		note := Suppression{Parent: plan.parent, Side: plan.side, Count: plan.count, Reason: plan.reason}
		if plan.side == sideOrig {
			note.Subject = orig.subject(key)
		} else {
			note.Subject = current.subject(key)
			if plan.side == sideBoth && note.Subject.ID == (manifest.NamedResource{}) {
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

func (s Snapshot) keep(plans map[manifest.NamedResource]suppressionPlan, side side) Snapshot {
	if len(plans) == 0 {
		return s
	}
	out := s
	out.Manifests = make([]manifest.DocumentBatch, 0, len(s.Manifests))
	out.Producers = nil
	for i, batch := range s.Manifests {
		if batch.Parent != nil {
			key, _ := s.Bindings.Slot(batch.Parent.Named())
			if plan, ok := plans[key]; ok {
				plan.parent = objectParent(batch.Parent)
				if plan.side == sideBoth || plan.side != side {
					plan.count += len(batch.Documents)
				}
				plans[key] = plan
				continue
			}
		}
		if producers := s.Producers[i]; len(producers) != 0 {
			if out.Producers == nil {
				out.Producers = make(map[int]map[int]manifest.DiagnosticIdentity)
			}
			out.Producers[len(out.Manifests)] = producers
		}
		out.Manifests = append(out.Manifests, batch)
	}
	return out
}

func prepareSnapshots(orig, current Snapshot, opts *Options) (prepared, []Suppression, error) {
	var inputErr error
	for sideIndex, batches := range [2][]manifest.DocumentBatch{orig.Manifests, current.Manifests} {
		for i, batch := range batches {
			if _, err := batch.IdentityAccessor(); err != nil {
				inputErr = errors.Join(
					inputErr,
					fmt.Errorf("%s batch %d: %w", [2]side{sideOrig, sideCurrent}[sideIndex], i, err),
				)
			}
		}
	}
	retained := suppress(orig, current)
	options := *opts
	options.subjects = [2]map[manifest.NamedResource]manifest.DiagnosticIdentity{orig.Subjects, current.Subjects}
	if inputErr != nil {
		return prepared{Subjects: options.subjects}, retained.Suppressed, inputErr
	}
	return prepare(documents(retained.Orig), documents(retained.Current), &options), retained.Suppressed, nil
}

type suppressionResult struct {
	Orig, Current Snapshot
	Suppressed    []Suppression
}
