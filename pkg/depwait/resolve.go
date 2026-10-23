package depwait

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

// Resolution retains the current invocation's data, omission positions and wait.
type Resolution struct {
	Inputs  values.ResolvedInputs
	Omitted []int
	err     error
}

func (r *Resolution) park() *ErrBlocked {
	wait, ok := errors.AsType[*ErrBlocked](r.err)
	if !ok {
		wait = &ErrBlocked{}
		r.err = wait
	}
	return wait
}

func (r *Resolution) add(parent, id manifest.NamedResource, cl classification) {
	switch cl.Kind {
	case schedule.Blocked:
		wait := r.park()
		wait.Deps = append(wait.Deps, id)
	case schedule.DependencyFailed:
		if failed, ok := errors.AsType[*manifest.DependencyFailedError](r.err); ok {
			i, _ := slices.BinarySearchFunc(failed.Failed, id, manifest.NamedResource.Compare)
			failed.Failed = slices.Compact(slices.Insert(failed.Failed, i, id))
			failed.Reasons[id] = cl.Message
		} else if r.err == nil {
			r.err = &manifest.DependencyFailedError{Parent: parent, Failed: []manifest.NamedResource{id}, Reasons: map[manifest.NamedResource]string{id: cl.Message}}
		}
	}
}

// Err returns parking before canonical required failures.
func (r *Resolution) Err() error { return r.err }

// Classify resolves prerequisites and content uses into a zero-valued caller-owned result.
func (w *Waiter) Classify(attempt manifest.ReconcileAttempt, r *Resolution) {
	for _, dep := range w.Dependencies {
		if cl := w.classify(dep, attempt.Drain, w.Existence); cl.Kind != schedule.Terminal {
			r.add(w.Parent, dep.NamedResource, cl)
		}
	}
	if w.Declaration != nil {
		w.content(r, attempt)
	}
}

func (w *Waiter) content(r *Resolution, attempt manifest.ReconcileAttempt) {
	prior := attempt.Content
	if prior == nil {
		prior = &manifest.ContentWait{}
	}
	sampled := slices.Clone(prior.Suppliers)
	for i := range sampled {
		sampled[i].Observed = sampled[i].Word.Load()
	}
	snapshots := make([]store.ReferenceFacts, 0, 1)
	var suppliers []manifest.Opportunity
	var targets []manifest.NamedResource
	for ref := range manifest.References(w.Declaration) {
		if ref.Name == "" || ref.Use != "substituteFrom" && ref.Use != "valuesFrom" || ref.Kind != manifest.KindSecret && ref.Kind != manifest.KindConfigMap {
			continue
		}
		id := ref.NamedResource
		input := slices.IndexFunc(r.Inputs, func(v values.ResolvedInput) bool { return v.NamedResource == id })
		var facts store.ReferenceFacts
		data, provided := w.Supplied[id].(manifest.Data)
		if input >= 0 {
			facts = snapshots[input]
			data = r.Inputs[input].Value
		} else {
			facts = w.Store.ReferenceFacts(id, nil)
			if facts.Data == nil && !provided && w.Existence != nil && w.Existence.Promote(id) {
				facts = w.Store.ReferenceFacts(id, nil)
			}
			snapshots = append(snapshots, facts)
		}
		liveOnly, independent, self, pending := false, false, false, false
		start := len(suppliers)
		for _, p := range facts.Productions {
			liveOnly = liveOnly || p.Declaration != (manifest.NamedResource{})
			independent = independent || p.Owner == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) && p.Path != ""
			self = self || p.Owner == w.Parent
			if p.Owner != (manifest.NamedResource{}) && p.Owner != w.Parent {
				o := manifest.Opportunity{NamedResource: p.Owner}
				if i := slices.IndexFunc(sampled, func(o manifest.Opportunity) bool { return o.NamedResource == p.Owner }); i >= 0 {
					o = sampled[i]
				}
				suppliers = append(suppliers, o)
				pending = pending || o.Word == nil || o.Observed&1 != 0
			}
		}
		if input < 0 && !provided && (facts.Accepted || independent || len(suppliers) == start) {
			data = facts.Data
		}
		if data == nil && ref.Use == "substituteFrom" && w.WipeSecrets && liveOnly && !pending {
			data = values.DeclaredData(w.Store, id, facts.Productions)
		}
		if data != nil {
			suppliers = suppliers[:start]
		}
		if data == nil {
			if len(suppliers) > start {
				targets = append(targets, id)
			}
			if !ref.Optional && (ref.Use != "substituteFrom" || ref.Kind != manifest.KindSecret && !self) {
				if ref.Use == "valuesFrom" && (liveOnly && !pending || w.AllowMissingSecrets && attempt.Drain >= manifest.DrainCascade) {
					r.Omitted = append(r.Omitted, ref.Position)
				} else {
					r.add(w.Parent, id, w.classify(ref.DependencyRef, attempt.Drain, nil))
				}
			}
			if !ref.Optional && ref.Use == "substituteFrom" && !self {
				for _, supplier := range suppliers[start:] {
					r.add(w.Parent, supplier.NamedResource, w.classify(manifest.DependencyRef{NamedResource: supplier.NamedResource}, attempt.Drain, w.Existence))
				}
			}
		}
		if input < 0 {
			r.Inputs = append(r.Inputs, values.ResolvedInput{NamedResource: id, Value: data})
		}
	}

	slices.SortFunc(targets, manifest.NamedResource.Compare)
	targets = slices.Compact(targets)
	slices.SortFunc(suppliers, func(a, b manifest.Opportunity) int { return a.Compare(b.NamedResource) })
	suppliers = slices.Compact(suppliers)
	cut := prior.Declaration == w.Declaration && slices.Equal(targets, prior.Targets) && len(suppliers) == len(prior.Suppliers)
	blocked := false
	for i, o := range suppliers {
		word := o.Observed
		if o.Word != nil {
			word = o.Word.Load()
		}
		blocked = blocked || o.Word == nil || word != o.Observed || word&1 != 0
		cut = cut && o.Word != nil && o.NamedResource == prior.Suppliers[i].NamedResource && word == o.Observed && (word&1 == 0 || o.Cut == word)
	}
	if blocked && !cut {
		for i := range suppliers {
			suppliers[i].Cut = 0
		}
		r.park().Content = &manifest.ContentWait{Targets: targets, Suppliers: suppliers, Declaration: w.Declaration}
	}
}
