package orchestrator

import (
	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) failureResult() *Result {
	dagDispatcher{o}.BlockTillDone()
	return &Result{Warnings: o.store.Warnings(), Work: o.freezeWork()}
}

func (o *Orchestrator) freezeWork() manifest.WorkSummary {
	if !o.workFrozen {
		o.work = o.store.WorkSnapshot()
		o.workFrozen = true
	}
	return o.work
}
