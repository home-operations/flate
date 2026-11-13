package orchestrator

import (
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
)

func (o *Orchestrator) probeSlot(id manifest.NamedResource) **base.Execution {
	link := &o.probes
	for *link != nil {
		comparison := id.Compare((*link).Reads.ID())
		if comparison == 0 {
			break
		}
		if comparison < 0 {
			link = &(*link).ProbeLeft
		} else {
			link = &(*link).ProbeRight
		}
	}
	return link
}

func (o *Orchestrator) sweepProbes(final bool) {
	link := &o.probes
	count := 0
	for *link != nil {
		probe := *link
		if probe.ProbeLeft != nil {
			left := probe.ProbeLeft
			probe.ProbeLeft = left.ProbeRight
			left.ProbeRight = probe
			*link = left
			continue
		}
		remove := final || probe.ProbeConsumed
		if !remove {
			id := probe.Reads.ID()
			indexed := o.index.Records[id].Declaration
			if o.loader != nil && o.loader.CatalogObject(id) == nil {
				indexed = nil
			}
			remove = !probe.ProbeCurrent(indexed)
		}
		if remove {
			*link = probe.ProbeRight
			probe.ProbeLeft, probe.ProbeRight = nil, nil
			probe.Retire()
			continue
		}
		count++
		link = &probe.ProbeRight
	}
	list := o.probes
	o.probes = balanceProbes(&list, count)
}

func balanceProbes(list **base.Execution, count int) *base.Execution {
	if count == 0 {
		return nil
	}
	const branches = 2
	left := balanceProbes(list, count/branches)
	root := *list
	*list = root.ProbeRight
	root.ProbeLeft = left
	root.ProbeRight = balanceProbes(list, count-count>>1-1)
	return root
}
