package selection

import (
	"hash/maphash"
	"maps"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

const (
	needed uint8 = 1 << iota
	selected
	dirty
	ordinary
	promoted
	namedLookup
	readiness
	fatal
)

const decisionBuckets = 64

// Decision MUST remain immutable after publication, including every borrowed bucket.
type Decision struct {
	seed                 maphash.Seed
	buckets              [decisionBuckets]map[manifest.NamedResource]uint8
	revision             uint64
	ordering             map[manifest.NamedResource][]store.OrderingEdge
	orderingDeclarations map[manifest.NamedResource]orderingDeclaration
	execution            bool
	unrestricted         bool
	changedOnly          bool
}

type orderingDeclaration struct {
	declarationIdentity

	object manifest.BaseManifest
	source manifest.NamedResource
	owner  manifest.NamedResource
}

// OrderingDeclaration keeps indexed evidence subordinate to accepted producer ownership.
func (d *Decision) OrderingDeclaration(
	reads *store.ReadSet, id manifest.NamedResource, expressionOperand bool,
) manifest.BaseManifest {
	if obj := reads.GetDeclaration(id); obj != nil {
		return obj
	}
	if !d.EvaluationOnly(id) {
		return nil
	}
	evidence := d.orderingDeclarations[id]
	if evidence.object == nil || expressionOperand && evidence.owner != (manifest.NamedResource{}) {
		return nil
	}
	for _, owner := range reads.DeclarationProducers(id) {
		reads.ControllerDemand(owner)
		if reads.ProducerPublished(owner) || d.OrdinaryDemand(owner) && reads.GetDeclaration(owner) != nil {
			return nil
		}
	}
	if input := reads.GetArtifact(evidence.source, store.ArtifactContent); input != evidence.artifact ||
		!reads.SourceAdmission(id, evidence.source) || reads.ProducerPublished(evidence.owner) {
		return nil
	}
	return evidence.object
}

func (d *Decision) roles(id manifest.NamedResource) uint8 {
	if d == nil {
		return needed | selected | dirty | ordinary
	}
	return d.buckets[maphash.Comparable(d.seed, id)%decisionBuckets][id]
}

// Reconcile admits every identity when the decision is nil.
func (d *Decision) Reconcile(id manifest.NamedResource) bool {
	return d == nil || d.unrestricted || d.roles(id)&needed != 0
}

// Assess also dispatches readers reached only for failure reporting.
func (d *Decision) Assess(id manifest.NamedResource) bool {
	return d.Reconcile(id) || d.roles(id)&fatal != 0
}

// Output includes every identity when the decision is nil.
func (d *Decision) Output(id manifest.NamedResource) bool {
	return d == nil || d.unrestricted || d.roles(id)&selected != 0
}

// Changed treats unrestricted execution as changed.
func (d *Decision) Changed(id manifest.NamedResource) bool {
	return d == nil || d.unrestricted || d.roles(id)&dirty != 0
}

// OrdinaryDemand permits ordinary execution when the decision is nil.
func (d *Decision) OrdinaryDemand(id manifest.NamedResource) bool {
	return d == nil || d.unrestricted || d.roles(id)&ordinary != 0
}

// ReadinessDemand includes prerequisites that can fail without contributing output.
func (d *Decision) ReadinessDemand(id manifest.NamedResource) bool {
	return d != nil && !d.unrestricted && d.roles(id)&readiness != 0
}

// EvaluationOnly applies the change boundary to ordering prerequisites.
func (d *Decision) EvaluationOnly(id manifest.NamedResource) bool {
	return d != nil && d.changedOnly && !d.OrdinaryDemand(id)
}

func (d *Decision) reportsFailure(id manifest.NamedResource) bool {
	return d.Output(id) || d.roles(id)&fatal != 0
}

func (d *Decision) namespaces() map[string]struct{} {
	if !d.restricted() {
		return nil
	}
	var result map[string]struct{}
	for _, bucket := range d.buckets {
		for id, roles := range bucket {
			if roles&(selected|fatal) == 0 || id.ReportHidden() || id.Namespace == "" {
				continue
			}
			if result == nil {
				result = map[string]struct{}{}
			}
			result[id.Namespace] = struct{}{}
		}
	}
	return result
}

func (d *Decision) include(update *Admission, execution bool) (*Decision, []manifest.NamedResource) {
	if d.execution && !execution {
		return d, nil
	}
	next := *d
	if execution && !d.execution {
		next.buckets = [decisionBuckets]map[manifest.NamedResource]uint8{}
	}
	var copied [decisionBuckets]bool
	var included []manifest.NamedResource
	next.execution = execution
	changed := d.execution != execution
	for _, axis := range [7]struct {
		ids  []manifest.NamedResource
		role uint8
	}{
		{update.Ordinary, ordinary},
		{update.Needed, needed},
		{update.Selected, selected},
		{update.Dirty, dirty},
		{update.lookups, namedLookup},
		{update.Readiness, readiness},
		{update.Fatal, fatal},
	} {
		if !execution && (axis.role == selected || axis.role == dirty) {
			continue
		}
		for _, id := range axis.ids {
			if id == (manifest.NamedResource{}) {
				continue
			}
			updated, admitted := d.includeRole(&next, &copied, id, axis.role, execution)
			changed = changed || updated
			if admitted {
				included = append(included, id)
			}
		}
	}
	orderingChanged, included := d.updateOrdering(&next, update, &copied, included)
	declarationsChanged := next.replaceOrderingDeclarations(update.orderingDeclarations)
	if !changed && !orderingChanged && !declarationsChanged {
		return d, nil
	}
	next.revision = d.revision + 1
	slices.SortFunc(included, manifest.NamedResource.Compare)
	return &next, slices.Compact(included)
}

//nolint:revive,gocritic // Copy-on-write publication MUST retain both snapshots, copy ownership, and separate change/admission flags.
func (d *Decision) includeRole(
	next *Decision, copied *[decisionBuckets]bool, id manifest.NamedResource, role uint8, execution bool,
) (bool, bool) {
	bucket := maphash.Comparable(d.seed, id) % decisionBuckets
	previous := next.buckets[bucket][id]
	roles := previous | role
	if !d.changedOnly || role != readiness && role != fatal {
		roles |= needed
	}
	if role == selected {
		roles |= ordinary
	}
	var committed uint8
	if d.execution {
		committed = d.roles(id)
	}
	addsOrdinaryDemand := committed&ordinary == 0 && roles&ordinary != 0
	if committed&needed != 0 && addsOrdinaryDemand && execution && roles&dirty == 0 {
		roles |= dirty | promoted
	}
	if role == dirty {
		roles &^= promoted
	}
	if previous == roles {
		return false, false
	}
	next.setRoles(copied, id, roles)
	newlyDirty := committed&dirty == 0 && roles&dirty != 0
	demandChanged := committed&(ordinary|readiness) != roles&(ordinary|readiness)
	needsDispatch := committed&(needed|fatal) == 0 || demandChanged || newlyDirty
	return true, execution && roles&(needed|fatal) != 0 && needsDispatch
}

func (d *Decision) setRoles(copied *[decisionBuckets]bool, id manifest.NamedResource, roles uint8) {
	bucket := maphash.Comparable(d.seed, id) % decisionBuckets
	if !copied[bucket] {
		d.buckets[bucket] = maps.Clone(d.buckets[bucket])
		if d.buckets[bucket] == nil {
			d.buckets[bucket] = map[manifest.NamedResource]uint8{}
		}
		copied[bucket] = true
	}
	d.buckets[bucket][id] = roles
}

// MatchesOrdering rejects stale authored edges after effective inputs are available.
func (d *Decision) MatchesOrdering(reads *store.ReadSet) bool {
	if !d.restricted() || !reads.OrderingAssessed() {
		return true
	}
	existing := d.ordering[reads.ID()]
	count := 0
	for target := range reads.Ordering() {
		if _, known := slices.BinarySearchFunc(existing, target, store.OrderingEdge.Compare); !known {
			return false
		}
		count++
	}
	return count == len(existing)
}

// HasOrdering avoids capturing prior inventories when no ordering edges can be withdrawn.
func (d *Decision) HasOrdering() bool { return d != nil && len(d.ordering) != 0 }

func (d *Decision) updateOrdering(
	next *Decision, update *Admission, copied *[decisionBuckets]bool,
	included []manifest.NamedResource,
) (bool, []manifest.NamedResource) {
	if !next.execution || d.unrestricted || len(update.ordering) == 0 && !d.HasOrdering() {
		return false, included
	}
	changed := next.replaceOrdering(update.ordering)
	if !changed && len(included) == 0 && len(update.Fatal) == 0 {
		return false, included
	}
	targets := next.orderingTargets()
	for _, bucket := range next.buckets {
		for id, roles := range bucket {
			want := targets[id] || roles&fatal != 0
			if want == (roles&readiness != 0) {
				continue
			}
			roles ^= readiness
			next.setRoles(copied, id, roles)
			if roles&needed != 0 {
				included = append(included, id)
			}
			changed = true
		}
	}
	return changed, included
}

func (d *Decision) replaceOrdering(changes map[manifest.NamedResource][]store.OrderingEdge) bool {
	changed := false
	for reader, targets := range changes {
		if slices.Equal(targets, d.ordering[reader]) {
			continue
		}
		if !changed {
			d.ordering = maps.Clone(d.ordering)
			if d.ordering == nil {
				d.ordering = make(map[manifest.NamedResource][]store.OrderingEdge)
			}
			changed = true
		}
		if len(targets) == 0 {
			delete(d.ordering, reader)
		} else {
			d.ordering[reader] = targets
		}
	}
	return changed
}

func (d *Decision) replaceOrderingDeclarations(changes map[manifest.NamedResource]orderingDeclaration) bool {
	changed := false
	for id := range changes {
		evidence := changes[id]
		prior := d.orderingDeclarations[id]
		if prior.source == evidence.source && prior.owner == evidence.owner &&
			prior.declarationIdentity == evidence.declarationIdentity && manifest.Equal(prior.object, evidence.object) {
			continue
		}
		if !changed {
			d.orderingDeclarations = maps.Clone(d.orderingDeclarations)
			if d.orderingDeclarations == nil {
				d.orderingDeclarations = make(map[manifest.NamedResource]orderingDeclaration)
			}
			changed = true
		}
		if evidence.object == nil {
			delete(d.orderingDeclarations, id)
		} else {
			d.orderingDeclarations[id] = evidence
		}
	}
	return changed
}

func (d *Decision) orderingTargets() map[manifest.NamedResource]bool {
	var work []manifest.NamedResource
	needed := make(map[manifest.NamedResource]bool)
	for reader := range d.ordering {
		if d.roles(reader)&(ordinary|fatal) != 0 {
			work = append(work, reader)
		}
	}
	for i := 0; i < len(work); i++ {
		for _, edge := range d.ordering[work[i]] {
			target := edge.Target
			if !needed[target] {
				needed[target] = true
				work = append(work, target)
			}
		}
	}
	return needed
}
