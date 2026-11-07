package selection

import (
	"hash/maphash"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	needed uint8 = 1 << iota
	selected
	dirty
	ordinary
	promoted
)

const decisionBuckets = 64

// Decision MUST remain immutable after publication, including every borrowed bucket.
type Decision struct {
	seed      maphash.Seed
	buckets   [decisionBuckets]map[manifest.NamedResource]uint8
	revision  uint64
	execution bool
}

func (d *Decision) roles(id manifest.NamedResource) uint8 {
	if d == nil {
		return needed | selected | dirty | ordinary
	}
	return d.buckets[maphash.Comparable(d.seed, id)%decisionBuckets][id]
}

// Reconcile admits every identity when the decision is nil.
func (d *Decision) Reconcile(id manifest.NamedResource) bool {
	return d.roles(id)&needed != 0
}

// Output includes every identity when the decision is nil.
func (d *Decision) Output(id manifest.NamedResource) bool {
	return d.roles(id)&selected != 0
}

// Changed treats unrestricted execution as changed.
func (d *Decision) Changed(id manifest.NamedResource) bool {
	return d.roles(id)&dirty != 0
}

// OrdinaryDemand permits ordinary execution when the decision is nil.
func (d *Decision) OrdinaryDemand(id manifest.NamedResource) bool {
	return d.roles(id)&ordinary != 0
}

// Ordering preserves missing external dependency validation without admitting excluded inventory.
func (d *Decision) Ordering(id manifest.NamedResource, present, indexed bool) bool {
	return d == nil || d.Reconcile(id) || !present && !indexed
}

func (d *Decision) namespaces() map[string]struct{} {
	if d == nil {
		return nil
	}
	var result map[string]struct{}
	for _, bucket := range d.buckets {
		for id, roles := range bucket {
			if roles&selected == 0 || id.ReportHidden() || id.Namespace == "" {
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
	nextExecution := d.execution || execution
	changed := d.execution != nextExecution
	for _, axis := range [4]struct {
		ids  []manifest.NamedResource
		role uint8
	}{
		{update.Ordinary, ordinary},
		{update.Needed, needed},
		{update.Selected, selected},
		{update.Dirty, dirty},
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
	if !changed {
		return d, nil
	}
	next.execution, next.revision = nextExecution, d.revision+1
	slices.SortFunc(included, manifest.NamedResource.Compare)
	return &next, slices.Compact(included)
}

//nolint:revive,gocritic // Copy-on-write publication MUST retain both snapshots, copy ownership, and separate change/admission flags.
func (d *Decision) includeRole(
	next *Decision, copied *[decisionBuckets]bool, id manifest.NamedResource, role uint8, execution bool,
) (bool, bool) {
	bucket := maphash.Comparable(d.seed, id) % decisionBuckets
	previous := next.buckets[bucket][id]
	roles := previous | needed | role
	if role == selected {
		roles |= ordinary
	}
	var committed uint8
	if d.execution {
		committed = d.roles(id)
	}
	if committed&needed != 0 && committed&ordinary == 0 && roles&ordinary != 0 && execution && roles&dirty == 0 {
		roles |= dirty | promoted
	}
	if role == dirty {
		roles &^= promoted
	}
	if previous == roles {
		return false, false
	}
	if !copied[bucket] {
		next.buckets[bucket] = maps.Clone(next.buckets[bucket])
		if next.buckets[bucket] == nil {
			next.buckets[bucket] = map[manifest.NamedResource]uint8{}
		}
		copied[bucket] = true
	}
	next.buckets[bucket][id] = roles
	return true, execution &&
		(committed&needed == 0 || committed&ordinary != roles&ordinary || committed&dirty == 0 && roles&dirty != 0)
}
