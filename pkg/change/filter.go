package change

import (
	"log/slog"
	"slices"
	"sync"

	"github.com/home-operations/flate/pkg/manifest"
)

// Filter owns invocation-monotone execution membership; nil admits every identity.
// A nonnil zero value is restricted and initially empty.
type Filter struct {
	mu       sync.RWMutex
	keep     map[manifest.NamedResource]uint8
	revision uint64
}

const (
	needed uint8 = 1 << iota
	selected
	dirty
	ordinary
)

// NewFilter constructs empty restricted execution membership.
func NewFilter() *Filter { return &Filter{} }

// Enabled reports whether restricted execution membership is active.
func (f *Filter) Enabled() bool { return f != nil }

// Membership captures execution roles at one filter revision.
type Membership struct {
	Needed, Selected, Dirty bool
	Revision                uint64
}

// Membership reads an identity's needed, selected and dirty roles with their revision.
func (f *Filter) Membership(id manifest.NamedResource) Membership {
	if f == nil {
		return Membership{Needed: true, Selected: true, Dirty: true, Revision: 0}
	}
	f.mu.RLock()
	roles, revision := f.keep[id], f.revision
	f.mu.RUnlock()
	return Membership{
		Needed:   roles&needed != 0,
		Selected: roles&selected != 0,
		Dirty:    roles&dirty != 0,
		Revision: revision,
	}
}

// ShouldReconcile reports whether an identity is needed for this invocation.
func (f *Filter) ShouldReconcile(id manifest.NamedResource) bool {
	if f == nil {
		return true
	}
	f.mu.RLock()
	roles := f.keep[id]
	f.mu.RUnlock()
	return roles&needed != 0
}

// MembershipUpdate keeps ordinary demand distinct from qualification-only demand.
type MembershipUpdate struct {
	Needed, Selected, Dirty, Ordinary []manifest.NamedResource
}

// OrdinaryDemand reports purpose after the monotone membership transaction.
func (f *Filter) OrdinaryDemand(id manifest.NamedResource) bool {
	if f == nil {
		return true
	}
	f.mu.RLock()
	roles := f.keep[id]
	f.mu.RUnlock()
	return roles&ordinary != 0
}

// AddEmitted MUST return existing identities when they acquire ordinary demand.
// Callers MUST perform capture validation, inclusion arrivals and publication outside this lock.
//
//nolint:gocritic // The membership transaction consumes a stable value snapshot.
func (f *Filter) AddEmitted(
	update MembershipUpdate,
	revision uint64,
) ([]manifest.NamedResource, bool) {
	if f == nil {
		return nil, false
	}
	f.mu.Lock()
	if f.revision != revision {
		f.mu.Unlock()
		return nil, true
	}
	var added []manifest.NamedResource
	changed := false
	capacity := max(len(update.Needed), len(update.Selected), len(update.Dirty), len(update.Ordinary))
	for _, axis := range [4]struct {
		ids  []manifest.NamedResource
		role uint8
	}{
		{update.Ordinary, ordinary},
		{update.Selected, selected},
		{update.Needed, needed},
		{update.Dirty, dirty},
	} {
		for _, id := range axis.ids {
			roleChange := f.addRoles(id, axis.role, capacity)
			if !roleChange.updated {
				continue
			}
			changed = true
			if roleChange.needed {
				if added == nil {
					added = make([]manifest.NamedResource, 0, capacity)
				}
				added = append(added, id)
			}
		}
	}
	if changed {
		f.revision++
	}
	f.mu.Unlock()
	slices.SortFunc(added, manifest.NamedResource.Compare)
	return added, false
}

type roleUpdate struct{ needed, updated bool }

// f.mu MUST be held by the caller.
func (f *Filter) addRoles(id manifest.NamedResource, role uint8, capacity int) roleUpdate {
	if id == (manifest.NamedResource{}) {
		return roleUpdate{}
	}
	previous := f.keep[id]
	roles := previous | needed | role
	if role == selected {
		roles |= ordinary
	}
	// Qualification settlement MUST NOT satisfy a newly required ordinary body.
	if previous&needed != 0 && previous&ordinary == 0 && roles&ordinary != 0 {
		roles |= dirty
	}
	if roles == previous {
		return roleUpdate{}
	}
	if f.keep == nil {
		f.keep = make(map[manifest.NamedResource]uint8, capacity)
	}
	f.keep[id] = roles
	return roleUpdate{needed: previous&needed == 0 || roles&ordinary != previous&ordinary, updated: true}
}

// LogValue snapshots membership when the log handler requests it.
func (f *Filter) LogValue() slog.Value {
	items := f.KeepNames()
	return slog.GroupValue(slog.Int("size", len(items)), slog.Any("items", items))
}

// KeepNames returns sorted identities needed by this invocation.
func (f *Filter) KeepNames() []string {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	out := make([]string, 0, len(f.keep))
	for id, roles := range f.keep {
		if roles&needed != 0 {
			out = append(out, id.String())
		}
	}
	f.mu.RUnlock()
	slices.Sort(out)
	return out
}

// KeepNamespaces returns namespaces represented by selected output identities.
func (f *Filter) KeepNamespaces() map[string]struct{} {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	var out map[string]struct{}
	for id, roles := range f.keep {
		if roles&selected != 0 && !id.ReportHidden() && id.Namespace != "" {
			if out == nil {
				out = map[string]struct{}{}
			}
			out[id.Namespace] = struct{}{}
		}
	}
	f.mu.RUnlock()
	return out
}
