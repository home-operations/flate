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
)

// NewFilter constructs empty restricted execution membership.
func NewFilter() *Filter { return &Filter{} }

// Enabled reports whether restricted execution membership is active.
func (f *Filter) Enabled() bool { return f != nil }

// Membership reads an identity's needed, selected and dirty roles with their revision.
func (f *Filter) Membership(id manifest.NamedResource) (bool, bool, bool, uint64) {
	if f == nil {
		return true, true, true, 0
	}
	f.mu.RLock()
	roles, revision := f.keep[id], f.revision
	f.mu.RUnlock()
	return roles&needed != 0, roles&selected != 0, roles&dirty != 0, revision
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

// AddEmitted commits prepared roles if revision still applies, returning newly needed identities and retry.
// Callers MUST perform capture validation, inclusion arrivals and publication outside this lock.
func (f *Filter) AddEmitted(needs, selects, dirties []manifest.NamedResource, revision uint64) ([]manifest.NamedResource, bool) {
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
	for axis, ids := range [3][]manifest.NamedResource{needs, selects, dirties} {
		for _, id := range ids {
			if id == (manifest.NamedResource{}) {
				continue
			}
			previous := f.keep[id]
			roles := previous | needed | 1<<axis
			if roles == previous {
				continue
			}
			if f.keep == nil {
				f.keep = make(map[manifest.NamedResource]uint8, max(len(needs), len(selects), len(dirties)))
			}
			f.keep[id] = roles
			changed = true
			if previous&needed == 0 {
				if added == nil {
					added = make([]manifest.NamedResource, 0, max(len(needs), len(selects), len(dirties)))
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
