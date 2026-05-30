package testutil

import (
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// NewStoreWithStatuses returns a fresh Store with each id pre-set to
// the given status (empty message), collapsing the store.New() +
// per-id UpdateStatus boilerplate common to dependency and render
// tests that need a pre-populated store.
func NewStoreWithStatuses(statuses map[manifest.NamedResource]store.Status) *store.Store {
	s := store.New()
	for id, st := range statuses {
		s.UpdateStatus(id, st, "")
	}
	return s
}

// WaitForStatus polls st until id reaches want status or a 2-second
// deadline expires. It is the shared polling helper used by controller
// tests (helmrelease, kustomization, source) to avoid identical inline
// copies in each package.
func WaitForStatus(t *testing.T, st *store.Store, id manifest.NamedResource, want store.Status) store.StatusInfo {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, ok := st.GetStatus(id); ok && info.Status == want {
			return info
		}
		time.Sleep(5 * time.Millisecond)
	}
	info, _ := st.GetStatus(id)
	t.Fatalf("status %v not reached within deadline; last=%+v", want, info)
	return info
}
