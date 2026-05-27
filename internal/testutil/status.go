package testutil

import (
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
