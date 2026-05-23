package source

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// TestCache_ResetSerializesAgainstSlot exercises the mutex Cache.Reset
// acquires alongside Cache.Slot. A goroutine race-detector run with
// many parallel Slot/Reset pairs targeting the same path must complete
// without -race tripping. A regression that drops the lock from Reset
// (or removes it from Slot) would fail under `go test -race`.
func TestCache_ResetSerializesAgainstSlot(t *testing.T) {
	c := NewCache(t.TempDir())
	const goroutines = 16
	const iterations = 32
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				path, _, release, err := c.Slot("https://shared.example/repo", "main")
				if err != nil {
					t.Errorf("Slot: %v", err)
					return
				}
				_ = path
				release()
			}
		}()
		go func() {
			defer wg.Done()
			for range iterations {
				path, _, release, err := c.Slot("https://shared.example/repo", "main")
				if err != nil {
					t.Errorf("Slot: %v", err)
					return
				}
				if err := c.Reset(path); err != nil {
					t.Errorf("Reset: %v", err)
					release()
					return
				}
				release()
			}
		}()
	}
	wg.Wait()
}

// TestCache_SlotSerializesSameKey: two goroutines competing for the
// same (url, ref) must execute their critical sections serially — the
// second caller's exists=true observation must follow the first
// caller's writes, not race them. Reproduces the PR-137 cross-CR slot
// collision the previous Cache mutex (which guarded only allocation)
// allowed.
func TestCache_SlotSerializesSameKey(t *testing.T) {
	c := NewCache(t.TempDir())
	var firstReleased, secondAcquired atomic.Bool

	// g1Entered closes when G1 has acquired the slot lock; g2Start
	// closes after G2 may begin attempting acquisition. Deterministic
	// — no sleeps — so the test pins the serialization invariant
	// without depending on scheduler timing.
	g1Entered := make(chan struct{})
	g2Start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		path, _, release, err := c.Slot("https://shared.example/repo", "main")
		if err != nil {
			t.Errorf("Slot: %v", err)
			done <- struct{}{}
			return
		}
		close(g1Entered)
		// Hold until the harness has launched G2 and confirmed it is
		// blocked on acquisition.
		<-g2Start
		_ = os.WriteFile(filepath.Join(path, ".inprogress"), []byte("x"), 0o600)
		firstReleased.Store(true)
		release()
		done <- struct{}{}
	}()
	<-g1Entered // G1 holds the lock.
	go func() {
		_, exists, release, err := c.Slot("https://shared.example/repo", "main")
		if err != nil {
			t.Errorf("Slot: %v", err)
			done <- struct{}{}
			return
		}
		// G1 must have released by the time we got the lock.
		if !firstReleased.Load() {
			t.Errorf("G2 acquired before G1 released — serialization failed")
		}
		secondAcquired.Store(true)
		if !exists {
			t.Errorf("expected exists=true on second acquisition after G1 wrote a file")
		}
		release()
		done <- struct{}{}
	}()
	// G2 has been launched and is now blocking on the slot lock. Tell
	// G1 to finish and release.
	close(g2Start)
	<-done
	<-done
	if !secondAcquired.Load() {
		t.Errorf("G2 never acquired the slot")
	}
}

func TestSlugifyRepo(t *testing.T) {
	cases := map[string]string{
		"https://github.com/owner/cluster.git":                "cluster",
		"git@github.com:owner/cluster.git":                    "cluster",
		"https://example.com/long-path/with/slashes/repo.git": "repo",
		"oci://ghcr.io/stefanprodan/charts/podinfo":           "podinfo",
		"": "repo",
	}
	for in, want := range cases {
		if got := slugifyRepo(in); got != want {
			t.Errorf("slugifyRepo(%q) = %q want %q", in, got, want)
		}
	}
}
