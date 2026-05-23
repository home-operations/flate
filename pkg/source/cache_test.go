package source

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	var seq int32
	var firstReleased, secondAcquired int32

	done := make(chan struct{}, 2)
	go func() {
		path, _, release, err := c.Slot("https://shared.example/repo", "main")
		if err != nil {
			t.Errorf("Slot: %v", err)
			done <- struct{}{}
			return
		}
		// Hold the lock briefly while a sibling fetcher tries to enter.
		atomic.AddInt32(&seq, 1)
		// Write a tiny file to simulate in-progress fetch state.
		_ = os.WriteFile(filepath.Join(path, ".inprogress"), []byte("x"), 0o600)
		atomic.StoreInt32(&firstReleased, 1)
		release()
		done <- struct{}{}
	}()
	// Give G1 a moment to take the lock.
	time.Sleep(5 * time.Millisecond)
	go func() {
		_, exists, release, err := c.Slot("https://shared.example/repo", "main")
		if err != nil {
			t.Errorf("Slot: %v", err)
			done <- struct{}{}
			return
		}
		// We acquired only after G1 released, so should see exists=true
		// (G1 wrote a marker file).
		if atomic.LoadInt32(&firstReleased) != 1 {
			t.Errorf("second goroutine acquired before first released — serialization failed")
		}
		atomic.StoreInt32(&secondAcquired, 1)
		if !exists {
			t.Errorf("expected exists=true on second acquisition after first wrote a file")
		}
		release()
		done <- struct{}{}
	}()
	<-done
	<-done
	if atomic.LoadInt32(&secondAcquired) != 1 {
		t.Errorf("second goroutine never acquired the slot")
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
