package source

import (
	"sync"
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

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				path, _, err := c.Slot("https://shared.example/repo", "main")
				if err != nil {
					t.Errorf("Slot: %v", err)
					return
				}
				_ = path
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				path, _, err := c.Slot("https://shared.example/repo", "main")
				if err != nil {
					t.Errorf("Slot: %v", err)
					return
				}
				if err := c.Reset(path); err != nil {
					t.Errorf("Reset: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
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
