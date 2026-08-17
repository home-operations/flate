package mirror

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"

	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func TestCache_OpenOrFetchHonorsFilesystemLock(t *testing.T) {
	cases := []struct {
		name        string
		sharedURL   bool
		wantBlocked bool
	}{
		{"same URL", true, true},
		{"different URL", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			layout := cacheroot.New(t.TempDir())
			lockedURL := "file://" + filepath.Join(t.TempDir(), "locked")
			path := layout.GitMirror(urlHash(lockedURL))
			if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
				t.Fatal(err)
			}

			held := flock.New(path+".lock", flock.SetPermissions(0o600))
			if err := held.Lock(); err != nil {
				t.Fatalf("Lock: %v", err)
			}
			t.Cleanup(func() { _ = held.Close() })

			url := "file://" + filepath.Join(t.TempDir(), "missing")
			if tc.sharedURL {
				url = lockedURL
			}
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			defer cancel()
			_, err := New(layout).OpenOrFetch(ctx, url, nil, nil, FetchPlan{})
			if err == nil {
				t.Fatal("OpenOrFetch unexpectedly succeeded for a missing repository")
			}
			blocked := errors.Is(err, context.DeadlineExceeded)
			if blocked != tc.wantBlocked {
				t.Fatalf("OpenOrFetch error = %v, blocked = %t, want %t", err, blocked, tc.wantBlocked)
			}
		})
	}
}
