package bucket

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestSafeJoinUnderSlot locks the bucket fetcher's defense against
// malicious / mis-curated S3 keys whose name would climb out of the
// cache slot via ../ segments or absolute paths. Without this guard,
// `filepath.Join` cleans the climb-out and writeFile() lands on
// arbitrary host paths. Full unit coverage including rejectAbsolute
// semantics lives in pkg/source/safepath/safepath_test.go.
func TestSafeJoinUnderSlot(t *testing.T) {
	slot := filepath.Join(t.TempDir(), "slot")
	cases := []struct {
		name    string
		rel     string
		wantErr string
	}{
		{
			name: "normal nested key",
			rel:  "dir/sub/file.yaml",
		},
		{
			name: "plain filename",
			rel:  "file.yaml",
		},
		{
			name:    "single dotdot escape",
			rel:     "../etc/passwd",
			wantErr: "path traversal",
		},
		{
			name:    "deep dotdot escape",
			rel:     "../../../../../../etc/passwd",
			wantErr: "path traversal",
		},
		{
			name:    "interior dotdot reaches outside",
			rel:     "a/../../etc/passwd",
			wantErr: "path traversal",
		},
		{
			name: "interior dotdot stays inside",
			rel:  "a/../b/file.yaml",
		},
		{
			// An absolute key (a bucket owner naming an object "/etc/passwd")
			// is contained safely by filepath.Join, which treats the leading
			// slash as a component boundary rather than re-rooting at /. The
			// joined path stays under slot — not a traversal vector.
			name: "absolute-looking key stays under slot",
			rel:  "/etc/passwd",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := safeJoinUnderSlot(slot, tc.rel)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("rel=%q: err = %v, want substring %q", tc.rel, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("rel=%q: unexpected err: %v", tc.rel, err)
			}
			// Path must be inside slot.
			cleanSlot := filepath.Clean(slot) + string(filepath.Separator)
			if !strings.HasPrefix(filepath.Clean(got)+string(filepath.Separator), cleanSlot) &&
				filepath.Clean(got) != filepath.Clean(slot) {
				t.Errorf("rel=%q: got %q which is not under %q", tc.rel, got, slot)
			}
		})
	}
}
