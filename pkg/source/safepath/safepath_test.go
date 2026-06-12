package safepath_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/source/safepath"
)

// TestSafeJoin_RejectAbsolute exercises SafeJoin with rejectAbsolute=true,
// mirroring the OCI tar-extraction caller. Absolute and Windows-style
// volume paths in the entry name are treated as a sign of a malicious
// archive and must be rejected before any join is attempted.
func TestSafeJoin_RejectAbsolute(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	cases := []struct {
		name      string
		rel       string
		wantError bool
	}{
		// --- happy paths ---
		{name: "plain filename", rel: "Chart.yaml", wantError: false},
		{name: "nested relative", rel: "templates/cm.yaml", wantError: false},
		{name: "clean traversal stays inside", rel: "foo/../bar.txt", wantError: false},
		{name: "current dir prefix", rel: "./subdir/file.yaml", wantError: false},

		// --- relative traversal ---
		{name: "single dotdot escape", rel: "../escape.txt", wantError: true},
		{name: "deep dotdot escape", rel: "../../../etc/passwd", wantError: true},
		{name: "interior dotdot reaches outside", rel: "foo/../../escape", wantError: true},
		{name: "sneaky nullbyte-like deep climb", rel: "a/b/../../../etc/passwd", wantError: true},

		// --- absolute paths (rejected by rejectAbsolute=true) ---
		{name: "absolute path", rel: "/etc/passwd", wantError: true},
		{name: "absolute deep path", rel: "/var/run/secrets/token", wantError: true},
		{name: "absolute root only", rel: "/", wantError: true},

		// --- double-slash / unusual separators ---
		{name: "double slash prefix", rel: "//etc/passwd", wantError: true},
		{name: "double slash interior ok", rel: "foo//bar.yaml", wantError: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := safepath.SafeJoin(base, tc.rel, true)
			if tc.wantError {
				if err == nil {
					t.Errorf("SafeJoin(%q, rejectAbsolute=true) = %q, nil; want escape error", tc.rel, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SafeJoin(%q, rejectAbsolute=true): unexpected error: %v", tc.rel, err)
			}
			// Result must be strictly inside base.
			assertUnderBase(t, base, got, tc.rel)
		})
	}
}

// TestSafeJoin_AllowAbsolute exercises SafeJoin with rejectAbsolute=false,
// mirroring the bucket-key caller. Absolute-looking keys (e.g. "/etc/passwd")
// are contained safely by filepath.Join and must NOT trigger an error;
// traversal via dotdot must still be rejected.
func TestSafeJoin_AllowAbsolute(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	cases := []struct {
		name      string
		rel       string
		wantError bool
	}{
		// --- happy paths ---
		{name: "normal nested key", rel: "dir/sub/file.yaml", wantError: false},
		{name: "plain filename", rel: "file.yaml", wantError: false},
		{name: "interior dotdot stays inside", rel: "a/../b/file.yaml", wantError: false},
		// Absolute-looking keys must be ALLOWED (bucket semantics).
		{name: "absolute-looking key stays under base", rel: "/etc/passwd", wantError: false},
		{name: "absolute root key", rel: "/", wantError: false},

		// --- relative traversal (still rejected) ---
		{name: "single dotdot escape", rel: "../etc/passwd", wantError: true},
		{name: "deep dotdot escape", rel: "../../../../../../etc/passwd", wantError: true},
		{name: "interior dotdot reaches outside", rel: "a/../../etc/passwd", wantError: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := safepath.SafeJoin(base, tc.rel, false)
			if tc.wantError {
				if err == nil {
					t.Errorf("SafeJoin(%q, rejectAbsolute=false) = %q, nil; want traversal error", tc.rel, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SafeJoin(%q, rejectAbsolute=false): unexpected error: %v", tc.rel, err)
			}
			assertUnderBase(t, base, got, tc.rel)
		})
	}
}

// TestSafeJoin_ErrorMessages checks that error strings contain useful
// context (the offending path) for security-audit logs.
func TestSafeJoin_ErrorMessages(t *testing.T) {
	t.Parallel()
	base := t.TempDir()

	for _, rel := range []string{"../escape", "/etc/passwd"} {
		_, err := safepath.SafeJoin(base, rel, true)
		if err == nil {
			t.Errorf("expected error for %q", rel)
			continue
		}
		if !strings.Contains(err.Error(), rel) {
			t.Errorf("error %q does not contain offending path %q", err.Error(), rel)
		}
	}

	_, err := safepath.SafeJoin(base, "../escape", false)
	if err == nil {
		t.Error("expected error for ../escape with rejectAbsolute=false")
	}
}

// TestSafeJoin_ForwardSlashNormalization ensures that forward-slash keys
// from cross-platform sources (e.g. S3 on Windows) are normalised before
// validation, consistent with the bucket source's filepath.FromSlash call.
func TestSafeJoin_ForwardSlashNormalization(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	rel := "a/b/c.yaml" // always forward slashes

	got, err := safepath.SafeJoin(base, rel, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := filepath.Join(base, "a", "b", "c.yaml")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestContains exercises the pure containment check the kustomize remote-base
// copy uses to confine a symlink's resolved target to the base root. Both args
// are absolute, symlink-resolved paths; only the algebraic check is tested.
func TestContains(t *testing.T) {
	t.Parallel()
	root := filepath.Join(string(filepath.Separator), "base", "root")
	cases := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "file directly inside", target: filepath.Join(root, "a.yaml"), want: true},
		{name: "nested inside", target: filepath.Join(root, "sub", "a.yaml"), want: true},
		{name: "root itself", target: root, want: true},
		{name: "parent escape", target: filepath.Join(root, "..", "secret"), want: false},
		{name: "absolute outside", target: filepath.Join(string(filepath.Separator), "etc", "passwd"), want: false},
		// /base/rootx must NOT count as inside /base/root (prefix-boundary case).
		{name: "sibling prefix not contained", target: root + "x" + string(filepath.Separator) + "a.yaml", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := safepath.Contains(root, tc.target); got != tc.want {
				t.Errorf("Contains(%q, %q) = %v; want %v", root, tc.target, got, tc.want)
			}
		})
	}
}

// assertUnderBase verifies that path is strictly inside base (or equal to base).
func assertUnderBase(t *testing.T, base, path, rel string) {
	t.Helper()
	cleanBase := filepath.Clean(base) + string(filepath.Separator)
	cleanPath := filepath.Clean(path)
	if cleanPath != filepath.Clean(base) && !strings.HasPrefix(cleanPath+string(filepath.Separator), cleanBase) {
		t.Errorf("SafeJoin(%q): result %q is not under base %q", rel, path, base)
	}
}
