package discovery

import "testing"

// TestNormalizeGitURL_CanonicalForms exercises every URL shape the
// bootstrap-alias URL-match heuristic needs to recognize as the same
// remote. All four inputs below describe the same GitHub repository
// in different syntaxes; they must collapse to one key so a file-
// loaded GitRepository.spec.url written in any of them matches the
// working tree's `.git/config` regardless of style.
func TestNormalizeGitURL_CanonicalForms(t *testing.T) {
	want := "github.com/owner/repo"
	cases := []string{
		"ssh://git@github.com/Owner/Repo.git",
		"git@github.com:owner/repo.git",
		"https://github.com/owner/repo",
		"https://user:pass@github.com/owner/repo.git",
		"https://github.com:443/owner/repo",
	}
	for _, in := range cases {
		if got := normalizeGitURL(in); got != want {
			t.Errorf("normalizeGitURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestNormalizeGitURL_RejectsNonRemoteShapes locks the negative
// contract: file:// URLs and local paths are not remote sources and
// must not participate in URL-match aliasing — otherwise a working
// tree containing its own file://-aliased GitRepository would alias
// against itself recursively.
func TestNormalizeGitURL_RejectsNonRemoteShapes(t *testing.T) {
	cases := []string{
		"",
		"file:///tmp/repo",
		"/tmp/repo",
		"./repo",
	}
	for _, in := range cases {
		if got := normalizeGitURL(in); got != "" {
			t.Errorf("normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}

// TestNormalizeGitURL_TrailingSlashAndDotGit_NormalizeIdentically
// guards the cosmetic-differences vector. A GitRepository.spec.url
// might be written with or without trailing `.git` or `/`; the local
// `.git/config` typically omits the trailing slash but keeps `.git`.
// Both must reduce to the same key.
func TestNormalizeGitURL_TrailingSlashAndDotGit(t *testing.T) {
	a := normalizeGitURL("https://github.com/owner/repo.git/")
	b := normalizeGitURL("https://github.com/owner/repo")
	if a != b {
		t.Errorf("trailing .git/slash should normalize identically: %q vs %q", a, b)
	}
}
