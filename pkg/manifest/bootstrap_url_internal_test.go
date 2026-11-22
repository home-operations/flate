package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestNormalizeGitURL_CanonicalForms(t *testing.T) {
	t.Parallel()
	want := "github.com/owner/repo"
	cases := []string{
		"ssh://git@github.com/Owner/Repo.git",
		"git@github.com:owner/repo.git",
		"https://github.com/owner/repo",
		"https://user:pass@github.com/owner/repo.git",
		"https://github.com:443/owner/repo",
	}
	for _, in := range cases {
		{
			got := normalizeGitURL(in)
			assert.ErrorIf(t, got != want, "normalizeGitURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeGitURL_RejectsNonRemoteShapes(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		"file:///tmp/repo",
		"/tmp/repo",
		"./repo",
	}
	for _, in := range cases {
		{
			got := normalizeGitURL(in)
			assert.ErrorIf(t, got != "", "normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}

func TestNormalizeGitURL_TrailingSlashAndDotGit(t *testing.T) {
	t.Parallel()
	a := normalizeGitURL("https://github.com/owner/repo.git/")
	b := normalizeGitURL("https://github.com/owner/repo")
	assert.ErrorIf(t, a != b, "trailing .git/slash should normalize identically: %q vs %q", a, b)
}

func TestNormalizeGitURL_StripsQueryAndFragment(t *testing.T) {
	t.Parallel()
	want := "github.com/owner/repo"
	cases := []string{
		"https://github.com/owner/repo?deploy_key=prod",
		"https://github.com/owner/repo.git?ref=main#section",
		"https://github.com/owner/repo#section",
	}
	for _, in := range cases {
		{
			got := normalizeGitURL(in)
			assert.ErrorIf(t, got != want, "normalizeGitURL(%q) = %q, want %q", in, got, want)
		}
	}
	for _, tc := range []struct{ input, want string }{
		{"git@example.com:owner/repo?literal#name", "example.com/owner/repo?literal#name"},
		{"https://example.com/owner/repo%3Fliteral%23name?query#fragment", "example.com/owner/repo?literal#name"},
		{"ssh://git@[2001:db8::1]:2222/owner/repo%3Fname?query#fragment", "2001:db8::1/owner/repo?name"},
	} {
		assert.Equal(t, normalizeGitURL(tc.input), tc.want)
	}
}

func TestNormalizeGitURL_IPv6Host(t *testing.T) {
	t.Parallel()
	got := normalizeGitURL("https://[2001:db8::1]:8080/owner/repo.git")
	want := "2001:db8::1/owner/repo"
	assert.ErrorIf(t, got != want, "IPv6: normalizeGitURL = %q, want %q", got, want)
}

func TestNormalizeGitURL_RejectsRelativeScpStyle(t *testing.T) {
	t.Parallel()
	cases := []string{
		":onlypath",
		"./has:colon",
		"a/b:c",
	}
	for _, in := range cases {
		{
			got := normalizeGitURL(in)
			assert.ErrorIf(t, got != "", "normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}

func TestNormalizeGitURL_HostOrPathMissing(t *testing.T) {
	t.Parallel()
	cases := []string{
		"https://github.com",
		"https://github.com/",
		"https://",
		"https:///owner/repo",
		"ssh:///owner/repo",
		"git@bad\\host:owner/repo",
		"git@github.com:",
	}
	for _, in := range cases {
		{
			got := normalizeGitURL(in)
			assert.ErrorIf(t, got != "", "normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}
