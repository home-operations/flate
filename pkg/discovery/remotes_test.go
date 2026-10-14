package discovery

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSeedBootstrapSource_ExplicitURLsOverrideGitConfig(t *testing.T) {
	for _, identity := range []string{"ancestor", "explicit", "inferred"} {
		t.Run(identity, func(t *testing.T) {
			parent := discoveryRefFixture(t)
			testutil.WriteFile(t, parent, ".git/config", "[remote \"origin\"]\nurl = https://github.com/parent/repo.git\n")
			root := filepath.Join(parent, "child")
			testutil.WriteFile(t, root, "scan.yaml", "")
			d := &discoverer{cfg: Config{Path: root, RepoRoot: root, Store: store.New()}}
			want := "github.com/parent/repo"
			wantRoot := root
			if identity == "inferred" {
				d.cfg.RepoRoot = ""
				wantRoot = parent
			}
			if identity == "explicit" {
				assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
				d.cfg.SelfURLs = []string{"git@github.com:Owner/Repo.git", "not-a-remote"}
				want = "github.com/owner/repo"
			}
			got, err := d.seedBootstrapSource(t.Context())
			assert.Equal(t, err, nil)
			assert.Equal(t, got, wantRoot)
			assert.Diff(t, d.remotes, map[string]struct{}{want: {}})
			artifact := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
			assert.Equal(t, artifact.LocalPath, wantRoot)
			if artifact.FS == nil {
				t.Fatal("bootstrap source has no admitted filesystem")
			}
			if identity == "inferred" {
				content, err := fs.ReadFile(artifact.FS, "value.txt")
				assert.Equal(t, err, nil)
				assert.Equal(t, string(content), "dirty")
			}
		})
	}
}

func TestSeedBootstrapSource_NoRepository(t *testing.T) {
	for _, kind := range []string{"absent", "gitfile", "config", "lookup"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "scan.yaml", "admitted\n")
			d := &discoverer{cfg: Config{Path: root, Store: store.New()}}
			switch kind {
			case "gitfile":
				testutil.WriteFile(t, root, ".git", "gitdir: missing\n")
			case "config":
				_, err := git.PlainInit(root, false)
				assert.Equal(t, err, nil)
				testutil.WriteFile(t, root, ".git/config", "[invalid\n")
			case "lookup":
				assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
			}
			got, err := d.seedBootstrapSource(t.Context())
			assert.Equal(t, len(d.remotes), 0)
			assert.Equal(t, err, nil)
			assert.Equal(t, got, root)
			artifact := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
			assert.Equal(t, artifact.Kind, manifest.KindGitRepository)
			assert.Equal(t, artifact.URL, "file://"+root)
			assert.Equal(t, artifact.LocalPath, root)
			if artifact.FS == nil {
				t.Fatal("bootstrap source has no admitted filesystem")
			}
			content, err := fs.ReadFile(artifact.FS, "scan.yaml")
			assert.Equal(t, err, nil)
			assert.Equal(t, string(content), "admitted\n")
		})
	}
}

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

// TestNormalizeGitURL_StripsQueryAndFragment covers the round-6
// hardening: deploy-key URLs from cloud providers often carry
// query strings (?deploy_key=prod) or fragments, and the local
// .git/config typically stores the clean URL. Both must reduce to
// the same key so URL-match aliasing fires.
func TestNormalizeGitURL_StripsQueryAndFragment(t *testing.T) {
	want := "github.com/owner/repo"
	cases := []string{
		"https://github.com/owner/repo?deploy_key=prod",
		"https://github.com/owner/repo.git?ref=main#section",
		"https://github.com/owner/repo#section",
	}
	for _, in := range cases {
		if got := normalizeGitURL(in); got != want {
			t.Errorf("normalizeGitURL(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestNormalizeGitURL_IPv6Host covers the round-6 hardening:
// self-hosted Git instances on IPv6 (private-network setups,
// pre-DNS bootstrap clusters) use bracketed-host URLs. The
// previous string-splitting path corrupted the host; net/url
// handles bracketed IPv6 correctly.
func TestNormalizeGitURL_IPv6Host(t *testing.T) {
	got := normalizeGitURL("https://[2001:db8::1]:8080/owner/repo.git")
	want := "2001:db8::1/owner/repo"
	if got != want {
		t.Errorf("IPv6: normalizeGitURL = %q, want %q", got, want)
	}
}

// TestNormalizeGitURL_RejectsRelativeScpStyle locks the negative
// branch: scp-style strings that look like a path with a colon
// (e.g. `foo:bar`) are rejected so an accidental colon in a local
// kustomization-resource doesn't trip URL-match.
func TestNormalizeGitURL_RejectsRelativeScpStyle(t *testing.T) {
	cases := []string{
		":onlypath",
		"./has:colon",
		"a/b:c",
	}
	for _, in := range cases {
		if got := normalizeGitURL(in); got != "" {
			t.Errorf("normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}

// TestNormalizeGitURL_HostOrPathMissing rejects inputs that look
// remote-shaped but lack one of the two halves we need to build a
// key. Equally important as the positive cases — without these
// guards a degenerate URL would produce a key like "github.com/"
// that could spuriously match another degenerate entry.
func TestNormalizeGitURL_HostOrPathMissing(t *testing.T) {
	cases := []string{
		"https://github.com",
		"https://github.com/",
		"https://",
		"git@github.com:",
	}
	for _, in := range cases {
		if got := normalizeGitURL(in); got != "" {
			t.Errorf("normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}
