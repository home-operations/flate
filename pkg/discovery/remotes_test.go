package discovery

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSeedBootstrapSource_ExplicitURLsOverrideGitConfig(t *testing.T) {
	t.Parallel()
	for _, identity := range []string{"ancestor", "explicit", "inferred", "retained"} {
		t.Run(identity, func(t *testing.T) {
			t.Parallel()
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
			if identity == "retained" {
				input, _, err := baseline.AdmitDirectory(t.Context(), root, root)
				assert.Equal(t, err, nil)
				d.cfg.Store.SetArtifact(manifest.BootstrapSourceID, input)
				testutil.WriteFile(t, parent, ".git/config", "[remote \"origin\"]\nurl = https://github.com/replaced/repo.git\n")
			}
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.Equal(t, err, nil)
			assert.Equal(t, d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).LocalPath, wantRoot)
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
	t.Parallel()
	for _, kind := range []string{"absent", "gitfile", "config", "lookup"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "scan.yaml", "admitted\n")
			d := &discoverer{cfg: Config{Path: root, Store: store.New()}}
			switch kind {
			case "gitfile":
				testutil.WriteFile(t, root, ".git", "gitdir: missing\n")
			case "config":
				testutil.GitInit(t, root)
				testutil.WriteFile(t, root, ".git/config", "[invalid\n")
			case "lookup":
				assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
			}
			_, err := d.resolveEntrySources(t.Context(), false)
			assert.Equal(t, len(d.remotes), 0)
			assert.Equal(t, err, nil)
			assert.Equal(t, d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact).LocalPath, root)
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
		if got := normalizeGitURL(in); got != want {
			t.Errorf("normalizeGitURL(%q) = %q, want %q", in, got, want)
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
		if got := normalizeGitURL(in); got != "" {
			t.Errorf("normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}

func TestNormalizeGitURL_TrailingSlashAndDotGit(t *testing.T) {
	t.Parallel()
	a := normalizeGitURL("https://github.com/owner/repo.git/")
	b := normalizeGitURL("https://github.com/owner/repo")
	if a != b {
		t.Errorf("trailing .git/slash should normalize identically: %q vs %q", a, b)
	}
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
		if got := normalizeGitURL(in); got != want {
			t.Errorf("normalizeGitURL(%q) = %q, want %q", in, got, want)
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
	if got != want {
		t.Errorf("IPv6: normalizeGitURL = %q, want %q", got, want)
	}
}

func TestNormalizeGitURL_RejectsRelativeScpStyle(t *testing.T) {
	t.Parallel()
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
		if got := normalizeGitURL(in); got != "" {
			t.Errorf("normalizeGitURL(%q) should be rejected; got %q", in, got)
		}
	}
}
