package discovery

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/baseline"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSeedBootstrapSource_ExplicitURLsOverrideGitConfig(t *testing.T) {
	t.Parallel()
	for _, identity := range []string{"ancestor", "explicit", "inferred", "retained"} {
		t.Run(identity, func(t *testing.T) {
			t.Parallel()
			testSeedBootstrapSourceExplicitURLsOverrideGitConfigCase(
				t,
				&identity,
			)
		},
		)
	}
}

func TestSeedBootstrapSource_NoRepository(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"absent", "gitfile", "config", "lookup"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "scan.yaml", "admitted\n")
			input, _, err := baseline.AdmitDirectory(t.Context(), root, "", manifest.WorkScope{})
			assert.NoError(t, err)
			d := &Session{cfg: Config{Path: root, Input: input, Store: store.New()}}
			switch kind {
			case "gitfile":
				testutil.WriteFile(t, root, ".git", "gitdir: missing\n")
			case "config":
				testutil.GitInit(t, root)
				testutil.WriteFile(t, root, ".git/config", "[invalid\n")
			case "lookup":
				assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
			}
			_, err = d.resolveTestSources(t.Context(), false)
			assert.Equal(t, len(d.remotes), 0)
			assert.Equal(t, err, nil)
			assert.Equal(
				t,
				d.cfg.Store.GetArtifact(
					manifest.BootstrapSourceID,
					store.ArtifactContent,
				).(*store.SourceArtifact).LocalPath,
				root,
			)
			artifact := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, artifact.Kind, manifest.KindGitRepository)
			assert.Equal(t, artifact.URL, "file://"+root)
			assert.Equal(t, artifact.LocalPath, root)
			assert.FatalIf(t, artifact.FS == nil, "%v", "bootstrap source has no admitted filesystem")
			content, err := fs.ReadFile(artifact.FS, "scan.yaml")
			assert.Equal(t, err, nil)
			assert.Equal(t, string(content), "admitted\n")
		})
	}
}

func testSeedBootstrapSourceExplicitURLsOverrideGitConfigCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	parent := discoveryRefFixture(t)
	testutil.WriteFile(
		t,
		parent,
		".git/config",
		"[remote \"origin\"]\nurl = https://github.com/parent/repo.git\n",
	)
	root := filepath.Join(parent, "child")
	testutil.WriteFile(t, root, "scan.yaml", "")
	d := &Session{cfg: Config{Path: root, RepoRoot: root, Store: store.New()}}
	want := "github.com/parent/repo"
	wantRoot := root
	if (*testCaseInput) == "inferred" {
		d.cfg.RepoRoot = ""
		wantRoot = parent
	}
	if (*testCaseInput) == "explicit" {
		assert.Equal(t, os.Symlink(".git", filepath.Join(root, ".git")), nil)
		d.cfg.SelfURLs = []string{"git@github.com:Owner/Repo.git", "not-a-remote"}
		want = "github.com/owner/repo"
	}
	if (*testCaseInput) == "retained" {
		input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
		assert.Equal(t, err, nil)
		d.cfg.Store.SetArtifact(manifest.BootstrapSourceID, input)
		testutil.WriteFile(
			t,
			parent,
			".git/config",
			"[remote \"origin\"]\nurl = https://github.com/replaced/repo.git\n",
		)
	}
	_, err := d.resolveTestSources(t.Context(), false)
	assert.Equal(t, err, nil)
	assert.Equal(
		t,
		d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).LocalPath,
		wantRoot,
	)
	eligible := func(url string) bool {
		source := manifest.NewGitRepository(sourcev1.GitRepository{Spec: sourcev1.GitRepositorySpec{URL: url}})
		return manifest.BootstrapSourceEligible(source, false, d.remotes)
	}
	assert.Equal(t, eligible("https://"+want), true)
	switch *testCaseInput {
	case "explicit":
		assert.Equal(t, eligible("https://github.com/parent/repo"), false)
		assert.Equal(t, eligible("not-a-remote"), false)
	case "retained":
		assert.Equal(t, eligible("https://github.com/replaced/repo"), false)
	}
	artifact := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, artifact.LocalPath, wantRoot)
	assert.FatalIf(t, artifact.FS == nil, "%v", "bootstrap source has no admitted filesystem")
	if (*testCaseInput) == "inferred" {
		content, err := fs.ReadFile(artifact.FS, "value.txt")
		assert.Equal(t, err, nil)
		assert.Equal(t, string(content), "dirty")
	}
}
