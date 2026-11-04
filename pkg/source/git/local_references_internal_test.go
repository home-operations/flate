package git

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func testResolveLocalReferencesCase(t *testing.T, root *string,
	b *plumbing.Hash,
	tc *struct {
		name string
		ref  sourcev1.GitRepositoryRef
		want plumbing.Hash
		fail bool
	},
) {
	t.Helper()

	repository := localSource(&tc.ref)

	artifacts, err := ResolveLocal(
		t.Context(),
		localLocation(t, (*root)),
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
	if tc.fail {
		assert.FatalIf(t, !errors.Is(err, manifest.ErrFlux), "expected domain error, got %v", err)
		return
	}
	assert.NoError(t, err)

	artifact := artifacts[repository.Named()]
	if tc.want.IsZero() {
		assert.FatalIf(t, artifact != nil, "lookup miss installed an artifact: %+v", artifact)
		return
	}
	if artifact == nil || artifact.Revision != tc.want.String() || artifact.URL != repository.URL {
		t.Fatalf("artifact = %+v, want revision %s", artifact, tc.want)
	}
	assert.FatalIf(
		t,
		artifact.FS == nil || (artifact.Key == "") != (tc.want == (*b)),
		"FS/Key state did not distinguish HEAD from committed content: %+v",
		artifact,
	)
	view, pin, err := source.OperationView(t.Context(), artifact.FS)
	assert.NoError(t, err)
	assert.Equal(t, artifact.Content, pin)
	if artifact.Key == "" {
		assert.Equal(t, pin, "")
	} else {
		assert.Equal(t, pin, artifact.Key)
	}
	data, err := fs.ReadFile(view, "value.txt")
	assert.NoError(t, err)
	wantBody := "A"
	if tc.want == (*b) {
		wantBody = "dirty"
	}
	assert.Equal(t, string(data), wantBody)
	assert.FatalIf(t, artifact.ContentFingerprint == nil, "%s", "local source content callback is absent")

	fingerprint, err := artifact.ContentFingerprint(t.Context(), artifact.FS)
	assert.NoError(t, err)
	assert.Equal(t, fingerprint != "", true)
	assert.Equal(
		t,
		artifact.FS.(*gittree.FS).FileDigests()["value.txt"],
		manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(wantBody))),
	)

	if (artifact.LocalPath == (*root)) != (tc.want == (*b)) {
		t.Fatalf("unexpected artifact path %q", artifact.LocalPath)
	}
}

func testResolveLocalReferencesCase2(t *testing.T, metadata *string,
) {
	t.Helper()

	fixture := localFixture(t)

	metadataRoot := filepath.Join(fixture.root, ".git")
	if (*metadata) == "gitfile" {
		metadataRoot = filepath.Join(t.TempDir(), "repository")
		assert.Equal(t, os.Rename(filepath.Join(fixture.root, ".git"), metadataRoot), nil)
		testutil.WriteFile(t, fixture.root, ".git", "gitdir: "+metadataRoot+"\n")
	}
	testutil.WriteFile(
		t,
		metadataRoot,
		"config",
		"[remote \"origin\"]\nurl = https://fixture.invalid/original\n",
	)
	testutil.WriteFile(t, fixture.root, "nested/.git", "gitdir: absent\n")
	location := localLocation(t, fixture.root)
	assert.Equal(t, os.Remove(filepath.Join(metadataRoot, "config")), nil)
	artifacts, err := ResolveLocal(
		t.Context(),
		location,
		[]*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: "v2.0.0"})},
		LocalOptions{Revision: ""},
	)
	assert.Equal(t, err, nil)
	artifact := artifacts[localSource(&sourcev1.GitRepositoryRef{Tag: "v2.0.0"}).Named()]
	assert.FatalIf(
		t,
		artifact == nil || artifact.Revision != fixture.head.String(),
		"%s",
		"retained repository did not resolve HEAD",
	)

	assert.Diff(t, location.URLs, []string{"https://fixture.invalid/original"})
	for _, name := range []string{".git", ".git/config", "nested/.git"} {
		_, err := fs.Stat(artifact.FS, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		f, err := artifact.FS.Open(name)
		if f != nil {
			f.Close()
		}
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		_, err = fs.ReadFile(artifact.FS, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	for _, name := range []string{".", "nested"} {
		entries, err := fs.ReadDir(artifact.FS, name)
		assert.Equal(t, err, nil)
		for _, entry := range entries {
			assert.FatalIf(t, entry.Name() == ".git", "ReadDir(%q) exposes Git metadata", name)
		}
	}
}
