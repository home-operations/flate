package gittree

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func testNewEntryPolicyIdentity(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	current := fstest.MapFS{"file": {Data: []byte("dirty")}}
	location := &gitrepo.Location{ContentRoot: "root", Repository: repo}
	opts := ViewOptions{
		Content:  struct{ Root string }{"root"},
		Location: location,
		Mode:     Direct,
		Current:  current,
		Changes:  []string{"file"},
	}
	input, identity, err := New(t.Context(), backing, commit, opts)
	assert.NoError(t, err)
	assert.FatalIf(t, identity == "", "%s", "committed baseline with current routing has no identity")

	fingerprint, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, fingerprint, identity)
	assert.Equal(t, input.Location() == location, true)
	refreshed, _, err := operation(t, input, Direct)
	assert.NoError(t, err)
	assert.Equal(t, refreshed.Location() == location, true)
	data, err := fs.ReadFile(input, "file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "malicious")
	for _, tc := range []struct {
		name   string
		change func(*ViewOptions)
		equal  bool
	}{
		{"empty_sparse", func(o *ViewOptions) { o.Sparse = []string{} }, true},
		{"routing", func(o *ViewOptions) {
			o.Current = fstest.MapFS{"file": {Data: []byte("other")}}
			o.Changes = []string{"file", "absent"}
		}, true},
		{"coordinates", func(o *ViewOptions) { o.Content = struct{ Root string }{"other"} }, false},
		{"mode", func(o *ViewOptions) { o.Mode = Source }, false},
		{"sparse", func(o *ViewOptions) { o.Sparse = []string{"file"} }, false},
		{"ignore", func(o *ViewOptions) { o.Ignore = "absent\n" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := opts
			tc.change(&changed)
			_, got, newErr := New(t.Context(), backing, commit, changed)
			assert.NoError(t, newErr)
			if got == "" || (got == identity) != tc.equal {
				t.Fatalf("identity = %q, base = %q; equal=%t", got, identity, tc.equal)
			}
		})
	}
	other := craftedCommit(t, repo, object.TreeEntry{Name: "other", Mode: filemode.Regular})
	_, changed, err := New(t.Context(), backing, other, opts)
	assert.NoError(t, err)
	assert.FatalIf(t, changed == identity, "%s", "commit change reused an admission identity")

	for _, mode := range []ViewMode{Direct, Source} {
		pinned, pin, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: mode, Current: current, CurrentContent: "acquired"},
		)
		assert.NoError(t, err)
		assert.FatalIf(t, pin == "", "mode %d lost the wrapped acquisition identity", mode)
		_, retained, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: pinned})
		assert.NoError(t, err)
		assert.Equal(t, retained, pin)
		_, next, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: mode, Current: current, CurrentContent: "replacement"},
		)
		assert.NoError(t, err)
		assert.FatalIf(t, next == pin, "%s", "wrapped generation change reused its identity")
	}
	for _, mode := range []ViewMode{Direct, Source} {
		mutable, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: current})
		assert.NoError(t, err)
		assert.FatalIf(t, key != "", "mutable mode %d has identity %q", mode, key)
		_, key, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: mutable})
		assert.NoError(t, err)
		assert.FatalIf(t, key != "", "refreshed mutable mode %d has identity %q", mode, key)
	}
}

func testNewEntryPolicyRetainedIdentity(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	identity := "source-git-acquired@" + commit.String()
	input, pin, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Direct, Content: identity},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin == "", false)
	originalPin := pin
	held, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, pin, originalPin)
	assert.Equal(t, held == input, true)
	_, pin, err = New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Direct},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin == "", false)
}
