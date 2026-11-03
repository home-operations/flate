package gittree

import (
	"errors"
	"io/fs"
	"slices"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/store"
)

func testNewEntryPolicySparseShapingAndRefreshCase(
	t *testing.T,
	testCaseInput testNewEntryPolicySparseShapingAndRefreshCaseInputs,
) {
	t.Helper()

	admitted, err := admitLocal(t, (*testCaseInput.root))
	assert.NoError(t, err)
	before := slices.Clone(testCaseInput.tc.sparse)
	input, _, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{
			Mode:    Source,
			Current: admitted.FS,
			Sparse:  testCaseInput.tc.sparse,
			Ignore:  testCaseInput.tc.ignore,
		},
	)
	assert.NoError(t, err)
	want := make(map[string]string, len(testCaseInput.tc.want))
	for _, name := range testCaseInput.tc.want {
		want[name] = (*testCaseInput.files)[name]
	}
	var previous map[string]string
	for run := range 2 {
		if run > 0 {
			input, _, err = operation(t, input, Direct)
			assert.NoError(t, err)
		}
		got := map[string]string{}
		err = fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(input, name)
			got[name] = string(data)
			return err
		})
		assert.NoError(t, err)
		assert.Diff(t, got, want)
		assert.Diff(t, testCaseInput.tc.sparse, before)
		if run > 0 {
			assert.Diff(t, got, previous)
		}
		previous = got
	}
}

type testNewEntryPolicyRejectedLinksCaseInputs struct {
	root    *string
	commit  *plumbing.Hash
	backing *billy.Filesystem
	local   **store.SourceArtifact
	mode    *ViewMode
}

func testNewEntryPolicyRejectedLinksCase(t *testing.T, testCaseInput testNewEntryPolicyRejectedLinksCaseInputs) {
	t.Helper()

	committed, _, err := New(
		t.Context(),
		(*testCaseInput.backing),
		(*testCaseInput.commit),
		ViewOptions{Mode: (*testCaseInput.mode)},
	)
	assert.NoError(t, err)
	mutable, _, err := New(
		t.Context(), (*testCaseInput.backing), plumbing.ZeroHash,
		ViewOptions{Mode: (*testCaseInput.mode), Worktree: osfs.New((*testCaseInput.root))},
	)
	assert.NoError(t, err)
	mounted, _, err := operation(t, (*testCaseInput.local).FS, (*testCaseInput.mode))
	assert.NoError(t, err)
	for _, input := range []struct {
		name string
		fs   *FS
	}{{"committed", committed}, {"mutable", mutable}, {"mounted", mounted}} {
		refreshed, _, err := operation(t, input.fs, Source)
		assert.NoError(t, err)
		for _, view := range []struct {
			name string
			fs   *FS
		}{{"initial", input.fs}, {"refreshed", refreshed}} {
			reader := view.fs
			entries, err := fs.ReadDir(reader, ".")
			assert.NoError(t, err)
			for _, entry := range entries {
				assert.FatalIf(t, entry.Name() == "link", "%s", "listing exposes rejected link")
			}
			for _, name := range []string{"link", "link/child", "link/nested/child"} {
				t.Run(input.name+"/"+view.name+"/"+name, func(t *testing.T) {
					t.Parallel()
					testNewEntryPolicyRejectedLinksCaseCase(
						t,
						testNewEntryPolicyRejectedLinksCaseCaseInputs{reader: &reader, name: &name},
					)
				},
				)
			}
			_, err = fs.ReadFile(reader, "absent")
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			if reader == refreshed || (*testCaseInput.mode) == Source {
				_, err = fs.ReadFile(reader, "ignored")
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
			data, err := fs.ReadFile(reader, "regular")
			assert.NoError(t, err)
			assert.Equal(t, string(data), "content")
		}
	}
}

type testNewEntryPolicyRejectedLinksCaseCaseInputs struct {
	reader **FS
	name   *string
}

func testNewEntryPolicyRejectedLinksCaseCase(
	t *testing.T,
	testCaseInput testNewEntryPolicyRejectedLinksCaseCaseInputs,
) {
	t.Helper()

	data, readFileErr2 := fs.ReadFile((*testCaseInput.reader), (*testCaseInput.name))
	assert.Equal(t, len(data), 0)
	assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
	_, readFileErr2 = fs.Stat((*testCaseInput.reader), (*testCaseInput.name))
	assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
	target, readFileErr2 := fs.ReadLink((*testCaseInput.reader), (*testCaseInput.name))
	assert.Equal(t, target, "")
	assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
}
