package gittree

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testNewEntryPolicyShaping(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for _, name := range []string{"target", ".rootfile", ".hidden/file", "node_modules/file", "dir/file", "templates/file", "crds/file"} {
		testutil.WriteFile(t, root, name, name)
	}
	for name, target := range map[string]string{
		"alias": ".hidden/link", ".hidden/link": "../target", "escape": "../outside", "missing": "absent", "directory": "dir",
	} {
		assert.NoError(t, os.Symlink(target, filepath.Join(root, name)))
	}
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	cases := []struct {
		name  string
		opts  ViewOptions
		files []string
	}{
		{
			"source",
			ViewOptions{Mode: Source, Sparse: []string{"target", ".root"}, Ignore: "target/\n"},
			[]string{".rootfile", "target"},
		},
		{
			"source directories",
			ViewOptions{Mode: Source},
			[]string{
				".hidden/file",
				".rootfile",
				"crds/file",
				"dir/file",
				"node_modules/file",
				"target",
				"templates/file",
			},
		},
		{
			"direct",
			ViewOptions{Mode: Direct},
			[]string{
				".hidden/file",
				".rootfile",
				"crds/file",
				"dir/file",
				"node_modules/file",
				"target",
				"templates/file",
			},
		},
		{
			"remote",
			ViewOptions{Mode: RemoteBase},
			[]string{".rootfile", "alias", "crds/file", "dir/file", "target", "templates/file"},
		},
	}
	for index := range cases {
		tc := &cases[index]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyShapingCase(t, testNewEntryPolicyShapingCaseInputs{repo: &repo, commit: &commit, tc: tc})
		},
		)
	}
}

func testNewEntryPolicySparseShapingAndRefresh(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	files := map[string]string{
		".dot": "dot", ".sourceignore": "apps/drop.yaml\napps/keep.yaml\n",
		"apps/a.yaml": "a", "apps/b.yaml": "b", "apps-old/b.yaml": "old", "apps-extra/c.yaml": "extra",
		"apps/keep.yaml": "keep", "apps/drop.yaml": "drop", "infra/c/manifest.yaml": "infra",
		"target": "target", "target-old": "old-target",
	}
	for name, data := range files {
		testutil.WriteFile(t, root, name, data)
	}
	all := []string{
		".dot",
		".sourceignore",
		"apps-extra/c.yaml",
		"apps-old/b.yaml",
		"apps/a.yaml",
		"apps/b.yaml",
		"infra/c/manifest.yaml",
		"target",
		"target-old",
	}
	apps := []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml"}
	for _, tc := range []struct {
		name   string
		sparse []string
		ignore string
		want   []string
	}{
		{name: "nil", want: all},
		{name: "empty", sparse: []string{}, want: all},
		{name: "prefix", sparse: []string{"apps"}, want: apps},
		{name: "normalized prefix", sparse: []string{"./apps"}, want: apps},
		{name: "dot", sparse: []string{"."}, want: []string{".dot", ".sourceignore"}},
		{name: "missing", sparse: []string{"absent"}},
		{name: "file prefix", sparse: []string{"target"}, want: []string{"target", "target-old"}},
		{name: "normalized file prefix", sparse: []string{"./target"}, want: []string{"target", "target-old"}},
		{name: "empty normalized prefix", sparse: []string{"./"}, want: all},
		{name: "one prefix only", sparse: []string{"././apps"}},
		{name: "mixed", sparse: []string{"missing", "./apps", "./infra"}, want: []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml", "infra/c/manifest.yaml"}},
		{name: "ignore prefix", sparse: []string{"apps"}, ignore: "!apps/keep.yaml\n!infra/c/manifest.yaml\n", want: []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml", "apps/keep.yaml"}},
		{name: "ignore normalized prefix", sparse: []string{"./apps"}, ignore: "!apps/keep.yaml\n!infra/c/manifest.yaml\n", want: []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml", "apps/keep.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicySparseShapingAndRefreshCase(
				t,
				testNewEntryPolicySparseShapingAndRefreshCaseInputs{root: &root, files: &files, tc: &tc},
			)
		},
		)
	}
}

type testNewEntryPolicyShapingCaseInputs struct {
	repo   **git.Repository
	commit *plumbing.Hash
	tc     *struct {
		name string
		opts ViewOptions

		files []string
	}
}

func testNewEntryPolicyShapingCase(t *testing.T, testCaseInput testNewEntryPolicyShapingCaseInputs) {
	t.Helper()

	input, _, err := New(
		t.Context(),
		(*testCaseInput.repo).Storer.(*filesystem.Storage).Filesystem(),
		(*testCaseInput.commit),
		testCaseInput.tc.opts,
	)
	assert.NoError(t, err)
	var files []string
	assert.NoError(t, fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, name)
		}
		return nil
	}))
	assert.Diff(t, files, testCaseInput.tc.files)
	omitted := []string{"escape", "missing", "directory"}
	if testCaseInput.tc.opts.Mode == RemoteBase {
		omitted = append(omitted, ".hidden", "node_modules")
	} else {
		omitted = append(omitted, "alias", ".hidden/link")
	}
	for _, name := range omitted {
		want := fs.ErrNotExist
		if testCaseInput.tc.opts.Mode != RemoteBase {
			want = fs.ErrPermission
		}
		_, err := fs.Stat(input, name)
		assert.FatalIf(t, !errors.Is(err, want), "omitted %s: %v; want %v", name, err, want)
	}
	if testCaseInput.tc.opts.Mode == RemoteBase {
		data, err := fs.ReadFile(input, "alias")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "target")
	}
}

type testNewEntryPolicySparseShapingAndRefreshCaseInputs struct {
	root  *string
	files *map[string]string
	tc    *struct {
		name   string
		sparse []string
		ignore string
		want   []string
	}
}

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
