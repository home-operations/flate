package gittree

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFreshInputs_CurrentBytesAndMembership(t *testing.T) {
	t.Parallel()
	for _, mode := range []ViewMode{Direct, Source} {
		t.Run(map[ViewMode]string{Direct: "direct", Source: "source", RemoteBase: "remote"}[mode], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "file", "alpha")
			testutil.WriteFile(t, root, "ignored", "hidden")
			testutil.WriteFile(t, root, ".sourceignore", "ignored\n")
			testutil.WriteFile(t, root, ".gitignore", "git-hidden\ntracked-later\nrule-hidden\n")
			old := time.Unix(946684800, 0)
			assert.NoError(t, os.Chtimes(filepath.Join(root, "file"), old, old))
			testutil.GitCommitAll(
				t,
				repo,
				"fixture",
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: old}},
			)
			for _, name := range []string{"git-hidden", "tracked-later", "rule-hidden"} {
				testutil.WriteFile(t, root, name, name)
			}
			input, err := admitLocal(t, root)
			assert.NoError(t, err)
			selected, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: input.FS})
			assert.NoError(t, err)
			_, err = ContentFingerprint(t.Context(), selected)
			assert.NoError(t, err)
			before, err := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			_, err = selected.Stat("new")
			assert.Equal(t, os.IsNotExist(err), true)
			testutil.WriteFile(t, root, "file", "bravo")
			assert.NoError(t, os.Chtimes(filepath.Join(root, "file"), old, old))
			testutil.WriteFile(t, root, "new", "added")
			assert.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0o700))
			testutil.WriteFile(t, root, ".sourceignore", "")
			testutil.WriteFile(t, root, ".gitignore", "git-hidden\ntracked-later\nnoise.new\n")
			testutil.WriteFile(t, root, "noise.new", "new ignored bytes")
			worktree, err := repo.Worktree()
			assert.NoError(t, err)
			assert.NoError(t, worktree.AddWithOptions(&git.AddOptions{Path: "tracked-later", SkipStatus: true}))
			captured, err := selected.FreshInputs(
				t.Context(),
				[]string{"file", "new"},
				map[string]fs.FileMode{"file": 0},
			)
			assert.NoError(t, err)
			assert.Equal(
				t,
				captured.Files["file"],
				manifest.Fingerprint(testutil.GitBlobHash(t, []byte("bravo"))),
			)
			assert.Equal(t, captured.Files["new"], "")
			after, err := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			assert.Equal(
				t,
				after.Files["file"],
				manifest.Fingerprint(testutil.GitBlobHash(t, []byte("bravo"))),
			)
			assert.Equal(t, after.Files["file"] != before.Files["file"], true)
			assert.Equal(t, after.Files["new"] != "", true)
			assert.Equal(t, after.Files["ignored"] != "", true)
			assert.Equal(t, after.Files["git-hidden"], "")
			assert.Equal(t, after.Files["noise.new"], "")
			assert.Equal(t, after.Files["tracked-later"] != "", true)
			assert.Equal(t, after.Files["rule-hidden"] != "", true)
			assert.Equal(t, before.Files["tracked-later"], "")
			assert.Equal(t, before.Files["rule-hidden"], "")
			for _, name := range []string{"git-hidden", "tracked-later", "rule-hidden"} {
				_, statErr := selected.Stat(name)
				assert.Equal(t, os.IsNotExist(statErr), true)
			}
			assert.Equal(t, after.Types["empty"]&fs.ModeDir != 0, mode == Direct)
			assert.NoError(t, os.Remove(filepath.Join(root, "file")))
			assert.NoError(t, os.Mkdir(filepath.Join(root, "file"), 0o700))
			replaced, err := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, replaced.Files["file"], "")
			assert.Equal(t, replaced.Types["file"]&fs.ModeDir != 0, mode == Direct)
		})
	}
}

func TestFreshInputs_CapturedDirectory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	assert.NoError(t, os.Mkdir(filepath.Join(root, "directory"), 0o700))
	input, err := admitLocal(t, root)
	assert.NoError(t, err)
	observed, err := input.FS.(*FS).FreshInputs(t.Context(), nil, map[string]fs.FileMode{"directory": fs.ModeDir})
	assert.NoError(t, err)
	_, identified := observed.Files["directory"]
	assert.Equal(t, identified, false)
	assert.Equal(t, observed.Types["directory"].IsDir(), true)
}

func TestFreshInputs_ReusesObservedRegularKind(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "alpha")
	testutil.WriteFile(t, root, "unrelated", "untouched")
	input, err := admitLocal(t, root)
	assert.NoError(t, err)
	original := input.FS.(*FS)
	observed := &observedRegularView{view: original.view}
	original.view = observed
	first, err := original.FreshInputs(t.Context(), nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, observed.regularReads, 1)
	assert.Equal(t, observed.unclassifiedReads, 0)
	testutil.WriteFile(t, root, "file", "bravo")
	second, err := original.FreshInputs(t.Context(), []string{"file"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, observed.regularReads, 2)
	assert.Equal(t, observed.unclassifiedReads, 0)
	assert.Equal(t, first.Files["file"] != second.Files["file"], true)
	assert.Equal(t, second.Files["unrelated"], "")
	_, observedKind := second.Types["unrelated"]
	assert.Equal(t, observedKind, true)
	assert.Equal(t, observed.unrelatedReads, 1)
}

type observedRegularView struct {
	view

	regularReads, unclassifiedReads, unrelatedReads int
}

func (v *observedRegularView) ReadFile(name string) ([]byte, error) {
	if name == "file" {
		v.unclassifiedReads++
	}
	return v.view.ReadFile(name)
}

func (v *observedRegularView) OpenRegular(name string) (fs.File, error) {
	if name == "unrelated" {
		v.unrelatedReads++
	}
	if name == "file" {
		v.regularReads++
	}
	return v.view.OpenRegular(name)
}

func TestFreshInputs_StreamedIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	kinds := map[string]fs.FileMode{}
	want := map[string]string{}
	for _, file := range []struct {
		name string
		size int
	}{
		{name: "empty", size: 0},
		{name: "large", size: 3*32*1024 + 17},
		{name: "other", size: 5*32*1024 + 43},
	} {
		data := make([]byte, file.size)
		for i := range data {
			data[i] = byte((i*131 + i/251 + file.size) % 253)
		}
		testutil.WriteFile(t, root, file.name, string(data))
		kinds[file.name] = 0
		want[file.name] = manifest.Fingerprint(testutil.GitBlobHash(t, data))
	}
	input, err := admitLocal(t, root)
	assert.NoError(t, err)
	selected := input.FS.(*FS)
	first, err := selected.FreshInputs(t.Context(), nil, kinds)
	assert.NoError(t, err)
	assert.Diff(t, first, InputObservations{Files: want, Types: kinds})
	second, err := selected.FreshInputs(t.Context(), nil, kinds)
	assert.NoError(t, err)
	assert.Diff(t, second, first)
}

func TestFreshInputs_RegularReadFailures(t *testing.T) {
	t.Parallel()
	statErr := errors.New("stat failure")
	readErr := errors.New("read failure")
	closeErr := errors.New("close failure")
	for _, tt := range []struct {
		name                       string
		statErr, readErr, closeErr error
		cancel                     bool
		sizeDelta                  int64
		want                       []error
	}{
		{name: "cancellation", cancel: true, want: []error{context.Canceled}},
		{name: "cancellation with EOF", cancel: true, readErr: io.EOF, want: []error{context.Canceled}},
		{name: "stat and close", statErr: statErr, closeErr: closeErr, want: []error{statErr, closeErr}},
		{name: "read and close", readErr: readErr, closeErr: closeErr, want: []error{readErr, closeErr}},
		{name: "close after copy", closeErr: closeErr, want: []error{closeErr}},
		{name: "growth", sizeDelta: -1, want: []error{io.ErrUnexpectedEOF}},
		{name: "truncation and close", sizeDelta: 1, closeErr: closeErr, want: []error{io.ErrUnexpectedEOF, closeErr}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			testutil.WriteFile(t, root, "file", "regular file bytes")
			input, err := admitLocal(t, root)
			assert.NoError(t, err)
			selected := input.FS.(*FS)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var opened *identityTestFile
			opens := 0
			selected.view = &identityTestView{view: selected.view, wrap: func(file fs.File) fs.File {
				opens++
				opened = &identityTestFile{
					File:    file,
					statErr: tt.statErr, readErr: tt.readErr, closeErr: tt.closeErr, sizeDelta: tt.sizeDelta,
				}
				if tt.cancel {
					opened.afterRead = cancel
				}
				return opened
			}}
			got, err := selected.FreshInputs(ctx, []string{"file"}, map[string]fs.FileMode{"file": 0})
			for _, want := range tt.want {
				assert.ErrorIf(t, !errors.Is(err, want), "got %v, want error %v", err, want)
			}
			assert.Diff(t, got, InputObservations{})
			assert.Equal(t, opens, 1)
			assert.FatalIf(t, opened == nil, "regular file was not opened")
			assert.Equal(t, opened.closes, 1)
			assert.Equal(t, opened.reads > 0, tt.statErr == nil)
			if tt.sizeDelta != 0 {
				pathErr, ok := errors.AsType[*fs.PathError](err)
				assert.FatalIf(t, !ok, "got %v, want a path error", err)
				assert.Equal(t, pathErr.Op, "read")
				assert.Equal(t, pathErr.Path, "file")
			}
		})
	}
}

func TestFreshInputs_ConcurrentRegularReads(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	data := make([]byte, 3*32*1024+17)
	for i := range data {
		data[i] = byte((i*131 + i/251) % 253)
	}
	testutil.WriteFile(t, root, "file", string(data))
	input, err := admitLocal(t, root)
	assert.NoError(t, err)
	kinds := map[string]fs.FileMode{"file": 0}
	want := InputObservations{
		Files: map[string]string{"file": manifest.Fingerprint(testutil.GitBlobHash(t, data))},
		Types: kinds,
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	results := make(chan struct {
		observation InputObservations
		err         error
	}, 2)
	selected := input.FS.(*FS)
	selected.view = &identityTestView{view: selected.view, wrap: func(file fs.File) fs.File {
		return &identityTestFile{File: file, afterRead: func() {
			entered <- struct{}{}
			select {
			case <-release:
			case <-t.Context().Done():
			}
		}}
	}}
	for range 2 {
		go func() {
			observation, readErr := selected.FreshInputs(t.Context(), []string{"file"}, kinds)
			results <- struct {
				observation InputObservations
				err         error
			}{observation, readErr}
		}()
	}
	for range 2 {
		<-entered
	}
	for range 2 {
		release <- struct{}{}
	}
	for range 2 {
		result := <-results
		assert.NoError(t, result.err)
		assert.Diff(t, result.observation, want)
	}
}

type identityTestView struct {
	view

	wrap func(fs.File) fs.File
}

func (v *identityTestView) OpenRegular(name string) (fs.File, error) {
	file, err := v.view.OpenRegular(name)
	if err != nil {
		return nil, err
	}
	return v.wrap(file), nil
}

type identityTestFile struct {
	fs.File

	statErr, readErr, closeErr error
	sizeDelta                  int64
	afterRead                  func()
	reads, closes              int
}

func (f *identityTestFile) Stat() (fs.FileInfo, error) {
	if f.statErr != nil {
		return nil, f.statErr
	}
	info, err := f.File.Stat()
	if err != nil {
		return nil, err
	}
	return identityTestInfo{FileInfo: info, size: info.Size() + f.sizeDelta}, nil
}

func (f *identityTestFile) Read(p []byte) (int, error) {
	f.reads++
	n, err := f.File.Read(p)
	if f.afterRead != nil {
		afterRead := f.afterRead
		f.afterRead = nil
		afterRead()
	}
	if f.readErr != nil {
		return n, f.readErr
	}
	return n, err
}

func (f *identityTestFile) Close() error {
	f.closes++
	return errors.Join(f.File.Close(), f.closeErr)
}

type identityTestInfo struct {
	fs.FileInfo

	size int64
}

func (f identityTestInfo) Size() int64 { return f.size }
