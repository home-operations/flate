package change

import (
	"context"
	"errors"
	"io/fs"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestDetect_CandidateBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                           string
		candidates                     map[string]bool
		ignored, completed, membership bool
		want                           []string
		outside                        int64
	}{
		{name: "nil", want: []string{"file", "outside"}, outside: 2},
		{name: "empty", candidates: map[string]bool{}},
		{name: "candidate", candidates: map[string]bool{"file": false}, want: []string{"file"}},
		{name: "excluded", candidates: map[string]bool{"file": false}, ignored: true},
		{name: "completed_empty", candidates: map[string]bool{}, completed: true},
		{name: "completed_candidate", candidates: map[string]bool{"file": false}, completed: true, want: []string{"file"}},
		{name: "membership_boundary", candidates: map[string]bool{".sourceignore": false}, membership: true, want: []string{".sourceignore"}},
		{name: "completed_membership_boundary", candidates: map[string]bool{".sourceignore": false}, completed: true, membership: true, want: []string{".sourceignore"}},
		{name: "completed_excluded", candidates: map[string]bool{"file": false}, completed: true, ignored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			files := [2]fstest.MapFS{
				{"file": {Data: []byte{0}}, "outside": {Data: []byte{0}}},
				{"file": {Data: []byte{1}}, "outside": {Data: []byte{1}}},
			}
			if tc.ignored {
				files[0][".sourceignore"] = &fstest.MapFile{Data: []byte("file\n")}
				files[1][".sourceignore"] = &fstest.MapFile{Data: []byte("file\n")}
				files[1]["file"].Data = []byte("different size")
			}
			if tc.membership {
				files[0][".sourceignore"] = &fstest.MapFile{Data: []byte("#a")}
				files[1][".sourceignore"] = &fstest.MapFile{Data: []byte("#b")}
				delete(files[0], "outside")
			}
			inputs, readers := candidateInputs(t.Context(), t, files, tc.completed)
			outside := readers[0].outside.Load() + readers[1].outside.Load()
			opens := readers[0].opens.Load() + readers[1].opens.Load()
			outsideStats := readers[0].outsideStats.Load() + readers[1].outsideStats.Load()
			got, err := Detect(t.Context(), inputs[0], inputs[1], DetectConfig{Candidates: tc.candidates})
			assert.NoError(t, err)
			assert.Diff(t, got.Paths(), tc.want)
			assert.Equal(t, readers[0].outside.Load()+readers[1].outside.Load()-outside, tc.outside)
			if tc.completed {
				assert.Equal(t, readers[0].opens.Load()+readers[1].opens.Load(), opens)
			}
			if tc.candidates != nil && !tc.membership {
				assert.Equal(t, readers[0].outsideStats.Load()+readers[1].outsideStats.Load(), outsideStats)
			}
		})
	}
}

func TestDetect_UncertainCandidateReadFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                      string
		size, mode, completed, canceled, oneSided bool
	}{
		{name: "size", size: true},
		{name: "mode", mode: true},
		{name: "completed", completed: true},
		{name: "canceled", size: true, canceled: true},
		{name: "completed_canceled", completed: true, canceled: true},
		{name: "mode_canceled", mode: true, canceled: true},
		{name: "one_sided", oneSided: true},
		{name: "completed_one_sided", completed: true, oneSided: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			files := [2]fstest.MapFS{
				{"file": {Data: []byte("same"), Mode: 0o644}},
				{"file": {Data: []byte("same"), Mode: 0o644}},
			}
			if tc.size {
				files[1]["file"].Data = []byte("different size")
			}
			if tc.mode {
				files[1]["file"].Mode = 0o755
			}
			if tc.oneSided {
				delete(files[0], "file")
			}
			inputs, readers := candidateInputs(ctx, t, files, tc.completed)
			readers[1].failure = fs.ErrPermission
			if tc.canceled {
				readers[1].cancel = cancel
			}
			got, err := Detect(ctx, inputs[0], inputs[1], DetectConfig{Candidates: map[string]bool{"file": true}})
			want := fs.ErrPermission
			if tc.canceled {
				want = context.Canceled
			}
			assert.Equal(t, got == nil, true)
			assert.Equal(t, errors.Is(err, want), true)
			assert.Equal(t, readers[1].failed.Load(), int64(1))
		})
	}
}

func candidateInputs(
	ctx context.Context,
	t *testing.T,
	files [2]fstest.MapFS,
	completed bool,
) ([2]*store.SourceArtifact, [2]*candidateReadFS) {
	t.Helper()
	var inputs [2]*store.SourceArtifact
	var readers [2]*candidateReadFS
	for side := range inputs {
		readers[side] = &candidateReadFS{digestReadFS: &digestReadFS{FS: files[side]}}
		source, _, err := gittree.New(ctx, nil, plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: readers[side]})
		assert.NoError(t, err)
		inputs[side] = &store.SourceArtifact{FS: source}
		if completed {
			_, err := gittree.ContentFingerprint(ctx, source)
			assert.NoError(t, err)
		}
	}
	return inputs, readers
}

type candidateReadFS struct {
	*digestReadFS

	outside, outsideStats, failed atomic.Int64
	failure                       error
	cancel                        context.CancelFunc
}

func (f *candidateReadFS) Stat(name string) (fs.FileInfo, error) {
	if name == "outside" {
		f.outsideStats.Add(1)
	}
	return f.digestReadFS.Stat(name)
}

func (f *candidateReadFS) Open(name string) (fs.File, error) {
	file, err := f.digestReadFS.Open(name)
	if name == "outside" {
		f.outside.Add(1)
	}
	if err != nil || name != "file" || f.failure == nil {
		return file, err
	}
	return &candidateFailureFile{File: file, input: f}, nil
}

type candidateFailureFile struct {
	fs.File

	input *candidateReadFS
}

func (f *candidateFailureFile) Read([]byte) (int, error) {
	f.input.failed.Add(1)
	if f.input.cancel != nil {
		f.input.cancel()
	}
	return 0, f.input.failure
}

func TestSet_FileChanged(t *testing.T) {
	t.Parallel()
	input := &store.SourceArtifact{}
	for _, tc := range []struct {
		name     string
		delta    *Set
		location FileContext
		file     string
		want     bool
	}{
		{name: "nil", file: "file"},
		{name: "unpaired", delta: NewSet([]string{"file"}), location: FileContext{Input: input, Root: "/work", Origin: "relative"}, file: "file", want: true},
		{name: "paired_without_input", delta: NewSet([]string{"file"}), location: FileContext{Paired: true, Root: "/work", Origin: "relative"}, file: "file", want: true},
		{name: "normalized", delta: NewSet([]string{"file"}), location: FileContext{Paired: true, Input: input, ToIndex: ".", Root: "/work/nested", Origin: "/work"}, file: "dir/../file", want: true},
		{name: "origin", delta: NewSet([]string{"nested/file"}), location: FileContext{Paired: true, Input: input, Root: "/work/nested", Origin: "/work"}, file: "file", want: true},
		{name: "failed_coordinates", delta: NewSet([]string{"file"}), location: FileContext{Paired: true, Input: input, Root: "/work", Origin: "relative"}, file: "file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.delta.FileChanged(tc.location, tc.file), tc.want)
		})
	}
}
