package gittree

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/storer"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func testGitViewSparseBodyReadsCase(t *testing.T, size *struct {
	name  string
	files int
	bytes int64
},
) {
	t.Helper()

	if size.name == "exact" && os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip(
			"MEM-SPARSE not-run: env FLATE_E8_SLOW=1 go test ./pkg/source/gittree -run '^TestGitView_SparseBodyReads/exact$' -count=1 -v",
		)
	}
	fixture := testutil.NewSparseGit(t, size.files, size.bytes)
	reads := &testutil.GitReads{LargeHash: fixture.Large}
	g := &gitView{
		commit: fixture.Base,
		mode:   Source,
		nodes:  map[string]*gitNode{".": {name: ".", mode: fs.ModeDir | 0o755}},
	}
	g.newStorer = func() storer.EncodedObjectStorer {
		return &testutil.CountingStorer{Storer: fixture.Repo.Storer, Reads: reads}
	}
	input := &FS{view: g, snapshot: &localPolicy{}}
	commit, err := fixture.Repo.CommitObject(fixture.Base)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("app/input.yaml")
	assert.NoError(t, err)
	reader, err := g.openBlob(entry.Hash)
	assert.NoError(t, err)
	data, err := io.ReadAll(reader)
	assert.NoError(t, errors.Join(err, reader.Close()))
	assert.FatalIf(
		t,
		!strings.Contains(string(data), "before") || reads.Bodies.Load() != 1 || reads.Large.Load() != 0,
		"%s",
		"pool blob Reader calibration failed",
	)

	reads.Reset()
	warnings, err := g.walk(t.Context(), fixture.Base)
	assert.NoError(t, err)
	assert.Equal(t, len(warnings), 0)
	assert.FatalIf(t, reads.Metadata.Load() == 0 || reads.Bodies.Load() != 0, "%s", "walk opened a blob Reader")

	t.Logf(
		"walk metadata=%d bodies=%d large=%d",
		reads.Metadata.Load(),
		reads.Bodies.Load(),
		reads.Large.Load(),
	)
	reads.Reset()
	assert.NoError(t, g.shape(t.Context(), new(ViewOptions{Mode: Source}), input))
	count := 0
	assert.NoError(t, fs.WalkDir(input, ".", func(_ string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			count++
		}
		return err
	}))
	assert.Equal(t, count, size.files)
	info, err := fs.Stat(input, "unrelated/large.txt")
	assert.NoError(t, err)
	assert.Equal(t, info.Size(), size.bytes)
	assert.Equal(t, reads.Bodies.Load(), int64(0))
	t.Logf(
		"shape/list/stat metadata=%d bodies=%d large=%d",
		reads.Metadata.Load(),
		reads.Bodies.Load(),
		reads.Large.Load(),
	)
	reads.Reset()
	data, err = fs.ReadFile(input, "app/input.yaml")
	assert.NoError(t, err)
	assert.FatalIf(t, !strings.Contains(string(data), "before"), "base payload=%s", data)
	assert.Equal(t, reads.Bodies.Load(), int64(1))
	assert.Equal(t, reads.Large.Load(), int64(0))
	t.Logf(
		"consumed read metadata=%d bodies=%d large=%d",
		reads.Metadata.Load(),
		reads.Bodies.Load(),
		reads.Large.Load(),
	)
}
