package discovery

import (
	"errors"
	"io/fs"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func testAliasBootstrapSourcesDeclaredRefsCaseTail(
	t *testing.T,
	tailCaseInput *testAliasBootstrapSourcesDeclaredRefsCaseTailInputs,
) bool {
	t.Helper()
	artifact := (*tailCaseInput.st).GetArtifact((*tailCaseInput.repository).Named(), store.ArtifactContent).(*store.SourceArtifact)
	if (*tailCaseInput.testCaseInput).base != "" {
		for range 2 {
			content, innerInnerInnerInnerInnerLocalErr := fs.ReadFile(artifact.FS, "apps/value.txt")
			assert.FatalIf(
				t,
				innerInnerInnerInnerInnerLocalErr != nil || string(content) != "B",
				"pinned HEAD content = %q, %v; want B",
				content,
				innerInnerInnerInnerInnerLocalErr,
			)
		}
		assert.FatalIf(
			t,
			artifact.Revision != (*tailCaseInput.revision) || (artifact.Key == "") != (*tailCaseInput.testCaseInput).head ||
				artifact.LocalRoot != (*tailCaseInput.side) ||
				artifact.LocalPath == (*tailCaseInput.side) ||
				artifact.InputAlias != (*tailCaseInput.testCaseInput).head ||
				tailCaseInput.d.
					hasPins == (*tailCaseInput.testCaseInput).head,
			"pinned HEAD identity/classification = %+v, pins=%t aliases=%v",
			artifact,
			tailCaseInput.d.
				hasPins,
			(*tailCaseInput.aliased),
		)
		if !(*tailCaseInput.testCaseInput).head && artifact.URL != (*tailCaseInput.repository).URL {
			t.Fatalf("committed URL = %q, want %q", artifact.URL, (*tailCaseInput.repository).URL)
		}
		status, ok := (*tailCaseInput.st).GetStatus((*tailCaseInput.repository).Named())
		assert.FatalIf(
			t,
			!ok || status.Status != store.StatusReady,
			"pinned HEAD status = %v, %t",
			status,
			ok,
		)
		for _, name := range []string{"apps/ignored.txt", "apps/excluded.txt"} {
			{
				_, innerInnerInnerInnerInnerInnerLocalErr := fs.Stat(artifact.FS, name)
				assert.FatalIf(
					t,
					!errors.Is(innerInnerInnerInnerInnerInnerLocalErr, fs.ErrNotExist),
					"source retained excluded %s: %v",
					name,
					innerInnerInnerInnerInnerInnerLocalErr,
				)
			}
			{
				content, innerInnerInnerInnerInnerInnerInnerLocalErr := fs.ReadFile(
					(*tailCaseInput.bootstrap).FS,
					name,
				)
				assert.FatalIf(
					t,
					innerInnerInnerInnerInnerInnerInnerLocalErr != nil || len(content) == 0,
					"source shaping changed direct bootstrap %s: %q, %v",
					name,
					content,
					innerInnerInnerInnerInnerInnerInnerLocalErr,
				)
			}
		}
	}
	want := "A"
	switch {
	case (*tailCaseInput.testCaseInput).base != "":
		want = "B"
	case (*tailCaseInput.testCaseInput).head:
		want = "dirty"
		assert.FatalIf(
			t,
			artifact.LocalRoot != (*tailCaseInput.root) || artifact.LocalPath == (*tailCaseInput.root) || artifact.FS == nil ||
				artifact.Key != "",
			"mutable alias did not retain its admitted origin: %+v",
			artifact,
		)
	default:
		assert.FatalIf(
			t,
			artifact.FS == nil || artifact.Key == "" || artifact.URL != (*tailCaseInput.repository).URL,
			"committed artifact did not use supplied cache: %+v",
			artifact,
		)
	}
	assert.FatalIf(
		t,
		artifact.FS == nil ||
			(*tailCaseInput.testCaseInput).head && (*tailCaseInput.testCaseInput).base == "" && artifact.Key != "",
		"%v",
		"local admission violated the FS/Key state contract",
	)
	content, err := fs.ReadFile(artifact.FS, "value.txt")
	if (*tailCaseInput.testCaseInput).sparse {
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrNotExist),
			"sparse source retained excluded file: %q, %v",
			content,
			err,
		)
		data, innerInnerInnerInnerInnerInnerInnerInnerLocalErr := fs.ReadFile(artifact.FS, "apps/value.txt")
		assert.NoError(t, innerInnerInnerInnerInnerInnerInnerInnerLocalErr)
		assert.Equal(t, string(data), want)
		assert.Diff(t, (*tailCaseInput.repository).SparseCheckout, []string{"./apps"})
		assert.Equal(t, len((*tailCaseInput.st).Warnings()), 0)
		return true
	}
	assert.FatalIf(
		t,
		err != nil || string(content) != want,
		"source content = %q, %v; want %s",
		content,
		err,
		want,
	)
	{
		warnings := (*tailCaseInput.st).Warnings()
		assert.FatalIf(t, len(warnings) != 0, "materialization/HEAD unexpectedly warns: %+v", warnings)
	}
	return false
}

type testAliasBootstrapSourcesDeclaredRefsCaseTailInputs struct {
	st            **store.Store
	repository    **manifest.GitRepository
	testCaseInput **struct {
		name       string
		base       string
		commit     bool
		ref        *sourcev1.GitRepositoryRef
		head       bool
		fallback   bool
		fail       bool
		noStore    bool
		sparse     bool
		submodules bool
		lookupErr  bool
	}
	revision  *string
	side      *string
	d         *Session
	aliased   *int
	bootstrap **store.SourceArtifact
	root      *string
}
