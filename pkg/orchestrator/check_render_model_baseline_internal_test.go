package orchestrator

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/networktest"

	"github.com/home-operations/flate/internal/assert"

	gogit "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestIndependentModel_BaselineUnavailable(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"shallow", "missing-tree"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fixture := decodeRenderModelCase([]byte{1, 0})
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			writeRenderModelWorld(t, root, fixture.before)
			baseInput := testutil.GitCommitAll(
				t,
				repo,
				"before",
				&gogit.CommitOptions{
					Author: &object.Signature{Name: "fixture", Email: "fixture@example.test", When: time.Unix(0, 0)},
				},
			)
			selected := baseInput.String()
			if mode == "shallow" {
				{
					err := repo.Storer.SetShallow([]plumbing.Hash{baseInput})
					assert.FatalIf(t, err != nil, "%v", err)
				}
				selected = ""
			} else {
				commit, err := repo.CommitObject(baseInput)
				assert.FatalIf(t, err != nil, "%v", err)
				oid := commit.TreeHash.String()
				{
					err := os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:]))
					assert.FatalIf(t, err != nil, "%v", err)
				}
			}
			owner, err := sourcecache.New(t.Context(), "")
			assert.FatalIf(t, err != nil, "%v", err)
			t.Cleanup(func() {
				{
					localErr := owner.Close()
					assert.ErrorIf(t, localErr != nil, "%v", localErr)
				}
			})
			result, err := baseline.AutoResolve(
				t.Context(),
				baseline.Config{
					Network: networktest.New(t),
					Path:    filepath.Join(root, fixture.scan),
					Base:    selected,
					Cache:   owner,
				},
			)
			assert.FatalIf(
				t,
				!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux),
				"unavailable %s baseline accepted: result=%+v err=%v",
				mode,
				result,
				err,
			)
			assert.FatalIf(t, result != nil &&
				(result.BaseInput != nil && result.BaseInput.FS != nil || result.Source != nil && result.Source.FS != nil), "unavailable %s returned usable render inputs: %+v", mode, result)
			t.Logf("%s: baseline admission unavailable, no changed render claimed: %v", mode, err)
		})
	}
}
