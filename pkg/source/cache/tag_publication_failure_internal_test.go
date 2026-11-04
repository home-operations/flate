package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireTagSaveFailureDoesNotBecomeHitOutcome(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	index := filepath.Join(c.root.Name(), indexPath())
	{
		err := os.Remove(index)
		assert.NoError(t, err)
	}
	{
		err := os.Mkdir(index, 0o750)
		assert.NoError(t, err)
	}

	calls := 0
	for range 2 {
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "outcome-save"},
			&Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				calls++
				return ProducerResult{}, connReset()
			},
		)
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrFlux) ||
				!errors.Is(err, syscall.ECONNRESET), "publication cause=%v", err)
		}
		v, ok := c.entries.Load(
			manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"outcome-save"}}),
		)
		assert.FatalIf(t, !ok || v.(*entry).outcome != nil, "%s", "failed publication installed a durable hit")
	}
	assert.FatalIf(t, calls != 2, "uncommitted failure reused: calls=%d", calls)
}
