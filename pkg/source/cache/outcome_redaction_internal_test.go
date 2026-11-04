package cache

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func TestOutcome_LastGoodRedaction(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	r, _, id, err := c.Blob(
		t.Context(),
		BlobKey{Content: "last-good"},
		Policy{},
		producer(
			[]byte("held"), &ocispec.Descriptor{
				Annotations: map[string]string{
					"fixture-secret":     "fixture-secret",
					prefix + "chart-url": "https://fixture-secret",
				},
			},
		),
	)
	assert.NoError(t, err)

	last, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &last)
	for n := uint32(1); n <= 4; n++ {
		o.record.Count = n
		desc, publishOutcomeErr := c.publishOutcome(t.Context(), id, o, connReset())
		assert.NoError(t, publishOutcomeErr)

		config, data, _ := o.encode()
		assert.FatalIf(
			t,
			bytes.Contains(config, []byte("fixture-secret")) || bytes.Contains(data, []byte("fixture-secret")),
			"%s",
			"last-good annotations leaked",
		)

		o, publishOutcomeErr = c.readOutcome(t.Context(), &desc)
		assert.FatalIf(
			t,
			publishOutcomeErr != nil || o == nil || o.lastGood.Digest != last.Digest ||
				len(o.lastGood.Annotations) != 0,
			"direct payload: %+v, %v",
			o,
			publishOutcomeErr,
		)
	}
	{
		data, readFileErr := fs.ReadFile(r, "content")
		assert.FatalIf(t, readFileErr != nil || string(data) != "held", "held blob: %q, %v", data, readFileErr)
	}

	desc, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	old := time.Now().Add(-24 * time.Hour)
	{
		chtimesErr := c.root.Chtimes(blobPath(desc.Digest), old, old)
		assert.NoError(t, chtimesErr)
	}
	{
		closeErr3 := c.Close()
		assert.NoError(t, closeErr3)
	}

	res, err := Sweep(t.Context(), c.root.Name(), SweepOpts{MaxAge: time.Hour})
	assert.FatalIf(
		t,
		err != nil || len(res.Errors) != 0 || len(res.Removed) != 1 || res.Removed[0] != id,
		"retire last edge: %+v, %v",
		res,
		err,
	)
	{
		_, err := os.Stat(filepath.Join(c.root.Name(), blobPath(last.Digest)))
		assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "retired payload remains: %v", err)
	}
}
