package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

// The trailing allowance is exactly maxTrailingBytes; the producer returns right after its last write,
// so only the consumer's own check can reject the extra byte.
func TestAcquire_TrailingBoundary(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, extra := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/%d", backing, extra), func(t *testing.T) {
				t.Parallel()
				root := ""
				if backing == "disk" {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				_, _, _, err := c.Acquire(
					t.Context(),
					ArchiveKey{Content: "source-trailing-boundary"},
					Policy{MaxAge: 0},
					func(_ context.Context, w ProducerInput) (ProducerResult, error) {
						var payloadTar bytes.Buffer
						testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
						if _, err := w.Write(payloadTar.Bytes()); err != nil {
							return ProducerResult{}, err
						}
						_, err := w.Write(make([]byte, maxTrailingBytes+extra))
						return ProducerResult{}, err
					},
				)
				assert.FatalIf(t, (err != nil) != (extra > 0), "extra=%d: err=%v", extra, err)
			})
		}
	}
}

func TestAcquire_TrailingFailureKeepsCacheIdentity(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testAcquireTrailingFailureKeepsCacheIdentityCase(
				t,
				&backing,
			)
		},
		)
	}
}

func testAcquireTrailingFailureKeepsCacheIdentityCase(
	t *testing.T, backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	var payloadTar bytes.Buffer
	testutil.WriteTar(t, &payloadTar, map[string]string{"file": "x"}, 0o644)
	data := payloadTar.Bytes()
	trailingWriteFailed := false
	_, _, _, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "source-trailing-failure"},
		Policy{MaxAge: 0},
		func(_ context.Context, w ProducerInput) (ProducerResult, error) {
			if _, err := w.Write(data); err != nil {
				return ProducerResult{}, err
			}
			trailing := make([]byte, 32<<10)
			for {
				if _, err := w.Write(trailing); err != nil {
					trailingWriteFailed = true
					return ProducerResult{}, err
				}
			}
		},
	)
	assert.FatalIf(t, !trailingWriteFailed, "%s", "trailing write did not fail")

	{
		_, ok := errors.AsType[*CacheError](err)
		assert.FatalIf(t, !ok, "%T %v, want *CacheError", err, err)
	}
}
