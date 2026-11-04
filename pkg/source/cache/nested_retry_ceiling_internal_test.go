package cache

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

type testAcquireNestedTerminalDueCeilingCaseInputs struct {
	callers *int
	child   *string
}

func testAcquireNestedTerminalDueCeilingCase(
	t *testing.T,
	testCaseInput testAcquireNestedTerminalDueCeilingCaseInputs,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireNestedTerminalDueCeilingCaseCallback(
			t,
			testAcquireNestedTerminalDueCeilingCaseCallbackInputs{testCaseInput: &testCaseInput},
		)
	},
	)
}

type testAcquireNestedTerminalDueCeilingCaseCallbackInputs struct {
	testCaseInput *testAcquireNestedTerminalDueCeilingCaseInputs
}

func testAcquireNestedTerminalDueCeilingCaseCallback(
	t *testing.T,
	testCaseInput testAcquireNestedTerminalDueCeilingCaseCallbackInputs,
) {
	t.Helper()
	c := cachetest.New(t, t.TempDir(), New)
	fail := func(context.Context, ProducerInput) (ProducerResult, error) {
		return ProducerResult{}, connReset()
	}
	_, _, outerID, err := c.Blob(
		t.Context(),
		BlobKey{Content: "outer"},
		Policy{},
		producer(nil, &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	for _, id := range []string{"outer", "inner"} {
		if id == "inner" && (*testCaseInput.testCaseInput.child) != "due" {
			continue
		}
		if _, _, _, blobErr := c.Blob(
			t.Context(),
			BlobKey{Content: id},
			Policy{},
			fail,
		); !errors.Is(
			blobErr,
			syscall.ECONNRESET,
		) {
			t.Fatal(blobErr)
		}
	}
	time.Sleep(750 * time.Millisecond)
	if (*testCaseInput.testCaseInput.child) == "fresh" {
		if _, _, _, blobErr2 := c.Blob(
			t.Context(),
			BlobKey{Content: "inner"},
			Policy{},
			fail,
		); !errors.Is(
			blobErr2,
			syscall.ECONNRESET,
		) {
			t.Fatal(blobErr2)
		}
	}
	before, err := c.store.Resolve(t.Context(), outerID)
	assert.NoError(t, err)

	unlock, err := c.locks.Acquire(t.Context(), outerID)
	assert.NoError(t, err)

	done := make(chan error, (*testCaseInput.testCaseInput.callers))
	outer, inner := 0, 0
	for range *testCaseInput.testCaseInput.callers {
		go func() {
			_, _, _, blobErr3 := c.Blob(
				t.Context(),
				BlobKey{Content: "outer"},
				Policy{Retry: fastRetry(5)},
				func(ctx context.Context, _ ProducerInput) (ProducerResult, error) {
					outer++
					_, _, _, blobErr4 := c.Blob(
						ctx,
						BlobKey{Content: "inner"},
						Policy{Retry: fastRetry(7)},
						func(context.Context, ProducerInput) (ProducerResult, error) {
							inner++
							return ProducerResult{}, connReset()
						},
					)
					return ProducerResult{}, blobErr4
				},
			)
			done <- blobErr3
		}()
	}
	synctest.Wait()
	unlock()
	for range *testCaseInput.testCaseInput.callers {
		innerErr := <-done
		terminal, ok := errors.AsType[*TerminalError](innerErr)
		assert.FatalIf(
			t,
			!ok || !errors.Is(innerErr, syscall.ECONNRESET) ||
				(*testCaseInput.testCaseInput.child) == "fresh" && terminal.Attempted,
			"nested error=%v",
			innerErr,
		)
	}
	wantInner := 1
	if (*testCaseInput.testCaseInput.child) == "fresh" {
		wantInner = 0
	}
	assert.FatalIf(t, outer != 1 || inner != wantInner, "producer calls=%d/%d", outer, inner)
	after, err := c.store.Resolve(t.Context(), outerID)
	assert.NoError(t, err)

	o, err := c.readOutcome(t.Context(), &after)
	wantCount := uint32(2)
	if (*testCaseInput.testCaseInput.child) == "fresh" {
		wantCount = 1
		assert.FatalIf(t, before.Digest != after.Digest, "%s", "cached child renewed outer outcome")
	}
	assert.FatalIf(
		t,
		err != nil || o == nil || o.record.Count != wantCount,
		"outer outcome=%+v, error=%v",
		o,
		err,
	)
}
