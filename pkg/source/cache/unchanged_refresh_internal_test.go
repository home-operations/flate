package cache

import (
	"context"
	"errors"
	"io/fs"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireUnchangedRefreshRenewsCase(t *testing.T, scenario *string,
) {
	t.Helper()

	c := cachetest.New(t, "", New)
	key := ArchiveKey{Content: (*scenario)}
	if (*scenario) != "missing previous" {
		_, _, _, err := c.Acquire(
			t.Context(),
			key,
			Policy{},
			producer(orderedArchive(t, "file", "same"), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
	}
	input, _, id, err := c.Acquire(
		t.Context(),
		key,
		Policy{},
		func(callbackArg1 context.Context, w ProducerInput) (ProducerResult, error) {
			return testAcquireUnchangedRefreshRenewsCaseCallback(
				callbackArg1,
				w,
				testAcquireUnchangedRefreshRenewsCaseCallbackInputs{t: &t, scenario: &scenario},
			)
		},
	)
	want := manifest.ErrInput
	if (*scenario) == "producer error" {
		want = fs.ErrPermission
	}
	if input != nil || id != "" || !errors.Is(err, want) || !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("invalid unchanged result: filesystem=%v identity=%q error=%v", input != nil, id, err)
	}
}

type testAcquireUnchangedRefreshRenewsCaseCallbackInputs struct {
	t        **testing.T
	scenario **string
}

func testAcquireUnchangedRefreshRenewsCaseCallback(
	_ context.Context,
	w ProducerInput,
	testCaseInput testAcquireUnchangedRefreshRenewsCaseCallbackInputs,
) (ProducerResult, error) {
	if (*(*testCaseInput.scenario)) == "producer error" {
		return ProducerResult{Unchanged: true}, fs.ErrPermission
	}
	if (*(*testCaseInput.scenario)) == "wrote bytes" {
		_, err := w.Write(orderedArchive((*testCaseInput.t), "file", "new"))
		if err != nil {
			return ProducerResult{}, err
		}
	}
	return ProducerResult{Unchanged: true}, nil
}
