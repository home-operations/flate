package cache

import (
	"context"
	"errors"
	"math"
	"net/http"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestOutcome_AcquisitionValidity(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"future", "rollback", "missing", "corrupt", "saturation"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			testOutcomeAcquisitionValidityCase(t, &scenario)
		},
		)
	}
}

func TestOutcome_ClassifiedAcquisitionReplay(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "classified-cooldown"}
	calls := 0
	produce := func(context.Context, ProducerInput) (ProducerResult, error) {
		calls++
		return ProducerResult{}, manifest.NewError(
			manifest.ErrFlux,
			&fetcherr.ResponseError{StatusCode: 503, Op: http.MethodGet},
			"",
		)
	}
	_, _, _, live := c.Blob(t.Context(), key, &Policy{}, produce)
	assert.FatalIf(t, live == nil || live.Error() != "HTTP 503" || calls != 1, "live: %v calls=%d", live, calls)
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	c = cachetest.New(t, root, New)
	_, _, _, replay := c.Blob(t.Context(), key, &Policy{}, produce)
	assert.FatalIf(
		t,
		replay == nil || replay.Error() != live.Error() || calls != 1 || !errors.Is(replay, manifest.ErrFlux),
		"replay: %v calls=%d",
		replay,
		calls,
	)
	{
		terminal, ok := errors.AsType[*TerminalError](replay)
		assert.FatalIf(t, !ok || terminal.Attempted, "%s", "replay retried production")
	}
	{
		response, ok := errors.AsType[*fetcherr.ResponseError](
			replay,
		)
		assert.FatalIf(t, !ok || response.StatusCode != http.StatusServiceUnavailable ||
			response.Op != http.MethodGet, "%s", "lost typed response")
	}
}

func testOutcomeAcquisitionValidityCase(t *testing.T, scenario *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testOutcomeAcquisitionValidityCaseCallback(
			t,
			testOutcomeAcquisitionValidityCaseCallbackInputs{scenario: &scenario},
		)
	},
	)
}

type testOutcomeAcquisitionValidityCaseCallbackInputs struct {
	scenario **string
}

func testOutcomeAcquisitionValidityCaseCallback(
	t *testing.T,
	testCaseInput testOutcomeAcquisitionValidityCaseCallbackInputs,
) {
	t.Helper()
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	o := newOutcome(outcomeFailure(connReset()), time.Now().UTC(), 1, &ocispec.Descriptor{})
	switch *(*testCaseInput.scenario) {
	case "future":
		o.record.Completed = time.Now().UTC().Add(time.Hour)
	case "rollback":
		o.observed = time.Now().UTC().Add(time.Second)
	case "saturation":
		o.record.Completed = time.Now().UTC().Add(-15 * time.Minute)
		o.record.Count = math.MaxUint32
	}
	id := manifest.Fingerprint(struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"validity"}})
	desc, err := c.publishOutcome(t.Context(), id, o, o.replay())
	assert.NoError(t, err)

	switch *(*testCaseInput.scenario) {
	case "rollback":
		c.entries.Store(id, &entry{stored: desc, outcome: o})
	case "missing":
		removeErr2 := c.root.Remove(blobPath(desc.Digest))
		assert.NoError(t, removeErr2)

	case "corrupt":
		{
			chmodErr3 := c.root.Chmod(blobPath(desc.Digest), 0o600)
			assert.NoError(t, chmodErr3)
		}
		{
			writeFileErr5 := c.root.WriteFile(
				blobPath(desc.Digest),
				[]byte("invalid"),
				0o600,
			)
			assert.NoError(t, writeFileErr5)
		}
	}
	if (*(*testCaseInput.scenario)) == "future" || (*(*testCaseInput.scenario)) == "saturation" {
		{
			closeErr4 := c.Close()
			assert.NoError(t, closeErr4)
		}

		c = cachetest.New(t, root, New)
	}
	calls := 0
	_, _, _, err = c.Blob(
		t.Context(),
		BlobKey{Content: "validity"},
		&Policy{MaxAge: Forever, Retry: fastRetry(2)},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			calls++
			return ProducerResult{}, connReset()
		},
	)
	if (*(*testCaseInput.scenario)) == "missing" || (*(*testCaseInput.scenario)) == "corrupt" {
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(
				t,
				!ok || calls != 0,
				"broken persistence concealed: calls=%d, error=%v",
				calls,
				err,
			)
		}
		return
	}
	wantCalls, wantCount := 2, uint32(1)
	if (*(*testCaseInput.scenario)) == "saturation" {
		wantCalls, wantCount = 1, math.MaxUint32
	}
	assert.FatalIf(
		t,
		calls != wantCalls || !errors.Is(err, syscall.ECONNRESET),
		"validity calls=%d, want=%d, error=%v",
		calls,
		wantCalls,
		err,
	)
	desc, err = c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	next, err := c.readOutcome(t.Context(), &desc)
	assert.FatalIf(t, err != nil || next == nil || next.record.Count != wantCount ||
		!next.record.Completed.Equal(time.Now()), "validity outcome=%+v, error=%v", next, err)
}
