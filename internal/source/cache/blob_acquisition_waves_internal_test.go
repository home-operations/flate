package cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testBlobAcquisitionWavesCase(t *testing.T, backing *string,
) {
	t.Helper()

	for _, failed := range []bool{false, true} {
		t.Run(strconv.FormatBool(failed), func(t *testing.T) {
			t.Parallel()
			testBlobAcquisitionWavesCaseCase(
				t,
				testBlobAcquisitionWavesCaseCaseInputs{backing: &backing, failed: &failed},
			)
		},
		)
	}
}

type testBlobAcquisitionWavesCaseCaseInputs struct {
	backing **string
	failed  *bool
}

func testBlobAcquisitionWavesCaseCase(t *testing.T, testCaseInput testBlobAcquisitionWavesCaseCaseInputs) {
	t.Helper()

	root := ""
	if (*(*testCaseInput.backing)) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	synctest.Test(t, func(t *testing.T) {
		testBlobAcquisitionWavesCaseCaseCallback(
			t,
			testBlobAcquisitionWavesCaseCaseCallbackInputs{testCaseInput: &testCaseInput, c: &c},
		)
	},
	)
}

type testBlobAcquisitionWavesCaseCaseCallbackInputs struct {
	testCaseInput *testBlobAcquisitionWavesCaseCaseInputs
	c             **Cache
}

func testBlobAcquisitionWavesCaseCaseCallback(
	t *testing.T,
	testCaseInput testBlobAcquisitionWavesCaseCaseCallbackInputs,
) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	cause := errors.New("acquisition failure")
	produce := func(_ context.Context, w ProducerInput) (ProducerResult, error) {
		call := calls.Add(1)
		if call == 1 {
			close(entered)
			<-release
		}
		if *testCaseInput.testCaseInput.failed {
			return ProducerResult{}, cause
		}
		_, err := fmt.Fprint(w, call)
		return ProducerResult{}, err
	}
	key := BlobKey{Content: "acquisition-wave"}
	var generations [2]fs.FS
	var failures [2]error
	var wg sync.WaitGroup
	wg.Go(
		func() {
			generations[0], _, _, failures[0] = (*testCaseInput.c).Blob(t.Context(), key, &Policy{}, produce)
		},
	)
	<-entered
	wg.Go(
		func() {
			generations[1], _, _, failures[1] = (*testCaseInput.c).Blob(t.Context(), key, &Policy{}, produce)
		},
	)
	synctest.Wait()
	close(release)
	wg.Wait()
	assert.Equal(t, calls.Load(), int64(1))
	for i, generation := range generations {
		if *testCaseInput.testCaseInput.failed {
			if generation != nil || !errors.Is(failures[i], cause) {
				t.Fatalf("caller %d: generation=%v error=%v", i, generation, failures[i])
			}
			continue
		}
		assert.NoError(t, failures[i])
		body, err := fs.ReadFile(generation, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(body), "1")
	}
	generation, _, _, err := (*testCaseInput.c).Blob(t.Context(), key, &Policy{}, produce)
	assert.Equal(t, calls.Load(), int64(2))
	if *testCaseInput.testCaseInput.failed {
		assert.FatalIf(
			t,
			generation != nil || !errors.Is(err, cause),
			"later caller: generation=%v error=%v",
			generation,
			err,
		)
		return
	}
	assert.NoError(t, err)
	body, err := fs.ReadFile(generation, "content")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "2")
	body, err = fs.ReadFile(generations[0], "content")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "1")
}
