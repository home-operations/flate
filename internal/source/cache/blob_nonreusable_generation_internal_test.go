package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
)

func testBlobNonReusableGenerationCase(t *testing.T, backing *string,
) {
	t.Helper()
	root := ""
	if (*backing) != "memory" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	synctest.Test(t, func(t *testing.T) {
		testBlobNonReusableGenerationCaseCallback(
			t,
			testBlobNonReusableGenerationCaseCallbackInputs{backing: &backing, root: &root, c: &c},
		)
	},
	)
}

type testBlobNonReusableGenerationCaseCallbackInputs struct {
	backing **string
	root    *string
	c       **Cache
}

func testBlobNonReusableGenerationCaseCallback(
	t *testing.T,
	testCaseInput testBlobNonReusableGenerationCaseCallbackInputs,
) {
	t.Helper()
	key := BlobKey{Content: "changing-input", Render: RenderPending}
	if (*testCaseInput.root) != "" {
		key.Render = RenderAccepted
	}
	if (*(*testCaseInput.backing)) == "disk due" {
		_, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			key,
			&Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, connReset() },
		)
		assert.FatalIf(t, !errors.Is(err, syscall.ECONNRESET), "initial failure: %v", err)
		time.Sleep(time.Second)
	}
	var reads atomic.Int64
	readInput := func() string { return strconv.FormatInt(reads.Add(1), 10) }
	entered, release := make(chan struct{}), make(chan struct{})
	var generations [2]fs.FS
	var payloads [2]string
	run := func(i int) {
		calls, validations := 0, 0
		request := key
		request.Validate = func(_ context.Context, generation fs.FS) (bool, error) {
			validations++
			data, err := fs.ReadFile(generation, "content")
			return string(data) == readInput(), err
		}
		generation, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			request,
			&Policy{MaxAge: Forever},
			func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				if calls > 1 {
					return ProducerResult{}, fmt.Errorf("produced more than once in call %d", i)
				}
				payloads[i] = readInput()
				if i == 0 {
					close(entered)
					<-release
				}
				_, err := io.WriteString(w, payloads[i])
				return ProducerResult{}, err
			},
		)
		if err != nil || calls != 1 || validations != 1 {
			t.Errorf("call %d: productions=%d validations=%d error=%v", i, calls, validations, err)
			return
		}
		generations[i] = generation
	}
	var wg sync.WaitGroup
	wg.Go(func() { run(0) })
	<-entered
	wg.Go(func() { run(1) })
	synctest.Wait()
	close(release)
	wg.Wait()
	for i, generation := range generations {
		if generation == nil {
			continue
		}
		data, err := fs.ReadFile(generation, "content")
		if err != nil || string(data) != payloads[i] {
			t.Errorf("call %d: payload=%q want=%q error=%v", i, data, payloads[i], err)
		}
	}
	{
		generation, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			key,
			&Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, generation != nil ||
			!errors.Is(err, errdef.ErrNotFound), "reused rejected generation: %v", err)
	}
	if (*testCaseInput.root) != "" {
		assert.NoError(t, (*testCaseInput.c).Close())
		(*testCaseInput.c) = cachetest.New(t, (*testCaseInput.root), New)
		{
			generation, _, _, err := (*testCaseInput.c).Blob(
				t.Context(),
				key,
				&Policy{MaxAge: Forever},
				nil,
			)
			assert.FatalIf(t, generation != nil ||
				!errors.Is(err, errdef.ErrNotFound), "persisted rejected generation: %v", err)
		}
	}
}
