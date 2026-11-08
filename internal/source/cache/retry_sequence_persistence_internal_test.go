package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type testAcquireRetrySequencePersistenceCaseInputs struct {
	disk *bool
	tc   *struct {
		name  string
		cause error

		attempts int
		record   bool
	}
}

func testAcquireRetrySequencePersistenceCase(
	t *testing.T,
	testCaseInput testAcquireRetrySequencePersistenceCaseInputs,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		testAcquireRetrySequencePersistenceCaseCallback(
			t,
			testAcquireRetrySequencePersistenceCaseCallbackInputs{testCaseInput: &testCaseInput},
		)
	},
	)
}

type testAcquireRetrySequencePersistenceCaseCallbackInputs struct {
	testCaseInput *testAcquireRetrySequencePersistenceCaseInputs
}

func testAcquireRetrySequencePersistenceCaseCallback(
	t *testing.T,
	testCaseInput testAcquireRetrySequencePersistenceCaseCallbackInputs,
) {
	t.Helper()
	root := ""
	if *testCaseInput.testCaseInput.disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	calls := 0
	for i := range 2 {
		_, _, _, err := c.Blob(
			t.Context(),
			BlobKey{Content: "eligibility"},
			&Policy{Retry: fastRetry(3)},
			func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				_, _ = io.WriteString(w, "partial")
				return ProducerResult{}, testCaseInput.testCaseInput.tc.cause
			},
		)
		want := testCaseInput.testCaseInput.tc.attempts * (i + 1)
		if (*testCaseInput.testCaseInput.disk) && testCaseInput.testCaseInput.tc.record {
			want = 1
		}
		assert.FatalIf(
			t,
			calls != want || err == nil || !errors.Is(err, manifest.ErrFlux),
			"calls=%d, want=%d, error=%v",
			calls,
			want,
			err,
		)
		if !(*testCaseInput.testCaseInput.disk) || !testCaseInput.testCaseInput.tc.record || i == 0 {
			assert.FatalIf(t, !errors.Is(err, testCaseInput.testCaseInput.tc.cause), "live cause lost: %v", err)
		}
	}
	if *testCaseInput.testCaseInput.disk {
		desc, err := c.store.Resolve(
			t.Context(),
			manifest.Fingerprint(
				struct{ Archive, Blob, Git any }{Blob: struct{ Content any }{"eligibility"}},
			),
		)
		assert.FatalIf(
			t, testCaseInput.testCaseInput.tc.
				record && (err != nil || desc.MediaType != ocispec.MediaTypeImageManifest) ||
				!testCaseInput.testCaseInput.tc.record &&
					!errors.Is(err, errdef.ErrNotFound), "eligibility descriptor=%+v, error=%v",
			desc,
			err,
		)
		files, err := os.ReadDir(filepath.Join(c.root.Name(), ociDir))
		assert.NoError(t, err)

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "acquire-") {
				t.Fatalf("failed sink remains: %s", file.Name())
			}
		}
	}
}
