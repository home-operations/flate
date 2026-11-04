package bucket

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type testFetcherMachineContinuationCase1TailInputs struct {
	err *error

	artifact **store.
			SourceArtifact

	requests **atomic.
			Int32

	f **Fetcher

	b **manifest.
		Bucket

	reads *store.
		ReadSet

	tier **string

	access **string

	root *string
}

func testFetcherMachineContinuationCase1Tail(t *testing.T, inputs *testFetcherMachineContinuationCase1TailInputs) {
	t.Helper()

	assert.NoError(t, (*inputs.err))
	assert.FatalIf(t, (*inputs.artifact) == nil, "%s", "acquisition returned no source")
	assert.FatalIf(t, len((*inputs.artifact).Warnings) != 0,
		"advisory count=%d, want %d", len((*inputs.artifact).Warnings), 0)
	var data []byte

	data, (*inputs.err) = fs.ReadFile((*inputs.artifact).FS, "file.yaml")
	assert.NoError(t, (*inputs.err))
	assert.Equal(t, string(data), "data")
	assert.FatalIf(t, strings.Contains(
		fmt.Sprint(
			(*inputs.artifact).Key,
			(*inputs.artifact).Content,
			(*inputs.artifact).Metadata,
			(*inputs.artifact).Warnings,
		),
		"canary",
	), "%s", "machine credentials entered artifact identity or output")

	before := (*inputs.requests).Load()
	var warm *store.
		SourceArtifact

	warm, (*inputs.err) = (*inputs.f).Fetch(
		t.Context(),
		(*inputs.b),
		inputs.reads,
		&cache.Policy{MaxAge: time.Hour},
	)
	if missing, ok := errors.AsType[*source.MissingSecretError]((*inputs.err)); ok && missing.RetryAmbient != nil {
		warm, (*inputs.err) = missing.RetryAmbient(t.Context())
	}
	assert.NoError(t, (*inputs.err))
	assert.FatalIf(
		t,
		warm.Key != (*inputs.artifact).Key || warm.Content != (*inputs.artifact).Content || len(warm.Warnings) != 0 ||
			(*inputs.requests).
				Load() != before,
		"%s",
		"warm acquisition consulted credentials, changed identity or repeated success information",
	)

	if (*(*inputs.tier)) == "machine" {
		(*(*inputs.access)) = "rotated-canary"
		t.Setenv("AWS_ACCESS_KEY_ID", (*(*inputs.access)))
		t.Setenv("AWS_SECRET_ACCESS_KEY", "rotated-secret-canary")
		_, err := (*inputs.f).Fetch(t.Context(), (*inputs.b), inputs.reads, &cache.Policy{MaxAge: time.Hour})
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok, "%s", err)

		rotated, err := missing.RetryMachine(t.Context(), inputs.reads)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			rotated.Key == warm.Key || (*inputs.requests).Load() == before,
			"%s",
			"rotation reused credential identity or skipped signing",
		)

		t.Setenv("AWS_ACCESS_KEY_ID", "")
		t.Setenv("AWS_SECRET_ACCESS_KEY", "")
		testutil.WriteFileAt(t, os.Getenv("AWS_CONFIG_FILE"), "[default]\naws_access_key_id = partial\n")
		beforeFailure := (*inputs.requests).Load()
		failed, err := missing.RetryMachine(t.Context(), inputs.reads)
		assert.FatalIf(
			t,
			failed != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) || (*inputs.requests).
				Load() != beforeFailure,
			"failed preparation reused warm content: %v",
			err,
		)
	}
	var warmData []byte

	warmData, (*inputs.err) = fs.ReadFile(warm.FS, "file.yaml")
	assert.NoError(t, (*inputs.err))
	assert.Equal(t, string(warmData), string(data))
	if (*inputs.root) != "" {
		assert.NoError(t, filepath.WalkDir((*inputs.root), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			payload, err := os.ReadFile(path)
			assert.ErrorIf(
				t,
				strings.Contains(string(payload), "canary"),
				"%s",
				"machine credentials or success provenance entered persistent cache data",
			)

			return err
		}))
	}
}

type testFetcherMachineContinuationCase1Inputs struct {
	rejected *bool
	tier     *string
	access   *string
	requests *atomic.Int32
}
