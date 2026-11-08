package git

import (
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

type testFetcherHTTPSBasicAuthSnapshotRotationCaseInputs struct {
	fail      *atomic.Bool
	requests  *atomic.Int32
	expected  *atomic.Pointer[manifest.Secret]
	lookups   *atomic.Int32
	ca        *string
	sec       **manifest.Secret
	proxy     **httptest.Server
	cacheRoot *string
	repo      **manifest.GitRepository
	previous  *string
	wave      *string
}

func testFetcherHTTPSBasicAuthSnapshotRotationCase(
	t *testing.T, testCaseInputValue *testFetcherHTTPSBasicAuthSnapshotRotationCaseInputs,
) {
	t.Helper()
	testCaseInput := *testCaseInputValue
	if (*testCaseInput.wave) == "password" {
		(*testCaseInput.sec) = &manifest.Secret{
			StringData: map[string]any{"username": "alice", "password": "second", "ca.crt": (*testCaseInput.ca)},
		}
	}
	if (*testCaseInput.wave) == "certificate" {
		(*testCaseInput.sec) = &manifest.Secret{
			StringData: map[string]any{
				"username": "alice",
				"password": "second",
				"ca.crt":   testutil.SelfSignedCA(t),
			},
		}
	}
	testCaseInput.expected.
		Store((*testCaseInput.sec))
	proxyURL := strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:first@", 1)
	if (*testCaseInput.wave) == "proxy" || (*testCaseInput.wave) == "direct" {
		proxyURL = strings.Replace((*testCaseInput.proxy).URL, "://", "://proxy:second@", 1)
	}
	t.Setenv("http_proxy", proxyURL)
	if (*testCaseInput.wave) == "direct" {
		t.Setenv("no_proxy", "*")
	}
	c := cachetest.New(t, (*testCaseInput.cacheRoot), sourcecache.New)
	fInputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		testCaseInput.lookups.Add(1)
		return (*testCaseInput.sec)
	}}
	f := &Fetcher{Cache: c}
	before := testCaseInput.requests.Load()
	art, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		(*testCaseInput.repo),
		fInputs.readSet((*testCaseInput.repo)),
		&sourcecache.Policy{
			MaxAge: (*testCaseInput.repo).SourceInterval(),
			Retry:  sourcecache.RetryConfig{Attempts: 3},
		},
		nil,
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		art.Key == (*testCaseInput.previous) || testCaseInput.requests.Load() <= before,
		"%s",
		"changed material reused an acquisition",
	)

	(*testCaseInput.previous) = art.Key
	before = testCaseInput.requests.Load()
	{
		_, fetchErr := fetchtest.Fetch(
			t.Context(),
			f.Fetch,
			(*testCaseInput.repo),
			fInputs.readSet((*testCaseInput.repo)),
			&sourcecache.Policy{MaxAge: (*testCaseInput.repo).SourceInterval()},
			nil,
		)
		assert.FatalIf(t, fetchErr != nil || testCaseInput.requests.
			Load() != before, "warm snapshot missed: %v", fetchErr)
	}
	testCaseInput.fail.
		Store(true)
	failed := *(*testCaseInput.repo)
	failed.Interval.Duration = 0
	_, err = fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		&failed,
		fInputs.readSet(&failed),
		&sourcecache.Policy{MaxAge: failed.SourceInterval()},
		nil,
	)
	terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
	assert.FatalIf(t, !ok || !terminal.Attempted, "failed snapshot: %v", err)
	testCaseInput.fail.
		Store(false)
}
