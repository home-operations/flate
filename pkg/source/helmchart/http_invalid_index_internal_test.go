package helmchart

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetchIndexInvalidResponseDoesNotPublishCase(
	t *testing.T, tc *struct {
		name string
		data string
	},
) {
	t.Helper()

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			_, _ = io.WriteString(w, tc.data)
			return
		}
		_, _ = io.WriteString(w, helmRepoIndex(""))
	}))
	t.Cleanup(srv.Close)
	r := httpRepo(srv.URL)
	root := t.TempDir()
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	fInputs := testInputs{}
	f := New(nil, Config{Network: newTestNetwork(t), Cache: c})
	{
		_, fetchIndexErr := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, r), sourcecache.Policy{})
		assert.FatalIf(t, fetchIndexErr == nil, "%s", "accepted invalid index")
	}
	{
		_, ok := f.indexCache.Load("sha256:" + chartDigest([]byte(tc.data)))
		assert.FatalIf(t, ok, "%s", "invalid index entered parsed memo")
	}

	idx, err := f.fetchIndex(t.Context(), preparedHTTP(t, f, fInputs, r), sourcecache.Policy{})
	assert.FatalIf(t, err != nil, "retry within repository interval: %v", err)
	{
		_, getErr3 := idx.Get("app-template", "1.0.0")
		assert.NoError(t, getErr3)
	}
	{
		closeErr := c.Close()
		assert.NoError(t, closeErr)
	}

	c, err = sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	warmInputs := testInputs{}
	warm := New(nil, Config{Network: newTestNetwork(t), Cache: c})

	cached, err := warm.fetchIndex(t.Context(), preparedHTTP(t, warm, warmInputs, r), sourcecache.Policy{})
	assert.FatalIf(t, err != nil, "disk-cached index: %v", err)
	{
		memo, ok := warm.indexCache.Load(
			"sha256:" + chartDigest([]byte(helmRepoIndex(""))),
		)
		assert.FatalIf(t, !ok ||
			memo != cached, "%s", "disk-cached index did not populate digest-bound memo")
	}

	{
		got := attempts.Load()
		assert.FatalIf(t, got != 2, "index fetched %d times, want 2", got)
	}
}
