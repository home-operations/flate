package source

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testHTTPWorkLifetime(t *testing.T, scenario string) {
	t.Helper()
	inputs := newTestHTTPInputs(t, "")
	work := manifest.WorkRecorder{Key: manifest.WorkKey{
		Tree:   manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead},
		Owner:  manifest.NamedResource{Kind: manifest.KindKustomization, Name: "app"},
		Source: manifest.BootstrapSourceID,
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	calls := 0
	inputs.httpClient.Transport = remoteRoundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		if scenario == "canceled_waiter" {
			close(entered)
			<-release
		}
		code := http.StatusOK
		if scenario == "failed_wave" && calls == 1 {
			code = http.StatusServiceUnavailable
		}
		return &http.Response{
			StatusCode: code,
			Body:       io.NopCloser(strings.NewReader("accepted")),
			Header:     make(http.Header),
		}, nil
	})
	const url = "https://example.invalid/work"
	if scenario == "failed_wave" {
		_, err := inputs.Acquire(
			ctx,
			&HTTPRequest{Work: work.Scope(), URL: url, Retry: cache.RetryConfig{Attempts: 1}, HeldSlot: nil},
		)
		if err == nil {
			t.Fatal("missing first wave failure")
		}
		_, err = inputs.Acquire(
			ctx,
			&HTTPRequest{Work: work.Scope(), URL: url, Retry: cache.RetryConfig{Attempts: 1}, HeldSlot: nil},
		)
		assert.NoError(t, err)
	} else {
		done := make(chan error, 1)
		go func() {
			_, err := inputs.Acquire(
				ctx,
				&HTTPRequest{Work: work.Scope(), URL: url, Retry: cache.RetryConfig{Attempts: 1}, HeldSlot: nil},
			)
			done <- err
		}()
		<-entered
		cancel()
		assert.Equal(t, <-done, context.Canceled)
		close(release)
		synctest.Wait()
	}
	inputs.Close()
	var attempts manifest.WorkCounts
	{
		records := work.Scope().Snapshot(0)
		for recordIndex := range records {
			record := &records[recordIndex]
			assert.Equal(t, record.Key.Tree, work.Key.Tree)
			assert.Equal(t, record.Key.Owner, work.Key.Owner)
			assert.Equal(t, record.Key.Source, work.Key.Source)
			if record.Key.Phase == manifest.WorkAcquisition {
				if record.Key.Input == "" {
					t.Fatal("failed wave lost acquisition identity")
				}
				attempts.Add(&record.Counts)
			}
		}
	}
	want := manifest.WorkCounts{Attempts: 1, Completed: 1, RemoteTransfers: 1}
	if scenario == "failed_wave" {
		want = manifest.WorkCounts{Attempts: 2, Completed: 1, Failures: 1, RemoteTransfers: 1}
	}
	assert.Equal(t, attempts, want)
}

func TestHTTPInputs_WorkLifetimes(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"failed_wave", "canceled_waiter"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) { testHTTPWorkLifetime(t, scenario) })
		})
	}
}

func TestHTTPInputs_WorkValidation(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", cache.New)
	for attempt := range 2 {
		inputs := newHTTPInputs(t, owner, nil, task.NewBounded(2))
		work := manifest.WorkRecorder{Key: manifest.WorkKey{
			Tree: manifest.TreeID{
				Cluster: "cluster",
				Side:    manifest.TreeSide(attempt),
			},
			Source: manifest.BootstrapSourceID,
		}}
		inputs.httpClient.Transport = remoteRoundTrip(func(req *http.Request) (*http.Response, error) {
			code, body := http.StatusOK, "accepted"
			if attempt == 1 {
				assert.Equal(t, req.Header.Get("If-None-Match"), "one")
				code, body = http.StatusNotModified, ""
			}
			header := make(http.Header)
			header.Set("ETag", "one")
			return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: header}, nil
		})
		input, err := inputs.Acquire(t.Context(), &HTTPRequest{
			Work:     work.Scope(),
			URL:      "https://example.invalid/validation",
			Retry:    cache.RetryConfig{Attempts: 1},
			HeldSlot: nil,
		})
		assert.NoError(t, err)
		data, err := fs.ReadFile(input, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "accepted")
		inputs.Close()
		var transfers, validations, service uint64
		{
			records := work.Scope().Snapshot(0)
			for recordIndex := range records {
				record := &records[recordIndex]
				transfers += record.Counts.RemoteTransfers
				service += record.Counts.CacheService
				if record.Key.Phase == manifest.WorkValidation {
					validations += record.Counts.Completed
				}
			}
		}
		assert.Equal(t, transfers, uint64(1-attempt))
		assert.Equal(t, validations, uint64(attempt))
		assert.Equal(t, service, uint64(attempt))
	}
}
