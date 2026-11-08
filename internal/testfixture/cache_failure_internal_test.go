package testfixture

import (
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFixture_CacheFailureReplay(t *testing.T) {
	t.Parallel()
	dir := fixtureSource(t, "reset")
	s, err := load(dir)
	assert.NoError(t, err)
	failure := &cacheFailure{
		Cache: "default", Repository: "https://charts.example.invalid", Chart: "demo", Version: "0.1.0", Attempts: 12,
	}
	s.Overlays = append(s.Overlays, overlay{Name: "failure", Operations: []operation{{CacheFailure: failure}}})
	data, err := json.Marshal(s)
	assert.NoError(t, err)
	testutil.WriteFile(t, dir, "scenario.json", string(data))
	f := materialize(t, dir, "sequence")
	key := cacheFailureKey(t, failure)
	for range 2 {
		f.Restore(t)
		root := f.Cache(t, &Invocation{})
		c, err := sourcecache.New(t.Context(), root)
		assert.NoError(t, err)
		_, _, _, err = c.Blob(t.Context(), key, &sourcecache.Policy{},
			func(_ context.Context, target sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				const payload = "last-good chart"
				_, writeErr := target.Write([]byte(payload))
				return sourcecache.ProducerResult{
					Digest: digest.FromString(payload), Size: int64(len(payload)),
					Annotations: map[string]string{
						"flate.chart-version": "0.1.0",
						"flate.chart-url":     "https://charts.example.invalid/demo-0.1.0.tgz",
					},
				}, writeErr
			})
		assert.NoError(t, err)
		assert.NoError(t, c.Close())
		f.Edit(t, Step{Edit: &operation{Overlay: "failure"}})
		c, err = sourcecache.New(t.Context(), root)
		assert.NoError(t, err)
		_, _, _, err = c.Blob(t.Context(), key, &sourcecache.Policy{},
			func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				t.Fatal("capped replay invoked the producer")
				return sourcecache.ProducerResult{}, nil
			})
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		if !ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("declared failure lost its cached classification: %v", err)
		}
		assert.NoError(t, c.Close())
	}
}
