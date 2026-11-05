package harness

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // CLI invocations replace process-wide state.
func TestCase_TerminalReplayKeepsNonAttemptClassification(t *testing.T) {
	fixture := Open(t, "scope-orphans", "namespace-terminal-replay")
	want := fixture.Want(t)
	repository := &manifest.HelmRepository{}
	repository.URL = "https://charts.example.invalid"
	chart := &manifest.HelmChartSource{}
	chart.Chart, chart.Version = "demo", "0.1.0"
	material, err := source.ResolveTransport(nil, manifest.DiagnosticFor(repository.Named(), repository), nil)
	assert.NoError(t, err)
	key := sourcecache.BlobKey{
		Content: struct{ Repository, Chart, Transport any }{
			repository.Content(), chart.Content(), material.Identity(nil),
		},
		Required: []string{"chart-version", "chart-url"},
	}
	for range 2 {
		if got := fixture.Run(t); !slices.Equal(got, want) {
			t.Fatalf("want %#v, got %#v", want, got)
		}
		c, err := sourcecache.New(t.Context(), fixture.fixture.CachePath("default"))
		assert.NoError(t, err)
		_, _, _, err = c.Blob(t.Context(), key, &sourcecache.Policy{},
			func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
				t.Fatal("fresh replay invoked the producer")
				return sourcecache.ProducerResult{}, nil
			})
		terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
		if !ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("fresh replay lost its non-attempt classification: %v", err)
		}
		assert.NoError(t, c.Close())
	}
}

//nolint:paralleltest // The fixture replaces process-wide environment and transports.
func TestCase_FailedOwnerDoesNotAdmitRawReleases(t *testing.T) {
	fixture := Open(t, "scope-orphans", "namespace-failing-owner-2")
	fixture.fixture.Restore(t)
	objects := store.New()
	_, err := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(fixture.fixture.Repo, "flux"), RepoRoot: fixture.fixture.Repo, Store: objects,
	})
	assert.NoError(t, err)
	if releases := objects.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease); len(releases) != 0 {
		t.Fatalf("failed owner's raw releases entered discovery: %+v", releases)
	}
}
