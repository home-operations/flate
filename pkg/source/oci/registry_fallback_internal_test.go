package oci

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_ResolveConfig_RegistryFallbackSnapshot(t *testing.T) {
	t.Parallel()
	for _, callers := range []int{2, 4} {
		t.Run(strconv.Itoa(callers), func(t *testing.T) {
			t.Parallel()
			testFetcherResolveConfigRegistryFallbackSnapshotCase(
				t,
				&callers,
			)
		},
		)
	}
}

func testFetcherResolveConfigRegistryFallbackSnapshotCase(
	t *testing.T, callers *int,
) {
	t.Helper()

	ca := testutil.SelfSignedCA(t)
	artifact := newFakeOCIArtifact(t, map[string]string{"data.yaml": "concurrent"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(
		t,
		config,
		fmt.Sprintf(`{"auths":{%q:{"username":"alice","password":"test"}}}`, mustURL(t, upstream.URL).Host),
	)
	c := cachetest.New(t, t.TempDir(), sourcecache.New)
	var lookups atomic.Int32
	fInputs := testInputs{secrets: func(_, name string) *manifest.Secret {
		lookups.Add(1)
		if name == "tls" {
			return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
		}
		return nil
	}}
	f := &Fetcher{Network: newTestNetwork(t), Cache: c, RegistryConfig: config}
	repo := ociRepo("fallback", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, upstream.URL).Host + "/app"
		s.Insecure = true
		s.SecretRef = &manifest.LocalObjectReference{Name: "missing"}
		s.CertSecretRef = &manifest.LocalObjectReference{Name: "tls"}
	})
	var work manifest.WorkRecorder
	reads := fInputs.readSet(repo)
	reads.SetWork(&work)
	_, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		reads,
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil || lookups.Load() != 2 {
		t.Fatalf("snapshot before handoff: reads=%d error=%v", lookups.Load(), err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var wg sync.WaitGroup
	for range *callers {
		wg.Go(func() {
			_, err := fetchtest.Retry(ctx, missing.RetryAmbient)
			assert.ErrorIf(t, !errors.Is(err, context.Canceled), "canceled fallback: %v", err)
		})
	}

	wg.Wait()
	start := make(chan struct{})
	artifacts := make([]*store.SourceArtifact, (*callers))
	failures := make([]error, (*callers))
	for i := range *callers {
		wg.Go(func() { <-start; artifacts[i], failures[i] = fetchtest.Machine(t.Context(), missing, nil) })
	}
	close(start)
	wg.Wait()
	for i, artifact := range artifacts {
		if failures[i] != nil || artifact == nil || artifact.Key != artifacts[0].Key ||
			mustReadFile(t, artifact.FS, "data.yaml") != "concurrent" {
			t.Fatalf("concurrent prepared acquisition: artifact=%v error=%v", artifact, failures[i])
		}
	}
	if lookups.Load() != 2 {
		t.Fatalf("retained fallback reread mutable Secrets: %d", lookups.Load())
	}
	var transfers uint64
	{
		records := work.Scope().Snapshot(0)
		for recordIndex := range records {
			record := &records[recordIndex]
			assert.Equal(t, record.Key.Source, repo.Named())
			transfers += record.Counts.RemoteTransfers
		}
	}
	assert.Equal(t, transfers, uint64(1))
}
