package artifactgenerator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func compressedForTest(t *testing.T, files fstest.MapFS) []byte {
	t.Helper()
	var data bytes.Buffer
	gz := gzip.NewWriter(&data)
	w := tar.NewWriter(gz)
	{
		err := w.AddFS(files)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := w.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		err := gz.Close()
		assert.FatalIf(t, err != nil, "%v", err)
	}
	return data.Bytes()
}

func TestEvaluate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		operations []operation
		invalid    bool
	}{
		{"unknown_alias", []operation{{From: "@absent/file", To: "@artifact/file"}}, true},
		{"destination_prefix", []operation{{From: "@repo/app/file", To: "file"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := evaluation{
				identity: []inputIdentity{{Alias: "repo"}},
				inputs: map[string]fs.FS{
					"repo": fstest.MapFS{"app/file": {Data: []byte("file")}},
				},
			}
			_, err := e.composition(manifest.NamedResource{}, &request{Output: output{Copy: tc.operations}})
			assert.FatalIf(t, (err != nil) != tc.invalid, "evaluate() = %v; invalid=%v", err, tc.invalid)
		})
	}
}

func TestEvaluate_ExtractIdentity(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", cache.New)
	identities := []inputIdentity{
		{Alias: "repo", Input: store.ArtifactInput{Key: "family", Content: "generation", Digest: "generation"}},
	}
	recorder := &manifest.WorkRecorder{
		Key: manifest.WorkKey{Owner: manifest.NamedResource{Kind: "ArtifactGenerator", Name: "generator"}},
	}
	scope := recorder.Scope()
	e := evaluation{
		maxAge:   cache.Forever,
		identity: identities,
		inputs: map[string]fs.FS{
			"repo": fstest.MapFS{
				"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte("bytes")}})},
			},
		},
	}
	selection := archiveSelection{Alias: "repo", Path: "archive.tgz", Destination: "out"}
	req := request{
		Output: output{
			Name: "artifact",
			Copy: []operation{{From: "@repo/archive.tgz", To: "@artifact/out", Strategy: strategyExtract}},
		},
	}
	req.work.Scope = scope
	composition, err := e.composition(recorder.Key.Owner, &req)
	assert.FatalIf(t, err != nil, "%v", err)
	_, _, _, err = owner.Compose(t.Context(), composition, &cache.Policy{MaxAge: cache.Forever, Work: &req.work})
	assert.FatalIf(t, err != nil, "%v", err)
	key := cache.ArchiveKey{Policy: "extract", Content: struct {
		Namespace string
		Inputs    []inputIdentity
		Selection archiveSelection
	}{"artifactgenerator/extract/v1", identities, selection}}
	called := false
	input, _, id, err := owner.Acquire(
		t.Context(),
		key,
		&cache.Policy{MaxAge: cache.Forever},
		func(context.Context, cache.ProducerInput) (cache.ProducerResult, error) {
			called = true
			return cache.ProducerResult{Descriptor: ocispec.Descriptor{}}, errors.New("unexpected extraction")
		},
	)
	assert.FatalIf(
		t,
		err != nil || input == nil || id == "" || called,
		"nested extraction identity = %v, %q, %v; produced=%v",
		input,
		id,
		err,
		called,
	)
}
