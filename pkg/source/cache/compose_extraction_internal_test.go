package cache

import (
	"context"
	"errors"
	"testing"
	"testing/fstest"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/tree"
)

func TestComposition_ExtractionWork(t *testing.T) {
	t.Parallel()
	owner := cachetest.New(t, "", New)
	content := struct{ Namespace, Generation, Path, Destination string }{
		"composition/extract/test",
		"generation",
		"archive.tgz",
		"out",
	}
	recorder := &manifest.WorkRecorder{
		Key: manifest.WorkKey{Owner: manifest.NamedResource{Kind: "ArtifactGenerator", Name: "generator"}},
	}
	scope := recorder.Scope()
	e := composition{
		owner:  owner,
		maxAge: Forever,
		work:   manifest.AcquisitionWork{Scope: scope},
		request: Composition{Inputs: []CompositionInput{{FS: fstest.MapFS{
			"archive.tgz": {Data: compressedForTest(t, fstest.MapFS{"file": {Data: []byte("bytes")}})},
		}, Content: "generation"}}},
	}
	op := Copy{
		Input: 0, From: "archive.tgz", To: "out", Mode: ExtractArchive,
		ExtractContent: func(string, string) any { return content },
	}
	plan, err := e.prepareCopy(&op)
	assert.NoError(t, err)
	err = plan.extract(t.Context(), tree.NewBuilder(nil), op.From)
	assert.FatalIf(t, err != nil, "%v", err)
	key := plan.extractionKey(op.From)
	called := false
	input, _, id, err := owner.Acquire(
		t.Context(),
		key,
		&Policy{MaxAge: Forever},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			called = true
			return ProducerResult{Descriptor: ocispec.Descriptor{}}, errors.New("unexpected extraction")
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
	var preparation, acquisition, service manifest.WorkCounts
	for _, record := range scope.Snapshot(0) {
		assert.Equal(t, record.Key.Owner, recorder.Key.Owner)
		assert.Equal(t, record.Key.Input, id)
		if record.Key.Phase == manifest.WorkPreparation {
			preparation.Add(&record.Counts)
		}
		if record.Key.Phase == manifest.WorkAcquisition {
			acquisition.Add(&record.Counts)
		}
		if record.Key.Phase == manifest.WorkCache {
			service.Add(&record.Counts)
		}
	}
	assert.Equal(t, preparation, manifest.WorkCounts{RawKeyPrepares: 1})
	assert.Equal(t, acquisition, manifest.WorkCounts{Attempts: 1, Completed: 1})
	assert.Equal(t, service, manifest.WorkCounts{Attempts: 1, Completed: 1})
}
