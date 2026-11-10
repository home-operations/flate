package kustomize

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestRender_OversizedFrameMetadata(t *testing.T) {
	t.Parallel()
	renderer := newTestTreeCache(t, "")
	name := strings.Repeat("<", (16<<20)/6+1) + ".yaml"
	source := &store.SourceArtifact{LocalPath: "/source", FS: fstest.MapFS{
		"kustomization.yaml": {Data: []byte("resources: [" + name + "]\n")},
		name:                 {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: valid}\n")},
	}}
	input := RenderRequest{Source: source, SubPath: "."}
	request, err := renderer.prepareRender(t.Context(), &input)
	assert.NoError(t, err)
	request.request = input
	var result renderResult
	key := sourcecache.BlobKey{
		Content: request.key,
		Render:  sourcecache.RenderPending,
		Validate: func(ctx context.Context, generation fs.FS) (bool, error) {
			return request.validate(ctx, renderer, &result, generation)
		},
	}
	generation, _, identity, err := renderer.owner.Blob(
		t.Context(),
		key,
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		func(ctx context.Context, out sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			return request.produce(ctx, renderer, &result, out)
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, generation == nil, true)
	assert.Equal(t, identity, "")
	assert.Equal(t, len(result.docs), 1)
	assert.Diff(
		t,
		result.docs[0].Document,
		map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "valid"}},
	)
	_, _, _, err = renderer.owner.Blob(
		t.Context(),
		sourcecache.BlobKey{Content: request.key, Render: sourcecache.RenderPending},
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
}

func TestRecordRender_OversizedFrameMetadata(t *testing.T) {
	t.Parallel()
	renderer := newTestTreeCache(t, t.TempDir())
	if !renderer.owner.CanPersistRenders() {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(
		t,
		map[string]string{
			"kustomization.yaml": "resources: [cm.yaml]\n",
			"cm.yaml":            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: valid}\n",
		},
	)
	input := RenderRequest{Source: localSource(t, root), SubPath: "."}
	docs, receipt, err := testutil.RenderMaps(Render(t.Context(), renderer, input, sourcecache.RetryConfig{}))
	assert.NoError(t, err)
	acceptance := RenderAcceptance{
		Owner:   demoSpec.Named(),
		Receipt: receipt,
		Content: acceptedInput,
		Records: emptyRenderInventory,
		Sources: []manifest.SourceEvidence{},
	}
	assert.NoError(t, renderer.RecordRender(t.Context(), acceptance))
	frame := receipt.Value().(renderFrame)
	key := sourcecache.BlobKey{Content: frame.header.Key, Render: sourcecache.RenderAccepted}
	prior, descriptor, _, err := renderer.owner.Blob(
		t.Context(),
		key,
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	before, err := fs.ReadFile(prior, "content")
	assert.NoError(t, err)
	frame.stored = nil
	frame.header.Reads.Files = []localEntry{{Path: strings.Repeat("x", 16<<20)}}
	acceptance.Receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
	assert.NoError(t, renderer.RecordRender(t.Context(), acceptance))
	current, next, _, err := renderer.owner.Blob(
		t.Context(),
		key,
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	after, err := fs.ReadFile(current, "content")
	assert.NoError(t, err)
	assert.Equal(t, next.Digest, descriptor.Digest)
	assert.Diff(t, after, before)
	decoded, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, docs)
	var header renderHeader
	stored, err := sourcecache.OpenFrame(t.Context(), frameDomain, current, &header)
	assert.NoError(t, err)
	_, complete := stored.History(demoSpec.Named(), acceptedInput)
	assert.Equal(t, complete, true)
}

func TestApplicationReceipt_OversizedFrameMetadata(t *testing.T) {
	t.Parallel()
	renderer := newTestTreeCache(t, t.TempDir())
	if !renderer.owner.CanPersistRenders() {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{"producer.yaml": "accepted"})
	input := RenderRequest{Source: localSource(t, root), SubPath: "."}
	owner := demoSpec.Named()
	evidence := EntryEvidence{
		Owner:       owner,
		Producer:    owner,
		Root:        owner,
		Records:     emptyRenderInventory,
		Application: map[string]any{"kind": "Kustomization"},
	}
	prior, err := renderer.ApplicationReceipt(t.Context(), input, evidence, []string{"producer.yaml"})
	assert.NoError(t, err)
	assert.NoError(t, renderer.RecordEntry(t.Context(), prior.Receipt, nil))
	evidence.Application = map[string]any{"kind": "Kustomization", "description": strings.Repeat("x", 16<<20)}
	unavailable, err := renderer.ApplicationReceipt(t.Context(), input, evidence, []string{"producer.yaml"})
	assert.NoError(t, err)
	assert.Equal(t, unavailable.Receipt == nil, true)
	assert.NoError(t, renderer.RecordEntry(t.Context(), unavailable.Receipt, nil))
	current := renderer.PreviousEntry(t.Context(), input)
	assert.Equal(t, current.Receipt.Equal(prior.Receipt), true)
	assert.Diff(t, current.Application, prior.Application)
}
