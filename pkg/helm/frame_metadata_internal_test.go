package helm

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestTemplate_OversizedFrameMetadata(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
		"mychart/templates/cm.yaml": "{{/*" + strings.Repeat(
			"x",
			16<<20,
		) + "*/}}apiVersion: v1\nkind: ConfigMap\nmetadata: {name: valid}\n",
	})
	client := NewClient(cachetest.New(t, "", cache.New))
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	request, err := client.prepareTemplate(t.Context(), newHR(), nil, &input)
	assert.NoError(t, err)
	var result templateResult
	key := cache.BlobKey{
		Content: request.key,
		Render:  cache.RenderPending,
		Validate: func(ctx context.Context, generation fs.FS) (bool, error) {
			return request.validate(ctx, client, &result, generation)
		},
	}
	generation, _, identity, err := client.cache.Blob(
		t.Context(),
		key,
		&cache.Policy{MaxAge: cache.Forever},
		func(ctx context.Context, out cache.ProducerInput) (cache.ProducerResult, error) {
			return request.produce(ctx, client, &result, out)
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
	assert.Equal(t, result.receipt != nil, true)
	assert.Equal(t, result.produced == nil, true)
	_, _, _, err = client.cache.Blob(
		t.Context(),
		cache.BlobKey{Content: request.key, Render: cache.RenderPending},
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
}

func TestRecordRender_OversizedFrameMetadata(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
		"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: valid}\n",
	})
	client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	hr := newHR()
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	docs, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
	assert.NoError(t, err)
	acceptance := RenderAcceptance{
		Owner:   hr.Named(),
		Receipt: receipt,
		Records: emptyRenderInventory,
		Sources: []manifest.SourceEvidence{},
	}
	assert.NoError(t, client.RecordRender(t.Context(), acceptance))
	frame := receipt.Value().(renderFrame)
	key := cache.BlobKey{Content: frame.header.Key, Render: cache.RenderAccepted}
	prior, descriptor, _, err := client.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	before, err := fs.ReadFile(prior, "content")
	assert.NoError(t, err)
	frame.stored = nil
	frame.header.Analysis = &chartAnalysis{Version: "0.1.0", Templates: strings.Repeat("x", 16<<20)}
	acceptance.Receipt = manifest.NewRenderReceipt(frame, equalRenderFrame)
	assert.NoError(t, client.RecordRender(t.Context(), acceptance))
	current, next, _, err := client.cache.Blob(t.Context(), key, &cache.Policy{MaxAge: cache.Forever}, nil)
	assert.NoError(t, err)
	after, err := fs.ReadFile(current, "content")
	assert.NoError(t, err)
	assert.Equal(t, next.Digest, descriptor.Digest)
	assert.Diff(t, after, before)
	decoded, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Diff(t, decoded, docs)
	_, complete := client.PreviousRender(t.Context(), hr, nil, input)
	assert.Equal(t, complete, true)
}
