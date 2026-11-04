package helm

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_OCIResidentGenerationValidation(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	st, hr, directory := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	artifact := &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		FS:        chartFixtureFS(hr, os.DirFS(directory)),
		LocalPath: directory,
		Digest:    digest,
		Revision:  "6.15.0@" + digest,
	}
	st.SetArtifact(hr.ChartSource(), artifact)
	owner := cachetest.New(t, t.TempDir(), cache.New)
	client := NewClient(owner)
	input := RenderOptions{Sources: NewSources(st)}
	for range 2 {
		_, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
		assert.NoError(t, err)
		assert.NoError(
			t,
			client.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   hr.Named(),
					Receipt: receipt,
					Sources: []manifest.SourceEvidence{},
					Records: emptyRenderInventory,
				},
			),
		)
	}
	prepared, err := client.prepareTemplate(t.Context(), hr, nil, &input)
	assert.NoError(t, err)
	resident := prepared.loaded.record
	assert.FatalIf(
		t,
		resident == nil || resident.identity == "",
		"%s",
		"fixture did not retain the validated resident generation",
	)
	header := resident.frame.header
	analysis := *header.Analysis
	analysis.Version = "invalid"
	header.Analysis = &analysis
	payload, err := encodeFrame(header, resident.Output)
	assert.NoError(t, err)
	_, _, _, err = owner.Blob(
		t.Context(),
		cache.BlobKey{Content: resident.key, Render: cache.RenderAccepted},
		&cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, writeErr := w.Write(payload)
			return cache.ProducerResult{}, writeErr
		},
	)
	assert.NoError(t, err)

	{
		_, _, err := testutil.RenderMaps(
			client.Template(t.Context(), hr, nil, input),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "replaced OCI generation escaped version validation: %v", err)
	}
	{
		_, complete := client.PreviousRender(t.Context(), hr, nil, input)
		assert.FatalIf(t, complete, "%s", "invalid OCI generation supplied accepted history")
	}
	changed := *artifact
	changed.Revision = "invalid"
	st.SetArtifact(hr.ChartSource(), &changed)
	{
		_, _, err := testutil.RenderMaps(
			client.Template(t.Context(), hr, nil, input),
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "changed OCI revision borrowed resident validation: %v", err)
	}
}
