package kustomize

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest,tparallel // Cold and cached cases share one renderer and retained output.
func TestRecordRender_EngineProjection(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("accepted"),
	})
	renderer := newTestTreeCache(t, t.TempDir())
	spec := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
	request := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	source := request.Source.HistoryIdentity(spec.EffectiveSourceID())
	var previous []byte
	for _, state := range []string{"cold", "cached"} {
		t.Run(state, func(t *testing.T) {
			docs, receipt, err := Render(t.Context(), renderer, request, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			frame, ok := receipt.Value().(renderFrame)
			assert.Equal(t, ok, true)
			validation, err := frame.RenderPublication(t.Context(), nil)
			assert.NoError(t, err)
			assert.Equal(t, validation.Metadata == nil && validation.Content == nil && validation.Sources == nil, true)
			if previous != nil {
				assert.Equal(t, bytes.Equal(frame.Output, previous), true)
			}
			previous = bytes.Clone(frame.Output)
			backing := []manifest.SourceEvidence{{Source: source, Path: "provided.yaml"}, {Path: "sentinel"}}
			acceptance := sourcecache.RenderAcceptance{
				Owner: spec.Named(), Receipt: receipt, Records: emptyRenderInventory,
				Content: PrepareShape(&request, docs).Content(), Source: source, Sources: backing[:1],
			}
			projection, err := frame.RenderPublication(t.Context(), &acceptance)
			assert.NoError(t, err)
			assert.Equal(t, projection.Domain, frameDomain)
			assert.Equal(t, projection.Frame.Stored == frame.Stored, true)
			assert.Equal(t, manifest.ValueEqual(projection.Content, acceptance.Content), true)
			for _, path := range []string{"kustomization.yaml", "cm.yaml"} {
				assert.Equal(
					t,
					slices.Contains(projection.Sources, manifest.SourceEvidence{Source: source, Path: path}),
					true,
				)
			}
			assert.Equal(t, backing[1].Path, "sentinel")
			assert.Equal(t, len(acceptance.Sources), 1)
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
			for _, backing := range []string{"disk", "memory"} {
				t.Run(backing, func(t *testing.T) {
					owner := renderer.owner
					if backing == "memory" {
						owner = cachetest.New(t, "", sourcecache.New)
					}
					t.Logf("render persistence enabled=%t", owner.CanPersistRenders())
					invalid := frame
					invalid.Key = ""
					rejected := acceptance
					rejected.Receipt = manifest.NewRenderReceipt(invalid, equalRenderFrame)
					recordErr := owner.RecordRender(t.Context(), rejected)
					assert.Equal(t, errors.Is(recordErr, manifest.ErrFlux), true)
					assert.NoError(t, owner.RecordRender(t.Context(), acceptance))
				})
			}
			acceptance.Content = map[string]any{"unsupported": "content"}
			invalid, err := frame.RenderPublication(t.Context(), &acceptance)
			assert.NoError(t, err)
			assert.Equal(t, invalid.Content == nil && invalid.Sources == nil, true)
			assert.NoError(t, renderer.owner.RecordRender(t.Context(), acceptance))
			acceptance.Content = projection.Content
			acceptance.Sources = nil
			missing, err := frame.RenderPublication(t.Context(), &acceptance)
			assert.NoError(t, err)
			assert.Equal(t, missing.Sources == nil, true)
			assert.Equal(t, frame.Stored == receipt.Value().(renderFrame).Stored, true)
		})
	}
}
