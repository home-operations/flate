package helm

import (
	"bytes"
	"errors"
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordRender_AcceptedLimit(t *testing.T) {
	t.Parallel()
	if !renderPersistenceEligible(t) {
		t.Skip("requires a clean VCS-stamped build for positive persistence")
	}
	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{
		header: renderHeader{Key: "oversized-accepted", Analysis: &chartAnalysis{Version: "0.1.0"}},
		output: payload,
	}
	receipt := manifest.NewRenderReceipt(frame, equalRenderFrame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {File: string(bytes.Repeat([]byte{'x'}, frameHeaderLimit+1))},
	}
	{
		err := renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   owner,
				Receipt: receipt,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrFlux,
		), "oversized accepted inventory was published: %v", err)
	}
}
