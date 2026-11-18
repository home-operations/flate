package helm

import (
	"bytes"
	"errors"
	"maps"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRecordRender_AcceptedLimit(t *testing.T) {
	t.Parallel()
	t.Logf("render persistence reason=%q backing=disk", cache.RenderPersistenceDisabledReason())
	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New), nil)
	owner := manifest.NamedResource{Group: "group", Kind: "owner", Namespace: "team", Name: "renderer"}
	payload, err := testutil.EncodeRender([]map[string]any{{"value": "original"}})
	assert.NoError(t, err)
	frame := renderFrame{
		Key: "oversized-accepted", Output: payload,
		header:  renderHeader{Analysis: &chartAnalysis{Version: "0.1.0"}},
		content: newHR().Content("oversized-accepted", nil),
		persist: renderer.cache.CanPersistRenders(),
	}
	receipt := manifest.NewRenderReceipt(frame, equalRenderFrame)
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{
		owner: {File: string(bytes.Repeat([]byte{'x'}, (16<<20)+1))},
	}
	{
		err := renderer.cache.RecordRender(
			t.Context(),
			cache.RenderAcceptance{
				Owner:   owner,
				Receipt: receipt,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		)
		assert.NoError(t, err)
		_, _, _, lookupErr := renderer.cache.Blob(
			t.Context(),
			cache.BlobKey{Content: frame.Key, Render: cache.RenderAccepted},
			&cache.Policy{MaxAge: cache.Forever},
			nil,
		)
		assert.Equal(t, errors.Is(lookupErr, errdef.ErrNotFound), true)
	}
}
