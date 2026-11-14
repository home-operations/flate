package helm

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestRenderReceipt_AcceptedLifetime(t *testing.T) {
	t.Parallel()
	t.Logf("render persistence reason=%q backing=disk", cache.RenderPersistenceDisabledReason())
	renderer := NewClient(cachetest.New(t, t.TempDir(), cache.New))
	released := func() <-chan struct{} {
		payload, err := testutil.EncodeRender(
			[]map[string]any{{"value": string(bytes.Repeat([]byte{'x'}, (16<<20)+1))}},
		)
		assert.NoError(t, err)
		done := make(chan struct{})
		runtime.AddCleanup(&payload[0], func(done chan struct{}) { close(done) }, done)
		receipt := manifest.NewRenderReceipt(
			renderFrame{
				header: renderHeader{Key: "large-accepted-lifetime", Analysis: &chartAnalysis{Version: "0.1.0"}},
				output: payload,
			},
			equalRenderFrame,
		)
		owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
		assert.NoError(
			t,
			renderer.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   owner,
					Receipt: receipt,
					Sources: []manifest.SourceEvidence{},
					Records: emptyRenderInventory,
				},
			),
		)
		assert.NoError(t, renderer.cache.Close())
		runtime.GC()
		docs, err := testutil.DecodeRender(receipt.Value().(renderFrame).output)
		assert.NoError(t, err)
		assert.Equal(t, len(docs[0]["value"].(string)), (16<<20)+1)
		runtime.KeepAlive(receipt)
		return done
	}()
	runtime.GC()
	wait, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	select {
	case <-released:
	case <-wait.Done():
		t.Fatal("closed renderer retained a released large receipt payload")
	}
	runtime.KeepAlive(renderer)
}
