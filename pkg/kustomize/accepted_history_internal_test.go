package kustomize

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderReceipt_AcceptedLifetime(t *testing.T) {
	t.Parallel()
	t.Logf("render persistence reason=%q backing=disk", sourcecache.RenderPersistenceDisabledReason())
	renderer := newTestTreeCache(t, t.TempDir())
	released := func() <-chan struct{} {
		payload, err := testutil.EncodeRender(
			[]map[string]any{{"value": string(bytes.Repeat([]byte{'x'}, (16<<20)+1))}},
		)
		assert.NoError(t, err)
		done := make(chan struct{})
		runtime.AddCleanup(&payload[0], func(done chan struct{}) { close(done) }, done)
		receipt := manifest.NewRenderReceipt(
			renderFrame{header: renderHeader{Key: "large-accepted-lifetime"}, output: payload},
			equalRenderFrame,
		)
		owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
		assert.NoError(
			t,
			renderer.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: owner, Receipt: receipt, Records: emptyRenderInventory, Content: acceptedInput},
			),
		)
		renderer.Close()
		assert.NoError(t, renderer.owner.Close())
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
