package kustomize

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/engine/store"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderReceipt_AcceptedLifetime(t *testing.T) {
	t.Parallel()
	spec := &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}
	content := PrepareShape(&RenderRequest{Source: &store.SourceArtifact{}, Spec: spec}, nil).Content()
	t.Logf("render persistence reason=%q backing=disk", sourcecache.RenderPersistenceDisabledReason())
	renderer := newTestTreeCache(t, t.TempDir())
	released := func() <-chan struct{} {
		payload, err := testutil.EncodeRender(
			[]map[string]any{{"value": string(bytes.Repeat([]byte{'x'}, (16<<20)+1))}},
		)
		assert.NoError(t, err)
		done := make(chan struct{})
		runtime.AddCleanup(&payload[0], func(done chan struct{}) { close(done) }, done)
		receipt := renderDomain.Receipt(renderFrame{
			Key: "large-accepted-lifetime", Output: payload,
			Header: renderHeader{Reads: readSetSnapshot{Files: []localEntry{}}},
		})
		owner := manifest.NamedResource{Kind: "owner", Name: "renderer"}
		assert.NoError(
			t,
			renderer.owner.RecordRender(
				t.Context(),
				sourcecache.RenderAcceptance{
					Sources: []manifest.SourceEvidence{},
					Owner:   owner, Receipt: receipt, Records: emptyRenderInventory, Content: content,
				},
			),
		)
		assert.NoError(t, renderer.owner.Close())
		runtime.GC()
		docs, err := testutil.DecodeRender(receipt.Value().(renderFrame).Output)
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
