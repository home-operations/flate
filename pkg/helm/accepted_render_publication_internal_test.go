package helm

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func testTemplateAcceptedHistoryDurablePublication(
	t *testing.T,
	testCaseInput **testTemplateAcceptedHistoryDurableInputs,
	records map[manifest.NamedResource]manifest.DiscoveryRecord,
) {
	t.Helper()

	directory := t.TempDir()
	pending := (*(*testCaseInput).receipt)
	accepting := (*(*testCaseInput).hr).Named()
	open := func() *Client {
		renderer := NewClient(cachetest.New(t, directory, cache.New))
		return renderer
	}
	renderer := open()
	indexPath := filepath.Join(directory, "oci", "index.json")
	storedKey := func() string {
		t.Helper()
		reader, descriptor, _, blobErr2 := renderer.cache.Blob(
			t.Context(),
			(*(*testCaseInput).key),
			&cache.Policy{MaxAge: cache.Forever},
			nil,
		)
		assert.NoError(t, blobErr2)
		var stored renderRecord
		assert.NoError(t, readTestRecord(t.Context(), reader, &stored))
		content := descriptor.Annotations[acceptedRenderAnnotation]

		history, complete := stored.frame.stored.History(accepting, pending.Value().(renderFrame).content)
		assert.Equal(t, complete, true)
		storedPublication, blobErr2 := stored.frame.stored.PreparePublication(t.Context(), cache.FramePublicationInput{
			Owner: accepting, Content: pending.Value().(renderFrame).content, Records: history, Complete: true,
		})
		assert.NoError(t, blobErr2)
		assert.FatalIf(
			t,
			content == "" || storedPublication == nil,
			"%s",
			"stored accepted-frame key does not identify the actual accepted frame",
		)
		assert.Equal(t, content, storedPublication.Identity())
		return content
	}
	unchanged := func() func() {
		t.Helper()
		content, readFileErr := os.ReadFile(indexPath)
		assert.NoError(t, readFileErr)
		stamp := time.Unix(1, 0)
		assert.NoError(t, os.Chtimes(indexPath, stamp, stamp))
		before, readFileErr := os.Stat(indexPath)
		assert.NoError(t, readFileErr)
		return func() {
			t.Helper()
			after, err := os.Stat(indexPath)
			assert.NoError(t, err)
			current, err := os.ReadFile(indexPath)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				!bytes.Equal(current, content) || !after.ModTime().Equal(before.ModTime()),
				"%s",
				"unchanged accepted frame republished the OCI index",
			)
		}
	}
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	persistent := cache.RenderPersistenceDisabledReason() == ""
	if !persistent {
		input := RenderOptions{Options: (*testCaseInput).input.Options, Sources: (*testCaseInput).input.Sources}
		docs, receipt, err := testutil.RenderMaps(renderer.Template(t.Context(), *(*testCaseInput).hr, nil, input))
		assert.NoError(t, err)
		assert.Diff(t, docs, *(*testCaseInput).raw)
		assert.NoError(t, renderer.cache.Close())
		renderer = open()
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		foreign := accepting
		foreign.Namespace = "foreign"
		for _, state := range []struct {
			canceled bool
			owner    manifest.NamedResource
			receipt  *manifest.RenderReceipt
			records  map[manifest.NamedResource]manifest.DiscoveryRecord
			want     error
		}{
			{false, accepting, receipt, records, nil},
			{false, accepting, pending, nil, nil},
			{false, foreign, pending, records, nil},
			{false, accepting, receipt, records, nil},
			{true, accepting, pending, records, context.Canceled},
			{false, accepting, manifest.NewRenderReceipt("invalid", nil), records, manifest.ErrFlux},
		} {
			ctx := t.Context()
			if state.canceled {
				ctx = canceled
			}
			err := renderer.RecordRender(ctx, RenderAcceptance{
				Owner:   state.owner,
				Receipt: state.receipt,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(state.records),
			})
			assert.Equal(t, errors.Is(err, state.want), true)
			reader, _, _, lookupErr := renderer.cache.Blob(
				t.Context(),
				*(*testCaseInput).key,
				&cache.Policy{MaxAge: cache.Forever},
				nil,
			)
			assert.Equal(t, errors.Is(lookupErr, errdef.ErrNotFound), true)
			assert.Equal(t, reader == nil, true)
			previous, complete := renderer.PreviousRender(
				t.Context(),
				*(*testCaseInput).hr,
				nil,
				*(*testCaseInput).input,
			)
			assert.Equal(t, previous == nil, true)
			assert.Equal(t, complete, false)
		}
		assert.NoError(t, renderer.cache.Close())
		assert.NoError(t, renderer.RecordRender(t.Context(), RenderAcceptance{
			Owner: accepting, Receipt: pending, Sources: []manifest.SourceEvidence{}, Records: emptyRenderInventory,
		}))
		return
	}
	initialKey := storedKey()
	_, pending, (*(*testCaseInput).err) = testutil.RenderMaps(
		renderer.Template(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			RenderOptions{Options: (*testCaseInput).input.Options, Sources: (*testCaseInput).input.Sources},
		),
	)
	assert.NoError(t, (*(*testCaseInput).err))
	resident := pending.Value().(renderFrame)
	assert.FatalIf(
		t,
		resident.stored == nil,
		"%s",
		"warm receipt did not carry decoded accepted evidence",
	)
	check := unchanged()
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	check()
	reader, _, _, err := renderer.cache.Blob(
		t.Context(),
		(*(*testCaseInput).key),
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(reader, "content")
	assert.NoError(t, err)
	_, _, _, err = renderer.cache.Blob(
		t.Context(),
		(*(*testCaseInput).key),
		&cache.Policy{MaxAge: 0},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			_, writeErr2 := w.Write(data)
			return cache.ProducerResult{}, writeErr2
		},
	)
	assert.NoError(t, err)

	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	assert.Equal(t, storedKey(), initialKey)
	assert.NoError(t, renderer.cache.Close())
	renderer = open()
	assert.Equal(t, storedKey(), initialKey)
	check = unchanged()
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	check()
	initialIndex, err := os.ReadFile(indexPath)
	assert.NoError(t, err)
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			},
		),
	)
	changedIndex, err := os.ReadFile(indexPath)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		storedKey() == initialKey,
		"%s",
		"changed accepted frame retained the previous content key",
	)
	assert.FatalIf(t, bytes.Equal(initialIndex, changedIndex), "%s", "changed accepted frame was not published")
	previousRenderPrevious3, previousRenderErr := renderer.PreviousRender(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		(*(*testCaseInput).input),
	)
	assert.Equal(t, previousRenderErr && previousRenderPrevious3 != nil, true)
	for range previousRenderPrevious3 {
		t.Fatal("changed complete-empty frame retained children")
	}
	_, borrowedReceipt, err := testutil.RenderMaps(
		renderer.Template(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			RenderOptions{Options: (*testCaseInput).input.Options, Sources: (*testCaseInput).input.Sources},
		),
	)
	assert.NoError(t, err)
	foreign := accepting
	foreign.Namespace = "foreign"
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   foreign,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	{
		_, previousRenderComplete7 := renderer.PreviousRender(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			(*(*testCaseInput).input),
		)
		assert.FatalIf(t, previousRenderComplete7, "%s", "foreign overwrite retained the previous owner's history")
	}
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: borrowedReceipt,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	assert.Equal(t, storedKey(), initialKey)
	previousRenderPrevious3, previousRenderErr = renderer.PreviousRender(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		(*(*testCaseInput).input),
	)
	assert.Equal(t, previousRenderErr, true)
	got := maps.Collect(previousRenderPrevious3)
	assert.Diff(t, got, records)
	check = unchanged()
	assert.NoError(
		t,
		renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		),
	)
	check()
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	check = unchanged()
	{
		err := renderer.RecordRender(
			canceled,
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "canceled same-frame publication: %v", err)
	}
	check()
	check = unchanged()
	{
		err := renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: manifest.NewRenderReceipt("invalid", nil),
				Sources: []manifest.SourceEvidence{},
				Records: maps.All(records),
			},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrFlux,
		), "failed receipt publication: %v", err)
	}
	check()
	check = unchanged()
	assert.NoError(t, renderer.cache.Close())
	{
		err := renderer.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   accepting,
				Receipt: pending,
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrFlux,
		), "closed owner lookup: %v", err)
	}
	check()
	renderer = open()
	previousRenderPrevious3, previousRenderErr = renderer.PreviousRender(
		t.Context(),
		(*(*testCaseInput).hr),
		nil,
		(*(*testCaseInput).input),
	)
	assert.Equal(t, previousRenderErr, true)
	got = maps.Collect(previousRenderPrevious3)
	assert.Diff(t, got, records)
}
