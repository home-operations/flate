package helm

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

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
			t.Context(), (*(*testCaseInput).key), cache.Policy{MaxAge: cache.Forever},
			nil,
		)
		assert.NoError(t, blobErr2)
		data, blobErr2 := fs.ReadFile(reader, "content")
		assert.NoError(t, blobErr2)
		stored, blobErr2 := decodeFrame(data)
		assert.NoError(t, blobErr2)
		content := descriptor.Annotations[acceptedRenderAnnotation]
		var actual acceptedRender
		assert.NoError(
			t,
			json.Unmarshal(data[12+int(binary.BigEndian.Uint32(data[8:12]))+stored.header.Size:], &actual),
		)
		assert.FatalIf(
			t,
			content == "" || content != manifest.Fingerprint(actual),
			"%s",
			"stored accepted-frame key does not identify the actual accepted frame",
		)
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
	initialKey := storedKey()
	_, pending, (*(*testCaseInput).err) = testutil.RenderMaps(
		renderer.Template(
			t.Context(),
			(*(*testCaseInput).hr),
			nil,
			RenderOptions{Sources: (*testCaseInput).input.Sources},
		),
	)
	assert.NoError(t, (*(*testCaseInput).err))
	resident := pending.Value().(renderFrame)
	assert.FatalIf(
		t,
		resident.accepted == nil || resident.acceptedKey == "",
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
		cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(reader, "content")
	assert.NoError(t, err)
	_, _, _, err = renderer.cache.Blob(
		t.Context(), (*(*testCaseInput).key), cache.Policy{MaxAge: 0},
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
			RenderOptions{Sources: (*testCaseInput).input.Sources},
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
