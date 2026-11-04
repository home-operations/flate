package helm

import (
	"context"
	"io/fs"
	"maps"
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func testTemplateCachePersistedIntegration(t *testing.T, inputs *templateCacheIntegrationInputs) {
	t.Helper()

	t.Log("verified persisted completed-output hit")
	assert.FatalIf(
		t,
		(*inputs.err) != nil || !reflect.DeepEqual((*inputs.out), (*inputs.first)),
		"completed hit: output=%q, error=%v",
		(*inputs.out),
		(*inputs.err),
	)
	(*inputs.out)[0]["data"].(map[string]any)["greeting"] = "mutated"
	(*inputs.out)[0]["extra"].(map[string]any)["sequence"].([]any)[0].(map[string]any)["value"] = "mutated"
	again, receipt18, renderMapsErr := testutil.RenderMaps(
		(*inputs.cli).Template(t.Context(), newHR(), (*inputs.values), RenderOptions{Sources: (*inputs.cliSources)}),
	)
	if renderMapsErr == nil {
		{
			recordErr := (*inputs.cli).RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt18, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, renderMapsErr)
	assert.Diff(t, again, (*inputs.first))
	fresh := NewClient(cachetest.New(t, (*inputs.root), cache.New))
	var receipt19 *manifest.RenderReceipt
	again, receipt19, renderMapsErr = testutil.RenderMaps(
		fresh.Template(t.Context(), newHR(), (*inputs.values), RenderOptions{Sources: (*inputs.cliSources)}),
	)
	if renderMapsErr == nil {
		{
			recordErr := fresh.RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt19, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, renderMapsErr)
	assert.Diff(t, again, (*inputs.first))
	initial, renderMapsErr := (*inputs.cli).describeChart(t.Context(), newHR(), (*inputs.cliSources))
	assert.NoError(t, renderMapsErr)
	var receipt20 *manifest.RenderReceipt
	_, receipt20, renderMapsErr = testutil.RenderMaps(
		(*inputs.cli).Template(
			t.Context(),
			newHR(),
			(*inputs.values),
			RenderOptions{Options: Options{}, Sources: (*inputs.cliSources)},
		),
	)
	if renderMapsErr == nil {
		{
			recordErr := (*inputs.cli).RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt20, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, renderMapsErr)
	repeated, renderMapsErr := (*inputs.cli).describeChart(t.Context(), newHR(), (*inputs.cliSources))
	assert.NoError(t, renderMapsErr)
	assert.FatalIf(
		t,
		initial.record == nil || repeated.record != initial.record,
		"%s",
		"repeated hit did not share its typed record",
	)
	alternate := maps.Clone((*inputs.values))
	alternate["variant"] = true
	inputs.loaded.values, inputs.loaded.valuesErr = (*inputs.cli).observeValuesFiles(
		t.Context(), inputs.loaded, newHR().ChartValuesFiles,
		newHR().IgnoreMissingValuesFiles,
	)
	assert.NoError(t, inputs.loaded.valuesErr)
	alternateKey := cache.BlobKey{
		Content: computeTemplateKey(inputs.loaded, alternate, Options{}, newHR()),
		Render:  cache.RenderAccepted,
	}
	r, _, _, renderMapsErr := (*inputs.cli).cache.Blob(
		t.Context(),
		alternateKey,
		&cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr2 := writeTestRecord(w, alternateKey, initial.record)
			return cache.ProducerResult{}, writeTestRecordErr2
		},
	)
	assert.NoError(t, renderMapsErr)
	assert.Equal(t, cache.AcquisitionIdentity(r) != initial.record.identity, true)

	var receipt21 *manifest.RenderReceipt
	_, receipt21, renderMapsErr = testutil.RenderMaps(
		(*inputs.cli).Template(
			t.Context(),
			newHR(),
			alternate,
			RenderOptions{Options: Options{}, Sources: (*inputs.cliSources)},
		),
	)
	if renderMapsErr == nil {
		{
			recordErr := (*inputs.cli).RecordRender(
				t.Context(),
				RenderAcceptance{Owner: newHR().Named(), Receipt: receipt21, Records: emptyRenderInventory},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, renderMapsErr)
	distinct, renderMapsErr := (*inputs.cli).describeChart(t.Context(), newHR(), (*inputs.cliSources))
	assert.NoError(t, renderMapsErr)
	assert.FatalIf(
		t,
		distinct.record == initial.record || distinct.record.key != alternateKey.Content,
		"%s",
		"distinct release key reused a typed record",
	)
	testTemplateCacheSameKeyReplacements(t, inputs, &alternateKey, alternate)

	testTemplateCachePayloadRecovery(t, inputs, struct {
		again         *[]map[string]any
		renderMapsErr *error
		r             *fs.FS
	}{again: &again, renderMapsErr: &renderMapsErr, r: &r})
}
