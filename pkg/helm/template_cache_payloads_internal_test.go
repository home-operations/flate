package helm

import (
	"context"
	"io/fs"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func testTemplateCachePayloadRecovery(t *testing.T, inputs *templateCacheIntegrationInputs, recovery struct {
	again         *[]map[string]any
	renderMapsErr *error
	r             *fs.FS
},
) {
	t.Helper()
	recoveryValues := map[string]any{"greeting": "hi"}
	recoveryKey := cache.BlobKey{
		Content: computeTemplateKey(inputs.loaded, recoveryValues, Options{}, newHR()),
		Render:  cache.RenderAccepted,
	}
	_, _, _, (*recovery.renderMapsErr) = (*inputs.cli).cache.Blob(
		t.Context(),
		recoveryKey,
		&cache.Policy{},
		func(_ context.Context, w cache.ProducerInput) (cache.ProducerResult, error) {
			writeTestRecordErr3 := writeTestRecord(
				w,
				recoveryKey,
				&renderRecord{Output: []byte("corrupt"), Analysis: inputs.loaded.analyze()},
			)
			return cache.ProducerResult{}, writeTestRecordErr3
		},
	)
	assert.NoError(t, (*recovery.renderMapsErr))

	var beforeRecovery chartDescription
	beforeRecovery, (*recovery.renderMapsErr) = (*inputs.cli).describeChart(
		t.Context(),
		newHR(),
		inputs.cliSources.chartInput(newHR()),
	)
	assert.NoError(t, (*recovery.renderMapsErr))
	var recovered []map[string]any
	var receipt23 *manifest.RenderReceipt
	recovered, receipt23, (*recovery.renderMapsErr) = testutil.RenderMaps(
		(*inputs.cli).Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: (*inputs.cliSources)}),
	)
	if (*recovery.renderMapsErr) == nil {
		{
			recordErr := (*inputs.cli).RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt23,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, (*recovery.renderMapsErr))
	assert.Diff(t, recovered, (*inputs.first))
	var afterRecovery chartDescription
	afterRecovery, (*recovery.renderMapsErr) = (*inputs.cli).describeChart(
		t.Context(),
		newHR(),
		inputs.cliSources.chartInput(newHR()),
	)
	assert.NoError(t, (*recovery.renderMapsErr))
	assert.FatalIf(
		t,
		afterRecovery.record == beforeRecovery.record || afterRecovery.record == nil,
		"%s",
		"recovered generation did not replace the typed record",
	)
	var recoveredOutput []map[string]any
	recoveredOutput, (*recovery.renderMapsErr) = testutil.DecodeRender(afterRecovery.record.Output)
	assert.NoError(t, (*recovery.renderMapsErr))
	assert.Diff(t, recoveredOutput, (*inputs.first))
	(*recovery.r), _, _, (*recovery.renderMapsErr) = (*inputs.cli).cache.Blob(
		t.Context(),
		recoveryKey,
		&cache.Policy{MaxAge: cache.Forever},
		nil,
	)
	assert.NoError(t, (*recovery.renderMapsErr))
	var replacement renderRecord
	assert.NoError(t, readTestRecord((*recovery.r), &replacement))

	var decoded []map[string]any
	decoded, (*recovery.renderMapsErr) = testutil.DecodeRender(replacement.Output)
	assert.NoError(t, (*recovery.renderMapsErr))
	assert.Diff(t, decoded, (*inputs.first))
	var receipt24 *manifest.RenderReceipt
	(*recovery.again), receipt24, (*recovery.renderMapsErr) = testutil.RenderMaps(
		(*inputs.cli).Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: (*inputs.cliSources)}),
	)
	if (*recovery.renderMapsErr) == nil {
		{
			recordErr := (*inputs.cli).RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt24,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, (*recovery.renderMapsErr))
	assert.Diff(t, (*recovery.again), (*inputs.first))
	reopened := NewClient(cachetest.New(t, (*inputs.root), cache.New))
	var receipt25 *manifest.RenderReceipt
	(*recovery.again), receipt25, (*recovery.renderMapsErr) = testutil.RenderMaps(
		reopened.Template(t.Context(), newHR(), recoveryValues, RenderOptions{Sources: (*inputs.cliSources)}),
	)
	if (*recovery.renderMapsErr) == nil {
		{
			recordErr := reopened.RecordRender(
				t.Context(),
				RenderAcceptance{
					Owner:   newHR().Named(),
					Receipt: receipt25,
					Records: emptyRenderInventory,
					Sources: []manifest.SourceEvidence{},
				},
			)
			assert.NoError(t, recordErr)
		}
	}

	assert.NoError(t, (*recovery.renderMapsErr))
	assert.Diff(t, (*recovery.again), (*inputs.first))
	t.Log("verified corrupt payload recovery, replacement, subsequent and reopened hits")
}
