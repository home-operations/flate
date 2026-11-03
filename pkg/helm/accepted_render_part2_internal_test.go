package helm

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_AcceptedHistory(t *testing.T) {
	t.Parallel()
	chartDir := t.TempDir()
	testutil.WriteFile(t, chartDir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		chartDir,
		"mychart/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: original}\n",
	)
	root := t.TempDir()
	owner := cachetest.New(t, root, cache.New)
	client := NewClient(owner)
	hr := newHR()
	hr.TargetNamespace = "runtime"
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", chartDir)}
	t.Run("disabled preparation", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDisabledPreparation(
			t,
			&testTemplateAcceptedHistoryDisabledPreparationInputs{hr: &hr, input: &input},
		)
	},
	)
	docs, receipt, err := testutil.RenderMaps(client.Template(t.Context(), hr, nil, input))
	assert.NoError(t, err)
	{
		assert.FatalIf(t, receipt == nil, "%s", "successful raw template supplied no receipt")
	}
	frame := receipt.Value().(renderFrame)
	frame.header.Binding = manifest.Fingerprint(frame.binding)
	frame.header.Analysis = (&chartDescription{chart: frame.chart, analysis: frame.header.Analysis}).analyze()
	frame.encoded, err = encodeFrame(frame.header, frame.output)
	assert.NoError(t, err)
	frame.raw = manifest.Fingerprint(frame.encoded[12 : 12+int(binary.BigEndian.Uint32(frame.encoded[8:12]))])
	original := bytes.Clone(frame.output)
	docs[0]["metadata"].(map[string]any)["name"] = "mutated"
	assert.Equal(t, bytes.Equal(receipt.Value().(renderFrame).output, original), true)
	raw, err := testutil.DecodeRender(frame.output)
	assert.NoError(t, err)
	assert.Equal(t, hasRenderField(raw, "name", "original"), true)
	key := cache.BlobKey{Content: frame.header.Key, Render: cache.RenderAccepted}
	{
		_, _, _, blobErr := owner.Blob(
			t.Context(),
			key,
			cache.Policy{MaxAge: cache.Forever},
			nil,
		)
		assert.FatalIf(t, !errors.Is(
			blobErr,
			errdef.ErrNotFound,
		), "standalone template wrote durable output: %v", blobErr)
	}
	assert.NoError(
		t,
		client.RecordRender(
			t.Context(),
			RenderAcceptance{Owner: hr.Named(), Receipt: receipt, Sources: []manifest.SourceEvidence{}, Records: nil},
		),
	)
	assert.NoError(
		t,
		client.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   hr.Named(),
				Receipt: nil,
				Sources: []manifest.SourceEvidence{},
				Records: emptyRenderInventory,
			},
		),
	)
	input.Admit = func(string) (bool, error) { t.Fatal("history called Admit"); return false, nil }
	{
		previous, complete := client.PreviousRender(t.Context(), hr, nil, input)
		assert.FatalIf(t, complete || previous != nil, "%s", "unaccepted template supplied history")
	}
	t.Run("durable", func(t *testing.T) {
		t.Parallel()
		testTemplateAcceptedHistoryDurable(
			t,
			&testTemplateAcceptedHistoryDurableInputs{
				root:    &root,
				client:  &client,
				hr:      &hr,
				input:   &input,
				receipt: &receipt,
				err:     &err,
				frame:   &frame,
				raw:     &raw,
				key:     &key,
			},
		)
	},
	)
}

type testTemplateAcceptedHistoryDisabledPreparationInputs struct {
	hr    **manifest.HelmRelease
	input *RenderOptions
}

func testTemplateAcceptedHistoryDisabledPreparation(
	t *testing.T,
	testCaseInput *testTemplateAcceptedHistoryDisabledPreparationInputs,
) {
	t.Helper()

	artifact := *testCaseInput.input.Sources.LocalSourceArtifact((*testCaseInput.hr).ChartSource(), store.ArtifactContent)
	counted := &historyCountingFS{FS: artifact.FS}
	artifact.FS = counted
	objects := store.New()
	objects.SetArtifact((*testCaseInput.hr).ChartSource(), &artifact)
	sources := &historyCountingSources{sourceReader: objects}
	observed := (*testCaseInput.input)
	observed.Sources = NewSources(sources)
	fresh := NewClient(cachetest.New(t, "", cache.New))
	{
		previous, complete := fresh.PreviousRender(
			t.Context(),
			(*testCaseInput.hr),
			nil,
			observed,
		)
		assert.FatalIf(t, previous != nil ||
			complete, "%s", "disabled renderer supplied history")
	}
	if sources.calls.Load() != 0 || counted.opens.Load() != 0 {
		t.Fatalf(
			"disabled history prepared chart: resolutions=%d, opens=%d",
			sources.calls.Load(),
			counted.opens.Load(),
		)
	}
	_, err := fresh.describeChart(t.Context(), (*testCaseInput.hr), observed.Sources)
	assert.NoError(t, err)
	{
		assert.FatalIf(
			t,
			sources.calls.Load() == 0 || counted.opens.Load() == 0 || len(fresh.chartCache) == 0,
			"%s",
			"chart description was not primed",
		)
	}
	sources.calls.Store(0)
	counted.opens.Store(0)
	{
		previous, complete := fresh.PreviousRender(
			t.Context(),
			(*testCaseInput.hr),
			nil,
			observed,
		)
		assert.FatalIf(t, previous != nil ||
			complete, "%s", "disabled resident renderer supplied history")
	}
	if sources.calls.Load() != 0 || counted.opens.Load() != 0 {
		t.Fatalf(
			"disabled resident history prepared chart: resolutions=%d, opens=%d",
			sources.calls.Load(),
			counted.opens.Load(),
		)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	{
		previous, complete := fresh.PreviousRender(
			canceled,
			(*testCaseInput.hr),
			nil,
			observed,
		)
		assert.FatalIf(t, previous != nil ||
			complete, "%s", "canceled disabled renderer supplied history")
	}
	assert.Equal(t, sources.calls.Load(), 0)
	assert.Equal(t, counted.opens.Load(), 0)
}

type testTemplateAcceptedHistoryDurableInputs struct {
	root    *string
	client  **Client
	hr      **manifest.HelmRelease
	input   *RenderOptions
	receipt **manifest.RenderReceipt
	err     *error
	frame   *renderFrame
	raw     *[]map[string]any
	key     *cache.BlobKey
}
