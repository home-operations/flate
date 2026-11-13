package helm

import (
	"context"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

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
	assert.FatalIf(
		t,
		sources.calls.Load() != 0 || counted.opens.Load() != 0,
		"disabled history prepared chart: resolutions=%d, opens=%d",
		sources.calls.Load(),
		counted.opens.Load(),
	)
	_, err := fresh.describeChart(t.Context(), (*testCaseInput.hr), observed.Sources.chartInput((*testCaseInput.hr)))
	assert.NoError(t, err)

	assert.FatalIf(
		t,
		sources.calls.Load() == 0 || counted.opens.Load() == 0 || len(fresh.chartCache) == 0,
		"%s",
		"chart description was not primed",
	)

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
	assert.FatalIf(
		t,
		sources.calls.Load() != 0 || counted.opens.Load() != 0,
		"disabled resident history prepared chart: resolutions=%d, opens=%d",
		sources.calls.Load(),
		counted.opens.Load(),
	)
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
