package helm

import (
	"context"
	"encoding/json"
	"iter"

	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

func (r *templateRequest) entryPrecondition(ctx context.Context, c *Client) (bool, error) {
	hr, loaded := r.release, &r.loaded
	if err := c.decodeChart(ctx, loaded, &RenderOptions{Work: r.input.Work}); err != nil {
		return true, err
	}
	if len(hr.ChartValuesFiles) > 0 && loaded.values.inputs == nil && loaded.valuesErr == nil {
		if loaded.values.names == nil {
			loaded.values, loaded.valuesErr = c.observeValuesFiles(
				ctx, loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles,
			)
		}
		if loaded.valuesErr == nil {
			loaded.valuesErr = loaded.values.observe(ctx, loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		}
	}
	if loaded.valuesErr != nil || r.capabilityErr != nil {
		return true, nil //nolint:nilerr // Invalid inputs MUST retain ordinary error precedence.
	}
	if hr.TracksDigest() {
		if _, err := ociChartVersion(loaded.chart.Metadata.Version, loaded.artifact.Revision); err != nil {
			return true, nil //nolint:nilerr // Rendering owns the ordered source-version diagnostic.
		}
	}
	complete := true
	_, possible := manifest.ProjectProducer(hr, entryApplicationContent(r.values, loaded, &complete))
	return possible || !complete, nil
}

func entryApplicationContent(values map[string]any, loaded *chartDescription, complete *bool) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if !yieldApplicationJSON(values, yield, complete) {
			return
		}
		for _, file := range loaded.values.inputs {
			if !yield(file.Data) {
				return
			}
			if file.Defaults != nil && !yieldApplicationJSON(file.Defaults, yield, complete) {
				return
			}
		}
		for data := range chartApplicationContent(loaded.chart, complete) {
			if !yield(data) {
				return
			}
		}
	}
}

func chartApplicationContent(ch *chart.Chart, complete *bool) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if !yieldApplicationFiles(ch.Templates, yield) || !yieldApplicationFiles(ch.Files, yield) {
			return
		}
		if !yieldApplicationJSON(ch.Values, yield, complete) || !yieldApplicationJSON(ch.Metadata, yield, complete) ||
			!yieldApplicationJSON(ch.Lock, yield, complete) || !yield(ch.Schema) {
			return
		}

		for _, dependency := range ch.Dependencies() {
			for data := range chartApplicationContent(dependency, complete) {
				if !yield(data) {
					return
				}
			}
		}
	}
}

func yieldApplicationFiles(files []*common.File, yield func([]byte) bool) bool {
	for _, file := range files {
		if file != nil && !yield(file.Data) {
			return false
		}
	}
	return true
}

func yieldApplicationJSON(value any, yield func([]byte) bool, complete *bool) bool {
	data, err := json.Marshal(value)
	if err != nil {
		*complete = false
		return false
	}
	return yield(data)
}
