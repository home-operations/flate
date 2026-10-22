package helmrelease

import (
	"context"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *execution) resolvePreRenderValuesFrom(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	inputs := c.run.DataInputs(ctx, hr, c.data)
	return c.run.AwaitData(ctx, base.DataWait{Inputs: inputs, Resume: func(ctx context.Context, level manifest.DrainLevel, omitted []manifest.NamedResource) error {
		if len(omitted) != 0 {
			filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
			for edge := range manifest.References(hr, manifest.ReferenceRelations) {
				if edge.Use == "valuesFrom" && (edge.Optional || !slices.Contains(omitted, edge.NamedResource)) {
					filtered = append(filtered, hr.ValuesFrom[edge.Position])
				}
			}
			hr = hr.Clone()
			hr.ValuesFrom = filtered
		}
		return c.render(ctx, hr, level, omitted)
	}}, level)
}

func (c *execution) recordOmittedValues(id manifest.NamedResource, refs []manifest.NamedResource) {
	for _, ref := range refs {
		c.run.Reads.AddWarning(id, manifest.Warning{Resource: id, Category: manifest.WarnOmittedValues, Message: fmt.Sprintf("required valuesFrom %s omitted from offline render", ref)})
	}
}
