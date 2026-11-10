package helmrelease

import (
	"context"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *execution) resolvePreRenderValuesFrom(
	ctx context.Context,
	hr *manifest.HelmRelease,
	level manifest.DrainLevel,
) error {
	inputs, err := c.run.DataInputs(ctx, hr, c.data)
	if err != nil {
		return err
	}
	return c.run.AwaitData(
		ctx,
		base.DataWait{
			Inputs: inputs,
			Resume: func(ctx context.Context, level manifest.DrainLevel, omitted []manifest.NamedResource) error {
				omitted = requiredValuesOmissions(hr, omitted)
				c.recordOmittedValues(hr, omitted)
				hr = omitValues(hr, omitted)
				return c.render(ctx, hr, level, omitted)
			},
		},
		level,
	)
}

func (c *execution) recordOmittedValues(hr *manifest.HelmRelease, refs []manifest.NamedResource) {
	for _, ref := range refs {
		c.run.Reads.AddWarning(
			manifest.Warning{
				Resource: hr.Named(),
				Category: manifest.WarnOmittedValues,
				Message:  fmt.Sprintf("required valuesFrom %s omitted from offline render", ref),
			},
		)
	}
}

func omitValues(hr *manifest.HelmRelease, omitted []manifest.NamedResource) *manifest.HelmRelease {
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
	return hr
}

func requiredValuesOmissions(hr *manifest.HelmRelease, refs []manifest.NamedResource) []manifest.NamedResource {
	required := refs[:0]
	for _, ref := range refs {
		if slices.ContainsFunc(hr.ValuesFrom, func(input manifest.ValuesReference) bool {
			return !input.Optional && input.Kind == ref.Kind && input.Name == ref.Name
		}) {
			required = append(required, ref)
		}
	}
	return required
}
