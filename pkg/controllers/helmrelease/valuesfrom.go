package helmrelease

import (
	"context"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/values"
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
			Resume: func(ctx context.Context, level manifest.DrainLevel, omitted []depwait.DataDependency) error {
				omitted = requiredValuesOmissions(omitted)
				c.recordOmittedValues(hr, omitted)
				hr = omitValues(hr, omitted)
				return c.render(ctx, hr, level, omissionIDs(omitted))
			},
		},
		level,
	)
}

func (c *execution) recordOmittedValues(hr *manifest.HelmRelease, refs []depwait.DataDependency) {
	seen := map[string]bool{}
	for i := range refs {
		ref := &refs[i]
		message := fmt.Sprintf("required valuesFrom %s omitted from offline render", ref.NamedResource)
		if ref.Result.State() != values.Absent {
			message += ": " + ref.Result.Cause()
		}
		if seen[message] {
			continue
		}
		seen[message] = true
		c.run.Reads.AddWarning(
			manifest.Warning{
				Resource: hr.Named(),
				Category: manifest.WarnOmittedValues,
				Message:  message,
			},
		)
	}
}

func omitValues(hr *manifest.HelmRelease, omitted []depwait.DataDependency) *manifest.HelmRelease {
	if len(omitted) != 0 {
		filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
		for edge := range manifest.References(hr, manifest.ReferenceRelations) {
			if edge.Use == "valuesFrom" &&
				(edge.Optional || !slices.ContainsFunc(omitted, func(input depwait.DataDependency) bool { return input.Position == edge.Position })) {
				filtered = append(filtered, hr.ValuesFrom[edge.Position])
			}
		}
		hr = hr.Clone()
		hr.ValuesFrom = filtered
	}
	return hr
}

func requiredValuesOmissions(refs []depwait.DataDependency) []depwait.DataDependency {
	required := refs[:0]
	for i := range refs {
		ref := &refs[i]
		if ref.Use == "valuesFrom" && !ref.Optional {
			required = append(required, *ref)
		}
	}
	return required
}

func omissionIDs(refs []depwait.DataDependency) []manifest.NamedResource {
	var ids []manifest.NamedResource
	if len(refs) != 0 {
		ids = make([]manifest.NamedResource, 0, len(refs))
	}
	for i := range refs {
		ref := &refs[i]
		ids = append(ids, ref.NamedResource)
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return slices.Compact(ids)
}
