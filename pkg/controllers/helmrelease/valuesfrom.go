package helmrelease

import (
	"context"
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
)

func (c *execution) resolvePreRenderValuesFrom(ctx context.Context, hr *manifest.HelmRelease, level manifest.DrainLevel) error {
	c.SetPendingUnlessReady(hr.Named(), "awaiting pre-render references")
	for ref := range manifest.References(hr) {
		if (ref.Use == "chart" || ref.Use == "source") && ref.Kind == manifest.KindHelmChart && hr.Chart.Version == "" {
			c.run.Reads.Require(ref.DependencyRef)
		}
	}
	var inputs []depwait.DataDependency
	for ref := range manifest.References(hr) {
		if ref.Use != "valuesFrom" {
			continue
		}
		if !ref.NamedResource.KindInfo().Data {
			continue
		}
		c.run.PromoteData(ctx, ref.NamedResource)
		input := c.data.Lookup(ref.NamedResource)
		inputs = append(inputs, depwait.DataDependency{DependencyRef: ref.DependencyRef, Input: input, Optional: ref.Optional, AllowMissing: c.allowMissingSecrets})
	}
	return c.run.AwaitData(ctx, base.DataWait{Inputs: inputs, Resume: func(ctx context.Context, level manifest.DrainLevel, omitted []manifest.NamedResource) error {
		if len(omitted) != 0 {
			filtered := make([]manifest.ValuesReference, 0, len(hr.ValuesFrom))
			for edge := range manifest.References(hr) {
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
