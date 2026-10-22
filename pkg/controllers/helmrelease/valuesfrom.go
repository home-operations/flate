package helmrelease

import (
	"fmt"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

func (c *Controller) recordOmittedValues(id manifest.NamedResource, refs []manifest.NamedResource) {
	for _, ref := range refs {
		c.Store.AddWarning(id, manifest.Warning{
			Resource: id,
			Category: manifest.WarnOmittedValues,
			Message:  fmt.Sprintf("required valuesFrom %s omitted from offline render", ref),
		})
	}
}

func applyValuesFrom(hr *manifest.HelmRelease, positions []int) (*manifest.HelmRelease, []manifest.NamedResource) {
	if len(positions) == 0 {
		return hr, nil
	}
	out := hr.Clone()
	out.ValuesFrom = nil
	var omitted []manifest.NamedResource
	for ref := range manifest.References(hr) {
		if ref.Use != "valuesFrom" {
			continue
		}
		if slices.Contains(positions, ref.Position) {
			omitted = append(omitted, ref.NamedResource)
		} else {
			out.ValuesFrom = append(out.ValuesFrom, hr.ValuesFrom[ref.Position])
		}
	}
	slices.SortFunc(omitted, manifest.NamedResource.Compare)
	return out, slices.Compact(omitted)
}
