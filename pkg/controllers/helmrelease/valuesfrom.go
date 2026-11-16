package helmrelease

import (
	"context"

	"github.com/home-operations/flate/pkg/controllers/base"
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
	return c.run.AwaitData(ctx, base.DataWait{
		Inputs: inputs,
		Resume: func(ctx context.Context, level manifest.DrainLevel, results []values.Result) error {
			return c.render(ctx, hr, level, results)
		},
	}, level)
}
