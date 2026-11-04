package helm

import (
	"os"
	"reflect"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplateCache_ChartDigestTrackingOptions(t *testing.T) {
	t.Parallel()
	st, hr, dir := ociRenderFixture(t)
	const digest = "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8"
	st.SetArtifact(
		manifest.NamedResource{
			Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "podinfo",
		},
		&store.SourceArtifact{
			Kind:      manifest.KindOCIRepository,
			FS:        chartFixtureFS(hr, os.DirFS(dir)),
			LocalPath: dir,
			Digest:    digest,
			Revision:  "6.15.0@" + digest,
		},
	)
	owner := cachetest.New(t, "", cache.New)
	newClient := func() (*Client, Sources) {
		cli := NewClient(owner)
		cliSources := NewSources(st)
		return cli, cliSources
	}
	cli, cliSources := newClient()
	for _, disable := range []bool{false, true, false, true, false} {
		opts := Options{DisableChartDigestTracking: disable}
		out, receipt28, err := testutil.RenderMaps(
			cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt28, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		{
			assert.NoError(t, err)
		}
		version := "6.15.0"
		if !disable {
			version += "+ff3d3e14728f"
		}
		{
			assert.FatalIf(
				t,
				!hasRenderField(out, "version", version),
				"disable=%v returned incompatible output:\n%s",
				disable,
				out,
			)
		}
		freshClient, freshSources := newClient()
		fresh, receipt29, err := testutil.RenderMaps(
			freshClient.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: freshSources}),
		)
		if err == nil {
			{
				recordErr := freshClient.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: hr.Named(), Receipt: receipt29, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		{
			assert.FatalIf(
				t,
				err != nil || !reflect.DeepEqual(fresh, out),
				"fresh disable=%v result differs: err=%v output=%s",
				disable,
				err,
				fresh,
			)
		}
	}
}
