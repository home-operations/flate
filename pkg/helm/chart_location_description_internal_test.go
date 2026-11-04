package helm

import (
	"errors"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestDescription_CurrentChartLocation(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"first/Chart.yaml":  {Data: []byte("apiVersion: v2\nname: demo\nversion: invalid\n")},
		"second/Chart.yaml": {Data: []byte("apiVersion: v2\nname: demo\nversion: invalid\n")},
	}
	st := store.New()
	hr := minimalHR()
	hr.Chart = manifest.HelmChart{
		Name: "first",
		Source: manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Name:      "repo",
			Namespace: "default",
		},
	}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input})
	cli := NewClient(cachetest.New(t, "", cache.New))
	cliSources := NewSources(st)
	for _, location := range []string{"first", "second"} {
		request := hr.Clone()
		request.Chart.Name = location
		_, receipt12, err := testutil.RenderMaps(
			cli.Template(t.Context(), request, nil, RenderOptions{Options: Options{}, Sources: cliSources}),
		)
		if err == nil {
			{
				recordErr := cli.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: request.Named(), Receipt: receipt12, Records: emptyRenderInventory},
				)
				assert.NoError(t, recordErr)
			}
		}

		assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
			!strings.Contains(err.Error(), "load chart "+location+":"), "location %q: got %v", location, err)
	}
}
