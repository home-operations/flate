package helm

import (
	"os"
	"strconv"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSources_SelectedRevision(t *testing.T) {
	t.Parallel()
	for _, tracked := range []bool{false, true} {
		t.Run(strconv.FormatBool(tracked), func(t *testing.T) {
			t.Parallel()
			st, hr, dir := ociRenderFixture(t)
			hr = hr.Clone()
			if !tracked {
				hr.ChartRef = nil
			}
			owner := cachetest.New(t, "", cache.New)
			client := NewClient(owner)
			input := acquireChartFS(t, owner, chartFixtureFS(hr, os.DirFS(dir)))
			artifact := &store.SourceArtifact{
				Kind:     manifest.KindOCIRepository,
				FS:       input,
				Key:      "chart",
				Content:  cache.AcquisitionIdentity(input),
				Digest:   "sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
				Revision: "6.15.0@sha256:ff3d3e14728f75476ed4d43c14f80d52d81d36bc16906843463d464c6146f0d8",
			}
			st.SetArtifact(hr.ChartSource(), artifact)
			var reads store.ReadSet
			reads.Init(st, hr.Named(), nil)
			defer reads.Close()
			_, _, err := testutil.RenderMaps(
				client.Template(t.Context(), hr, nil, RenderOptions{Sources: NewSources(&reads)}),
			)
			assert.NoError(t, err)
			next := *artifact
			next.Revision = "6.16.0"
			next.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
			st.SetArtifact(hr.ChartSource(), &next)
			assert.Equal(t, reads.Valid(), !tracked)
			var fresh store.ReadSet
			fresh.Init(st, hr.Named(), nil)
			defer fresh.Close()
			loaded, err := client.describeChart(t.Context(), hr, NewSources(&fresh).chartInput(hr))
			assert.NoError(t, err)
			assert.Equal(t, loaded.artifact.Revision, next.Revision)
			later := next
			later.Revision = "6.17.0"
			st.SetArtifact(hr.ChartSource(), &later)
			assert.Equal(t, fresh.Valid(), !tracked)
		})
	}
}
