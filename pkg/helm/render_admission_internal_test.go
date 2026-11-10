package helm

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_CurrentRenderAdmission(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml":        "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
		"mychart/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\ndata: {value: {{ .Values.value | quote }}}\n",
	})
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	for _, tc := range []struct {
		name, value, revision string
		warm, reusable        bool
	}{
		{name: "cold", value: "first"},
		{name: "pending replay", value: "first", warm: true, reusable: true},
		{name: "source refresh", value: "first", revision: "refreshed", warm: true, reusable: true},
		{name: "supplied values", value: "changed", warm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := NewClient(cachetest.New(t, t.TempDir(), cache.New))
			hr := newHR()
			var initial *manifest.RenderReceipt
			if tc.warm {
				_, receipt, err := client.Template(t.Context(), hr, map[string]any{"value": "first"}, input)
				assert.NoError(t, err)
				initial = receipt
			}
			current := input
			artifact := *input.Sources.LocalSourceArtifact(hr.ChartSource(), store.ArtifactContent)
			artifact.Revision = tc.revision
			sources := store.New()
			sources.SetArtifact(hr.ChartSource(), &artifact)
			current.Sources = NewSources(sources)
			var observed RenderAdmission
			calls := 0
			current.Admit = func(admission RenderAdmission) (bool, error) {
				calls++
				observed = admission
				return tc.reusable, nil
			}
			values := map[string]any{"value": tc.value}
			_, receipt, err := client.Template(t.Context(), hr, values, current)
			assert.NoError(t, err)
			assert.Equal(t, calls, 1)
			assert.Equal(t, observed.Key != "", true)
			assert.Equal(t, observed.Receipt != nil, tc.reusable)
			assert.Equal(t, observed.Accepted, false)
			assert.Equal(t, observed.Documents == nil, true)
			if !tc.reusable {
				assert.Equal(t, receipt != nil, true)
				return
			}
			frame := observed.Receipt.Value().(renderFrame)
			assert.Equal(t, frame.lookup, "")
			accepted := false
			if frame.stored != nil {
				_, accepted = frame.stored.History(hr.Named(), frame.content)
			}
			assert.Equal(t, accepted, false)
			prepared, err := client.prepareTemplate(t.Context(), hr, values, &current)
			assert.NoError(t, err)
			assert.Equal(t, frame.binding.same(&prepared.binding), true)
			assert.Equal(t, observed.Receipt.Equal(initial), tc.revision == "")
		})
	}
}
