package helm

import (
	"testing"
	"time"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPreparation_Matches(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
	})
	client := NewClient(cachetest.New(t, "", cache.New), nil)
	hr := newHR()
	var preparation Preparation
	input := RenderOptions{Preparation: &preparation, Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	_, err := client.prepareTemplate(t.Context(), hr, nil, &input)
	assert.NoError(t, err)
	changed := hr.Clone()
	changed.TargetNamespace = "other"
	patched := hr.Clone()
	patched.Interval.Duration += time.Minute
	policy := input
	policy.SkipTests = true
	for _, tc := range []struct {
		name        string
		preparation *Preparation
		client      *Client
		declaration *manifest.HelmRelease
		request     *RenderOptions
		want        bool
	}{
		{"nil", nil, client, hr, &input, false},
		{"incomplete", &Preparation{client: client, declaration: hr}, client, hr, &input, false},
		{"same owner", &preparation, client, hr, &input, true},
		{"equal declaration", &preparation, client, hr.Clone(), &input, true},
		{"different client", &preparation, nil, hr, &input, false},
		{"missing declaration", &preparation, client, nil, &input, false},
		{"different render inputs", &preparation, client, changed, &input, false},
		{"reconcile policy only", &preparation, client, patched, &input, true},
		{"different policy", &preparation, client, hr, &policy, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.preparation.Matches(tc.client, tc.declaration, tc.request), tc.want)
		})
	}
}
