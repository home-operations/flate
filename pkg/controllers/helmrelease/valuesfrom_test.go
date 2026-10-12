package helmrelease

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestResolvePreRenderValuesFrom_FailedChartRefTerminates(t *testing.T) {
	c, s := newTestControllerWithOptions(t, ReconcileOptions{AllowMissingSecrets: true})
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	s.AddObject(chart)
	s.UpdateStatus(chart.Named(), store.StatusFailed, "chart unavailable")
	hr := &manifest.HelmRelease{Name: "app", Namespace: "apps", Chart: manifest.HelmChart{
		RepoKind: manifest.KindHelmChart, RepoName: chart.Name, RepoNamespace: chart.Namespace,
	}}
	s.AddObject(hr)
	attempts := 0
	unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == hr.Named() && payload.(store.StatusInfo).Message == "awaiting pre-render references" {
			attempts++
			if attempts > 1 {
				t.Fatal("retried a failed chartRef with no removable valuesFrom refs")
			}
			// Identical status writes suppress events, so reset the progress message.
			s.UpdateStatus(id, store.StatusPending, "checking references")
		}
	}, false)
	t.Cleanup(unsub)
	_, err := c.resolvePreRenderValuesFrom(t.Context(), hr, manifest.DrainForce)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok {
		t.Fatalf("error = %v, want DependencyFailedError", err)
	}
	assert.Diff(t, failed.Failed, []manifest.NamedResource{chart.Named()})
	assert.Equal(t, attempts, 1)
}
