package helmrelease

import (
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_OmittedValuesAfterWait(t *testing.T) {
	for _, tc := range []struct {
		name         string
		waitValues   bool
		materialized bool
		duplicate    bool
		fail         bool
		wantGreeting string
		wantWarnings int
	}{
		{name: "values wait, still missing", waitValues: true, wantGreeting: "fallback", wantWarnings: 1},
		{name: "values wait, materialized", waitValues: true, materialized: true, wantGreeting: "supplied"},
		{name: "chart wait, still missing", wantGreeting: "fallback", wantWarnings: 1},
		{name: "chart wait, materialized", materialized: true, wantGreeting: "supplied"},
		{name: "chart failed", fail: true},
		{name: "duplicate omitted ref", duplicate: true, wantGreeting: "fallback", wantWarnings: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: demo}
data:
  greeting: {{ .Values.greeting | default "fallback" | quote }}
`)
			c, st := newTestControllerWithOptions(t, ReconcileOptions{})
			src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
			st.AddObject(src)
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindGitRepository, URL: "file://" + dir, LocalPath: dir,
			})
			st.UpdateStatus(src.Named(), store.StatusPending, "fetching")
			if tc.waitValues {
				st.UpdateStatus(src.Named(), store.StatusReady, "")
			}
			st.AddObject(&manifest.RawObject{
				Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
				Name: "app-creds", Namespace: "default",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}},
			})
			hr := &manifest.HelmRelease{
				Name: "demo", Namespace: "default",
				ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "app-values"}},
				Chart: manifest.HelmChart{
					Name: "mychart", RepoName: "charts",
					RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
				},
			}
			if tc.duplicate {
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindSecret, Name: "app-values", ValuesKey: "other.yaml"})
			}
			if tc.waitValues {
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: "chart-values"})
			}
			st.AddObject(hr)
			if blocked := c.ReconcileNode(t.Context(), hr.Named(), manifest.DrainNone); len(blocked) == 0 {
				t.Fatal("expected reconciliation to wait for a dependency")
			}
			assert.Equal(t, len(st.Warnings()), 0)
			st.AddObject(&manifest.ConfigMap{
				Name: "chart-values", Namespace: "default",
				Data: map[string]any{"values.yaml": "{}"},
			})
			if tc.materialized {
				st.AddObject(&manifest.Secret{
					Name: "app-values", Namespace: "default",
					StringData: map[string]any{"values.yaml": "greeting: supplied\n"},
				})
			}
			status := store.StatusReady
			if tc.fail {
				status = store.StatusFailed
			}
			st.UpdateStatus(src.Named(), status, "chart source")
			assert.Equal(t, dispatchToFixpoint(t, c, st, hr.Named()).Status, status)
			assert.Equal(t, len(st.Warnings()), tc.wantWarnings)
			for _, warning := range st.Warnings() {
				assert.Equal(t, warning.Count, 1)
			}
			if tc.fail {
				return
			}
			art, ok := st.GetArtifact(hr.Named()).(*store.HelmReleaseArtifact)
			if !ok {
				t.Fatal("HelmRelease artifact was not written")
			}
			assert.Equal(t, renderedConfigMapValue(art.Manifests, "greeting"), tc.wantGreeting)
		})
	}
}

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
	_, _, err := c.resolvePreRenderValuesFrom(t.Context(), hr, manifest.DrainForce)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok {
		t.Fatalf("error = %v, want DependencyFailedError", err)
	}
	assert.Diff(t, failed.Failed, []manifest.NamedResource{chart.Named()})
	assert.Equal(t, attempts, 1)
}
