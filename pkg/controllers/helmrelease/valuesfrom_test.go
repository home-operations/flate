package helmrelease

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/home-operations/flate/pkg/depwait"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_OmittedValuesAfterWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		waitValues   bool
		materialized bool
		wiped        bool
		duplicate    bool
		fail         bool
		wantGreeting string
		wantWarnings int
	}{
		{name: "values wait, still missing", waitValues: true, wantGreeting: "fallback", wantWarnings: 1},
		{name: "values wait, materialized", waitValues: true, materialized: true, wantGreeting: "supplied"},
		{name: "chart wait, still missing", wantGreeting: "fallback", wantWarnings: 1},
		{name: "chart wait, materialized", materialized: true, wantGreeting: "supplied"},
		{name: "chart failed", fail: true, wantWarnings: 1},
		{name: "wiped values file", wiped: true, wantGreeting: "fallback", wantWarnings: 1},
		{name: "duplicate omitted ref", duplicate: true, wantGreeting: "fallback", wantWarnings: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: demo}
data:
  greeting: {{ .Values.greeting | default "fallback" | quote }}
`)
			c, st := newTestControllerWithOptions(t, base.Options{})
			src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
			st.AddObject(src)
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
			})
			st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "fetching"})
			if tc.waitValues {
				st.UpdateStatus(src.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			}
			publishRawFixture(t, c, &manifest.RawObject{
				Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
				Name: "app-creds", Namespace: "default",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}},
			})
			hr := &manifest.HelmRelease{
				Name: "demo", Namespace: "default",
				ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "app-values"}},
				Chart:      manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "flux-system", Name: "charts"}},
			}
			if tc.duplicate {
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindSecret, Name: "app-values", ValuesKey: "other.yaml"})
			}
			if tc.waitValues {
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: "chart-values"})
			}
			st.AddObject(hr)
			if blocked := c.ReconcileNode(t.Context(), hr.Named(), manifest.DrainNone).Blocked; len(blocked) == 0 {
				t.Fatal("expected reconciliation to wait for a dependency")
			}
			assert.Equal(t, len(st.Warnings()), 0)
			st.AddObject(&manifest.ConfigMap{
				Name: "chart-values", Namespace: "default",
				Data: map[string]any{"values.yaml": "{}"},
			})
			if tc.materialized || tc.wiped {
				value := "greeting: supplied\n"
				if tc.wiped {
					value = "..PLACEHOLDER_values.yaml.."
				}
				st.AddObject(&manifest.Secret{
					Name: "app-values", Namespace: "default",
					StringData: map[string]any{"values.yaml": value},
				})
			}
			status := store.StatusReady
			if tc.fail {
				status = store.StatusFailed
			}
			st.UpdateStatus(src.Named(), store.StatusInfo{Status: status, Reason: "", Message: "chart source"})
			assert.Equal(t, controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: c.Tasks, Store: st, ID: hr.Named(), ReconcileNode: c.ReconcileNode}).Status, status)
			assert.Equal(t, len(st.Warnings()), tc.wantWarnings)
			for _, warning := range st.Warnings() {
				assert.Equal(t, warning.Count, 1)
			}
			if tc.fail {
				return
			}
			art, ok := st.GetArtifact(hr.Named()).(*store.Emission)
			if !ok {
				t.Fatal("HelmRelease artifact was not written")
			}
			assert.Equal(t, renderedConfigMapValue(art.Manifests, "greeting"), tc.wantGreeting)
		})
	}
}

func TestResolvePreRenderValuesFrom_FailedChartRefTerminates(t *testing.T) {
	t.Parallel()
	c, s := newTestControllerWithOptions(t, base.Options{AllowMissingSecrets: true})
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	s.AddObject(chart)
	s.UpdateStatus(chart.Named(), store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "chart unavailable"})
	hr := &manifest.HelmRelease{Name: "app", Namespace: "apps", Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: chart.Namespace, Name: chart.Name}}}
	s.AddObject(hr)
	attempts := 0
	unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == hr.Named() && payload.(store.StatusInfo).Message == "resolving dependencies" {
			attempts++
			if attempts > 1 {
				t.Fatal("retried a failed chartRef with no removable valuesFrom refs")
			}
			// Identical status writes suppress events, so reset the progress message.
			s.UpdateStatus(id, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "checking references"})
		}
	}, false)
	t.Cleanup(unsub)
	err := c.newExecution(hr.Named()).reconcile(t.Context(), hr, manifest.DrainForce)
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok {
		t.Fatalf("error = %v, want DependencyFailedError", err)
	}
	assert.Diff(t, failed.Failed, []manifest.NamedResource{chart.Named()})
	assert.Equal(t, attempts, 1)
}

func TestController_ValuesFromMixedProductionPrecedence(t *testing.T) {
	for _, declarationFirst := range []bool{false, true} {
		for _, materializer := range []string{"none", "pending", "actual"} {
			t.Run(fmt.Sprintf("declarationFirst=%t/%s", declarationFirst, materializer), func(t *testing.T) {
				c, s := newTestControllerWithOptions(t, base.Options{})
				target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
				owner := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "a"}
				if !declarationFirst {
					owner.Name = "z"
				}
				if materializer == "pending" {
					s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: []manifest.Production{{Owner: owner, Authored: target, Path: "source/values.yaml", File: "values.yaml"}}}}})
				}
				publishRawFixture(t, c, &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Namespace: "apps", Name: "declaration", Spec: map[string]any{"target": map[string]any{"name": "values"}}})
				if materializer == "actual" {
					s.AddObject(&manifest.Secret{Name: target.Name, Namespace: target.Namespace, StringData: map[string]any{"values.yaml": "real"}})
				}
				execution := c.newExecution(manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "reader"})
				s.UpdateStatus(testutil.KustomizationID("apps", "producer-root"), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
				input := execution.data.Lookup(target)
				w := depwait.Waiter{Store: &execution.run.Reads, Parent: execution.run.Reads.ID()}
				result := w.Available(t.Context(), depwait.DataDependency{NamedResource: target, Input: input}, manifest.DrainNone)
				hasProducer, omit := input.Generated, result.Omit
				if omit != (materializer == "none") || omit && !hasProducer {
					t.Fatalf("mixed evidence omitted=%t producer=%t", omit, hasProducer)
				}
			})
		}
	}
}
