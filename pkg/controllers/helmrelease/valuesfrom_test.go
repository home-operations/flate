package helmrelease

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestController_OmittedValuesAfterWait(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		waitValues   bool
		materialized bool
		duplicate    bool
		refreshed    bool
		fail         bool
		wantGreeting string
		wantWarnings int
	}{
		{name: "values wait, still missing", waitValues: true, wantGreeting: "fallback", wantWarnings: 1},
		{name: "values wait, materialized", waitValues: true, materialized: true, wantGreeting: "supplied"},
		{name: "chart wait, still missing", wantGreeting: "fallback", wantWarnings: 1},
		{name: "chart wait, materialized", materialized: true, wantGreeting: "supplied"},
		{name: "chart wait, current positions", refreshed: true, wantGreeting: "supplied"},
		{name: "chart failed", fail: true},
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
			c, st := newTestControllerWithOptions(t, ReconcileOptions{})
			src := &manifest.GitRepository{Name: "charts", Namespace: "flux-system"}
			st.AddObject(src)
			st.SetArtifact(src.Named(), &store.SourceArtifact{
				Kind: manifest.KindGitRepository, URL: "file://" + dir, FS: os.DirFS(dir), LocalPath: dir,
			})
			st.UpdateStatus(src.Named(), store.StatusPending, "", "fetching")
			if tc.waitValues {
				st.UpdateStatus(src.Named(), store.StatusReady, "", "")
			}
			publishRawFixture(t, c, &manifest.RawObject{
				Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1",
				Name: "app-creds", Namespace: "default",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}},
			})
			hr := &manifest.HelmRelease{
				Name: "demo", Namespace: "default",
				ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "app-values"}},
				Chart:      manifest.HelmChart{Name: "mychart", Source: manifest.NamedResource{Name: "charts", Namespace: "flux-system", Group: manifest.SourceDomain, Kind: manifest.KindGitRepository}},
			}
			if tc.duplicate {
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindSecret, Name: "app-values", ValuesKey: "other.yaml"})
			}
			if tc.waitValues {
				hr.ValuesFrom = append(hr.ValuesFrom, manifest.ValuesReference{Kind: manifest.KindConfigMap, Name: "chart-values"})
			}
			expectedRefs := slices.Clone(hr.ValuesFrom)
			st.AddObject(hr)
			if _, blocked := c.ReconcileNode(t.Context(), hr.Named(), manifest.ReconcileAttempt{Drain: manifest.DrainNone}); blocked.Empty() {
				t.Fatal("expected reconciliation to wait for a dependency")
			}
			assert.Equal(t, len(st.Warnings()), 0)
			if tc.refreshed {
				fresh := hr.Clone()
				fresh.ValuesFrom = []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: "current-values"}}
				expectedRefs = slices.Clone(fresh.ValuesFrom)
				st.AddObject(fresh)
				st.AddObject(&manifest.Secret{Name: "current-values", Namespace: hr.Namespace, StringData: map[string]any{"values.yaml": "greeting: supplied\n"}})
			}
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
			st.UpdateStatus(src.Named(), status, "", "chart source")
			assert.Equal(t, runValuesFixture(t, c, hr).Status, status)
			assert.Equal(t, len(st.Warnings()), tc.wantWarnings)
			stored, _ := st.Get[*manifest.HelmRelease](hr.Named())
			assert.Diff(t, stored.ValuesFrom, expectedRefs)
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
	c, s := newTestControllerWithOptions(t, ReconcileOptions{AllowMissingSecrets: true})
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	s.AddObject(chart)
	s.UpdateStatus(chart.Named(), store.StatusFailed, "", "chart unavailable")
	hr := &manifest.HelmRelease{Name: "app", Namespace: "apps", Chart: manifest.HelmChart{Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: chart.Name, Namespace: chart.Namespace}}}
	s.AddObject(hr)
	err := c.Require(&depwait.Waiter{Parent: hr.Named(), Dependencies: preparePrereqs(hr), Declaration: hr, AllowMissingSecrets: true}, manifest.ReconcileAttempt{Drain: manifest.DrainForce}, &depwait.Resolution{})
	failed, ok := errors.AsType[*manifest.DependencyFailedError](err)
	if !ok {
		t.Fatalf("error = %v, want DependencyFailedError", err)
	}
	assert.Diff(t, failed.Failed, []manifest.NamedResource{chart.Named()})
	assert.Equal(t, len(hr.ValuesFrom), 0)
}

func TestController_ValuesFromMixedProductionPrecedence(t *testing.T) {
	for _, declarationFirst := range []bool{false, true} {
		for _, materializer := range []string{"none", "pending", "actual"} {
			t.Run(fmt.Sprintf("declarationFirst=%t/%s", declarationFirst, materializer), func(t *testing.T) {
				c, s := newTestControllerWithOptions(t, ReconcileOptions{})
				target := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "values"}
				owner := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "a"}
				if !declarationFirst {
					owner.Name = "z"
				}
				if materializer == "pending" {
					s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: []manifest.Production{{Owner: owner, Authored: target, Path: "source/values.yaml", File: "values.yaml"}}}}})
				}
				declaration := &manifest.RawObject{APIVersion: "external-secrets.io/v1", Kind: "ExternalSecret", Namespace: "apps", Name: "declaration", Spec: map[string]any{"target": map[string]any{"name": "values"}}}
				s.AddObject(declaration)
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{target: {Productions: []manifest.Production{{Declaration: declaration.Named()}}}}}
				if materializer == "pending" {
					index.Records[target] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Declaration: declaration.Named()}, {Owner: owner, Authored: target, Path: "source/values.yaml", File: "values.yaml"}}}
				}
				s.SetDiscoveryIndex(index)
				if materializer == "actual" {
					s.AddObject(&manifest.Secret{Name: target.Name, Namespace: target.Namespace, StringData: map[string]any{"values.yaml": "real"}})
				}
				hr := &manifest.HelmRelease{Name: "reader", Namespace: "apps", ValuesFrom: []manifest.ValuesReference{{Kind: manifest.KindSecret, Name: target.Name}}}
				var resolved depwait.Resolution
				err := c.Require(&depwait.Waiter{Parent: hr.Named(), Declaration: hr}, manifest.ReconcileAttempt{}, &resolved)
				if (len(resolved.Omitted) > 0) != (materializer == "none") {
					t.Fatalf("mixed evidence resolution=%+v err=%v", resolved, err)
				}
				if materializer == "pending" {
					if _, blocked := errors.AsType[*depwait.ErrBlocked](err); !blocked {
						t.Fatalf("pending supplier did not block: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
