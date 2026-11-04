package manifest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseHelmRelease_DependsOn(t *testing.T) {
	t.Parallel()
	hr, err := parseHelmRelease(helmReleaseDoc(t, `  dependsOn:
    - name: other
    - {name: cross, namespace: other-ns}`))
	{
		assert.FatalIf(t, err != nil, "parseHelmRelease: %v", err)
	}
	deps := refsForUse(t, hr, "dependsOn")
	{
		got, want := len(deps), 2
		assert.FatalIf(t, got != want, "DependsOn len = %d, want %d", got, want)
	}
	{
		got := deps[0].NamespacedName()
		assert.ErrorIf(t, got != "ns/other", "DependsOn[0] = %q, want ns/other", got)
	}
	if deps[1].Namespace != "other-ns" || deps[1].Name != "cross" {
		t.Errorf("DependsOn[1] = %+v", deps[1])
	}
}

func TestParseHelmRelease_Inline(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: podinfo
  namespace: podinfo
spec:
  targetNamespace: apps
  chart:
    spec:
      chart: podinfo
      version: 6.3.2
      sourceRef:
        kind: HelmRepository
        name: podinfo
        namespace: flux-system
  values:
    replicaCount: 3
  valuesFrom:
    - kind: ConfigMap
      name: extra
      valuesKey: values.yaml
      optional: true
`)
	hr, err := parseHelmRelease(doc)
	{
		assert.FatalIf(t, err != nil, "parseHelmRelease: %v", err)
	}

	cases := []struct {
		name string
		got  any
		want any
	}{
		{"Name", hr.Name, "podinfo"},
		{"Namespace", hr.Namespace, "podinfo"},
		{"ReleaseName", hr.ReleaseName(), "apps-podinfo"},
		{"ReleaseNamespace", hr.ReleaseNamespace(), "apps"},
		{"Chart.Name", hr.Chart.Name, "podinfo"},
		{"Chart.Version", hr.Chart.Version, "6.3.2"},
		{"Chart.Source.Kind", hr.Chart.Source.Kind, "HelmChart"},
		{"Chart.Source.Name", hr.Chart.Source.Name, "podinfo-podinfo"},
		{"replicaCount", hr.Values["replicaCount"], float64(3)},
		{"ValuesFrom[0].Optional", hr.ValuesFrom[0].Optional, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				assert.ErrorIf(t, tc.got != tc.want, "got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestParseHelmRelease_ReleaseNameOverride(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, spec, want string }{
		{"metadata namespace only", "", "hr"},
		{"target namespace", "  targetNamespace: apps", "apps-hr"},
		{"explicit override", "  targetNamespace: apps\n  releaseName: my-explicit-release", "my-explicit-release"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr, err := parseHelmRelease(helmReleaseDoc(t, tc.spec))
			assert.NoError(t, err)
			before := Fingerprint(hr.HelmReleaseSpec)
			assert.Equal(t, hr.ReleaseName(), tc.want)
			assert.Equal(t, Fingerprint(hr.HelmReleaseSpec), before)
		})
	}
}

func TestReleaseName_Shortens(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, metadata, target, explicit, want string }{
		{"short", "short-name", "", "", "short-name"},
		{"boundary", strings.Repeat("a", 53), "", "", strings.Repeat("a", 53)},
		{"composed boundary", strings.Repeat("a", 48), "apps", "", "apps-" + strings.Repeat("a", 48)},
		{"long metadata", "this-is-a-very-long-helmrelease-name-that-exceeds-53-chars", "", "", "this-is-a-very-long-helmrelease-name-tha-23e95a162bda"},
		{"long composed", "demo", "namespace-with-a-very-long-name-for-release-composition", "", "namespace-with-a-very-long-name-for-rele-e5340b09d048"},
		{"explicit override", "demo", "namespace-with-a-very-long-name-for-release-composition", "chosen", "chosen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := &HelmRelease{
				Name:            tc.metadata,
				HelmReleaseSpec: helmv2.HelmReleaseSpec{TargetNamespace: tc.target, ReleaseName: tc.explicit},
			}
			assert.Equal(t, hr.ReleaseName(), tc.want)
			assert.Equal(t, hr.ReleaseName(), tc.want)
		})
	}
}

func TestParseHelmRelease_ChartRef(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: podinfo
  namespace: podinfo
spec:
  chartRef:
    kind: HelmChart
    name: my-chart
    namespace: flux-system
`)
	hr, err := parseHelmRelease(doc)
	{
		assert.FatalIf(t, err != nil, "parseHelmRelease: %v", err)
	}
	assert.Equal(
		t,
		hr.Chart.Source,
		NamedResource{Group: SourceDomain, Kind: KindHelmChart, Namespace: "flux-system", Name: "my-chart"},
	)
	if hr.Chart.Source.Kind != KindHelmChart || hr.Chart.Version != "" {
		t.Fatalf("expected unresolved chartRef, got %+v", hr.Chart)
	}

	src := &HelmChartSource{
		Name: "my-chart", Namespace: "flux-system",
		Chart: "podinfo", Version: "6.3.2",
		SourceRef: sourcev1.LocalHelmChartSourceReference{
			Name: "podinfo", Kind: KindHelmRepository,
		},
	}
	lookup := func(id NamedResource) BaseManifest {
		if id == src.Named() {
			return src
		}
		return nil
	}
	{
		err := hr.ResolveChartSource(lookup)
		assert.FatalIf(t, err != nil, "ResolveChartSource: %v", err)
	}
	if hr.Chart.Version != "6.3.2" || hr.Chart.Name != "podinfo" {
		t.Errorf("after resolve: %+v", hr.Chart)
	}

	missing := &HelmRelease{
		ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindHelmChart, Name: "absent", Namespace: "ns"},
	}
	none := func(NamedResource) BaseManifest { return nil }
	{
		err := missing.ResolveChartSource(none)
		assert.ErrorIf(t, !errors.Is(err, ErrObjectNotFound), "expected ErrObjectNotFound, got %v", err)
	}
}

func TestParseHelmRepository_OCI(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: podinfo
  namespace: flux-system
spec:
  url: oci://ghcr.io/stefanprodan/charts
  type: oci
`)
	r, err := parseHelmRepository(doc)
	{
		assert.FatalIf(t, err != nil, "parseHelmRepository: %v", err)
	}
	if r.Type != "oci" {
		t.Errorf("RepoType = %q", r.Type)
	}
}

func TestParseHelmRelease_DisableValidationCombinations(t *testing.T) {
	t.Parallel()
	for _, flags := range []struct {
		name            string
		schema, openAPI bool
	}{
		{name: "neither"},
		{name: "schema", schema: true},
		{name: "openapi", openAPI: true},
		{name: "both", schema: true, openAPI: true},
	} {
		for _, stage := range []string{"neither", "install", "upgrade", "both"} {
			t.Run(flags.name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				enabled := fmt.Sprintf(
					"{disableSchemaValidation: %t, disableOpenAPIValidation: %t}",
					flags.schema,
					flags.openAPI,
				)
				var spec string
				if stage == "install" || stage == "both" {
					spec += "  install: " + enabled + "\n"
				}
				if stage == "upgrade" || stage == "both" {
					spec += "  upgrade: " + enabled + "\n"
				}
				hr, err := parseHelmRelease(helmReleaseDoc(t, spec))
				{
					assert.NoError(t, err)
				}
				wantSchema := (stage == "install" || stage == "both") && flags.schema
				assert.Equal(t, hr.DisableSchemaValidation, wantSchema)
				release := helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}
				assert.Equal(
					t,
					release.GetInstall().DisableOpenAPIValidation,
					(stage == "install" || stage == "both") && flags.openAPI,
				)
				assert.Equal(
					t,
					release.GetUpgrade().DisableOpenAPIValidation,
					(stage == "upgrade" || stage == "both") && flags.openAPI,
				)
			})
		}
	}
}
