package manifest

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
)

func TestReferences_SourceValuesFiles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, group  string
		files, chart bool
	}{
		{KindGitRepository, SourceDomain, true, true},
		{KindBucket, SourceDomain, true, true},
		{KindHelmRepository, SourceDomain, false, false},
		{KindOCIRepository, SourceDomain, false, false},
		{KindExternalArtifact, SourceDomain, false, true},
		{KindGitRepository, "foreign.example", false, false},
	} {
		t.Run(tc.group+"/"+tc.kind, func(t *testing.T) {
			t.Parallel()
			testReferencesSourceValuesFilesCase(t, &tc)
		},
		)
	}
}

func assertReferenceEdges(t *testing.T, got, want []Reference) {
	t.Helper()
	{
		diff := cmp.Diff(
			want,
			got,
			cmpopts.IgnoreFields(DependencyRef{}, "Artifact", "ArtifactOnly", "ChartSource"),
			cmpopts.IgnoreFields(
				Reference{},
				"Gate",
				"ArtifactStages",
				"Ordering",
				"Selector",
				"Key",
				"NamespaceDefault",
			),
		)
		assert.FatalIf(t, diff != "", "reference edges (-want +got):\n%s", diff)
	}
}

func TestReferences_ChartPaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, want string }{
		{".", "."},
		{"./", "./"},
		{"./charts/demo", "./charts/demo"},
		{"charts/demo.tgz", "charts/demo.tgz"},
		{"", ""},
		{"../escape", ""},
		{"charts/../../escape", ""},
		{"/absolute", ""},
		{"bad\x00chart", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obj := &HelmChartSource{
				Name:      "chart",
				Chart:     tc.name,
				SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindGitRepository, Name: "source"},
			}
			refs := slices.Collect(References(obj, ReferenceRelations))
			assert.Equal(t, refs[0].ChartPath, tc.want)
			assert.Equal(t, obj.Chart, tc.name)
		})
	}
}

func testReferencesSourceValuesFilesCase(t *testing.T, testCaseInput *struct {
	kind  string
	group string
	files bool
	chart bool
},
) {
	t.Helper()

	for _, form := range []string{"inline", "resolved", "standalone"} {
		if form == "inline" &&
			(testCaseInput.kind == KindOCIRepository || testCaseInput.kind == KindExternalArtifact) {
			continue
		}
		t.Run(form, func(t *testing.T) {
			t.Parallel()
			files := []string{"./config/prod.yaml", "config/extra.yaml"}
			chart := &HelmChartSource{
				Namespace: "authored",
				Name:      "chart",
				Chart:     "./charts/demo",
				SourceRef: sourcev1.LocalHelmChartSourceReference{
					APIVersion: testCaseInput.group + "/v1",
					Kind:       testCaseInput.kind,
					Name:       "source",
				},
				ValuesFiles: files,
			}
			var obj BaseManifest
			switch form {
			case "inline":
				hr := &HelmRelease{
					Name:             "app",
					Namespace:        "authored",
					Chart:            helmChartFromSource(chart),
					ChartValuesFiles: files,
				}
				hr.Chart.Source.Namespace = ""
				hr.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
					Chart:       chart.Chart,
					ValuesFiles: files,
					SourceRef: &helmv2.CrossNamespaceObjectReference{
						APIVersion: testCaseInput.group + "/v1",
						Kind:       testCaseInput.kind,
						Name:       "source",
					},
				}}
				obj = hr.WithNamespace("effective")
			case "resolved":
				hr := &HelmRelease{
					Namespace: "effective",
					Chart: HelmChart{
						Source: NamedResource{Group: SourceDomain, Kind: KindHelmChart, Name: "chart"},
					},
				}
				hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: KindHelmChart, Name: "chart"}
				assert.NoError(t, hr.ResolveChartSource(func(id NamedResource) BaseManifest {
					assert.Equal(
						t,
						id,
						NamedResource{
							Group:     SourceDomain,
							Kind:      KindHelmChart,
							Namespace: "effective",
							Name:      "chart",
						},
					)
					return chart.WithNamespace(id.Namespace).(*HelmChartSource)
				}))
				obj = hr
			case "standalone":
				obj = chart.WithNamespace("effective")
			}
			refs := slices.Collect(References(obj, ReferenceRelations))
			if form != "standalone" {
				assert.Equal(t, refs[0].Gate&GateInputs != 0, form == "resolved")
			}
			source := Reference{
				Group:     testCaseInput.group,
				Kind:      testCaseInput.kind,
				Namespace: "effective",
				Name:      "source",
				Use:       "source",
				Content:   true,
			}
			if testCaseInput.files {
				source.Files = files
			}
			if testCaseInput.chart {
				source.ChartPath = "./charts/demo"
			}
			want := []Reference{source}
			if form == "inline" {
				want = append(
					[]Reference{
						{
							Group:     SourceDomain,
							Kind:      KindHelmChart,
							Namespace: "effective",
							Name:      "effective-app",
							Use:       "chart",
							Content:   true,
						},
					},
					want...)
			}
			if form == "resolved" {
				want = append(
					[]Reference{
						{
							Group:     SourceDomain,
							Kind:      KindHelmChart,
							Namespace: "effective",
							Name:      "chart",
							Use:       "chart",
							Content:   true,
						},
					},
					want...)
			}
			assertReferenceEdges(t, refs, want)
			if testCaseInput.files && len(refs[len(refs)-1].Files) != 0 {
				got := refs[len(refs)-1].Files
				assert.Equal(t, &got[0] == &files[0], true)
			}
			assert.Equal(t, chart.Namespace, "authored")
		})
	}
}
