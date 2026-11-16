package helm

import (
	"errors"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestPrepare_AuthoredReferencePolicy(t *testing.T) {
	t.Parallel()
	hr := newHR()
	hr.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
		Chart: "mychart", SourceRef: &helmv2.CrossNamespaceObjectReference{
			Kind: manifest.KindGitRepository, Name: "chart-repo", Namespace: "flux-system",
		},
	}}
	hr.ValuesFrom = []manifest.ValuesReference{{Kind: manifest.KindConfigMap, Name: "late", Optional: true}}
	hr.Values = map[string]any{"VALUE": "early"}
	before := hr.Clone()
	var inputs [2][]values.Result
	var releases [2]*manifest.HelmRelease
	for i := range releases {
		var facts store.ReferenceFacts
		if i == 1 {
			data := &manifest.ConfigMap{
				Name: "late", Namespace: hr.Namespace, Data: map[string]any{"values.yaml": "VALUE: late\n"},
			}
			facts = store.ReferenceFacts{Declared: true, Data: data, Original: data}
		}
		for ref := range manifest.References(hr, manifest.ReferenceRelations) {
			if ref.Use == "valuesFrom" {
				inputs[i] = append(inputs[i], values.Resolve(&values.ResolveRequest{
					Reference: ref, Facts: facts, Demand: values.HelmDemand,
				}))
			}
		}
		var err error
		releases[i], err = Prepare(hr, nil, inputs[i], nil)
		assert.NoError(t, err)
		assert.Diff(t, releases[i].ValuesFrom, before.ValuesFrom)
		assert.Diff(t, releases[i].Values, before.Values)
		for ref := range manifest.References(releases[i], manifest.ReferenceRelations) {
			if ref.Use == "valuesFrom" {
				assert.Equal(t, ref.Position, 0)
				assert.Equal(t, ref.Gate&manifest.GateArtifacts, manifest.GateStage(0))
			}
		}
	}
	assert.Equal(t, inputs[0][0].Effect(), values.Omitted)
	assert.Equal(t, inputs[1][0].Effect(), values.Used)
	assert.Equal(t, releases[0].HistoryPolicy().Equal(releases[1].HistoryPolicy()), true)
	assert.Equal(t, manifest.ValueEqual(
		releases[0].Content("chart", values.OmittedIDs(inputs[0])),
		releases[1].Content("chart", values.OmittedIDs(inputs[1])),
	), false)
	root := testutil.WriteTree(t, map[string]string{
		"mychart/Chart.yaml": "apiVersion: v2\nname: mychart\nversion: 0.1.0\n",
	})
	client := NewClient(cachetest.New(t, "", cache.New))
	var previous Preparation
	defer func() { previous = Preparation{} }()
	request := RenderOptions{
		Preparation: &previous,
		Sources:     localChartResolver(t, "chart-repo", "flux-system", root),
		Omitted:     values.OmittedIDs(inputs[0]),
	}
	_, err := client.prepareTemplate(t.Context(), releases[0], releases[0].Values, &request)
	assert.NoError(t, err)
	request.Omitted = values.OmittedIDs(inputs[1])
	assert.Equal(t, previous.Matches(client, releases[1], &request), true)
	assert.Equal(t, manifest.Equal(hr, before), true)
	assert.FatalIf(t, len(releases[0].ValuesFrom) != 1, "authored reference was removed")
	releases[0].ValuesFrom[0].Name = "mutated"
	assert.Equal(t, hr.ValuesFrom[0].Name, "late")
}

func TestPrepare_FailedExpansionKeepsDeclaration(t *testing.T) {
	t.Parallel()
	hr := newHR()
	hr.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
		Chart: "mychart", SourceRef: &helmv2.CrossNamespaceObjectReference{
			Kind: manifest.KindGitRepository, Name: "chart-repo", Namespace: "flux-system",
		},
	}}
	hr.ValuesFrom = []manifest.ValuesReference{
		{Kind: manifest.KindConfigMap, Name: "absent", Optional: true},
		{Kind: manifest.KindConfigMap, Name: "malformed"},
	}
	hr.Values = map[string]any{"inline": map[string]any{"key": "unchanged"}}
	before := hr.Clone()
	data := &manifest.ConfigMap{
		Name: "malformed", Namespace: hr.Namespace, Data: map[string]any{"values.yaml": "value: ["},
	}
	var inputs []values.Result
	for ref := range manifest.References(hr, manifest.ReferenceRelations) {
		if ref.Use == "valuesFrom" {
			var facts store.ReferenceFacts
			if ref.Name == "malformed" {
				facts = store.ReferenceFacts{Declared: true, Data: data, Original: data}
			}
			inputs = append(inputs, values.Resolve(&values.ResolveRequest{
				Reference: ref, Facts: facts, Demand: values.HelmDemand,
			}))
		}
	}
	assert.Equal(t, inputs[0].Effect(), values.Omitted)
	prepared, err := Prepare(hr, nil, inputs, nil)
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, prepared == nil, true)
	assert.Equal(t, manifest.Equal(hr, before), true)
}

func TestPrepare_ChartReferenceNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ namespace, want string }{{"", "effective"}, {"foreign", "foreign"}} {
		namespace := tc.namespace
		t.Run("authored_"+namespace, func(t *testing.T) {
			t.Parallel()
			hr := &manifest.HelmRelease{
				Namespace: "authored",
				Chart: manifest.HelmChart{
					Source: manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindHelmChart,
						Namespace: namespace,
						Name:      "source",
					},
				},
			}
			hr.Values = map[string]any{"key": "original"}
			hr.ChartRef = &helmv2.CrossNamespaceSourceReference{
				Kind:      manifest.KindHelmChart,
				Namespace: namespace,
				Name:      "source",
			}
			projected := hr.WithNamespace("effective").(*manifest.HelmRelease)
			want := tc.want
			got, err := Prepare(projected, func(id manifest.NamedResource) manifest.BaseManifest {
				assert.Equal(
					t,
					id,
					manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindHelmChart,
						Namespace: want,
						Name:      "source",
					},
				)
				return &manifest.HelmChartSource{
					Namespace:                "sources",
					Chart:                    "resolved-name",
					ValuesFiles:              []string{"custom.yaml"},
					IgnoreMissingValuesFiles: true,
					SourceRef: sourcev1.LocalHelmChartSourceReference{
						Kind: manifest.KindHelmRepository,
						Name: "resolved",
					},
				}
			}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, got.Chart.Source.Namespace, tc.want)
			assert.Equal(t, got.Chart.Source.Name, "source")
			assert.Equal(t, hr.Namespace, "authored")
			assert.Equal(t, hr.ChartRef.Namespace, namespace)
			assert.Equal(t, projected.ChartSource().Namespace, tc.want)
			assert.Equal(t, got.Chart.Name, "resolved-name")
			assert.Equal(t, got.Chart.Version, "")
			assert.Diff(t, got.ChartValuesFiles, []string{"custom.yaml"})
			assert.Equal(t, got.IgnoreMissingValuesFiles, true)
			got.ChartRef.Namespace = "mutated"
			got.Values["key"] = "mutated"
			assert.Equal(t, hr.ChartRef.Namespace, namespace)
			assert.Equal(t, hr.Values["key"], any("original"))
			hr.ChartRef.Kind, hr.Chart.Source.Kind = manifest.KindOCIRepository, manifest.KindOCIRepository
			got, err = Prepare(hr.WithNamespace("effective").(*manifest.HelmRelease), nil, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, got.Chart.Source.Namespace, want)
		})
	}
}
