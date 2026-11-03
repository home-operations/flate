package helm

import (
	"reflect"
	"strings"
	"testing"

	manifest "github.com/home-operations/flate/pkg/manifest"

	v2 "helm.sh/helm/v4/pkg/chart/v2"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
)

func TestComputeTemplateKey_DifferingFieldsDiverge(t *testing.T) {
	t.Parallel()
	ch, hr := minimalChart(), minimalHR()
	values := map[string]any{"k": "v"}
	base := computeTemplateKey(
		chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch},
		values,
		Options{},
		hr,
	)
	assert.FatalIf(t, base == "", "%v", "valid inputs produced an empty key")
	excluded := map[string]string{
		"DisableChartDigestTracking": "OCI tracking is projected only for direct OCI releases",
		"SkipKinds":                  "resource filtering occurs downstream of Template",
	}
	typ := reflect.TypeFor[Options]()
	for i := range typ.NumField() {
		field := typ.Field(i)
		t.Run("Options/"+field.Name, func(t *testing.T) {
			t.Parallel()
			computeTemplateKeyDifferingFieldsDivergeCase(t, &i, &field, struct {
				ch       **v2.Chart
				values   *map[string]any
				hr       **manifest.HelmRelease
				excluded *map[string]string
				base     *string
			}{ch: &ch, values: &values, hr: &hr, excluded: &excluded, base: &base})
		})
	}
	for _, field := range []string{
		"Name", "Namespace", "HelmReleaseSpec.ReleaseName", "TargetNamespace",
		"Install.CRDs", "DisableSchemaValidation", "Install.DisableOpenAPIValidation",
		"Install.DisableHooks", "Install.Replace", "Test.Enable",
		"Chart.Version", "ChartValuesFiles", "IgnoreMissingValuesFiles", "PostRenderers",
	} {
		t.Run("HelmRelease/"+field, func(t *testing.T) {
			t.Parallel()
			computeTemplateKeyDifferingFieldsDivergeCase2(t, &hr, &field, struct {
				ch     **v2.Chart
				values *map[string]any
				base   *string
			}{ch: &ch, values: &values, base: &base})
		})
	}
	t.Run("ChartContent", func(t *testing.T) {
		t.Parallel()
		computeTemplateKeyDifferingFieldsDivergeChartContent(t, &values, &hr, &base)
	})

	t.Run("Values", func(t *testing.T) {
		t.Parallel()
		computeTemplateKeyDifferingFieldsDivergeValues(t, &ch, &hr, &base)
	})
}

func TestComputeTemplateKey_SelectedActionPolicy(t *testing.T) {
	t.Parallel()
	loaded := chartFixtureDescription(t, minimalChart())

	t.Run("install", func(t *testing.T) {
		t.Parallel()
		opts := Options{}
		hr := minimalHR()
		base := computeTemplateKey(loaded, nil, opts, hr)
		for _, tc := range []struct {
			name                   string
			policy                 helmv2.CRDsPolicy
			openAPI, active, equal bool
		}{
			{"empty", "", false, true, true},
			{"default", helmv2.Create, false, true, true},
			{"replace", helmv2.CreateReplace, false, true, false},
			{"skip", helmv2.Skip, false, true, false},
			{"openapi", "", true, true, false},
			{"inactive", helmv2.Skip, true, false, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				changed := hr.Clone()
				if !tc.active {
					changed.Upgrade = &helmv2.Upgrade{CRDs: tc.policy, DisableOpenAPIValidation: tc.openAPI}
				} else {
					changed.Install = &helmv2.Install{CRDs: tc.policy, DisableOpenAPIValidation: tc.openAPI}
				}
				key := computeTemplateKey(loaded, nil, opts, changed)
				assert.Equal(t, key != "", true)
				assert.Equal(t, key == base, tc.equal)
			})
		}
	})
}

// Common metadata MUST remain outside the cached render and its key.
func TestComputeTemplateKey_CommonMetadataExcluded(t *testing.T) {
	t.Parallel()
	ch := minimalChart()
	values := map[string]any{"k": "v"}
	opts := Options{KubeVersion: "1.33"}
	base := minimalHR()
	baseKey := computeTemplateKey(
		chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch},
		values,
		opts,
		base,
	)

	t.Run("CommonMetadata does not change the key", func(t *testing.T) {
		t.Parallel()
		alt := *base
		alt.CommonMetadata = &helmv2.CommonMetadata{
			Labels:      map[string]string{"team": "platform"},
			Annotations: map[string]string{"note": "x"},
		}
		{
			got := computeTemplateKey(
				chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch},
				values,
				opts,
				&alt,
			)
			assert.ErrorIf(
				t,
				got != baseKey,
				"CommonMetadata must NOT affect the cache key (applied downstream of Template):\nbase=%s\nalt =%s",
				baseKey,
				got,
			)
		}
	})

	t.Run("control: PostRenderers still diverges", func(t *testing.T) {
		t.Parallel()
		alt := *base
		alt.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{}}}
		{
			got := computeTemplateKey(
				chartDescription{fingerprint: chartFixtureDescription(t, ch).fingerprint, chart: ch},
				values,
				opts,
				&alt,
			)
			assert.ErrorIf(
				t,
				got == baseKey,
				"%v",
				"PostRenderers changes rendered output and must still diverge the key (guards against dropping a real input)",
			)
		}
	})
}

type templateActionProbe chan struct{}

func (p templateActionProbe) MarshalJSON() ([]byte, error) { return []byte(`"fresh"`), nil }

func (p templateActionProbe) String() string { p <- struct{}{}; return "fresh" }

func computeTemplateKeyDifferingFieldsDivergeCase(
	t *testing.T,
	i *int,
	field *reflect.StructField,
	capturedInputs struct {
		ch       **v2.Chart
		values   *map[string]any
		hr       **manifest.HelmRelease
		excluded *map[string]string
		base     *string
	},
) {
	t.Helper()

	var opts Options
	v := reflect.ValueOf(&opts).Elem().Field((*i))
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("different")
	case reflect.Slice:
		v.Set(reflect.ValueOf([]string{"different"}))
	default:
		t.Fatalf("new option %s requires an explicit cache-key decision", (*field).Name)
	}
	key := computeTemplateKey(
		chartDescription{
			fingerprint: chartFixtureDescription(t, (*capturedInputs.ch)).fingerprint,
			chart:       (*capturedInputs.ch),
		},
		(*capturedInputs.values),
		opts,
		(*capturedInputs.hr),
	)
	assert.FatalIf(t, key == "", "%v", "valid option produced an empty key")
	_, skip := (*capturedInputs.excluded)[(*field).Name]
	assert.Equal(t, key == (*capturedInputs.base), skip)
}

func computeTemplateKeyDifferingFieldsDivergeCase2(
	t *testing.T,
	hr **manifest.HelmRelease,
	field *string,
	capturedInputs struct {
		ch     **v2.Chart
		values *map[string]any
		base   *string
	},
) {
	t.Helper()

	alt := (*hr).Clone()
	v := reflect.ValueOf(alt).Elem()
	for name := range strings.SplitSeq((*field), ".") {
		v = v.FieldByName(name)
		if v.Kind() == reflect.Pointer {
			v.Set(reflect.New(v.Type().Elem()))
			v = v.Elem()
		}
	}
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(true)
	case reflect.String:
		v.SetString("different")
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		if v.Type().Elem().Kind() == reflect.String {
			v.Index(0).SetString("different")
		}
	default:
		t.Fatalf("field %s requires an explicit cache-key decision", (*field))
	}
	key := computeTemplateKey(
		chartDescription{
			fingerprint: chartFixtureDescription(t, (*capturedInputs.ch)).fingerprint,
			chart:       (*capturedInputs.ch),
		},
		(*capturedInputs.values),
		Options{},
		alt,
	)
	assert.FatalIf(
		t,
		key == "" || key == (*capturedInputs.base),
		"%v",
		"changed release input did not produce a distinct valid key",
	)
}

func computeTemplateKeyDifferingFieldsDivergeChartContent(
	t *testing.T,
	values *map[string]any,
	hr **manifest.HelmRelease,
	base *string,
) {
	t.Helper()

	changed := minimalChart()
	changed.Metadata.Version = "9.0.0"
	assert.Equal(
		t,
		computeTemplateKey(
			chartDescription{fingerprint: chartFixtureDescription(t, changed).fingerprint, chart: changed},
			(*values),
			Options{},
			(*hr),
		) == (*base),
		false,
	)
}

func computeTemplateKeyDifferingFieldsDivergeValues(
	t *testing.T,
	ch **v2.Chart,
	hr **manifest.HelmRelease,
	base *string,
) {
	t.Helper()

	assert.Equal(
		t,
		computeTemplateKey(
			chartDescription{fingerprint: chartFixtureDescription(t, (*ch)).fingerprint, chart: (*ch)},
			map[string]any{"k": "different"},
			Options{},
			(*hr),
		) == (*base),
		false,
	)
}
