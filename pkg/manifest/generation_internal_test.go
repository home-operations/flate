package manifest

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGeneratedObjects_ChartTemplate(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"", KindHelmChart, KindOCIRepository} {
		for _, overrideInterval := range []bool{false, true} {
			t.Run(
				kind+"/override="+map[bool]string{false: "false", true: "true"}[overrideInterval],
				func(t *testing.T) {
					t.Parallel()
					testGeneratedObjectsChartTemplateCase(
						t,
						&testGeneratedObjectsChartTemplateCaseInputs{kind: &kind, overrideInterval: &overrideInterval},
					)
				},
			)
		}
	}
}

func TestGeneratedObjects_OCISelector(t *testing.T) {
	t.Parallel()
	for _, selector := range []*sourcev1.OCILayerSelector{nil, {}, {MediaType: "custom", Operation: sourcev1.OCILayerExtract}} {
		t.Run(cmp.Diff(nil, selector), func(t *testing.T) {
			t.Parallel()
			hr := &HelmRelease{
				Name:      "app",
				Namespace: "apps",
				HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: &helmv2.HelmChartTemplate{
					Kind: KindOCIRepository,
					Spec: helmv2.HelmChartTemplateSpec{
						OCIRepositorySpec: sourcev1.OCIRepositorySpec{
							URL:           "oci://registry.example/chart",
							LayerSelector: selector,
						},
					},
				}},
			}
			objects, err := GeneratedObjects(hr)
			assert.NoError(t, err)
			got := objects[0].(*OCIRepository).LayerSelector
			if selector == nil {
				assert.FatalIf(
					t,
					got == nil || got.Operation != sourcev1.OCILayerCopy,
					"%s",
					"nil selector did not receive the chart default",
				)
			} else {
				assert.FatalIf(
					t,
					got == selector || *got != *selector,
					"explicit selector changed or remained borrowed: %v",
					got,
				)
			}
		})
	}
}

func TestGeneratedObjects_EmptyRule(t *testing.T) {
	t.Parallel()
	for _, obj := range []BaseManifest{&Kustomization{}, &HelmRelease{HelmReleaseSpec: helmv2.HelmReleaseSpec{Suspend: true, Chart: &helmv2.HelmChartTemplate{Kind: KindOCIRepository, Spec: helmv2.HelmChartTemplateSpec{OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://registry.example/chart"}}}}}, &HelmRelease{ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "chart"}}} {
		assertGeneratedTargets(t, obj)
		objects, err := GeneratedObjects(obj)
		assert.FatalIf(t, err != nil || objects != nil, "%T generated %v: %v", obj, objects, err)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestReferences_InlineTemplateNoAllocations(t *testing.T) {
	for _, kind := range []string{KindHelmChart, KindOCIRepository} {
		t.Run(kind, func(t *testing.T) {
			template := &helmv2.HelmChartTemplate{
				Kind: kind,
				Spec: helmv2.HelmChartTemplateSpec{
					Chart:     "chart",
					SourceRef: &helmv2.CrossNamespaceObjectReference{Kind: KindGitRepository, Name: "source"},
				},
			}
			if kind == KindOCIRepository {
				template.Spec = helmv2.HelmChartTemplateSpec{
					URL:            "oci://registry.example/chart",
					SecretRef:      &LocalObjectReference{Name: "auth"},
					CertSecretRef:  &LocalObjectReference{Name: "cert"},
					ProxySecretRef: &LocalObjectReference{Name: "proxy"},
				}
			}
			hr := &HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: template}}
			{
				err := hr.normalizeChartSource()
				assert.NoError(t, err)
			}
			refs := slices.Collect(References(hr, ReferenceRelations))
			limits := hr.ReferenceCapacity()
			capacity := limits.References
			assert.FatalIf(t, len(refs) == 0 || len(refs) > capacity, "references=%v capacity=%d", refs, capacity)
			{
				allocations := testing.AllocsPerRun(100, func() {
					for range References(hr, ReferenceRelations) {
					}
				})
				assert.FatalIf(t, allocations != 0, "reference iteration allocates %g times", allocations)
			}
		})
	}
}

//nolint:paralleltest,tparallel // Subtests finish clone checks before the parent reuses the shared original.
func TestHelmRelease_HistoryTemplatePolicy(t *testing.T) {
	t.Parallel()
	hr := &HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{
		Chart: &helmv2.HelmChartTemplate{
			Kind: KindOCIRepository,
			ObjectMeta: &helmv2.HelmChartTemplateObjectMeta{
				Labels: map[string]string{"label": "one"}, Annotations: map[string]string{"annotation": "one"},
			},
			Spec: helmv2.HelmChartTemplateSpec{
				Chart:             "chart",
				Version:           "one",
				ReconcileStrategy: "ChartVersion",
				ValuesFiles:       []string{"first", "second"},
				SourceRef: &helmv2.CrossNamespaceObjectReference{
					APIVersion: "v1", Kind: KindHelmRepository, Namespace: "charts", Name: "repository",
				},
				OCIRepositorySpec: sourcev1.OCIRepositorySpec{
					URL:       "oci://registry.example/app",
					Reference: &sourcev1.OCIRepositoryRef{Tag: "one"},
					Interval:  &metav1.Duration{Duration: time.Minute},
					Timeout:   &metav1.Duration{Duration: time.Minute},
					Ignore:    new("one"),
					SecretRef: &LocalObjectReference{Name: "auth"},
					CertSecretRef: &LocalObjectReference{
						Name: "cert",
					},
					ProxySecretRef: &LocalObjectReference{Name: "proxy"},
					LayerSelector:  &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy},
					Verify: &sourcev1.OCIRepositoryVerification{
						Provider: "cosign", SecretRef: &LocalObjectReference{Name: "key"},
						TrustedRootSecretRef: &LocalObjectReference{Name: "trusted"},
						MatchOIDCIdentity: []sourcev1.OIDCIdentityMatch{
							{Issuer: "issuer", Subject: "subject"}, {Issuer: "other", Subject: "other"},
						},
					},
				},
			},
		},
	}}
	before := hr.HistoryPolicy()
	for _, tc := range []struct {
		name   string
		change func(*helmv2.HelmChartTemplate)
	}{
		{"kind", func(t *helmv2.HelmChartTemplate) { t.Kind = KindHelmChart }},
		{"chart", func(t *helmv2.HelmChartTemplate) { t.Spec.Chart = "other" }},
		{"version", func(t *helmv2.HelmChartTemplate) { t.Spec.Version = "other" }},
		{"strategy", func(t *helmv2.HelmChartTemplate) { t.Spec.ReconcileStrategy = "Revision" }},
		{"missing values", func(t *helmv2.HelmChartTemplate) { t.Spec.IgnoreMissingValuesFiles = true }},
		{"source API", func(t *helmv2.HelmChartTemplate) { t.Spec.SourceRef.APIVersion = "other" }},
		{"source kind", func(t *helmv2.HelmChartTemplate) { t.Spec.SourceRef.Kind = "other" }},
		{"source name", func(t *helmv2.HelmChartTemplate) { t.Spec.SourceRef.Name = "other" }},
		{"source namespace", func(t *helmv2.HelmChartTemplate) { t.Spec.SourceRef.Namespace = "other" }},
		{"source presence", func(t *helmv2.HelmChartTemplate) { t.Spec.SourceRef = nil }},
		{"values file", func(t *helmv2.HelmChartTemplate) { t.Spec.ValuesFiles[0] = "other" }},
		{"values order", func(t *helmv2.HelmChartTemplate) { slices.Reverse(t.Spec.ValuesFiles) }},
		{"values length", func(t *helmv2.HelmChartTemplate) { t.Spec.ValuesFiles = t.Spec.ValuesFiles[:1] }},
		{"metadata presence", func(t *helmv2.HelmChartTemplate) { t.ObjectMeta = nil }},
		{"digest", func(t *helmv2.HelmChartTemplate) { t.Spec.Reference.Digest = "other" }},
		{"semver", func(t *helmv2.HelmChartTemplate) { t.Spec.Reference.SemVer = "other" }},
		{"semver filter", func(t *helmv2.HelmChartTemplate) { t.Spec.Reference.SemverFilter = "other" }},
		{"reference presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Reference = nil }},
		{"media type", func(t *helmv2.HelmChartTemplate) { t.Spec.LayerSelector.MediaType = "other" }},
		{"selector presence", func(t *helmv2.HelmChartTemplate) { t.Spec.LayerSelector = nil }},
		{"auth presence", func(t *helmv2.HelmChartTemplate) { t.Spec.SecretRef = nil }},
		{"cert presence", func(t *helmv2.HelmChartTemplate) { t.Spec.CertSecretRef = nil }},
		{"proxy presence", func(t *helmv2.HelmChartTemplate) { t.Spec.ProxySecretRef = nil }},
		{"interval presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Interval = nil }},
		{"timeout presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Timeout = nil }},
		{"ignore presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Ignore = nil }},
		{"verification presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify = nil }},
		{"verification provider", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.Provider = "other" }},
		{"verification key presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.SecretRef = nil }},
		{"trusted root", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.TrustedRootSecretRef.Name = "other" }},
		{"trusted root presence", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.TrustedRootSecretRef = nil }},
		{"OIDC issuer", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.MatchOIDCIdentity[0].Issuer = "other" }},
		{"OIDC subject", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.MatchOIDCIdentity[0].Subject = "other" }},
		{"OIDC order", func(t *helmv2.HelmChartTemplate) { slices.Reverse(t.Spec.Verify.MatchOIDCIdentity) }},
		{"OIDC length", func(t *helmv2.HelmChartTemplate) {
			t.Spec.Verify.MatchOIDCIdentity = t.Spec.Verify.MatchOIDCIdentity[:1]
		}},
		{"url", func(t *helmv2.HelmChartTemplate) { t.Spec.URL += "/other" }},
		{"ref", func(t *helmv2.HelmChartTemplate) { t.Spec.Reference.Tag = "other" }},
		{"selector", func(t *helmv2.HelmChartTemplate) { t.Spec.LayerSelector.Operation = sourcev1.OCILayerExtract }},
		{"auth", func(t *helmv2.HelmChartTemplate) { t.Spec.SecretRef.Name = "other" }},
		{"cert", func(t *helmv2.HelmChartTemplate) { t.Spec.CertSecretRef.Name = "other" }},
		{"proxy", func(t *helmv2.HelmChartTemplate) { t.Spec.ProxySecretRef.Name = "other" }},
		{"verify", func(t *helmv2.HelmChartTemplate) { t.Spec.Verify.SecretRef.Name = "other" }},
		{"interval", func(t *helmv2.HelmChartTemplate) { t.Spec.Interval.Duration *= 2 }},
		{"timeout", func(t *helmv2.HelmChartTemplate) { t.Spec.Timeout.Duration *= 2 }},
		{"ignore", func(t *helmv2.HelmChartTemplate) { *t.Spec.Ignore = "other" }},
		{"provider", func(t *helmv2.HelmChartTemplate) { t.Spec.Provider = "aws" }},
		{"service account", func(t *helmv2.HelmChartTemplate) { t.Spec.ServiceAccountName = "other" }},
		{"suspend", func(t *helmv2.HelmChartTemplate) { t.Spec.Suspend = true }},
		{"insecure", func(t *helmv2.HelmChartTemplate) { t.Spec.Insecure = true }},
		{"labels", func(t *helmv2.HelmChartTemplate) { t.ObjectMeta.Labels["label"] = "other" }},
		{"annotations", func(t *helmv2.HelmChartTemplate) { t.ObjectMeta.Annotations["annotation"] = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := hr.Clone()
			tc.change(changed.HelmReleaseSpec.Chart)
			originalTemplate, changedTemplate := hr.HelmReleaseSpec.Chart, changed.HelmReleaseSpec.Chart
			assert.FatalIf(t, equalChartTemplate(originalTemplate, changedTemplate) ||
				equalChartTemplate(changedTemplate, originalTemplate), "%s", "changed template compared equal")
			assert.FatalIf(
				t,
				before.Equal(changed.HistoryPolicy()) || !before.Equal(hr.HistoryPolicy()),
				"%s",
				"template policy change reused history or mutated its predecessor",
			)
		})
	}
	assert.FatalIf(t, before.Equal(hr.WithNamespace("other").(*HelmRelease).HistoryPolicy()) ||
		!before.Equal(
			hr.Clone().HistoryPolicy(),
		), "%s", "effective identity or no-op template history policy changed")
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestEqual_HelmReleaseProjectionCache(t *testing.T) {
	hr := (&HelmRelease{Name: "app", Namespace: "apps", ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "chart"}}).WithNamespace("apps").(*HelmRelease)
	clone := hr.Clone()
	{
		allocations := testing.AllocsPerRun(100, func() {
			if !Equal(hr, clone) {
				t.Fatal("derived cache state changed declaration equality")
			}
		})
		assert.FatalIf(t, allocations != 0, "equal chart declaration comparison allocates %g times", allocations)
	}
	clone.ChartRef.Name = "other"
	assert.FatalIf(t, Equal(hr, clone), "%v", "changed chart declaration compared equal")
}

func TestEqualChartTemplate_OptionalValues(t *testing.T) {
	t.Parallel()
	assert.FatalIf(
		t,
		!equalChartTemplate(nil, nil) || equalChartTemplate(nil, &helmv2.HelmChartTemplate{}),
		"%s",
		"template presence differs",
	)
	assert.FatalIf(
		t,
		equalChartTemplate(&helmv2.HelmChartTemplate{}, nil),
		"%s",
		"template presence comparison is asymmetric",
	)
	for _, tc := range []struct {
		name   string
		mutate func(*helmv2.HelmChartTemplate)
	}{
		{"values", func(c *helmv2.HelmChartTemplate) { c.Spec.ValuesFiles = []string{} }},
		{"labels", func(c *helmv2.HelmChartTemplate) { c.ObjectMeta.Labels = map[string]string{} }},
		{"annotations", func(c *helmv2.HelmChartTemplate) { c.ObjectMeta.Annotations = map[string]string{} }},
		{"OIDC identities", func(c *helmv2.HelmChartTemplate) { c.Spec.Verify.MatchOIDCIdentity = []sourcev1.OIDCIdentityMatch{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testEqualChartTemplateOptionalValuesCase(t, &tc)
		},
		)
	}
	assert.FatalIf(
		t,
		!equalChartTemplate(&helmv2.HelmChartTemplate{}, &helmv2.HelmChartTemplate{}),
		"%s",
		"absent optional fields differ",
	)
}

type testGeneratedObjectsChartTemplateCaseInputs struct {
	kind             *string
	overrideInterval *bool
}

func testGeneratedObjectsChartTemplateCase(t *testing.T, testCaseInput *testGeneratedObjectsChartTemplateCaseInputs) {
	t.Helper()

	template := &helmv2.HelmChartTemplate{
		Kind: (*testCaseInput.kind),
		ObjectMeta: &helmv2.HelmChartTemplateObjectMeta{
			Labels:      map[string]string{"custom": "label", HelmReleaseDomain + "/name": "wrong"},
			Annotations: map[string]string{"custom": "annotation"},
		},
		Spec: helmv2.HelmChartTemplateSpec{
			Chart:             "./chart",
			Version:           "1.2.3",
			ReconcileStrategy: sourcev1.ReconcileStrategyRevision,
			SourceRef: &helmv2.CrossNamespaceObjectReference{
				Kind:      KindGitRepository,
				Name:      "repository",
				Namespace: "sources",
			},
			ValuesFiles:              []string{"values.yaml"},
			IgnoreMissingValuesFiles: true,
		},
	}
	if (*testCaseInput.kind) == KindOCIRepository {
		template.Spec = helmv2.HelmChartTemplateSpec{
			URL: "oci://registry.example/charts/app",
			Reference: &sourcev1.OCIRepositoryRef{
				Digest:       "sha256:abc",
				Tag:          "v1",
				SemVer:       "1.x",
				SemverFilter: "v.*",
			},
			Provider:           "aws",
			ServiceAccountName: "registry",
			Insecure:           true,
			Suspend:            true,
			Ignore:             new("ignored"),
			SecretRef: &LocalObjectReference{
				Name: "auth",
			},
			CertSecretRef:  &LocalObjectReference{Name: "cert"},
			ProxySecretRef: &LocalObjectReference{Name: "proxy"},
			Timeout: &metav1.Duration{
				Duration: time.Minute,
			},
			Verify: &sourcev1.OCIRepositoryVerification{
				Provider:             "notation",
				SecretRef:            &LocalObjectReference{Name: "keys"},
				TrustedRootSecretRef: &LocalObjectReference{Name: "root"},
			},
		}
	}
	if *testCaseInput.overrideInterval {
		template.Spec.Interval = &metav1.Duration{Duration: 2 * time.Minute}
	}
	hr := &HelmRelease{
		Name:      "app",
		Namespace: "authored",
		HelmReleaseSpec: helmv2.HelmReleaseSpec{
			Chart:           template,
			Interval:        metav1.Duration{Duration: time.Hour},
			TargetNamespace: "target",
			ReleaseName:     "installed",
		},
	}
	before := hr.DeepCopy()
	projected := hr.WithNamespace("effective").(*HelmRelease)
	assertGeneratedTargets(t, projected)
	objects, err := GeneratedObjects(projected)
	assert.FatalIf(t, err != nil || len(objects) != 1, "objects=%v error=%v", objects, err)
	release := helmv2.HelmRelease{
		Name:      projected.Name,
		Namespace: projected.Namespace,
		Spec:      projected.HelmReleaseSpec,
	}
	ref := release.GetHelmChartTemplateReference()
	{
		got := objects[0].Named()
		assert.FatalIf(
			t,
			got != (NamedResource{Group: SourceDomain, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}),
			"generated identity=%s, upstream=%+v",
			got,
			ref,
		)
	}
	subject := DiagnosticFor(objects[0].Named(), objects[0])
	assert.FatalIf(
		t,
		subject.ID != objects[0].Named() || subject.Resource != projected.Named(),
		"pre-emission diagnostic = %+v",
		subject,
	)
	copied := objects[0].WithNamespace("another-source-namespace")
	{
		got := DiagnosticFor(copied.Named(), copied)
		assert.FatalIf(t, got.Resource != projected.Named(), "namespace moved real owner: %+v", got)
	}
	var labels, annotations map[string]string
	if chart, ok := objects[0].(*HelmChartSource); ok {
		labels, annotations = chart.Labels, chart.Annotations
		want := sourcev1.HelmChartSpec{
			Chart:             template.Spec.Chart,
			Version:           template.Spec.Version,
			Interval:          release.GetTemplateInterval(),
			ReconcileStrategy: template.Spec.ReconcileStrategy,
			SourceRef: sourcev1.LocalHelmChartSourceReference{
				Kind: KindGitRepository,
				Name: "repository",
			},
			ValuesFiles:              template.Spec.ValuesFiles,
			IgnoreMissingValuesFiles: true,
		}
		{
			diff := cmp.Diff(want, chart.HelmChartSpec)
			assert.FatalIf(t, diff != "", "%v", diff)
		}
		chart.ValuesFiles[0] = "mutated"
	} else {
		source := objects[0].(*OCIRepository)
		labels, annotations = source.Labels, source.Annotations
		want := template.Spec.OCIRepositorySpec.DeepCopy()
		interval := release.GetTemplateInterval()
		want.Interval = &interval
		want.LayerSelector = &sourcev1.OCILayerSelector{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Operation: sourcev1.OCILayerCopy,
		}
		{
			diff := cmp.Diff(want, &source.OCIRepositorySpec)
			assert.FatalIf(t, diff != "", "%v", diff)
		}
		source.Reference.Tag, source.Verify.SecretRef.Name, *source.Ignore = "mutated", "mutated", "mutated"
	}
	assert.FatalIf(t, labels[HelmReleaseDomain+"/name"] != "app" ||
		labels[HelmReleaseDomain+"/namespace"] != "effective" ||
		labels["custom"] != "label" ||
		annotations["custom"] != "annotation", "metadata=%v %v", labels, annotations)
	labels["custom"], annotations["custom"] = "mutated", "mutated"
	{
		diff := cmp.Diff(before, &hr.HelmReleaseSpec)
		assert.FatalIf(t, diff != "", "borrowed declaration changed: %s", diff)
	}
}

func testEqualChartTemplateOptionalValuesCase(
	t *testing.T,
	testCaseInput *struct {
		name   string
		mutate func(*helmv2.HelmChartTemplate)
	},
) {
	t.Helper()

	original := &helmv2.HelmChartTemplate{
		ObjectMeta: &helmv2.HelmChartTemplateObjectMeta{},
		Spec: helmv2.HelmChartTemplateSpec{OCIRepositorySpec: sourcev1.OCIRepositorySpec{
			Verify: &sourcev1.OCIRepositoryVerification{},
		}},
	}
	changed := original.DeepCopy()
	testCaseInput.mutate(changed)
	assert.FatalIf(
		t,
		!equalChartTemplate(original, changed) || !equalChartTemplate(changed, original),
		"%s",
		"nil and empty collection semantics differ",
	)
}

func TestGeneratedTargets_InvalidTemplate(t *testing.T) {
	t.Parallel()
	for _, template := range []*helmv2.HelmChartTemplate{
		{Kind: "Bucket"},
		{Kind: KindHelmChart},
		{Kind: KindOCIRepository},
	} {
		t.Run(template.Kind, func(t *testing.T) {
			t.Parallel()
			assertGeneratedTargets(
				t,
				&HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: template}},
			)
		})
	}
}

func assertGeneratedTargets(t *testing.T, object BaseManifest) {
	t.Helper()
	objects, objectErr := GeneratedObjects(object)
	targets, targetErr := GeneratedTargets(object)
	assert.Equal(t, targetErr == nil, objectErr == nil)
	assert.Equal(t, errors.Is(targetErr, ErrInput), errors.Is(objectErr, ErrInput))
	assert.Equal(t, errors.Is(targetErr, ErrFlux), errors.Is(objectErr, ErrFlux))
	identities := make([]NamedResource, len(objects))
	for i, child := range objects {
		identities[i] = child.Named()
	}
	projected := make([]NamedResource, len(targets))
	for i, target := range targets {
		projected[i] = target.NamedResource
	}
	assert.Diff(t, projected, identities)
}
