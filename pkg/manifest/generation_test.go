package manifest

import (
	"slices"
	"testing"
	"time"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGeneratedObjects_ChartTemplate(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"", KindHelmChart, KindOCIRepository} {
		for _, overrideInterval := range []bool{false, true} {
			t.Run(kind+"/override="+map[bool]string{false: "false", true: "true"}[overrideInterval], func(t *testing.T) {
				t.Parallel()
				template := &helmv2.HelmChartTemplate{Kind: kind,
					ObjectMeta: &helmv2.HelmChartTemplateObjectMeta{Labels: map[string]string{"custom": "label", HelmReleaseDomain + "/name": "wrong"}, Annotations: map[string]string{"custom": "annotation"}},
					Spec: helmv2.HelmChartTemplateSpec{Chart: "./chart", Version: "1.2.3", ReconcileStrategy: sourcev1.ReconcileStrategyRevision,
						SourceRef:   &helmv2.CrossNamespaceObjectReference{Kind: KindGitRepository, Name: "repository", Namespace: "sources"},
						ValuesFiles: []string{"values.yaml"}, IgnoreMissingValuesFiles: true}}
				if kind == KindOCIRepository {
					template.Spec = helmv2.HelmChartTemplateSpec{
						URL: "oci://registry.example/charts/app", Reference: &sourcev1.OCIRepositoryRef{Digest: "sha256:abc", Tag: "v1", SemVer: "1.x", SemverFilter: "v.*"},
						Provider: "aws", ServiceAccountName: "registry", Insecure: true, Suspend: true, Ignore: new("ignored"),
						SecretRef: &LocalObjectReference{Name: "auth"}, CertSecretRef: &LocalObjectReference{Name: "cert"}, ProxySecretRef: &LocalObjectReference{Name: "proxy"},
						Timeout: &metav1.Duration{Duration: time.Minute}, Verify: &sourcev1.OCIRepositoryVerification{Provider: "notation", SecretRef: &LocalObjectReference{Name: "keys"}, TrustedRootSecretRef: &LocalObjectReference{Name: "root"}}}
				}
				if overrideInterval {
					template.Spec.Interval = &metav1.Duration{Duration: 2 * time.Minute}
				}
				hr := &HelmRelease{Name: "app", Namespace: "authored", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: template, Interval: metav1.Duration{Duration: time.Hour}, TargetNamespace: "target", ReleaseName: "installed"}}
				before := hr.DeepCopy()
				projected := hr.WithNamespace("effective").(*HelmRelease)
				objects, err := GeneratedObjects(projected)
				if err != nil || len(objects) != 1 {
					t.Fatalf("objects=%v error=%v", objects, err)
				}
				release := helmv2.HelmRelease{Name: projected.Name, Namespace: projected.Namespace, Spec: projected.HelmReleaseSpec}
				ref := release.GetHelmChartTemplateReference()
				if got := objects[0].Named(); got != (NamedResource{Group: SourceDomain, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name}) {
					t.Fatalf("generated identity=%s, upstream=%+v", got, ref)
				}
				subject := DiagnosticFor(objects[0].Named(), objects[0])
				if subject.ID != objects[0].Named() || subject.Resource != projected.Named() {
					t.Fatalf("pre-emission diagnostic = %+v", subject)
				}
				copied := objects[0].WithNamespace("another-source-namespace")
				if got := DiagnosticFor(copied.Named(), copied); got.Resource != projected.Named() {
					t.Fatalf("namespace moved real owner: %+v", got)
				}
				var labels, annotations map[string]string
				if chart, ok := objects[0].(*HelmChartSource); ok {
					labels, annotations = chart.Labels, chart.Annotations
					want := sourcev1.HelmChartSpec{Chart: template.Spec.Chart, Version: template.Spec.Version, Interval: release.GetTemplateInterval(),
						ReconcileStrategy: template.Spec.ReconcileStrategy, SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindGitRepository, Name: "repository"},
						ValuesFiles: template.Spec.ValuesFiles, IgnoreMissingValuesFiles: true}
					if diff := cmp.Diff(want, chart.HelmChartSpec); diff != "" {
						t.Fatal(diff)
					}
					chart.ValuesFiles[0] = "mutated"
				} else {
					source := objects[0].(*OCIRepository)
					labels, annotations = source.Labels, source.Annotations
					want := template.Spec.OCIRepositorySpec.DeepCopy()
					interval := release.GetTemplateInterval()
					want.Interval = &interval
					want.LayerSelector = &sourcev1.OCILayerSelector{MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip", Operation: sourcev1.OCILayerCopy}
					if diff := cmp.Diff(want, &source.OCIRepositorySpec); diff != "" {
						t.Fatal(diff)
					}
					source.Reference.Tag, source.Verify.SecretRef.Name, *source.Ignore = "mutated", "mutated", "mutated"
				}
				if labels[HelmReleaseDomain+"/name"] != "app" || labels[HelmReleaseDomain+"/namespace"] != "effective" || labels["custom"] != "label" || annotations["custom"] != "annotation" {
					t.Fatalf("metadata=%v %v", labels, annotations)
				}
				labels["custom"], annotations["custom"] = "mutated", "mutated"
				if diff := cmp.Diff(before, &hr.HelmReleaseSpec); diff != "" {
					t.Fatalf("borrowed declaration changed: %s", diff)
				}
			})
		}
	}
}

func TestGeneratedObjects_OCISelector(t *testing.T) {
	t.Parallel()
	for _, selector := range []*sourcev1.OCILayerSelector{nil, {}, {MediaType: "custom", Operation: sourcev1.OCILayerExtract}} {
		t.Run(cmp.Diff(nil, selector), func(t *testing.T) {
			t.Parallel()
			hr := &HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: &helmv2.HelmChartTemplate{Kind: KindOCIRepository,
				Spec: helmv2.HelmChartTemplateSpec{OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://registry.example/chart", LayerSelector: selector}}}}}
			objects, err := GeneratedObjects(hr)
			if err != nil {
				t.Fatal(err)
			}
			got := objects[0].(*OCIRepository).LayerSelector
			if selector == nil {
				if got == nil || got.Operation != sourcev1.OCILayerCopy {
					t.Fatal("nil selector did not receive the chart default")
				}
			} else if got == selector || *got != *selector {
				t.Fatalf("explicit selector changed or remained borrowed: %v", got)
			}
		})
	}
}

func TestGeneratedObjects_EmptyRule(t *testing.T) {
	t.Parallel()
	for _, obj := range []BaseManifest{&Kustomization{}, &HelmRelease{HelmReleaseSpec: helmv2.HelmReleaseSpec{Suspend: true, Chart: &helmv2.HelmChartTemplate{Kind: KindOCIRepository, Spec: helmv2.HelmChartTemplateSpec{OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://registry.example/chart"}}}}}, &HelmRelease{ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "chart"}}} {
		objects, err := GeneratedObjects(obj)
		if err != nil || objects != nil {
			t.Fatalf("%T generated %v: %v", obj, objects, err)
		}
	}
}

func TestReferences_InlineTemplateNoAllocations(t *testing.T) {
	for _, kind := range []string{KindHelmChart, KindOCIRepository} {
		t.Run(kind, func(t *testing.T) {
			template := &helmv2.HelmChartTemplate{Kind: kind, Spec: helmv2.HelmChartTemplateSpec{Chart: "chart", SourceRef: &helmv2.CrossNamespaceObjectReference{Kind: KindGitRepository, Name: "source"}}}
			if kind == KindOCIRepository {
				template.Spec = helmv2.HelmChartTemplateSpec{URL: "oci://registry.example/chart", SecretRef: &LocalObjectReference{Name: "auth"}, CertSecretRef: &LocalObjectReference{Name: "cert"}, ProxySecretRef: &LocalObjectReference{Name: "proxy"}}
			}
			hr := &HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: template}}
			if err := hr.normalizeChartSource(); err != nil {
				t.Fatal(err)
			}
			refs := slices.Collect(References(hr, ReferenceRelations))
			capacity, _ := hr.ReferenceCapacity()
			if len(refs) == 0 || len(refs) > capacity {
				t.Fatalf("references=%v capacity=%d", refs, capacity)
			}
			if allocations := testing.AllocsPerRun(100, func() {
				for range References(hr, ReferenceRelations) {
				}
			}); allocations != 0 {
				t.Fatalf("reference iteration allocates %g times", allocations)
			}
		})
	}
}

func TestHelmRelease_HistoryTemplatePolicy(t *testing.T) {
	t.Parallel()
	hr := &HelmRelease{Name: "app", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{Chart: &helmv2.HelmChartTemplate{Kind: KindOCIRepository,
		ObjectMeta: &helmv2.HelmChartTemplateObjectMeta{Labels: map[string]string{"label": "one"}, Annotations: map[string]string{"annotation": "one"}},
		Spec: helmv2.HelmChartTemplateSpec{OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://registry.example/app", Reference: &sourcev1.OCIRepositoryRef{Tag: "one"},
			Interval: &metav1.Duration{Duration: time.Minute}, Timeout: &metav1.Duration{Duration: time.Minute}, Ignore: new("one"),
			SecretRef: &LocalObjectReference{Name: "auth"}, CertSecretRef: &LocalObjectReference{Name: "cert"}, ProxySecretRef: &LocalObjectReference{Name: "proxy"},
			LayerSelector: &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}, Verify: &sourcev1.OCIRepositoryVerification{Provider: "cosign", SecretRef: &LocalObjectReference{Name: "key"}}}}}}}
	before := hr.HistoryPolicy()
	for _, tc := range []struct {
		name   string
		change func(*helmv2.HelmChartTemplate)
	}{
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
			t.Parallel()
			changed := hr.Clone()
			tc.change(changed.HelmReleaseSpec.Chart)
			if before.Equal(changed.HistoryPolicy()) || !before.Equal(hr.HistoryPolicy()) {
				t.Fatal("template policy change reused history or mutated its predecessor")
			}
		})
	}
	if before.Equal(hr.WithNamespace("other").(*HelmRelease).HistoryPolicy()) || !before.Equal(hr.Clone().HistoryPolicy()) {
		t.Fatal("effective identity or no-op template history policy changed")
	}
}

func TestEqual_HelmReleaseProjectionCache(t *testing.T) {
	hr := (&HelmRelease{Name: "app", Namespace: "apps", ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "chart"}}).WithNamespace("apps").(*HelmRelease)
	clone := hr.Clone()
	if allocations := testing.AllocsPerRun(100, func() {
		if !Equal(hr, clone) {
			t.Fatal("derived cache state changed declaration equality")
		}
	}); allocations != 0 {
		t.Fatalf("equal chart declaration comparison allocates %g times", allocations)
	}
	clone.ChartRef.Name = "other"
	if Equal(hr, clone) {
		t.Fatal("changed chart declaration compared equal")
	}
}
