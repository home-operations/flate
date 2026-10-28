package manifest

import (
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestHelmChartSource_OCIRepository(t *testing.T) {
	t.Parallel()
	repository := &HelmRepository{Name: "repository", Namespace: "sources", URL: "oci://registry.example/charts/",
		Provider: "aws", SecretRef: &LocalObjectReference{Name: "auth"}, CertSecretRef: &LocalObjectReference{Name: "tls"},
		Insecure: true, Interval: metav1.Duration{Duration: time.Hour}, Timeout: &metav1.Duration{Duration: 3 * time.Minute}}
	chart := NewHelmChartSource(sourcev1.HelmChart{Name: "chart", Namespace: "sources",
		Spec: sourcev1.HelmChartSpec{Chart: "app", Version: "1.0.0"}})
	got := chart.OCIRepository(repository)
	assert.Equal(t, got.Named(), NamedResource{Group: SourceDomain, Kind: KindOCIRepository, Name: chart.Name, Namespace: chart.Namespace})
	assert.Equal(t, DiagnosticFor(got.Named(), got), DiagnosticIdentity{ID: got.Named(), Resource: chart.Named()})
	assert.Equal(t, DiagnosticFor(got.WithNamespace("other").Named(), got.WithNamespace("other")).Resource, chart.Named())
	alias, _ := got.BootstrapAlias(t.TempDir())
	assert.Equal(t, DiagnosticFor(alias.Named(), alias).Resource, chart.Named())
	assert.Equal(t, got.URL, "oci://registry.example/charts/app")
	assert.Equal(t, got.Provider, repository.Provider)
	assert.Equal(t, got.SecretRef, repository.SecretRef)
	assert.Equal(t, got.CertSecretRef, repository.CertSecretRef)
	assert.Equal(t, got.Insecure, true)
	assert.Equal(t, got.SourceInterval(), time.Hour)
	assert.Equal(t, got.LayerOperation(), sourcev1.OCILayerExtract)
	if got.Timeout == repository.Timeout || got.Interval == &repository.Interval {
		t.Fatal("adapter retained mutable duration pointers")
	}
	assert.Equal(t, got.OperationTimeout(), 3*time.Minute)
}

func TestHelmChartSource_OCIRepositoryVersion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, version string
		want          *sourcev1.OCIRepositoryRef
	}{
		{"omitted", "", nil},
		{"exact", "1.20.0", &sourcev1.OCIRepositoryRef{Tag: "1.20.0"}},
		{"prefixed", "v1.20.0", &sourcev1.OCIRepositoryRef{Tag: "v1.20.0"}},
		{"minor", "1.20", &sourcev1.OCIRepositoryRef{Tag: "1.20"}},
		{"major", "1", &sourcev1.OCIRepositoryRef{Tag: "1"}},
		{"wildcard", "1.20.x", &sourcev1.OCIRepositoryRef{SemVer: "1.20.x"}},
		{"uppercase wildcard", "1.20.X", &sourcev1.OCIRepositoryRef{SemVer: "1.20.X"}},
		{"star", "*", &sourcev1.OCIRepositoryRef{SemVer: "*"}},
		{"tilde", "~1.20.0", &sourcev1.OCIRepositoryRef{SemVer: "~1.20.0"}},
		{"caret", "^1.20.0", &sourcev1.OCIRepositoryRef{SemVer: "^1.20.0"}},
		{"range", ">=1.20.0 <1.21.0", &sourcev1.OCIRepositoryRef{SemVer: ">=1.20.0 <1.21.0"}},
		{"digest", "sha256:abc", &sourcev1.OCIRepositoryRef{Digest: "sha256:abc"}},
		{"tag", "latest", &sourcev1.OCIRepositoryRef{Tag: "latest"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chart := NewHelmChartSource(sourcev1.HelmChart{Spec: sourcev1.HelmChartSpec{Chart: "app", Version: tc.version}})
			got := chart.OCIRepository(&HelmRepository{URL: "oci://registry.example/charts"})
			if diff := cmp.Diff(tc.want, got.Reference); diff != "" {
				t.Fatalf("reference (-want +got):\n%s", diff)
			}
		})
	}
}

func TestHelmChartSource_OCIRepositoryTimeout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		timeout *metav1.Duration
		want    time.Duration
	}{
		{"omitted", nil, time.Minute},
		{"zero", &metav1.Duration{}, 0},
		{"explicit", &metav1.Duration{Duration: 3 * time.Minute}, 3 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			chart := NewHelmChartSource(sourcev1.HelmChart{Spec: sourcev1.HelmChartSpec{Chart: "app"}})
			repository := &HelmRepository{URL: "oci://registry.example", Timeout: tc.timeout}
			got := chart.OCIRepository(repository)
			assert.Equal(t, got.OperationTimeout(), tc.want)
			if got.Timeout == nil || got.Timeout == tc.timeout {
				t.Fatal("adapter did not own the effective timeout")
			}
		})
	}
}
