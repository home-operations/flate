package manifest

import (
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHelmRepository_IsOCI(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		repo *HelmRepository
		want bool
	}{
		{"type oci", &HelmRepository{URL: "https://x", Type: sourcev1.HelmRepositoryTypeOCI}, true},
		{"oci:// url no type", &HelmRepository{URL: "oci://reg/x"}, true},
		{"http default", &HelmRepository{URL: "https://charts.example"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.repo.IsOCI(); got != tc.want {
				t.Errorf("IsOCI = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOCIRepository_LayerDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                 string
		selector             *OCILayerSelector
		mediaType, operation string
	}{
		{"nil", nil, "", sourcev1.OCILayerExtract},
		{"empty", &OCILayerSelector{}, "", sourcev1.OCILayerExtract},
		{"copy", &OCILayerSelector{MediaType: "custom", Operation: sourcev1.OCILayerCopy}, "custom", sourcev1.OCILayerCopy},
		{"extract", &OCILayerSelector{Operation: sourcev1.OCILayerExtract}, "", sourcev1.OCILayerExtract},
		{"unsupported", &OCILayerSelector{Operation: "invalid"}, "", "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := &OCIRepository{LayerSelector: tc.selector}
			if repo.LayerMediaType() != tc.mediaType || repo.LayerOperation() != tc.operation {
				t.Fatalf("layer = %q/%q, want %q/%q", repo.LayerMediaType(), repo.LayerOperation(), tc.mediaType, tc.operation)
			}
		})
	}
}

func TestOCIRepositoryRef_Pinned(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ref  *OCIRepositoryRef
		want bool
	}{
		{"nil", nil, false}, {"empty", &OCIRepositoryRef{}, false},
		{"tag", &OCIRepositoryRef{Tag: "v1"}, false},
		{"semver", &OCIRepositoryRef{SemVer: "1.x"}, false},
		{"digest", &OCIRepositoryRef{Digest: "sha256:abc"}, true},
		{"mixed digest", &OCIRepositoryRef{Digest: "sha256:abc", Tag: "v1", SemVer: "1.x"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.ref.Pinned() != tc.want {
				t.Fatalf("Pinned = %t, want %t", tc.ref.Pinned(), tc.want)
			}
		})
	}
}

func TestSource_Declaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		obj  interface {
			ExistenceOnly() bool
			SourceInterval() time.Duration
		}
		existenceOnly bool
		interval      time.Duration
	}{
		{"git", &GitRepository{Interval: metav1.Duration{Duration: time.Minute}}, false, time.Minute},
		{"oci", &OCIRepository{Interval: metav1.Duration{Duration: time.Minute}}, false, time.Minute},
		{"bucket", &Bucket{Interval: metav1.Duration{Duration: time.Minute}}, false, time.Minute},
		{"external", &ExternalArtifact{}, false, 0},
		{"repository", &HelmRepository{Interval: metav1.Duration{Duration: time.Minute}}, true, time.Minute},
		{"chart repository", &HelmChartSource{SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindHelmRepository}, Interval: metav1.Duration{Duration: time.Minute}}, false, time.Minute},
		{"chart default", &HelmChartSource{Interval: metav1.Duration{Duration: time.Minute}}, false, time.Minute},
		{"chart git", &HelmChartSource{SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindGitRepository}, Interval: metav1.Duration{Duration: time.Minute}}, true, time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.obj.ExistenceOnly() != tc.existenceOnly || tc.obj.SourceInterval() != tc.interval {
				t.Fatalf("declaration = %t/%s, want %t/%s", tc.obj.ExistenceOnly(), tc.obj.SourceInterval(), tc.existenceOnly, tc.interval)
			}
		})
	}
}
