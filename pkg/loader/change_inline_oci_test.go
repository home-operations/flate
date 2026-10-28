package loader

import (
	"slices"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_InlineOCICredentialChangeSelectsRelease(t *testing.T) {
	t.Parallel()
	for _, credential := range []string{"secretRef", "certSecretRef", "proxySecretRef"} {
		for _, form := range []string{"declared", "indexed_only", "emitted"} {
			t.Run(credential+"/"+form, func(t *testing.T) {
				t.Parallel()
				st := store.New()
				input := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "credentials"}
				release := &manifest.HelmRelease{Name: "consumer", Namespace: "apps", HelmReleaseSpec: helmv2.HelmReleaseSpec{
					TargetNamespace: "rendered", Chart: &helmv2.HelmChartTemplate{Kind: manifest.KindOCIRepository, Spec: helmv2.HelmChartTemplateSpec{OCIRepositorySpec: sourcev1.OCIRepositorySpec{URL: "oci://example.test/chart"}}},
				}}
				ref := &manifest.LocalObjectReference{Name: input.Name}
				switch credential {
				case "secretRef":
					release.HelmReleaseSpec.Chart.Spec.SecretRef = ref
				case "certSecretRef":
					release.HelmReleaseSpec.Chart.Spec.CertSecretRef = ref
				case "proxySecretRef":
					release.HelmReleaseSpec.Chart.Spec.ProxySecretRef = ref
				}
				sibling := release.Clone()
				sibling.Name = "sibling"
				sibling.HelmReleaseSpec.Chart.Spec.SecretRef = nil
				sibling.HelmReleaseSpec.Chart.Spec.CertSecretRef = nil
				sibling.HelmReleaseSpec.Chart.Spec.ProxySecretRef = nil
				st.AddObject(sibling)
				parent := testutil.KustomizationID("apps", "parent")
				records := map[manifest.NamedResource]manifest.DiscoveryRecord{
					input: {File: "credentials.yaml"}, sibling.Named(): {File: "sibling.yaml"}, parent: {File: "parent.yaml"},
				}
				if form != "emitted" {
					records[release.Named()] = manifest.DiscoveryRecord{File: "release.yaml", Refs: slices.Collect(manifest.ContentInputs(release, true))}
				}
				if form == "declared" {
					st.AddObject(release)
				}
				index := &manifest.DiscoveryIndex{Records: records}
				f := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"credentials.yaml"}), index: index, inventory: st})
				if form == "emitted" {
					f.AddEmitted(parent, store.Child{Obj: release}, st)
				}
				assert.Equal(t, f.ShouldReconcile(release.Named()), true)
				assert.Equal(t, f.ShouldReconcile(sibling.Named()), false)
				if st.GetObject(release.ChartSource()) != nil || len(st.ListObjects(manifest.SourceDomain, manifest.KindOCIRepository)) != 0 {
					t.Fatal("selection depended on generated-source publication")
				}
			})
		}
	}
}
