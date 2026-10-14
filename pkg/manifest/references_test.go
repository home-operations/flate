package manifest

import (
	"slices"
	"testing"
	"unsafe"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
)

func refsForUse(t *testing.T, obj BaseManifest, use string) []Reference {
	t.Helper()
	var refs []Reference
	for ref := range References(obj) {
		if ref.Use == use {
			refs = append(refs, ref)
		}
	}
	return refs
}

func TestReferences_KustomizationPolicy(t *testing.T) {
	k := &Kustomization{Namespace: "effective"}
	assert.Equal(t, len(slices.Collect(References(k))), 0)
	k.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: KindGitRepository, Name: "source"}
	k.DependsOn = []kustomizev1.DependencyReference{{Name: "relative", ReadyExpr: "true"}, {Name: "explicit", Namespace: "other"}}
	k.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{
		{Kind: KindConfigMap, Name: "shared"}, {Kind: "Unsupported"}, {Kind: KindSecret, Name: "shared"}, {Kind: KindConfigMap, Name: "shared", Optional: true}, {Kind: KindConfigMap, Name: "shared"},
	}}
	assert.Diff(t, slices.Collect(References(k)), []Reference{
		{Kind: KindGitRepository, Namespace: "effective", Name: "source", Use: "source", Content: true},
		{Kind: KindKustomization, Namespace: "effective", Name: "relative", Use: "dependsOn", ReadyExpr: "true"},
		{Kind: KindKustomization, Namespace: "other", Name: "explicit", Use: "dependsOn", Position: 1},
		{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 2, Content: true},
		{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 3, Optional: true},
		{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 4, Content: true},
	})
}

func TestReferences_HelmReleaseEffectiveNamespace(t *testing.T) {
	hr := &HelmRelease{Namespace: "effective", Chart: HelmChart{RepoKind: KindOCIRepository, RepoNamespace: "authored", RepoName: "chart"}}
	hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "chart"}
	hr.DependsOn = []helmv2.DependencyReference{{Name: "parent"}}
	hr.ValuesFrom = []ValuesReference{{Kind: KindConfigMap, Name: "vars", Optional: true}}
	assert.Diff(t, slices.Collect(References(hr)), []Reference{
		{Kind: KindOCIRepository, Namespace: "effective", Name: "chart", Use: "chart", Content: true},
		{Kind: KindHelmRelease, Namespace: "effective", Name: "parent", Use: "dependsOn"},
		{Kind: KindConfigMap, Namespace: "effective", Name: "vars", Use: "valuesFrom", Optional: true, Content: true},
	})
	hr.Chart = HelmChart{RepoKind: KindGitRepository, RepoName: "resolved"}
	assert.Diff(t, refsForUse(t, hr, "source"), []Reference{{Kind: KindGitRepository, Namespace: "effective", Name: "resolved", Use: "source", Content: true}})
}

func TestReferences_ResourceSetEdgeSemantics(t *testing.T) {
	r := &ResourceSet{Namespace: "apps"}
	r.DependsOn = []fluxopv1.Dependency{{Kind: KindKustomization, Name: "ready", Ready: true, ReadyExpr: "true"}, {Kind: KindKustomization, Name: "exists"}, {APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "cluster", Namespace: "ignored"}}
	r.InputsFrom = []fluxopv1.InputProviderReference{{Kind: "Unsupported", Name: "inputs"}, {Selector: &metav1.LabelSelector{}}}
	assert.Diff(t, slices.Collect(References(r)), []Reference{
		{Kind: KindKustomization, Namespace: "apps", Name: "ready", Use: "dependsOn", ReadyExpr: "true"},
		{Kind: KindKustomization, Namespace: "apps", Name: "exists", Use: "dependsOn", Position: 1},
		{Kind: "ClusterRole", Name: "cluster", Use: "dependsOn", Position: 2},
		{Kind: KindResourceSetInputProvider, Namespace: "apps", Name: "inputs", Use: "inputsFrom", Content: true},
		{Kind: KindResourceSetInputProvider, Namespace: "apps", Use: "inputsFrom", Position: 1, Content: true},
	})
	assert.Equal(t, r.InputsFrom[0].Kind, "Unsupported")
}

func TestReferences_ActiveCredentials(t *testing.T) {
	o := &OCIRepository{Namespace: "effective"}
	o.SecretRef, o.CertSecretRef, o.ProxySecretRef = &LocalObjectReference{Name: "auth"}, &LocalObjectReference{Name: "cert"}, &LocalObjectReference{Name: "proxy"}
	o.Verify = &sourcev1.OCIRepositoryVerification{Provider: "cosign", SecretRef: &LocalObjectReference{Name: "ignored"}}
	assert.Diff(t, slices.Collect(References(o)), []Reference{
		{Kind: KindSecret, Namespace: "effective", Name: "auth", Use: "secretRef", Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "cert", Use: "certSecretRef", Position: 1, Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "proxy", Use: "proxySecretRef", Position: 2, Content: true},
	})
}

func TestReferences_NoAllocations(t *testing.T) {
	var kinds []string
	for info := range Kinds() {
		if info.Includes("test") {
			kinds = append(kinds, info.Kind)
		}
	}
	assert.Diff(t, kinds, []string{KindKustomization, KindHelmRelease, KindGitRepository, KindOCIRepository, KindHelmRepository, KindBucket})
	assert.Equal(t, KindOf(nil), KindInfo{})
	assert.Equal(t, KindOf(&Kustomization{}), kindTable[kindKustomization].KindInfo)
	if unsafe.Sizeof(uintptr(0)) == 8 {
		assert.Equal(t, unsafe.Sizeof(DependencyRef{}), uintptr(64))
		assert.Equal(t, unsafe.Sizeof(Reference{}), uintptr(96))
		assert.Equal(t, unsafe.Sizeof(KindInfo{}), uintptr(40))
	}
	for _, obj := range []BaseManifest{&Kustomization{SourceRef: kustomizev1.CrossNamespaceSourceReference{Name: "source"}, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{{Kind: KindConfigMap, Name: "values"}}}}, &HelmRelease{ValuesFrom: []ValuesReference{{Name: "values"}}}, &ResourceSet{InputsFrom: []fluxopv1.InputProviderReference{{Name: "input"}}}, &GitRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, &OCIRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, &HelmRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, &Bucket{SecretRef: &LocalObjectReference{Name: "auth"}}} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			assert.Equal(t, testing.AllocsPerRun(100, func() { KindOf(obj) }), float64(0))
			count := 0
			assert.Equal(t, testing.AllocsPerRun(100, func() {
				for range References(obj) {
					count++
				}
			}), float64(0))
			stops := 0
			References(obj)(func(Reference) bool { stops++; return false })
			assert.Equal(t, stops, 1)
			assert.Equal(t, count > 0, true)
		})
	}
}
