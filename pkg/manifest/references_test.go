package manifest

import (
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func refsForUse(t *testing.T, obj BaseManifest, use string) []DependencyRef {
	t.Helper()
	var refs []DependencyRef
	for ref := range References(obj) {
		if ref.Use == use {
			refs = append(refs, ref)
		}
	}
	return refs
}

func TestReferences_KustomizationPolicy(t *testing.T) {
	obj, _, _, err := ParseDoc(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: authored}
spec:
  sourceRef: {kind: GitRepository, name: source}
  dependsOn:
    - {name: relative, readyExpr: 'true'}
    - {name: explicit, namespace: other}
  decryption: {provider: sops, secretRef: {name: ignored}}
  postBuild:
    substituteFrom:
      - {kind: ConfigMap, name: shared}
      - {kind: Unsupported, name: ignored}
      - {kind: Secret, name: shared}
      - {kind: ConfigMap, name: shared, optional: true}
      - {kind: ConfigMap, name: shared}
`), ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	k := obj.(*Kustomization).Clone()
	k.Namespace = "effective"
	assert.Diff(t, slices.Collect(References(k)), []DependencyRef{
		{Kind: KindGitRepository, Namespace: "effective", Name: "source", Use: "source", Content: true, Ready: true},
		{Kind: KindKustomization, Namespace: "effective", Name: "relative", Use: "dependsOn", Ready: true, ReadyExpr: "true"},
		{Kind: KindKustomization, Namespace: "other", Name: "explicit", Use: "dependsOn", Position: 1, Ready: true},
		{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Content: true, Ready: true},
		{Kind: KindSecret, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 2, Content: true},
		{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 3, Optional: true},
		{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 4, Content: true, Ready: true},
	})
	assert.Equal(t, obj.Named().Namespace, "authored")
}

func TestReferences_HelmReleaseChartIdentities(t *testing.T) {
	for _, namespace := range []string{"", "other"} {
		t.Run(namespace, func(t *testing.T) {
			h, err := parseHelmRelease(mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app, namespace: authored}
spec:
  chartRef: {kind: HelmChart, name: chart, namespace: '`+namespace+`'}
  valuesFrom:
    - {kind: ConfigMap, name: values, optional: true}
    - {kind: Invalid, name: ''}
    - {kind: ConfigMap, name: values}
`))
			if err != nil {
				t.Fatal(err)
			}
			h.Namespace = "effective"
			wantNS := namespace
			if wantNS == "" {
				wantNS = "effective"
			}
			assert.Diff(t, refsForUse(t, h, "chart"), []DependencyRef{{Kind: KindHelmChart, Namespace: wantNS, Name: "chart", Use: "chart", Content: true, Ready: true}})
			if err := h.ResolveChartRef(func(ns, name string) *HelmChartSource {
				assert.Equal(t, ns, wantNS)
				assert.Equal(t, name, "chart")
				return &HelmChartSource{Namespace: "chart-source", Chart: "package", SourceRef: sourcev1.LocalHelmChartSourceReference{Name: "repository"}}
			}); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, refsForUse(t, h, "source"), []DependencyRef{{Kind: KindHelmRepository, Namespace: "chart-source", Name: "repository", Use: "source", Content: true, Ready: true}})
			values := refsForUse(t, h, "valuesFrom")
			assert.Equal(t, len(values), 3)
			assert.Equal(t, values[0].Optional, true)
			assert.Equal(t, values[0].Content, true)
			assert.Equal(t, values[0].Ready, false)
			assert.Equal(t, values[1].Kind, "Invalid")
			assert.Equal(t, values[1].Name, "")
			assert.Equal(t, values[2].Position, 2)
		})
	}
}

func TestReferences_InlineChartEffectiveNamespace(t *testing.T) {
	h, err := parseHelmRelease(mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: app, namespace: authored}
spec:
  chart:
    spec:
      chart: package
      sourceRef: {name: repository}
`))
	if err != nil {
		t.Fatal(err)
	}
	h.Namespace = "effective"
	assert.Diff(t, slices.Collect(References(h)), []DependencyRef{{Kind: KindHelmRepository, Namespace: "effective", Name: "repository", Use: "source", Content: true, Ready: true}})
}

func TestReferences_ResourceSetEdgeSemantics(t *testing.T) {
	obj, _, _, err := ParseDoc(mustYAML(t, `
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: app, namespace: apps}
spec:
  dependsOn:
    - {apiVersion: kustomize.toolkit.fluxcd.io/v1, kind: Kustomization, name: ready, ready: true, readyExpr: 'true'}
    - {apiVersion: kustomize.toolkit.fluxcd.io/v1, kind: Kustomization, name: exists}
    - {apiVersion: rbac.authorization.k8s.io/v1, kind: ClusterRole, name: cluster, namespace: ignored}
    - {apiVersion: foreign.example/v1, kind: ClusterRole, name: foreign}
  inputsFrom:
    - {name: inputs}
    - {selector: {matchLabels: {app: example}}}
    - {kind: Unsupported, name: inputs}
`), ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, slices.Collect(References(obj)), []DependencyRef{
		{Kind: KindKustomization, Namespace: "apps", Name: "ready", Use: "dependsOn", Ready: true, ReadyExpr: "true"},
		{Kind: KindKustomization, Namespace: "apps", Name: "exists", Use: "dependsOn", Position: 1},
		{Kind: "ClusterRole", Name: "cluster", Use: "dependsOn", Position: 2},
		{Kind: "ClusterRole", Namespace: "apps", Name: "foreign", Use: "dependsOn", Position: 3},
		{Kind: KindResourceSetInputProvider, Namespace: "apps", Name: "inputs", Use: "inputsFrom", Content: true},
		{Kind: KindResourceSetInputProvider, Namespace: "apps", Use: "inputsFrom", Position: 1, Content: true},
		{Kind: KindResourceSetInputProvider, Namespace: "apps", Name: "inputs", Use: "inputsFrom", Position: 2, Content: true},
	})
	assert.Equal(t, obj.(*ResourceSet).InputsFrom[2].Kind, "Unsupported")
}

func TestReferences_ActiveCredentials(t *testing.T) {
	obj, _, _, err := ParseDoc(mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: source, namespace: authored}
spec:
  url: oci://example/chart
  secretRef: {name: credentials}
  certSecretRef: {name: certificate}
  proxySecretRef: {name: proxy}
  verify: {provider: cosign, secretRef: {name: ignored}}
`), ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	o := *obj.(*OCIRepository)
	o.Namespace = "effective"
	assert.Diff(t, slices.Collect(References(&o)), []DependencyRef{
		{Kind: KindSecret, Namespace: "effective", Name: "credentials", Use: "secretRef", Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "certificate", Use: "certSecretRef", Position: 1, Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "proxy", Use: "proxySecretRef", Position: 2, Content: true},
	})
	for ref := range References(&o) {
		assert.Equal(t, ref.Use, "secretRef")
		break
	}
}
