package manifest

import (
	"encoding/json"
	"slices"
	"testing"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

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
	count := 0
	for ref := range References(obj, ReferenceRelations) {
		count++
		if ref.Use == use {
			refs = append(refs, ref)
		}
	}
	capacity, _ := obj.ReferenceCapacity()
	if count > capacity {
		t.Fatalf("reference capacity %d is below the %d authored references", capacity, count)
	}
	return refs
}

func TestReferenceCapacity_DataInputBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		obj  BaseManifest
		data int
	}{
		{"source", &Kustomization{SourceRef: kustomizev1.CrossNamespaceSourceReference{Kind: KindGitRepository, Name: "source"}}, 0},
		{"ordering", &Kustomization{DependsOn: []kustomizev1.DependencyReference{{Name: "parent"}}}, 0},
		{"selector", &ResourceSet{InputsFrom: []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}}, 0},
		{"chart", &HelmRelease{Chart: HelmChart{Source: NamedResource{Group: SourceDomain, Kind: KindOCIRepository, Name: "chart"}}}, 0},
		{"credentials", &GitRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, 0},
		{"substitution", &Kustomization{PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{{Kind: KindConfigMap, Name: "config"}, {Kind: KindSecret, Name: "secret", Optional: true}}}}, 2},
		{"values", &HelmRelease{ValuesFrom: []ValuesReference{{Kind: KindConfigMap, Name: "values"}}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			capacity, data := tc.obj.ReferenceCapacity()
			count, inputs := 0, 0
			for ref := range References(tc.obj, ReferenceAll) {
				count++
				if ref.Use == "substituteFrom" || ref.Use == "valuesFrom" {
					inputs++
				}
			}
			if count == 0 || count > capacity {
				t.Fatalf("reference count %d, capacity %d", count, capacity)
			}
			assert.Equal(t, data, tc.data)
			assert.Equal(t, inputs, data)
		})
	}
}

func TestReferences_EffectiveNamespace(t *testing.T) {
	t.Parallel()
	k := &Kustomization{Namespace: "authored"}
	assert.Equal(t, len(slices.Collect(References(k, ReferenceRelations))), 0)
	k.SourceRef = kustomizev1.CrossNamespaceSourceReference{Kind: KindGitRepository, Name: "source"}
	k.DependsOn = []kustomizev1.DependencyReference{{Name: "relative", ReadyExpr: "true"}, {Name: "explicit", Namespace: "other"}}
	k.PostBuild = &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{
		{Kind: KindConfigMap, Name: "shared"}, {Kind: "Unsupported"}, {Kind: KindSecret, Name: "shared"}, {Kind: KindConfigMap, Name: "shared", Optional: true}, {Kind: KindConfigMap, Name: "shared"},
	}}
	hr := &HelmRelease{Namespace: "authored", Chart: HelmChart{Source: NamedResource{Group: SourceDomain, Kind: KindOCIRepository, Name: "chart"}}}
	hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: KindOCIRepository, Name: "chart"}
	hr.DependsOn = []helmv2.DependencyReference{{Name: "parent"}}
	hr.ValuesFrom = []ValuesReference{{Kind: KindConfigMap, Name: "vars", Optional: true}}
	r := &ResourceSet{Namespace: "authored"}
	r.DependsOn = []fluxopv1.Dependency{
		{APIVersion: FluxKustomizeDomain + "/v1", Kind: KindKustomization, Name: "ready", Ready: true, ReadyExpr: "true"},
		{APIVersion: FluxKustomizeDomain + "/v1", Kind: KindKustomization, Name: "exists"},
		{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "cluster", Namespace: "ignored"},
		{APIVersion: "foreign.example/v1", Kind: "ClusterRole", Name: "foreign"},
		{APIVersion: "nvidia.com/v1", Kind: "ClusterPolicy", Name: "gpu", Namespace: "ignored"},
		{APIVersion: "foreign.example/v1", Kind: "ClusterPolicy", Name: "foreign-policy"},
		{APIVersion: "kyverno.io/v1", Kind: "ClusterPolicy", Name: "policy", Namespace: "ignored"},
		{APIVersion: "cert-manager.io/v1", Kind: "ClusterIssuer", Name: "issuer", Namespace: "ignored"},
		{APIVersion: "external-secrets.io/v1beta1", Kind: "ClusterSecretStore", Name: "store", Namespace: "ignored"},
		{APIVersion: "rbac.authorization.k8s.io/v999", Kind: "ClusterRole", Name: "unknown-version"},
		{APIVersion: "v1", Kind: KindKustomization, Name: "ready"},
		{APIVersion: "foreign.example/v2", Kind: KindKustomization, Name: "ready"},
		{APIVersion: FluxKustomizeDomain + "/v2", Kind: KindKustomization, Name: "ready"},
	}
	for _, dep := range r.DependsOn {
		KindOf(&RawObject{APIVersion: dep.APIVersion, Kind: dep.Kind}).Namespaced()
	}
	r.InputsFrom = []fluxopv1.InputProviderReference{
		{Kind: "Unsupported", Name: "inputs"}, {Selector: &metav1.LabelSelector{}},
		{Name: "inputs"}, {APIVersion: "v1", Name: "inputs"},
		{Kind: KindGitRepository, Name: "source"},
		{APIVersion: "foreign.example/v1", Kind: KindResourceSetInputProvider, Name: "inputs"},
	}
	for _, tc := range []struct {
		name, namespace string
		obj             BaseManifest
		want            []Reference
	}{
		{"kustomization", "effective", k, []Reference{
			{Group: SourceDomain, Kind: KindGitRepository, Namespace: "effective", Name: "source", Use: "source", Content: true},
			{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "effective", Name: "relative", Use: "dependsOn", ReadyExpr: new("true")},
			{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "other", Name: "explicit", Use: "dependsOn", Position: 1},
			{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Content: true},
			{Kind: KindSecret, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 2, Content: true},
			{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 3, Optional: true},
			{Kind: KindConfigMap, Namespace: "effective", Name: "shared", Use: "substituteFrom", Position: 4, Content: true},
		}},
		{"helmrelease", "effective", hr, []Reference{
			{Group: SourceDomain, Kind: KindOCIRepository, Namespace: "effective", Name: "chart", Use: "chart", Content: true},
			{Group: HelmReleaseDomain, Kind: KindHelmRelease, Namespace: "effective", Name: "parent", Use: "dependsOn"},
			{Kind: KindConfigMap, Namespace: "effective", Name: "vars", Use: "valuesFrom", Optional: true, Content: true},
		}},
		{"resourceset", "apps", r, []Reference{
			{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "apps", Name: "ready", Use: "dependsOn", ReadyExpr: new("true")},
			{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "apps", Name: "exists", Use: "dependsOn", Position: 1},
			{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster", Use: "dependsOn", Position: 2},
			{Group: "foreign.example", Kind: "ClusterRole", Namespace: "apps", Name: "foreign", Use: "dependsOn", Position: 3},
			{Group: "nvidia.com", Kind: "ClusterPolicy", Namespace: "ignored", Name: "gpu", Use: "dependsOn", Position: 4},
			{Group: "foreign.example", Kind: "ClusterPolicy", Namespace: "apps", Name: "foreign-policy", Use: "dependsOn", Position: 5},
			{Group: "kyverno.io", Kind: "ClusterPolicy", Namespace: "ignored", Name: "policy", Use: "dependsOn", Position: 6},
			{Group: "cert-manager.io", Kind: "ClusterIssuer", Namespace: "ignored", Name: "issuer", Use: "dependsOn", Position: 7},
			{Group: "external-secrets.io", Kind: "ClusterSecretStore", Namespace: "ignored", Name: "store", Use: "dependsOn", Position: 8},
			{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Namespace: "apps", Name: "unknown-version", Use: "dependsOn", Position: 9},
			{Kind: KindKustomization, Namespace: "apps", Name: "ready", Use: "dependsOn", Position: 10},
			{Group: "foreign.example", Kind: KindKustomization, Namespace: "apps", Name: "ready", Use: "dependsOn", Position: 11},
			{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "apps", Name: "ready", Use: "dependsOn", Position: 12},
			{Kind: "Unsupported", Namespace: "apps", Name: "inputs", Use: "inputsFrom", Content: true},
			{Group: FluxOperatorDomain, Kind: KindResourceSetInputProvider, Namespace: "apps", Use: "inputsFrom", Position: 1, Content: true},
			{Group: FluxOperatorDomain, Kind: KindResourceSetInputProvider, Namespace: "apps", Name: "inputs", Use: "inputsFrom", Position: 2, Content: true},
			{Kind: KindResourceSetInputProvider, Namespace: "apps", Name: "inputs", Use: "inputsFrom", Position: 3, Content: true},
			{Group: SourceDomain, Kind: KindGitRepository, Namespace: "apps", Name: "source", Use: "inputsFrom", Position: 4, Content: true},
			{Group: "foreign.example", Kind: KindResourceSetInputProvider, Namespace: "apps", Name: "inputs", Use: "inputsFrom", Position: 5, Content: true},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertReferenceEdges(t, slices.Collect(References(tc.obj.WithNamespace(tc.namespace), ReferenceRelations)), tc.want)
			assert.Equal(t, tc.obj.Named().Namespace, "authored")
			var spec any
			switch obj := tc.obj.(type) {
			case *Kustomization:
				spec = obj.KustomizationSpec
			case *HelmRelease:
				spec = obj.HelmReleaseSpec
			case *ResourceSet:
				spec = obj.ResourceSetSpec
			}
			body, err := json.Marshal(spec)
			assert.NoError(t, err)
			var fields map[string]any
			assert.NoError(t, json.Unmarshal(body, &fields))
			info := KindOf(tc.obj)
			doc := map[string]any{"apiVersion": info.Group + "/v1", "kind": info.Kind, "metadata": map[string]any{"name": "projected"}, "spec": fields}
			before := DeepCopyMap(doc)
			obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: tc.namespace})
			assert.NoError(t, err)
			assertReferenceEdges(t, slices.Collect(References(obj, ReferenceRelations)), tc.want)
			switch parsed := obj.(type) {
			case *Kustomization:
				assert.Diff(t, parsed.KustomizationSpec, spec)
			case *HelmRelease:
				assert.Diff(t, parsed.HelmReleaseSpec, spec)
			case *ResourceSet:
				assert.Diff(t, parsed.ResourceSetSpec, spec)
			}
			assert.Equal(t, changed, true)
			assert.Diff(t, doc, before)
			before["metadata"].(map[string]any)["namespace"] = tc.namespace
			assert.Diff(t, authoritative, before)
		})
	}
	hr = hr.WithNamespace("effective").(*HelmRelease)
	hr.Chart = HelmChart{Source: NamedResource{Group: SourceDomain, Kind: KindGitRepository, Name: "resolved"}}
	hr.ChartValuesFiles = []string{"./config/prod.yaml"}
	assertReferenceEdges(t, refsForUse(t, hr, "source"), []Reference{{Group: SourceDomain, Kind: KindGitRepository, Namespace: "effective", Name: "resolved", Use: "source", Content: true, Files: hr.ChartValuesFiles}})
	assert.Equal(t, r.InputsFrom[0].Kind, "Unsupported")
}

func TestResolveChartRef_EffectiveNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace, want, apiVersion, group string
		kind, version                            string
		resolve                                  bool
	}{
		{"inherited namespace", "", "effective", "", SourceDomain, KindHelmChart, "", true},
		{"explicit namespace", "other", "other", "", SourceDomain, KindHelmChart, "", true},
		{"explicit source group", "", "effective", SourceDomain + "/v1", SourceDomain, KindHelmChart, "", true},
		{"explicit core group", "", "effective", "v1", "", KindHelmChart, "", false},
		{"foreign group", "", "effective", "foreign.example/v1", "foreign.example", KindHelmChart, "", false},
		{"non-chart kind", "", "effective", "", SourceDomain, KindOCIRepository, "", false},
		{"already versioned", "", "effective", "", SourceDomain, KindHelmChart, "1.2.3", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ref := map[string]any{"kind": tc.kind, "name": "chart", "namespace": tc.namespace}
			if tc.apiVersion != "" {
				ref["apiVersion"] = tc.apiVersion
			}
			obj, _, _, err := ParseDoc(map[string]any{
				"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": KindHelmRelease,
				"metadata": map[string]any{"name": "app", "namespace": "authored"},
				"spec":     map[string]any{"chartRef": ref},
			}, ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			hr := obj.(*HelmRelease)
			hr.Chart.Version = tc.version
			assert.Equal(t, hr.Chart.Source.Namespace, tc.namespace)
			projected := hr.WithNamespace("effective").(*HelmRelease)
			before := projected.Chart
			assert.Equal(t, projected.ChartSource(), NamedResource{Group: tc.group, Kind: tc.kind, Namespace: tc.want, Name: "chart"})
			called := false
			if err := projected.ResolveChartRef(func(id NamedResource) *HelmChartSource {
				called = true
				assert.Equal(t, id, NamedResource{Group: SourceDomain, Kind: KindHelmChart, Namespace: tc.want, Name: "chart"})
				return &HelmChartSource{Namespace: id.Namespace, Chart: "resolved", ValuesFiles: []string{"values.yaml"}, IgnoreMissingValuesFiles: true, SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindHelmRepository, Name: id.Name}}
			}); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, called, tc.resolve)
			if tc.resolve {
				assert.Equal(t, projected.ChartSource(), NamedResource{Group: SourceDomain, Kind: KindHelmRepository, Namespace: tc.want, Name: "chart"})
				assert.Equal(t, projected.Chart.Name, "resolved")
				assert.Equal(t, projected.Chart.Version, "")
				assert.Diff(t, projected.ChartValuesFiles, []string{"values.yaml"})
				assert.Equal(t, projected.IgnoreMissingValuesFiles, true)
			} else {
				assert.Equal(t, projected.Chart, before)
			}
			assert.Equal(t, hr.Chart, before)
			assert.Equal(t, hr.ChartRef.Namespace, tc.namespace)
			assert.Equal(t, hr.Namespace, "authored")
		})
	}
}

func TestReferences_ActiveCredentials(t *testing.T) {
	t.Parallel()
	o := &OCIRepository{Namespace: "effective"}
	o.SecretRef, o.CertSecretRef, o.ProxySecretRef = &LocalObjectReference{Name: "auth"}, &LocalObjectReference{Name: "cert"}, &LocalObjectReference{Name: "proxy"}
	o.Verify = &sourcev1.OCIRepositoryVerification{Provider: "cosign", SecretRef: &LocalObjectReference{Name: "ignored"}}
	assertReferenceEdges(t, slices.Collect(References(o, ReferenceRelations)), []Reference{
		{Kind: KindSecret, Namespace: "effective", Name: "auth", Use: "secretRef", Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "cert", Use: "certSecretRef", Position: 1, Content: true},
		{Kind: KindSecret, Namespace: "effective", Name: "proxy", Use: "proxySecretRef", Position: 2, Content: true},
	})
	for _, tc := range []struct {
		name    string
		content func(*LocalObjectReference, *LocalObjectReference, *LocalObjectReference) any
		cases   []struct {
			name                string
			secret, cert, proxy *LocalObjectReference
			want                string
		}
	}{
		{name: "GitRepository", content: func(secret, _, proxy *LocalObjectReference) any {
			g := &GitRepository{Namespace: "effective"}
			g.SecretRef = secret
			g.ProxySecretRef = proxy
			g.URL = "git-url"
			g.Ignore = new("ignored")
			g.SparseCheckout = []string{"apps"}
			g.RecurseSubmodules = true
			g.Verification = &sourcev1.GitRepositoryVerification{SecretRef: LocalObjectReference{Name: "verification"}}
			return g.Content(sourcev1.GitRepositoryRef{Branch: "main"})
		}, cases: []struct {
			name                string
			secret, cert, proxy *LocalObjectReference
			want                string
		}{
			{"none", nil, nil, nil, `{"URL":"git-url","Ref":{"branch":"main"},"Auth":null,"Ignore":"ignored","SparseCheckout":["apps"],"RecurseSubmodules":true}`},
			{"secret", &LocalObjectReference{Name: "auth"}, nil, nil, `{"URL":"git-url","Ref":{"branch":"main"},"Auth":[{"kind":"Secret","namespace":"effective","name":"auth"}],"Ignore":"ignored","SparseCheckout":["apps"],"RecurseSubmodules":true}`},
			{"proxy", nil, nil, &LocalObjectReference{Name: "proxy"}, `{"URL":"git-url","Ref":{"branch":"main"},"Auth":[{"kind":"Secret","namespace":"effective","name":"proxy"}],"Ignore":"ignored","SparseCheckout":["apps"],"RecurseSubmodules":true}`},
			{"all", &LocalObjectReference{Name: "auth"}, &LocalObjectReference{Name: "cert"}, &LocalObjectReference{Name: "proxy"}, `{"URL":"git-url","Ref":{"branch":"main"},"Auth":[{"kind":"Secret","namespace":"effective","name":"auth"},{"kind":"Secret","namespace":"effective","name":"proxy"}],"Ignore":"ignored","SparseCheckout":["apps"],"RecurseSubmodules":true}`},
			{"duplicates", &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, `{"URL":"git-url","Ref":{"branch":"main"},"Auth":[{"kind":"Secret","namespace":"effective","name":"shared"},{"kind":"Secret","namespace":"effective","name":"shared"}],"Ignore":"ignored","SparseCheckout":["apps"],"RecurseSubmodules":true}`},
			{"empty names", &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, `{"URL":"git-url","Ref":{"branch":"main"},"Auth":null,"Ignore":"ignored","SparseCheckout":["apps"],"RecurseSubmodules":true}`},
		}},
		{name: "OCIRepository", content: func(secret, cert, proxy *LocalObjectReference) any {
			o := &OCIRepository{Namespace: "effective"}
			o.SecretRef = secret
			o.CertSecretRef = cert
			o.ProxySecretRef = proxy
			o.URL = "oci-url"
			o.Ignore = new("ignored")
			o.Insecure = true
			o.LayerSelector = &OCILayerSelector{MediaType: "media", Operation: "copy"}
			o.Verify = &sourcev1.OCIRepositoryVerification{Provider: "cosign", SecretRef: &LocalObjectReference{Name: "verification"}}
			return o.Content(sourcev1.OCIRepositoryRef{Tag: "v1"})
		}, cases: []struct {
			name                string
			secret, cert, proxy *LocalObjectReference
			want                string
		}{
			{"none", nil, nil, nil, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":null,"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
			{"secret", &LocalObjectReference{Name: "auth"}, nil, nil, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":[{"kind":"Secret","namespace":"effective","name":"auth"}],"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
			{"cert", nil, &LocalObjectReference{Name: "cert"}, nil, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":[{"kind":"Secret","namespace":"effective","name":"cert"}],"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
			{"proxy", nil, nil, &LocalObjectReference{Name: "proxy"}, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":[{"kind":"Secret","namespace":"effective","name":"proxy"}],"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
			{"all", &LocalObjectReference{Name: "auth"}, &LocalObjectReference{Name: "cert"}, &LocalObjectReference{Name: "proxy"}, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":[{"kind":"Secret","namespace":"effective","name":"auth"},{"kind":"Secret","namespace":"effective","name":"cert"},{"kind":"Secret","namespace":"effective","name":"proxy"}],"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
			{"duplicates", &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":[{"kind":"Secret","namespace":"effective","name":"shared"},{"kind":"Secret","namespace":"effective","name":"shared"},{"kind":"Secret","namespace":"effective","name":"shared"}],"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
			{"empty names", &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, `{"URL":"oci-url","Ref":{"tag":"v1"},"Auth":null,"Insecure":true,"LayerSelector":{"mediaType":"media","operation":"copy"},"Ignore":"ignored"}`},
		}},
		{name: "HelmRepository", content: func(secret, cert, _ *LocalObjectReference) any {
			h := &HelmRepository{Namespace: "effective"}
			h.SecretRef = secret
			h.CertSecretRef = cert
			h.URL = "helm-url"
			h.PassCredentials = true
			return h.Content()
		}, cases: []struct {
			name                string
			secret, cert, proxy *LocalObjectReference
			want                string
		}{
			{"none", nil, nil, nil, `{"URL":"helm-url","Auth":null,"PassCredentials":true}`},
			{"secret", &LocalObjectReference{Name: "auth"}, nil, nil, `{"URL":"helm-url","Auth":[{"kind":"Secret","namespace":"effective","name":"auth"}],"PassCredentials":true}`},
			{"cert", nil, &LocalObjectReference{Name: "cert"}, nil, `{"URL":"helm-url","Auth":[{"kind":"Secret","namespace":"effective","name":"cert"}],"PassCredentials":true}`},
			{"all", &LocalObjectReference{Name: "auth"}, &LocalObjectReference{Name: "cert"}, &LocalObjectReference{Name: "proxy"}, `{"URL":"helm-url","Auth":[{"kind":"Secret","namespace":"effective","name":"auth"},{"kind":"Secret","namespace":"effective","name":"cert"}],"PassCredentials":true}`},
			{"duplicates", &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, `{"URL":"helm-url","Auth":[{"kind":"Secret","namespace":"effective","name":"shared"},{"kind":"Secret","namespace":"effective","name":"shared"}],"PassCredentials":true}`},
			{"empty names", &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, `{"URL":"helm-url","Auth":null,"PassCredentials":true}`},
		}},
		{name: "Bucket", content: func(secret, cert, proxy *LocalObjectReference) any {
			b := &Bucket{Namespace: "effective"}
			b.SecretRef = secret
			b.CertSecretRef = cert
			b.ProxySecretRef = proxy
			b.BucketName, b.Prefix, b.Region = "bucket", "prefix", "region"
			return b.Content("endpoint", true)
		}, cases: []struct {
			name                string
			secret, cert, proxy *LocalObjectReference
			want                string
		}{
			{"none", nil, nil, nil, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":null}`},
			{"secret", &LocalObjectReference{Name: "auth"}, nil, nil, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":[{"kind":"Secret","namespace":"effective","name":"auth"}]}`},
			{"cert", nil, &LocalObjectReference{Name: "cert"}, nil, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":[{"kind":"Secret","namespace":"effective","name":"cert"}]}`},
			{"proxy", nil, nil, &LocalObjectReference{Name: "proxy"}, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":[{"kind":"Secret","namespace":"effective","name":"proxy"}]}`},
			{"all", &LocalObjectReference{Name: "auth"}, &LocalObjectReference{Name: "cert"}, &LocalObjectReference{Name: "proxy"}, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":[{"kind":"Secret","namespace":"effective","name":"auth"},{"kind":"Secret","namespace":"effective","name":"cert"},{"kind":"Secret","namespace":"effective","name":"proxy"}]}`},
			{"duplicates", &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, &LocalObjectReference{Name: "shared"}, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":[{"kind":"Secret","namespace":"effective","name":"shared"},{"kind":"Secret","namespace":"effective","name":"shared"},{"kind":"Secret","namespace":"effective","name":"shared"}]}`},
			{"empty names", &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, &LocalObjectReference{Name: ""}, `{"Endpoint":"endpoint","Secure":true,"BucketName":"bucket","Prefix":"prefix","Region":"region","Auth":null}`},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, c := range tc.cases {
				t.Run(c.name, func(t *testing.T) {
					t.Parallel()
					content := tc.content(c.secret, c.cert, c.proxy)
					got, err := json.Marshal(content)
					assert.NoError(t, err)
					assert.Equal(t, string(got), c.want)
					assert.Equal(t, Fingerprint(content), Fingerprint(json.RawMessage(c.want)))
				})
			}
		})
	}

}

func TestReferences_NoAllocations(t *testing.T) {
	var kinds []string
	for info := range Kinds() {
		if info.Test {
			kinds = append(kinds, info.Kind)
		}
	}
	assert.Diff(t, kinds, []string{KindKustomization, KindHelmRelease, KindGitRepository, KindOCIRepository, KindHelmRepository, KindBucket})
	assert.Equal(t, KindOf(nil), KindInfo{})
	assert.Equal(t, KindOf(&Kustomization{}), kindTable[kindKustomization].KindInfo)
	if unsafe.Sizeof(uintptr(0)) == 8 {
		assert.Equal(t, unsafe.Sizeof(DependencyRef{}), uintptr(80))
		assert.Equal(t, unsafe.Sizeof(Reference{}), uintptr(136))
		assert.Equal(t, unsafe.Sizeof(KindInfo{}), uintptr(64))
	}
	KindOf(&RawObject{APIVersion: "v1", Kind: KindConfigMap}).Namespaced()
	for _, obj := range []BaseManifest{&Kustomization{DependsOn: []kustomizev1.DependencyReference{{Name: "dependency", ReadyExpr: "true"}}, SourceRef: kustomizev1.CrossNamespaceSourceReference{Name: "source"}, PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []SubstituteReference{{Kind: KindConfigMap, Name: "values"}}}}, &HelmRelease{DependsOn: []helmv2.DependencyReference{{Name: "dependency", ReadyExpr: "true"}}, Chart: HelmChart{Source: NamedResource{Group: SourceDomain, Kind: KindGitRepository, Name: "source"}}, ChartValuesFiles: []string{"./config/prod.yaml"}, ValuesFrom: []ValuesReference{{Name: "values"}}}, &HelmChartSource{SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: KindBucket, Name: "source"}, ValuesFiles: []string{"config/prod.yaml"}}, &ResourceSet{DependsOn: []fluxopv1.Dependency{{APIVersion: "v1", Kind: KindConfigMap, Name: "dependency", ReadyExpr: "true"}}, InputsFrom: []fluxopv1.InputProviderReference{{Name: "input"}}}, &GitRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, &OCIRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, &HelmRepository{SecretRef: &LocalObjectReference{Name: "auth"}}, &Bucket{SecretRef: &LocalObjectReference{Name: "auth"}}} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			assert.Equal(t, testing.AllocsPerRun(100, func() { KindOf(obj) }), float64(0))
			count := 0
			assert.Equal(t, testing.AllocsPerRun(100, func() {
				for range References(obj, ReferenceRelations) {
					count++
				}
			}), float64(0))
			stops := 0
			References(obj, ReferenceRelations)(func(Reference) bool { stops++; return false })
			assert.Equal(t, stops, 1)
			assert.Equal(t, count > 0, true)
		})
	}
}

func TestReferences_Views(t *testing.T) {
	obj := &Kustomization{
		Name: "consumer", Namespace: "apps", Path: "./consumer",
		DependsOn: []kustomizev1.DependencyReference{{Name: "dependency"}},
	}
	dependency := Reference{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: "apps", Name: "dependency", Use: "dependsOn", Gate: GateInputs, Ordering: true}
	bootstrap := Reference{NamedResource: BootstrapSourceID, Gate: GateInputs | GateArtifacts, ArtifactStages: GateInputs | GateArtifacts, Artifact: true}
	for _, tc := range []struct {
		name string
		view ReferenceView
		want []Reference
	}{
		{"relationships", ReferenceRelations, []Reference{dependency}},
		{"all prerequisites", ReferenceAll, []Reference{bootstrap, dependency}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Diff(t, slices.Collect(References(obj, tc.view)), tc.want)
			assert.Equal(t, testing.AllocsPerRun(100, func() {
				for range References(obj, tc.view) {
				}
			}), float64(0))
			calls := 0
			References(obj, tc.view)(func(Reference) bool { calls++; return false })
			assert.Equal(t, calls, 1)
		})
	}
}

func TestReferences_SourceValuesFiles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, group string
		files       bool
	}{
		{KindGitRepository, SourceDomain, true},
		{KindBucket, SourceDomain, true},
		{KindHelmRepository, SourceDomain, false},
		{KindOCIRepository, SourceDomain, false},
		{KindExternalArtifact, SourceDomain, false},
		{KindGitRepository, "foreign.example", false},
	} {
		t.Run(tc.group+"/"+tc.kind, func(t *testing.T) {
			t.Parallel()
			for _, form := range []string{"inline", "resolved", "standalone"} {
				t.Run(form, func(t *testing.T) {
					t.Parallel()
					files := []string{"./config/prod.yaml", "config/extra.yaml"}
					chart := &HelmChartSource{Namespace: "authored", Name: "chart",
						SourceRef:   sourcev1.LocalHelmChartSourceReference{APIVersion: tc.group + "/v1", Kind: tc.kind, Name: "source"},
						ValuesFiles: files}
					var obj BaseManifest
					switch form {
					case "inline":
						hr := &HelmRelease{Namespace: "authored", Chart: helmChartFromSource(chart), ChartValuesFiles: files}
						hr.Chart.Source.Namespace = ""
						hr.HelmReleaseSpec.Chart = &helmv2.HelmChartTemplate{Spec: helmv2.HelmChartTemplateSpec{
							SourceRef: helmv2.CrossNamespaceObjectReference{APIVersion: tc.group + "/v1", Kind: tc.kind, Name: "source"},
						}}
						obj = hr.WithNamespace("effective")
					case "resolved":
						hr := &HelmRelease{Namespace: "effective", Chart: HelmChart{Source: NamedResource{Group: SourceDomain, Kind: KindHelmChart, Name: "chart"}}}
						hr.ChartRef = &helmv2.CrossNamespaceSourceReference{Kind: KindHelmChart, Name: "chart"}
						assert.NoError(t, hr.ResolveChartRef(func(id NamedResource) *HelmChartSource {
							assert.Equal(t, id, NamedResource{Group: SourceDomain, Kind: KindHelmChart, Namespace: "effective", Name: "chart"})
							return chart.WithNamespace(id.Namespace).(*HelmChartSource)
						}))
						obj = hr
					case "standalone":
						obj = chart.WithNamespace("effective")
					}
					refs := slices.Collect(References(obj, ReferenceRelations))
					source := Reference{Group: tc.group, Kind: tc.kind, Namespace: "effective", Name: "source", Use: "source", Content: true}
					if tc.files {
						source.Files = files
					}
					want := []Reference{source}
					if form == "resolved" {
						want = append([]Reference{{Group: SourceDomain, Kind: KindHelmChart, Namespace: "effective", Name: "chart", Use: "chart", Content: true}}, want...)
					}
					assertReferenceEdges(t, refs, want)
					if tc.files && len(refs[len(refs)-1].Files) != 0 {
						got := refs[len(refs)-1].Files
						assert.Equal(t, &got[0] == &files[0], true)
					}
					assert.Equal(t, chart.Namespace, "authored")
				})
			}
		})
	}
}

func assertReferenceEdges(t *testing.T, got, want []Reference) {
	t.Helper()
	if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(DependencyRef{}, "Artifact", "UnknownArtifactRoot", "ChartSource"), cmpopts.IgnoreFields(Reference{}, "Gate", "ArtifactStages", "Ordering")); diff != "" {
		t.Fatalf("reference edges (-want +got):\n%s", diff)
	}
}
