package manifest

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseDoc_Dispatch(t *testing.T) {
	t.Parallel()
	type dispatchCase = struct {
		name    string
		yaml    string
		kind    string
		cluster bool
	}
	cases := make([]dispatchCase, 0, 21)
	cases = append(cases, []dispatchCase{
		{"kustomization", `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: x, namespace: ns}
spec: {path: ./a, sourceRef: {kind: GitRepository, name: r}}`, KindKustomization, false},
		{"helmrelease", `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: r, namespace: ns}
spec: {chart: {spec: {chart: c, sourceRef: {kind: HelmRepository, name: rr}}}}`, KindHelmRelease, false},
		{"helmrepository", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: r, namespace: ns}
spec: {url: https://example}`, KindHelmRepository, false},
		{"helmchart", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: hc, namespace: ns}
spec: {chart: c, sourceRef: {name: rr}}`, KindHelmChart, false},
		{"gitrepository", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: r, namespace: ns}
spec: {url: https://example.git}`, KindGitRepository, false},
		{"ocirepository", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: r, namespace: ns}
spec: {url: oci://example}`, KindOCIRepository, false},
		{"configmap", `
apiVersion: v1
kind: ConfigMap
metadata: {name: c, namespace: ns}`, KindConfigMap, false},
		{"secret", `
apiVersion: v1
kind: Secret
metadata: {name: s, namespace: ns}`, KindSecret, false},
		{
			"externalartifact",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: ExternalArtifact\nmetadata: {name: a}",
			KindExternalArtifact,
			false,
		},
		{
			"bucket",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: Bucket\nmetadata: {name: b}\nspec: {bucketName: b, endpoint: example.test}",
			KindBucket,
			false,
		},
		{
			"resourceset",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: s}",
			KindResourceSet,
			false,
		},
		{
			"resourcesetinputprovider",
			"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: p}",
			KindResourceSetInputProvider,
			false,
		},
		{"unknown", `
apiVersion: example.com/v1
kind: Foo
metadata: {name: x}`, "Foo", false},
	}...)
	for _, raw := range []struct {
		apiVersion, kind string
		cluster          bool
	}{
		{"apps/v1", "Deployment", false},
		{"rbac.authorization.k8s.io/v1", "ClusterRole", true},
		{"foreign.example/v1", "ClusterRole", false},
		{"rbac.authorization.k8s.io/v999", "ClusterRole", false},
		{"gateway.networking.k8s.io/v1", "GatewayClass", false},
		{"kyverno.io/v1", "ClusterPolicy", false},
		{"nvidia.com/v1", "ClusterPolicy", false},
		{"cert-manager.io/v1", "ClusterIssuer", false},
	} {
		cases = append(cases, struct {
			name, yaml, kind string
			cluster          bool
		}{
			raw.kind + "/" + raw.apiVersion,
			fmt.Sprintf(
				"apiVersion: %s\nkind: %s\nmetadata: {name: raw}\nspec: {value: keep}",
				raw.apiVersion,
				raw.kind,
			),
			raw.kind,
			raw.cluster,
		})
	}
	for _, tc := range cases {
		for _, ns := range []struct{ authored, fallback string }{{"", ""}, {"", "apps"}, {"explicit", "apps"}} {
			t.Run(tc.name+"/authored="+ns.authored+"/default="+ns.fallback, func(t *testing.T) {
				t.Parallel()
				testParseDocDispatchCase(t, &testParseDocDispatchCaseInputs{tc: &tc, ns: &ns})
			},
			)
		}
	}
}

// Typed decoding requires the exact API group and the supported core version.
func TestParseDoc_ForeignAPIGroup(t *testing.T) {
	t.Parallel()
	type foreignGroupCase = struct {
		name string
		yaml string
	}
	cases := make([]foreignGroupCase, 0, 19)
	cases = append(cases, []foreignGroupCase{
		{"slash-less flux version", `
apiVersion: kustomize.toolkit.fluxcd.io
kind: Kustomization
metadata: {name: raw, namespace: default}
spec: {path: ./apps}`},
		{"seaweedfs bucket", `
apiVersion: seaweed.seaweedfs.com/v1
kind: Bucket
metadata: {name: greptimedb, namespace: observability}
spec: {name: greptimedb, reclaimPolicy: Retain}`},
		{"ack s3 bucket", `
apiVersion: s3.services.k8s.aws/v1alpha1
kind: Bucket
metadata: {name: assets, namespace: default}
spec: {name: assets}`},
		{"helm.fluxcd.io helmrelease", `
apiVersion: helm.fluxcd.io/v1
kind: HelmRelease
metadata: {name: app, namespace: default}
spec: {chart: {repository: https://example, name: c, version: 1.0.0}}`},
		{"foreign gitrepository", `
apiVersion: example.com/v1
kind: GitRepository
metadata: {name: r, namespace: default}
spec: {url: https://example.git}`},
		{"foreign ocirepository", `
apiVersion: example.com/v1
kind: OCIRepository
metadata: {name: r, namespace: default}
spec: {url: oci://example}`},
		{"foreign helmrepository", `
apiVersion: example.com/v1
kind: HelmRepository
metadata: {name: r, namespace: default}
spec: {url: https://example}`},
		{"foreign configmap", `
apiVersion: example.com/v1
kind: ConfigMap
metadata: {name: c, namespace: default}`},
		{"unsupported core version", `
apiVersion: v2
kind: ConfigMap
metadata: {name: c, namespace: default}`},
		// Group prefixes cannot select typed decoders.
		{"group with kustomize prefix", `
apiVersion: kustomize.config.k8s.io.example.com/v1
kind: Kustomization
metadata: {name: custom, namespace: default}
spec: {value: keep}`},
		{"component with kustomize prefix", `
apiVersion: kustomize.config.k8s.io.example.com/v1
kind: Component
metadata: {name: custom, namespace: default}
spec: {value: keep}`},
		{"group with flux prefix", `
apiVersion: source.toolkit.fluxcd.io.example.com/v1
kind: Bucket
metadata: {name: b, namespace: default}
spec: {name: b}`},
	}...)
	for _, kind := range []string{KindKustomization, KindHelmChart, KindExternalArtifact, KindResourceSet, KindResourceSetInputProvider, KindSecret} {
		cases = append(
			cases,
			struct{ name, yaml string }{
				kind,
				"apiVersion: foreign.example/v1\nkind: " + kind + "\nmetadata: {name: raw}",
			},
		)
	}
	cases = append(
		cases,
		struct{ name, yaml string }{
			"explicit core flux kind",
			"apiVersion: v1\nkind: Kustomization\nmetadata: {name: raw}",
		},
	)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(t, tc.yaml)
			delete(doc["metadata"].(map[string]any), "namespace")
			before := DeepCopyMap(doc)
			parsed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
			obj, authoritative, changed := parsed.Object, parsed.Document, parsed.Changed
			assert.FatalIf(t, err != nil, "ParseDoc: %v", err)
			info := KindOf(obj)
			assert.FatalIf(
				t,
				info.Tracked || info.Runnable || info.Status || info.Discovery || info.Test,
				"got %T, want *RawObject",
				obj,
			)
			assert.Equal(t, obj.Named().Namespace, "apps")
			assert.Equal(t, obj.Named().Group, apiGroup(DocAPIVersion(doc)))
			assert.Equal(t, changed, true)
			assert.Diff(t, doc, before)
			before["metadata"].(map[string]any)["namespace"] = "apps"
			assert.Diff(t, authoritative, before)
			raw := obj.(*RawObject)
			assert.ErrorIf(
				t,
				raw.Kind != DocKind(mustYAML(t, tc.yaml)) || raw.Name == "",
				"RawObject lost identity: %+v",
				raw,
			)
		})
	}
}

type testParseDocDispatchCaseInputs struct {
	tc *struct {
		name    string
		yaml    string
		kind    string
		cluster bool
	}
	ns *struct {
		authored string
		fallback string
	}
}

func testParseDocDispatchCase(t *testing.T, testCaseInput *testParseDocDispatchCaseInputs) {
	t.Helper()

	doc := mustYAML(t, testCaseInput.tc.yaml)
	md := doc["metadata"].(map[string]any)
	delete(md, "namespace")
	if testCaseInput.ns.authored != "" {
		md["namespace"] = testCaseInput.ns.authored
	}
	before := DeepCopyMap(doc)
	parsed2, err := ParseDoc(
		doc,
		ParseDocOptions{DefaultNamespace: testCaseInput.ns.fallback},
	)
	obj, authoritative, changed := parsed2.Object, parsed2.Document, parsed2.Changed
	assert.NoError(t, err)
	want := testCaseInput.ns.authored
	if want == "" && !testCaseInput.tc.cluster {
		want = testCaseInput.ns.fallback
	}
	assert.Equal(t, obj.Named().Kind, testCaseInput.tc.kind)
	assert.Equal(t, obj.Named().Group, apiGroup(DocAPIVersion(doc)))
	assert.Equal(t, obj.Named().Namespace, want)
	defaulted := testCaseInput.ns.authored == "" && want != ""
	assert.Equal(t, changed, defaulted)
	assert.Diff(t, doc, before)
	if defaulted {
		before["metadata"].(map[string]any)["namespace"] = want
	}
	assert.Diff(t, authoritative, before)
	parsed3, err := ParseDoc(
		authoritative,
		ParseDocOptions{DefaultNamespace: testCaseInput.ns.fallback},
	)
	replayed, replayDoc, replayChanged := parsed3.Object, parsed3.Document, parsed3.Changed
	assert.NoError(t, err)
	assert.Equal(t, replayed.Named(), obj.Named())
	assert.Equal(t, replayChanged, false)
	assert.Diff(t, replayDoc, authoritative)
	if raw, ok := obj.(*RawObject); ok {
		info := KindOf(raw)
		assert.Equal(t, info.Group, apiGroup(DocAPIVersion(doc)))
		assert.Equal(t, info.Tracked || info.Runnable || info.Status || info.Discovery || info.Test, false)
		spec, _ := doc["spec"].(map[string]any)
		assert.Diff(t, raw.Spec, spec)
	}
}
