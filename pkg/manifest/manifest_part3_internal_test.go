package manifest

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseSecret(t *testing.T) {
	t.Parallel()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"
	yamlDoc := `
apiVersion: v1
kind: Secret
metadata: {name: creds, namespace: flux-system}
data:
  password: dGVzdA==
  cosign.pub: dGhpcy1pcy1ub3QtYS1zZWNyZXQ=
  enc-blob: ` + sops + `
stringData:
  apiKey: real-key
  enc-key: ` + sops + `
`
	t.Run("sops wiped, plaintext preserved", func(t *testing.T) {
		t.Parallel()
		testParseSecretSopsWipedPlaintextPreserved(
			t,
			&yamlDoc,
		)
	},
	)

	t.Run("preserved with wipeSecrets=false", func(t *testing.T) {
		t.Parallel()
		testParseSecretPreservedWithWipeSecretsFalse(
			t,
			&yamlDoc,
		)
	},
	)

	t.Run(
		"base64-embedded YAML preserved",
		func(t *testing.T) { t.Parallel(); testParseSecretBase64EmbeddedYAMLPreserved(t) },
	)
}

func TestParseSecret_Idempotent(t *testing.T) {
	t.Parallel()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"
	yamlDoc := `
apiVersion: v1
kind: Secret
metadata: {name: s, namespace: ns}
data:
  token: c2VjcmV0
  enc: ` + sops + `
stringData:
  apiKey: plaintext
`
	doc := mustYAML(t, yamlDoc)

	s1obj, _, _, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	s1 := s1obj.(*Secret)
	{
		assert.FatalIf(t, err != nil, "first parse: %v", err)
	}
	s2obj, _, _, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	s2 := s2obj.(*Secret)
	{
		assert.FatalIf(t, err != nil, "second parse: %v", err)
	}

	if s1.Data["enc"] != s2.Data["enc"] || s1.Data["token"] != s2.Data["token"] {
		t.Errorf("data not idempotent: first=%v second=%v", s1.Data, s2.Data)
	}
	if s1.StringData["apiKey"] != s2.StringData["apiKey"] {
		t.Errorf("stringData not idempotent: first=%q second=%q", s1.StringData["apiKey"], s2.StringData["apiKey"])
	}

	gotData := doc["data"].(map[string]any)
	{
		assert.ErrorIf(
			t,
			gotData["token"] != "c2VjcmV0" || gotData["enc"] != sops,
			"parseSecret mutated doc[\"data\"]: %v",
			gotData,
		)
	}
	{
		got := doc["stringData"].(map[string]any)["apiKey"]
		assert.ErrorIf(
			t,
			got != "plaintext",
			"parseSecret mutated doc[\"stringData\"]: got %q want %q",
			got,
			"plaintext",
		)
	}
}

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
			obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
			{
				assert.FatalIf(t, err != nil, "ParseDoc: %v", err)
			}
			info := KindOf(obj)
			{
				assert.FatalIf(
					t,
					info.Tracked || info.Runnable || info.Status || info.Discovery || info.Test,
					"got %T, want *RawObject",
					obj,
				)
			}
			assert.Equal(t, obj.Named().Namespace, "apps")
			assert.Equal(t, obj.Named().Group, apiGroup(DocAPIVersion(doc)))
			assert.Equal(t, changed, true)
			assert.Diff(t, doc, before)
			before["metadata"].(map[string]any)["namespace"] = "apps"
			assert.Diff(t, authoritative, before)
			raw := obj.(*RawObject)
			{
				assert.ErrorIf(
					t,
					raw.Kind != DocKind(mustYAML(t, tc.yaml)) || raw.Name == "",
					"RawObject lost identity: %+v",
					raw,
				)
			}
		})
	}
}

func TestStripResourceAttributes(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "Deployment",
		"metadata": map[string]any{
			"annotations": map[string]any{"helm.sh/chart": "myapp-1.2.3", "keep": "yes"},
			"labels":      map[string]any{"app.kubernetes.io/version": "1.2.3"},
		},
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{"checksum/config": "abc123"},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{
		"helm.sh/chart",
		"app.kubernetes.io/version",
		"checksum/config",
	})

	meta := r["metadata"].(map[string]any)
	ann := meta["annotations"].(map[string]any)
	{
		_, ok := ann["helm.sh/chart"]
		assert.ErrorIf(t, ok, "annotation not stripped")
	}
	{
		assert.ErrorIf(t, ann["keep"] != "yes", "kept annotation lost")
	}
	{
		_, ok := meta["labels"]
		assert.ErrorIf(t, ok, "empty labels map should have been removed")
	}
	tplMeta := r["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := tplMeta["annotations"]
		assert.ErrorIf(t, ok, "template annotation not stripped (empty map should have been removed)")
	}
	for _, malformed := range []map[string]any{
		{"metadata": "bad", "spec": "bad"},
		{"metadata": map[string]any{"labels": "bad", "annotations": map[string]any(nil)}, "spec": map[string]any{"template": "bad", "jobTemplate": map[string]any{"metadata": "bad", "spec": "bad"}, "volumeClaimTemplates": "bad"}},
		{"spec": map[string]any{"volumeClaimTemplates": []any{nil, "bad", map[string]any{"metadata": "bad"}}}},
	} {
		before := DeepCopyMap(malformed)
		StripResourceAttributes(malformed, []string{"helm.sh/chart", "checksum/"})
		assert.Diff(t, malformed, before)
	}
}

// TestStripResourceAttributes_CronJob pins the CronJob walk: chart
// labels live both on the JobTemplateSpec metadata AND on the nested
// PodTemplateSpec metadata. Bitnami / app-template charts decorate
// both, so chart bumps would otherwise produce two diff entries per
// CronJob after the strip pass missed them.
func TestStripResourceAttributes_CronJob(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "CronJob",
		"metadata": map[string]any{
			"labels": map[string]any{"helm.sh/chart": "backup-1.0.0", "keep": "yes"},
		},
		"spec": map[string]any{
			"jobTemplate": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"helm.sh/chart": "backup-1.0.0"},
				},
				"spec": map[string]any{
					"template": map[string]any{
						"metadata": map[string]any{
							"annotations": map[string]any{"checksum/config": "abc123"},
						},
					},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{"helm.sh/chart", "checksum/config"})

	{
		meta := r["metadata"].(map[string]any)
		assert.ErrorIf(t, meta["labels"].(map[string]any)["keep"] != "yes", "top-level kept label lost")
	}
	jobTmpl := r["spec"].(map[string]any)["jobTemplate"].(map[string]any)
	{
		_, ok := jobTmpl["metadata"].(map[string]any)["labels"]
		assert.ErrorIf(t, ok, "jobTemplate.metadata.labels should have been emptied + removed")
	}
	podTmplMeta := jobTmpl["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := podTmplMeta["annotations"]
		assert.ErrorIf(t, ok, "jobTemplate.spec.template.metadata.annotations should have been emptied + removed")
	}
}

// TestStripResourceAttributes_StatefulSetPVCs pins the
// volumeClaimTemplates walk: chart labels on StatefulSet PVC
// templates (common pattern in Postgres / Loki / Prometheus charts)
// must be stripped, or every chart bump produces N diff entries
// (one per replica's PVC template).
func TestStripResourceAttributes_StatefulSetPVCs(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "StatefulSet",
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"helm.sh/chart": "pg-1.0.0"},
				},
			},
			"volumeClaimTemplates": []any{
				map[string]any{
					"metadata": map[string]any{
						"name":   "data",
						"labels": map[string]any{"helm.sh/chart": "pg-1.0.0", "app": "pg"},
					},
				},
				map[string]any{
					"metadata": map[string]any{
						"name":        "wal",
						"annotations": map[string]any{"checksum/config": "abc"},
					},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{"helm.sh/chart", "checksum/config"})

	pvcs := r["spec"].(map[string]any)["volumeClaimTemplates"].([]any)
	data := pvcs[0].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := data["labels"].(map[string]any)["helm.sh/chart"]
		assert.ErrorIf(t, ok, "PVC[0].metadata.labels.helm.sh/chart should have been stripped")
	}
	{
		assert.ErrorIf(
			t,
			data["labels"].(map[string]any)["app"] != "pg",
			"PVC[0].metadata.labels.app (non-stripped) should remain",
		)
	}
	wal := pvcs[1].(map[string]any)["metadata"].(map[string]any)
	{
		_, ok := wal["annotations"]
		assert.ErrorIf(t, ok, "PVC[1].metadata.annotations should have been emptied + removed")
	}
}

// TestStripResourceAttributes_ChecksumPrefix pins the trailing-slash
// prefix match: a single "checksum/" entry drops every suffix charts
// emit (config, secret, secrets, …). The plural "checksum/secrets"
// (matrix-synapse) used to slip past the exact-match list and churn
// `flate diff`; non-checksum annotations must survive untouched.
func TestStripResourceAttributes_ChecksumPrefix(t *testing.T) {
	t.Parallel()
	r := map[string]any{
		"kind": "Deployment",
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{
						"checksum/config":            "a",
						"checksum/secret":            "b",
						"checksum/secrets":           "c",
						"reloader.stakater.com/auto": "true",
					},
				},
			},
		},
	}
	StripResourceAttributes(r, []string{"checksum/"})

	ann := r["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
	for _, k := range []string{"checksum/config", "checksum/secret", "checksum/secrets"} {
		{
			_, ok := ann[k]
			assert.ErrorIf(t, ok, "prefix strip left %q behind", k)
		}
	}
	{
		assert.ErrorIf(
			t,
			ann["reloader.stakater.com/auto"] != "true",
			"non-checksum annotation should survive prefix strip; got %v",
			ann,
		)
	}
}

// TestRawObject_DoesNotAliasParsedDoc pins the deep-copy fix: a
// RawObject.Spec must not alias the loader's parsed YAML map.
// Previously r.Spec = doc["spec"].(map[string]any) shared the map
// pointer; mutating r.Spec corrupted the original doc the loader
// reused across passes.
func TestRawObject_DoesNotAliasParsedDoc(t *testing.T) {
	t.Parallel()
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "foo"},
		"spec":       map[string]any{"key": "original"},
	}
	obj, err := parseRawObject(doc)
	{
		assert.FatalIf(t, err != nil, "parseRawObject: %v", err)
	}
	// Mutate the RawObject's spec. The original doc must NOT change.
	obj.Spec["key"] = "mutated"
	got := doc["spec"].(map[string]any)["key"]
	{
		assert.ErrorIf(t, got != "original", "RawObject.Spec aliases parsed doc; got %q in source after mutation", got)
	}
	// Clone() produces an independent copy too.
	clone := obj.Clone()
	clone.Spec["key"] = "clone-only"
	{
		assert.ErrorIf(t, obj.Spec["key"] != "mutated", "Clone aliases original: clone mutation propagated to source")
	}
}

func TestParseDoc_MissingFields(t *testing.T) {
	t.Parallel()
	{
		_, _, _, err := ParseDoc(
			map[string]any{"kind": "Foo"},
			ParseDocOptions{DefaultNamespace: "apps"},
		)
		assert.ErrorIf(t, err == nil ||
			!strings.Contains(err.Error(), "apiVersion"), "expected apiVersion error, got %v", err)
	}
	// Bare data files (no kind:) are not Kubernetes resources.
	obj, _, _, err := ParseDoc(map[string]any{"apiVersion": "v1"}, ParseDocOptions{DefaultNamespace: "apps"})
	{
		assert.ErrorIf(t, err != nil, "missing-kind docs should not error, got %v", err)
	}
	{
		assert.ErrorIf(t, obj != nil, "expected nil for missing-kind doc, got %T", obj)
	}
	for _, tc := range []struct {
		name             string
		metadata         any
		absent           bool
		raw, core, typed string
	}{
		{name: "absent", absent: true, raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease missing metadata.name"},
		{name: "null", raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease missing metadata.name"},
		{name: "scalar", metadata: "bad", raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease decode: json: cannot unmarshal string"},
		{name: "numeric metadata", metadata: 7, raw: "missing metadata for Widget", core: "ConfigMap missing metadata", typed: "HelmRelease decode: json: cannot unmarshal number"},
		{name: "empty map", metadata: map[string]any{}, raw: "missing metadata.name for Widget", core: "ConfigMap missing metadata.name", typed: "HelmRelease missing metadata.name"},
		{name: "numeric name", metadata: map[string]any{"name": 7}, raw: "missing metadata.name for Widget", core: "ConfigMap missing metadata.name", typed: "HelmRelease decode: json: cannot unmarshal number"},
		{name: "numeric namespace", metadata: map[string]any{"name": "app", "namespace": 7}, typed: "HelmRelease decode: json: cannot unmarshal number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testParseDocMissingFieldsCase(t, &tc)
		},
		)
	}
	t.Run(
		"typed spec error precedes missing metadata",
		func(t *testing.T) { t.Parallel(); testParseDocMissingFieldsTypedSpecErrorPrecedesMissingMetadata(t) },
	)
}

func testParseSecretSopsWipedPlaintextPreserved(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	sobj, _, _, err := ParseDoc(
		mustYAML(t, (*testCaseInput)),
		ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""},
	)
	s := sobj.(*Secret)
	{
		assert.FatalIf(t, err != nil, "parseSecret: %v", err)
	}
	if s.Data["password"] != "dGVzdA==" {
		t.Errorf("plaintext .data wiped: %v", s.Data["password"])
	}
	if s.Data["cosign.pub"] != "dGhpcy1pcy1ub3QtYS1zZWNyZXQ=" {
		t.Errorf("public key wiped: %v", s.Data["cosign.pub"])
	}
	if s.StringData["apiKey"] != "real-key" {
		t.Errorf("plaintext .stringData wiped: %v", s.StringData["apiKey"])
	}
	wantB64 := base64.StdEncoding.EncodeToString([]byte(ValuePlaceholder("enc-blob")))
	if s.Data["enc-blob"] != wantB64 {
		t.Errorf("SOPS .data not wiped: %v", s.Data["enc-blob"])
	}
	if s.StringData["enc-key"] != ValuePlaceholder("enc-key") {
		t.Errorf("SOPS .stringData not wiped: %v", s.StringData["enc-key"])
	}
}

func testParseSecretPreservedWithWipeSecretsFalse(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"

	sobj, _, _, err := ParseDoc(
		mustYAML(t, (*testCaseInput)),
		ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""},
	)
	s := sobj.(*Secret)
	{
		assert.FatalIf(t, err != nil, "parseSecret: %v", err)
	}
	if s.Data["password"] != "dGVzdA==" || s.Data["enc-blob"] != sops {
		t.Errorf("value changed despite wipeSecrets=false: %v", s.Data)
	}
}

func testParseSecretBase64EmbeddedYAMLPreserved(t *testing.T) {
	t.Helper()
	const sops = "ENC[AES256_GCM,data:Zm9v,iv:YmFy,tag:YmF6,type:str]"

	valuesYAML := "mariadb:\n  enabled: " + sops + "\nwordpressSkipInstall: " + sops + "\n"
	doc := mustYAML(t, `
apiVersion: v1
kind: Secret
metadata: {name: wordpress-values, namespace: default}
data:
  values.yaml: `+base64.StdEncoding.EncodeToString([]byte(valuesYAML))+`
`)
	sobj, _, _, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	s := sobj.(*Secret)
	{
		assert.FatalIf(t, err != nil, "parseSecret: %v", err)
	}
	wantB64 := base64.StdEncoding.EncodeToString([]byte(valuesYAML))
	if s.Data["values.yaml"] != wantB64 {
		t.Errorf("embedded YAML changed: %v", s.Data["values.yaml"])
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
