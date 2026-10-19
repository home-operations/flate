package manifest

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

// defaultParseDocOptions returns the standard ParseDoc options — secrets wiped.
// Test-only helper; production callers pass an explicit ParseDocOptions.
func defaultParseDocOptions() ParseDocOptions {
	return ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""}
}

// mustYAML decodes a single YAML document literal into a generic map.
func mustYAML(t *testing.T, doc string) map[string]any {
	t.Helper()
	docs, err := SplitDocs([]byte(doc))
	if err != nil {
		t.Fatalf("SplitDocs: %v\n%s", err, doc)
	}
	if len(docs) != 1 {
		t.Fatalf("expected single doc, got %d", len(docs))
	}
	return docs[0]
}

func TestNamedResource(t *testing.T) {
	t.Parallel()
	n := NamedResource{Group: FluxKustomizeDomain, Kind: "Kustomization", Namespace: "flux-system", Name: "apps"}
	if got, want := n.NamespacedName(), "flux-system/apps"; got != want {
		t.Errorf("NamespacedName = %q, want %q", got, want)
	}
	if got, want := n.String(), "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apps"; got != want {
		t.Errorf("String = %q, want %q", got, want)
	}

	cluster := NamedResource{Kind: "ClusterRole", Name: "view"}
	if got, want := cluster.NamespacedName(), "view"; got != want {
		t.Errorf("cluster-scoped NamespacedName = %q, want %q", got, want)
	}

	for _, tc := range []struct {
		name string
		a, b NamedResource
	}{
		{"kind before group", NamedResource{Kind: "A", Group: "z"}, NamedResource{Kind: "B"}},
		{"core before group", NamedResource{Kind: "A", Namespace: "z"}, NamedResource{Kind: "A", Group: "a"}},
		{"group before namespace", NamedResource{Kind: "A", Group: "a", Namespace: "z"}, NamedResource{Kind: "A", Group: "b"}},
		{"namespace before name", NamedResource{Kind: "A", Namespace: "a", Name: "z"}, NamedResource{Kind: "A", Namespace: "b"}},
		{"name", NamedResource{Kind: "A", Name: "1"}, NamedResource{Kind: "A", Name: "2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.a.Compare(tc.b), -1)
			assert.Equal(t, tc.b.Compare(tc.a), 1)
			assert.Equal(t, tc.a.Compare(tc.a), 0)
		})
	}
	assert.Equal(t, cluster.String(), "ClusterRole/view")
	for _, tc := range []struct {
		obj   BaseManifest
		group string
	}{
		{&Kustomization{}, FluxKustomizeDomain},
		{&HelmRelease{}, HelmReleaseDomain},
		{&GitRepository{}, SourceDomain},
		{&OCIRepository{}, SourceDomain},
		{&HelmRepository{}, SourceDomain},
		{&HelmChartSource{}, SourceDomain},
		{&ExternalArtifact{}, SourceDomain},
		{&Bucket{}, SourceDomain},
		{&ResourceSet{}, FluxOperatorDomain},
		{&ResourceSetInputProvider{}, FluxOperatorDomain},
		{&ConfigMap{}, ""},
		{&Secret{}, ""},
		{&RawObject{Kind: KindKustomization, APIVersion: "foreign.example/v2"}, "foreign.example"},
	} {
		t.Run(tc.obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			id := tc.obj.Named()
			assert.Equal(t, id.Group, tc.group)
			id.Namespace = "effective"
			assert.Equal(t, tc.obj.WithNamespace("effective").Named(), id)
			assert.Equal(t, Resolve(tc.obj.Named(), "effective", "fallback"), id)
		})
	}
	assert.Equal(t, (&RawObject{Kind: KindKustomization, APIVersion: "foreign.example/v1"}).Named(),
		(&RawObject{Kind: KindKustomization, APIVersion: "foreign.example/v2"}).Named())
}

func TestParseDoc_Suspended(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		kind, apiVersion, spec string
		suspendable            bool
	}{
		{KindHelmRelease, HelmReleaseDomain + "/v2", "{chart: {spec: {chart: c, sourceRef: {kind: HelmRepository, name: r}}}}", true},
		{KindKustomization, FluxKustomizeDomain + "/v1", "{path: ./apps, sourceRef: {kind: GitRepository, name: src}, interval: 5m}", true},
		{KindGitRepository, SourceDomain + "/v1", "{url: 'https://example.test/repo.git'}", true},
		{KindOCIRepository, SourceDomain + "/v1", "{url: 'oci://example.test/chart'}", true},
		{KindBucket, SourceDomain + "/v1", "{bucketName: x, endpoint: e, interval: 5m}", true},
		{KindHelmRepository, SourceDomain + "/v1", "{url: 'https://example.test/charts'}", true},
		{KindHelmChart, SourceDomain + "/v1", "{chart: c, sourceRef: {kind: HelmRepository, name: r}}", true},
		{KindResourceSet, FluxOperatorDomain + "/v1", "{}", true},
		{KindExternalArtifact, SourceDomain + "/v1", "{}", false},
		{KindResourceSetInputProvider, FluxOperatorDomain + "/v1", "{}", false},
		{KindConfigMap, CoreAPIVersion, "{}", false},
		{KindSecret, CoreAPIVersion, "{}", false},
		{"Widget", "example.test/v1", "{}", false},
	} {
		for _, suspend := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/suspend=%t", tc.kind, suspend), func(t *testing.T) {
				t.Parallel()
				doc := mustYAML(t, fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata: {name: test, namespace: ns}\nspec: %s\n", tc.apiVersion, tc.kind, tc.spec))
				doc["spec"].(map[string]any)["suspend"] = suspend
				if tc.kind == KindResourceSet && suspend {
					doc["metadata"].(map[string]any)["annotations"] = map[string]any{FluxOperatorDomain + "/reconcile": "disabled"}
				}
				obj, _, _, err := ParseDoc(doc, defaultParseDocOptions())
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				assert.Equal(t, obj.Suspended(), tc.suspendable && suspend)
			})
		}
	}
}

func TestNamedResource_IsCRD(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		id   NamedResource
		want bool
	}{
		{"canonical", NamedResource{Group: "apiextensions.k8s.io", Kind: KindCustomResourceDefinition, Name: "widgets.example.test"}, true},
		{"explicit namespace", NamedResource{Group: "apiextensions.k8s.io", Kind: KindCustomResourceDefinition, Namespace: "ns"}, true},
		{"foreign group", NamedResource{Group: "example.test", Kind: KindCustomResourceDefinition}, false},
		{"core group", NamedResource{Kind: KindCustomResourceDefinition}, false},
		{"different kind", NamedResource{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinitionList"}, false},
		{"zero identity", NamedResource{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.id.IsCRD(), tc.want)
		})
	}
}

// helmReleaseDoc wraps the given spec body (indented under spec:) in the
// standard HelmRelease envelope used by the parse tests.
func helmReleaseDoc(t *testing.T, specBody string) map[string]any {
	t.Helper()
	return mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
`+specBody+`
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`)
}

func TestParseHelmRelease_ValuesFromEnvsubstDefault(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		valuesKey string
		refName   string
		wantKey   string
		wantName  string
	}{
		{"default := resolves", "${CLUSTER_NAME:=biohazard}.yaml", "cm", "biohazard.yaml", "cm"},
		{"default :- resolves", "${X:-foo}.yaml", "cm", "foo.yaml", "cm"},
		{"literal key untouched", "biohazard.yaml", "cm", "biohazard.yaml", "cm"},
		{"bare var left for postBuild", "${CLUSTER_NAME}.yaml", "cm", "${CLUSTER_NAME}.yaml", "cm"},
		{"templated name default resolves", "values.yaml", "${ENV:=prod}-cm", "values.yaml", "prod-cm"},
		{"bare var name untouched", "values.yaml", "${ENV}-cm", "values.yaml", "${ENV}-cm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := "  valuesFrom:\n    - kind: ConfigMap\n      name: \"" + tc.refName + "\"\n      valuesKey: \"" + tc.valuesKey + "\"\n"
			hr, err := parseHelmRelease(helmReleaseDoc(t, spec))
			if err != nil {
				t.Fatalf("parseHelmRelease: %v", err)
			}
			if len(hr.ValuesFrom) != 1 {
				t.Fatalf("expected 1 valuesFrom, got %d", len(hr.ValuesFrom))
			}
			if got := hr.ValuesFrom[0].ValuesKey; got != tc.wantKey {
				t.Errorf("valuesKey = %q, want %q", got, tc.wantKey)
			}
			if got := hr.ValuesFrom[0].Name; got != tc.wantName {
				t.Errorf("name = %q, want %q", got, tc.wantName)
			}
		})
	}
}

func TestParseHelmRelease_CRDsPolicy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		specBody string
		want     string
	}{
		{"install only", "  install: {crds: Create}", "Create"},
		{"empty upgrade uses install", "  install: {crds: Create}\n  upgrade: {}", "Create"},
		{"upgrade wins over install", "  install: {crds: Create}\n  upgrade: {crds: CreateReplace}", "CreateReplace"},
		{"upgrade only", "  upgrade: {crds: CreateReplace}", "CreateReplace"},
		{"empty upgrade preserves install", "  install: {crds: Create}\n  upgrade: {}", "Create"},
		{"skip suppresses", "  upgrade: {crds: Skip}", "Skip"},
		{"empty when neither set", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr, err := parseHelmRelease(helmReleaseDoc(t, tc.specBody))
			if err != nil {
				t.Fatalf("parseHelmRelease: %v", err)
			}
			if hr.CRDsPolicy != tc.want {
				t.Errorf("CRDsPolicy = %q, want %q", hr.CRDsPolicy, tc.want)
			}
		})
	}
}

func TestParseHelmChartSource_DefaultsAndReconcileStrategy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, namespace, strategy, source string
	}{
		{"explicit", "ns", "Revision", "{kind: HelmRepository, name: r}"},
		{"default", "", "", "{name: r}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hc, err := parseHelmChartSource(mustYAML(t, "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: c, namespace: '"+tc.namespace+"'}\nspec: {chart: nginx, reconcileStrategy: '"+tc.strategy+"', sourceRef: "+tc.source+"}\n"))
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, hc.Namespace, tc.namespace)
			assert.Equal(t, hc.SourceRef.Kind, KindHelmRepository)
			assert.Equal(t, hc.ReconcileStrategy, tc.strategy)
		})
	}
}

func TestParseDoc_EmptyEnvsubstDefaults(t *testing.T) {
	t.Parallel()
	for _, operator := range []string{"-", "="} {
		t.Run(operator, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(t, fmt.Sprintf(`
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ks, namespace: ns}
spec:
  path: '${PREFIX:%s}apps'
  sourceRef:
    kind: GitRepository
    name: '${PREFIX:%s}repo'
  interval: 5m
`, operator, operator))
			obj, _, _, err := ParseDoc(doc, ParseDocOptions{})
			assert.NoError(t, err)
			k, ok := obj.(*Kustomization)
			if !ok {
				t.Fatalf("ParseDoc returned %T, want *Kustomization", obj)
			}
			assert.Equal(t, k.Path, "apps")
			assert.Equal(t, k.SourceRef.Kind, KindGitRepository)
			assert.Equal(t, k.SourceRef.Name, "repo")
		})
	}
}

// TestParseKustomization_SourceRefMissingNameRejected locks the
// truncated-YAML guard: a Kustomization with sourceRef.kind set but
// sourceRef.name empty (common shape for files that end mid-mapping)
// must fail at parse time with a clear "check for truncated YAML"
// hint, not 30 lines later as a misleading "path does not exist".
func TestParseKustomization_SourceRefMissingNameRejected(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ks, namespace: ns}
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: ""
  interval: 5m
`)
	_, err := parseKustomization(doc)
	if err == nil {
		t.Fatal("expected parse error for empty sourceRef.name with kind set")
	}
	if !strings.Contains(err.Error(), "sourceRef.name is empty") {
		t.Errorf("error should mention sourceRef.name being empty; got %v", err)
	}
}

func TestParseBucket_GenericWithSecret(t *testing.T) {
	t.Parallel()
	b, err := parseBucket(mustYAML(t, `apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata: {name: assets, namespace: apps}
spec:
  bucketName: my-bucket
  endpoint: minio.minio.svc:9000
  secretRef: {name: minio-creds}
`))
	if err != nil {
		t.Fatalf("parseBucket: %v", err)
	}
	if b.Provider != "generic" {
		t.Errorf("default Provider should be 'generic', got %q", b.Provider)
	}
}

// TestParse_EmptySecretRefNameRejected pins the schema check that
// turns a typo'd `secretRef: {}` into a hard parse error. Without
// it, the empty name would fall through to a runtime lookup of
// "<ns>/" → "secret not found" → with --allow-missing-secrets on,
// silent soft-skip — masking the typo entirely.
func TestParse_EmptySecretRefNameRejected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		yaml  string
		want  string
		parse func(map[string]any) error
	}{
		{
			name: "OCIRepository spec.secretRef.name empty",
			yaml: `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: x, namespace: ns}
spec:
  url: oci://example/x
  secretRef: {name: ""}`,
			want: "spec.secretRef.name is empty",
			parse: func(d map[string]any) error {
				_, _, _, err := ParseDoc(d, ParseDocOptions{DefaultNamespace: ""})
				return err
			},
		},
		{
			name: "GitRepository spec.secretRef.name empty",
			yaml: `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: x, namespace: ns}
spec:
  url: https://example/x.git
  secretRef: {name: ""}`,
			want:  "spec.secretRef.name is empty",
			parse: func(d map[string]any) error { _, err := parseGitRepository(d); return err },
		},
		{
			name: "HelmRepository spec.secretRef.name empty",
			yaml: `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: x, namespace: ns}
spec:
  url: https://example.com/charts
  secretRef: {name: ""}`,
			want:  "spec.secretRef.name is empty",
			parse: func(d map[string]any) error { _, err := parseHelmRepository(d); return err },
		},
		{
			name: "Bucket spec.secretRef.name empty",
			yaml: `apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata: {name: x, namespace: ns}
spec:
  bucketName: b
  endpoint: e
  secretRef: {name: ""}`,
			want:  "spec.secretRef.name is empty",
			parse: func(d map[string]any) error { _, err := parseBucket(d); return err },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(t, tc.yaml)
			err := tc.parse(doc)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("expected error containing %q, got %q", tc.want, err.Error())
			}
		})
	}
}

func TestParseResourceSet(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata:
  name: apps
  namespace: flux-system
spec:
  inputs:
    - tenant: frontend
    - tenant: backend
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata:
        name: << inputs.tenant >>-cm
`)
	rs, err := parseResourceSet(doc)
	if err != nil {
		t.Fatalf("parseResourceSet: %v", err)
	}
	if rs.Name != "apps" || rs.Namespace != "flux-system" {
		t.Errorf("unexpected name/ns: %s/%s", rs.Namespace, rs.Name)
	}
	if len(rs.Inputs) != 2 {
		t.Errorf("expected 2 inputs, got %d", len(rs.Inputs))
	}
	if len(rs.Resources) != 1 {
		t.Errorf("expected 1 templated resource, got %d", len(rs.Resources))
	}
	for _, tc := range []struct {
		name, field string
		ref         map[string]any
		valid       bool
	}{
		{"required missing", "dependsOn", map[string]any{}, false},
		{"required empty", "dependsOn", map[string]any{"apiVersion": ""}, false},
		{"required slash", "dependsOn", map[string]any{"apiVersion": "/"}, false},
		{"required no version", "dependsOn", map[string]any{"apiVersion": "example.test/"}, false},
		{"required multi slash", "dependsOn", map[string]any{"apiVersion": "example.test/v1/extra"}, false},
		{"required core", "dependsOn", map[string]any{"apiVersion": "v1"}, true},
		{"optional omitted", "inputsFrom", map[string]any{}, true},
		{"optional core", "inputsFrom", map[string]any{"apiVersion": "v1"}, true},
		{"optional empty", "inputsFrom", map[string]any{"apiVersion": ""}, false},
		{"optional slash", "inputsFrom", map[string]any{"apiVersion": "/"}, false},
		{"optional no version", "inputsFrom", map[string]any{"apiVersion": "example.test/"}, false},
		{"optional multi slash", "inputsFrom", map[string]any{"apiVersion": "example.test/v1/extra"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := DeepCopyMap(doc)
			tc.ref["kind"], tc.ref["name"] = KindResourceSetInputProvider, "inputs"
			input["spec"].(map[string]any)[tc.field] = []any{tc.ref}
			before := DeepCopyMap(input)
			_, _, _, err := ParseDoc(input, defaultParseDocOptions())
			if tc.valid {
				assert.NoError(t, err)
			} else if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) || !strings.Contains(err.Error(), "spec."+tc.field+"[0].apiVersion") {
				t.Fatalf("invalid reference error = %v", err)
			}
			assert.Diff(t, input, before)
		})
	}
}

func TestParseResourceSet_PreservesEmptyNamespace(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata:
  name: apps
spec: {}
`)
	rs, err := parseResourceSet(doc)
	if err != nil {
		t.Fatalf("parseResourceSet: %v", err)
	}
	if rs.Namespace != "" {
		t.Errorf("namespace=%q want empty before loader inheritance/defaulting", rs.Namespace)
	}
}

func TestParseResourceSetInputProvider_PreservesEmptyNamespace(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata:
  name: apps
spec:
  type: Static
`)
	rsip, err := parseResourceSetInputProvider(doc)
	if err != nil {
		t.Fatalf("parseResourceSetInputProvider: %v", err)
	}
	if rsip.Namespace != "" {
		t.Errorf("namespace=%q want empty before loader inheritance/defaulting", rsip.Namespace)
	}
}

func TestParseExternalArtifact(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: ExternalArtifact
metadata:
  name: pulled-tarball
  namespace: apps
spec:
  sourceRef:
    apiVersion: image.toolkit.fluxcd.io/v1beta2
    kind: ImageUpdateAutomation
    name: app-image
    namespace: apps
status:
  artifact:
    url: file:///cache/ea/pulled-tarball.tar.gz
    revision: v1.2.3@sha256:abc
    digest: sha256:abc
`)
	ea, err := parseExternalArtifact(doc)
	if err != nil {
		t.Fatalf("parseExternalArtifact: %v", err)
	}
	if ea.Name != "pulled-tarball" || ea.Namespace != "apps" {
		t.Errorf("identity: %+v", ea)
	}
	if ea.SourceRef == nil || ea.SourceRef.Kind != "ImageUpdateAutomation" {
		t.Errorf("sourceRef: %+v", ea.SourceRef)
	}
	if ea.ArtifactURL != "file:///cache/ea/pulled-tarball.tar.gz" {
		t.Errorf("artifact URL: %q", ea.ArtifactURL)
	}
	if ea.Revision != "v1.2.3@sha256:abc" {
		t.Errorf("revision: %q", ea.Revision)
	}
}

func TestParseKustomization_DependsOnReadyExpr(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps, namespace: flux-system}
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: src}
  dependsOn:
    - name: infra
      readyExpr: |
        object.status.conditions.exists(c, c.type == "InfraInitialized" && c.status == "True")
  interval: 5m
`)
	k, err := parseKustomization(doc)
	if err != nil {
		t.Fatalf("parseKustomization: %v", err)
	}
	deps := refsForUse(t, k, "dependsOn")
	if len(deps) != 1 {
		t.Fatalf("DependsOn len = %d", len(deps))
	}
	if deps[0].Name != "infra" {
		t.Errorf("name: %q", deps[0].Name)
	}
	if !strings.Contains(deps[0].ReadyExpr, "InfraInitialized") {
		t.Errorf("ReadyExpr lost: %q", deps[0].ReadyExpr)
	}
}

func TestParseHelmRelease_DependsOn(t *testing.T) {
	t.Parallel()
	hr, err := parseHelmRelease(helmReleaseDoc(t, `  dependsOn:
    - name: other
    - {name: cross, namespace: other-ns}`))
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
	}
	deps := refsForUse(t, hr, "dependsOn")
	if got, want := len(deps), 2; got != want {
		t.Fatalf("DependsOn len = %d, want %d", got, want)
	}
	if got := deps[0].NamespacedName(); got != "ns/other" {
		t.Errorf("DependsOn[0] = %q, want ns/other", got)
	}
	if deps[1].Namespace != "other-ns" || deps[1].Name != "cross" {
		t.Errorf("DependsOn[1] = %+v", deps[1])
	}
}

func TestParseHelmRelease_Inline(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: podinfo
  namespace: podinfo
spec:
  targetNamespace: apps
  chart:
    spec:
      chart: podinfo
      version: 6.3.2
      sourceRef:
        kind: HelmRepository
        name: podinfo
        namespace: flux-system
  values:
    replicaCount: 3
  valuesFrom:
    - kind: ConfigMap
      name: extra
      valuesKey: values.yaml
      optional: true
`)
	hr, err := parseHelmRelease(doc)
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
	}

	cases := []struct {
		name string
		got  any
		want any
	}{
		{"Name", hr.Name, "podinfo"},
		{"Namespace", hr.Namespace, "podinfo"},
		{"ReleaseName", hr.ReleaseName(), "podinfo"},
		{"ReleaseNamespace", hr.ReleaseNamespace(), "apps"},
		{"Chart.Name", hr.Chart.Name, "podinfo"},
		{"Chart.Version", hr.Chart.Version, "6.3.2"},
		{"Chart.RepoKind", hr.Chart.RepoKind, "HelmRepository"},
		{"Chart.RepoName", hr.Chart.RepoName, "podinfo"},
		{"replicaCount", hr.Values["replicaCount"], float64(3)},
		{"ValuesFrom[0].Optional", hr.ValuesFrom[0].Optional, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestParseHelmRelease_ReleaseNameOverride(t *testing.T) {
	t.Parallel()
	hr, err := parseHelmRelease(helmReleaseDoc(t, "  releaseName: my-explicit-release"))
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
	}
	if hr.HelmReleaseSpec.ReleaseName != "my-explicit-release" {
		t.Errorf("HelmReleaseSpec.ReleaseName = %q, want my-explicit-release", hr.HelmReleaseSpec.ReleaseName)
	}
	if got := hr.ReleaseName(); got != "my-explicit-release" {
		t.Errorf("ReleaseName() with override = %q, want my-explicit-release", got)
	}
}

// TestReleaseName_Shortens locks the helm-controller release.ShortenName
// contract: names ≤53 chars pass through; longer names become a 40-char
// prefix + "-" + 12 hex chars of sha256(name). Without this flate's
// `.Release.Name` diverges from a real cluster's for long HR names.
func TestReleaseName_Shortens(t *testing.T) {
	t.Parallel()
	short := &HelmRelease{Name: "short-name"}
	if got := short.ReleaseName(); got != "short-name" {
		t.Errorf("short name should pass through: got %q", got)
	}

	// 60-char name exceeds the 53 threshold.
	longName := "this-is-a-very-long-helmrelease-name-that-exceeds-53-chars"
	if len(longName) <= 53 {
		t.Fatalf("test setup: longName should be >53 chars, got %d", len(longName))
	}
	hr := &HelmRelease{Name: longName}
	got := hr.ReleaseName()
	if len(got) != 53 {
		t.Errorf("shortened name should be exactly 53 chars; got %d (%q)", len(got), got)
	}
	if got == longName {
		t.Errorf("expected shortening; got %q", got)
	}
	// Deterministic: same input → same hash suffix.
	if got2 := hr.ReleaseName(); got2 != got {
		t.Errorf("non-deterministic shortening: %q vs %q", got, got2)
	}
}

func TestParseHelmRelease_ChartRef(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: podinfo
  namespace: podinfo
spec:
  chartRef:
    kind: HelmChart
    name: my-chart
    namespace: flux-system
`)
	hr, err := parseHelmRelease(doc)
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
	}
	if hr.Chart.RepoKind != KindHelmChart || hr.Chart.Version != "" {
		t.Fatalf("expected unresolved chartRef, got %+v", hr.Chart)
	}

	src := &HelmChartSource{
		Name: "my-chart", Namespace: "flux-system",
		Chart: "podinfo", Version: "6.3.2",
		SourceRef: sourcev1.LocalHelmChartSourceReference{
			Name: "podinfo", Kind: KindHelmRepository,
		},
	}
	lookup := func(namespace, name string) *HelmChartSource {
		if namespace == src.Namespace && name == src.Name {
			return src
		}
		return nil
	}
	if err := hr.ResolveChartRef(lookup); err != nil {
		t.Fatalf("ResolveChartRef: %v", err)
	}
	if hr.Chart.Version != "6.3.2" || hr.Chart.Name != "podinfo" {
		t.Errorf("after resolve: %+v", hr.Chart)
	}

	missing := &HelmRelease{Chart: HelmChart{RepoGroup: SourceDomain, RepoKind: KindHelmChart, RepoName: "absent", RepoNamespace: "ns"}}
	none := func(_, _ string) *HelmChartSource { return nil }
	if err := missing.ResolveChartRef(none); !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestParseHelmRepository_OCI(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata:
  name: podinfo
  namespace: flux-system
spec:
  url: oci://ghcr.io/stefanprodan/charts
  type: oci
`)
	r, err := parseHelmRepository(doc)
	if err != nil {
		t.Fatalf("parseHelmRepository: %v", err)
	}
	if r.Type != "oci" {
		t.Errorf("RepoType = %q", r.Type)
	}
}

func TestGitRepositoryRef_Precedence(t *testing.T) {
	t.Parallel()
	const commit = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		name                     string
		ref                      *GitRepositoryRef
		kind, value, label, spec string
		normalized               sourcev1.GitRepositoryRef
		pinned                   bool
	}{
		{name: "nil"},
		{name: "empty", ref: &GitRepositoryRef{}},
		{
			name:       "commit",
			ref:        &GitRepositoryRef{Commit: commit},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
			pinned:     true,
		},
		{
			name:       "commit branch",
			ref:        &GitRepositoryRef{Commit: commit, Branch: "main"},
			kind:       "commit",
			value:      commit,
			label:      "branch:main@commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit, Branch: "main"},
		},
		{
			name:       "commit tag",
			ref:        &GitRepositoryRef{Commit: commit, Tag: "bad tag"},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
		},
		{
			name:       "commit semver",
			ref:        &GitRepositoryRef{Commit: commit, SemVer: "invalid"},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
		},
		{
			name:       "commit name",
			ref:        &GitRepositoryRef{Commit: commit, Name: "bad name"},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
		},
		{
			name:       "mixed commit",
			ref:        &GitRepositoryRef{Branch: "main", Tag: "v1.0.0", SemVer: ">=1.0", Name: "refs/heads/release", Commit: commit},
			kind:       "commit",
			value:      commit,
			label:      "branch:main@commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit, Branch: "main"},
		},
		{
			name:       "name",
			ref:        &GitRepositoryRef{Branch: "bad branch", Tag: "v1.0.0", SemVer: "invalid", Name: "refs/heads/release"},
			kind:       "name",
			value:      "refs/heads/release",
			label:      "name:refs/heads/release",
			spec:       "+refs/heads/release:refs/heads/release",
			normalized: sourcev1.GitRepositoryRef{Name: "refs/heads/release"},
		},
		{
			name:       "semver",
			ref:        &GitRepositoryRef{Branch: "bad branch", Tag: "bad tag", SemVer: ">=1.0"},
			kind:       "semver",
			value:      ">=1.0",
			label:      "semver:>=1.0",
			spec:       "+refs/tags/*:refs/tags/*",
			normalized: sourcev1.GitRepositoryRef{SemVer: ">=1.0"},
		},
		{
			name:       "name only",
			ref:        &GitRepositoryRef{Name: "refs/pull/420/head"},
			kind:       "name",
			value:      "refs/pull/420/head",
			label:      "name:refs/pull/420/head",
			spec:       "+refs/pull/420/head:refs/pull/420/head",
			normalized: sourcev1.GitRepositoryRef{Name: "refs/pull/420/head"},
		},
		{
			name:       "tag",
			ref:        &GitRepositoryRef{Tag: "v1.2.3", Branch: "bad branch"},
			kind:       "tag",
			value:      "v1.2.3",
			label:      "tag:v1.2.3",
			spec:       "+refs/tags/v1.2.3:refs/tags/v1.2.3",
			normalized: sourcev1.GitRepositoryRef{Tag: "v1.2.3"},
		},
		{
			name:       "branch",
			ref:        &GitRepositoryRef{Branch: "main"},
			kind:       "branch",
			value:      "main",
			label:      "branch:main",
			spec:       "+refs/heads/main:refs/heads/main",
			normalized: sourcev1.GitRepositoryRef{Branch: "main"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := Fingerprint(tc.ref)
			kind, value := tc.ref.Effective()
			assert.Equal(t, kind, tc.kind)
			assert.Equal(t, value, tc.value)
			want := ""
			if tc.kind != "" {
				want = tc.kind + ":" + tc.value
			}
			assert.Equal(t, tc.ref.String(), want)
			assert.Equal(t, tc.ref.Label(), tc.label)
			assert.Equal(t, string(tc.ref.RefSpec()), tc.spec)
			assert.Equal(t, tc.ref.Normalized(), tc.normalized)
			assert.Equal(t, tc.ref.Pinned(), tc.pinned)
			assert.Equal(t, tc.ref.Validate(), nil)
			assert.Equal(t, Fingerprint(tc.ref), before)
		})
	}
}

func TestGitRepositoryRef_Validate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ref  GitRepositoryRef
	}{
		{"short commit", GitRepositoryRef{Commit: "abc"}},
		{"nonhex commit", GitRepositoryRef{Commit: strings.Repeat("g", 40)}},
		{"commit branch", GitRepositoryRef{Commit: strings.Repeat("a", 40), Branch: "bad branch"}},
		{"name", GitRepositoryRef{Name: "bad name"}},
		{"semver", GitRepositoryRef{SemVer: "invalid"}},
		{"tag", GitRepositoryRef{Tag: "bad tag"}},
		{"branch", GitRepositoryRef{Branch: "bad branch"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.ref.Validate(); err == nil {
				t.Fatal("invalid selected reference accepted")
			}
		})
	}
}

func TestRenderDocument_SequentialClone(t *testing.T) {
	t.Parallel()
	ks, err := parseKustomization(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps}
spec: {path: ./p0}
`))
	assert.NoError(t, err)
	first, err := ks.RenderDocument()
	assert.NoError(t, err)
	snapshot := DeepCopyMap(first)
	clone := ks.Clone()
	clone.Path = "./p1"
	second, err := clone.RenderDocument()
	assert.NoError(t, err)
	assert.Equal(t, second["spec"].(map[string]any)["path"], any("./p1"))
	assert.Diff(t, first, snapshot)
	repeated, err := ks.RenderDocument()
	assert.NoError(t, err)
	assert.Diff(t, repeated, snapshot)
}

func TestRenderDocument_BufferOwnership(t *testing.T) {
	factory := kustomizationEncoders.New
	t.Cleanup(func() { kustomizationEncoders = sync.Pool{New: factory} })
	var encoder *kustomizationEncoder
	kustomizationEncoders = sync.Pool{New: func() any {
		encoder = factory().(*kustomizationEncoder)
		return encoder
	}}
	ks, err := parseKustomization(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps}
spec: {path: ./p0}
`))
	assert.NoError(t, err)
	_, err = ks.RenderDocument()
	assert.NoError(t, err)
	if encoder == nil || encoder.buffer.Len() == 0 {
		t.Fatal("render did not use an encoder buffer")
	}
	published := ks.projection.Load().raw
	snapshot := bytes.Clone(published)
	encoder.buffer.Bytes()[0] ^= 1
	assert.Diff(t, published, snapshot)
}

func TestRenderDocument_OmitsInternalShape(t *testing.T) {
	t.Parallel()
	ks, err := parseKustomization(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: apps}
spec: {namePrefix: ""}
`))
	assert.NoError(t, err)
	if ks.shape == nil {
		t.Fatal("authored empty prefix did not produce a shape")
	}
	for range 2 {
		doc, err := ks.RenderDocument()
		assert.NoError(t, err)
		if _, exists := doc["_flateShape"]; exists {
			t.Error("render document contains internal shape")
		}
		assert.Equal(t, doc["spec"].(map[string]any)["namePrefix"], any(""))
	}
}

func TestParseKustomization_DependsOn(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef:
    kind: GitRepository
    name: cluster
  dependsOn:
    - name: infra
    - name: extras
      namespace: other
  postBuild:
    substitute:
      DOMAIN: example.com
    substituteFrom:
      - kind: ConfigMap
        name: cluster-vars
        optional: true
`)
	k, err := parseKustomization(doc)
	if err != nil {
		t.Fatalf("parseKustomization: %v", err)
	}
	deps := refsForUse(t, k, "dependsOn")
	if len(deps) != 2 {
		t.Fatalf("DependsOn len = %d, want 2", len(deps))
	}
	if got := deps[0].NamespacedName(); got != "flux-system/infra" {
		t.Errorf("DependsOn[0] = %q, want flux-system/infra", got)
	}
	if got := deps[1].NamespacedName(); got != "other/extras" {
		t.Errorf("DependsOn[1] = %q, want other/extras", got)
	}
	if len(k.PostBuild.SubstituteFrom) != 1 || !k.PostBuild.SubstituteFrom[0].Optional {
		t.Errorf("substituteFrom = %+v", k.PostBuild.SubstituteFrom)
	}
	if k.PostBuild.Substitute["DOMAIN"] != "example.com" {
		t.Errorf("substitute = %+v", k.PostBuild.Substitute)
	}
}

func TestParseConfigMap(t *testing.T) {
	t.Parallel()
	doc := mustYAML(t, `
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-vars
  namespace: flux-system
data:
  DOMAIN: example.com
`)
	cmobj, _, _, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	cm := cmobj.(*ConfigMap)
	if err != nil {
		t.Fatalf("parseConfigMap: %v", err)
	}
	assert.Diff(t, cm.Data, map[string]any{"DOMAIN": "example.com"})
}

// TestParseConfigMap_WipesSopsCiphertext pins the offline-SOPS fix: a
// SOPS-encrypted ConfigMap (commonly a postBuild.substituteFrom source)
// must surface placeholders, not raw ciphertext, when wipeSecrets is set
// (the default). flate can't decrypt, and the `:` inside ENC[...]
// otherwise leaks into envsubst and trips chart validation (Ingress
// hosts, cert-manager dnsNames). Cleartext entries are left untouched,
// and wipeSecrets=false preserves the ciphertext to track Secret wiping.
func TestParseConfigMap_WipesSopsCiphertext(t *testing.T) {
	t.Parallel()
	const ciphertext = "ENC[AES256_GCM,data:NWahjtvi/hiAX9sVkctDGcmWOn0=,iv:2fxQddhm7pwpnO23VkFFgPPEXudfa7TgI0oxUkAjZtA=,tag:rS3MK2jGptzB3RyWOj+Amg==,type:str]"
	src := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-config
  namespace: flux-system
data:
  BASE_DOMAIN: ` + ciphertext + `
  PLAINTEXT: keep-me
`
	t.Run("wiped", func(t *testing.T) {
		t.Parallel()
		cmobj, _, _, err := ParseDoc(mustYAML(t, src), ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
		cm := cmobj.(*ConfigMap)
		if err != nil {
			t.Fatalf("parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": fmt.Sprintf(ValuePlaceholderTemplate, "BASE_DOMAIN"),
			"PLAINTEXT":   "keep-me",
		})
	})
	t.Run("preserved when wipeSecrets=false", func(t *testing.T) {
		t.Parallel()
		cmobj, _, _, err := ParseDoc(mustYAML(t, src), ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""})
		cm := cmobj.(*ConfigMap)
		if err != nil {
			t.Fatalf("parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": ciphertext,
			"PLAINTEXT":   "keep-me",
		})
	})
}

func TestParseSecret(t *testing.T) {
	t.Parallel()
	// flate renders the user's own repo offline, so it does NOT blanket-wipe
	// Secret values — plaintext (incl. a published cosign public key) passes
	// through. Only SOPS ciphertext is neutralized: flate can't decrypt it and
	// raw ENC[...] poisons downstream rendering.
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
		sobj, _, _, err := ParseDoc(mustYAML(t, yamlDoc), ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
		s := sobj.(*Secret)
		if err != nil {
			t.Fatalf("parseSecret: %v", err)
		}
		// Plaintext .data / .stringData pass through verbatim.
		if s.Data["password"] != "dGVzdA==" {
			t.Errorf("plaintext .data wiped: %v", s.Data["password"])
		}
		if s.Data["cosign.pub"] != "dGhpcy1pcy1ub3QtYS1zZWNyZXQ=" {
			t.Errorf("public key wiped: %v", s.Data["cosign.pub"])
		}
		if s.StringData["apiKey"] != "real-key" {
			t.Errorf("plaintext .stringData wiped: %v", s.StringData["apiKey"])
		}
		// SOPS ciphertext is neutralized: base64(placeholder) in .data,
		// plaintext placeholder in .stringData.
		wantB64 := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, ValuePlaceholderTemplate, "enc-blob"))
		if s.Data["enc-blob"] != wantB64 {
			t.Errorf("SOPS .data not wiped: %v", s.Data["enc-blob"])
		}
		if s.StringData["enc-key"] != fmt.Sprintf(ValuePlaceholderTemplate, "enc-key") {
			t.Errorf("SOPS .stringData not wiped: %v", s.StringData["enc-key"])
		}
	})

	t.Run("preserved with wipeSecrets=false", func(t *testing.T) {
		t.Parallel()
		sobj, _, _, err := ParseDoc(mustYAML(t, yamlDoc), ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""})
		s := sobj.(*Secret)
		if err != nil {
			t.Fatalf("parseSecret: %v", err)
		}
		// Even SOPS ciphertext is kept when wiping is off.
		if s.Data["password"] != "dGVzdA==" || s.Data["enc-blob"] != sops {
			t.Errorf("value changed despite wipeSecrets=false: %v", s.Data)
		}
	})

	// The common "SOPS-encrypted helm values file mounted via valuesFrom
	// Secret" pattern: the whole values.yaml (with ENC[...] scalars) is one
	// base64 blob under .data. It must be wiped — otherwise the raw ciphertext
	// leaks into chart values and corrupts the render (e.g. a bool flips
	// truthy and a subchart drops). The marker lives INSIDE the base64.
	t.Run("base64-embedded SOPS values file wiped", func(t *testing.T) {
		t.Parallel()
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
		if err != nil {
			t.Fatalf("parseSecret: %v", err)
		}
		wantB64 := base64.StdEncoding.EncodeToString(fmt.Appendf(nil, ValuePlaceholderTemplate, "values.yaml"))
		if s.Data["values.yaml"] != wantB64 {
			t.Errorf("embedded SOPS values blob not wiped: %v", s.Data["values.yaml"])
		}
	})
}

// TestParseSecret_Idempotent pins the copy-before-mutate fix: parseSecret
// must not write placeholders back into the caller-owned doc map. The wipe
// loop mutating doc["data"] in place would re-encode an already-encoded
// placeholder on a second parse, causing spurious EventObjectAdded on every
// cache-hit reconcile. Only SOPS values are wiped, so the test pins both a
// wiped SOPS value and a passed-through plaintext value.
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
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	s2obj, _, _, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ""})
	s2 := s2obj.(*Secret)
	if err != nil {
		t.Fatalf("second parse: %v", err)
	}

	// Both parses must yield identical values (wiped SOPS + passed-through).
	if s1.Data["enc"] != s2.Data["enc"] || s1.Data["token"] != s2.Data["token"] {
		t.Errorf("data not idempotent: first=%v second=%v", s1.Data, s2.Data)
	}
	if s1.StringData["apiKey"] != s2.StringData["apiKey"] {
		t.Errorf("stringData not idempotent: first=%q second=%q", s1.StringData["apiKey"], s2.StringData["apiKey"])
	}

	// The original doc must be unmodified: still the raw values.
	gotData := doc["data"].(map[string]any)
	if gotData["token"] != "c2VjcmV0" || gotData["enc"] != sops {
		t.Errorf("parseSecret mutated doc[\"data\"]: %v", gotData)
	}
	if got := doc["stringData"].(map[string]any)["apiKey"]; got != "plaintext" {
		t.Errorf("parseSecret mutated doc[\"stringData\"]: got %q want %q", got, "plaintext")
	}
}

func TestParseDoc_Dispatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		yaml    string
		kind    string
		cluster bool
	}{
		{"kustomization", `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: x, namespace: ns}
spec: {path: ./a, sourceRef: {kind: GitRepository, name: r}}`, KindKustomization, false},
		{"helmrelease", `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: r, namespace: ns}
spec: {chart: {spec: {chart: c, sourceRef: {name: rr}}}}`, KindHelmRelease, false},
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
		{"externalartifact", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: ExternalArtifact\nmetadata: {name: a}", KindExternalArtifact, false},
		{"bucket", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: Bucket\nmetadata: {name: b}\nspec: {bucketName: b, endpoint: example.test}", KindBucket, false},
		{"resourceset", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: s}", KindResourceSet, false},
		{"resourcesetinputprovider", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSetInputProvider\nmetadata: {name: p}", KindResourceSetInputProvider, false},
		{"unknown", `
apiVersion: example.com/v1
kind: Foo
metadata: {name: x}`, "Foo", false},
	}
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
			fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata: {name: raw}\nspec: {value: keep}", raw.apiVersion, raw.kind),
			raw.kind, raw.cluster,
		})
	}
	for _, tc := range cases {
		for _, ns := range []struct{ authored, fallback string }{{"", ""}, {"", "apps"}, {"explicit", "apps"}} {
			t.Run(tc.name+"/authored="+ns.authored+"/default="+ns.fallback, func(t *testing.T) {
				t.Parallel()
				doc := mustYAML(t, tc.yaml)
				md := doc["metadata"].(map[string]any)
				delete(md, "namespace")
				if ns.authored != "" {
					md["namespace"] = ns.authored
				}
				before := DeepCopyMap(doc)
				obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{WipeSecrets: true, DefaultNamespace: ns.fallback})
				assert.NoError(t, err)
				want := ns.authored
				if want == "" && !tc.cluster {
					want = ns.fallback
				}
				assert.Equal(t, obj.Named().Kind, tc.kind)
				assert.Equal(t, obj.Named().Group, apiGroup(DocAPIVersion(doc)))
				assert.Equal(t, obj.Named().Namespace, want)
				defaulted := ns.authored == "" && want != ""
				assert.Equal(t, changed, defaulted)
				assert.Diff(t, doc, before)
				if defaulted {
					before["metadata"].(map[string]any)["namespace"] = want
				}
				assert.Diff(t, authoritative, before)
				if raw, ok := obj.(*RawObject); ok {
					info := KindOf(raw)
					assert.Equal(t, info.Group, apiGroup(DocAPIVersion(doc)))
					assert.Equal(t, info.Tracked || info.Runnable || info.Status || info.Discovery || info.Test, false)
					spec, _ := doc["spec"].(map[string]any)
					assert.Diff(t, raw.Spec, spec)
				}
			})
		}
	}
}

// Typed decoding requires the exact API group and the supported core version.
func TestParseDoc_ForeignAPIGroup(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		yaml string
	}{
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
	}
	for _, kind := range []string{KindKustomization, KindHelmChart, KindExternalArtifact, KindResourceSet, KindResourceSetInputProvider, KindSecret} {
		cases = append(cases, struct{ name, yaml string }{kind, "apiVersion: foreign.example/v1\nkind: " + kind + "\nmetadata: {name: raw}"})
	}
	cases = append(cases, struct{ name, yaml string }{"explicit core flux kind", "apiVersion: v1\nkind: Kustomization\nmetadata: {name: raw}"})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(t, tc.yaml)
			delete(doc["metadata"].(map[string]any), "namespace")
			before := DeepCopyMap(doc)
			obj, authoritative, changed, err := ParseDoc(doc, ParseDocOptions{DefaultNamespace: "apps"})
			if err != nil {
				t.Fatalf("ParseDoc: %v", err)
			}
			info := KindOf(obj)
			if info.Tracked || info.Runnable || info.Status || info.Discovery || info.Test {
				t.Fatalf("got %T, want *RawObject", obj)
			}
			assert.Equal(t, obj.Named().Namespace, "apps")
			assert.Equal(t, obj.Named().Group, apiGroup(DocAPIVersion(doc)))
			assert.Equal(t, changed, true)
			assert.Diff(t, doc, before)
			before["metadata"].(map[string]any)["namespace"] = "apps"
			assert.Diff(t, authoritative, before)
			raw := obj.(*RawObject)
			if raw.Kind != DocKind(mustYAML(t, tc.yaml)) || raw.Name == "" {
				t.Errorf("RawObject lost identity: %+v", raw)
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
	if _, ok := ann["helm.sh/chart"]; ok {
		t.Errorf("annotation not stripped")
	}
	if ann["keep"] != "yes" {
		t.Errorf("kept annotation lost")
	}
	if _, ok := meta["labels"]; ok {
		t.Errorf("empty labels map should have been removed")
	}
	tplMeta := r["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	if _, ok := tplMeta["annotations"]; ok {
		t.Errorf("template annotation not stripped (empty map should have been removed)")
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

	if meta := r["metadata"].(map[string]any); meta["labels"].(map[string]any)["keep"] != "yes" {
		t.Errorf("top-level kept label lost")
	}
	jobTmpl := r["spec"].(map[string]any)["jobTemplate"].(map[string]any)
	if _, ok := jobTmpl["metadata"].(map[string]any)["labels"]; ok {
		t.Errorf("jobTemplate.metadata.labels should have been emptied + removed")
	}
	podTmplMeta := jobTmpl["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	if _, ok := podTmplMeta["annotations"]; ok {
		t.Errorf("jobTemplate.spec.template.metadata.annotations should have been emptied + removed")
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
	if _, ok := data["labels"].(map[string]any)["helm.sh/chart"]; ok {
		t.Errorf("PVC[0].metadata.labels.helm.sh/chart should have been stripped")
	}
	if data["labels"].(map[string]any)["app"] != "pg" {
		t.Errorf("PVC[0].metadata.labels.app (non-stripped) should remain")
	}
	wal := pvcs[1].(map[string]any)["metadata"].(map[string]any)
	if _, ok := wal["annotations"]; ok {
		t.Errorf("PVC[1].metadata.annotations should have been emptied + removed")
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
		if _, ok := ann[k]; ok {
			t.Errorf("prefix strip left %q behind", k)
		}
	}
	if ann["reloader.stakater.com/auto"] != "true" {
		t.Errorf("non-checksum annotation should survive prefix strip; got %v", ann)
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
	if err != nil {
		t.Fatalf("parseRawObject: %v", err)
	}
	// Mutate the RawObject's spec. The original doc must NOT change.
	obj.Spec["key"] = "mutated"
	got := doc["spec"].(map[string]any)["key"]
	if got != "original" {
		t.Errorf("RawObject.Spec aliases parsed doc; got %q in source after mutation", got)
	}
	// Clone() produces an independent copy too.
	clone := obj.Clone()
	clone.Spec["key"] = "clone-only"
	if obj.Spec["key"] != "mutated" {
		t.Errorf("Clone aliases original: clone mutation propagated to source")
	}
}

func TestParseDoc_MissingFields(t *testing.T) {
	t.Parallel()
	if _, _, _, err := ParseDoc(map[string]any{"kind": "Foo"}, ParseDocOptions{DefaultNamespace: "apps"}); err == nil || !strings.Contains(err.Error(), "apiVersion") {
		t.Errorf("expected apiVersion error, got %v", err)
	}
	// Bare data files (no kind:) are not Kubernetes resources.
	obj, _, _, err := ParseDoc(map[string]any{"apiVersion": "v1"}, ParseDocOptions{DefaultNamespace: "apps"})
	if err != nil {
		t.Errorf("missing-kind docs should not error, got %v", err)
	}
	if obj != nil {
		t.Errorf("expected nil for missing-kind doc, got %T", obj)
	}
}

func TestSplitDocs(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: v1
kind: ConfigMap
metadata: {name: a, namespace: ns}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: b, namespace: ns}
`)
	docs, err := SplitDocs(data)
	if err != nil {
		t.Fatalf("SplitDocs: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(docs))
	}
	names := []string{
		docs[0]["metadata"].(map[string]any)["name"].(string),
		docs[1]["metadata"].(map[string]any)["name"].(string),
	}
	assert.Diff(t, names, []string{"a", "b"})
}

// docNames returns each doc's metadata.name, for asserting flatten output.
func docNames(docs []map[string]any) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		md, _ := d["metadata"].(map[string]any)
		out[i], _ = md["name"].(string)
	}
	return out
}

// TestFlattenLists_DecodedYAML pins that a Kubernetes List wrapper decoded
// from a real multi-doc stream is expanded into its items, each a name-able
// top-level document, with surrounding docs and order preserved. This is
// the property that keeps an un-named List from forcing dyff off name-based
// pairing. Exercises the decode → FlattenLists sequence the render paths run.
func TestFlattenLists_DecodedYAML(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: v1
kind: ConfigMapList
metadata:
  labels: {helm.toolkit.fluxcd.io/name: x}
items:
  - apiVersion: v1
    kind: ConfigMap
    metadata: {name: dash-a, namespace: ns}
    data: {k: v}
  - apiVersion: v1
    kind: ConfigMap
    metadata: {name: dash-b, namespace: ns}
    data: {k: v}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: plain, namespace: ns}
`)
	docs, err := SplitDocs(data)
	if err != nil {
		t.Fatalf("SplitDocs: %v", err)
	}
	docs = FlattenLists(docs)
	// Names assert non-empty identity on every doc; kinds confirm the
	// wrapper itself is gone and only ConfigMaps remain.
	assert.Diff(t, docNames(docs), []string{"dash-a", "dash-b", "plain"})
	for _, d := range docs {
		if DocKind(d) != KindConfigMap {
			t.Errorf("flattened doc has kind %q, want ConfigMap", DocKind(d))
		}
	}
}

func TestFlattenLists(t *testing.T) {
	t.Parallel()
	cmItem := func(name string) map[string]any {
		return map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name, "namespace": "ns"},
		}
	}
	list := func(kind string, items ...map[string]any) map[string]any {
		its := make([]any, len(items))
		for i, it := range items {
			its[i] = it
		}
		return map[string]any{"apiVersion": "v1", "kind": kind, "items": its}
	}

	t.Run("promotes items and passes non-list docs through", func(t *testing.T) {
		t.Parallel()
		in := []map[string]any{list("ConfigMapList", cmItem("a"), cmItem("b")), cmItem("plain")}
		assert.Diff(t, docNames(FlattenLists(in)), []string{"a", "b", "plain"})
	})

	t.Run("derives identity for bare items in a typed list", func(t *testing.T) {
		t.Parallel()
		bare := map[string]any{"metadata": map[string]any{"name": "c"}}
		out := FlattenLists([]map[string]any{list("ConfigMapList", bare)})
		if len(out) != 1 {
			t.Fatalf("want 1 doc, got %d", len(out))
		}
		assert.Equal(t, DocKind(out[0]), "ConfigMap")
		assert.Equal(t, DocAPIVersion(out[0]), "v1")
		if _, mutated := bare["kind"]; mutated {
			t.Error("derivation mutated the original item map")
		}
	})

	t.Run("generic List keeps self-describing items", func(t *testing.T) {
		t.Parallel()
		item := map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "d", "namespace": "ns"},
		}
		out := FlattenLists([]map[string]any{list("List", item)})
		if len(out) != 1 {
			t.Fatalf("want 1 doc, got %d", len(out))
		}
		assert.Equal(t, DocKind(out[0]), "Deployment")
	})

	t.Run("flattens any typed list, not just ConfigMapList", func(t *testing.T) {
		t.Parallel()
		secret := func(name string) map[string]any {
			return map[string]any{
				"apiVersion": "v1", "kind": "Secret",
				"metadata": map[string]any{"name": name, "namespace": "ns"},
			}
		}
		out := FlattenLists([]map[string]any{list("SecretList", secret("s1"), secret("s2"))})
		assert.Diff(t, docNames(out), []string{"s1", "s2"})
		for _, d := range out {
			assert.Equal(t, DocKind(d), KindSecret)
		}
	})

	t.Run("non-collection kinds ending in List are left untouched", func(t *testing.T) {
		t.Parallel()
		// items are scalars, not resource maps.
		scalarItems := map[string]any{
			"apiVersion": "example.com/v1", "kind": "AllowList",
			"metadata": map[string]any{"name": "acl"},
			"items":    []any{"1.2.3.4", "5.6.7.8"},
		}
		// no top-level items at all.
		noItems := map[string]any{
			"apiVersion": "example.com/v1", "kind": "WaitList",
			"metadata": map[string]any{"name": "wl"},
			"spec":     map[string]any{"items": []any{}},
		}
		out := FlattenLists([]map[string]any{scalarItems, noItems})
		assert.Diff(t, docNames(out), []string{"acl", "wl"})
	})

	t.Run("no list returns the input slice unchanged", func(t *testing.T) {
		t.Parallel()
		in := []map[string]any{cmItem("a"), cmItem("b")}
		out := FlattenLists(in)
		if &out[0] != &in[0] {
			t.Error("expected the original slice (no allocation) when no List is present")
		}
	})
}

func TestParseDoc_PoolReuseConsistency(t *testing.T) {
	t.Parallel()
	hr := []byte(`
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: app
  namespace: default
spec:
  chart:
    spec:
      chart: app-template
      version: "3.5.1"
      sourceRef:
        kind: HelmRepository
        name: bjw-s
        namespace: flux-system
  values:
    replicas: 2
    env:
      TZ: UTC
`)
	ks := []byte(`
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: apps
  namespace: flux-system
spec:
  path: ./apps
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild:
    substitute:
      X: y
`)
	cm := []byte(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
  namespace: ns
data:
  key1: value1
  key2: value2
`)
	secret := "apiVersion: v1\nkind: Secret\nmetadata: {name: credentials, namespace: ns}\ndata: {key: dmFsdWU=}\n"
	raw := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: app}\nspec: {value: keep}\n"
	for _, input := range []string{string(hr), string(ks), string(cm), secret, raw} {
		t.Run(DocKind(mustYAML(t, input)), func(t *testing.T) {
			t.Parallel()
			docs, err := DecodeDocs(strings.NewReader(input))
			assert.NoError(t, err)
			obj, _, _, err := ParseDoc(docs[0], defaultParseDocOptions())
			assert.NoError(t, err)
			before := Fingerprint(obj)
			var projected map[string]any
			if k, ok := obj.(*Kustomization); ok {
				projected, err = k.RenderDocument()
				assert.NoError(t, err)
			}
			ReleaseDoc(docs[0])
			next, err := DecodeDocs(strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next}\ndata: {key: next}\n"))
			assert.NoError(t, err)
			ReleaseDoc(next[0])
			assert.Equal(t, Fingerprint(obj), before)
			switch v := obj.(type) {
			case *Kustomization:
				doc, err := v.RenderDocument()
				assert.NoError(t, err)
				assert.Diff(t, doc, projected)
				start := make(chan struct{})
				done := make(chan error, 2)
				for i := range 2 {
					changed := v.Clone()
					changed.Path = fmt.Sprintf("typed-%d", i)
					changed.PostBuild.Substitute["X"] = changed.Path
					go func() {
						<-start
						edited, err := changed.RenderDocument()
						if err == nil && edited["spec"].(map[string]any)["path"] != changed.Path {
							err = fmt.Errorf("projection did not reflect typed path %q", changed.Path)
						}
						done <- err
					}()
				}
				close(start)
				for range 2 {
					assert.NoError(t, <-done)
				}
				unchanged, err := v.RenderDocument()
				assert.NoError(t, err)
				assert.Diff(t, unchanged, projected)
				assert.Diff(t, doc, projected)
				assert.Equal(t, v.PostBuild.Substitute["X"], "y")
			case *ConfigMap:
				assert.Equal(t, v.Data["key1"], any("value1"))
			case *Secret:
				assert.Equal(t, v.Data["key"], any("dmFsdWU="))
			case *RawObject:
				assert.Equal(t, v.Spec["value"], any("keep"))
			}
		})
	}

	for _, input := range []string{string(hr), string(ks), string(cm), secret, raw} {
		docs, err := DecodeDocs(strings.NewReader(input))
		assert.NoError(t, err)
		doc := docs[0]
		delete(doc["metadata"].(map[string]any), "namespace")
		before := DeepCopyMap(doc)
		opts := ParseDocOptions{WipeSecrets: true, DefaultNamespace: "apps"}
		obj, authoritative, changed, err := ParseDoc(doc, opts)
		assert.NoError(t, err)
		assert.Equal(t, obj.Named().Namespace, "apps")
		assert.Equal(t, changed, true)
		replay, retained, changed, err := ParseDoc(authoritative, opts)
		assert.NoError(t, err)
		assert.Equal(t, replay.Named(), obj.Named())
		assert.Equal(t, changed, false)
		{
			next, err := DecodeDocs(strings.NewReader("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: next, namespace: other}\ndata: {key: next}\n"))
			assert.NoError(t, err)
			_, _, _, err = ParseDoc(next[0], opts)
			assert.NoError(t, err)
			ReleaseDoc(next[0])
		}
		assert.Diff(t, doc, before)
		expected := DeepCopyMap(before)
		expected["metadata"].(map[string]any)["namespace"] = "apps"
		assert.Diff(t, authoritative, expected)
		assert.Diff(t, retained, expected)
		switch obj := obj.(type) {
		case *RawObject:
			assert.Diff(t, obj.Spec, before["spec"])
		}
	}
}

// TestSplitDocs_AliasAsMapKey pins the home-ops pattern of using a
// YAML anchor as a mapping key (e.g. `&app sonarr` in metadata.name,
// referenced later as `*app :` to share the literal across the doc).
// Sigs/Flux YAML parsers resolve the alias to its underlying scalar
// before assigning into the map; flate's manual node walk used to
// reject any non-scalar key kind. See m00nwtchr/homelab-cluster
// apps/media/sonarr/app/helmrelease.yaml.
func TestSplitDocs_AliasAsMapKey(t *testing.T) {
	t.Parallel()
	data := []byte(`
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata:
  name: &app sonarr
spec:
  values:
    controllers:
      *app :
        replicas: 2
`)
	docs, err := SplitDocs(data)
	if err != nil {
		t.Fatalf("SplitDocs: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
	controllers, _ := docs[0]["spec"].(map[string]any)["values"].(map[string]any)["controllers"].(map[string]any)
	if _, ok := controllers["sonarr"]; !ok {
		t.Errorf("alias key not resolved to scalar; controllers=%v", controllers)
	}
}

func TestParseDoc_BuildDirectives(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"Kustomization", "Component"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			obj, _, _, err := ParseDoc(map[string]any{"kind": kind, "apiVersion": "kustomize.config.k8s.io/v1beta1"}, ParseDocOptions{DefaultNamespace: "apps"})
			if err != nil || obj != nil {
				t.Fatalf("ParseDoc = %T, %v; want nil, nil", obj, err)
			}
		})
	}
}

func TestParseHelmRelease_DisableValidationCombinations(t *testing.T) {
	t.Parallel()
	for _, flags := range []struct {
		name            string
		schema, openAPI bool
	}{
		{name: "neither"},
		{name: "schema", schema: true},
		{name: "openapi", openAPI: true},
		{name: "both", schema: true, openAPI: true},
	} {
		for _, stage := range []string{"neither", "install", "upgrade", "both"} {
			t.Run(flags.name+"/"+stage, func(t *testing.T) {
				t.Parallel()
				enabled := fmt.Sprintf("{disableSchemaValidation: %t, disableOpenAPIValidation: %t}", flags.schema, flags.openAPI)
				var spec string
				if stage == "install" || stage == "both" {
					spec += "  install: " + enabled + "\n"
				}
				if stage == "upgrade" || stage == "both" {
					spec += "  upgrade: " + enabled + "\n"
				}
				hr, err := parseHelmRelease(helmReleaseDoc(t, spec))
				if err != nil {
					t.Fatal(err)
				}
				wantSchema := (stage == "install" || stage == "both") && flags.schema
				wantOpenAPI := stage != "neither" && flags.openAPI
				if hr.DisableSchemaValidation != wantSchema || hr.DisableOpenAPIValidation != wantOpenAPI {
					t.Errorf("validation flags = (%t, %t), want (%t, %t)", hr.DisableSchemaValidation, hr.DisableOpenAPIValidation, wantSchema, wantOpenAPI)
				}
			})
		}
	}
}

func TestCheckAPIVersion_ExactGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		version string
		wantErr error
	}{
		{version: SourceDomain + "/v1"},
		{version: SourceDomain + ".example.com/v1", wantErr: ErrInput},
	} {
		t.Run(tc.version, func(t *testing.T) {
			t.Parallel()
			err := checkAPIVersion(map[string]any{"apiVersion": tc.version}, SourceDomain)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestResolve_NamespacePrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, forced, authored, fallback, want string }{
		{"forced", "forced", "authored", "fallback", "forced"},
		{"authored", "", "authored", "fallback", "authored"},
		{"fallback", "", "", "fallback", "fallback"},
		{"empty", "", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.authored, Name: "app"}
			want := NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.want, Name: "app"}
			assert.Equal(t, Resolve(id, tc.forced, tc.fallback), want)
			assert.Equal(t, id, NamedResource{Group: FluxKustomizeDomain, Kind: KindKustomization, Namespace: tc.authored, Name: "app"})
		})
	}
}

func TestNamedResource_DecodeIdentity(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{KindGitRepository, KindOCIRepository, KindBucket} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			id := NamedResource{Group: SourceDomain, Kind: kind, Namespace: "other", Name: "entry"}
			obj, err := id.DecodeIdentity()
			assert.NoError(t, err)
			assert.Equal(t, obj.Named(), id)
			assert.Equal(t, KindOf(obj).APIVersion(), sourcev1.GroupVersion.String())
			switch kind {
			case KindGitRepository:
				if g, ok := obj.(*GitRepository); !ok || g.URL != "file://." || g.Provider != sourcev1.GitProviderGeneric {
					t.Fatalf("decoded Git identity: %#v", obj)
				}
			case KindOCIRepository:
				if o, ok := obj.(*OCIRepository); !ok || o.URL != "file://." || o.Provider != sourcev1.GenericOCIProvider {
					t.Fatalf("decoded OCI identity: %#v", obj)
				}
			case KindBucket:
				if b, ok := obj.(*Bucket); !ok || b.Endpoint != "file://." || b.BucketName != id.Name || b.Provider != sourcev1.BucketProviderGeneric {
					t.Fatalf("decoded bucket identity: %#v", obj)
				}
			}
			doc := map[string]any{"apiVersion": sourcev1.GroupVersion.String(), "kind": kind, "metadata": map[string]any{"name": id.Name, "namespace": id.Namespace}}
			_, _, _, err = ParseDoc(doc, defaultParseDocOptions())
			if !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
				t.Fatalf("authored missing spec: %v", err)
			}
		})
	}
	for _, id := range []NamedResource{
		{Group: "wrong.group", Kind: KindGitRepository, Namespace: "other", Name: "entry"},
		{Group: SourceDomain, Kind: "Unknown", Namespace: "other", Name: "entry"},
		{Group: SourceDomain, Kind: KindBucket, Name: "entry"},
		{Group: SourceDomain, Kind: KindOCIRepository, Namespace: "other"},
		{Kind: KindConfigMap, Namespace: "other", Name: "entry"},
	} {
		if obj, err := id.DecodeIdentity(); obj != nil || !errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) {
			t.Fatalf("invalid identity %s: %v, %v", id, obj, err)
		}
	}
}

func TestBootstrapAlias_SourceKinds(t *testing.T) {
	t.Parallel()
	ignore := "excluded/"
	ref := &sourcev1.GitRepositoryRef{Tag: "v1"}
	git := &GitRepository{Name: "entry", Namespace: "other", URL: "https://fixture.invalid/repo", Reference: ref, Ignore: &ignore, SparseCheckout: []string{"apps"}, RecurseSubmodules: true}
	oci := &OCIRepository{Name: "entry", Namespace: "other", URL: "oci://fixture.invalid/repo", Ignore: &ignore}
	bucket := &Bucket{Name: "entry", Namespace: "other", Endpoint: "fixture.invalid", BucketName: "authored"}
	for _, obj := range []interface {
		BaseManifest
		BootstrapAlias(string) (BaseManifest, *GitRepository)
	}{git, oci, bucket} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			before := Fingerprint(obj)
			alias, descriptor := obj.BootstrapAlias("/repo/subtree")
			assert.Equal(t, alias.Named(), obj.Named())
			if alias == obj || Fingerprint(obj) != before {
				t.Fatal("alias mutated or retained receiver")
			}
			switch a := alias.(type) {
			case *GitRepository:
				if descriptor != git || a.URL != "file:///repo/subtree" || a.Reference != ref || a.Ignore != &ignore || !a.RecurseSubmodules || len(a.SparseCheckout) != 1 {
					t.Fatalf("Git policy/view: %+v, %p", a, descriptor)
				}
			case *OCIRepository:
				if descriptor != nil || a.URL != "file:///repo/subtree" || a.Ignore != &ignore {
					t.Fatalf("OCI policy/view: %+v", a)
				}
			case *Bucket:
				if descriptor != nil || a.Endpoint != "file:///repo/subtree" || a.BucketName != a.Name {
					t.Fatalf("bucket policy/view: %+v", a)
				}
			}
		})
	}
}
