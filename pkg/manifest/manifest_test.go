package manifest

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

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
	n := NamedResource{Kind: "Kustomization", Namespace: "flux-system", Name: "apps"}
	if got, want := n.NamespacedName(), "flux-system/apps"; got != want {
		t.Errorf("NamespacedName = %q, want %q", got, want)
	}
	if got, want := n.String(), "Kustomization/flux-system/apps"; got != want {
		t.Errorf("String = %q, want %q", got, want)
	}

	cluster := NamedResource{Kind: "ClusterRole", Name: "view"}
	if got, want := cluster.NamespacedName(), "view"; got != want {
		t.Errorf("cluster-scoped NamespacedName = %q, want %q", got, want)
	}

	a := NamedResource{Kind: "A", Namespace: "ns", Name: "1"}
	b := NamedResource{Kind: "A", Namespace: "ns", Name: "2"}
	if !a.Less(b) || a.Compare(b) >= 0 {
		t.Errorf("expected a < b for %v / %v", a, b)
	}
}

func TestParseHelmRelease_Suspend(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  suspend: true
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`)
	hr, err := ParseHelmRelease(doc)
	if err != nil {
		t.Fatalf("ParseHelmRelease: %v", err)
	}
	if !hr.Suspend {
		t.Errorf("expected Suspend=true; got false")
	}
}

func TestParseKustomization_Suspend(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ks, namespace: ns}
spec:
  suspend: true
  path: ./apps
  sourceRef: {kind: GitRepository, name: src, namespace: ns}
  interval: 5m
`)
	ks, err := ParseKustomization(doc)
	if err != nil {
		t.Fatalf("ParseKustomization: %v", err)
	}
	if !ks.Suspend {
		t.Errorf("expected Suspend=true; got false")
	}
}

func TestParseHelmRelease_DependsOn(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  dependsOn:
    - name: other
    - {name: cross, namespace: other-ns}
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`)
	hr, err := ParseHelmRelease(doc)
	if err != nil {
		t.Fatalf("ParseHelmRelease: %v", err)
	}
	if got, want := len(hr.DependsOn), 2; got != want {
		t.Fatalf("DependsOn len = %d, want %d", got, want)
	}
	if hr.DependsOn[0] != "ns/other" {
		t.Errorf("DependsOn[0] = %q, want ns/other", hr.DependsOn[0])
	}
}

func TestParseHelmRelease_Inline(t *testing.T) {
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
	hr, err := ParseHelmRelease(doc)
	if err != nil {
		t.Fatalf("ParseHelmRelease: %v", err)
	}

	cases := []struct {
		name string
		got  any
		want any
	}{
		{"Name", hr.Name, "podinfo"},
		{"Namespace", hr.Namespace, "podinfo"},
		{"ReleaseName", hr.ReleaseName(), "podinfo-podinfo"},
		{"ReleaseNamespace", hr.ReleaseNamespace(), "apps"},
		{"Chart.Name", hr.Chart.Name, "podinfo"},
		{"Chart.Version", hr.Chart.Version, "6.3.2"},
		{"Chart.RepoKind", hr.Chart.RepoKind, "HelmRepository"},
		{"Chart.RepoName", hr.Chart.RepoName, "podinfo"},
		{"replicaCount", hr.Values["replicaCount"], float64(3)},
		{"ValuesFrom[0].Optional", hr.ValuesFrom[0].Optional, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Errorf("got %v, want %v", c.got, c.want)
			}
		})
	}
}

func TestParseHelmRelease_ChartRef(t *testing.T) {
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
	hr, err := ParseHelmRelease(doc)
	if err != nil {
		t.Fatalf("ParseHelmRelease: %v", err)
	}
	if hr.Chart.RepoKind != KindHelmChart || hr.Chart.Version != "" {
		t.Fatalf("expected unresolved chartRef, got %+v", hr.Chart)
	}

	src := &HelmChartSource{
		Name: "my-chart", Namespace: "flux-system",
		Chart: "podinfo", Version: "6.3.2",
		RepoName: "podinfo", RepoNamespace: "flux-system",
		RepoKind: KindHelmRepository,
	}
	if err := hr.ResolveChartRef(map[string]*HelmChartSource{src.ResourceFullName(): src}); err != nil {
		t.Fatalf("ResolveChartRef: %v", err)
	}
	if hr.Chart.Version != "6.3.2" || hr.Chart.Name != "podinfo" {
		t.Errorf("after resolve: %+v", hr.Chart)
	}

	missing := &HelmRelease{Chart: HelmChart{RepoKind: KindHelmChart, RepoName: "absent", RepoNamespace: "ns"}}
	if err := missing.ResolveChartRef(map[string]*HelmChartSource{}); !errors.Is(err, ErrObjectNotFound) {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestParseHelmRepository_OCI(t *testing.T) {
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
	r, err := ParseHelmRepository(doc)
	if err != nil {
		t.Fatalf("ParseHelmRepository: %v", err)
	}
	if r.RepoType != "oci" {
		t.Errorf("RepoType = %q", r.RepoType)
	}
	if got, want := r.HelmChartName(HelmChart{Name: "podinfo"}), "oci://ghcr.io/stefanprodan/charts/podinfo"; got != want {
		t.Errorf("HelmChartName = %q, want %q", got, want)
	}
}

func TestParseGitRepository_Ref(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: charts
  namespace: flux-system
spec:
  url: https://example.test/charts.git
  ref:
    tag: v1.2.3
`)
	g, err := ParseGitRepository(doc)
	if err != nil {
		t.Fatalf("ParseGitRepository: %v", err)
	}
	if got, want := g.Ref.RefString(), "tag:v1.2.3"; got != want {
		t.Errorf("RefString = %q, want %q", got, want)
	}
}

func TestParseOCIRepository_VersionedURL(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"tag", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: x, namespace: ns}
spec: {url: oci://example/x, ref: {tag: v1}}
`, "oci://example/x:v1"},
		{"digest", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: x, namespace: ns}
spec: {url: oci://example/x, ref: {digest: sha256:abc}}
`, "oci://example/x@sha256:abc"},
		{"no-ref", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: x, namespace: ns}
spec: {url: oci://example/x}
`, "oci://example/x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := ParseOCIRepository(mustYAML(t, tc.yaml))
			if err != nil {
				t.Fatalf("ParseOCIRepository: %v", err)
			}
			if got := r.VersionedURL(); got != tc.want {
				t.Errorf("VersionedURL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseKustomization_DependsOn(t *testing.T) {
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
	k, err := ParseKustomization(doc)
	if err != nil {
		t.Fatalf("ParseKustomization: %v", err)
	}
	wantDeps := []string{"flux-system/infra", "other/extras"}
	if !slices.Equal(k.DependsOn, wantDeps) {
		t.Errorf("DependsOn = %v, want %v", k.DependsOn, wantDeps)
	}
	if len(k.PostBuildSubstituteFrom) != 1 || !k.PostBuildSubstituteFrom[0].Optional {
		t.Errorf("substituteFrom = %+v", k.PostBuildSubstituteFrom)
	}
	if k.PostBuildSubstitute["DOMAIN"] != "example.com" {
		t.Errorf("substitute = %+v", k.PostBuildSubstitute)
	}

	k.ValidateDependsOn(map[string]struct{}{"flux-system/infra": {}})
	if !slices.Equal(k.DependsOn, []string{"flux-system/infra"}) {
		t.Errorf("ValidateDependsOn left %v", k.DependsOn)
	}
}

func TestKustomization_UpdatePostBuildSubstitutions(t *testing.T) {
	k := &Kustomization{Contents: map[string]any{"spec": map[string]any{}}}
	k.UpdatePostBuildSubstitutions(map[string]any{"K": "v"})

	if k.PostBuildSubstitute["K"] != "v" {
		t.Fatalf("substitute map not updated: %+v", k.PostBuildSubstitute)
	}
	spec := k.Contents["spec"].(map[string]any)
	post := spec["postBuild"].(map[string]any)
	sub := post["substitute"].(map[string]any)
	if sub["K"] != "v" {
		t.Errorf("contents not updated: %+v", sub)
	}
}

func TestParseConfigMap(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-vars
  namespace: flux-system
data:
  DOMAIN: example.com
`)
	cm, err := ParseConfigMap(doc)
	if err != nil {
		t.Fatalf("ParseConfigMap: %v", err)
	}
	if cm.Data["DOMAIN"] != "example.com" {
		t.Errorf("data = %+v", cm.Data)
	}
}

func TestParseSecret(t *testing.T) {
	const yamlDoc = `
apiVersion: v1
kind: Secret
metadata: {name: creds, namespace: flux-system}
data:
  password: dGVzdA==
stringData:
  apiKey: real-key
`
	t.Run("wiped", func(t *testing.T) {
		s, err := ParseSecret(mustYAML(t, yamlDoc), true)
		if err != nil {
			t.Fatalf("ParseSecret: %v", err)
		}
		wantPlaceholder := fmt.Sprintf(ValuePlaceholderTemplate, "password")
		wantB64 := base64.StdEncoding.EncodeToString([]byte(wantPlaceholder))
		if s.Data["password"] != wantB64 {
			t.Errorf("data not wiped: %v", s.Data["password"])
		}
		if s.StringData["apiKey"] != fmt.Sprintf(ValuePlaceholderTemplate, "apiKey") {
			t.Errorf("stringData not wiped: %v", s.StringData["apiKey"])
		}
	})

	t.Run("preserved", func(t *testing.T) {
		s, err := ParseSecret(mustYAML(t, yamlDoc), false)
		if err != nil {
			t.Fatalf("ParseSecret: %v", err)
		}
		if s.Data["password"] != "dGVzdA==" {
			t.Errorf("data was wiped despite false: %v", s.Data["password"])
		}
	})
}

func TestParseDoc_Dispatch(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		kind string
	}{
		{"kustomization", `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: x, namespace: ns}
spec: {path: ./a, sourceRef: {kind: GitRepository, name: r}}`, KindKustomization},
		{"helmrelease", `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: r, namespace: ns}
spec: {chart: {spec: {chart: c, sourceRef: {name: rr}}}}`, KindHelmRelease},
		{"helmrepository", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: r, namespace: ns}
spec: {url: https://example}`, KindHelmRepository},
		{"helmchart", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: hc, namespace: ns}
spec: {chart: c, sourceRef: {name: rr}}`, KindHelmChart},
		{"gitrepository", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: r, namespace: ns}
spec: {url: https://example.git}`, KindGitRepository},
		{"ocirepository", `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: r, namespace: ns}
spec: {url: oci://example}`, KindOCIRepository},
		{"configmap", `
apiVersion: v1
kind: ConfigMap
metadata: {name: c, namespace: ns}`, KindConfigMap},
		{"secret", `
apiVersion: v1
kind: Secret
metadata: {name: s, namespace: ns}`, KindSecret},
		{"unknown", `
apiVersion: example.com/v1
kind: Foo
metadata: {name: x}`, "Foo"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := ParseDoc(mustYAML(t, tc.yaml), DefaultParseDocOptions())
			if err != nil {
				t.Fatalf("ParseDoc: %v", err)
			}
			if obj.Named().Kind != tc.kind {
				t.Errorf("Kind = %q, want %q", obj.Named().Kind, tc.kind)
			}
		})
	}
}

func TestStripResourceAttributes(t *testing.T) {
	r := map[string]any{
		"kind": "Deployment",
		"metadata": map[string]any{
			"annotations": map[string]any{"config.kubernetes.io/index": "0", "keep": "yes"},
			"labels":      map[string]any{"internal.config.kubernetes.io/index": "1"},
		},
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]any{"config.kubernetes.io/index": "0"},
				},
			},
		},
	}
	StripResourceAttributes(r, StripAttributes)

	meta := r["metadata"].(map[string]any)
	ann := meta["annotations"].(map[string]any)
	if _, ok := ann["config.kubernetes.io/index"]; ok {
		t.Errorf("annotation not stripped")
	}
	if ann["keep"] != "yes" {
		t.Errorf("kept annotation lost")
	}
	if _, ok := meta["labels"]; ok {
		t.Errorf("empty labels should have been removed")
	}
	tplMeta := r["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	if _, ok := tplMeta["annotations"]; ok {
		t.Errorf("template annotation not stripped")
	}
}

func TestParseDoc_MissingFields(t *testing.T) {
	if _, err := ParseDoc(map[string]any{"kind": "Foo"}, DefaultParseDocOptions()); err == nil || !strings.Contains(err.Error(), "apiVersion") {
		t.Errorf("expected apiVersion error, got %v", err)
	}
	// Bare data files (no kind:) are silently dropped as RawObjects —
	// helm values, config blobs, etc. that aren't k8s resources.
	obj, err := ParseDoc(map[string]any{"apiVersion": "v1"}, DefaultParseDocOptions())
	if err != nil {
		t.Errorf("missing-kind docs should not error, got %v", err)
	}
	if _, ok := obj.(*RawObject); !ok {
		t.Errorf("expected RawObject for missing-kind doc, got %T", obj)
	}
}

func TestSplitDocs(t *testing.T) {
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
	if !slices.Equal(names, []string{"a", "b"}) {
		t.Errorf("names = %v", names)
	}
}
