package manifest

import (
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
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
	if a.Compare(b) >= 0 {
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

func TestParseHelmRelease_ServiceAccountName(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  serviceAccountName: privileged-installer
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`)
	hr, err := ParseHelmRelease(doc)
	if err != nil {
		t.Fatalf("ParseHelmRelease: %v", err)
	}
	if hr.ServiceAccountName != "privileged-installer" {
		t.Errorf("ServiceAccountName = %q, want %q", hr.ServiceAccountName, "privileged-installer")
	}
}

func TestParseHelmRelease_CRDsPolicy(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "install only",
			yaml: `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  install: {crds: Create}
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`,
			want: "Create",
		},
		{
			name: "upgrade wins over install",
			yaml: `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  install: {crds: Create}
  upgrade: {crds: CreateReplace}
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`,
			want: "CreateReplace",
		},
		{
			name: "skip suppresses",
			yaml: `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  upgrade: {crds: Skip}
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`,
			want: "Skip",
		},
		{
			name: "empty when neither set",
			yaml: `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hr, err := ParseHelmRelease(mustYAML(t, tc.yaml))
			if err != nil {
				t.Fatalf("ParseHelmRelease: %v", err)
			}
			if hr.CRDsPolicy != tc.want {
				t.Errorf("CRDsPolicy = %q, want %q", hr.CRDsPolicy, tc.want)
			}
		})
	}
}

func TestParseHelmChartSource_ReconcileStrategy(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: c, namespace: ns}
spec:
  chart: nginx
  reconcileStrategy: Revision
  sourceRef: {kind: HelmRepository, name: r}
`)
	hc, err := ParseHelmChartSource(doc)
	if err != nil {
		t.Fatalf("ParseHelmChartSource: %v", err)
	}
	if hc.ReconcileStrategy != "Revision" {
		t.Errorf("ReconcileStrategy = %q, want %q", hc.ReconcileStrategy, "Revision")
	}
}

func TestParseGitRepository_Verify(t *testing.T) {
	cases := []struct {
		name     string
		modeYAML string
		want     string
	}{
		{"HEAD", "HEAD", "HEAD"},
		{"head legacy lowercase normalizes", "head", "HEAD"},
		{"Tag", "Tag", "Tag"},
		{"TagAndHEAD", "TagAndHEAD", "TagAndHEAD"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: r, namespace: flux-system}
spec:
  url: https://github.com/owner/repo.git
  verify:
    mode: `+tc.modeYAML+`
    secretRef:
      name: trusted-pgp-keys
  interval: 5m
`)
			g, err := ParseGitRepository(doc)
			if err != nil {
				t.Fatalf("ParseGitRepository: %v", err)
			}
			if g.Verification == nil {
				t.Fatalf("expected Verify parsed")
			}
			if string(g.Verification.Mode) != tc.want {
				t.Errorf("Mode = %q, want %q", g.Verification.Mode, tc.want)
			}
			if g.Verification.SecretRef.Name != "trusted-pgp-keys" {
				t.Errorf("SecretRef = %+v", g.Verification.SecretRef)
			}
		})
	}
}

func TestParseProxySecretRef_AllSourceKinds(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		get  func(doc map[string]any) (*LocalObjectReference, error)
	}{
		{
			name: "GitRepository",
			yaml: `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: r, namespace: flux-system}
spec:
  url: https://github.com/owner/repo.git
  proxySecretRef: {name: corp-proxy}
  interval: 5m
`,
			get: func(d map[string]any) (*LocalObjectReference, error) {
				g, err := ParseGitRepository(d)
				if err != nil {
					return nil, err
				}
				return g.ProxySecretRef, nil
			},
		},
		{
			name: "OCIRepository",
			yaml: `
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: r, namespace: ns}
spec:
  url: oci://ghcr.io/x/y
  proxySecretRef: {name: corp-proxy}
  interval: 5m
`,
			get: func(d map[string]any) (*LocalObjectReference, error) {
				o, err := ParseOCIRepository(d)
				if err != nil {
					return nil, err
				}
				return o.ProxySecretRef, nil
			},
		},
		{
			name: "Bucket",
			yaml: `
apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata: {name: b, namespace: ns}
spec:
  bucketName: x
  endpoint: minio:9000
  proxySecretRef: {name: corp-proxy}
  interval: 5m
`,
			get: func(d map[string]any) (*LocalObjectReference, error) {
				b, err := ParseBucket(d)
				if err != nil {
					return nil, err
				}
				return b.ProxySecretRef, nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ref, err := tc.get(mustYAML(t, tc.yaml))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if ref == nil || ref.Name != "corp-proxy" {
				t.Errorf("ProxySecretRef = %+v, want {Name: corp-proxy}", ref)
			}
		})
	}
}

func TestParseGitRepository_SparseCheckout(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: monorepo, namespace: flux-system}
spec:
  url: https://github.com/owner/big-monorepo.git
  sparseCheckout:
    - kubernetes/apps/media
    - kubernetes/components/shared
  interval: 5m
`)
	g, err := ParseGitRepository(doc)
	if err != nil {
		t.Fatalf("ParseGitRepository: %v", err)
	}
	wantDirs := []string{"kubernetes/apps/media", "kubernetes/components/shared"}
	if !slices.Equal(g.SparseCheckout, wantDirs) {
		t.Errorf("SparseCheckout = %v, want %v", g.SparseCheckout, wantDirs)
	}
}

func TestParseGitRepository_RefNameAndSubmodules(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pr, namespace: flux-system}
spec:
  url: https://github.com/owner/repo.git
  ref:
    name: refs/pull/420/head
  recurseSubmodules: true
  interval: 5m
`)
	g, err := ParseGitRepository(doc)
	if err != nil {
		t.Fatalf("ParseGitRepository: %v", err)
	}
	if g.Reference == nil || g.Reference.Name != "refs/pull/420/head" {
		t.Errorf("Reference.Name = %q", g.Reference)
	}
	if !g.RecurseSubmodules {
		t.Errorf("RecurseSubmodules should be true")
	}
	if want := "name:refs/pull/420/head"; GitRefString(*g.Reference) != want {
		t.Errorf("RefString = %q, want %q", GitRefString(*g.Reference), want)
	}
}

// TestParseKustomization_SourceRefMissingNameRejected locks the
// truncated-YAML guard: a Kustomization with sourceRef.kind set but
// sourceRef.name empty (common shape for files that end mid-mapping)
// must fail at parse time with a clear "check for truncated YAML"
// hint, not 30 lines later as a misleading "path does not exist".
func TestParseKustomization_SourceRefMissingNameRejected(t *testing.T) {
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
	_, err := ParseKustomization(doc)
	if err == nil {
		t.Fatal("expected parse error for empty sourceRef.name with kind set")
	}
	if !strings.Contains(err.Error(), "sourceRef.name is empty") {
		t.Errorf("error should mention sourceRef.name being empty; got %v", err)
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

func TestParseBucket_GenericWithSecret(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata:
  name: assets
  namespace: apps
spec:
  bucketName: my-bucket
  endpoint: minio.minio.svc:9000
  insecure: true
  prefix: configs/
  region: us-east-1
  secretRef:
    name: minio-creds
  interval: 5m
`)
	b, err := ParseBucket(doc)
	if err != nil {
		t.Fatalf("ParseBucket: %v", err)
	}
	if b.Provider != "generic" {
		t.Errorf("default Provider should be 'generic', got %q", b.Provider)
	}
	if b.BucketName != "my-bucket" {
		t.Errorf("BucketName: %q", b.BucketName)
	}
	if b.Endpoint != "minio.minio.svc:9000" {
		t.Errorf("Endpoint: %q", b.Endpoint)
	}
	if !b.Insecure {
		t.Errorf("Insecure should be true")
	}
	if b.Prefix != "configs/" {
		t.Errorf("Prefix: %q", b.Prefix)
	}
	if b.SecretRef == nil || b.SecretRef.Name != "minio-creds" {
		t.Errorf("SecretRef: %+v", b.SecretRef)
	}
}

func TestParseBucket_Suspend(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata: {name: b, namespace: ns}
spec:
  suspend: true
  bucketName: x
  endpoint: e
  interval: 5m
`)
	b, err := ParseBucket(doc)
	if err != nil {
		t.Fatalf("ParseBucket: %v", err)
	}
	if !b.Suspend {
		t.Errorf("expected Suspend=true")
	}
}

func TestParseResourceSet(t *testing.T) {
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
	rs, err := ParseResourceSet(doc)
	if err != nil {
		t.Fatalf("ParseResourceSet: %v", err)
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
}

func TestParseResourceSet_DefaultsNamespace(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata:
  name: apps
spec: {}
`)
	rs, err := ParseResourceSet(doc)
	if err != nil {
		t.Fatalf("ParseResourceSet: %v", err)
	}
	if rs.Namespace != DefaultNamespace {
		t.Errorf("namespace=%q want %q", rs.Namespace, DefaultNamespace)
	}
}

func TestParseExternalArtifact(t *testing.T) {
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
	ea, err := ParseExternalArtifact(doc)
	if err != nil {
		t.Fatalf("ParseExternalArtifact: %v", err)
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
	k, err := ParseKustomization(doc)
	if err != nil {
		t.Fatalf("ParseKustomization: %v", err)
	}
	if len(k.DependsOn) != 1 {
		t.Fatalf("DependsOn len = %d", len(k.DependsOn))
	}
	if k.DependsOn[0].Name != "infra" {
		t.Errorf("name: %q", k.DependsOn[0].Name)
	}
	if !strings.Contains(k.DependsOn[0].ReadyExpr, "InfraInitialized") {
		t.Errorf("ReadyExpr lost: %q", k.DependsOn[0].ReadyExpr)
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
	if got := hr.DependsOn[0].NamespacedName(); got != "ns/other" {
		t.Errorf("DependsOn[0] = %q, want ns/other", got)
	}
	if hr.DependsOn[1].Namespace != "other-ns" || hr.DependsOn[1].Name != "cross" {
		t.Errorf("DependsOn[1] = %+v", hr.DependsOn[1])
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
		{"ReleaseName", hr.ReleaseName(), "podinfo"},
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

func TestParseHelmRelease_ReleaseNameOverride(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  releaseName: my-explicit-release
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`)
	hr, err := ParseHelmRelease(doc)
	if err != nil {
		t.Fatalf("ParseHelmRelease: %v", err)
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
		HelmChartSpec: sourcev1.HelmChartSpec{
			Chart: "podinfo", Version: "6.3.2",
			SourceRef: sourcev1.LocalHelmChartSourceReference{
				Name: "podinfo", Kind: KindHelmRepository,
			},
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

	missing := &HelmRelease{Chart: HelmChart{RepoKind: KindHelmChart, RepoName: "absent", RepoNamespace: "ns"}}
	none := func(_, _ string) *HelmChartSource { return nil }
	if err := missing.ResolveChartRef(none); !errors.Is(err, ErrObjectNotFound) {
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
	if r.Type != "oci" {
		t.Errorf("RepoType = %q", r.Type)
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
	if g.Reference == nil {
		t.Fatalf("expected Reference parsed")
	}
	if got, want := GitRefString(*g.Reference), "tag:v1.2.3"; got != want {
		t.Errorf("RefString = %q, want %q", got, want)
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
	if len(k.DependsOn) != 2 {
		t.Fatalf("DependsOn len = %d, want 2", len(k.DependsOn))
	}
	if got := k.DependsOn[0].NamespacedName(); got != "flux-system/infra" {
		t.Errorf("DependsOn[0] = %q, want flux-system/infra", got)
	}
	if got := k.DependsOn[1].NamespacedName(); got != "other/extras" {
		t.Errorf("DependsOn[1] = %q, want other/extras", got)
	}
	if len(k.PostBuildSubstituteFrom) != 1 || !k.PostBuildSubstituteFrom[0].Optional {
		t.Errorf("substituteFrom = %+v", k.PostBuildSubstituteFrom)
	}
	if k.PostBuildSubstitute["DOMAIN"] != "example.com" {
		t.Errorf("substitute = %+v", k.PostBuildSubstitute)
	}

	kept, dropped := FilterDependsOn(k.DependsOn, map[string]struct{}{"flux-system/infra": {}})
	if len(kept) != 1 || kept[0].NamespacedName() != "flux-system/infra" {
		t.Errorf("FilterDependsOn kept = %v", kept)
	}
	if dropped == 0 {
		t.Errorf("expected at least one dropped entry")
	}
}

// FilterDependsOn is the free function shared by KS and HR pruning;
// verify it works on a synthetic HR-style dep list too.
func TestFilterDependsOn_AcrossKinds(t *testing.T) {
	deps := []DependencyRef{
		{NamedResource: NamedResource{Kind: KindHelmRelease, Namespace: "media", Name: "plex"}},
		{NamedResource: NamedResource{Kind: KindHelmRelease, Namespace: "media", Name: "gone"}},
	}
	known := map[string]struct{}{"media/plex": {}}
	kept, dropped := FilterDependsOn(deps, known)
	if len(kept) != 1 || kept[0].NamespacedName() != "media/plex" {
		t.Errorf("kept = %v", kept)
	}
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1", dropped)
	}

	// nil-known: every dep dropped.
	_, dropped = FilterDependsOn(deps, nil)
	if dropped != 2 {
		t.Errorf("nil-known dropped = %d, want 2", dropped)
	}

	// empty deps: no work, no allocation.
	out, dropped := FilterDependsOn(nil, known)
	if out != nil || dropped != 0 {
		t.Errorf("empty deps: out=%v dropped=%d", out, dropped)
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

// TestSplitDocs_AliasAsMapKey pins the home-ops pattern of using a
// YAML anchor as a mapping key (e.g. `&app sonarr` in metadata.name,
// referenced later as `*app :` to share the literal across the doc).
// Sigs/Flux YAML parsers resolve the alias to its underlying scalar
// before assigning into the map; flate's manual node walk used to
// reject any non-scalar key kind. See m00nwtchr/homelab-cluster
// apps/media/sonarr/app/helmrelease.yaml.
func TestSplitDocs_AliasAsMapKey(t *testing.T) {
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
