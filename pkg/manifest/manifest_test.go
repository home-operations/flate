package manifest

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
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

// TestParse_Suspend covers the spec.suspend passthrough across the
// three suspendable kinds (HelmRelease, Kustomization, Bucket); each
// parse fn returns a distinct type sharing a Suspend bool field.
func TestParse_Suspend(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		get  func(map[string]any) (bool, error)
	}{
		{
			name: "HelmRelease",
			yaml: `
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: hr, namespace: ns}
spec:
  suspend: true
  chart:
    spec:
      chart: c
      sourceRef: {kind: HelmRepository, name: r, namespace: ns}
`,
			get: func(d map[string]any) (bool, error) {
				hr, err := parseHelmRelease(d)
				if err != nil {
					return false, err
				}
				return hr.Suspend, nil
			},
		},
		{
			name: "Kustomization",
			yaml: `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: ks, namespace: ns}
spec:
  suspend: true
  path: ./apps
  sourceRef: {kind: GitRepository, name: src, namespace: ns}
  interval: 5m
`,
			get: func(d map[string]any) (bool, error) {
				ks, err := parseKustomization(d)
				if err != nil {
					return false, err
				}
				return ks.Suspend, nil
			},
		},
		{
			name: "Bucket",
			yaml: `
apiVersion: source.toolkit.fluxcd.io/v1
kind: Bucket
metadata: {name: b, namespace: ns}
spec:
  suspend: true
  bucketName: x
  endpoint: e
  interval: 5m
`,
			get: func(d map[string]any) (bool, error) {
				b, err := parseBucket(d)
				if err != nil {
					return false, err
				}
				return b.Suspend, nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suspend, err := tc.get(mustYAML(t, tc.yaml))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if !suspend {
				t.Errorf("expected Suspend=true; got false")
			}
		})
	}
}

func TestParseHelmRelease_ServiceAccountName(t *testing.T) {
	hr, err := parseHelmRelease(helmReleaseDoc(t, "  serviceAccountName: privileged-installer"))
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
	}
	if hr.ServiceAccountName != "privileged-installer" {
		t.Errorf("ServiceAccountName = %q, want %q", hr.ServiceAccountName, "privileged-installer")
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

func TestParseHelmRelease_CRDsPolicy(t *testing.T) {
	cases := []struct {
		name     string
		specBody string
		want     string
	}{
		{"install only", "  install: {crds: Create}", "Create"},
		{"upgrade wins over install", "  install: {crds: Create}\n  upgrade: {crds: CreateReplace}", "CreateReplace"},
		{"skip suppresses", "  upgrade: {crds: Skip}", "Skip"},
		{"empty when neither set", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
	hc, err := parseHelmChartSource(doc)
	if err != nil {
		t.Fatalf("parseHelmChartSource: %v", err)
	}
	if hc.ReconcileStrategy != "Revision" {
		t.Errorf("ReconcileStrategy = %q, want %q", hc.ReconcileStrategy, "Revision")
	}
}

func TestParseHelmChartSource_PreservesEmptyNamespace(t *testing.T) {
	doc := mustYAML(t, `
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmChart
metadata: {name: c}
spec:
  chart: nginx
  sourceRef: {kind: HelmRepository, name: r}
`)
	hc, err := parseHelmChartSource(doc)
	if err != nil {
		t.Fatalf("parseHelmChartSource: %v", err)
	}
	if hc.Namespace != "" {
		t.Errorf("namespace=%q want empty before loader inheritance/defaulting", hc.Namespace)
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
			g, err := parseGitRepository(doc)
			if err != nil {
				t.Fatalf("parseGitRepository: %v", err)
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
				g, err := parseGitRepository(d)
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
				b, err := parseBucket(d)
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
	g, err := parseGitRepository(doc)
	if err != nil {
		t.Fatalf("parseGitRepository: %v", err)
	}
	wantDirs := []string{"kubernetes/apps/media", "kubernetes/components/shared"}
	assert.Diff(t, g.SparseCheckout, wantDirs)
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
	g, err := parseGitRepository(doc)
	if err != nil {
		t.Fatalf("parseGitRepository: %v", err)
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
	_, err := parseKustomization(doc)
	if err == nil {
		t.Fatal("expected parse error for empty sourceRef.name with kind set")
	}
	if !strings.Contains(err.Error(), "sourceRef.name is empty") {
		t.Errorf("error should mention sourceRef.name being empty; got %v", err)
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
	b, err := parseBucket(doc)
	if err != nil {
		t.Fatalf("parseBucket: %v", err)
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

// TestParse_EmptySecretRefNameRejected pins the schema check that
// turns a typo'd `secretRef: {}` into a hard parse error. Without
// it, the empty name would fall through to a runtime lookup of
// "<ns>/" → "secret not found" → with --allow-missing-secrets on,
// silent soft-skip — masking the typo entirely.
func TestParse_EmptySecretRefNameRejected(t *testing.T) {
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
			want:  "spec.secretRef.name is empty",
			parse: func(d map[string]any) error { _, err := ParseOCIRepository(d); return err },
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
}

func TestParseResourceSet_PreservesEmptyNamespace(t *testing.T) {
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
	hr, err := parseHelmRelease(helmReleaseDoc(t, `  dependsOn:
    - name: other
    - {name: cross, namespace: other-ns}`))
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
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
			if tc.got != tc.want {
				t.Errorf("got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestParseHelmRelease_ReleaseNameOverride(t *testing.T) {
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
	hr, err := parseHelmRelease(doc)
	if err != nil {
		t.Fatalf("parseHelmRelease: %v", err)
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
	r, err := parseHelmRepository(doc)
	if err != nil {
		t.Fatalf("parseHelmRepository: %v", err)
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
	g, err := parseGitRepository(doc)
	if err != nil {
		t.Fatalf("parseGitRepository: %v", err)
	}
	if g.Reference == nil {
		t.Fatalf("expected Reference parsed")
	}
	if got, want := GitRefString(*g.Reference), "tag:v1.2.3"; got != want {
		t.Errorf("RefString = %q, want %q", got, want)
	}
}

func TestGitRefString_Precedence(t *testing.T) {
	ref := GitRepositoryRef{
		Branch: "main",
		Tag:    "v1.0.0",
		SemVer: ">=1.0",
		Name:   "refs/heads/release",
		Commit: "0123456789abcdef0123456789abcdef01234567",
	}
	if got, want := GitRefString(ref), "commit:0123456789abcdef0123456789abcdef01234567"; got != want {
		t.Errorf("GitRefString precedence = %q, want %q", got, want)
	}
	ref.Commit = ""
	if got, want := GitRefString(ref), "name:refs/heads/release"; got != want {
		t.Errorf("GitRefString name precedence = %q, want %q", got, want)
	}
	ref.Name = ""
	if got, want := GitRefString(ref), "semver:>=1.0"; got != want {
		t.Errorf("GitRefString semver precedence = %q, want %q", got, want)
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
	k, err := parseKustomization(doc)
	if err != nil {
		t.Fatalf("parseKustomization: %v", err)
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

	kept, dropped := filterDependsOn(k.DependsOn, map[string]struct{}{"flux-system/infra": {}})
	if len(kept) != 1 || kept[0].NamespacedName() != "flux-system/infra" {
		t.Errorf("filterDependsOn kept = %v", kept)
	}
	if dropped == 0 {
		t.Errorf("expected at least one dropped entry")
	}
}

// filterDependsOn is the free function shared by KS and HR pruning;
// verify it works on a synthetic HR-style dep list too.
func TestFilterDependsOn_AcrossKinds(t *testing.T) {
	deps := []DependencyRef{
		{NamedResource: NamedResource{Kind: KindHelmRelease, Namespace: "media", Name: "plex"}},
		{NamedResource: NamedResource{Kind: KindHelmRelease, Namespace: "media", Name: "gone"}},
	}
	known := map[string]struct{}{"media/plex": {}}
	kept, dropped := filterDependsOn(deps, known)
	if len(kept) != 1 || kept[0].NamespacedName() != "media/plex" {
		t.Errorf("kept = %v", kept)
	}
	if dropped != 1 {
		t.Errorf("dropped = %d, want 1", dropped)
	}

	// nil-known: every dep dropped.
	_, dropped = filterDependsOn(deps, nil)
	if dropped != 2 {
		t.Errorf("nil-known dropped = %d, want 2", dropped)
	}

	// empty deps: no work, no allocation.
	out, dropped := filterDependsOn(nil, known)
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
	assert.Diff(t, sub, map[string]any{"K": "v"})
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
	cm, err := parseConfigMap(doc, true)
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
		cm, err := parseConfigMap(mustYAML(t, src), true)
		if err != nil {
			t.Fatalf("parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": fmt.Sprintf(ValuePlaceholderTemplate, "BASE_DOMAIN"),
			"PLAINTEXT":   "keep-me",
		})
	})
	t.Run("preserved when wipeSecrets=false", func(t *testing.T) {
		cm, err := parseConfigMap(mustYAML(t, src), false)
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
		s, err := parseSecret(mustYAML(t, yamlDoc), true)
		if err != nil {
			t.Fatalf("parseSecret: %v", err)
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
		s, err := parseSecret(mustYAML(t, yamlDoc), false)
		if err != nil {
			t.Fatalf("parseSecret: %v", err)
		}
		if s.Data["password"] != "dGVzdA==" {
			t.Errorf("data was wiped despite false: %v", s.Data["password"])
		}
	})
}

// TestParseSecret_Idempotent pins the copy-before-mutate fix: parseSecret
// must not write placeholders back into the caller-owned doc map.
// Previously the wipe loop mutated doc["data"] in place, so a second
// parse on the same doc would re-encode an already-encoded placeholder,
// producing base64(PLACEHOLDER_<base64(PLACEHOLDER_key)>) and causing
// spurious EventObjectAdded on every cache-hit reconcile.
func TestParseSecret_Idempotent(t *testing.T) {
	const yamlDoc = `
apiVersion: v1
kind: Secret
metadata: {name: s, namespace: ns}
data:
  token: c2VjcmV0
stringData:
  apiKey: plaintext
`
	doc := mustYAML(t, yamlDoc)

	s1, err := parseSecret(doc, true)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	s2, err := parseSecret(doc, true)
	if err != nil {
		t.Fatalf("second parse: %v", err)
	}

	// Both parses must yield identical placeholder values.
	if s1.Data["token"] != s2.Data["token"] {
		t.Errorf("data not idempotent: first=%q second=%q", s1.Data["token"], s2.Data["token"])
	}
	if s1.StringData["apiKey"] != s2.StringData["apiKey"] {
		t.Errorf("stringData not idempotent: first=%q second=%q", s1.StringData["apiKey"], s2.StringData["apiKey"])
	}

	// The original doc must be unmodified: still the raw base64 value.
	if got := doc["data"].(map[string]any)["token"]; got != "c2VjcmV0" {
		t.Errorf("parseSecret mutated doc[\"data\"]: got %q want %q", got, "c2VjcmV0")
	}
	if got := doc["stringData"].(map[string]any)["apiKey"]; got != "plaintext" {
		t.Errorf("parseSecret mutated doc[\"stringData\"]: got %q want %q", got, "plaintext")
	}
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
			obj, err := ParseDoc(mustYAML(t, tc.yaml), defaultParseDocOptions())
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

// TestRawObject_DoesNotAliasParsedDoc pins the deep-copy fix: a
// RawObject.Spec must not alias the loader's parsed YAML map.
// Previously r.Spec = doc["spec"].(map[string]any) shared the map
// pointer; mutating r.Spec corrupted the original doc the loader
// reused across passes.
func TestRawObject_DoesNotAliasParsedDoc(t *testing.T) {
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
	if _, err := ParseDoc(map[string]any{"kind": "Foo"}, defaultParseDocOptions()); err == nil || !strings.Contains(err.Error(), "apiVersion") {
		t.Errorf("expected apiVersion error, got %v", err)
	}
	// Bare data files (no kind:) are silently dropped as RawObjects —
	// helm values, config blobs, etc. that aren't k8s resources.
	obj, err := ParseDoc(map[string]any{"apiVersion": "v1"}, defaultParseDocOptions())
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
		in := []map[string]any{list("ConfigMapList", cmItem("a"), cmItem("b")), cmItem("plain")}
		assert.Diff(t, docNames(FlattenLists(in)), []string{"a", "b", "plain"})
	})

	t.Run("derives identity for bare items in a typed list", func(t *testing.T) {
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
		in := []map[string]any{cmItem("a"), cmItem("b")}
		out := FlattenLists(in)
		if &out[0] != &in[0] {
			t.Error("expected the original slice (no allocation) when no List is present")
		}
	})
}

// TestParseDoc_PoolReuseConsistency drives the DecodeDocs sync.Pool
// hard: 1000 parse-and-release iterations on a fixed input must yield
// byte-identical typed results. Catches any aliasing or stale-state
// bug introduced by reusing a cleared map[string]any across decodes.
//
// Covers the three retention shapes:
//   - HelmRelease (decodeTyped JSON round-trip, no doc retention)
//   - Kustomization (retains the TOP-LEVEL doc as Contents — must not
//     be returned to the pool)
//   - ConfigMap (aliases the inner data submap; clear(top) leaves the
//     submap independently rooted)
func TestParseDoc_PoolReuseConsistency(t *testing.T) {
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

	const iters = 1000
	opts := defaultParseDocOptions()

	for i := range iters {
		// HelmRelease: doc fully releasable.
		docs, err := DecodeDocs(bytes.NewReader(hr))
		if err != nil {
			t.Fatalf("HR iter %d DecodeDocs: %v", i, err)
		}
		obj, err := ParseDoc(docs[0], opts)
		if err != nil {
			t.Fatalf("HR iter %d ParseDoc: %v", i, err)
		}
		got, ok := obj.(*HelmRelease)
		if !ok {
			t.Fatalf("HR iter %d: not *HelmRelease", i)
		}
		if got.Name != "app" || got.Namespace != "default" {
			t.Fatalf("HR iter %d: wrong identity %s/%s", i, got.Namespace, got.Name)
		}
		if got.Chart.Name != "app-template" || got.Chart.Version != "3.5.1" {
			t.Fatalf("HR iter %d: chart drift %+v", i, got.Chart)
		}
		ReleaseIfNotRetained(docs[0], obj)

		// Kustomization: must NOT be released (Contents retains doc).
		ksDocs, err := DecodeDocs(bytes.NewReader(ks))
		if err != nil {
			t.Fatalf("KS iter %d DecodeDocs: %v", i, err)
		}
		kobj, err := ParseDoc(ksDocs[0], opts)
		if err != nil {
			t.Fatalf("KS iter %d ParseDoc: %v", i, err)
		}
		k, ok := kobj.(*Kustomization)
		if !ok {
			t.Fatalf("KS iter %d: not *Kustomization", i)
		}
		if k.Name != "apps" || k.Path != "./apps" {
			t.Fatalf("KS iter %d: drift %s %s", i, k.Name, k.Path)
		}
		// Validate retained Contents survives ReleaseIfNotRetained.
		ReleaseIfNotRetained(ksDocs[0], kobj)
		if k.Contents["kind"] != "Kustomization" {
			t.Fatalf("KS iter %d: Contents corrupted after Release: %v", i, k.Contents)
		}

		// ConfigMap: aliases inner data submap. Release of top-level
		// must not corrupt the parsed inner data.
		cmDocs, err := DecodeDocs(bytes.NewReader(cm))
		if err != nil {
			t.Fatalf("CM iter %d DecodeDocs: %v", i, err)
		}
		cobj, err := ParseDoc(cmDocs[0], opts)
		if err != nil {
			t.Fatalf("CM iter %d ParseDoc: %v", i, err)
		}
		c, ok := cobj.(*ConfigMap)
		if !ok {
			t.Fatalf("CM iter %d: not *ConfigMap", i)
		}
		if c.Data["key1"] != "value1" || c.Data["key2"] != "value2" {
			t.Fatalf("CM iter %d: data drift %v", i, c.Data)
		}
		ReleaseIfNotRetained(cmDocs[0], cobj)
		// Re-check after release: pooled top-level is gone but the
		// inner submap must still hold our values.
		if c.Data["key1"] != "value1" || c.Data["key2"] != "value2" {
			t.Fatalf("CM iter %d: data corrupted post-release %v", i, c.Data)
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

func TestIsKustomizeBuildDirective(t *testing.T) {
	cases := []struct {
		name string
		obj  BaseManifest
		want bool
	}{
		{"kustomize-config Kustomization", &RawObject{Kind: "Kustomization", APIVersion: "kustomize.config.k8s.io/v1beta1"}, true},
		{"kustomize-config Component", &RawObject{Kind: "Component", APIVersion: "kustomize.config.k8s.io/v1alpha1"}, true},
		{"flux Kustomization (typed)", &Kustomization{}, false},
		{"unknown resource RawObject", &RawObject{Kind: "Widget", APIVersion: "example.com/v1"}, false},
		{"helmrelease (typed)", &HelmRelease{}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsKustomizeBuildDirective(tc.obj); got != tc.want {
				t.Errorf("IsKustomizeBuildDirective = %v, want %v", got, tc.want)
			}
		})
	}
}
