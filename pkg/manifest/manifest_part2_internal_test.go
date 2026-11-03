package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/home-operations/flate/internal/assert"
)

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
	{
		assert.FatalIf(t, err != nil, "parseResourceSetInputProvider: %v", err)
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
`)
	ea, err := parseExternalArtifact(doc)
	{
		assert.FatalIf(t, err != nil, "parseExternalArtifact: %v", err)
	}
	{
		assert.ErrorIf(t, ea.Name != "pulled-tarball" || ea.Namespace != "apps", "identity: %+v", ea)
	}
	if ea.SourceRef == nil || ea.SourceRef.Kind != "ImageUpdateAutomation" {
		t.Errorf("sourceRef: %+v", ea.SourceRef)
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
	{
		assert.FatalIf(t, err != nil, "parseKustomization: %v", err)
	}
	deps := refsForUse(t, k, "dependsOn")
	{
		assert.FatalIf(t, len(deps) != 1, "DependsOn len = %d", len(deps))
	}
	if deps[0].Name != "infra" {
		t.Errorf("name: %q", deps[0].Name)
	}
	if !strings.Contains(*deps[0].ReadyExpr, "InfraInitialized") {
		t.Errorf("ReadyExpr lost: %q", *deps[0].ReadyExpr)
	}
}

func TestParseHelmRelease_DependsOn(t *testing.T) {
	t.Parallel()
	hr, err := parseHelmRelease(helmReleaseDoc(t, `  dependsOn:
    - name: other
    - {name: cross, namespace: other-ns}`))
	{
		assert.FatalIf(t, err != nil, "parseHelmRelease: %v", err)
	}
	deps := refsForUse(t, hr, "dependsOn")
	{
		got, want := len(deps), 2
		assert.FatalIf(t, got != want, "DependsOn len = %d, want %d", got, want)
	}
	{
		got := deps[0].NamespacedName()
		assert.ErrorIf(t, got != "ns/other", "DependsOn[0] = %q, want ns/other", got)
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
	{
		assert.FatalIf(t, err != nil, "parseHelmRelease: %v", err)
	}

	cases := []struct {
		name string
		got  any
		want any
	}{
		{"Name", hr.Name, "podinfo"},
		{"Namespace", hr.Namespace, "podinfo"},
		{"ReleaseName", hr.ReleaseName(), "apps-podinfo"},
		{"ReleaseNamespace", hr.ReleaseNamespace(), "apps"},
		{"Chart.Name", hr.Chart.Name, "podinfo"},
		{"Chart.Version", hr.Chart.Version, "6.3.2"},
		{"Chart.Source.Kind", hr.Chart.Source.Kind, "HelmChart"},
		{"Chart.Source.Name", hr.Chart.Source.Name, "podinfo-podinfo"},
		{"replicaCount", hr.Values["replicaCount"], float64(3)},
		{"ValuesFrom[0].Optional", hr.ValuesFrom[0].Optional, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				assert.ErrorIf(t, tc.got != tc.want, "got %v, want %v", tc.got, tc.want)
			}
		})
	}
}

func TestParseHelmRelease_ReleaseNameOverride(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, spec, want string }{
		{"metadata namespace only", "", "hr"},
		{"target namespace", "  targetNamespace: apps", "apps-hr"},
		{"explicit override", "  targetNamespace: apps\n  releaseName: my-explicit-release", "my-explicit-release"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr, err := parseHelmRelease(helmReleaseDoc(t, tc.spec))
			assert.NoError(t, err)
			before := Fingerprint(hr.HelmReleaseSpec)
			assert.Equal(t, hr.ReleaseName(), tc.want)
			assert.Equal(t, Fingerprint(hr.HelmReleaseSpec), before)
		})
	}
}

func TestReleaseName_Shortens(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, metadata, target, explicit, want string }{
		{"short", "short-name", "", "", "short-name"},
		{"boundary", strings.Repeat("a", 53), "", "", strings.Repeat("a", 53)},
		{"composed boundary", strings.Repeat("a", 48), "apps", "", "apps-" + strings.Repeat("a", 48)},
		{"long metadata", "this-is-a-very-long-helmrelease-name-that-exceeds-53-chars", "", "", "this-is-a-very-long-helmrelease-name-tha-23e95a162bda"},
		{"long composed", "demo", "namespace-with-a-very-long-name-for-release-composition", "", "namespace-with-a-very-long-name-for-rele-e5340b09d048"},
		{"explicit override", "demo", "namespace-with-a-very-long-name-for-release-composition", "chosen", "chosen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := &HelmRelease{
				Name:            tc.metadata,
				HelmReleaseSpec: helmv2.HelmReleaseSpec{TargetNamespace: tc.target, ReleaseName: tc.explicit},
			}
			assert.Equal(t, hr.ReleaseName(), tc.want)
			assert.Equal(t, hr.ReleaseName(), tc.want)
		})
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
	{
		assert.FatalIf(t, err != nil, "parseHelmRelease: %v", err)
	}
	assert.Equal(
		t,
		hr.Chart.Source,
		NamedResource{Group: SourceDomain, Kind: KindHelmChart, Namespace: "flux-system", Name: "my-chart"},
	)
	if hr.Chart.Source.Kind != KindHelmChart || hr.Chart.Version != "" {
		t.Fatalf("expected unresolved chartRef, got %+v", hr.Chart)
	}

	src := &HelmChartSource{
		Name: "my-chart", Namespace: "flux-system",
		Chart: "podinfo", Version: "6.3.2",
		SourceRef: sourcev1.LocalHelmChartSourceReference{
			Name: "podinfo", Kind: KindHelmRepository,
		},
	}
	lookup := func(id NamedResource) BaseManifest {
		if id == src.Named() {
			return src
		}
		return nil
	}
	{
		err := hr.ResolveChartSource(lookup)
		assert.FatalIf(t, err != nil, "ResolveChartSource: %v", err)
	}
	if hr.Chart.Version != "6.3.2" || hr.Chart.Name != "podinfo" {
		t.Errorf("after resolve: %+v", hr.Chart)
	}

	missing := &HelmRelease{
		ChartRef: &helmv2.CrossNamespaceSourceReference{Kind: KindHelmChart, Name: "absent", Namespace: "ns"},
	}
	none := func(NamedResource) BaseManifest { return nil }
	{
		err := missing.ResolveChartSource(none)
		assert.ErrorIf(t, !errors.Is(err, ErrObjectNotFound), "expected ErrObjectNotFound, got %v", err)
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
	{
		assert.FatalIf(t, err != nil, "parseHelmRepository: %v", err)
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
			{
				err := tc.ref.Validate()
				assert.FatalIf(t, err == nil, "%s", "invalid selected reference accepted")
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

//nolint:paralleltest // The test replaces a package variable shared by other tests.
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
	{
		assert.FatalIf(t, encoder == nil || encoder.buffer.Len() == 0, "%s", "render did not use an encoder buffer")
	}
	published := ks.projection.Load().raw
	snapshot := bytes.Clone(published)
	encoder.buffer.Bytes()[0] ^= 1
	assert.Diff(t, published, snapshot)
}

func TestRenderDocument_TypedSpec(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, spec string }{
		{"absent", "{}"},
		{"null fields", "{targetNamespace: null, namePrefix: null, nameSuffix: null, patches: null, images: null, components: null, buildMetadata: null}"},
		{"empty fields", "{targetNamespace: '', namePrefix: '', nameSuffix: '', patches: [], images: [], components: [], buildMetadata: []}"},
		{"zero elements", "{patches: [null], images: [null], components: [null], buildMetadata: [null]}"},
		{"metadata values", "{commonMetadata: {labels: {app: null, team: typed}, annotations: {note: null, owner: typed}}}"},
		{"generator-only fields", "{patchesStrategicMerge: [{metadata: {name: ignored}}], patchesJson6902: not-a-list}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := mustYAML(
				t,
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps}\nspec: "+tc.spec+"\n",
			)
			raw, err := json.Marshal(doc)
			assert.NoError(t, err)
			var cr kustomizev1.Kustomization
			assert.NoError(t, json.Unmarshal(raw, &cr))
			want, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&cr)
			assert.NoError(t, err)
			ks, err := parseKustomization(doc)
			assert.NoError(t, err)
			for range 2 {
				got, err := ks.RenderDocument()
				assert.NoError(t, err)
				assert.Diff(t, got["spec"], want["spec"])
			}
		})
	}
}

func TestParseKustomization_InvalidGeneratorFields(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"patches", "images", "components", "buildMetadata"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			_, err := parseKustomization(
				mustYAML(
					t,
					"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: apps}\nspec: {"+field+": not-a-list}\n",
				),
			)
			{
				assert.FatalIf(
					t,
					!errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux),
					"invalid typed field error = %v, want input error",
					err,
				)
			}
			{
				assert.FatalIf(
					t,
					!strings.Contains(err.Error(), "cannot unmarshal string"),
					"missing decoder diagnostic: %v",
					err,
				)
			}
		})
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
	{
		assert.FatalIf(t, err != nil, "parseKustomization: %v", err)
	}
	deps := refsForUse(t, k, "dependsOn")
	{
		assert.FatalIf(t, len(deps) != 2, "DependsOn len = %d, want 2", len(deps))
	}
	{
		got := deps[0].NamespacedName()
		assert.ErrorIf(t, got != "flux-system/infra", "DependsOn[0] = %q, want flux-system/infra", got)
	}
	{
		got := deps[1].NamespacedName()
		assert.ErrorIf(t, got != "other/extras", "DependsOn[1] = %q, want other/extras", got)
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
	{
		assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)
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
		{
			assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": ValuePlaceholder("BASE_DOMAIN"),
			"PLAINTEXT":   "keep-me",
		})
	})
	t.Run("preserved when wipeSecrets=false", func(t *testing.T) {
		t.Parallel()
		cmobj, _, _, err := ParseDoc(mustYAML(t, src), ParseDocOptions{WipeSecrets: false, DefaultNamespace: ""})
		cm := cmobj.(*ConfigMap)
		{
			assert.FatalIf(t, err != nil, "parseConfigMap: %v", err)
		}
		assert.Diff(t, cm.Data, map[string]any{
			"BASE_DOMAIN": ciphertext,
			"PLAINTEXT":   "keep-me",
		})
	})
}
