package manifest

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
)

// defaultParseDocOptions returns the standard ParseDoc options — secrets wiped.
// Test-only helper; production callers pass an explicit ParseDocOptions.
func defaultParseDocOptions() ParseDocOptions {
	return ParseDocOptions{DefaultNamespace: ""}
}

// mustYAML decodes a single YAML document literal into a generic map.
func mustYAML(t *testing.T, doc string) map[string]any {
	t.Helper()
	docs, err := splitDocMaps([]byte(doc))
	assert.FatalIf(t, err != nil, "SplitDocs: %v\n%s", err, doc)
	assert.FatalIf(t, len(docs) != 1, "expected single doc, got %d", len(docs))
	return docs[0]
}

func TestNamedResource(t *testing.T) {
	t.Parallel()
	n := NamedResource{Group: FluxKustomizeDomain, Kind: "Kustomization", Namespace: "flux-system", Name: "apps"}
	{
		got, want := n.NamespacedName(), "flux-system/apps"
		assert.ErrorIf(t, got != want, "NamespacedName = %q, want %q", got, want)
	}
	{
		got, want := n.String(), "Kustomization.kustomize.toolkit.fluxcd.io/flux-system/apps"
		assert.ErrorIf(t, got != want, "String = %q, want %q", got, want)
	}

	cluster := NamedResource{Kind: "ClusterRole", Name: "view"}
	{
		got, want := cluster.NamespacedName(), "view"
		assert.ErrorIf(t, got != want, "cluster-scoped NamespacedName = %q, want %q", got, want)
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
			//nolint:gocritic // Comparing an identity with itself tests reflexivity.
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

func TestProjectionCompatible_ContentBoundary(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                          string
		desiredTarget, previousTarget string
		path, namespace               string
		compatible                    bool
	}{
		{name: "equal target", desiredTarget: "apps", previousTarget: "apps", compatible: true},
		{name: "different target", desiredTarget: "apps", previousTarget: "media"},
		{name: "empty targets", compatible: true},
		{name: "empty desired target", previousTarget: "apps"},
		{name: "empty previous target", desiredTarget: "apps"},
		{name: "changed path", desiredTarget: "apps", previousTarget: "apps", path: "./other", compatible: true},
		{name: "different metadata namespace", desiredTarget: "apps", previousTarget: "apps", namespace: "projected", compatible: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			previous := &Kustomization{
				Name: "app", Namespace: "authored",
				Path: "./app", TargetNamespace: tc.previousTarget,
			}
			desired := previous.Clone()
			desired.TargetNamespace = tc.desiredTarget
			if tc.path != "" {
				desired.Path = tc.path
			}
			if tc.namespace != "" {
				desired.Namespace = tc.namespace
			}
			previousSnapshot, desiredSnapshot := previous.Clone(), desired.Clone()
			assert.Equal(t, desired.ProjectionCompatible(previous), tc.compatible)
			assert.Equal(t, Equal(previous, previousSnapshot), true)
			assert.Equal(t, Equal(desired, desiredSnapshot), true)
		})
	}
	t.Run("changed repository URL", func(t *testing.T) {
		t.Parallel()
		previous := &GitRepository{
			Name: "source", Namespace: "apps",
			URL: "https://fixture.invalid/previous",
		}
		desired := *previous
		desired.GitRepositorySpec = *previous.DeepCopy()
		desired.URL = "https://fixture.invalid/desired"
		previousSnapshot, desiredSnapshot := *previous, desired
		assert.Equal(t, desired.ProjectionCompatible(previous), true)
		assert.Diff(t, *previous, previousSnapshot)
		assert.Diff(t, desired, desiredSnapshot)
	})
}

func TestEqual_Operands(t *testing.T) {
	t.Parallel()
	ks := &Kustomization{Name: "app"}
	for _, tc := range []struct {
		name  string
		a, b  BaseManifest
		equal bool
	}{
		{"nil interfaces", nil, nil, true},
		{"nil and typed nil", nil, (*Kustomization)(nil), false},
		{"matching typed nil", (*Kustomization)(nil), (*Kustomization)(nil), true},
		{"different typed nil", (*Kustomization)(nil), (*GitRepository)(nil), false},
		{"nil and present", (*Kustomization)(nil), ks, false},
		{"same pointer", ks, ks, true},
		{"different concrete types", ks, &ConfigMap{Name: "app"}, false},
		{"equal typed content", &ConfigMap{Name: "app", Data: map[string]any{"key": "value"}}, &ConfigMap{Name: "app", Data: map[string]any{"key": "value"}}, true},
		{"changed typed content", &ConfigMap{Name: "app", Data: map[string]any{"key": "value"}}, &ConfigMap{Name: "app", Data: map[string]any{"key": "other"}}, false},
		{"typed nil and empty", &ConfigMap{Name: "app"}, &ConfigMap{Name: "app", Data: map[string]any{}}, false},
		{"raw nil interface", nil, (*RawObject)(nil), false},
		{"raw matching typed nil", (*RawObject)(nil), (*RawObject)(nil), true},
		{"raw mismatched typed nil", (*RawObject)(nil), (*Kustomization)(nil), false},
		{"raw nil and present", (*RawObject)(nil), &RawObject{}, false},
		{"raw mismatched concrete type", &RawObject{Kind: KindConfigMap, Name: "app"}, &ConfigMap{Name: "app"}, false},
		{"equal raw content", &RawObject{Kind: "Widget", Spec: map[string]any{"items": []any{int64(1)}}}, &RawObject{Kind: "Widget", Spec: map[string]any{"items": []any{int64(1)}}}, true},
		{"raw nil and empty", &RawObject{Kind: "Widget"}, &RawObject{Kind: "Widget", Spec: map[string]any{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, Equal(tc.a, tc.b), tc.equal)
			assert.Equal(t, Equal(tc.b, tc.a), tc.equal)
		})
	}
}

func TestEqual_KustomizationContent(t *testing.T) {
	t.Parallel()
	parse := func(t *testing.T) *Kustomization {
		t.Helper()
		ks, err := parseKustomization(mustYAML(t, `
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: ns}
spec:
  path: ./app
  sourceRef: {kind: GitRepository, name: repo}
  postBuild: {substitute: {KEY: same}}
`))
		assert.NoError(t, err)
		return ks
	}
	for _, tc := range []struct {
		name     string
		populate bool
		change   func(*Kustomization)
		equal    bool
	}{
		{name: "cold cache", equal: true},
		{name: "populated cache", equal: true, populate: true},
		{name: "absent cache", equal: true, change: func(k *Kustomization) { k.projection = nil }},
		{name: "name", change: func(k *Kustomization) { k.Name = "other" }},
		{name: "namespace", change: func(k *Kustomization) { k.Namespace = "other" }},
		{name: "spec", change: func(k *Kustomization) { k.SourceRef.Name = "other" }},
		{name: "nested spec", change: func(k *Kustomization) { k.PostBuild.Substitute["KEY"] = "other" }},
		{name: "labels", change: func(k *Kustomization) { k.Labels = map[string]string{"app": "other"} }},
		{name: "annotations", change: func(k *Kustomization) { k.Annotations = map[string]string{"note": "other"} }},
		{name: "nil and empty labels", change: func(k *Kustomization) { k.Labels = map[string]string{} }},
		{name: "nil and empty annotations", change: func(k *Kustomization) { k.Annotations = map[string]string{} }},
		{name: "nil and empty spec slice", change: func(k *Kustomization) { k.Components = []string{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first, second := parse(t), parse(t)
			if tc.populate {
				_, err := second.RenderDocument()
				assert.NoError(t, err)
			}
			if tc.change != nil {
				tc.change(second)
			}
			assert.Equal(t, Equal(first, second), tc.equal)
			assert.Equal(t, Equal(second, first), tc.equal)
		})
	}
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
				doc := mustYAML(
					t,
					fmt.Sprintf(
						"apiVersion: %s\nkind: %s\nmetadata: {name: test, namespace: ns}\nspec: %s\n",
						tc.apiVersion,
						tc.kind,
						tc.spec,
					),
				)
				doc["spec"].(map[string]any)["suspend"] = suspend
				if tc.kind == KindResourceSet && suspend {
					doc["metadata"].(map[string]any)["annotations"] = map[string]any{
						FluxOperatorDomain + "/reconcile": "disabled",
					}
				}
				parsed, err := ParseDoc(doc, defaultParseDocOptions())
				obj := parsed.Object
				assert.FatalIf(t, err != nil, "parse: %v", err)
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

func TestParseHelmRelease_CRDsPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, spec       string
		install, upgrade helmv2.CRDsPolicy
	}{
		{"install only", "  install: {crds: Create}", helmv2.Create, ""},
		{"empty upgrade", "  install: {crds: Skip}\n  upgrade: {}", helmv2.Skip, ""},
		{"opposite", "  install: {crds: Create}\n  upgrade: {crds: Skip}", helmv2.Create, helmv2.Skip},
		{"upgrade only", "  upgrade: {crds: CreateReplace}", "", helmv2.CreateReplace},
		{"neither", "", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr, err := parseHelmRelease(helmReleaseDoc(t, tc.spec))
			assert.NoError(t, err)
			release := helmv2.HelmRelease{Spec: hr.HelmReleaseSpec}
			assert.Equal(t, release.GetInstall().CRDs, tc.install)
			assert.Equal(t, release.GetUpgrade().CRDs, tc.upgrade)
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
			hc, err := parseHelmChartSource(
				mustYAML(
					t,
					"apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmChart\nmetadata: {name: c, namespace: '"+tc.namespace+"'}\nspec: {chart: nginx, reconcileStrategy: '"+tc.strategy+"', sourceRef: "+tc.source+"}\n",
				),
			)
			assert.NoError(t, err)
			assert.Equal(t, hc.Namespace, tc.namespace)
			assert.Equal(t, hc.SourceRef.Kind, KindHelmRepository)
			assert.Equal(t, hc.ReconcileStrategy, tc.strategy)
		})
	}
}

func TestParseDoc_PreservesEnvsubstFields(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, value string }{
		{"equals default", "${ENV:=prod}"},
		{"dash default", "${ENV:-prod}"},
		{"empty equals default", "${ENV:=}"},
		{"empty dash default", "${ENV:-}"},
		{"nested default", "${ENV:=${OTHER:=prod}}"},
		{"bare", "${ENV}"},
		{"escaped", "$${ENV:=prod}"},
		{"literal", "prod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testParseDocPreservesEnvsubstFieldsCase(t, &tc)
		},
		)
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
	assert.FatalIf(t, err == nil, "%s", "expected parse error for empty sourceRef.name with kind set")
	assert.ErrorIf(
		t,
		!strings.Contains(err.Error(), "sourceRef.name is empty"),
		"error should mention sourceRef.name being empty; got %v",
		err,
	)
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
	assert.FatalIf(t, err != nil, "parseBucket: %v", err)
	assert.ErrorIf(t, b.Provider != "generic",
		"default Provider should be 'generic', got %q", b.Provider)
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
				_, err := ParseDoc(d, ParseDocOptions{DefaultNamespace: ""})
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
			assert.FatalIf(t, err == nil, "expected error, got nil")
			assert.ErrorIf(t, !strings.Contains(err.Error(), tc.want),
				"expected error containing %q, got %q", tc.want, err.Error())
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
	assert.FatalIf(t, err != nil, "parseResourceSet: %v", err)
	assert.ErrorIf(t, rs.Name != "apps" || rs.Namespace != "flux-system",
		"unexpected name/ns: %s/%s", rs.Namespace, rs.Name)
	assert.ErrorIf(t, len(rs.Inputs) != 2,
		"expected 2 inputs, got %d", len(rs.Inputs))
	assert.ErrorIf(t, len(rs.Resources) != 1,
		"expected 1 templated resource, got %d", len(rs.Resources))

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
			testParseResourceSetCase(t, &testParseResourceSetCaseInputs{doc: &doc, tc: &tc})
		},
		)
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
	assert.FatalIf(t, err != nil, "parseResourceSet: %v", err)
	assert.ErrorIf(t, rs.Namespace != "",
		"namespace=%q want empty before loader inheritance/defaulting", rs.Namespace)
}

func testParseDocPreservesEnvsubstFieldsCase(
	t *testing.T,
	testCaseInput *struct {
		name  string
		value string
	},
) {
	t.Helper()

	for _, kind := range []string{KindKustomization, KindGitRepository, KindOCIRepository, KindHelmRelease} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			value := testCaseInput.value
			var doc map[string]any
			switch kind {
			case KindKustomization:
				doc = mustYAML(t, fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: apps}
spec:
  path: '%[1]s'
  sourceRef: {kind: GitRepository, name: '%[1]s', namespace: '%[1]s'}
  dependsOn: [{name: sibling, namespace: '%[1]s'}]
  postBuild: {substituteFrom: [{kind: ConfigMap, name: optional, optional: true}]}
`, value))
			case KindGitRepository:
				doc = mustYAML(t, fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: child, namespace: apps}
spec:
  url: '%[1]s'
  ref: {branch: '%[1]s', tag: '%[1]s', semver: '%[1]s', commit: '%[1]s', name: '%[1]s'}
`, value))
			case KindOCIRepository:
				doc = mustYAML(t, fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata: {name: child, namespace: apps}
spec:
  url: '%[1]s'
  ref: {tag: '%[1]s', semver: '%[1]s', digest: '%[1]s'}
`, value))
			case KindHelmRelease:
				doc = helmReleaseDoc(
					t,
					fmt.Sprintf(
						"  valuesFrom: [{kind: ConfigMap, name: '%s', valuesKey: '%s', optional: true}]",
						value,
						value,
					),
				)
			}
			before := DeepCopyMap(doc)
			parsed3, err := ParseDoc(doc, ParseDocOptions{})
			obj, authoritative, changed := parsed3.Object, parsed3.Document, parsed3.Changed
			assert.NoError(t, err)
			assert.Equal(t, changed, false)
			assert.Diff(t, doc, before)
			assert.Diff(t, authoritative, before)
			var fields map[string]string
			switch obj := obj.(type) {
			case *Kustomization:
				fields = map[string]string{
					"path":                obj.Path,
					"sourceRef.name":      obj.SourceRef.Name,
					"sourceRef.namespace": obj.SourceRef.Namespace,
					"dependsOn.namespace": obj.DependsOn[0].Namespace,
				}
				assert.Equal(t, obj.PostBuild.SubstituteFrom[0].Optional, true)
			case *GitRepository:
				ref := obj.Reference
				fields = map[string]string{
					"url":        obj.URL,
					"ref.branch": ref.Branch,
					"ref.tag":    ref.Tag,
					"ref.semver": ref.SemVer,
					"ref.commit": ref.Commit,
					"ref.name":   ref.Name,
				}
			case *OCIRepository:
				ref := obj.Reference
				fields = map[string]string{
					"url":        obj.URL,
					"ref.tag":    ref.Tag,
					"ref.semver": ref.SemVer,
					"ref.digest": ref.Digest,
				}
			case *HelmRelease:
				fields = map[string]string{
					"valuesFrom.name":      obj.ValuesFrom[0].Name,
					"valuesFrom.valuesKey": obj.ValuesFrom[0].ValuesKey,
				}
				assert.Equal(t, obj.ValuesFrom[0].Optional, true)
			default:
				t.Fatalf("unexpected child %T", obj)
			}
			for field, got := range fields {
				assert.ErrorIf(t, got != value, "%s = %q, want %q", field, got, value)
			}
		})
	}
}

type testParseResourceSetCaseInputs struct {
	doc *map[string]any
	tc  *struct {
		name  string
		field string
		ref   map[string]any
		valid bool
	}
}

func testParseResourceSetCase(t *testing.T, testCaseInput *testParseResourceSetCaseInputs) {
	t.Helper()

	input := DeepCopyMap((*testCaseInput.doc))
	testCaseInput.tc.ref["kind"], testCaseInput.tc.ref["name"] = KindResourceSetInputProvider, "inputs"
	input["spec"].(map[string]any)[testCaseInput.tc.field] = []any{testCaseInput.tc.ref}
	before := DeepCopyMap(input)
	_, err := ParseDoc(input, defaultParseDocOptions())
	if testCaseInput.tc.valid {
		assert.NoError(t, err)
	} else {
		assert.FatalIf(
			t,
			!errors.Is(err, ErrInput) || !errors.Is(err, ErrFlux) ||
				!strings.Contains(err.Error(), "spec."+testCaseInput.tc.field+"[0].apiVersion"),
			"invalid reference error = %v",
			err,
		)
	}
	assert.Diff(t, input, before)
}
