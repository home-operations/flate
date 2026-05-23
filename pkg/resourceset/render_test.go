package resourceset_test

import (
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
)

// TestRender_PermuteIsRejected locks the upstream-fidelity guard: a
// ResourceSet requesting spec.inputStrategy.name=Permute fails loud
// rather than silently falling through to Flatten. Flatten produces
// wrong cardinality for a Permute consumer — flate would render
// differently from what flux-operator emits in cluster.
func TestRender_PermuteIsRejected(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			InputStrategy: &fluxopv1.InputStrategySpec{
				Name: fluxopv1.InputStrategyPermute,
			},
			Inputs: []fluxopv1.ResourceSetInput{
				{"x": jsonTmpl(t, `"a"`)},
				{"x": jsonTmpl(t, `"b"`)},
			},
			ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: << .input.x >>`,
		},
	}
	_, err := resourceset.Render(rs, nil)
	if err == nil {
		t.Fatal("expected Permute to be rejected; got nil error")
	}
	if !strings.Contains(err.Error(), "Permute") {
		t.Errorf("error should mention Permute; got %v", err)
	}
}

func jsonTmpl(t *testing.T, raw string) *apix.JSON {
	t.Helper()
	return &apix.JSON{Raw: []byte(raw)}
}

// TestRender_InputsExpandTemplates locks the core ResourceSet semantics:
// one template + N inputs → N rendered objects, each substituting
// inputs.X with the per-input value.
func TestRender_InputsExpandTemplates(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"tenant": jsonTmpl(t, `"frontend"`)},
				{"tenant": jsonTmpl(t, `"backend"`)},
			},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1",
					"kind": "ConfigMap",
					"metadata": {"name": "<< inputs.tenant >>-cm", "namespace": "<< inputs.tenant >>"}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(docs))
	}
	names := map[string]string{}
	for _, doc := range docs {
		md := doc["metadata"].(map[string]any)
		names[md["name"].(string)] = md["namespace"].(string)
	}
	if names["frontend-cm"] != "frontend" || names["backend-cm"] != "backend" {
		t.Errorf("inputs not substituted: %v", names)
	}
}

// TestRender_Deduplication asserts that shared resources (e.g. a single
// OCIRepository referenced by all tenants) emit exactly once even when
// templated inside a per-input matrix.
func TestRender_Deduplication(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"tenant": jsonTmpl(t, `"a"`)},
				{"tenant": jsonTmpl(t, `"b"`)},
			},
			Resources: []*apix.JSON{
				// Shared — same name regardless of input.
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "shared", "namespace": "flux-system"}
				}`),
				// Per-tenant.
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.tenant >>", "namespace": "flux-system"}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 3 {
		t.Errorf("expected 3 unique docs (1 shared + 2 per-tenant), got %d", len(docs))
	}
}

// TestRender_NoInputsRendersOnce covers d2-fleet's policies.yaml shape:
// spec.inputs absent, just a fixed set of resources. The renderer must
// still emit them (with a nil input set).
func TestRender_NoInputsRendersOnce(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "policies", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "flux-allowlist", "namespace": "flux-system"}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
}

// TestRender_DefaultsNamespace asserts that namespaced resources
// without an explicit metadata.namespace inherit the ResourceSet's
// own namespace, while cluster-scoped kinds (Namespace, ClusterRole,
// CRD, etc.) stay namespace-less.
func TestRender_DefaultsNamespace(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "tenant-x",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"a"`)}},
			Resources: []*apix.JSON{
				// Namespaced — should default to tenant-x.
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.name >>"}
				}`),
				// Cluster-scoped — must stay namespace-less.
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "Namespace",
					"metadata": {"name": "<< inputs.name >>"}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	for _, doc := range docs {
		kind := doc["kind"].(string)
		md := doc["metadata"].(map[string]any)
		ns, _ := md["namespace"].(string)
		switch kind {
		case "ConfigMap":
			if ns != "tenant-x" {
				t.Errorf("ConfigMap namespace=%q want tenant-x", ns)
			}
		case "Namespace":
			if ns != "" {
				t.Errorf("Namespace got injected namespace=%q (cluster-scoped)", ns)
			}
		}
	}
}

// TestRender_CommonMetadata stamps labels + annotations on every
// emitted object.
func TestRender_CommonMetadata(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			CommonMetadata: &fluxopv1.CommonMetadata{
				Labels:      map[string]string{"team": "platform"},
				Annotations: map[string]string{"owner": "x"},
			},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "x", "namespace": "flux-system"}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	md := docs[0]["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	if labels["team"] != "platform" {
		t.Errorf("commonMetadata.labels not merged: %v", labels)
	}
	ann, _ := md["annotations"].(map[string]any)
	if ann["owner"] != "x" {
		t.Errorf("commonMetadata.annotations not merged: %v", ann)
	}
}

// TestRender_SprigFunctions exercises a few stdlib + slugify funcs to
// confirm the template engine plumbs them through.
func TestRender_SprigFunctions(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"tenant": jsonTmpl(t, `"Team One"`)},
			},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.tenant | slugify >>", "namespace": "flux-system"},
					"data": {"upper": "<< inputs.tenant | upper >>"}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	md := docs[0]["metadata"].(map[string]any)
	if md["name"] != "team-one" {
		t.Errorf("slugify failed: %v", md["name"])
	}
	data := docs[0]["data"].(map[string]any)
	if data["upper"] != "TEAM ONE" {
		t.Errorf("sprig upper failed: %v", data["upper"])
	}
}

// TestRender_DisabledReconcileAnnotationSkips covers the conditional-
// exclusion pattern documented for ResourceSet: a resource with
// `fluxcd.controlplane.io/reconcile: disabled` is dropped.
func TestRender_DisabledReconcileAnnotationSkips(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"tenant": jsonTmpl(t, `"a"`)},
				{"tenant": jsonTmpl(t, `"b"`)},
			},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {
						"name": "<< inputs.tenant >>", "namespace": "flux-system",
						"annotations": {
							"fluxcd.controlplane.io/reconcile": "<< if eq inputs.tenant \"a\" >>enabled<< else >>disabled<< end >>"
						}
					}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc (disabled filtered), got %d", len(docs))
	}
	md := docs[0]["metadata"].(map[string]any)
	if md["name"] != "a" {
		t.Errorf("wrong tenant kept: %v", md["name"])
	}
}

// TestRender_InputsProviderBuiltinField asserts every rendered input
// carries the built-in inputs.provider block per the upstream
// flux-operator contract. Templates rely on inputs.provider.kind to
// distinguish ResourceSet inline inputs from ResourceSetInputProvider
// inputs once that's supported.
func TestRender_InputsProviderBuiltinField(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{{"tenant": jsonTmpl(t, `"a"`)}},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "x", "namespace": "flux-system"},
					"data": {
						"providerKind":      "<< inputs.provider.kind >>",
						"providerName":      "<< inputs.provider.name >>",
						"providerNamespace": "<< inputs.provider.namespace >>"
					}
				}`),
			},
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	data := docs[0]["data"].(map[string]any)
	if data["providerKind"] != "ResourceSet" {
		t.Errorf("inputs.provider.kind=%v want ResourceSet", data["providerKind"])
	}
	if data["providerName"] != "apps" {
		t.Errorf("inputs.provider.name=%v want apps", data["providerName"])
	}
	if data["providerNamespace"] != "flux-system" {
		t.Errorf("inputs.provider.namespace=%v want flux-system", data["providerNamespace"])
	}
}

// TestRender_MissingKeyErrors locks the upstream Option("missingkey=error")
// behavior — a template referencing an undefined input must fail with a
// useful error rather than silently rendering "<no value>". Templates
// that work in flate must also work in real flux-operator and vice versa.
func TestRender_MissingKeyErrors(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{{"tenant": jsonTmpl(t, `"a"`)}},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.nonexistent >>", "namespace": "flux-system"}
				}`),
			},
		},
	}
	_, err := resourceset.Render(rs, nil)
	if err == nil {
		t.Fatal("expected error for undefined input key, got nil")
	}
}

// TestRender_MalformedTemplateErrors surfaces a parse error rather than
// silently swallowing the broken template.
func TestRender_MalformedTemplateErrors(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"a"`)}},
			Resources: []*apix.JSON{
				jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.name "}
				}`), // unterminated template
			},
		},
	}
	_, err := resourceset.Render(rs, nil)
	if err == nil {
		t.Fatal("expected parse error for malformed template, got nil")
	}
}

// TestRender_ToYamlNindent covers the canonical upstream pattern
// `<< value | toYaml | nindent N >>` for embedding nested structs as
// child YAML. Pins both that toYaml is registered as the silent variant
// (no error wrapping needed) and that the resulting indentation lines
// up with the surrounding YAML.
func TestRender_ToYamlNindent(t *testing.T) {
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"layerSelector": jsonTmpl(t, `{"mediaType": "x", "operation": "copy"}`)},
			},
			ResourcesTemplate: `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: app
  namespace: flux-system
spec:
  layerSelector: << inputs.layerSelector | toYaml | nindent 4 >>
`,
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	spec := docs[0]["spec"].(map[string]any)
	sel := spec["layerSelector"].(map[string]any)
	if sel["mediaType"] != "x" || sel["operation"] != "copy" {
		t.Errorf("toYaml|nindent did not produce nested map: %v", sel)
	}
}

// TestRender_InputsFrom_StaticProvider locks the billimek volsync
// pattern: a ResourceSet with no inline inputs that references a Static
// ResourceSetInputProvider, whose defaultValues become a single input
// set the template iterates over with `<<- range $app := inputs.apps >>`.
func TestRender_InputsFrom_StaticProvider(t *testing.T) {
	rsip := &manifest.ResourceSetInputProvider{
		Name: "apps", Namespace: "kube-system",
		ResourceSetInputProviderSpec: fluxopv1.ResourceSetInputProviderSpec{
			Type: fluxopv1.InputProviderStatic,
			DefaultValues: fluxopv1.ResourceSetInput{
				"defaults": jsonTmpl(t, `{"capacity": "1Gi"}`),
				"apps":     jsonTmpl(t, `[{"app": "alpha"}, {"app": "bravo", "capacity": "5Gi"}]`),
			},
		},
	}
	rs := &manifest.ResourceSet{
		Name: "volsync", Namespace: "kube-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			InputsFrom: []fluxopv1.InputProviderReference{
				{Name: "apps"},
			},
			ResourcesTemplate: `<<- range $app := inputs.apps >>
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: << $app.app >>
  namespace: default
data:
  capacity: << get $app "capacity" | default inputs.defaults.capacity >>
<<- end >>
`,
		},
	}
	resolver := func(ref fluxopv1.InputProviderReference, ns string) ([]*manifest.ResourceSetInputProvider, error) {
		if ref.Name == "apps" && ns == "kube-system" {
			return []*manifest.ResourceSetInputProvider{rsip}, nil
		}
		return nil, nil
	}
	docs, err := resourceset.Render(rs, resolver)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 ConfigMaps, got %d", len(docs))
	}
	caps := map[string]string{}
	for _, doc := range docs {
		md := doc["metadata"].(map[string]any)
		data := doc["data"].(map[string]any)
		caps[md["name"].(string)] = data["capacity"].(string)
	}
	// alpha falls back to inputs.defaults.capacity (1Gi); bravo
	// overrides via its own per-app capacity (5Gi).
	if caps["alpha"] != "1Gi" || caps["bravo"] != "5Gi" {
		t.Errorf("expected alpha=1Gi, bravo=5Gi; got %v", caps)
	}
}

// TestRender_InputsFrom_DynamicProviderEmptySkip verifies that a
// non-Static provider (which flate can't query offline) contributes
// zero input sets rather than erroring — the ResourceSet still renders
// with whatever inline inputs it has.
func TestRender_InputsFrom_DynamicProviderEmptySkip(t *testing.T) {
	rsip := &manifest.ResourceSetInputProvider{
		Name: "branches", Namespace: "flux-system",
		ResourceSetInputProviderSpec: fluxopv1.ResourceSetInputProviderSpec{
			Type: fluxopv1.InputProviderGitHubBranch,
			URL:  "https://github.com/foo/bar",
		},
	}
	rs := &manifest.ResourceSet{
		Name: "matrix", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"tenant": jsonTmpl(t, `"inline-only"`)},
			},
			InputsFrom: []fluxopv1.InputProviderReference{
				{Name: "branches"},
			},
			ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: << inputs.tenant >>
  namespace: flux-system
`,
		},
	}
	resolver := func(_ fluxopv1.InputProviderReference, _ string) ([]*manifest.ResourceSetInputProvider, error) {
		return []*manifest.ResourceSetInputProvider{rsip}, nil
	}
	docs, err := resourceset.Render(rs, resolver)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc from inline input (dynamic provider contributes nothing), got %d", len(docs))
	}
	md := docs[0]["metadata"].(map[string]any)
	if md["name"] != "inline-only" {
		t.Errorf("expected name=inline-only; got %v", md["name"])
	}
}

// TestRender_ResourcesTemplate covers spec.resourcesTemplate (multi-doc
// YAML string variant).
func TestRender_ResourcesTemplate(t *testing.T) {
	tmpl := `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: << inputs.name >>
  namespace: flux-system
---
apiVersion: v1
kind: Namespace
metadata:
  name: << inputs.name >>
`
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			Inputs: []fluxopv1.ResourceSetInput{
				{"name": jsonTmpl(t, `"a"`)},
				{"name": jsonTmpl(t, `"b"`)},
			},
			ResourcesTemplate: tmpl,
		},
	}
	docs, err := resourceset.Render(rs, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 4 {
		t.Errorf("expected 4 docs (2 inputs × 2 docs each), got %d", len(docs))
	}
}
