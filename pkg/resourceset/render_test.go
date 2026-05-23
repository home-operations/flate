package resourceset_test

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
)

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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
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
	docs, err := resourceset.Render(rs)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 4 {
		t.Errorf("expected 4 docs (2 inputs × 2 docs each), got %d", len(docs))
	}
}
