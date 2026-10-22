package resourceset_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
)

// TestRender_PermuteScopesByProviderName locks the core Permute
// semantic: each provider's input set is nested under its normalized
// name, so templates dereference `inputs.<provider>.foo` instead of
// the flat `inputs.foo`. Matches flux-operator/internal/inputs/
// permuter.go. With one RSET-inline input and one Static RSIP, the
// Cartesian product is 1×1=1 — the cardinality match is the same as
// Flatten, but the SCOPING differs, which is the observable change.
func TestRender_PermuteScopesByProviderName(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "myrset", Namespace: "default",
		InputStrategy: &fluxopv1.InputStrategySpec{Name: fluxopv1.InputStrategyPermute},
		Inputs:        []fluxopv1.ResourceSetInput{{"env": jsonTmpl(t, `"prod"`)}},
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind: manifest.KindResourceSetInputProvider, Name: "myrsip",
		}},
		// Permute templates dereference inputs by provider name.
		// `myrset` (the rs.Name) wraps the inline input;
		// `myrsip` wraps the RSIP's defaultValues.
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: << index inputs "myrset" "env" >>-<< index inputs "myrsip" "tenant" >>
  namespace: default`,
	}
	rsip := &manifest.ResourceSetInputProvider{
		Name: "myrsip", Namespace: "default",
		Type:          fluxopv1.InputProviderStatic,
		DefaultValues: fluxopv1.ResourceSetInput{"tenant": jsonTmpl(t, `"alpha"`)},
	}
	resolver := func(ref fluxopv1.InputProviderReference, _ manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		if ref.Name == "myrsip" {
			return []*manifest.ResourceSetInputProvider{rsip}, nil
		}
		return nil, nil
	}

	docs, _, err := resourceset.Render(rs, resolver, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc (1 inline × 1 static = 1), got %d:\n%+v", len(docs), docs)
	}
	md, _ := docs[0]["metadata"].(map[string]any)
	if got, _ := md["name"].(string); got != "prod-alpha" {
		t.Errorf("expected name=prod-alpha, got %q", got)
	}
}

// TestRender_PermuteMultiInlineCartesian covers the Cartesian
// expansion across provider rows: 2 RSET-inline inputs × 1 Static
// RSIP = 2 permutations, each combining the inline value with the
// shared RSIP value.
func TestRender_PermuteMultiInlineCartesian(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "rset", Namespace: "default",
		InputStrategy: &fluxopv1.InputStrategySpec{Name: fluxopv1.InputStrategyPermute},
		Inputs: []fluxopv1.ResourceSetInput{
			{"env": jsonTmpl(t, `"prod"`)},
			{"env": jsonTmpl(t, `"stage"`)},
		},
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind: manifest.KindResourceSetInputProvider, Name: "rsip",
		}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: << index inputs "rset" "env" >>-<< index inputs "rsip" "tenant" >>
  namespace: default`,
	}
	resolver := func(_ fluxopv1.InputProviderReference, _ manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		return []*manifest.ResourceSetInputProvider{{
			Name: "rsip", Namespace: "default",
			Type:          fluxopv1.InputProviderStatic,
			DefaultValues: fluxopv1.ResourceSetInput{"tenant": jsonTmpl(t, `"alpha"`)},
		}}, nil
	}
	docs, _, err := resourceset.Render(rs, resolver, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("expected 2 perm docs (2 inline × 1 rsip), got %d:\n%+v", len(docs), docs)
	}
	gotNames := map[string]struct{}{}
	for _, d := range docs {
		md, _ := d["metadata"].(map[string]any)
		name, _ := md["name"].(string)
		gotNames[name] = struct{}{}
	}
	assert.Diff(t, gotNames, map[string]struct{}{"prod-alpha": {}, "stage-alpha": {}})
}

// TestRender_PermuteSkipsEmptyProviderByDefault matches upstream:
// when a provider exports zero input sets, the default (includeEmpty=
// false) silently drops it from the product. Only the non-empty
// provider's inputs survive — and their scoping is preserved.
func TestRender_PermuteSkipsEmptyProviderByDefault(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "rset", Namespace: "default",
		InputStrategy: &fluxopv1.InputStrategySpec{Name: fluxopv1.InputStrategyPermute},
		Inputs:        []fluxopv1.ResourceSetInput{{"env": jsonTmpl(t, `"prod"`)}},
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind: manifest.KindResourceSetInputProvider, Name: "dyn",
		}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata:
  name: << index inputs "rset" "env" >>
  namespace: default`,
	}
	resolver := func(_ fluxopv1.InputProviderReference, _ manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		// Dynamic provider (GitHubBranch etc.) — flate can't fetch
		// remote APIs offline, so it exports zero input sets.
		return []*manifest.ResourceSetInputProvider{{
			Name: "dyn", Namespace: "default",
			Type: fluxopv1.InputProviderGitHubBranch,
		}}, nil
	}
	docs, _, err := resourceset.Render(rs, resolver, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Errorf("expected 1 doc (dyn provider dropped, 1 inline remains); got %d:\n%+v", len(docs), docs)
	}
}

// TestRender_PermuteIncludeEmptyProvidersCollapses verifies the
// IncludeEmptyProviders=true branch: the empty provider participates
// in the Cartesian product and collapses it to zero results, matching
// upstream's "permutes to empty when includeEmptyProviders is true"
// test in combine_test.go.
func TestRender_PermuteIncludeEmptyProvidersCollapses(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "rset", Namespace: "default",
		InputStrategy: &fluxopv1.InputStrategySpec{
			Name:                  fluxopv1.InputStrategyPermute,
			IncludeEmptyProviders: true,
		},
		Inputs: []fluxopv1.ResourceSetInput{{"env": jsonTmpl(t, `"prod"`)}},
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind: manifest.KindResourceSetInputProvider, Name: "dyn",
		}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: x, namespace: default}`,
	}
	resolver := func(_ fluxopv1.InputProviderReference, _ manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		return []*manifest.ResourceSetInputProvider{{
			Name: "dyn", Namespace: "default",
			Type: fluxopv1.InputProviderGitHubBranch,
		}}, nil
	}
	docs, _, err := resourceset.Render(rs, resolver, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 0 {
		t.Errorf("expected 0 docs (Cartesian product with an empty provider), got %d:\n%+v", len(docs), docs)
	}
}

// TestRender_PermuteMaxPermutationsRejected guards the 10000-cap that
// matches upstream's permuter.go. flate must fail loud rather than
// burn host RAM on a pathological combination set. With 10001 inline
// inputs, even the single-RSET-provider case crosses the threshold.
func TestRender_PermuteMaxPermutationsRejected(t *testing.T) {
	t.Parallel()
	inputs := make([]fluxopv1.ResourceSetInput, 10001)
	for i := range inputs {
		inputs[i] = fluxopv1.ResourceSetInput{"i": jsonTmpl(t, `"x"`)}
	}
	rs := &manifest.ResourceSet{
		Name: "rset", Namespace: "default",
		InputStrategy: &fluxopv1.InputStrategySpec{Name: fluxopv1.InputStrategyPermute},
		Inputs:        inputs,
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: x, namespace: default}`,
	}
	_, _, err := resourceset.Render(rs, nil, nil)
	if err == nil {
		t.Fatal("expected Render to reject the >10000-permutation case")
	}
	if !strings.Contains(err.Error(), "permutations") {
		t.Errorf("error should mention permutations; got %v", err)
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
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
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
	assert.Diff(t, names, map[string]string{"frontend-cm": "frontend", "backend-cm": "backend"})
}

// TestRender_Deduplication asserts that shared resources (e.g. a single
// OCIRepository referenced by all tenants) emit exactly once even when
// templated inside a per-input matrix.
func TestRender_Deduplication(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
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
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "policies", Namespace: "flux-system",
		Resources: []*apix.JSON{
			jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "flux-allowlist", "namespace": "flux-system"}
				}`),
		},
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
}

func TestRender_PreservesNamespaces(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "tenant-x",
		Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"a"`)}},
		Resources: []*apix.JSON{
			jsonTmpl(t, `{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "<< inputs.name >>"}}`),
			jsonTmpl(t, `{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "<< inputs.name >>"}}`),
			jsonTmpl(t, `{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": {"name": "<< inputs.name >>"}}`),
			jsonTmpl(t, `{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "<< inputs.name >>", "namespace": "authored"}}`),
			jsonTmpl(t, `{"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "explicit", "namespace": "authored"}}`),
		},
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 5 {
		t.Fatalf("documents = %d", len(docs))
	}
	for i, want := range []string{"", "", "", "authored", "authored"} {
		_, ns := manifest.DocMetadata(docs[i])
		if ns != want {
			t.Errorf("document %d namespace = %q, want %q", i, ns, want)
		}
	}
}

// TestRender_CommonMetadata stamps labels + annotations on every
// emitted object.
func TestRender_CommonMetadata(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
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

func TestRender_OwnerLabels(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "tenant-a",
		Resources: []*apix.JSON{
			jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "x"}
				}`),
		},
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	md := docs[0]["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	if labels[fluxopv1.OwnerLabelResourceSetName] != "apps" {
		t.Errorf("owner name label missing: %v", labels)
	}
	if labels[fluxopv1.OwnerLabelResourceSetNamespace] != "tenant-a" {
		t.Errorf("owner namespace label missing: %v", labels)
	}
}

// TestRender_SprigFunctions exercises a few stdlib + slugify funcs to
// confirm the template engine plumbs them through.
func TestRender_SprigFunctions(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
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
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
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
	rs.Resources = []*apix.JSON{
		jsonTmpl(t, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"same","annotations":{"fluxcd.controlplane.io/reconcile":"disabled"}}}`),
		jsonTmpl(t, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"same"},"data":{"value":"enabled"}}`),
	}
	docs, _, err = resourceset.Render(rs, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Diff(t, docs[0]["data"], map[string]any{"value": "enabled"})
	for _, value := range []string{"Disabled", "false", "enabled"} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			candidate := &manifest.ResourceSet{Name: "exact", ResourcesTemplate: fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n name: exact\n annotations: {fluxcd.controlplane.io/reconcile: %q}", value)}
			docs, _, err := resourceset.Render(candidate, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, len(docs), 1)
		})
	}
}

// TestRender_InputsProviderBuiltinField asserts every rendered input
// carries the built-in inputs.provider block per the upstream
// flux-operator contract. Templates rely on inputs.provider.kind to
// distinguish ResourceSet inline inputs from ResourceSetInputProvider
// inputs once that's supported.
func TestRender_InputsProviderBuiltinField(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
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
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		Inputs: []fluxopv1.ResourceSetInput{{"tenant": jsonTmpl(t, `"a"`)}},
		Resources: []*apix.JSON{
			jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.nonexistent >>", "namespace": "flux-system"}
				}`),
		},
	}
	_, _, err := resourceset.Render(rs, nil, nil)
	if err == nil {
		t.Fatal("expected error for undefined input key, got nil")
	}
}

func TestRender_UsesResourceSetInputProviderStatusExportedInputs(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "flux-system",
		InputsFrom: []fluxopv1.InputProviderReference{{
			Kind: manifest.KindResourceSetInputProvider, Name: "branches",
		}},
		Resources: []*apix.JSON{
			jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.name >>", "namespace": "flux-system"},
					"data": {"id": "<< inputs.id >>"}
				}`),
		},
	}
	rsip := &manifest.ResourceSetInputProvider{
		Name: "branches", Namespace: "flux-system",
		Type: fluxopv1.InputProviderGitHubBranch,
		Status: fluxopv1.ResourceSetInputProviderStatus{
			ExportedInputs: []fluxopv1.ResourceSetInput{{
				"name": jsonTmpl(t, `"feature-a"`),
				"id":   jsonTmpl(t, `"status-id"`),
			}},
		},
	}

	docs, _, err := resourceset.Render(rs, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		return []*manifest.ResourceSetInputProvider{rsip}, nil
	}, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected one status-exported doc, got %d", len(docs))
	}
	if docs[0]["metadata"].(map[string]any)["name"] != "feature-a" {
		t.Fatalf("status exported name not used: %+v", docs[0])
	}
	if docs[0]["data"].(map[string]any)["id"] != "status-id" {
		t.Fatalf("status exported id should be preserved: %+v", docs[0])
	}
}

func TestResourceSetInputProvider_StatusExportedInputsOverrideDerivedID(t *testing.T) {
	t.Parallel()
	rsip := &manifest.ResourceSetInputProvider{
		Name: "static", Namespace: "flux-system",
		Type:          fluxopv1.InputProviderStatic,
		DefaultValues: fluxopv1.ResourceSetInput{"id": jsonTmpl(t, `"default-id"`)},
		Status: fluxopv1.ResourceSetInputProviderStatus{
			ExportedInputs: []fluxopv1.ResourceSetInput{{
				"id": jsonTmpl(t, `"exported-id"`),
			}},
		},
	}

	inputs, err := rsip.ExportedInputs()
	if err != nil {
		t.Fatalf("ExportedInputs: %v", err)
	}
	if len(inputs) != 1 || inputs[0]["id"] != "exported-id" {
		t.Fatalf("status exported id should win, got %+v", inputs)
	}
}

// TestRender_MalformedTemplateErrors surfaces a parse error rather than
// silently swallowing the broken template.
func TestRender_MalformedTemplateErrors(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
		Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"a"`)}},
		Resources: []*apix.JSON{
			jsonTmpl(t, `{
					"apiVersion": "v1", "kind": "ConfigMap",
					"metadata": {"name": "<< inputs.name "}
				}`), // unterminated template
		},
	}
	_, _, err := resourceset.Render(rs, nil, nil)
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
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "test", Namespace: "flux-system",
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
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	spec := docs[0]["spec"].(map[string]any)
	sel := spec["layerSelector"].(map[string]any)
	if sel["mediaType"] != "x" || sel["operation"] != "copy" {
		t.Errorf("toYaml|nindent did not produce nested map: %v", sel)
	}
}

func TestRender_InputsFrom_StaticProvider(t *testing.T) {
	t.Parallel()
	rsip := &manifest.ResourceSetInputProvider{
		Name: "apps", Namespace: "kube-system", Labels: map[string]string{"set": "apps"},
		Type: fluxopv1.InputProviderStatic,
		DefaultValues: fluxopv1.ResourceSetInput{
			"defaults": jsonTmpl(t, `{"capacity": "1Gi"}`),
			"apps":     jsonTmpl(t, `[{"app": "alpha"}, {"app": "bravo", "capacity": "5Gi"}]`),
		},
	}
	rs := &manifest.ResourceSet{
		Name: "volsync", Namespace: "kube-system",
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
	}
	st := store.New()
	st.AddObject(rsip)
	other := *rsip
	other.Namespace = "other"
	st.AddObject(&other)
	st.AddObject(&manifest.RawObject{APIVersion: "example.test/v1", Kind: manifest.KindResourceSetInputProvider, Name: rsip.Name, Namespace: rsip.Namespace})
	docs, _, err := resourceset.Render(rs, resourceset.StoreResolver(st), nil)
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
	assert.Diff(t, caps, map[string]string{"alpha": "1Gi", "bravo": "5Gi"})
	for _, selector := range []bool{false, true} {
		for _, tc := range []struct {
			name, apiVersion, kind string
			want                   int
		}{
			{"omitted", "", "", 2},
			{"explicit", "fluxcd.controlplane.io/v1", manifest.KindResourceSetInputProvider, 2},
			{"kind only", "", manifest.KindResourceSetInputProvider, 2},
			{"version only", "fluxcd.controlplane.io/v1", "", 2},
			{"foreign", "example.test/v1", "", 0},
			{"core", "v1", "", 0},
			{"other kind", "", manifest.KindResourceSet, 0},
		} {
			t.Run(fmt.Sprintf("%s/selector=%t", tc.name, selector), func(t *testing.T) {
				t.Parallel()
				candidate := *rs
				ref := fluxopv1.InputProviderReference{APIVersion: tc.apiVersion, Kind: tc.kind, Name: rsip.Name}
				if selector {
					ref.Name = ""
					ref.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"set": "apps"}}
				}
				candidate.InputsFrom = []fluxopv1.InputProviderReference{ref}
				docs, _, err := resourceset.Render(&candidate, resourceset.StoreResolver(st), nil)
				if err != nil || len(docs) != tc.want {
					t.Fatalf("docs=%d, error=%v, want %d", len(docs), err, tc.want)
				}
			})
		}
	}
	for _, tc := range []struct {
		name string
		refs []fluxopv1.InputProviderReference
		want int
	}{
		{"expressions", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "set", Operator: metav1.LabelSelectorOpIn, Values: []string{"apps"}}}}}}, 2},
		{"empty selector", []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{}}}, 2},
		{"named invalid selector", []fluxopv1.InputProviderReference{{Name: rsip.Name, Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "set", Operator: "invalid"}}}}}, 2},
		{"nil selector", []fluxopv1.InputProviderReference{{}}, 0},
		{"overlapping references", []fluxopv1.InputProviderReference{{Name: rsip.Name}, {Selector: &metav1.LabelSelector{}}, {Selector: &metav1.LabelSelector{MatchLabels: rsip.Labels}}}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := *rs
			candidate.InputsFrom = tc.refs
			got, _, err := resourceset.Render(&candidate, resourceset.StoreResolver(st), nil)
			assert.NoError(t, err)
			if len(got) != tc.want {
				t.Fatalf("docs=%d, want %d", len(got), tc.want)
			}
			if tc.want != 0 {
				assert.Diff(t, got, docs)
			}
		})
	}

}

func TestStoreResolver_CollectionAssessmentTotal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		provider  bool
		selector  *metav1.LabelSelector
		wantError bool
		wantDocs  int
	}{
		{name: "empty", selector: &metav1.LabelSelector{}},
		{name: "null values", provider: true, selector: &metav1.LabelSelector{}, wantDocs: 1},
		{name: "selector error", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			if tc.provider {
				st.AddObject(&manifest.ResourceSetInputProvider{Name: "provider", Namespace: "apps", DefaultValues: fluxopv1.ResourceSetInput{"value": nil}})
			}
			rs := &manifest.ResourceSet{Name: "reader", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Selector: tc.selector}}, ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n"}
			var reads store.ReadSet
			reads.Init(st, rs.Named(), nil)
			defer reads.Retire()
			docs, _, err := resourceset.Render(rs, resourceset.StoreResolver(&reads), nil)
			if (err != nil) != tc.wantError {
				t.Fatalf("prepare error=%v, want error=%t", err, tc.wantError)
			}
			if tc.wantError {
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, len(docs), tc.wantDocs)
			assert.Equal(t, reads.Valid(), true)
		})
	}
}

func TestStoreResolver_SelectorInputs(t *testing.T) {
	t.Parallel()
	db := &manifest.ResourceSetInputProvider{Name: "db", Namespace: "apps", Labels: map[string]string{"role": "db"}, DefaultValues: fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"first"`)}}
	web := *db
	web.Name, web.Labels = "web", map[string]string{"role": "web"}
	other := *db
	other.Namespace = "other"
	invalid := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}
	selected := &metav1.LabelSelector{MatchLabels: map[string]string{"role": "db"}}
	scope := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "apps"}
	for _, tc := range []struct {
		name      string
		providers []manifest.BaseManifest
		ref       fluxopv1.InputProviderReference
		id        manifest.NamedResource
		want      []string
		wantError bool
	}{
		{"success", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: selected}, scope, []string{"db"}, false},
		{"empty selector", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: &metav1.LabelSelector{}}, scope, []string{"db", "web"}, false},
		{"invalid eligible", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid empty", nil, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid other namespace", []manifest.BaseManifest{&other}, fluxopv1.InputProviderReference{Selector: invalid}, scope, nil, true},
		{"invalid foreign group", []manifest.BaseManifest{db, &manifest.RawObject{APIVersion: "other.example/v1", Kind: scope.Kind, Namespace: scope.Namespace, Name: db.Name}}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: "other.example", Kind: scope.Kind, Namespace: scope.Namespace}, nil, true},
		{"invalid foreign kind", []manifest.BaseManifest{db, &manifest.ResourceSet{Name: db.Name, Namespace: scope.Namespace}}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: scope.Group, Kind: manifest.KindResourceSet, Namespace: scope.Namespace}, nil, true},
		{"named invalid selector", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, db.Named(), []string{"db"}, false},
		{"unnamed nil selector", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{}, scope, nil, false},
		{"omitted group and kind", []manifest.BaseManifest{db, &web}, fluxopv1.InputProviderReference{Selector: selected}, manifest.NamedResource{Namespace: scope.Namespace}, []string{"db"}, false},
		{"omitted group", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Kind: scope.Kind, Namespace: scope.Namespace}, nil, true},
		{"omitted kind", []manifest.BaseManifest{db}, fluxopv1.InputProviderReference{Selector: invalid}, manifest.NamedResource{Group: scope.Group, Namespace: scope.Namespace}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			for _, p := range tc.providers {
				st.AddObject(p)
			}
			resolve := resourceset.StoreResolver(st)
			for range 2 {
				got, err := resolve(tc.ref, tc.id)
				if (err != nil) != tc.wantError {
					t.Fatalf("resolve error=%v, want error=%t", err, tc.wantError)
				}
				var names []string
				for _, p := range got {
					names = append(names, p.Name)
				}
				assert.Diff(t, names, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		prior *metav1.LabelSelector
	}{
		{"first cached error in empty scope", nil},
		{"additional cached error in empty scope", selected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.AddObject(db)
			resolve := resourceset.StoreResolver(st)
			if tc.prior != nil {
				got, err := resolve(fluxopv1.InputProviderReference{Selector: tc.prior}, scope)
				assert.NoError(t, err)
				assert.Equal(t, len(got), 1)
			}
			ref := fluxopv1.InputProviderReference{Selector: invalid}
			empty := scope
			empty.Namespace = "empty"
			for range 2 {
				if _, err := resolve(ref, scope); err == nil {
					t.Fatal("invalid selector accepted with an eligible provider")
				}
				got, err := resolve(ref, empty)
				if err == nil || len(got) != 0 {
					t.Fatalf("empty scope query = %v, %v; want selector error", got, err)
				}
			}
		})
	}
	t.Run("distinct selectors and live providers", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		st.AddObject(db)
		st.AddObject(&web)
		resolve := resourceset.StoreResolver(st)
		webSelector := &metav1.LabelSelector{MatchLabels: web.Labels}
		for _, tc := range []struct {
			selector *metav1.LabelSelector
			want     manifest.NamedResource
		}{{selected, db.Named()}, {webSelector, web.Named()}, {selected, db.Named()}, {webSelector, web.Named()}} {
			got, err := resolve(fluxopv1.InputProviderReference{Selector: tc.selector}, scope)
			assert.NoError(t, err)
			if len(got) != 1 || got[0].Named() != tc.want {
				t.Fatalf("providers=%v, want %s", got, tc.want)
			}
		}
		fresh := *db
		fresh.Labels = map[string]string{"role": "web"}
		fresh.DefaultValues = fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"updated"`)}
		st.AddObject(&fresh)
		got, err := resolve(fluxopv1.InputProviderReference{Selector: selected}, scope)
		assert.NoError(t, err)
		assert.Equal(t, len(got), 0)
		got, err = resolve(fluxopv1.InputProviderReference{Selector: webSelector}, scope)
		assert.NoError(t, err)
		if len(got) != 2 || got[0].Named() != db.Named() || got[1].Named() != web.Named() {
			t.Fatalf("updated providers=%v, want db and web", got)
		}
		inputs, err := got[0].ExportedInputs()
		assert.NoError(t, err)
		assert.Equal(t, inputs[0]["value"], "updated")
	})
}

// TestRender_InputsFrom_DynamicProviderEmptySkip verifies that a
// non-Static provider (which flate can't query offline) contributes
// zero input sets rather than erroring — the ResourceSet still renders
// with whatever inline inputs it has.
func TestRender_InputsFrom_DynamicProviderEmptySkip(t *testing.T) {
	t.Parallel()
	rsip := &manifest.ResourceSetInputProvider{
		Name: "branches", Namespace: "flux-system",
		Type: fluxopv1.InputProviderGitHubBranch,
		URL:  "https://github.com/foo/bar",
	}
	rs := &manifest.ResourceSet{
		Name: "matrix", Namespace: "flux-system",
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
	}
	resolver := func(_ fluxopv1.InputProviderReference, _ manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		return []*manifest.ResourceSetInputProvider{rsip}, nil
	}
	docs, warnings, err := resourceset.Render(rs, resolver, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc from inline input (dynamic provider contributes nothing), got %d", len(docs))
	}
	if len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider || warnings[0].Resource != rs.Named() || warnings[0].Detail[0] != rsip.Named().String() {
		t.Fatalf("offline provider warning = %+v", warnings)
	}
	md := docs[0]["metadata"].(map[string]any)
	if md["name"] != "inline-only" {
		t.Errorf("expected name=inline-only; got %v", md["name"])
	}
}

func TestRender_ResourcesTemplate(t *testing.T) {
	t.Parallel()
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
		Inputs: []fluxopv1.ResourceSetInput{
			{"name": jsonTmpl(t, `"a"`)},
			{"name": jsonTmpl(t, `"b"`)},
		},
		ResourcesTemplate: tmpl,
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if len(docs) != 4 {
		t.Errorf("expected 4 docs (2 inputs × 2 docs each), got %d", len(docs))
	}
	t.Run("invalid stream separator", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{Name: "stream", ResourcesTemplate: "\n---oops: kept\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: rejected}"}
		_, _, err := resourceset.Render(candidate, nil, nil)
		if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
			t.Fatalf("error = %v, want classified invalid stream separator", err)
		}
	})
	t.Run("duplicate mapping keys", func(t *testing.T) {
		t.Parallel()
		candidate := &manifest.ResourceSet{Name: "duplicate-keys", ResourcesTemplate: `--- # comment
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata: {name: db}
spec:
  postgresql: {value: first}
  postgresql: {value: last}
`}
		docs, _, err := resourceset.Render(candidate, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(docs) != 1 {
			t.Fatalf("documents = %d, want 1", len(docs))
		}
		assert.Diff(t, docs[0]["spec"], map[string]any{"postgresql": map[string]any{"value": "last"}})
	})
}

func TestRender_NullInput(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "null", Namespace: "apps",
		Inputs: []fluxopv1.ResourceSetInput{{"value": nil, "malformed": {Raw: []byte("invalid")}}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: null-input}
data:
  value: << if eq inputs.value nil >>"null"<< end >>
  malformed: << if hasKey inputs "malformed" >>present<< else >>omitted<< end >>`,
	}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatalf("documents = %d, want 1", len(docs))
	}
	assert.Diff(t, docs[0]["data"], map[string]any{"value": "null", "malformed": "omitted"})
}

func TestRender_SlugifyLength(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, input, want string }{
		{"truncate", strings.Repeat("a", 64), strings.Repeat("a", 63)},
		{"smart truncate", strings.Repeat("a", 32) + " " + strings.Repeat("b", 32), strings.Repeat("a", 32)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{
				Name: "slug", Namespace: "apps",
				Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"`+tc.input+`"`)}},
				ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: << inputs.name | slugify >>}`,
			}
			docs, _, err := resourceset.Render(rs, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(docs) != 1 {
				t.Fatalf("documents = %d, want 1", len(docs))
			}
			assert.Equal(t, docs[0]["metadata"].(map[string]any)["name"].(string), tc.want)
		})
	}
}

func TestRender_Steps(t *testing.T) {
	t.Parallel()
	resource := jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"first"}}`)
	template := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: second}"
	for _, tt := range []struct {
		name    string
		spec    fluxopv1.ResourceSetSpec
		names   []string
		wantErr string
	}{
		{name: "ordered steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource}},
			{Name: "config", ResourcesTemplate: template},
		}}, names: []string{"first", "second"}},
		{name: "dedup within step", spec: fluxopv1.ResourceSetSpec{Inputs: []fluxopv1.ResourceSetInput{{}}, Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource, resource}},
		}}, names: []string{"first"}},
		{name: "no-input duplicate inside a step keeps both", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "deploy", Resources: []*apix.JSON{resource, resource}},
		}}, names: []string{"first", "first"}},
		{name: "duplicate across steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "first", Resources: []*apix.JSON{resource}},
			{Name: "second", Resources: []*apix.JSON{jsonTmpl(t, `{"apiVersion":"apps/v1beta2","kind":"Deployment","metadata":{"name":"first"}}`)}},
		}}, wantErr: "duplicate resource"},
		{name: "empty identity across steps", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{
			{Name: "first", Resources: []*apix.JSON{jsonTmpl(t, `{}`)}},
			{Name: "second", Resources: []*apix.JSON{jsonTmpl(t, `{}`)}},
		}}, wantErr: "duplicate resource"},
		{name: "steps and resources", spec: fluxopv1.ResourceSetSpec{Resources: []*apix.JSON{resource}, Steps: []fluxopv1.ResourceSetStep{{Name: "deploy", Resources: []*apix.JSON{resource}}}}, wantErr: "mutually exclusive"},
		{name: "steps and template", spec: fluxopv1.ResourceSetSpec{ResourcesTemplate: template, Steps: []fluxopv1.ResourceSetStep{{Name: "deploy", Resources: []*apix.JSON{resource}}}}, wantErr: "mutually exclusive"},
		{name: "empty step with empty providers", spec: fluxopv1.ResourceSetSpec{InputsFrom: []fluxopv1.InputProviderReference{{Name: "empty"}}, Steps: []fluxopv1.ResourceSetStep{{Name: "empty"}}}, wantErr: "at least one"},
		{name: "step template error", spec: fluxopv1.ResourceSetSpec{Steps: []fluxopv1.ResourceSetStep{{Name: "bad", ResourcesTemplate: "<< inputs.missing >>"}}}, wantErr: `step "bad"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{Name: "steps", Namespace: "default", ResourceSetSpec: tt.spec}
			docs, _, err := resourceset.Render(rs, nil, nil)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			names := make([]string, 0, len(docs))
			for _, doc := range docs {
				name, ns := manifest.DocMetadata(doc)
				if ns != "" {
					t.Errorf("namespace = %q", ns)
				}
				labels := doc["metadata"].(map[string]any)["labels"].(map[string]any)
				if labels[fluxopv1.OwnerLabelResourceSetName] != "steps" {
					t.Errorf("labels = %v", labels)
				}
				names = append(names, name)
			}
			assert.Diff(t, names, tt.names)
		})
	}
}

func TestRender_EmptyInputRule(t *testing.T) {
	t.Parallel()
	for _, strategy := range []string{fluxopv1.InputStrategyFlatten, fluxopv1.InputStrategyPermute} {
		for _, referenced := range []bool{false, true} {
			name := strategy + "/inline-only"
			if referenced {
				name = strategy + "/empty-provider"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				rs := &manifest.ResourceSet{Name: "empty", Namespace: "default", InputStrategy: &fluxopv1.InputStrategySpec{Name: strategy}, ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: static}"}
				want := 1
				if referenced {
					rs.InputsFrom = []fluxopv1.InputProviderReference{{Name: "empty"}}
					want = 0
				}
				resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
					return []*manifest.ResourceSetInputProvider{{Name: "empty", Type: fluxopv1.InputProviderGitHubBranch}}, nil
				}
				docs, _, err := resourceset.Render(rs, resolve, nil)
				if err != nil {
					t.Fatal(err)
				}
				if len(docs) != want {
					t.Fatalf("docs = %d, want %d", len(docs), want)
				}
			})
		}
	}
}

func TestRender_GroupDedup(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{Name: "group", Namespace: "default", Inputs: []fluxopv1.ResourceSetInput{{}}, Resources: []*apix.JSON{
		jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared"}}`),
		jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"StatefulSet","metadata":{"name":"shared"}}`),
		jsonTmpl(t, `{"apiVersion":"example.test/v1","kind":"Widget"}`),
		jsonTmpl(t, `{"apiVersion":"other.test/v1","kind":"Widget"}`),
		jsonTmpl(t, `{"apiVersion":"v1","metadata":{"name":"kindless"}}`),
		jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared","namespace":"a"}}`),
		jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"shared","namespace":"b"}}`),
		jsonTmpl(t, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"other"}}`),
		jsonTmpl(t, `{"data":{"value":"zero"}}`),
		jsonTmpl(t, `{}`),
	}, ResourcesTemplate: `apiVersion: apps/v1beta2
kind: Deployment
metadata: {name: shared}
---
apiVersion: other.example/v1
kind: Deployment
metadata: {name: shared}
---
apiVersion: example.test/v1
kind: Deployment
metadata: {}
---
apiVersion: example.test/v1
metadata: {name: incomplete}`}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 10 {
		t.Fatalf("docs = %d, want 10", len(docs))
	}
	assert.Diff(t, []string{manifest.DocAPIVersion(docs[0]), manifest.DocAPIVersion(docs[len(docs)-1])}, []string{"apps/v1", "other.example/v1"})
	malformed := map[string]any{"apiVersion": "example.io/v1/extra", "kind": "Deployment", "metadata": map[string]any{"name": "shared", "namespace": "default"}}
	assert.Equal(t, manifest.DocIdentity(malformed).Group, "example.io")
	assert.Equal(t, resourceset.DedupKey(malformed), manifest.NamedResource{Kind: "Deployment", Namespace: "default", Name: "shared"})
}

func TestRender_NoInputJSONBypassesExclusionAndDedup(t *testing.T) {
	t.Parallel()
	raw := jsonTmpl(t, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"static","annotations":{"fluxcd.controlplane.io/reconcile":"disabled"}}}`)
	rs := &manifest.ResourceSet{Name: "static", Namespace: "default", Resources: []*apix.JSON{raw, raw}, ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: static}
---
apiVersion: v1
kind: ConfigMap
metadata:
 name: excluded
 annotations: {fluxcd.controlplane.io/reconcile: disabled}`}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("docs = %d, want 2 JSON duplicates", len(docs))
	}
	for _, doc := range docs {
		if doc["metadata"].(map[string]any)["annotations"].(map[string]any)[fluxopv1.ReconcileAnnotation] != fluxopv1.DisabledValue {
			t.Fatalf("wrong resource: %v", doc)
		}
	}
}

func TestRender_ListItems(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{Name: "list", Namespace: "default", ResourcesTemplate: `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: first}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: second}
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: first}
- apiVersion: v1
  kind: ConfigMap
  metadata:
    name: excluded
    annotations: {fluxcd.controlplane.io/reconcile: disabled}`}
	docs, _, err := resourceset.Render(rs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(docs))
	for _, doc := range docs {
		name, ns := manifest.DocMetadata(doc)
		names = append(names, name)
		if ns != "" {
			t.Errorf("namespace = %q", ns)
		}
	}
	assert.Diff(t, names, []string{"first", "second"})
	for _, tc := range []struct {
		name, template string
		count          int
		wantErr        bool
	}{
		{"ordinary missing name", "apiVersion: v1\nkind: ConfigMap", 0, false},
		{"ordinary missing kind", "apiVersion: v1\nmetadata: {name: excluded}", 0, false},
		{"ordinary missing apiVersion", "kind: ConfigMap\nmetadata: {name: excluded}", 0, false},
		{"mixed case directive", "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: kUsToMiZaTiOn\nmetadata: {name: excluded}", 0, false},
		{"non directive group", "apiVersion: another.example/v1\nkind: Kustomization\nmetadata: {name: kept}", 1, false},
		{"items without suffix", "items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: item}}]", 1, false},
		{"suffix without items", "apiVersion: example.test/v1\nkind: WidgetList\nmetadata: {name: ordinary}", 1, false},
		{"typed incomplete items", "apiVersion: v1\nkind: ConfigMapList\nitems: [{metadata: {name: kindless}}, {apiVersion: v1, kind: ConfigMap}]", 2, false},
		{"nested list stays wrapped", "items: [{kind: List, items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]}]", 1, false},
		{"directive item survives", "items: [{apiVersion: kustomize.config.k8s.io/v1beta1, kind: Kustomization, metadata: {name: directive}}]", 1, false},
		{"invalid YAML", "metadata: [unterminated", 0, true},
		{"null member", "items: [null]", 0, true},
		{"scalar member", "items: [plain]", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := &manifest.ResourceSet{Name: "boundary", Namespace: "apps", ResourcesTemplate: tc.template}
			got, _, err := resourceset.Render(candidate, nil, nil)
			if tc.wantErr {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("error = %v, want classified malformed list error", err)
				}
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, len(got), tc.count)
			if tc.name == "typed incomplete items" && len(got) == 2 {
				assert.Equal(t, manifest.DocKind(got[0]), "")
				assert.Equal(t, manifest.DocAPIVersion(got[0]), "")
				name, _ := manifest.DocMetadata(got[1])
				assert.Equal(t, name, "")
			}
			if tc.name == "nested list stays wrapped" && len(got) == 1 {
				assert.Equal(t, manifest.DocKind(got[0]), "List")
				assert.Equal(t, len(got[0]["items"].([]any)), 1)
			}
		})
	}

}

func TestRender_Admission(t *testing.T) {
	t.Parallel()
	failure := errors.New("admission failed")
	for _, tc := range []struct {
		name string
		hit  bool
		err  error
	}{
		{"replay", true, nil},
		{"error", false, failure},
		{"template", false, manifest.ErrInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rs := &manifest.ResourceSet{Name: "reader", Namespace: "apps", ResourcesTemplate: "<< invalid template",
				Inputs: []fluxopv1.ResourceSetInput{{"name": jsonTmpl(t, `"inline"`)}}, InputsFrom: []fluxopv1.InputProviderReference{{Name: "dynamic"}}}
			provider := &manifest.ResourceSetInputProvider{Name: "dynamic", Namespace: "apps", Type: fluxopv1.InputProviderGitHubBranch}
			resolved := 0
			resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
				resolved++
				return []*manifest.ResourceSetInputProvider{provider}, nil
			}
			calls := 0
			docs, warnings, err := resourceset.Render(rs, resolve, func(any) (bool, error) {
				calls++
				if tc.name == "template" {
					return false, nil
				}
				return tc.hit, tc.err
			})
			assert.Equal(t, calls, 1)
			assert.Equal(t, resolved, 1)
			if len(warnings) != 1 || warnings[0].Category != manifest.WarnOfflineInputProvider || warnings[0].Resource != rs.Named() {
				t.Fatalf("admission lost provider warning: %+v", warnings)
			}
			if docs != nil || (tc.name == "template" && err == nil) || (tc.name != "template" && !errors.Is(err, tc.err)) {
				t.Fatalf("admission: documents=%v error=%v, want %v", docs, err, tc.err)
			}
		})
	}
	invalid := &manifest.ResourceSet{Name: "invalid", Steps: []fluxopv1.ResourceSetStep{{Name: "empty"}}}
	called := false
	_, _, err := resourceset.Render(invalid, nil, func(any) (bool, error) { called = true; return true, nil })
	if !errors.Is(err, manifest.ErrInput) || called {
		t.Fatalf("invalid steps reached admission: called=%v error=%v", called, err)
	}
}

func TestRender_UsesOneProviderQuery(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "apps",
		InputsFrom:        []fluxopv1.InputProviderReference{{Name: "provider"}},
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.value >>'}\n",
	}
	provider := &manifest.ResourceSetInputProvider{
		Name: "provider", Namespace: "apps",
		DefaultValues: fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"captured"`)},
	}
	calls := 0
	docs, _, err := resourceset.Render(rs, func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
		calls++
		if calls != 1 {
			t.Fatal("provider query repeated")
		}
		return []*manifest.ResourceSetInputProvider{provider}, nil
	}, nil)
	assert.NoError(t, err)
	if calls != 1 || len(docs) != 1 || docs[0]["metadata"].(map[string]any)["name"] != "captured" {
		t.Fatalf("query calls=%d output=%v", calls, docs)
	}
}
