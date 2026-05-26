// Package resourceset renders flux-operator ResourceSet CRs offline.
//
// A ResourceSet is a templating CRD that emits a fixed set of Kubernetes
// resources across a matrix of input values. flate evaluates each
// spec.resources / spec.resourcesTemplate entry once per input set using
// slim-sprig with the `<<  >>` delimiter pair (so ResourceSet templates
// can sit inside Helm charts without delimiter conflicts).
//
// Supported:
//   - spec.inputs (in-YAML inline inputs)
//   - spec.inputsFrom (ResourceSetInputProvider, Static type) — resolved
//     via the ProviderResolver passed to Render. Dynamic providers
//     (GitHubBranch, OCIArtifactTag, ExternalService, …) need network
//     access flate doesn't have and contribute zero input sets.
//   - spec.inputStrategy: Flatten (default) AND Permute. Permute
//     Cartesian-products inputs across providers and scopes each
//     under its normalized name; templates access values via
//     `inputs.<provider>.foo`. ID generation matches upstream's
//     adler32 scheme so renders are byte-equivalent to flux-operator
//     output. Capped at 10000 permutations.
//   - spec.resources (typed JSON template list)
//   - spec.resourcesTemplate (multi-document YAML string)
//   - spec.commonMetadata (labels + annotations applied to every emitted object)
//   - Deduplication by (apiVersion, kind, namespace, name)
//   - Default-namespace fallback to the ResourceSet's own namespace
//
// Deferred:
//   - fluxcd.controlplane.io/copyFrom / convertKubeConfigFrom / checksumFrom
//     annotations — those need live cluster data flate does not have.
package resourceset

import (
	"bytes"
	"fmt"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// ProviderResolver returns the ResourceSetInputProviders matching a
// single spec.inputsFrom reference within the given namespace. Callers
// implement this against their object store (orchestrator) or pass nil
// to disable inputsFrom resolution (tests, simple cases).
type ProviderResolver func(ref fluxopv1.InputProviderReference, namespace string) ([]*manifest.ResourceSetInputProvider, error)

// Render evaluates rs.Spec across rs.Spec.Inputs (flatten strategy) and
// returns the resulting Kubernetes manifests as decoded YAML documents,
// already deduplicated and stamped with rs.Spec.CommonMetadata. The
// caller is responsible for parsing each doc into a typed manifest.
//
// When rs.Spec.Inputs is empty (e.g. d2-fleet's policies.yaml — static
// resources with no matrix), templates render once with a nil input set.
//
// resolve is optional: when non-nil, spec.inputsFrom references are
// resolved and their providers' exported inputs are flattened into the
// matrix alongside any inline rs.Spec.Inputs.
func Render(rs *manifest.ResourceSet, resolve ProviderResolver) ([]map[string]any, error) {
	if rs == nil {
		return nil, nil
	}

	inputs, err := buildInputSets(rs, resolve)
	if err != nil {
		return nil, fmt.Errorf("ResourceSet %s: %w", rs.Named().NamespacedName(), err)
	}
	// Under Permute, an empty input slice means the Cartesian product
	// genuinely collapsed (every-provider-empty with includeEmpty=true,
	// or no providers contributed). Render zero docs rather than
	// falling through to renderResources' "no matrix → render once
	// with nil" fallback, which only applies under Flatten where an
	// empty matrix legitimately means "static resources, no inputs".
	if len(inputs) == 0 && rs.InputStrategy != nil && rs.InputStrategy.Name == fluxopv1.InputStrategyPermute {
		return nil, nil
	}
	var docs []map[string]any
	seen := map[string]bool{}
	appendUnique := func(doc map[string]any) {
		key := DedupKey(doc)
		if key == "" || seen[key] {
			return
		}
		seen[key] = true
		docs = append(docs, doc)
	}

	for i, raw := range rs.Resources {
		rendered, err := renderResources(raw, inputs)
		if err != nil {
			return nil, fmt.Errorf("ResourceSet %s: spec.resources[%d]: %w", rs.Named().NamespacedName(), i, err)
		}
		for _, doc := range rendered {
			appendUnique(doc)
		}
	}

	if rs.ResourcesTemplate != "" {
		rendered, err := renderResourcesTemplate(rs.ResourcesTemplate, inputs)
		if err != nil {
			return nil, fmt.Errorf("ResourceSet %s: spec.resourcesTemplate: %w", rs.Named().NamespacedName(), err)
		}
		for _, doc := range rendered {
			appendUnique(doc)
		}
	}

	for _, doc := range docs {
		defaultNamespace(doc, rs.Namespace)
		applyOwnerLabels(doc, rs)
		applyCommonMetadata(doc, rs.CommonMetadata)
	}
	return docs, nil
}

// renderResources templates a single spec.resources entry once per
// input set (or once with nil when inputs is empty), returning the
// decoded YAML docs.
func renderResources(raw *apix.JSON, inputs []map[string]any) ([]map[string]any, error) {
	if raw == nil {
		return nil, nil
	}
	yamlTemplate, err := yaml.JSONToYAML(raw.Raw)
	if err != nil {
		return nil, fmt.Errorf("convert template to YAML: %w", err)
	}
	tmplStr := string(yamlTemplate)

	if len(inputs) == 0 {
		doc, err := renderSingle(tmplStr, nil)
		if err != nil {
			return nil, err
		}
		if doc == nil || disabledByReconcileAnnotation(doc) {
			return nil, nil
		}
		return []map[string]any{doc}, nil
	}
	var out []map[string]any
	for _, in := range inputs {
		doc, err := renderSingle(tmplStr, in)
		if err != nil {
			return nil, err
		}
		if doc == nil {
			continue
		}
		if disabledByReconcileAnnotation(doc) {
			continue
		}
		out = append(out, doc)
	}
	return out, nil
}

// renderResourcesTemplate templates the multi-document YAML string in
// spec.resourcesTemplate once per input set.
func renderResourcesTemplate(tmplStr string, inputs []map[string]any) ([]map[string]any, error) {
	filter := func(docs []map[string]any) []map[string]any {
		out := docs[:0]
		for _, doc := range docs {
			if disabledByReconcileAnnotation(doc) {
				continue
			}
			out = append(out, doc)
		}
		return out
	}
	if len(inputs) == 0 {
		docs, err := splitMultiDoc(tmplStr, nil)
		if err != nil {
			return nil, err
		}
		return filter(docs), nil
	}
	var out []map[string]any
	for _, in := range inputs {
		docs, err := splitMultiDoc(tmplStr, in)
		if err != nil {
			return nil, err
		}
		out = append(out, filter(docs)...)
	}
	return out, nil
}

func splitMultiDoc(tmplStr string, inputSet map[string]any) ([]map[string]any, error) {
	rendered, err := execute(tmplStr, inputSet)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for chunk := range bytes.SplitSeq(rendered, []byte("\n---")) {
		chunk = bytes.TrimSpace(chunk)
		if len(chunk) == 0 {
			continue
		}
		var doc map[string]any
		if err := yaml.Unmarshal(chunk, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal rendered doc: %w", err)
		}
		if len(doc) == 0 {
			continue
		}
		out = append(out, doc)
	}
	return out, nil
}

func renderSingle(tmplStr string, inputSet map[string]any) (map[string]any, error) {
	rendered, err := execute(tmplStr, inputSet)
	if err != nil {
		return nil, err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(rendered, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal rendered doc: %w", err)
	}
	if len(doc) == 0 {
		return nil, nil
	}
	return doc, nil
}
