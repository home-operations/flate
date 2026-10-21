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
//   - spec.steps (ordered builds with cross-step duplicate rejection)
//   - spec.resources (typed JSON template list)
//   - spec.resourcesTemplate (multi-document YAML string)
//   - spec.commonMetadata (labels + annotations applied to every emitted object)
//   - Deduplication by (API group, kind, namespace, name)
//
// Deferred:
//   - fluxcd.controlplane.io/copyFrom / convertKubeConfigFrom / checksumFrom
//     annotations — those need live cluster data flate does not have.
package resourceset

import (
	"bytes"
	"fmt"
	"strings"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// ProviderResolver returns the ResourceSetInputProviders matching a
// single spec.inputsFrom reference at the canonical identity. Callers
// implement this against their object store (orchestrator) or pass nil
// to disable inputsFrom resolution (tests, simple cases).
type ProviderResolver func(ref fluxopv1.InputProviderReference, id manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error)

// Render evaluates the ResourceSet's resources or ordered steps across its
// input matrix, deduplicates within each step, and stamps common metadata.
// Referenced providers with no combined inputs produce no resources.
// admit receives borrowed canonical content before matrix/template work; true skips rendering.
// resolve is optional and supplies providers referenced by spec.inputsFrom.
func Render(rs *manifest.ResourceSet, resolve ProviderResolver, admit func(any) (bool, error)) ([]map[string]any, error) {
	if rs == nil {
		return nil, nil
	}
	if len(rs.Steps) > 0 && (len(rs.Resources) > 0 || rs.ResourcesTemplate != "") {
		return nil, fmt.Errorf("%w: spec.steps is mutually exclusive with spec.resources and spec.resourcesTemplate", manifest.ErrInput)
	}
	for _, step := range rs.Steps {
		if len(step.Resources) == 0 && step.ResourcesTemplate == "" {
			return nil, fmt.Errorf("%w: step %q: at least one of resources or resourcesTemplate must be set", manifest.ErrInput, step.Name)
		}
	}
	providers, err := collectProviderInputs(rs, resolve)
	if err != nil {
		return nil, fmt.Errorf("ResourceSet %s: %w", rs.Named().NamespacedName(), err)
	}
	if admit != nil {
		handled, err := admit(struct {
			Name      string
			Namespace string
			Spec      *fluxopv1.ResourceSetSpec
			Providers []providerContent
		}{rs.Name, rs.Namespace, &rs.ResourceSetSpec, providers})
		if handled || err != nil {
			return nil, err
		}
	}
	inputs, err := buildInputSets(rs, providers)
	if err != nil {
		return nil, fmt.Errorf("ResourceSet %s: %w", rs.Named().NamespacedName(), err)
	}
	if len(rs.InputsFrom) > 0 && len(inputs) == 0 {
		return nil, nil
	}

	steps := rs.Steps
	if len(steps) == 0 {
		steps = []fluxopv1.ResourceSetStep{{Resources: rs.Resources, ResourcesTemplate: rs.ResourcesTemplate}}
	}
	var docs []map[string]any
	stepOfObject := make(map[manifest.NamedResource]string)
	for _, step := range steps {
		rendered, err := renderStep(step, inputs)
		if err != nil {
			if len(rs.Steps) > 0 {
				err = fmt.Errorf("step %q: %w", step.Name, err)
			}
			return nil, fmt.Errorf("ResourceSet %s: %w", rs.Named().NamespacedName(), err)
		}
		if len(rs.Steps) > 0 {
			for _, doc := range rendered {
				key := DedupKey(doc)
				if prev, found := stepOfObject[key]; found && prev != step.Name {
					return nil, fmt.Errorf("%w: duplicate resource %s in step %q, already defined in step %q", manifest.ErrInput, key, step.Name, prev)
				}
				stepOfObject[key] = step.Name
			}
		}
		if docs == nil {
			docs = rendered
		} else {
			docs = append(docs, rendered...)
		}
	}
	for _, doc := range docs {
		applyOwnerLabels(doc, rs)
		applyCommonMetadata(doc, rs.CommonMetadata)
	}
	return docs, nil
}

func renderStep(step fluxopv1.ResourceSetStep, inputs []map[string]any) ([]map[string]any, error) {
	seen := make(map[manifest.NamedResource]struct{})
	var docs []map[string]any
	appendUnique := func(rendered []map[string]any, dedup bool) {
		for _, doc := range rendered {
			if dedup && disabledByReconcileAnnotation(doc) {
				continue
			}
			key := DedupKey(doc)
			if _, dup := seen[key]; dedup && dup {
				continue
			}
			seen[key] = struct{}{}
			docs = append(docs, doc)
		}
	}
	// Flux preserves no-input JSON duplicates while giving them precedence over YAML.
	for i, raw := range step.Resources {
		rendered, err := renderResources(raw, inputs)
		if err != nil {
			return nil, fmt.Errorf("spec.resources[%d]: %w", i, err)
		}
		appendUnique(rendered, len(inputs) > 0)
	}
	if step.ResourcesTemplate != "" {
		rendered, err := renderResourcesTemplate(step.ResourcesTemplate, inputs)
		if err != nil {
			return nil, fmt.Errorf("spec.resourcesTemplate: %w", err)
		}
		appendUnique(rendered, true)
	}
	return docs, nil
}

func renderResources(raw *apix.JSON, inputs []map[string]any) ([]map[string]any, error) {
	if raw == nil {
		return nil, nil
	}
	yamlBytes, err := yaml.JSONToYAML(raw.Raw)
	if err != nil {
		return nil, fmt.Errorf("convert template to YAML: %w", err)
	}
	pt, err := parseTemplate(string(yamlBytes))
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, len(inputs))
	for _, in := range defaultNilInput(inputs) {
		rendered, err := executeTemplate(pt, in)
		if err != nil {
			return nil, err
		}
		var doc map[string]any
		if err := yaml.Unmarshal(rendered, &doc); err != nil {
			return nil, fmt.Errorf("%w: unmarshal rendered doc: %w", manifest.ErrInput, err)
		}
		if doc == nil {
			continue
		}
		out = append(out, doc)
	}
	return out, nil
}

func renderResourcesTemplate(tmplStr string, inputs []map[string]any) ([]map[string]any, error) {
	pt, err := parseTemplate(tmplStr)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for _, in := range defaultNilInput(inputs) {
		rendered, err := executeTemplate(pt, in)
		if err != nil {
			return nil, err
		}
		// Flux rejects non-comment content after a column-zero document separator.
		for line := range bytes.SplitSeq(rendered, []byte{'\n'}) {
			if bytes.HasPrefix(line, []byte("---")) {
				tail := bytes.TrimSpace(line[3:])
				if len(tail) > 0 && tail[0] != '#' {
					return nil, fmt.Errorf("%w: invalid Yaml document separator: %s", manifest.ErrInput, tail)
				}
			}
		}
		for chunk := range bytes.SplitSeq(rendered, []byte("\n---")) {
			var doc map[string]any
			if err := yaml.Unmarshal(bytes.TrimPrefix(chunk, []byte("---")), &doc); err != nil {
				return nil, fmt.Errorf("%w: decode rendered YAML: %w", manifest.ErrInput, err)
			}
			obj := &unstructured.Unstructured{Object: doc}
			if obj.IsList() {
				if err := obj.EachListItem(func(item runtime.Object) error {
					out = append(out, item.(*unstructured.Unstructured).Object)
					return nil
				}); err != nil {
					return nil, fmt.Errorf("%w: read rendered list: %w", manifest.ErrInput, err)
				}
			} else if obj.GetName() != "" && obj.GetKind() != "" && obj.GetAPIVersion() != "" &&
				(!strings.EqualFold(obj.GetKind(), "kustomization") || !strings.HasPrefix(obj.GetAPIVersion(), "kustomize.config.k8s.io/")) {
				out = append(out, doc)
			}
		}
	}
	return out, nil
}

func defaultNilInput(inputs []map[string]any) []map[string]any {
	if len(inputs) == 0 {
		return []map[string]any{nil}
	}
	return inputs
}
