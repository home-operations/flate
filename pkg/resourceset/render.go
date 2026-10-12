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

// Render evaluates the ResourceSet's resources or ordered steps across its
// input matrix, deduplicates within each step, and stamps common metadata.
// Referenced providers with no combined inputs produce no resources.
// resolve is optional and supplies providers referenced by spec.inputsFrom.
func Render(rs *manifest.ResourceSet, resolve ProviderResolver) ([]map[string]any, error) {
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
	inputs, err := buildInputSets(rs, resolve)
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
	stepOfObject := make(map[string]string)
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
		// Reuse the first step's slice without copying to avoid an extra allocation per render.
		if docs == nil {
			docs = rendered
		} else {
			docs = append(docs, rendered...)
		}
	}
	for _, doc := range docs {
		manifest.StampNamespace(doc, rs.Namespace)
		applyOwnerLabels(doc, rs)
		applyCommonMetadata(doc, rs.CommonMetadata)
	}
	return docs, nil
}

func renderStep(step fluxopv1.ResourceSetStep, inputs []map[string]any) ([]map[string]any, error) {
	seen := make(map[string]struct{})
	var docs []map[string]any
	appendUnique := func(rendered []map[string]any, dedup bool) {
		for _, doc := range rendered {
			key := DedupKey(doc)
			if key == "" {
				continue
			}
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

// renderResources templates a single spec.resources entry once per
// input set (or once with nil when inputs is empty), returning the
// decoded YAML docs. The template is parsed once and executed N times
// to avoid repeated parse overhead on large input matrices.
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
		doc, err := renderSingle(pt, in)
		if err != nil {
			return nil, err
		}
		if doc == nil || (len(inputs) > 0 && disabledByReconcileAnnotation(doc)) {
			continue
		}
		out = append(out, doc)
	}
	return out, nil
}

// renderResourcesTemplate templates the multi-document YAML string in
// spec.resourcesTemplate once per input set. The template is parsed once
// and executed N times to avoid repeated parse overhead on large input
// matrices.
func renderResourcesTemplate(tmplStr string, inputs []map[string]any) ([]map[string]any, error) {
	pt, err := parseTemplate(tmplStr)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for _, in := range defaultNilInput(inputs) {
		docs, err := splitMultiDoc(pt, in)
		if err != nil {
			return nil, err
		}
		out = append(out, filterDisabled(docs)...)
	}
	return out, nil
}

// defaultNilInput yields the input sets to template against: the given
// slice, or a single nil set when it is empty. An empty matrix means
// "render once with no inputs" (static resources), so both callers
// would otherwise carry an identical len==0 special case.
func defaultNilInput(inputs []map[string]any) []map[string]any {
	if len(inputs) == 0 {
		return []map[string]any{nil}
	}
	return inputs
}

// filterDisabled removes docs carrying the reconcile-disabled annotation
// in place, reusing the slice backing array.
func filterDisabled(docs []map[string]any) []map[string]any {
	out := docs[:0]
	for _, doc := range docs {
		if !disabledByReconcileAnnotation(doc) {
			out = append(out, doc)
		}
	}
	return out
}

func splitMultiDoc(pt *parsedTemplate, inputSet map[string]any) ([]map[string]any, error) {
	rendered, err := executeTemplate(pt, inputSet)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for chunk := range bytes.SplitSeq(rendered, []byte("\n---")) {
		doc, err := unmarshalDoc(bytes.TrimSpace(chunk))
		if err != nil {
			return nil, err
		}
		if doc != nil {
			out = append(out, doc)
		}
	}
	return manifest.FlattenLists(out), nil
}

func renderSingle(pt *parsedTemplate, inputSet map[string]any) (map[string]any, error) {
	rendered, err := executeTemplate(pt, inputSet)
	if err != nil {
		return nil, err
	}
	return unmarshalDoc(rendered)
}

// unmarshalDoc decodes a single rendered YAML document, returning a nil
// map (no error) when the chunk is empty or decodes to an empty object —
// the caller treats that as "nothing to emit".
func unmarshalDoc(chunk []byte) (map[string]any, error) {
	if len(chunk) == 0 {
		return nil, nil
	}
	var doc map[string]any
	if err := yaml.Unmarshal(chunk, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal rendered doc: %w", err)
	}
	if len(doc) == 0 {
		return nil, nil
	}
	return doc, nil
}
