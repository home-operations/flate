// Package resourceset renders flux-operator ResourceSet CRs offline.
//
// A ResourceSet is a templating CRD that emits a fixed set of Kubernetes
// resources across a matrix of input values. flate evaluates each
// spec.resources / spec.resourcesTemplate entry once per input set using
// slim-sprig with the `<<  >>` delimiter pair (so ResourceSet templates
// can sit inside Helm charts without delimiter conflicts).
//
// Scope on this first pass:
//   - spec.inputs (in-YAML inline inputs)
//   - spec.resources (typed JSON template list)
//   - spec.resourcesTemplate (multi-document YAML string)
//   - spec.commonMetadata (labels + annotations applied to every emitted object)
//   - Deduplication by (apiVersion, kind, namespace, name)
//   - Default-namespace fallback to the ResourceSet's own namespace
//
// Deferred:
//   - spec.inputsFrom (ResourceSetInputProvider) — needs a separate CRD type.
//   - spec.inputStrategy: Permute — start with the implicit Flatten.
//   - fluxcd.controlplane.io/copyFrom / convertKubeConfigFrom / checksumFrom
//     annotations — those need live cluster data flate does not have.
package resourceset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	sprig "github.com/go-task/slim-sprig/v3"
	"github.com/gosimple/slug"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

// Render evaluates rs.Spec across rs.Spec.Inputs (flatten strategy) and
// returns the resulting Kubernetes manifests as decoded YAML documents,
// already deduplicated and stamped with rs.Spec.CommonMetadata. The
// caller is responsible for parsing each doc into a typed manifest.
//
// When rs.Spec.Inputs is empty (e.g. d2-fleet's policies.yaml — static
// resources with no matrix), templates render once with a nil input set.
func Render(rs *manifest.ResourceSet) ([]map[string]any, error) {
	if rs == nil {
		return nil, nil
	}

	inputs := buildInputSets(rs)
	var docs []map[string]any
	seen := map[string]struct{}{}
	appendUnique := func(doc map[string]any) {
		key := dedupKey(doc)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		docs = append(docs, doc)
	}

	for i, raw := range rs.Resources {
		rendered, err := renderResources(raw, inputs)
		if err != nil {
			return nil, fmt.Errorf("ResourceSet %s: spec.resources[%d]: %w", rs.NamespacedName(), i, err)
		}
		for _, doc := range rendered {
			appendUnique(doc)
		}
	}

	if rs.ResourcesTemplate != "" {
		rendered, err := renderResourcesTemplate(rs.ResourcesTemplate, inputs)
		if err != nil {
			return nil, fmt.Errorf("ResourceSet %s: spec.resourcesTemplate: %w", rs.NamespacedName(), err)
		}
		for _, doc := range rendered {
			appendUnique(doc)
		}
	}

	for _, doc := range docs {
		defaultNamespace(doc, rs.Namespace)
		applyCommonMetadata(doc, rs.CommonMetadata)
	}
	return docs, nil
}

// buildInputSets returns the flattened in-YAML input matrix. Each entry
// is a map[string]any with the built-in inputs.id + inputs.provider
// fields injected, matching the flux-operator runtime contract.
func buildInputSets(rs *manifest.ResourceSet) []map[string]any {
	if len(rs.Inputs) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(rs.Inputs))
	for i, in := range rs.Inputs {
		decoded := map[string]any{}
		for k, v := range in {
			if v == nil {
				decoded[k] = nil
				continue
			}
			var raw any
			if err := json.Unmarshal(v.Raw, &raw); err != nil {
				// flate skips malformed entries silently — the parser
				// already accepted the document, and at render time we
				// have no good signaling channel beyond the per-resource
				// log line a controller would emit.
				continue
			}
			decoded[k] = raw
		}
		decoded["id"] = fmt.Sprintf("%s-%d", rs.Name, i)
		decoded["provider"] = map[string]any{
			"apiVersion": fluxopv1.GroupVersion.String(),
			"kind":       manifest.KindResourceSet,
			"name":       rs.Name,
			"namespace":  rs.Namespace,
		}
		out = append(out, decoded)
	}
	return out
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
		if doc == nil {
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
	if len(inputs) == 0 {
		return splitMultiDoc(tmplStr, nil)
	}
	var out []map[string]any
	for _, in := range inputs {
		docs, err := splitMultiDoc(tmplStr, in)
		if err != nil {
			return nil, err
		}
		for _, doc := range docs {
			if disabledByReconcileAnnotation(doc) {
				continue
			}
			out = append(out, doc)
		}
	}
	return out, nil
}

func splitMultiDoc(tmplStr string, inputSet map[string]any) ([]map[string]any, error) {
	rendered, err := execute(tmplStr, inputSet)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for _, chunk := range bytes.Split(rendered, []byte("\n---")) {
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

func execute(tmplStr string, inputSet map[string]any) ([]byte, error) {
	tmpl, err := template.New("resourceset").
		Delims("<<", ">>").
		Funcs(sprig.HermeticTxtFuncMap()).
		Funcs(template.FuncMap{
			"slugify": slug.Make,
			"toYaml":  toYAML,
			"inputs":  func() any { return inputSet },
		}).
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	if inputSet != nil {
		// Promote inputs.X access by wrapping execution data in a map
		// with an "inputs" key — slim-sprig's funcs reach `.inputs.X`
		// via the standard text/template dot-walk.
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, map[string]any{"inputs": inputSet}); err != nil {
			return nil, fmt.Errorf("execute template: %w", err)
		}
		return buf.Bytes(), nil
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	return buf.Bytes(), nil
}

func toYAML(v any) (string, error) {
	out, err := yaml.Marshal(v)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(out), "\n"), nil
}

func dedupKey(doc map[string]any) string {
	apiVersion, _ := doc["apiVersion"].(string)
	kind, _ := doc["kind"].(string)
	md, _ := doc["metadata"].(map[string]any)
	name, _ := md["name"].(string)
	ns, _ := md["namespace"].(string)
	if kind == "" || name == "" {
		return ""
	}
	return apiVersion + "|" + kind + "|" + ns + "|" + name
}

func defaultNamespace(doc map[string]any, ns string) {
	if ns == "" {
		return
	}
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		doc["metadata"] = md
	}
	if cur, _ := md["namespace"].(string); cur != "" {
		return
	}
	// Don't inject a namespace on cluster-scoped kinds — match the
	// upstream operator behavior of leaving Namespace, ClusterRole etc.
	// without a metadata.namespace.
	if isClusterScoped(doc) {
		return
	}
	md["namespace"] = ns
}

func isClusterScoped(doc map[string]any) bool {
	kind, _ := doc["kind"].(string)
	switch kind {
	case "Namespace",
		"ClusterRole", "ClusterRoleBinding",
		"CustomResourceDefinition",
		"PersistentVolume",
		"StorageClass",
		"PriorityClass",
		"MutatingWebhookConfiguration", "ValidatingWebhookConfiguration",
		"APIService",
		"Node":
		return true
	}
	return false
}

func applyCommonMetadata(doc map[string]any, cm *fluxopv1.CommonMetadata) {
	if cm == nil || (len(cm.Labels) == 0 && len(cm.Annotations) == 0) {
		return
	}
	md, _ := doc["metadata"].(map[string]any)
	if md == nil {
		md = map[string]any{}
		doc["metadata"] = md
	}
	mergeStringMap(md, "labels", cm.Labels)
	mergeStringMap(md, "annotations", cm.Annotations)
}

func mergeStringMap(md map[string]any, key string, in map[string]string) {
	if len(in) == 0 {
		return
	}
	out, _ := md[key].(map[string]any)
	if out == nil {
		out = make(map[string]any, len(in))
	}
	for k, v := range in {
		out[k] = v
	}
	md[key] = out
}

func disabledByReconcileAnnotation(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	if v, _ := ann[fluxopv1.ReconcileAnnotation].(string); v == fluxopv1.DisabledValue {
		return true
	}
	return false
}
