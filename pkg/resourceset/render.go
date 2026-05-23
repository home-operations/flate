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
//   - spec.resources (typed JSON template list)
//   - spec.resourcesTemplate (multi-document YAML string)
//   - spec.commonMetadata (labels + annotations applied to every emitted object)
//   - Deduplication by (apiVersion, kind, namespace, name)
//   - Default-namespace fallback to the ResourceSet's own namespace
//
// Deferred:
//   - spec.inputStrategy: Permute — start with the implicit Flatten.
//   - fluxcd.controlplane.io/copyFrom / convertKubeConfigFrom / checksumFrom
//     annotations — those need live cluster data flate does not have.
package resourceset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"text/template"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	sprig "github.com/go-task/slim-sprig/v3"
	"github.com/gosimple/slug"
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
		return nil, fmt.Errorf("ResourceSet %s: %w", rs.NamespacedName(), err)
	}
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

// buildInputSets returns the flattened input matrix. The ResourceSet's
// own inline spec.inputs come first (with a provider block pointing at
// the ResourceSet itself), followed by every input set exported by each
// referenced ResourceSetInputProvider (with a provider block pointing
// at the RSIP). Matches upstream's Flatten strategy: simple concat,
// order = rset's inline inputs first, then providers in sorted
// (kind, namespace, name) order.
//
// Permute strategy is not yet implemented (#109). inputs.id is left to
// the provider when it's a Static RSIP; inline-only inputs see no
// synthetic id under Flatten.
func buildInputSets(rs *manifest.ResourceSet, resolve ProviderResolver) ([]map[string]any, error) {
	var out []map[string]any
	for _, in := range rs.Inputs {
		decoded := decodeInputSet(in)
		decoded["provider"] = map[string]any{
			"apiVersion": fluxopv1.GroupVersion.String(),
			"kind":       manifest.KindResourceSet,
			"name":       rs.Name,
			"namespace":  rs.Namespace,
		}
		out = append(out, decoded)
	}
	if resolve == nil || len(rs.InputsFrom) == 0 {
		return out, nil
	}

	seen := make(map[string]struct{})
	var providers []*manifest.ResourceSetInputProvider
	for _, ref := range rs.InputsFrom {
		matches, err := resolve(ref, rs.Namespace)
		if err != nil {
			return nil, fmt.Errorf("inputsFrom %q: %w", ref.Name, err)
		}
		for _, p := range matches {
			if p == nil {
				continue
			}
			k := p.Namespace + "/" + p.Name
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			providers = append(providers, p)
		}
	}
	// Sort providers by (namespace, name) for deterministic output,
	// matching upstream's Combine routine ordering.
	sort.Slice(providers, func(i, j int) bool {
		if providers[i].Namespace != providers[j].Namespace {
			return providers[i].Namespace < providers[j].Namespace
		}
		return providers[i].Name < providers[j].Name
	})
	for _, p := range providers {
		exported, err := p.ExportedInputs()
		if err != nil {
			return nil, fmt.Errorf("ResourceSetInputProvider %s: %w", p.NamespacedName(), err)
		}
		if exported == nil && p.Type != "" && p.Type != fluxopv1.InputProviderStatic {
			slog.Warn("resourceset: dynamic input provider contributes no inputs offline",
				"resourceSet", rs.NamespacedName(),
				"provider", p.NamespacedName(),
				"type", p.Type)
		}
		for _, set := range exported {
			set["provider"] = map[string]any{
				"apiVersion": fluxopv1.GroupVersion.String(),
				"kind":       manifest.KindResourceSetInputProvider,
				"name":       p.Name,
				"namespace":  p.Namespace,
			}
			out = append(out, set)
		}
	}
	return out, nil
}

func decodeInputSet(in fluxopv1.ResourceSetInput) map[string]any {
	decoded := map[string]any{}
	for k, v := range in {
		if v == nil {
			decoded[k] = nil
			continue
		}
		var raw any
		if err := json.Unmarshal(v.Raw, &raw); err != nil {
			// Malformed entries are skipped silently — the parser
			// already accepted the document, and there's no good
			// signaling channel beyond a controller log line.
			continue
		}
		decoded[k] = raw
	}
	return decoded
}

// MatchSelector returns true when sel matches lbls. Helper for
// ProviderResolver implementations that filter by InputProviderReference.Selector.
func MatchSelector(sel *metav1.LabelSelector, lbls map[string]string) (bool, error) {
	if sel == nil {
		return true, nil
	}
	s, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return false, err
	}
	return s.Matches(labels.Set(lbls)), nil
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
			"slugify":    slug.Make,
			"toYaml":     toYaml,
			"mustToYaml": mustToYaml,
			"inputs":     func() any { return inputSet },
		}).
		Option("missingkey=error").
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	return buf.Bytes(), nil
}

// toYaml mirrors upstream's silent-error variant — encodes v as YAML;
// any marshal error collapses to "". Templates expect this signature
// so authors don't have to wrap every call in {{ if }}…{{ end }}.
func toYaml(v any) string {
	s, err := mustToYaml(v)
	if err != nil {
		return ""
	}
	return s
}

// mustToYaml is the explicit error-returning variant — surfaces a
// marshal error to the template engine, which aborts execution. Use
// this when the template is willing to fail loudly on malformed inputs.
func mustToYaml(v any) (string, error) {
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
