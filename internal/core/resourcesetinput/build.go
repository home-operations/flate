package resourcesetinput

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"
)

// ErrDocument identifies a document error that the renderer reports as input failure.
var ErrDocument = errors.New("invalid ResourceSet document")

// ValidateSteps applies the ResourceSet step requirements before input evaluation.
func ValidateSteps(spec *fluxopv1.ResourceSetSpec) error {
	if len(spec.Steps) > 0 && (len(spec.Resources) > 0 || spec.ResourcesTemplate != "") {
		return errors.New("spec.steps is mutually exclusive with spec.resources and spec.resourcesTemplate")
	}
	for _, step := range spec.Steps {
		if len(step.Resources) == 0 && step.ResourcesTemplate == "" {
			return fmt.Errorf("step %q: at least one of resources or resourcesTemplate must be set", step.Name)
		}
	}
	return nil
}

// Build applies Flux's precedence to rendered documents. Partial builds retain
// successful evaluations for projection and return the first error.
func Build(spec *fluxopv1.ResourceSetSpec, inputs []map[string]any, partial bool) ([]map[string]any, error) {
	steps := spec.Steps
	if len(steps) == 0 {
		steps = []fluxopv1.ResourceSetStep{{Resources: spec.Resources, ResourcesTemplate: spec.ResourcesTemplate}}
	}
	var docs []map[string]any
	var firstErr error
	stepOfObject := make(map[ResourceKey]string)
	for i := range steps {
		step := &steps[i]
		rendered, err := buildStep(step, inputs, partial)
		if err != nil && len(spec.Steps) > 0 {
			err = fmt.Errorf("step %q: %w", step.Name, err)
		}
		if err == nil && len(spec.Steps) > 0 {
			err = recordStepObjects(step.Name, rendered, stepOfObject)
		}
		if err != nil && !partial {
			return nil, err
		}
		if firstErr == nil {
			firstErr = err
		}
		if docs == nil {
			docs = rendered
		} else {
			docs = append(docs, rendered...)
		}
	}
	return docs, firstErr
}

func buildStep(step *fluxopv1.ResourceSetStep, inputs []map[string]any, partial bool) ([]map[string]any, error) {
	documents := stepDocuments{seen: make(map[ResourceKey]struct{})}
	var firstErr error
	for i, raw := range step.Resources {
		if raw == nil {
			continue
		}
		rendered, err := executeJSON(raw.Raw, inputs, partial)
		if err != nil {
			err = fmt.Errorf("spec.resources[%d]: %w", i, err)
			if !partial {
				return nil, err
			}
			if firstErr == nil {
				firstErr = err
			}
		}
		// Flux preserves no-input JSON duplicates while giving them precedence over YAML.
		documents.add(rendered, len(inputs) > 0)
	}
	if step.ResourcesTemplate != "" {
		rendered, err := execute(step.ResourcesTemplate, inputs, appendTemplateDocuments, partial)
		documents.add(rendered, true)
		if firstErr == nil && err != nil {
			firstErr = fmt.Errorf("spec.resourcesTemplate: %w", err)
		}
	}
	return documents.docs, firstErr
}

type stepDocuments struct {
	seen map[ResourceKey]struct{}
	docs []map[string]any
}

func (d *stepDocuments) add(rendered []map[string]any, dedup bool) {
	for _, doc := range rendered {
		key := Key(doc)
		if _, dup := d.seen[key]; dedup && (dup || disabled(doc)) {
			continue
		}
		d.seen[key] = struct{}{}
		d.docs = append(d.docs, doc)
	}
}

func executeJSON(data []byte, inputs []map[string]any, partial bool) ([]map[string]any, error) {
	converted, err := yaml.JSONToYAML(data)
	if err != nil {
		return nil, fmt.Errorf("convert template to YAML: %w", err)
	}
	return execute(string(converted), inputs, appendJSONDocument, partial)
}

func appendJSONDocument(out []map[string]any, rendered []byte) ([]map[string]any, error) {
	var doc map[string]any
	if err := yaml.Unmarshal(rendered, &doc); err != nil {
		return out, documentError{message: "unmarshal rendered doc", cause: err}
	}
	if doc != nil {
		out = append(out, doc)
	}
	return out, nil
}

func execute(
	data string,
	inputs []map[string]any,
	decode func([]map[string]any, []byte) ([]map[string]any, error),
	partial bool,
) ([]map[string]any, error) {
	tmpl, err := Parse(data)
	if err != nil {
		return nil, err
	}
	if len(inputs) == 0 {
		inputs = []map[string]any{nil}
	}
	var out []map[string]any
	var firstErr error
	for _, input := range inputs {
		rendered, err := tmpl.Execute(input)
		if err == nil {
			out, err = decode(out, rendered)
		}
		if err == nil {
			continue
		}
		if !partial {
			return nil, err
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return out, firstErr
}

func appendTemplateDocuments(out []map[string]any, rendered []byte) ([]map[string]any, error) {
	// Flux rejects non-comment content after a column-zero document separator.
	for line := range bytes.SplitSeq(rendered, []byte{'\n'}) {
		if bytes.HasPrefix(line, []byte("---")) {
			tail := bytes.TrimSpace(line[3:])
			if len(tail) > 0 && tail[0] != '#' {
				return out, documentError{message: fmt.Sprintf("invalid Yaml document separator: %s", tail)}
			}
		}
	}
	for chunk := range bytes.SplitSeq(rendered, []byte("\n---")) {
		var doc map[string]any
		if err := yaml.Unmarshal(bytes.TrimPrefix(chunk, []byte("---")), &doc); err != nil {
			return out, documentError{message: "decode rendered YAML", cause: err}
		}
		obj := &unstructured.Unstructured{Object: doc}
		if obj.IsList() {
			if err := obj.EachListItem(func(item runtime.Object) error {
				//nolint:errcheck // Unstructured.EachListItem supplies unstructured objects.
				out = append(out, item.(*unstructured.Unstructured).Object)
				return nil
			}); err != nil {
				return out, documentError{message: "read rendered list", cause: err}
			}
		} else if obj.GetName() != "" && obj.GetKind() != "" && obj.GetAPIVersion() != "" &&
			(!strings.EqualFold(obj.GetKind(), "kustomization") || !strings.HasPrefix(obj.GetAPIVersion(), "kustomize.config.k8s.io/")) {
			out = append(out, doc)
		}
	}
	return out, nil
}

func recordStepObjects(step string, docs []map[string]any, stepOfObject map[ResourceKey]string) error {
	for _, doc := range docs {
		key := Key(doc)
		if previous, found := stepOfObject[key]; found && previous != step {
			kind := key.Kind
			if key.Group != "" {
				kind += "." + key.Group
			}
			name := key.Name
			if key.Namespace != "" {
				name = key.Namespace + "/" + name
			}
			return documentError{message: fmt.Sprintf(
				"duplicate resource %s/%s in step %q, already defined in step %q",
				kind, name, step, previous,
			)}
		}
		stepOfObject[key] = step
	}
	return nil
}

// ResourceKey is the identity used to deduplicate authored documents.
type ResourceKey struct {
	Group, Kind, Namespace, Name string
}

// Key extracts a resource identity before namespace defaulting.
func Key(doc map[string]any) ResourceKey {
	apiVersion, _ := doc["apiVersion"].(string)
	version, _ := schema.ParseGroupVersion(apiVersion)
	kind, _ := doc["kind"].(string)
	metadata, _ := doc["metadata"].(map[string]any)
	namespace, _ := metadata["namespace"].(string)
	name, _ := metadata["name"].(string)
	return ResourceKey{Group: version.Group, Kind: kind, Namespace: namespace, Name: name}
}

func disabled(doc map[string]any) bool {
	md, _ := doc["metadata"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	value, _ := ann[fluxopv1.ReconcileAnnotation].(string)
	return value == fluxopv1.DisabledValue
}

type documentError struct {
	message string
	cause   error
}

func (e documentError) Error() string {
	if e.cause == nil {
		return e.message
	}
	return e.message + ": " + e.cause.Error()
}

func (e documentError) Unwrap() error { return e.cause }

func (e documentError) Is(target error) bool { return target == ErrDocument }
