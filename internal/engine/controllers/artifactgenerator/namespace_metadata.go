package artifactgenerator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

type namespaceTargets struct {
	ordered []*request
	first   map[string]*request
}

func collectNamespaceTargets(requests []request) namespaceTargets {
	targets := namespaceTargets{first: make(map[string]*request)}
	for i := range requests {
		req := &requests[i]
		name := req.Namespace.Name
		if name != "" && targets.first[name] == nil {
			targets.first[name] = req
			targets.ordered = append(targets.ordered, req)
		}
	}
	return targets
}

func (t namespaceTargets) validate(
	ctx context.Context,
	policies *namespacePolicies,
	requests []request,
	source *manifest.ArtifactGeneratorNamespaceMetadataSource,
) error {
	for _, req := range t.ordered {
		if err := policies.validateName(ctx, req.Namespace.Name); err != nil {
			return err
		}
	}
	if source == nil {
		return nil
	}
	for i := range requests {
		req := &requests[i]
		name := req.Namespace.Name
		if name == "" {
			continue
		}
		if err := req.NamespaceMetadataPathError; err != nil {
			return fmt.Errorf(
				"namespace reconciliation failed: invalid namespace metadata path %q: %w",
				source.Path,
				err,
			)
		}
		if first := t.first[name].NamespaceMetadataPath; first != req.NamespaceMetadataPath {
			return fmt.Errorf(
				"namespace reconciliation failed: namespace metadata path %q resolves to %q and %q "+
					"for different artifacts targeting namespace %q, it must only use namespace-level captures",
				source.Path, first, req.NamespaceMetadataPath, name,
			)
		}
	}
	return nil
}

type namespaceMetadataValues struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

func (p *namespacePolicies) decodeMetadata(
	ctx context.Context,
	path string,
	data []byte,
) (namespaceMetadataValues, error) {
	var doc struct {
		metav1.TypeMeta `json:",inline"`

		Metadata namespaceMetadataValues `json:"metadata"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return namespaceMetadataValues{}, fmt.Errorf("failed to parse namespace metadata file %q: %w", path, err)
	}
	const metadataVersion = manifest.SourceExtensionsDomain + "/v1beta1"
	if doc.APIVersion != "" && doc.APIVersion != metadataVersion {
		return namespaceMetadataValues{}, fmt.Errorf(
			"namespace metadata file %q has unsupported apiVersion %q",
			path,
			doc.APIVersion,
		)
	}
	if doc.Kind != "" && doc.Kind != "NamespaceMetadata" {
		return namespaceMetadataValues{}, fmt.Errorf(
			"namespace metadata file %q has unsupported kind %q",
			path,
			doc.Kind,
		)
	}
	labels, err := filterNamespaceMetadata(ctx, doc.Metadata.Labels, p.labels, "label")
	if err != nil {
		return namespaceMetadataValues{}, fmt.Errorf("namespace metadata file %q: %w", path, err)
	}
	annotations, err := filterNamespaceMetadata(ctx, doc.Metadata.Annotations, p.annotations, "annotation")
	if err != nil {
		return namespaceMetadataValues{}, fmt.Errorf("namespace metadata file %q: %w", path, err)
	}
	return namespaceMetadataValues{Labels: labels, Annotations: annotations}, nil
}

func filterNamespaceMetadata(
	ctx context.Context,
	values map[string]string,
	allowed map[string]stringAssertion,
	kind string,
) (map[string]string, error) {
	if len(allowed) == 0 {
		return nil, nil
	}
	var filtered map[string]string
	for _, key := range slices.Sorted(maps.Keys(values)) {
		expr, ok := allowed[key]
		if !ok {
			continue
		}
		value := values[key]
		accepted, err := expr.evaluate(ctx, map[string]any{metadataVariable: value})
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate the allowed %s expression for key %q: %w", kind, key, err)
		}
		if !accepted {
			return nil, fmt.Errorf(
				"%s %q with value %q is rejected by the allowed expression %q",
				kind,
				key,
				value,
				expr.expression,
			)
		}
		if filtered == nil {
			filtered = make(map[string]string)
		}
		filtered[key] = value
	}
	return filtered, nil
}

func namespaceMetadataFor(
	spec *manifest.ArtifactGeneratorSpec,
	name, uid string,
	sourced namespaceMetadataValues,
) (metav1.ObjectMeta, error) {
	meta := metav1.ObjectMeta{
		Name:        name,
		Labels:      mergeMetadata(maps.Clone(spec.CommonMetadata.Labels), sourced.Labels, true),
		Annotations: mergeMetadata(maps.Clone(spec.CommonMetadata.Annotations), sourced.Annotations, true),
	}
	if policy := spec.ManagedNamespaces.Metadata; policy != nil {
		for i := range policy.From {
			entry := &policy.From[i]
			if entry.Namespace != "*" && entry.Namespace != name {
				continue
			}
			switch entry.Strategy {
			case "Reset":
				meta.Labels, meta.Annotations = maps.Clone(entry.Labels), maps.Clone(entry.Annotations)
			default:
				override := entry.Strategy == "Override"
				meta.Labels = mergeMetadata(meta.Labels, entry.Labels, override)
				meta.Annotations = mergeMetadata(meta.Annotations, entry.Annotations, override)
			}
		}
	}
	meta.Labels = ownGeneratorLabels(meta.Labels, uid)
	if err := validateResolvedNamespaceMetadata(&meta); err != nil {
		return metav1.ObjectMeta{}, err
	}
	return meta, nil
}

func mergeMetadata(dst, src map[string]string, override bool) map[string]string {
	if dst == nil {
		return maps.Clone(src)
	}
	for k, v := range src {
		if _, exists := dst[k]; override || !exists {
			dst[k] = v
		}
	}
	return dst
}

func ownGeneratorLabels(labels map[string]string, uid string) map[string]string {
	if labels == nil {
		labels = make(map[string]string, generatedLabelCount)
	}
	labels["app.kubernetes.io/managed-by"], labels["source.extensions.fluxcd.io/generator"] = "source-watcher", uid
	return labels
}

func validateResolvedNamespaceMetadata(meta *metav1.ObjectMeta) error {
	const externalFinalizer = manifest.SourceExtensionsDomain + "/externalFinalizer"
	if meta.Annotations[externalFinalizer] != "" {
		return fmt.Errorf(
			"metadata for namespace %q must not set the reserved %q annotation, it is reserved for external finalizers",
			meta.Name, externalFinalizer,
		)
	}
	errs := apivalidation.ValidateObjectMeta(meta, false, apivalidation.NameIsDNSLabel, field.NewPath("metadata"))
	if len(errs) == 0 {
		return nil
	}
	// Keep Kubernetes field order and stabilize the diagnostics within each map field.
	for start := 0; start < len(errs); {
		end := start + 1
		for end < len(errs) && errs[end].Field == errs[start].Field {
			end++
		}
		slices.SortFunc(errs[start:end], func(a, b *field.Error) int {
			return strings.Compare(a.Error(), b.Error())
		})
		start = end
	}
	return fmt.Errorf("invalid metadata for namespace %q: %w", meta.Name, errs.ToAggregate())
}

func (p *generatorPublication) prepareNamespaces(
	ctx context.Context,
	docs []manifest.DocOccurrence,
	targets namespaceTargets,
	requests []request,
) error {
	var source *manifest.ArtifactGeneratorNamespaceMetadataSource
	if metadata := p.spec.ManagedNamespaces.Metadata; metadata != nil {
		source = metadata.FromSource
	}
	if err := targets.validate(ctx, p.policies, requests, source); err != nil {
		return err
	}
	for i, req := range targets.ordered {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		doc, err := p.namespaceDocument(ctx, req, source != nil)
		if err != nil {
			return fmt.Errorf("namespace reconciliation failed: %w", err)
		}
		docs[i] = manifest.DocOccurrence{Document: doc, Ordinal: i}
	}
	return nil
}

func (p *generatorPublication) namespaceDocument(
	ctx context.Context, req *request, readMetadata bool,
) (map[string]any, error) {
	var sourced namespaceMetadataValues
	if readMetadata {
		data, err := p.evaluation.namespaceMetadata(ctx, req)
		if err != nil {
			return nil, err
		}
		sourced, err = p.policies.decodeMetadata(ctx, req.NamespaceMetadataPath, data)
		if err != nil {
			return nil, err
		}
	}
	meta, err := namespaceMetadataFor(p.spec, req.Namespace.Name, p.uid, sourced)
	if err != nil {
		return nil, err
	}
	metadata := map[string]any{"name": meta.Name}
	for field, values := range map[string]map[string]string{"labels": meta.Labels, "annotations": meta.Annotations} {
		if len(values) != 0 {
			decoded := make(map[string]any, len(values))
			for key, value := range values {
				decoded[key] = value
			}
			metadata[field] = decoded
		}
	}
	return map[string]any{"apiVersion": "v1", "kind": req.Namespace.Kind, "metadata": metadata}, nil
}

func (e *evaluation) namespaceMetadata(ctx context.Context, req *request) ([]byte, error) {
	if !strings.HasPrefix(req.NamespaceMetadataPath, "@"+artifactAlias+"/") {
		return e.readSourceFile(ctx, req.NamespaceMetadataPath, nil, namespaceFile)
	}
	if req.stagedMetadataError != nil {
		return nil, req.stagedMetadataError
	}
	if failure := req.stagedMetadata.Failure; failure != "" {
		return nil, errors.New(failure)
	}
	return req.stagedMetadata.Data, nil
}
