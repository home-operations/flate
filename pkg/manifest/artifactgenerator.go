package manifest

import (
	"cmp"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"k8s.io/apimachinery/pkg/api/validate/content"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/core/artifactsyntax"
)

// ExternalArtifact constructs a declaration after evaluation resolves its output metadata.
//
//nolint:gocritic // The SDK preserves this value input and its snapshot boundary.
func (r *RawObject) ExternalArtifact(metadata metav1.ObjectMeta) *ExternalArtifact {
	return newExternalArtifact(sourcev1.ExternalArtifact{ObjectMeta: metadata, Spec: sourcev1.ExternalArtifactSpec{
		SourceRef: &meta.NamespacedObjectKindReference{
			APIVersion: r.APIVersion,
			Kind:       r.Kind,
			Name:       r.Name,
			Namespace:  r.Namespace,
		},
	}})
}

func (o *ArtifactGeneratorOutput) identityTemplate(namespace string) NamedResource {
	return NamedResource{
		Group: SourceDomain, Kind: KindExternalArtifact,
		Namespace: cmp.Or(o.Namespace, namespace), Name: o.Name,
	}
}

func (o *ArtifactGeneratorOutput) needsCandidates() bool {
	return strings.ContainsAny(o.Name, "{}") || strings.ContainsAny(o.Namespace, "{}")
}

// ResolvedArtifactGeneratorOutput carries the authored output with its expansion results.
type ResolvedArtifactGeneratorOutput struct {
	Artifact  NamedResource
	Namespace NamedResource
	Output    ArtifactGeneratorOutput

	// A Namespace metadata path error is deferred until after artifact preparation.
	NamespaceMetadataPath      string
	NamespaceMetadataPathError error
}

// ResolveOutput uses normalized captures for identities and raw captures for file paths.
// Nil captures retain templates for declaration validation.
func (s *ArtifactGeneratorSpec) ResolveOutput(
	o *ArtifactGeneratorOutput, namespace string, normalized, raw map[string]string,
) (ResolvedArtifactGeneratorOutput, error) {
	result := ResolvedArtifactGeneratorOutput{Artifact: o.identityTemplate(namespace), Output: *o}
	if normalized != nil {
		if err := result.expand(normalized, raw); err != nil {
			return ResolvedArtifactGeneratorOutput{}, err
		}
	}
	result.Namespace = s.namespaceTarget(o, result.Artifact.Namespace, namespace)
	if result.Namespace.Name != "" {
		result.NamespaceMetadataPath = s.namespaceMetadataPath()
		if normalized != nil {
			result.NamespaceMetadataPath, result.NamespaceMetadataPathError = expandArtifactTemplate(
				result.NamespaceMetadataPath, raw,
			)
		}
	}
	return result, nil
}

func (r *ResolvedArtifactGeneratorOutput) expand(normalized, raw map[string]string) error {
	var err error
	r.Artifact.Name, err = expandArtifactTemplate(r.Artifact.Name, normalized)
	if err != nil {
		return err
	}
	if r.Output.Namespace != "" {
		r.Artifact.Namespace, err = expandArtifactTemplate(r.Artifact.Namespace, normalized)
		if err != nil {
			return err
		}
	}
	r.Output.InputsFrom, err = expandArtifactTemplate(r.Output.InputsFrom, raw)
	if err != nil {
		return err
	}
	r.Output.Copy = slices.Clone(r.Output.Copy)
	for i := range r.Output.Copy {
		operation := &r.Output.Copy[i]
		operation.From, err = expandArtifactTemplate(operation.From, raw)
		if err != nil {
			return err
		}
		operation.To, err = expandArtifactTemplate(operation.To, raw)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ArtifactGeneratorSpec) namespaceTarget(
	o *ArtifactGeneratorOutput, target, ownerNamespace string,
) NamedResource {
	if s.ManagedNamespaces == nil || o.Namespace == "" || !managedNamespaceEligible(target, ownerNamespace) {
		return NamedResource{}
	}
	return NamedResource{Kind: namespaceKind, Name: target}
}

func managedNamespaceEligible(target, ownerNamespace string) bool {
	return target != "" && target != ownerNamespace
}

func expandArtifactTemplate(template string, captures map[string]string) (string, error) {
	if !strings.ContainsAny(template, "{}") {
		return template, nil
	}
	var out strings.Builder
	_, err := artifactsyntax.ScanTemplate(template, func(literal, capture string) error {
		out.WriteString(literal)
		if capture == "" {
			return nil
		}
		value, ok := captures[capture]
		if !ok {
			return fmt.Errorf(
				"invalid template %q: capture variable %q is not defined by pathPattern",
				template,
				capture,
			)
		}
		out.WriteString(value)
		return nil
	})
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

const artifactMatchBudget = 4096

func (r *RawObject) artifactTargets(candidates []NamedResource) []ProducerTarget {
	spec := &r.artifactGeneratorSpec().spec
	var targets []ProducerTarget
	var captures []string
	if len(candidates) != 0 {
		pattern := artifactsyntax.ParseSelector(spec.PathPattern)
		if pattern.HasPath && pattern.Alias != "" {
			var scratch [8]artifactsyntax.Token
			_, captures, _ = artifactsyntax.Tokens(pattern.Path, nil, scratch[:0], true)
		}
	}
	for i := range spec.Artifacts {
		output := &spec.Artifacts[i]
		if output.Name == "" {
			continue
		}
		id := output.identityTemplate(r.Namespace)
		targets = r.appendArtifactTargets(targets, id, candidates, captures)
		if namespace := spec.namespaceTarget(output, id.Namespace, r.Namespace); namespace.Name != "" {
			targets = r.appendArtifactTargets(targets, namespace, candidates, captures)
		}
	}
	slices.SortFunc(targets, func(a, b ProducerTarget) int { return a.Compare(b.NamedResource) })
	return slices.CompactFunc(targets, func(a, b ProducerTarget) bool { return a.NamedResource == b.NamedResource })
}

// The shared budget bounds search after tokenization to O(4096 * (T + N + 63)).
// T is the token count and N is the candidate byte length; captures are at most 63 bytes.
func artifactMatch(tokens []artifactsyntax.Token, name string, bound []string, budget *int) bool {
	if *budget == 0 {
		return false
	}
	*budget--
	if len(tokens) == 0 {
		return name == ""
	}
	if tail := tokens[len(tokens)-1].Literal; tail != "" && !strings.HasSuffix(name, tail) {
		return false
	}
	token := tokens[0]
	if token.Capture == 0 {
		rest, ok := strings.CutPrefix(name, token.Literal)
		return ok && artifactMatch(tokens[1:], rest, bound, budget)
	}
	i := token.Capture - 1
	if bound[i] != "" {
		rest, ok := strings.CutPrefix(name, bound[i])
		return ok && artifactMatch(tokens[1:], rest, bound, budget)
	}
	minimum, occurrences := 0, 1
	for _, next := range tokens[1:] {
		switch {
		case next.Capture == 0:
			minimum += len(next.Literal)
		case next.Capture == token.Capture:
			occurrences++
		case bound[next.Capture-1] != "":
			minimum += len(bound[next.Capture-1])
		default:
			minimum++
		}
	}
	for end := 1; end <= min(maxArtifactCaptureLength, (len(name)-minimum)/occurrences); end++ {
		if *budget == 0 {
			break
		}
		*budget--
		value := name[:end]
		if len(content.IsLabelValue(value)) != 0 {
			continue
		}
		bound[i] = value
		if artifactMatch(tokens[1:], name[end:], bound, budget) {
			return true
		}
	}
	bound[i] = ""
	return false
}

func (r *RawObject) references(yield func(Reference) bool) {
	if application, valid := r.EntryApplication(); valid {
		if !yield(Reference{
			NamedResource: application.Source, Use: sourceKey, Content: true, sourceRoles: bindsSource,
		}) {
			return
		}
	}
	if !r.isArtifactGenerator() {
		return
	}
	decoded := r.artifactGeneratorSpec()
	for i, source := range decoded.sources {
		id := Resolve(
			NamedResource{
				Group:     referenceGroup(source.Kind, ""),
				Kind:      source.Kind,
				Name:      source.Name,
				Namespace: source.Namespace,
			},
			"",
			r.Namespace,
		)
		files := decoded.spec.inputPaths(source.Alias)
		if !yield(
			Reference{
				NamedResource:    id,
				Use:              artifactKey,
				Position:         i,
				NamespaceDefault: source.Namespace == "",
				Files:            files,
				Subtree:          len(files) != 0,
				Content:          true,
				Gate:             GateArtifacts,
				Artifact:         true,
				ArtifactOnly:     true,
				ArtifactStages:   GateArtifacts,
			},
		) {
			return
		}
	}
}

func (s *ArtifactGeneratorSpec) inputPaths(alias string) []string {
	if alias == "" {
		return nil
	}
	var files []string
	add := func(selector artifactsyntax.Selector, variables string, cleanFirst bool) {
		if !selector.HasPath || selector.Alias != alias {
			return
		}
		name := selector.Path
		// Variable components describe potential reads, including absent matches on either diff side.
		if cleanFirst {
			name = path.Clean(name)
		}
		if variable := strings.IndexAny(name, variables); variable >= 0 {
			name = path.Dir(name[:variable])
		}
		name = path.Clean(name)
		if !slices.Contains(files, name) {
			files = append(files, name)
		}
	}
	addFile := func(value string) {
		selector := artifactsyntax.ParseSelector(value)
		if !selector.StagedFile() {
			add(selector, "{", true)
		}
	}
	add(artifactsyntax.ParseSelector(s.PathPattern), "*?[{", false)
	for i := range s.Artifacts {
		for _, operation := range s.Artifacts[i].Copy {
			add(artifactsyntax.ParseSelector(operation.From), "*?[{", false)
		}
		addFile(s.Artifacts[i].InputsFrom)
	}
	addFile(s.namespaceMetadataPath())
	slices.Sort(files)
	return files
}

const (
	maxArtifactCaptureLength = 63
)

func (r *RawObject) appendArtifactTargets(
	targets []ProducerTarget, id NamedResource, candidates []NamedResource, captures []string,
) []ProducerTarget {
	dynamicNamespace := id.Namespace != r.Namespace && strings.ContainsAny(id.Namespace, "{}")
	if !dynamicNamespace && !strings.ContainsAny(id.Name, "{}") {
		return append(targets, ProducerTarget{NamedResource: id})
	}
	if len(captures) == 0 {
		return targets
	}
	template := id.Name
	if dynamicNamespace {
		// A capture is a label value and can never consume the identity separator.
		template = id.Namespace + "/" + id.Name
	}
	var scratch [8]artifactsyntax.Token
	tokens, _, err := artifactsyntax.Tokens(template, captures, scratch[:0], false)
	if err != nil {
		return targets
	}
	validateName := apivalidation.NameIsDNSSubdomain
	isNamespace := id.Kind == namespaceKind
	if isNamespace {
		validateName = apivalidation.ValidateNamespaceName
	}
	bound := make([]string, len(captures))
	for _, candidate := range candidates {
		switch {
		case candidate.Group != id.Group || candidate.Kind != id.Kind,
			!dynamicNamespace && candidate.Namespace != id.Namespace,
			len(validateName(candidate.Name, false)) != 0,
			isNamespace && !managedNamespaceEligible(candidate.Name, r.Namespace):
			continue
		}
		name := candidate.Name
		if dynamicNamespace {
			if len(apivalidation.ValidateNamespaceName(candidate.Namespace, false)) != 0 {
				continue
			}
			name = candidate.Namespace + "/" + name
		}
		clear(bound)
		budget := artifactMatchBudget
		if artifactMatch(tokens, name, bound, &budget) {
			targets = append(targets, ProducerTarget{NamedResource: candidate})
		}
	}
	return targets
}
