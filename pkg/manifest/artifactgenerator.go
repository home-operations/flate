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

func (r *RawObject) artifactGeneratorCatalogSpec() map[string]any {
	fields := [...]string{"artifacts", "pathPattern"}
	spec := make(map[string]any, len(fields)+1)
	for _, field := range fields {
		found := false
		for key, value := range r.Spec {
			if strings.EqualFold(key, field) {
				spec[key] = value
				found = true
			}
		}
		if !found {
			spec[field] = nil
		}
	}
	if r.artifactGeneratorSpec().spec.ManagedNamespaces != nil {
		spec["managedNamespaces"] = map[string]any{}
	}
	return spec
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
	err := scanArtifactTemplate(template, func(literal, capture string) error {
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

func scanArtifactTemplate(template string, visit func(literal, capture string) error) error {
	rest := template
	for rest != "" {
		i := strings.IndexAny(rest, "{}")
		if i < 0 {
			return visit(rest, "")
		}
		offset := len(template) - len(rest) + i
		if rest[i] == '}' {
			return fmt.Errorf(
				"invalid template %q: capture ending at offset %d is missing opening brace",
				template,
				offset,
			)
		}
		name, tail, ok := strings.Cut(rest[i+1:], "}")
		if !ok {
			return fmt.Errorf(
				"invalid template %q: capture starting at offset %d is missing closing brace",
				template,
				offset,
			)
		}
		if name == "" {
			return fmt.Errorf("invalid template %q: empty capture variable", template)
		}
		if !validArtifactCapture(name) {
			return fmt.Errorf(
				"invalid template %q: capture variable %q must be a valid capture name matching [A-Za-z_][A-Za-z0-9_]*",
				template,
				name,
			)
		}
		if err := visit(rest[:i], name); err != nil {
			return err
		}
		rest = tail
	}
	return nil
}

type artifactToken struct {
	literal string
	capture int
}

const artifactMatchBudget = 4096

func (r *RawObject) artifactTargets(candidates []NamedResource) []ProducerTarget {
	spec := &r.artifactGeneratorSpec().spec
	var targets []ProducerTarget
	var captures []string
	pattern := spec.PathPattern
	if len(candidates) != 0 && pattern != "" {
		alias, patternPath, ok := strings.Cut(pattern, "/")
		if ok && strings.HasPrefix(alias, "@") && len(alias) > 1 {
			_, captures, _ = artifactTokens(patternPath, nil, true)
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

func artifactTokens(template string, captures []string, declare bool) ([]artifactToken, []string, bool) {
	var tokens []artifactToken
	err := scanArtifactTemplate(template, func(literal, capture string) error {
		if literal != "" {
			tokens = append(tokens, artifactToken{literal: literal})
		}
		if capture == "" {
			return nil
		}
		index := slices.Index(captures, capture)
		if declare {
			if index >= 0 || len(content.IsLabelKey(capture)) != 0 {
				return fmt.Errorf("invalid path capture %q", capture)
			}
			index = len(captures)
			captures = append(captures, capture)
		} else if index < 0 {
			return fmt.Errorf("undefined path capture %q", capture)
		}
		tokens = append(tokens, artifactToken{capture: index + 1})
		return nil
	})
	if err != nil {
		return nil, nil, false
	}
	return tokens, captures, true
}

// The shared budget bounds search after tokenization to O(4096 * (T + N + 63)).
// T is the token count and N is the candidate byte length; captures are at most 63 bytes.
func artifactMatch(tokens []artifactToken, name string, bound []string, budget *int) bool {
	if *budget == 0 {
		return false
	}
	*budget--
	if len(tokens) == 0 {
		return name == ""
	}
	if tail := tokens[len(tokens)-1].literal; tail != "" && !strings.HasSuffix(name, tail) {
		return false
	}
	token := tokens[0]
	if token.capture == 0 {
		rest, ok := strings.CutPrefix(name, token.literal)
		return ok && artifactMatch(tokens[1:], rest, bound, budget)
	}
	i := token.capture - 1
	if bound[i] != "" {
		rest, ok := strings.CutPrefix(name, bound[i])
		return ok && artifactMatch(tokens[1:], rest, bound, budget)
	}
	minimum, occurrences := 0, 1
	for _, next := range tokens[1:] {
		switch {
		case next.capture == 0:
			minimum += len(next.literal)
		case next.capture == token.capture:
			occurrences++
		case bound[next.capture-1] != "":
			minimum += len(bound[next.capture-1])
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
		if !yield(Reference{NamedResource: application.Source, Use: sourceKey, Content: true, bindsSource: true}) {
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
	add := func(value, variables string, cleanFirst bool) {
		input, name, found := strings.Cut(value, "/")
		input, marked := strings.CutPrefix(input, "@")
		if !found || !marked || input != alias {
			return
		}
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
		if alias != "artifact" {
			add(value, "{", true)
		}
	}
	add(s.PathPattern, "*?[{", false)
	for i := range s.Artifacts {
		for _, operation := range s.Artifacts[i].Copy {
			add(operation.From, "*?[{", false)
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
	tokens, _, ok := artifactTokens(template, captures, false)
	if !ok {
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

func validArtifactCapture(name string) bool {
	return !(name == "" || strings.IndexFunc(name, func(r rune) bool {
		return r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9')
	}) >= 0 || name[0] >= '0' && name[0] <= '9')
}
