package loader

import (
	"maps"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

func identitySubstituted(id manifest.NamedResource) bool {
	return manifest.HasEnvsubstReference(id.Name) || manifest.HasEnvsubstReference(id.Namespace)
}

func selectorSubstituted(selector *metav1.LabelSelector) bool {
	if selector == nil {
		return false
	}
	if labelsSubstituted(selector.MatchLabels) {
		return true
	}
	for _, expression := range selector.MatchExpressions {
		if manifest.HasEnvsubstReference(expression.Key) ||
			manifest.HasEnvsubstReference(string(expression.Operator)) ||
			slices.ContainsFunc(expression.Values, manifest.HasEnvsubstReference) {
			return true
		}
	}
	return false
}

func referenceSubstituted(ref *manifest.Reference) bool {
	if identitySubstituted(ref.NamedResource) || manifest.HasEnvsubstReference(ref.Key) ||
		manifest.HasEnvsubstReference(ref.ChartPath) {
		return true
	}
	return slices.ContainsFunc(ref.Files, manifest.HasEnvsubstReference) || selectorSubstituted(ref.Selector)
}

func labelsSubstituted(labels map[string]string) bool {
	for key, value := range labels {
		if manifest.HasEnvsubstReference(key) || manifest.HasEnvsubstReference(value) {
			return true
		}
	}
	return false
}

func targetsSubstituted(targets []manifest.ProducerTarget) bool {
	for _, target := range targets {
		if identitySubstituted(target.NamedResource) ||
			slices.ContainsFunc(target.DeclaredKeys, manifest.HasEnvsubstReference) {
			return true
		}
	}
	return false
}

func (f *occurrenceFacts) hasSubstitutions(id manifest.NamedResource) bool {
	if identitySubstituted(id) || identitySubstituted(f.source) || manifest.HasEnvsubstReference(f.path) ||
		slices.ContainsFunc(f.roots, manifest.HasEnvsubstReference) {
		return true
	}
	if labelsSubstituted(f.labels) || targetsSubstituted(f.targets) ||
		f.policy != nil && manifest.HasEnvsubstReference(f.policy.Namespace) {
		return true
	}
	for i := range f.refs {
		if referenceSubstituted(&f.refs[i]) {
			return true
		}
	}
	for i := range f.templateInputs {
		input := &f.templateInputs[i]
		if identitySubstituted(input.Reader) || referenceSubstituted(&input.Reference) {
			return true
		}
	}
	return slices.ContainsFunc(f.templateObjects, func(object manifest.BaseManifest) bool {
		return identitySubstituted(object.Named())
	})
}

func projectStrings(values []string, resolve func(string) string) []string {
	if !slices.ContainsFunc(values, manifest.HasEnvsubstReference) {
		return values
	}
	values = slices.Clone(values)
	for i, value := range values {
		values[i] = resolve(value)
	}
	return values
}

func projectLabels(labels map[string]string, resolve func(string) string) map[string]string {
	if !labelsSubstituted(labels) {
		return labels
	}
	projected := make(map[string]string, len(labels))
	for _, key := range slices.Sorted(maps.Keys(labels)) {
		projected[resolve(key)] = resolve(labels[key])
	}
	return projected
}

func projectReference(ref *manifest.Reference, resolve func(string) string) manifest.Reference {
	if !referenceSubstituted(ref) {
		return *ref
	}
	projected := ref.Clone()
	projected.Name, projected.Namespace = resolve(ref.Name), resolve(ref.Namespace)
	projected.Key, projected.ChartPath = resolve(ref.Key), resolve(ref.ChartPath)
	for i, file := range ref.Files {
		projected.Files[i] = resolve(file)
	}
	if selector := projected.Selector; selector != nil {
		selector.MatchLabels = projectLabels(selector.MatchLabels, resolve)
		for i := range selector.MatchExpressions {
			expression := &selector.MatchExpressions[i]
			expression.Key = resolve(expression.Key)
			expression.Operator = metav1.LabelSelectorOperator(resolve(string(expression.Operator)))
			for j, value := range expression.Values {
				expression.Values[j] = resolve(value)
			}
		}
	}
	return projected
}

func projectReferences(refs []manifest.Reference, resolve func(string) string) []manifest.Reference {
	projected := refs
	copied := false
	for i := range refs {
		if !referenceSubstituted(&refs[i]) {
			continue
		}
		if !copied {
			projected, copied = slices.Clone(refs), true
		}
		projected[i] = projectReference(&refs[i], resolve)
	}
	return projected
}

func projectTargets(targets []manifest.ProducerTarget, resolve func(string) string) []manifest.ProducerTarget {
	if !targetsSubstituted(targets) {
		return targets
	}
	targets = slices.Clone(targets)
	for i := range targets {
		target := &targets[i]
		target.Name, target.Namespace = resolve(target.Name), resolve(target.Namespace)
		target.DeclaredKeys = projectStrings(target.DeclaredKeys, resolve)
	}
	return targets
}
