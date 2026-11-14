package manifest

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klabels "k8s.io/apimachinery/pkg/labels"
)

// ReferenceMatcher matches a named identity or a compiled label selector.
type ReferenceMatcher struct {
	id       NamedResource
	selector klabels.Selector
}

// Matcher compiles reference matching while preserving named-reference precedence.
// Empty group and kind permit selector queries across the full inventory.
func (r NamedResource) Matcher(selector *metav1.LabelSelector) (ReferenceMatcher, error) {
	matcher := ReferenceMatcher{id: r}
	if r.Name != "" || selector == nil {
		return matcher, nil
	}
	compiled, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return ReferenceMatcher{}, NewError(ErrInput, fmt.Errorf("invalid reference selector: %w", err), "")
	}
	matcher.selector = compiled
	return matcher, nil
}

// Matches reports whether the identity and labels satisfy the reference.
func (m ReferenceMatcher) Matches(id NamedResource, values map[string]string) bool {
	if m.id.Name != "" {
		return id == m.id
	}
	if m.selector == nil || id.Namespace != m.id.Namespace ||
		(m.id.Group != "" || m.id.Kind != "") && (id.Group != m.id.Group || id.Kind != m.id.Kind) {
		return false
	}
	return m.selector.Matches(klabels.Set(values))
}
