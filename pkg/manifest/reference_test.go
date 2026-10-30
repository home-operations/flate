package manifest

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestReferenceMatcher_Matches(t *testing.T) {
	t.Parallel()
	provider := &ResourceSetInputProvider{Name: "images", Namespace: "apps", Labels: map[string]string{"role": "images"}}
	scope := provider.Named()
	scope.Name = ""
	selected := &metav1.LabelSelector{MatchLabels: map[string]string{"role": "images"}}
	invalid := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}
	otherNamespace, otherLabels, otherName := *provider, *provider, *provider
	otherNamespace.Namespace = "other"
	otherLabels.Labels = map[string]string{"role": "other"}
	otherName.Name = "other"
	for _, tc := range []struct {
		name    string
		ref     Reference
		obj     BaseManifest
		want    bool
		invalid bool
	}{
		{"named", Reference{NamedResource: provider.Named()}, provider, true, false},
		{"named_precedence", Reference{NamedResource: provider.Named(), Selector: invalid}, provider, true, false},
		{"named_other_name", Reference{NamedResource: provider.Named(), Selector: selected}, &otherName, false, false},
		{"selected", Reference{NamedResource: scope, Selector: selected}, provider, true, false},
		{"other_name_selected", Reference{NamedResource: scope, Selector: selected}, &otherName, true, false},
		{"namespace", Reference{NamedResource: scope, Selector: selected}, &otherNamespace, false, false},
		{"group", Reference{NamedResource: scope, Selector: selected}, &RawObject{APIVersion: "foreign.example/v1", Kind: scope.Kind, Namespace: scope.Namespace}, false, false},
		{"kind", Reference{NamedResource: scope, Selector: selected}, &ResourceSet{Namespace: scope.Namespace, Labels: provider.Labels}, false, false},
		{"labels", Reference{NamedResource: scope, Selector: selected}, &otherLabels, false, false},
		{"expression", Reference{NamedResource: scope, Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: metav1.LabelSelectorOpIn, Values: []string{"images"}}}}}, provider, true, false},
		{"empty_selector", Reference{NamedResource: scope, Selector: &metav1.LabelSelector{}}, &otherLabels, true, false},
		{"nil_selector", Reference{NamedResource: scope}, provider, false, false},
		{"invalid", Reference{NamedResource: scope, Selector: invalid}, provider, false, true},
		{"invalid_empty_inventory", Reference{NamedResource: scope, Selector: invalid}, nil, false, true},
		{"empty_inventory", Reference{NamedResource: scope, Selector: selected}, nil, false, false},
		{"all_kinds", Reference{Namespace: scope.Namespace, Selector: selected}, provider, true, false},
		{"core_group", Reference{Kind: scope.Kind, Namespace: scope.Namespace, Selector: selected}, provider, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			matcher, err := tc.ref.Matcher(tc.ref.Selector)
			if tc.invalid {
				assert.Equal(t, errors.Is(err, ErrInput), true)
				return
			}
			assert.NoError(t, err)
			var id NamedResource
			var values map[string]string
			if tc.obj != nil {
				id = tc.obj.Named()
				if labeled, ok := tc.obj.(interface{ GetLabels() map[string]string }); ok {
					values = labeled.GetLabels()
				}
			}
			assert.Equal(t, matcher.Matches(id, values), tc.want)
		})
	}
}
