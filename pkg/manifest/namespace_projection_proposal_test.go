package manifest

import "testing"

func TestEmissionPolicy_MatchesNamespace(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy EmissionPolicy
		target NamedResource
		want   bool
	}{
		{"same", EmissionPolicy{Namespace: "apps"}, NamedResource{Kind: KindSecret, Namespace: "apps"}, true},
		{"foreign", EmissionPolicy{Namespace: "other"}, NamedResource{Kind: KindSecret, Namespace: "apps"}, false},
		{"unset", EmissionPolicy{}, NamedResource{Kind: KindSecret, Namespace: "apps"}, true},
		{"expression", EmissionPolicy{Namespace: "${TARGET}"}, NamedResource{Kind: KindSecret, Namespace: "apps"}, true},
		{"unknown_target", EmissionPolicy{Namespace: "other"}, NamedResource{Kind: KindSecret}, true},
		{"cluster_scoped", EmissionPolicy{Namespace: "other"}, NamedResource{Kind: "Namespace", Name: "apps"}, true},
		{"internal", EmissionPolicy{Namespace: "other", InternalNamespace: true}, NamedResource{Kind: KindSecret, Namespace: "apps"}, true},
		{"excluded_children", EmissionPolicy{Namespace: "other", ExcludeChildren: true}, NamedResource{Kind: KindSecret, Namespace: "apps"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.policy.MatchesNamespace(tc.target); got != tc.want {
				t.Fatalf("MatchesNamespace = %t, want %t", got, tc.want)
			}
		})
	}
}
