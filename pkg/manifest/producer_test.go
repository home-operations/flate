package manifest

import (
	"reflect"
	"strings"
	"testing"
)

// secret is a small helper so each case reads as identity + declared keys.
func secret(ns, name string, keys ...string) ProducerTarget {
	return ProducerTarget{
		Kind: KindSecret, Namespace: ns, Name: name,
		DeclaredKeys: keys,
	}
}

func TestProducerTargets(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		raw  *RawObject
		want []ProducerTarget
	}{
		{
			name: "ExternalSecret explicit target.name",
			raw: &RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"target": map[string]any{"name": "app-values"}}},
			want: []ProducerTarget{secret("default", "app-values")},
		},
		{
			name: "ExternalSecret no target falls back to own name",
			raw:  &RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Namespace: "staging", Name: "my-secret", Spec: map[string]any{}},
			want: []ProducerTarget{secret("staging", "my-secret")},
		},
		{
			name: "ExternalSecret template.data declares the output keys",
			raw: &RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"target": map[string]any{"template": map[string]any{
					"data": map[string]any{"PASSWORD": "{{ .pw }}", "USERNAME": "{{ .user }}"}}}}},
			want: []ProducerTarget{secret("default", "app-creds", "PASSWORD", "USERNAME")},
		},
		{
			name: "ExternalSecret without template declares data[].secretKey",
			raw: &RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"data": []any{
					map[string]any{"secretKey": "TOKEN"},
					map[string]any{"secretKey": "API_KEY"},
				}}},
			want: []ProducerTarget{secret("default", "app-creds", "API_KEY", "TOKEN")},
		},
		{
			name: "ExternalSecret dataFrom-only declares nothing",
			raw: &RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Namespace: "default", Name: "app-creds",
				Spec: map[string]any{"dataFrom": []any{map[string]any{"extract": map[string]any{"key": "vault/app"}}}}},
			want: []ProducerTarget{secret("default", "app-creds")},
		},
		{
			name: "SealedSecret template.metadata.name + encryptedData keys",
			raw: &RawObject{Kind: "SealedSecret", APIVersion: "bitnami.com/v1alpha1", Namespace: "prod", Name: "sealed",
				Spec: map[string]any{
					"template":      map[string]any{"metadata": map[string]any{"name": "sealed-db"}},
					"encryptedData": map[string]any{"password": "AgB...", "username": "AgC..."}}},
			want: []ProducerTarget{secret("prod", "sealed-db", "password", "username")},
		},
		{
			name: "SealedSecret no template falls back to own name, no keys",
			raw:  &RawObject{Kind: "SealedSecret", APIVersion: "bitnami.com/v1alpha1", Namespace: "prod", Name: "sealed-db", Spec: map[string]any{}},
			want: []ProducerTarget{secret("prod", "sealed-db")},
		},
		{
			name: "ObjectBucketClaim produces a Secret and a ConfigMap named after the claim",
			raw:  &RawObject{Kind: "ObjectBucketClaim", APIVersion: "objectbucket.io/v1alpha1", Namespace: "default", Name: "netbox-obc"},
			want: []ProducerTarget{
				secret("default", "netbox-obc"),
				{Kind: KindConfigMap, Namespace: "default", Name: "netbox-obc"},
			},
		},
		{
			name: "non-producer kind",
			raw:  &RawObject{Kind: "Certificate", Namespace: "default", Name: "tls"},
			want: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := ProducerTargets(c.raw); !reflect.DeepEqual(got, c.want) {
				t.Errorf("targets = %#v, want %#v", got, c.want)
			}
		})
	}
}

func TestProducerTargets_ForeignAPIGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, group string }{
		{"ExternalSecret", "external-secrets.io"},
		{"SealedSecret", "bitnami.com"},
		{"ObjectBucketClaim", "objectbucket.io"},
		{"ArtifactGenerator", "source.extensions.fluxcd.io"},
	} {
		for _, group := range []string{"example.com/v1", tc.group + ".example.com/v1", "", "v1"} {
			t.Run(tc.kind+"/"+group, func(t *testing.T) {
				t.Parallel()
				raw := &RawObject{Kind: tc.kind, APIVersion: group, Name: "producer", Namespace: "apps", Spec: map[string]any{"artifacts": []any{map[string]any{"name": "output"}}}}
				if got := ProducerTargets(raw); len(got) != 0 {
					t.Errorf("foreign producer targets = %v, want none", got)
				}
			})
		}
	}
}

func TestProducerTargets_TypedManifests(t *testing.T) {
	t.Parallel()

	for _, obj := range []BaseManifest{&Kustomization{}, &HelmRelease{}, &HelmRepository{}, &HelmChartSource{}, &GitRepository{}, &OCIRepository{}, &ExternalArtifact{}, &Bucket{}, &ResourceSet{}, &ResourceSetInputProvider{}, &ConfigMap{}, &Secret{}} {
		if got := ProducerTargets(obj); got != nil {
			t.Fatalf("%T contributes cluster-only targets: %v", obj, got)
		}
	}
}

func TestProducerTargets_ArtifactGenerator(t *testing.T) {
	t.Parallel()
	ea := NamedResource{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "apps", Name: "api-prod"}
	for _, tc := range []struct {
		name, pattern, template, candidate string
		want                               bool
	}{
		{"literal", "", "api-prod", "", true},
		{"dynamic", "@repo/apps/{app}/{env}", "{app}-{env}", "api-prod", true},
		{"no_candidates", "@repo/{app}", "{app}", "", false},
		{"literal_mismatch", "@repo/{app}", "app-{app}", "api-prod", false},
		{"empty_capture", "@repo/{app}", "api-{app}", "api-", false},
		{"repeated", "@repo/{app}", "{app}-{app}", "api-api", true},
		{"alternate_split", "@repo/{app}", "{app}-{app}", "api-v2-api-v2", true},
		{"unequal_repeated", "@repo/{app}", "{app}-{app}", "api-web", false},
		{"adjacent", "@repo/{app}/{env}", "{app}{env}-{app}", "apiprod-api", true},
		{"adjacent_repeated", "@repo/{app}", "{app}{app}", "apiapi", true},
		{"search_budget", "@repo/{a}/{b}/{c}/{d}/{e}/{f}/{g}/{h}/{i}/{j}", "{a}{b}{c}{d}{e}{f}{g}{h}{i}{j}{a}", strings.Repeat("a", 62) + "b", false},
		{"search_budget_late_match", "@repo/{a}/{b}/{c}", "{a}{b}{c}{a}", strings.Repeat("a", 12) + "bcc" + strings.Repeat("a", 12) + "b", false},
		{"undeclared", "@repo/{app}", "{env}", "prod", false},
		{"duplicate_pattern", "@repo/{app}/{app}", "{app}", "api", false},
		{"missing_pattern_close", "@repo/{app", "{app}", "api", false},
		{"missing_pattern_open", "@repo/app}", "{app}", "api", false},
		{"missing_alias", "apps/{app}", "{app}", "api", false},
		{"empty_alias", "@/{app}", "{app}", "api", false},
		{"invalid_capture", "@repo/{app-name}", "{app-name}", "api", false},
		{"digit_capture", "@repo/{1app}", "{1app}", "api", false},
		{"invalid_label_key", "@repo/{_app}", "{_app}", "api", false},
		{"missing_name_close", "@repo/{app}", "{app", "api", false},
		{"missing_name_open", "@repo/{app}", "app}", "api", false},
		{"empty_name_capture", "@repo/{app}", "{}", "api", false},
		{"invalid_candidate", "@repo/{app}", "{app}", "API", false},
		{"invalid_capture_value", "@repo/{app}", "x{app}x", "x-api-x", false},
		{"empty_name", "", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ea := ea
			raw := &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator",
				Spec: map[string]any{"pathPattern": tc.pattern, "artifacts": []any{map[string]any{"name": tc.template}}}}
			var candidates []NamedResource
			if tc.candidate != "" {
				ea.Name = tc.candidate
				candidates = []NamedResource{ea}
			}
			got := ProducerTargets(raw, candidates...)
			var want []ProducerTarget
			if tc.want {
				ea.Name = tc.template
				if tc.candidate != "" {
					ea.Name = tc.candidate
				}
				want = []ProducerTarget{{NamedResource: ea}}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("targets = %#v, want %#v", got, want)
			}
		})
	}
}

func TestArtifactMatch_Budget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, template, candidate string
		budget                    int
		want                      bool
	}{
		{"exhausted", "{a}{b}{c}{d}{e}{f}{g}{h}{i}{j}{a}", strings.Repeat("a", 62) + "b", artifactMatchBudget, false},
		{"zero", "{a}", "a", 0, false},
		{"terminal_exhausted", "", "", 0, false},
		{"terminal_sufficient", "", "", 1, true},
		{"capture_attempt_exhausted", "{a}", "a", 1, false},
		{"acceptance_exhausted", "{a}", "a", 2, false},
		{"sufficient", "{a}", "a", 3, true},
		{"repeated_sufficient", "{a}-{a}", "a-a", 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			captures := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
			tokens, _, ok := artifactTokens(tc.template, captures, false)
			if !ok {
				t.Fatal("invalid template")
			}
			bound := make([]string, len(captures))
			budget := tc.budget
			if got := artifactMatch(tokens, tc.candidate, bound, &budget); got != tc.want {
				t.Errorf("match = %t, want %t", got, tc.want)
			}
			if budget != 0 {
				t.Errorf("remaining budget = %d, want zero", budget)
			}
			if !tc.want {
				for _, value := range bound {
					if value != "" {
						t.Fatalf("rejected match retained bindings: %v", bound)
					}
				}
			}
		})
	}
}

func TestProducerTargets_ArtifactCandidates(t *testing.T) {
	t.Parallel()
	ea := NamedResource{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "apps", Name: "api-prod"}
	raw := &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator",
		Spec: map[string]any{"pathPattern": "@repo/{app}", "artifacts": []any{map[string]any{"name": "z"}, map[string]any{"name": "{app}"}, map[string]any{"name": "api-prod"}, map[string]any{"name": "z"}, nil, map[string]any{"name": 1}}}}
	want := []ProducerTarget{{NamedResource: ea}, {Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "apps", Name: "z"}}
	if got := ProducerTargets(raw, ea, ea); !reflect.DeepEqual(got, want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	raw.Spec["artifacts"] = []any{map[string]any{"name": "{app}"}}
	for _, tc := range []struct {
		name string
		edit func(*NamedResource)
	}{
		{"group", func(id *NamedResource) { id.Group = "foreign.example" }},
		{"kind", func(id *NamedResource) { id.Kind = KindGitRepository }},
		{"namespace", func(id *NamedResource) { id.Namespace = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			id := ea
			tc.edit(&id)
			if got := ProducerTargets(raw, id); len(got) != 0 {
				t.Fatalf("foreign candidate: %v", got)
			}
		})
	}
	for _, spec := range []map[string]any{nil, {}, {"artifacts": "wrong"}, {"artifacts": []any{"wrong", nil}}} {
		cloned := *raw
		cloned.Spec = spec
		if got := ProducerTargets(&cloned, ea); len(got) != 0 {
			t.Fatalf("invalid spec: %v", got)
		}
	}
}

func TestProducerTargets_NoOpAllocations(t *testing.T) {
	for _, obj := range []BaseManifest{&Kustomization{}, &RawObject{Kind: "Unrelated"}} {
		if got := testing.AllocsPerRun(10, func() { ProducerTargets(obj) }); got != 0 {
			t.Fatalf("%T: %v allocations", obj, got)
		}
	}
}

func TestProducerTargets_CandidateRequirement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		obj  BaseManifest
		want bool
	}{
		{name: "nil"},
		{name: "typed", obj: &Kustomization{}},
		{name: "fixed", obj: &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Spec: map[string]any{"artifacts": []any{map[string]any{"name": "literal"}}}}},
		{name: "dynamic", obj: &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Spec: map[string]any{"artifacts": []any{map[string]any{"name": "app-{app}"}}}}, want: true},
		{name: "foreign", obj: &RawObject{APIVersion: "example.com/v1", Kind: "ArtifactGenerator", Spec: map[string]any{"artifacts": []any{map[string]any{"name": "app-{app}"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ProducerNeedsCandidates(tc.obj); got != tc.want {
				t.Fatalf("candidate requirement = %t, want %t", got, tc.want)
			}
		})
	}
}

func BenchmarkProducerTargets_ArtifactGenerator(b *testing.B) {
	for _, tc := range []struct{ name, template, candidate string }{
		{"NoCandidates", "{app}", ""},
		{"Literal", "api", ""},
		{"Repeated", "{app}-{app}", "api-v2-api-v2"},
		{"Adjacent", "{app}{env}-{app}", "apiprod-api"},
		{"Adversarial", "{app}{env}{app}{env}{app}", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"},
		{"Incompatible", "app-{app}", "api"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			raw := &RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Namespace: "apps",
				Spec: map[string]any{"pathPattern": "@repo/{app}/{env}", "artifacts": []any{map[string]any{"name": tc.template}}}}
			var candidates []NamedResource
			if tc.candidate != "" {
				candidates = []NamedResource{{Group: SourceDomain, Kind: KindExternalArtifact, Namespace: "apps", Name: tc.candidate}}
			}
			b.ReportAllocs()
			for b.Loop() {
				ProducerTargets(raw, candidates...)
			}
		})
	}
}
