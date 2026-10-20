package manifest

import (
	"path/filepath"
	"slices"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestBuildKSClaims_PathsAndComponents(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, source, component string
		want                          []string
	}{
		{name: "stub"},
		{name: "source_root", source: "flux-system", want: []string{""}},
		{name: "explicit_root", path: "./", want: []string{""}},
		{name: "dot_prefix", path: "./apps", component: "../..foo", want: []string{"..foo/", "apps/"}},
		{name: "parent", path: "./apps", component: "../..", want: []string{"apps/"}},
		{name: "outside", path: "./apps", component: "../../outside", want: []string{"apps/"}},
		{name: "empty", path: "./apps", want: []string{"apps/"}},
		{name: "dot", path: "./apps", component: "..", want: []string{"apps/"}},
		{name: "absolute", path: "./apps", component: "/outside", want: []string{"apps/"}},
		{name: "url", path: "./apps", component: "https://example.com/x", want: []string{"apps/"}},
		{name: "same_base", path: "./apps", component: ".", want: []string{"apps/", "apps/"}},
		{name: "slash_component", path: "./apps", component: "../shared/part", want: []string{"shared/part/", "apps/"}},
		{name: "backslash_component", path: "./apps", component: `..\shared`, want: []string{`apps/..\shared/`, "apps/"}},
	} {
		for _, input := range []string{"authored", "decoded"} {
			t.Run(input+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				ks := &Kustomization{Name: "root", Path: tc.path, SourceRef: kustomizev1.CrossNamespaceSourceReference{Name: tc.source}, Components: []string{tc.component}}
				var components map[NamedResource][]string
				if input == "decoded" {
					ks.Components = nil
					components = map[NamedResource][]string{ks.Named(): {tc.component}}
				}
				claims := BuildKSClaims([]*Kustomization{ks}, components)
				var prefixes []string
				for _, claim := range claims {
					if claim.ID != ks.Named() {
						t.Fatalf("claim ID = %v, want %v", claim.ID, ks.Named())
					}
					prefixes = append(prefixes, claim.Prefix)
				}
				if !slices.Equal(prefixes, tc.want) {
					t.Fatalf("claim prefixes = %v, want %v", prefixes, tc.want)
				}
			})
		}
	}
}

func TestBuildKSClaims_StableOrder(t *testing.T) {
	t.Parallel()
	first := &Kustomization{Name: "first", Path: "./apps", Components: []string{"."}}
	second := &Kustomization{Name: "second", Path: "./peer"}
	claims := BuildKSClaims([]*Kustomization{first, second}, map[NamedResource][]string{first.Named(): {"../team"}})
	assert.Diff(t, claims, []KSClaim{
		{ID: first.Named(), Prefix: "apps/"},
		{ID: first.Named(), Prefix: "apps/"},
		{ID: first.Named(), Prefix: "team/"},
		{ID: second.Named(), Prefix: "peer/"},
	})
}

func TestNormalizeClaimBase(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"./kubernetes/apps", "kubernetes/apps"},
		{"kubernetes/apps/", "kubernetes/apps"},
		{"/kubernetes/apps", "kubernetes/apps"},
		{"/kubernetes/apps/", "kubernetes/apps"},
		{"//kubernetes/apps", "kubernetes/apps"},
		{"./apps/../x", "apps/../x"},
		{"apps//team", "apps/team"},
		{filepath.FromSlash("apps/team"), "apps/team"},
		{"alias/../base", "alias/../base"},
		{"./alias//./../base/.", "alias/../base"},
		{".//apps", "apps"},
		{"././", ""},
		{"../x", "x"},
		{"../../x", "x"},
		{".", ""},
		{"..", ""},
		{"apps/../../x", "apps/../../x"},
		{"..foo", "..foo"},
		{"./", ""},
		{"/", ""},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeClaimBase(tc.in); got != tc.want {
				t.Errorf("NormalizeClaimBase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
