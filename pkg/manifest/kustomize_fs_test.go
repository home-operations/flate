package manifest

import (
	"slices"
	"testing"
)

func TestBuildKSClaims_PathsAndComponents(t *testing.T) {
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			ks := &Kustomization{Name: "root", Path: tc.path, SourceName: tc.source, Components: []string{tc.component}}
			claims := BuildKSClaims([]*Kustomization{ks}, nil)
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

// TestNormalizeClaimBase pins the spec.path shapes that must all claim the
// same repo-relative files. The leading-slash case is #920: Flux resolves
// spec.path root-relatively, so `/kubernetes/apps` must not keep a slash that
// a repo-relative source file can never match.
func TestNormalizeClaimBase(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"./kubernetes/apps", "kubernetes/apps"},
		{"kubernetes/apps/", "kubernetes/apps"},
		{"/kubernetes/apps", "kubernetes/apps"},
		{"/kubernetes/apps/", "kubernetes/apps"},
		{"//kubernetes/apps", "kubernetes/apps"},
		{"./apps/../x", "x"},
		{"apps//team", "apps/team"},
		{".//apps", "apps"},
		{"././", ""},
		{"../x", "x"},
		{"apps/../../x", "x"},
		{"..foo", "..foo"},
		{"./", ""},
		{"/", ""},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := NormalizeClaimBase(tc.in); got != tc.want {
				t.Errorf("NormalizeClaimBase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
