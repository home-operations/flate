package manifest

import (
	"path/filepath"
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

func TestNormalizeClaimBase(t *testing.T) {
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
			if got := NormalizeClaimBase(tc.in); got != tc.want {
				t.Errorf("NormalizeClaimBase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
