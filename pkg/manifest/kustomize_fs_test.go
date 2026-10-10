package manifest

import "testing"

func TestBuildKSClaims_EmptyPathRequiresSource(t *testing.T) {
	for _, tc := range []struct {
		name, path, source string
		wantClaims         int
	}{
		{name: "stub"},
		{name: "source_root", source: "flux-system", wantClaims: 1},
		{name: "explicit_root", path: "./", wantClaims: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ks := &Kustomization{Name: "root", Path: tc.path, SourceName: tc.source}
			claims := BuildKSClaims([]*Kustomization{ks}, nil)
			if len(claims) != tc.wantClaims {
				t.Fatalf("claims = %v, want %d claims", claims, tc.wantClaims)
			}
			if len(claims) != 0 && (claims[0].ID != ks.Named() || claims[0].Prefix != "") {
				t.Fatalf("claims = %v, want %s claiming the source root", claims, ks.Named())
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
