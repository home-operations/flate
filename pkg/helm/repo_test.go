package helm

import "testing"

func TestOCIPullRef(t *testing.T) {
	const repo = "oci://ghcr.io/bjw-s-labs/helm/app-template"
	for _, tc := range []struct {
		name    string
		version string
		want    string
	}{
		{"empty version", "", repo},
		{"semver tag", "1.2.3", repo + ":1.2.3"},
		{"named tag", "latest", repo + ":latest"},
		{"sha256 digest", "sha256:70a7cb6766eb468068c2c1700c8450253070dc671a9fbbd1a6346a66545e2b2b",
			repo + "@sha256:70a7cb6766eb468068c2c1700c8450253070dc671a9fbbd1a6346a66545e2b2b"},
		{"sha512 digest", "sha512:deadbeef", repo + "@sha512:deadbeef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ociPullRef(repo, tc.version); got != tc.want {
				t.Errorf("ociPullRef(%q, %q) = %q, want %q", repo, tc.version, got, tc.want)
			}
		})
	}
}
