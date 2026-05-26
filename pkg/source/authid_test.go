package source

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
)

func TestAuthIdentity(t *testing.T) {
	cases := []struct {
		name  string
		parts []string
		want  string
	}{
		{"empty", nil, ""},
		{"all empty", []string{"", "", ""}, ""},
		{"single", []string{"a"}, "a"},
		{"strips empties", []string{"a", "", "b"}, "a\x00b"},
		{"order matters", []string{"b", "a"}, "b\x00a"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := AuthIdentity(c.parts...); got != c.want {
				t.Errorf("AuthIdentity(%q) = %q, want %q", c.parts, got, c.want)
			}
		})
	}
}

func TestAuthIdentityFromRefs(t *testing.T) {
	mk := func(name string) *manifest.LocalObjectReference {
		return &manifest.LocalObjectReference{Name: name}
	}
	cases := []struct {
		name string
		ns   string
		refs []*manifest.LocalObjectReference
		want string
	}{
		{"no refs", "ns", nil, ""},
		{"all nil", "ns", []*manifest.LocalObjectReference{nil, nil}, ""},
		{"single secret", "ns", []*manifest.LocalObjectReference{mk("creds")}, "ns/creds"},
		{
			name: "matches positional AuthIdentity",
			ns:   "ns",
			refs: []*manifest.LocalObjectReference{mk("a"), nil, mk("b")},
			want: AuthIdentity("ns/a", "", "ns/b"),
		},
		{
			name: "namespace prefix isolates colliding names",
			ns:   "team-a",
			refs: []*manifest.LocalObjectReference{mk("shared")},
			want: "team-a/shared",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := AuthIdentityFromRefs(c.ns, c.refs...); got != c.want {
				t.Errorf("AuthIdentityFromRefs(%q, %v) = %q, want %q", c.ns, c.refs, got, c.want)
			}
		})
	}
}

// TestAuthIdentityFromRefs_BackwardCompat: the typed helper must produce
// the same identities the three fetchers used to compute by hand —
// otherwise existing on-disk cache slots would orphan on upgrade.
func TestAuthIdentityFromRefs_BackwardCompat(t *testing.T) {
	mk := func(name string) *manifest.LocalObjectReference {
		return &manifest.LocalObjectReference{Name: name}
	}
	ns := "demo"

	// Git: (Secret, Proxy)
	gitWant := AuthIdentity(secretRefID(ns, "g-secret"), secretRefID(ns, "g-proxy"))
	if got := AuthIdentityFromRefs(ns, mk("g-secret"), mk("g-proxy")); got != gitWant {
		t.Errorf("git shape: %q vs legacy %q", got, gitWant)
	}

	// Bucket: (Secret, Cert, Proxy)
	bucketWant := AuthIdentity(
		secretRefID(ns, "b-secret"),
		secretRefID(ns, "b-cert"),
		secretRefID(ns, "b-proxy"))
	if got := AuthIdentityFromRefs(ns, mk("b-secret"), mk("b-cert"), mk("b-proxy")); got != bucketWant {
		t.Errorf("bucket shape: %q vs legacy %q", got, bucketWant)
	}

	// OCI: (Secret, Cert, Proxy, Verify)
	ociWant := AuthIdentity(
		secretRefID(ns, "o-secret"),
		secretRefID(ns, "o-cert"),
		secretRefID(ns, "o-proxy"),
		secretRefID(ns, "o-verify"))
	if got := AuthIdentityFromRefs(ns, mk("o-secret"), mk("o-cert"), mk("o-proxy"), mk("o-verify")); got != ociWant {
		t.Errorf("oci shape: %q vs legacy %q", got, ociWant)
	}
}
