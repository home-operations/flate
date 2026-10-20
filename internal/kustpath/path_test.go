package kustpath

import (
	"path/filepath"
	"testing"
)

func TestResolve_SlashPolicies(t *testing.T) {
	t.Parallel()
	absolute := filepath.Join(t.TempDir(), "file")
	for _, tc := range []struct {
		name, base, ref, want string
		policy                Policy
		ok                    bool
	}{
		{name: "root_empty", ref: "", want: ".", policy: Root, ok: true},
		{name: "root_dot", ref: ".", want: ".", policy: Root, ok: true},
		{name: "root_clean", ref: "apps/../base", want: "base", policy: Root, ok: true},
		{name: "root_dot_prefix", ref: "..foo", want: "..foo", policy: Root, ok: true},
		{name: "root_absolute", ref: "/apps", want: "/apps", policy: Root, ok: true},
		{name: "root_url", ref: "https://example.com/x", want: "https:/example.com/x", policy: Root, ok: true},
		{name: "root_parent", ref: "apps/../..", policy: Root},
		{name: "root_escape", ref: "apps/../../base", policy: Root},
		{name: "transformer_absolute", base: "apps", ref: absolute, policy: Transformer},
		{name: "transformer_slash_absolute", base: "apps", ref: "/file", policy: Transformer},
		{name: "transformer_dot", base: "apps", ref: "..", policy: Transformer},
		{name: "transformer_parent", base: ".", ref: "..", policy: Transformer},
		{name: "transformer_native", base: "apps", ref: filepath.FromSlash("../shared/part"), want: "shared/part", policy: Transformer, ok: true},
		{name: "claim_slashes", base: "apps", ref: "../shared/part", want: "shared/part", policy: Claim, ok: true},
		{name: "claim_backslashes", base: "apps", ref: `..\shared\part`, want: `apps/..\shared\part`, policy: Claim, ok: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := Resolve(tc.base, tc.ref, tc.policy)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("Resolve(%q, %q, %v) = (%q, %v), want (%q, %v)", tc.base, tc.ref, tc.policy, got, ok, tc.want, tc.ok)
			}
		})
	}
}
