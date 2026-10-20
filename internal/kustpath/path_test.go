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
		{name: "dependency_child", base: "apps", ref: "child", want: "apps/child", policy: Dependency, ok: true},
		{name: "dependency_parent", base: "apps", ref: "../base", want: "base", policy: Dependency, ok: true},
		{name: "dependency_escape", base: "apps", ref: "../../base", want: "../base", policy: Dependency, ok: true},
		{name: "dependency_absolute", base: "apps", ref: absolute, want: absolute, policy: Dependency, ok: true},
		{name: "dependency_empty", base: "apps", ref: "", want: "", policy: Dependency, ok: false},
		{name: "dependency_dot", base: "apps", ref: ".", want: "apps", policy: Dependency, ok: true},
		{name: "dependency_http", base: "apps", ref: "https://example.com/repo", policy: Dependency},
		{name: "dependency_git", base: "apps", ref: "git::example.com/repo", policy: Dependency},
		{name: "dependency_gh", base: "apps", ref: "gh:org/repo", policy: Dependency},
		{name: "dependency_ssh", base: "apps", ref: "git@example.com:org/repo", policy: Dependency},
		{name: "dependency_github_colon", base: "apps", ref: "github.com:org/repo", policy: Dependency},
		{name: "dependency_github_slash", base: "apps", ref: "github.com/org/repo", policy: Dependency},
		{name: "directory_http", base: "apps", ref: "https://example.com/repo", policy: Directory},
		{name: "directory_git", base: "apps", ref: "git::example.com/repo", policy: Directory},
		{name: "directory_gh", base: "apps", ref: "gh:org/repo", policy: Directory},
		{name: "directory_ssh", base: "apps", ref: "git@example.com:org/repo", policy: Directory},
		{name: "directory_github_colon", base: "apps", ref: "github.com:org/repo", policy: Directory},
		{name: "directory_github_slash", base: "apps", ref: "github.com/org/repo", policy: Directory},
		{name: "transformer_http", base: "apps", ref: "https://example.com/repo", policy: Transformer},
		{name: "transformer_git", base: "apps", ref: "git::example.com/repo", policy: Transformer},
		{name: "transformer_gh", base: "apps", ref: "gh:org/repo", policy: Transformer},
		{name: "transformer_ssh", base: "apps", ref: "git@example.com:org/repo", policy: Transformer},
		{name: "transformer_github_colon", base: "apps", ref: "github.com:org/repo", policy: Transformer},
		{name: "transformer_github_slash", base: "apps", ref: "github.com/org/repo", policy: Transformer},
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
