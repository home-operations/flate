package kustpath

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
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
			assert.FatalIf(
				t,
				got != tc.want || ok != tc.ok,
				"Resolve(%q, %q, %v) = (%q, %v), want (%q, %v)",
				tc.base,
				tc.ref,
				tc.policy,
				got,
				ok,
				tc.want,
				tc.ok,
			)
		})
	}
}

func TestClaimPrefix_CoordinateDomains(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ directory, want string }{
		{"", ""},
		{".", ""},
		{"/", "/"},
		{"C:/", "C:/"},
		{"//server/share/", "//server/share/"},
		{"apps/../shared", "apps/../shared/"},
		{"apps//", "apps/"},
	} {
		t.Run(tc.directory, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, ClaimPrefix(tc.directory), tc.want)
		})
	}
}

func TestRelateHost_PathCoordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, base, target string
		want               HostRelation
	}{
		{name: "clean equivalent", base: "/apps/./nested", target: "/apps/nested", want: HostEqual},
		{name: "nested", base: "/apps", target: "/apps/nested", want: HostAncestor},
		{name: "ancestor", base: "/apps/nested", target: "/apps", want: HostDescendant},
		{name: "sibling prefix", base: "/apps", target: "/apps-other", want: HostDisjoint},
		{name: "host root", base: "/", target: "/apps", want: HostAncestor},
		{name: "relative root", base: ".", target: "apps", want: HostAncestor},
		{name: "relative escape", base: ".", target: "../apps", want: HostDisjoint},
		{name: "relative parent", base: "..", target: "../..", want: HostDisjoint},
		{name: "relative parent subtree", base: "..", target: "../../child", want: HostDisjoint},
		{name: "relative parent child", base: "..", target: "../child", want: HostAncestor},
		{name: "missing evidence", base: "", target: ".", want: HostDisjoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, RelateHost(tc.base, tc.target), tc.want)
		})
	}
}

func TestRelateHost_WindowsCoordinates(t *testing.T) {
	t.Parallel()
	if runtime.GOOS != "windows" {
		t.Skip("Windows path semantics")
	}
	for _, tc := range []struct {
		name, base, target string
		want               HostRelation
	}{
		{name: "folded directory", base: "C:/Source/K", target: "c:/SOURCE/k", want: HostEqual},
		{name: "alternate separator", base: "C:/Source", target: "c:" + string(filepath.Separator) + "SOURCE/k", want: HostAncestor},
		{name: "different volume", base: "C:/Source", target: "D:/Source/k", want: HostDisjoint},
		{name: "volume relative", base: "C:/", target: "C:Source", want: HostDisjoint},
		{name: "share root", base: "//host/share", target: "//HOST/SHARE/source", want: HostAncestor},
		{name: "different share", base: "//host/share", target: "//host/shared/source", want: HostDisjoint},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, RelateHost(tc.base, tc.target), tc.want)
			if tc.want == HostEqual {
				assert.Equal(t, HostKey(tc.base), HostKey(tc.target))
			}
		})
	}
}
