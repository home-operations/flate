package kustomizeinput

import (
	"testing"
	"time"

	"sigs.k8s.io/kustomize/api/resource"

	"github.com/home-operations/flate/internal/testutil/assert"
)

func TestParseGitBase_UpstreamGrammar(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, raw string
		repoRoot  bool
	}{
		{name: "markerless GitHub", raw: "https://github.com/org/repo/path"},
		{name: "schemeless GitHub", raw: "github.com/org/repo/path"},
		{name: "SSH GitHub", raw: "ssh://git@github.com/org/repo/path"},
		{name: "file", raw: "file:///repo//base"},
		{name: "SCP", raw: "user@host.example:org/repo//base"},
		{name: "first git suffix", raw: "https://host.example/org/repo.gitignore"},
		{name: "explicit separator", raw: "https://host.example/org/repo//base/nested?version=v1&ref=v2"},
		{name: "Azure", raw: "https://dev.azure.com/org/project/_git/repo/base"},
		{name: "repository root", raw: "https://github.com/org/repo?version=v1", repoRoot: true},
		{name: "HTTP", raw: "http://host.example/org/repo/base"},
		{name: "mixed case forced Git", raw: "GiT::HTTPS://GitHub.Com/org/repo//base"},
		{name: "GitHub user", raw: "HTTPS://user@GitHub.com/org/repo", repoRoot: true},
		{name: "mixed case SSH", raw: "SSH://github.com/org/repo", repoRoot: true},
		{name: "SSH port", raw: "ssh://user@host.example:2222/org/repo//base"},
		{name: "file root", raw: "file:///one/two/three", repoRoot: true},
		{name: "Azure suffix and separator", raw: "https://host.example/project/_git/repo.git//base"},
		{name: "Git suffix and separator", raw: "https://host.example/org/repo.git//base"},
		{name: "trailing slash", raw: "https://github.com/org/", repoRoot: true},
		{name: "empty organization", raw: "https://github.com//repo", repoRoot: true},
		{name: "unsupported scheme", raw: "oci://host/repo"},
		{name: "schemeless non-GitHub", raw: "host.example/org/repo"},
		{name: "local path", raw: "./relative"},
		{name: "absolute path", raw: "/absolute"},
		{name: "invalid remote", raw: "https://host.example/only"},
		{name: "path escapes repository", raw: "https://host.example/org/repo//../../escape"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			origin := (&resource.Origin{}).Append(tc.raw)
			got, ok := ParseGitBase(tc.raw)
			wantOK := origin.Repo != ""
			// Origin.Path can name the clone directory for a repository root or a local path on rejection.
			var wantPath string
			if wantOK && !tc.repoRoot {
				wantPath = origin.Path
			}
			assert.ErrorIf(
				t,
				ok != wantOK || got.RepoURL != origin.Repo || got.SubPath != wantPath || got.Ref != origin.Ref,
				"ParseGitBase(%q) = %+v, %t; want repo=%q subpath=%q ref=%q accepted=%t",
				tc.raw,
				got,
				ok,
				origin.Repo,
				wantPath,
				origin.Ref,
				wantOK,
			)
		})
	}
}

func TestParseGitBase_QueryPolicy(t *testing.T) {
	t.Parallel()
	// resource.Origin.Append omits the private internal/git.parseQuery defaults:
	// defaultTimeout is 27 seconds and defaultSubmodules is true.
	const defaultTimeout = 27 * time.Second
	for _, tc := range []struct {
		query      string
		timeout    time.Duration
		submodules bool
	}{
		{"", defaultTimeout, true},
		{"version=old&ref=new&timeout=61&submodules=false", 61 * time.Second, false},
		{"version=old&ref=&timeout=1m&submodules=1", time.Minute, true},
		{"timeout=0&submodules=0", defaultTimeout, false},
		{"timeout=-1s&submodules=invalid", defaultTimeout, true},
		{"timeout=invalid", defaultTimeout, true},
		{"timeout=2ms&submodules=FALSE", 2 * time.Millisecond, false},
		{"ref=new&timeout=%ZZ&submodules=false", defaultTimeout, true},
		{"ref=first&ref=second", defaultTimeout, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()
			raw := "https://github.com/org/repo?" + tc.query
			origin := (&resource.Origin{}).Append(raw)
			got, ok := ParseGitBase(raw)
			assert.FatalIf(
				t,
				!ok || got.Ref != origin.Ref || got.Timeout != tc.timeout || got.Submodules != tc.submodules,
				"effective query policy = %+v, %t; want ref=%q timeout=%v submodules=%t",
				got,
				ok,
				origin.Ref,
				tc.timeout,
				tc.submodules,
			)
		})
	}
}

//nolint:paralleltest // allocation measurements require serial execution.
func TestParseQuery_DefaultAllocs(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		_, _, _ = parseQuery("")
	})
	assert.FatalIf(t, allocs != 0, "default query allocations = %v, want 0", allocs)
}
