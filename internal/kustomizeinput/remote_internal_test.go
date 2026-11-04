package kustomizeinput

import (
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

func TestParseGitBase_UpstreamGrammar(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, raw, repo, subpath, ref string
	}{
		{"markerless GitHub", "https://github.com/org/repo/path", "https://github.com/org/repo", "path", ""},
		{"schemeless GitHub", "github.com/org/repo/path", "https://github.com/org/repo", "path", ""},
		{"SSH GitHub", "ssh://git@github.com/org/repo/path", "git@github.com:org/repo", "path", ""},
		{"file", "file:///repo//base", "file:///repo", "base", ""},
		{"SCP", "user@host.example:org/repo//base", "user@host.example:org/repo", "base", ""},
		{"first git suffix", "https://host.example/org/repo.gitignore", "https://host.example/org/repo.git", "ignore", ""},
		{"explicit separator", "https://host.example/org/repo//base?version=v1&ref=v2", "https://host.example/org/repo", "base", "v2"},
		{"Azure", "https://dev.azure.com/org/project/_git/repo/base", "https://dev.azure.com/org/project/_git/repo", "base", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ParseGitBase(tc.raw)
			assert.ErrorIf(
				t,
				!ok || got.RepoURL != tc.repo || got.SubPath != tc.subpath || got.Ref != tc.ref,
				"ParseGitBase(%q) = %+v, %t; want repo=%q subpath=%q ref=%q",
				tc.raw,
				got,
				ok,
				tc.repo,
				tc.subpath,
				tc.ref,
			)
		})
	}
}

func TestParseGitBase_QueryPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		query, ref string
		timeout    time.Duration
		submodules bool
	}{
		{"", "", 27 * time.Second, true},
		{"version=old&ref=new&timeout=61&submodules=false", "new", 61 * time.Second, false},
		{"version=old&ref=&timeout=1m&submodules=1", "old", time.Minute, true},
		{"timeout=0&submodules=0", "", 27 * time.Second, false},
		{"timeout=-1s&submodules=invalid", "", 27 * time.Second, true},
		{"timeout=invalid", "", 27 * time.Second, true},
		{"timeout=2ms&submodules=FALSE", "", 2 * time.Millisecond, false},
		{"ref=new&timeout=%ZZ&submodules=false", "", 27 * time.Second, true},
		{"ref=first&ref=second", "first", 27 * time.Second, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			t.Parallel()
			got, ok := ParseGitBase("https://github.com/org/repo?" + tc.query)
			assert.FatalIf(
				t,
				!ok || got.Ref != tc.ref || got.Timeout != tc.timeout || got.Submodules != tc.submodules,
				"effective query policy = %+v, %t",
				got,
				ok,
			)
		})
	}
}

func TestParseGitBase_Boundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ raw, repo, subpath string }{
		{"http://host.example/org/repo/base", "http://host.example/org/repo", "base"},
		{"GiT::HTTPS://GitHub.Com/org/repo//base", "https://github.com/org/repo", "base"},
		{"HTTPS://user@GitHub.com/org/repo", "user@github.com:org/repo", ""},
		{"SSH://github.com/org/repo", "github.com:org/repo", ""},
		{"ssh://user@host.example:2222/org/repo//base", "ssh://user@host.example:2222/org/repo", "base"},
		{"file:///one/two/three", "file:///one/two/three", ""},
		{"https://host.example/project/_git/repo.git//base", "https://host.example/project/_git/repo.git", "base"},
		{"https://host.example/org/repo.git//base", "https://host.example/org/repo.git", "base"},
		{"https://github.com/org/", "https://github.com/org/", ""},
		{"https://github.com//repo", "https://github.com//repo", ""},
		{"./relative", "", ""},
		{"/absolute", "", ""},
		{"oci://host/repo", "", ""},
		{"host.example/org/repo", "", ""},
		{"https://host/only", "", ""},
		{"https://host/org/repo//../../escape", "", ""},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			t.Parallel()
			got, ok := ParseGitBase(tc.raw)
			assert.FatalIf(
				t,
				ok != (tc.repo != "") || got.RepoURL != tc.repo || got.SubPath != tc.subpath,
				"ParseGitBase(%q)=%+v,%t, want repo=%q path=%q",
				tc.raw,
				got,
				ok,
				tc.repo,
				tc.subpath,
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
