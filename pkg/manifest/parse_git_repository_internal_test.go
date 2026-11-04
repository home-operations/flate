package manifest

import (
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestGitRepositoryRef_Precedence(t *testing.T) {
	t.Parallel()
	const commit = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		name                     string
		ref                      *GitRepositoryRef
		kind, value, label, spec string
		normalized               sourcev1.GitRepositoryRef
		pinned                   bool
	}{
		{name: "nil"},
		{name: "empty", ref: &GitRepositoryRef{}},
		{
			name:       "commit",
			ref:        &GitRepositoryRef{Commit: commit},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
			pinned:     true,
		},
		{
			name:       "commit branch",
			ref:        &GitRepositoryRef{Commit: commit, Branch: "main"},
			kind:       "commit",
			value:      commit,
			label:      "branch:main@commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit, Branch: "main"},
		},
		{
			name:       "commit tag",
			ref:        &GitRepositoryRef{Commit: commit, Tag: "bad tag"},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
		},
		{
			name:       "commit semver",
			ref:        &GitRepositoryRef{Commit: commit, SemVer: "invalid"},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
		},
		{
			name:       "commit name",
			ref:        &GitRepositoryRef{Commit: commit, Name: "bad name"},
			kind:       "commit",
			value:      commit,
			label:      "commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit},
		},
		{
			name:       "mixed commit",
			ref:        &GitRepositoryRef{Branch: "main", Tag: "v1.0.0", SemVer: ">=1.0", Name: "refs/heads/release", Commit: commit},
			kind:       "commit",
			value:      commit,
			label:      "branch:main@commit:" + commit,
			normalized: sourcev1.GitRepositoryRef{Commit: commit, Branch: "main"},
		},
		{
			name:       "name",
			ref:        &GitRepositoryRef{Branch: "bad branch", Tag: "v1.0.0", SemVer: "invalid", Name: "refs/heads/release"},
			kind:       "name",
			value:      "refs/heads/release",
			label:      "name:refs/heads/release",
			spec:       "+refs/heads/release:refs/heads/release",
			normalized: sourcev1.GitRepositoryRef{Name: "refs/heads/release"},
		},
		{
			name:       "semver",
			ref:        &GitRepositoryRef{Branch: "bad branch", Tag: "bad tag", SemVer: ">=1.0"},
			kind:       "semver",
			value:      ">=1.0",
			label:      "semver:>=1.0",
			spec:       "+refs/tags/*:refs/tags/*",
			normalized: sourcev1.GitRepositoryRef{SemVer: ">=1.0"},
		},
		{
			name:       "name only",
			ref:        &GitRepositoryRef{Name: "refs/pull/420/head"},
			kind:       "name",
			value:      "refs/pull/420/head",
			label:      "name:refs/pull/420/head",
			spec:       "+refs/pull/420/head:refs/pull/420/head",
			normalized: sourcev1.GitRepositoryRef{Name: "refs/pull/420/head"},
		},
		{
			name:       "tag",
			ref:        &GitRepositoryRef{Tag: "v1.2.3", Branch: "bad branch"},
			kind:       "tag",
			value:      "v1.2.3",
			label:      "tag:v1.2.3",
			spec:       "+refs/tags/v1.2.3:refs/tags/v1.2.3",
			normalized: sourcev1.GitRepositoryRef{Tag: "v1.2.3"},
		},
		{
			name:       "branch",
			ref:        &GitRepositoryRef{Branch: "main"},
			kind:       "branch",
			value:      "main",
			label:      "branch:main",
			spec:       "+refs/heads/main:refs/heads/main",
			normalized: sourcev1.GitRepositoryRef{Branch: "main"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := Fingerprint(tc.ref)
			kind, value := tc.ref.Effective()
			assert.Equal(t, kind, tc.kind)
			assert.Equal(t, value, tc.value)
			want := ""
			if tc.kind != "" {
				want = tc.kind + ":" + tc.value
			}
			assert.Equal(t, tc.ref.String(), want)
			assert.Equal(t, tc.ref.Label(), tc.label)
			assert.Equal(t, string(tc.ref.RefSpec()), tc.spec)
			assert.Equal(t, tc.ref.Normalized(), tc.normalized)
			assert.Equal(t, tc.ref.Pinned(), tc.pinned)
			assert.Equal(t, tc.ref.Validate(), nil)
			assert.Equal(t, Fingerprint(tc.ref), before)
		})
	}
}

func TestGitRepositoryRef_Validate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ref  GitRepositoryRef
	}{
		{"short commit", GitRepositoryRef{Commit: "abc"}},
		{"nonhex commit", GitRepositoryRef{Commit: strings.Repeat("g", 40)}},
		{"commit branch", GitRepositoryRef{Commit: strings.Repeat("a", 40), Branch: "bad branch"}},
		{"name", GitRepositoryRef{Name: "bad name"}},
		{"semver", GitRepositoryRef{SemVer: "invalid"}},
		{"tag", GitRepositoryRef{Tag: "bad tag"}},
		{"branch", GitRepositoryRef{Branch: "bad branch"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			{
				err := tc.ref.Validate()
				assert.FatalIf(t, err == nil, "%s", "invalid selected reference accepted")
			}
		})
	}
}
