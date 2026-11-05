package manifest

import (
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestBootstrapAlias_SourceKinds(t *testing.T) {
	t.Parallel()
	ignore := "excluded/"
	ref := &sourcev1.GitRepositoryRef{Branch: "main"}
	git := &GitRepository{
		Name:              "entry",
		Namespace:         "other",
		URL:               "https://fixture.invalid/repo",
		Reference:         ref,
		Ignore:            &ignore,
		SparseCheckout:    []string{"apps"},
		RecurseSubmodules: true,
	}
	for _, obj := range []BaseManifest{git, &OCIRepository{}, &Bucket{}, &HelmRepository{}} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			t.Parallel()
			behavior, ok := obj.(interface {
				BootstrapAlias(string) (BaseManifest, *GitRepository, string)
			})
			assert.FatalIf(
				t,
				ok != (obj == git || obj.Named().Kind == KindOCIRepository),
				"bootstrap capability on %T",
				obj,
			)
			if !ok {
				return
			}
			before := Fingerprint(obj)
			alias, descriptor, expression := behavior.BootstrapAlias("/repo")
			if obj != git {
				assert.Equal(t, alias, obj)
				assert.Equal(t, descriptor, (*GitRepository)(nil))
				assert.Equal(t, expression, "")
				return
			}
			assert.Equal(t, expression, ignore)
			assert.Equal(t, alias.Named(), obj.Named())
			assert.FatalIf(
				t,
				alias == obj || Fingerprint(obj) != before,
				"%s",
				"alias mutated or retained receiver",
			)
			a := alias.(*GitRepository)
			assert.Equal(t, descriptor, git)
			assert.Equal(t, a.URL, "file:///repo")
			assert.Equal(t, a.Reference, ref)
			assert.Equal(t, a.Ignore, &ignore)
			assert.Equal(t, a.RecurseSubmodules, true)
			assert.Diff(t, a.SparseCheckout, []string{"apps"})
		})
	}
}

func TestBootstrapAlias_GitReference(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		ref     *sourcev1.GitRepositoryRef
		mutable bool
	}{
		{name: "default", mutable: true},
		{name: "empty", ref: &sourcev1.GitRepositoryRef{}, mutable: true},
		{name: "branch", ref: &sourcev1.GitRepositoryRef{Branch: "main"}, mutable: true},
		{name: "named branch", ref: &sourcev1.GitRepositoryRef{Name: "refs/heads/main"}, mutable: true},
		{name: "bare name", ref: &sourcev1.GitRepositoryRef{Name: "main"}},
		{name: "invalid branch", ref: &sourcev1.GitRepositoryRef{Branch: "invalid..branch"}},
		{name: "named tag", ref: &sourcev1.GitRepositoryRef{Name: "refs/tags/v1"}},
		{name: "other named ref", ref: &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"}},
		{name: "tag", ref: &sourcev1.GitRepositoryRef{Tag: "v1"}},
		{name: "semver", ref: &sourcev1.GitRepositoryRef{SemVer: ">=1"}},
		{name: "commit overrides branch", ref: &sourcev1.GitRepositoryRef{Branch: "main", Commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
		{name: "invalid commit", ref: &sourcev1.GitRepositoryRef{Commit: "bad"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			object := &GitRepository{Reference: tc.ref}
			alias, descriptor, _ := object.BootstrapAlias("/repo")
			assert.Equal(t, alias != nil, tc.mutable)
			assert.Equal(t, descriptor, object)
		})
	}
}

func TestBootstrapAlias_OCIReference(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		ref     *sourcev1.OCIRepositoryRef
		mutable bool
	}{
		{name: "default", mutable: true},
		{name: "empty", ref: &sourcev1.OCIRepositoryRef{}, mutable: true},
		{name: "tag", ref: &sourcev1.OCIRepositoryRef{Tag: "latest"}},
		{name: "digest", ref: &sourcev1.OCIRepositoryRef{Digest: "sha256:abc"}},
		{name: "semver", ref: &sourcev1.OCIRepositoryRef{SemVer: ">=1"}},
		{name: "filter", ref: &sourcev1.OCIRepositoryRef{SemverFilter: "v.*"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ignore := "excluded/"
			object := &OCIRepository{Reference: tc.ref, Ignore: &ignore}
			alias, descriptor, expression := object.BootstrapAlias("/repo")
			assert.Equal(t, alias != nil, tc.mutable)
			assert.Equal(t, descriptor, (*GitRepository)(nil))
			assert.Equal(t, expression, ignore)
		})
	}
}
