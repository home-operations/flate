package manifest

import (
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
)

func TestBootstrapAlias_SourceKinds(t *testing.T) {
	t.Parallel()
	ignore := "excluded/"
	ref := &sourcev1.GitRepositoryRef{Tag: "v1"}
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
				BootstrapAlias(string) (BaseManifest, *GitRepository)
			})
			{
				assert.FatalIf(t, ok != (obj == git), "bootstrap capability on %T", obj)
			}
			if !ok {
				return
			}
			before := Fingerprint(obj)
			alias, descriptor := behavior.BootstrapAlias("/repo")
			assert.Equal(t, alias.Named(), obj.Named())
			{
				assert.FatalIf(
					t,
					alias == obj || Fingerprint(obj) != before,
					"%s",
					"alias mutated or retained receiver",
				)
			}
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
