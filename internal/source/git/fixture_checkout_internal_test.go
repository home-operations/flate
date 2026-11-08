package git

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	gogit "github.com/go-git/go-git/v5"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveLocal_FixturePreservesCheckout(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"tag", "commit", "semver", "head", "no-ref"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			fixture := testfixture.Materialize(t, "source-pins", "nonhead-renders-preserve-checkout-"+kind)
			repo, err := gogit.PlainOpen(fixture.Repo)
			assert.NoError(t, err)
			wt, err := repo.Worktree()
			assert.NoError(t, err)
			beforeHead, err := repo.Head()
			assert.NoError(t, err)
			beforeStatus, err := wt.Status()
			assert.NoError(t, err)
			before := make(map[string][]byte)
			for _, path := range []string{".git/HEAD", ".git/index", "flux/entry.yaml", "apps/cm.yaml", "apps/kustomization.yaml"} {
				before[path], err = os.ReadFile(filepath.Join(fixture.Repo, path))
				assert.NoError(t, err)
			}
			ref := &sourcev1.GitRepositoryRef{Tag: "v1.0.0"}
			want := "v1.0.0"
			switch kind {
			case "commit":
				ref = &sourcev1.GitRepositoryRef{Commit: fixture.Resolve(t, "@@commit:base@@", "")}
			case "semver":
				ref = &sourcev1.GitRepositoryRef{SemVer: "<2.0.0"}
			case "head":
				ref, want = &sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, "dirty"
			case "no-ref":
				ref, want = nil, "dirty"
			}
			source := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system", Reference: ref}
			artifacts, err := ResolveLocal(
				t.Context(),
				localLocation(t, fixture.Repo),
				[]*manifest.GitRepository{source},
				LocalOptions{},
			)
			assert.NoError(t, err)
			var content []byte
			if kind == "no-ref" {
				assert.Equal(t, len(artifacts), 0)
				content, err = os.ReadFile(filepath.Join(fixture.Repo, "apps", "cm.yaml"))
			} else {
				artifact := artifacts[source.Named()]
				assert.FatalIf(t, artifact == nil, "local source was not resolved")
				content, err = fs.ReadFile(artifact.FS, "apps/cm.yaml")
			}
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				!strings.Contains(string(content), "value: "+want),
				"source bytes = %q, want %s",
				content,
				want,
			)
			afterHead, err := repo.Head()
			assert.NoError(t, err)
			assert.Equal(t, afterHead.Hash(), beforeHead.Hash())
			assert.Equal(t, afterHead.Name(), beforeHead.Name())
			afterStatus, err := wt.Status()
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				cmp.Diff(beforeStatus, afterStatus) != "",
				"checkout status changed (-want +got):\n%s",
				cmp.Diff(beforeStatus, afterStatus),
			)
			for path, want := range before {
				got, err := os.ReadFile(filepath.Join(fixture.Repo, path))
				assert.NoError(t, err)
				assert.FatalIf(t, !bytes.Equal(got, want), "checkout file %s changed", path)
			}
		})
	}
}
