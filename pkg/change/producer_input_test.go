package change_test

import (
	"fmt"
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_ProducerInputChangeRoutes(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"directory", "committed"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			write := func(root, ref string) {
				testutil.WriteFile(t, root, "entry/source.yaml", fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: input, namespace: apps}\nspec: {url: https://example.test/input.git, ref: {branch: %s}}\n", ref))
				testutil.WriteFile(t, root, "entry/generator.yaml", `apiVersion: source.extensions.fluxcd.io/v1beta1
kind: ArtifactGenerator
metadata: {name: generator, namespace: apps}
spec:
  sources: [{alias: repo, kind: GitRepository, name: input}]
  artifacts: [{name: generated, copy: [{from: '@repo/app/', to: '@artifact/app/'}]}]
`)
				testutil.WriteFile(t, root, "entry/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./app\n  sourceRef: {kind: ExternalArtifact, name: generated}\n"))
				testutil.WriteFile(t, root, "entry/unrelated.yaml", testutil.KustomizationYAML("unrelated", "apps", "  path: ./unrelated\n  sourceRef: {kind: GitRepository, name: independent}\n"))
				testutil.WriteFile(t, root, "entry/independent.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: independent, namespace: apps}\nspec: {url: https://example.test/independent.git}\n")
			}
			root := t.TempDir()
			write(root, "before")
			repo := testutil.GitInit(t, root)
			options := &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.test", When: time.Unix(0, 0)}}
			base := testutil.GitCommitAll(t, repo, "before", options)
			write(root, "after")
			var inputs *baseline.Result
			var err error
			if route == "directory" {
				before := t.TempDir()
				write(before, "before")
				inputs, err = baseline.ResolvePaths(t.Context(), root, before)
			} else {
				testutil.GitCommitAll(t, repo, "after", options)
				inputs, err = baseline.AutoResolve(t.Context(), root, base.String(), cachetest.New(t, "", sourcecache.New))
			}
			assert.NoError(t, err)
			changes := inputs.Changes
			if route == "directory" {
				changes, err = change.Detect(t.Context(), inputs.BaseInput, inputs.Source)
				assert.NoError(t, err)
			}
			assert.Diff(t, changes.Paths(), []string{"entry/source.yaml"})
			for _, file := range []string{"entry/generator.yaml", "entry/consumer.yaml", "entry/unrelated.yaml"} {
				before, err := fs.ReadFile(inputs.BaseInput.FS, file)
				assert.NoError(t, err)
				after, err := fs.ReadFile(inputs.Source.FS, file)
				assert.NoError(t, err)
				assert.Diff(t, string(after), string(before))
			}
			generator := manifest.NamedResource{Group: "source.extensions.fluxcd.io", Kind: "ArtifactGenerator", Namespace: "apps", Name: "generator"}
			input := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Namespace: "apps", Name: "input"}
			output := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindExternalArtifact, Namespace: "apps", Name: "generated"}
			consumer := testutil.KustomizationID("apps", "consumer")
			for side, source := range []*store.SourceArtifact{inputs.BaseInput, inputs.Source} {
				st := store.New()
				result, err := discovery.Run(t.Context(), discovery.Config{Input: source, Store: st})
				assert.NoError(t, err)
				record, ok := result.Index.Lookup(generator)
				if !ok || !slices.Contains(record.Refs, input) || st.GetObject(generator) == nil || st.GetObject(consumer) == nil {
					t.Fatalf("side %d: missing real generator/input/consumer evidence: %+v", side, record)
				}
				assert.Diff(t, st.Productions(output, result.Index), []manifest.Production{{Owner: generator, Declaration: generator, Authored: generator, Source: manifest.BootstrapSourceID, File: "entry/generator.yaml"}})
				owners, ancestors := result.Index.OwnersOfFile("entry/source.yaml")
				assert.Diff(t, owners, []manifest.NamedResource(nil))
				assert.Diff(t, ancestors, []manifest.NamedResource(nil))
				filter := change.NewFilter(changes, result.Index, st)
				for _, id := range []manifest.NamedResource{input, generator, consumer} {
					if !filter.ShouldReconcile(id) {
						t.Errorf("side %d: changed producer input did not select %s", side, id)
					}
				}
				assert.Equal(t, filter.ShouldReconcile(testutil.KustomizationID("apps", "unrelated")), false)
			}
		})
	}
}
