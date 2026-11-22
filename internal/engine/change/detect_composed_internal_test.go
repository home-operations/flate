package change

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/gittree"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestQualify_ComposedBootstrapMembership(t *testing.T) {
	t.Parallel()
	const app = "apps/unchanged.yaml"
	const copied = "apps/copied.yaml"
	const policyFile = ".sourceignore"
	const declaration = "source.yaml"
	const authored = "first"
	const payload = "unchanged\n"
	const keep = "# Keep the app.\n"
	const exclude = "/" + app + "\n"
	for _, tc := range []struct {
		name        string
		rules       [2]string
		copies      [2]string
		changedFile string
		rawPaths    []string
		paths       []string
		present     [2]bool
	}{
		{
			name: "removal", rules: [2]string{keep, exclude}, copies: [2]string{"included", "included"},
			changedFile: app, rawPaths: []string{policyFile}, paths: []string{policyFile, app},
			present: [2]bool{true, false},
		},
		{
			name: "restoration", rules: [2]string{exclude, keep}, copies: [2]string{"included", "included"},
			changedFile: app, rawPaths: []string{policyFile}, paths: []string{policyFile, app},
			present: [2]bool{false, true},
		},
		{
			name: "replacement", rules: [2]string{keep, keep}, copies: [2]string{authored, "other"},
			changedFile: copied, paths: []string{copied}, present: [2]bool{true, true},
		},
		{
			name: "include added", rules: [2]string{keep, keep}, copies: [2]string{"", "other"},
			changedFile: copied, rawPaths: []string{declaration}, paths: []string{copied, declaration},
			present: [2]bool{true, true},
		},
		{
			name: "include removed", rules: [2]string{keep, keep}, copies: [2]string{"other", ""},
			changedFile: copied, rawPaths: []string{declaration}, paths: []string{copied, declaration},
			present: [2]bool{true, true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owner := cachetest.New(t, "", cache.New)
			root := t.TempDir()
			stores := [2]*store.Store{store.New(), store.New()}
			var raw [2]*store.SourceArtifact
			for side, rules := range tc.rules {
				base := fstest.MapFS{
					app:         {Data: []byte(payload)},
					copied:      {Data: []byte(authored)},
					policyFile:  {Data: []byte(rules)},
					declaration: {Data: []byte("include")},
				}
				raw[side] = &store.SourceArtifact{FS: base, LocalPath: root}
				included := tc.copies[side]
				if included == "" {
					base[declaration] = &fstest.MapFile{Data: []byte("plain")}
					view, identity, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
						Mode: gittree.Source, Current: base, CurrentContent: "plain",
					})
					assert.NoError(t, err)
					assert.FatalIf(t, gittree.PolicyKey(view) == "", "plain Git view must have a policy key")
					stores[side].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
						FS: view, LocalPath: root, Content: identity,
					})
					continue
				}
				dependency := fstest.MapFS{"include.txt": {Data: []byte(included)}}
				output, _, key, err := owner.Compose(t.Context(), cache.Composition{
					Base:   cache.CompositionInput{FS: base, Content: rules},
					Inputs: []cache.CompositionInput{{FS: dependency, Content: included}},
					Copies: []cache.Copy{{Input: 0, From: "include.txt", To: copied, Mode: cache.PlaceTree}},
					Policy: "source",
				}, &cache.Policy{})
				assert.NoError(t, err)
				data, err := fs.ReadFile(output, copied)
				assert.NoError(t, err)
				assert.Equal(t, string(data), included)
				data, err = fs.ReadFile(output, app)
				if rules == keep {
					assert.NoError(t, err)
					assert.Equal(t, string(data), payload)
				} else {
					assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
				}
				stores[side].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
					FS: output, LocalPath: root, Content: cache.AcquisitionIdentity(output), Key: key,
				})
			}

			changes, err := Detect(t.Context(), raw[0], raw[1], DetectConfig{})
			assert.NoError(t, err)
			assert.Diff(t, changes.Paths(), tc.rawPaths)
			qualified, err := Qualify(t.Context(), changes, SourcePair{Stores: stores}, manifest.WorkScope{})
			assert.NoError(t, err)
			assert.Diff(t, qualified.Paths(), tc.paths)
			for side, want := range tc.present {
				present, known := qualified.Present(manifest.TreeSide(side), tc.changedFile)
				assert.Equal(t, known, true)
				assert.Equal(t, present, want)
			}
		})
	}
}
