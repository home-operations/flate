package discovery

import (
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	basecontroller "github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestDiscovery_ComposedBootstrapIncludeAdmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, ignore string
		admitted     bool
		suppliedLink bool
		selfInclude  bool
	}{
		{name: "admitted declaration", admitted: true},
		{name: "excluded declaration", ignore: "  ignore: declarations/input.yaml\n"},
		{
			name:         "excluded declaration through supplied include",
			ignore:       "  ignore: declarations/input.yaml\n",
			suppliedLink: true,
		},
		{
			name:         "excluded declaration through supplied include with a cycle",
			ignore:       "  ignore: declarations/input.yaml\n",
			suppliedLink: true,
			selfInclude:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			cacheOwner := cachetest.New(t, "", cache.New)
			id := (&manifest.GitRepository{Name: "input", Namespace: "flux-system"}).Named()
			const payload = "include payload"
			leaf := &store.SourceArtifact{Content: "input", FS: fstest.MapFS{
				"payload.txt": {Data: []byte(payload)},
			}}
			st.SetArtifact(id, leaf)
			include := "input"
			if tc.suppliedLink {
				middle := &manifest.GitRepository{
					Name: "middle", Namespace: "flux-system", URL: "https://middle.invalid/repo",
					Include: []sourcev1.GitRepositoryInclude{
						{
							GitRepositoryRef: manifest.LocalObjectReference{Name: "input"},
							FromPath:         "payload.txt",
							ToPath:           "payload.txt",
						},
					},
				}
				if tc.selfInclude {
					middle.Include = append(middle.Include, sourcev1.GitRepositoryInclude{
						GitRepositoryRef: manifest.LocalObjectReference{Name: middle.Name},
					})
				}
				st.AddDeclaration(store.Declaration{Obj: middle, Origin: manifest.SuppliedDeclaration})
				st.SetArtifact(middle.Named(), leaf)
				include = middle.Name
			}
			const declaration = "declarations/input.yaml"
			files := fstest.MapFS{
				"source.yaml": {Data: []byte(fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec:
  url: https://self.invalid/repo
  include:
  - repository: {name: %s}
    fromPath: payload.txt
    toPath: copied.txt
%s`, include, tc.ignore))},
				declaration: {Data: []byte(`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: input, namespace: flux-system}
spec: {url: 'https://input.invalid/repo'}
`)},
			}
			d, err := Begin(t.Context(), Config{
				Path: root, Input: &store.SourceArtifact{LocalPath: root, FS: files},
				Store: st, Cache: cacheOwner, Trees: kustomize.NewTreeCache(cacheOwner, nil, nil),
				PrepareDocuments: basecontroller.PrepareDocuments,
			})
			assert.FatalIf(t, err != nil, "Begin: %v", err)
			defer d.Close()
			assert.FatalIf(t, st.GetDeclaration(id) == nil, "leaf declaration missing from the bootstrap include chain")
			assert.Equal(t, st.Policy(id).Root, false)
			ready := false
			for !ready {
				assert.FatalIf(
					t,
					len(d.PendingSources()) == 0,
					"discovery stalled before composing the bootstrap source",
				)
				assert.NoError(t, constructTestSources(t.Context(), d))
				ready, err = d.Load(t.Context())
				assert.FatalIf(t, err != nil, "Load: %v", err)
			}
			artifact, _ := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
			assert.FatalIf(t, artifact == nil, "composed bootstrap source missing")
			data, err := fs.ReadFile(artifact.FS, "copied.txt")
			assert.NoError(t, err)
			assert.Equal(t, string(data), payload)
			assert.Equal(t, d.loader.IsFileIndexed(id), tc.admitted)
			assert.Equal(t, st.Policy(id).Root, tc.admitted)
			assert.Equal(t, st.Policy(id).Report, tc.admitted)
			rootEvidence := selection.CaptureRoot(d.loader.RootEvidence(id, nil), st, d.loader.Index)
			assert.Equal(t, selection.Root(rootEvidence).Admitted, tc.admitted)
		})
	}
}
