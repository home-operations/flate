package loader

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFinalizeIndex_ProducedChildPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, postbuild string
		known           bool
	}{
		{name: "inline", postbuild: "  postBuild: {substitute: {ROOT: apps/red}}\n", known: true},
		{name: "reference", postbuild: "  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}\n", known: true},
		{name: "unknown", postbuild: "  postBuild: {substituteFrom: [{kind: ConfigMap, name: missing}]}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"flux/kustomization.yaml":      {Data: []byte("resources: [owner.yaml,settings.yaml]\n")},
				"flux/owner.yaml":              {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: owner, namespace: apps}\nspec:\n  path: ./producers\n  targetNamespace: apps\n" + tc.postbuild)},
				"flux/settings.yaml":           {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {ROOT: apps/red}\n")},
				"producers/kustomization.yaml": {Data: []byte("resources: [child.yaml]\n")},
				"producers/child.yaml":         {Data: []byte("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: child, namespace: apps}\nspec: {path: './${ROOT}'}\n")},
				"apps/red/kustomization.yaml":  {Data: []byte("resources: []\n")},
			}})
			l := New(st)
			l.SourceRoot = root
			l.Options.Selection = manifest.Selection{Changed: true}
			l.Options.Changes = change.NewSet([]string{"apps/red/cm.yaml"})
			l.Options.Filter = change.NewFilter()
			_, err := l.Load(t.Context(), filepath.Join(root, "flux"))
			assert.NoError(t, err)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			child := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "child"}
			owner := child
			owner.Name = "owner"
			record, _ := l.Index.Lookup(child)
			want := []string{"apps/red"}
			if !tc.known {
				want = []string{""}
			}
			assert.Diff(t, record.ContentRoots[manifest.BootstrapSourceID], want)
			assert.Equal(t, l.CatalogObject(child).(*manifest.Kustomization).Path, "./${ROOT}")
			parent, _ := l.Index.Lookup(owner)
			assert.Equal(t, parent.StaticOutput, tc.known)
			proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, slices.Contains(proposal.Needed, child), true)
			assert.Equal(t, slices.Contains(proposal.Needed, owner), true)
		})
	}
}
