package loader

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_OwnUnreadableInput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
		"owner.yaml": {
			Data: []byte(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: app, namespace: apps}\nspec:\n  interval: 1m\n  path: ./app\n  postBuild:\n    substituteFrom: [{kind: Secret, name: vars}]\n",
			),
		},
		"app/kustomization.yaml": {Data: []byte("resources: [secret.yaml]\n")},
		"app/secret.yaml": {
			Data: []byte(
				"apiVersion: v1\nkind: Secret\nmetadata: {name: vars, namespace: apps}\nstringData: {KEY: 'ENC[AES256_GCM,data:YWJj,iv:ZGVm,tag:Z2hp,type:str]'}\nsops: {mac: encrypted}\n",
			),
		},
	}})
	l := New(st)
	l.SourceRoot = root
	l.Options.Filter = change.NewFilter()
	l.Options.Selection = manifest.Selection{
		Roots: []manifest.NamedResource{
			{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "app"},
		},
	}
	_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
	proposal, err := l.Prepare(t.Context(), PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	found := false
	for _, child := range proposal.Objects {
		if secret, ok := child.Obj.(*manifest.Secret); ok {
			found = true
			assert.Equal(t, secret.HasKeys(), false)
		}
	}
	assert.Equal(t, found, false)
	assert.Equal(t, len(st.Objects()), 0)
}
