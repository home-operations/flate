package discovery

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestAdvance_UnreadableInputReachesFixpoint(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "namespace: apps\nresources: [consumer.yaml, source.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"flux/consumer.yaml",
		testutil.KustomizationYAML(
			"consumer",
			"",
			"  path: ./app\n  sourceRef: {kind: GitRepository, name: cluster}\n  postBuild:\n    substituteFrom: [{kind: Secret, name: settings}]\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster}\nspec: {url: 'git://fixture.invalid/repo'}\n",
	)
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [settings.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/settings.yaml",
		"apiVersion: v1\nkind: Secret\nmetadata: {name: settings}\nstringData: {KEY: 'ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]'}\nsops: {mac: encrypted}\n",
	)
	st := store.New()
	l := loader.New(st)
	l.Options.Filter = change.NewFilter()
	l.Options.Changes = change.NewSet([]string{"flux/consumer.yaml"})
	session, err := Begin(
		t.Context(),
		Config{
			Path:      filepath.Join(root, "flux"),
			RepoRoot:  root,
			SelfURLs:  []string{"git://fixture.invalid/repo"},
			Store:     st,
			Loader:    l,
			Trees:     kustomize.NewTreeCache(cachetest.New(t, "", cache.New), nil, task.NewBounded(2)),
			Selection: manifest.Selection{Changed: true},
		},
	)
	assert.NoError(t, err)
	for range 3 {
		proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
		assert.NoError(t, err)
		advanced, err := session.Advance(t.Context(), proposal)
		assert.NoError(t, err)
		if !advanced {
			return
		}
	}
	t.Fatal("construction repeatedly readmitted the unreadable input without reaching a fixpoint")
}
