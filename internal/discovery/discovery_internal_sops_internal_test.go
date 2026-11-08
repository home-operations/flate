package discovery

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/change"
	"github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/loader"
	"github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
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
	configureSelection(t, l, selection.Request{Changed: true})
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
			Selection: selection.Request{Changed: true},
		},
	)
	assert.NoError(t, err)
	for range 3 {
		proposal, err := prepareSelection(t.Context(), t, l,
			selection.PrepareRequest{Generation: nil, Reads: nil, History: nil, Invocation: true},
		)
		assert.NoError(t, err)
		advanced, err := session.Advance(t.Context(), proposal)
		assert.NoError(t, err)
		if !advanced {
			return
		}
	}
	t.Fatal("construction repeatedly readmitted the unreadable input without reaching a fixpoint")
}
