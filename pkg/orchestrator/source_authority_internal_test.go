package orchestrator

import (
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_AuthoredSelfSourceAuthority(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const url = "https://fixture.invalid/self"
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: [root.yaml, source.yaml, cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"entry/root.yaml",
		testutil.KustomizationYAML("root", "apps", "  path: ./entry\n  sourceRef: {kind: GitRepository, name: self}\n"),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/source.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: self, namespace: apps}\nspec: {url: "+url+"}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"entry/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join(root, "entry"), RepoRoot: root, SelfURLs: []string{url}},
		Config{Concurrency: 2},
	)
	assert.NoError(t, err)
	result, err := o.render(t.Context())
	assert.NoError(t, err)
	assert.FatalIf(t, result == nil, "missing render result")
	found := false
	for _, batch := range result.Manifests {
		for _, doc := range batch.Documents {
			found = found ||
				manifest.DocKind(doc) == manifest.KindConfigMap && manifest.DocIdentity(doc).Name == "payload"
		}
	}
	assert.Equal(t, found, true)
}
