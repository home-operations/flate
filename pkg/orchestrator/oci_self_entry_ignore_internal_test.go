package orchestrator

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_OCISelfEntryIgnore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t,
		root,
		"entry/root.yaml",
		testutil.KustomizationYAML("root",
			"apps",
			"  path: ./entry\n  sourceRef: {kind: OCIRepository, name: self}\n"))
	for _, name := range []string{"first", "second"} {
		testutil.WriteFile(
			t,
			root,
			"entry/"+name+".yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: "+name+", namespace: apps}\n",
		)
	}
	for _, ignored := range []string{"first", "second"} {
		testutil.WriteFile(t, root, "entry/source.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\n"+
			"kind: OCIRepository\n"+
			"metadata: {name: self, namespace: apps}\n"+
			"spec: {url: oci://unreachable.invalid/self, ignore: 'entry/"+ignored+".yaml'}\n")
		o, err := newTestOrchestrator(t, Tree{Path: root + "/entry", RepoRoot: root}, Config{Concurrency: 2})
		assert.NoError(t, err)
		fetcher := &generatedOCIFetcher{err: fmt.Errorf("%w: acquisition intercepted", manifest.ErrInput)}
		o.src.Fetchers[manifest.KindOCIRepository] = fetcher
		result, err := o.render(t.Context())
		assert.NoError(t, err)
		assert.Equal(t, fetcher.fetched.Load(), false)
		assert.Equal(t, fetcher.calls.Load(), int64(0))
		id := testutil.KustomizationID("apps", "root")
		assert.Equal(t, o.entryBinding.Producer, id)
		assert.Equal(t, o.entryBinding.Source.Kind, manifest.KindOCIRepository)
		seen := map[string]bool{}
		for _, doc := range outputByParent(result.Manifests)[id] {
			if manifest.DocKind(doc) == manifest.KindConfigMap {
				seen[manifest.DocIdentity(doc).Name] = true
			}
		}
		assert.Equal(t, seen[ignored], false)
		assert.Equal(t, len(seen), 1)
		o.stop()
	}
}
