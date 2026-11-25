package loader

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestInputCache_PublicationOrderPreservesAdmission(t *testing.T) {
	t.Parallel()
	if reason := cache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skipf("render persistence disabled: %s", reason)
	}
	for _, tc := range []struct {
		name         string
		publishFirst bool
	}{
		{name: "lookup_before_publication"},
		{name: "publication_before_lookup", publishFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testInputCacheAdmission(t, tc.publishFirst)
		})
	}
}

func testInputCacheAdmission(t *testing.T, publishFirst bool) {
	t.Helper()
	root := t.TempDir()
	owner, err := cache.New(t.Context(), t.TempDir())
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	files := fstest.MapFS{}
	for _, name := range []string{"sibling", "reached"} {
		files[name+".yaml"] = &fstest.MapFile{Data: fmt.Appendf(nil, `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: %[1]s, namespace: apps}
spec:
  chart:
    spec:
      chart: charts/%[1]s
      sourceRef: {kind: GitRepository, name: checkout}
`, name)}
	}
	base := cachedFixtureLoader(t, root, files, owner)
	defer base.Close()
	if publishFirst {
		assert.NoError(t, base.PersistInputs(t.Context()))
	}
	headFiles := maps.Clone(files)
	headFiles["charts/reached/values.yaml"] = &fstest.MapFile{Data: []byte("replicas: 2\n")}
	head := cachedFixtureLoader(t, root, headFiles, owner)
	defer head.Close()
	if !publishFirst {
		assert.NoError(t, base.PersistInputs(t.Context()))
	}
	_, restored := head.inputCaches[root].files[inputCoordinate{name: "sibling.yaml"}]
	assert.Equal(t, restored, publishFirst)
	assertInputCacheAdmission(t, base, head)
}

func assertInputCacheAdmission(t *testing.T, base, head *Loader) {
	t.Helper()
	source := &manifest.GitRepository{Name: "checkout", Namespace: "apps"}
	sourceID := source.Named()
	releaseKind := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease}
	reached, sibling := releaseKind, releaseKind
	reached.Name, reached.Namespace = "reached", "apps"
	sibling.Name, sibling.Namespace = "sibling", "apps"
	for _, loader := range []*Loader{base, head} {
		loader.Store.AddObject(source)
		input, _ := loader.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
		loader.Store.SetArtifact(sourceID, input)
		loader.Index.Records[sourceID] = manifest.DiscoveryRecord{Inputs: []string{"charts/reached/values.yaml"}}
	}
	changes, err := change.Qualify(t.Context(), nil, change.SourcePair{
		Stores: [2]*store.Store{base.Store, head.Store},
	}, manifest.WorkScope{})
	assert.NoError(t, err)
	head.Options.Changes = changes
	assert.NoError(t, head.FinalizeIndex(t.Context(), head.SourceRoot, false))
	configureSelection(t, head, selection.Request{Changed: true, Roots: []manifest.NamedResource{releaseKind}})
	result, committed, err := head.Options.Session.Admit(t.Context(), selection.AdmissionOptions{
		Request: selection.PrepareRequest{Invocation: true, Changes: changes, Missing: head.Options.Missing},
		Stage:   selection.Execution,
	})
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Diff(t, result.Selected, []manifest.NamedResource{reached})
	assert.Equal(t, slices.Contains(result.Needed, sourceID), true)
	assert.Equal(t, slices.Contains(result.Ordinary, reached), true)
	assert.Equal(t, slices.Contains(result.Ordinary, sibling), false)
	assert.Equal(t, head.Options.Session.Snapshot().Assess(sibling), false)
	occurrence, known := head.Occurrence(sibling)
	assert.Equal(t, known, true)
	assert.Equal(t, slices.ContainsFunc(occurrence.Refs, func(ref manifest.Reference) bool {
		return ref.NamedResource == sourceID && ref.BindsSource() && ref.ReadsSourceTree()
	}), true)
}
