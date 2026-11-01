package kustomize

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestPreviousEntry_ApplicationInputs(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{
		"kustomization.yaml": existingKustomization("cm.yaml"),
		"cm.yaml":            cmYAML("original"),
		"generator.yaml":     "original producer input",
	})
	cache := newTestTreeCache(t, t.TempDir())
	if !cache.owner.CanPersistRenders() {
		t.Skip("entry receipt requires a clean VCS-stamped build")
	}
	spec := manifest.NewEntryKustomization(manifest.BootstrapSourceID, ".", manifest.NamedResource{})
	request := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: "."}
	application := map[string]any{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "FluxInstance", "metadata": map[string]any{"name": "applier"}}
	evidence := EntryEvidence{Application: application, Owner: manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "generator"}, Records: emptyRenderInventory}
	entry, err := cache.ApplicationReceipt(t.Context(), request.Source, evidence, []string{"generator.yaml"})
	assert.NoError(t, err)
	_, receipt, err := Render(t.Context(), cache, request, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: spec.Named(), Receipt: receipt, Entry: entry, Records: emptyRenderInventory}))
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("changed"))
	previous, complete := cache.PreviousRender(t.Context(), request, spec.Named())
	assert.Equal(t, previous == nil && !complete, true)
	accepted := cache.PreviousEntry(t.Context(), request)
	assert.Diff(t, accepted.Application, application)
	assert.Equal(t, accepted.Owner, evidence.Owner)
	testutil.WriteFile(t, root, "generator.yaml", "changed producer input")
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Application == nil, true)
}
