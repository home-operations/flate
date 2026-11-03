package kustomize

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
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
	application := map[string]any{
		"apiVersion": "fluxcd.controlplane.io/v1",
		"kind":       "FluxInstance",
		"metadata":   map[string]any{"name": "applier"},
	}
	evidence := EntryEvidence{
		Records:     emptyRenderInventory,
		Application: application,
		Owner: manifest.NamedResource{
			Group: manifest.HelmReleaseDomain,
			Kind:  manifest.KindHelmRelease,
			Name:  "generator",
		},
	}
	chart := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "chart",
	}
	artifact := &store.SourceArtifact{
		Kind:     chart.Kind,
		Content:  manifest.Fingerprint("first chart"),
		FS:       fstest.MapFS{},
		Revision: "same-tag",
		Key:      "same-family",
	}
	evidence.Inputs = []EntryInput{{Resource: chart, Artifact: artifact}}
	entry, err := cache.ApplicationReceipt(
		t.Context(),
		request.Source,
		evidence,
		[]string{"generator.yaml", "optional.yaml"},
	)
	assert.NoError(t, err)
	docs, receipt, err := Render(t.Context(), cache, request, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.NoError(
		t,
		cache.RecordRender(
			t.Context(),
			RenderAcceptance{
				Owner:   spec.Named(),
				Receipt: receipt,
				Entry:   entry,
				Records: emptyRenderInventory,
				Content: request.Content(docs),
			},
		),
	)
	testutil.WriteFile(t, root, "cm.yaml", cmYAML("changed"))
	accepted := cache.PreviousEntry(t.Context(), request)
	assert.Diff(t, accepted.Application, application)
	assert.Equal(t, accepted.Owner, evidence.Owner)
	st := store.New()
	st.SetArtifact(chart, artifact)
	assert.Equal(t, cache.EntryValid(t.Context(), accepted, st), true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, cache.EntryValid(ctx, accepted, st), false)
	changed := *artifact
	changed.Content = manifest.Fingerprint("different chart behind same tag")
	st.SetArtifact(chart, &changed)
	assert.Equal(t, cache.EntryValid(t.Context(), accepted, st), false)
	st.SetArtifact(chart, nil)
	assert.Equal(t, cache.EntryValid(t.Context(), accepted, st), false)
	incomplete := *artifact
	incomplete.Content = ""
	evidence.Inputs[0].Artifact = &incomplete
	missing, err := cache.ApplicationReceipt(t.Context(), request.Source, evidence, []string{"generator.yaml"})
	assert.NoError(t, err)
	assert.Equal(t, missing == nil, true)
	testutil.WriteFile(t, root, "generator.yaml", "changed producer input")
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Application == nil, true)
	testutil.WriteFile(t, root, "generator.yaml", "original producer input")
	testutil.WriteFile(t, root, "optional.yaml", "new producer input")
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Application == nil, true)
}

func TestEntryApplication_RequiresCompleteFrame(t *testing.T) {
	t.Parallel()
	for _, application := range []entryApplication{{}, {Complete: true}} {
		encoded, err := encodeFrame(renderHeader{Entry: &application}, []byte("output"))
		assert.NoError(t, err)
		_, err = decodeFrame(encoded)
		assert.Equal(t, err != nil, true)
	}
}
