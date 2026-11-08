package kustomize

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
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
	var cell Preparation
	t.Cleanup(cell.Retire)
	request := RenderRequest{Source: localSource(t, root), Spec: spec, SubPath: ".", Preparation: &cell}
	cold := cache.PreviousEntry(t.Context(), request)
	assert.Equal(t, cold.Application == nil, true)
	application := map[string]any{
		"apiVersion": "fluxcd.controlplane.io/v1",
		"kind":       "FluxInstance",
		"metadata":   map[string]any{"name": "applier"},
	}
	evidence := EntryEvidence{
		Producer: manifest.NamedResource{
			Group:     manifest.FluxKustomizeDomain,
			Kind:      manifest.KindKustomization,
			Namespace: "apps",
			Name:      "real-entry",
		},
		Records:     emptyRenderInventory,
		Application: application,
		Owner: manifest.NamedResource{
			Group: manifest.HelmReleaseDomain,
			Kind:  manifest.KindHelmRelease,
			Name:  "generator",
		},
	}
	evidence.Root = evidence.Owner
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
	secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "settings"}
	data := manifest.NewSecret(secret, nil, map[string]any{"VALUE": "original"})
	evidence.Inputs = []EntryInput{{Resource: chart, Artifact: artifact}, {Resource: secret, Data: data}}
	entry, err := cache.ApplicationReceipt(
		t.Context(),
		request,
		evidence,
		[]string{"generator.yaml", "optional.yaml"},
	)
	assert.NoError(t, err)
	assert.NoError(t, cache.RecordEntry(t.Context(), entry.Receipt, nil))
	raw := request
	raw.Spec = nil
	_, rawReceipt, err := Render(t.Context(), cache, raw, sourcecache.RetryConfig{})
	assert.NoError(t, err)
	assert.NoError(t, cache.RecordRender(t.Context(), RenderAcceptance{Owner: spec.Named(), Receipt: rawReceipt}))

	testutil.WriteFile(t, root, "cm.yaml", cmYAML("changed"))
	accepted := cache.PreviousEntry(t.Context(), request)
	assert.Diff(t, accepted.Application, application)
	assert.Equal(t, accepted.Owner, evidence.Owner)
	assert.Equal(t, accepted.Producer, evidence.Producer)
	assert.Equal(t, accepted.Root, evidence.Root)
	st := store.New()
	st.SetArtifact(chart, artifact)
	st.AddDeclaration(store.Declaration{Obj: data, Origin: manifest.SuppliedDeclaration})
	assert.Equal(t, cache.EntryValid(t.Context(), accepted, st), true)
	for _, scenario := range []string{"unchanged", "changed", "removed", "repository precedence"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			current := store.New()
			current.SetArtifact(chart, artifact)
			if scenario != "removed" {
				value := "original"
				if scenario == "changed" || scenario == "repository precedence" {
					value = "changed"
				}
				current.AddDeclaration(store.Declaration{
					Obj: manifest.NewSecret(secret,
						nil,
						map[string]any{"VALUE": value}),
					Origin: manifest.SuppliedDeclaration,
				})
			}
			if scenario == "repository precedence" {
				current.AddObject(data, testutil.ObjectDocument(data))
			}
			assert.Equal(t,
				cache.EntryValid(t.Context(),
					accepted,
					current),
				scenario == "unchanged" || scenario == "repository precedence")
		})
	}
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
	missing, err := cache.ApplicationReceipt(t.Context(), request, evidence, []string{"generator.yaml"})
	assert.NoError(t, err)
	assert.Equal(t, missing.Receipt == nil, true)
	testutil.WriteFile(t, root, "generator.yaml", "changed producer input")
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Application == nil, true)
	testutil.WriteFile(t, root, "generator.yaml", "original producer input")
	testutil.WriteFile(t, root, "optional.yaml", "new producer input")
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Application == nil, true)
	cell.Retire()
	assert.Equal(t, cell.complete || cell.source != nil || cell.spec != nil || cell.lower != nil, false)
}

func TestEntryApplication_RequiresCompleteFrame(t *testing.T) {
	t.Parallel()
	for _, application := range []entryApplication{{}, {Complete: true}} {
		receipt := manifest.NewRenderReceipt(entrySnapshot{Application: application}, equalEntryApplication)
		err := newTestTreeCache(t, t.TempDir()).RecordEntry(t.Context(), receipt, nil)
		assert.Equal(t, err != nil, true)
	}
}

func TestRecordEntry_RequiresCurrentAcceptance(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, t.TempDir())
	if !cache.owner.CanPersistRenders() {
		t.Skip("requires a clean VCS-stamped build")
	}
	root := testutil.WriteTree(t, map[string]string{"producer.yaml": "accepted"})
	request := RenderRequest{Source: localSource(t, root), SubPath: "."}
	owner := manifest.NamedResource{
		Group: manifest.FluxKustomizeDomain,
		Kind:  manifest.KindKustomization,
		Name:  "owner",
	}
	evidence, err := cache.ApplicationReceipt(t.Context(), request, EntryEvidence{
		Producer: owner, Root: owner, Owner: owner, Records: emptyRenderInventory,
		Application: map[string]any{"kind": "Kustomization"},
	}, []string{"producer.yaml"})
	assert.NoError(t, err)
	st := store.New()
	object := &manifest.Kustomization{Name: "owner"}
	st.AddObject(object)
	var reads store.ReadSet
	reads.Init(st, owner, nil)
	reads.GetObject(owner)
	replaced := object.Clone()
	replaced.Path = "replacement"
	st.AddObject(replaced)
	assert.NoError(t, cache.RecordEntry(t.Context(), evidence.Receipt, &reads))
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Receipt == nil, true)
	st.Retire(&reads)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, cache.RecordEntry(ctx, evidence.Receipt, nil), context.Canceled)
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Receipt == nil, true)
	assert.NoError(t, cache.RecordEntry(t.Context(), evidence.Receipt, nil))
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Producer, owner)
}
