package kustomize

import (
	"bytes"
	"context"
	"maps"
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
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	assert.Equal(t, cache.owner.CanPersistRenders(), persistent)
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
	if persistent {
		assert.Diff(t, accepted.Application, application)
		assert.Equal(t, accepted.Owner, evidence.Owner)
		assert.Equal(t, accepted.Producer, evidence.Producer)
		assert.Equal(t, accepted.Root, evidence.Root)
	} else {
		assert.Diff(t, accepted, EntryEvidence{})
	}
	st := store.New()
	st.SetArtifact(chart, artifact)
	st.AddDeclaration(store.Declaration{Obj: data, Origin: manifest.SuppliedDeclaration})
	assert.Equal(t, cache.EntryValid(t.Context(), accepted, st), persistent)
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
				persistent && (scenario == "unchanged" || scenario == "repository precedence"))
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

func TestPreviousEntry_SourceBinding(t *testing.T) {
	t.Parallel()
	for _, lifetime := range []string{"retained", "transferred"} {
		t.Run(lifetime, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{"generator.yaml": "unchanged application input"})
			renderer := newTestTreeCache(t, t.TempDir())
			if !renderer.owner.CanPersistRenders() {
				t.Skip("entry receipt requires a clean VCS-stamped build")
			}
			var preparation, successor Preparation
			t.Cleanup(preparation.Retire)
			t.Cleanup(successor.Retire)
			source := localSource(t, root)
			source.Content, source.Revision, source.URL = "equal-entry-content", "same-revision", "https://example.test/before"
			request := RenderRequest{Source: source, SubPath: ".", Preparation: &preparation}
			before, err := renderer.prepareRender(t.Context(), &request)
			assert.NoError(t, err)
			entryBefore, err := renderer.prepareEntry(t.Context(), &request)
			assert.NoError(t, err)
			owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "producer"}
			evidence := EntryEvidence{
				Producer: owner, Owner: owner, Root: owner, Records: emptyRenderInventory,
				Application: map[string]any{"kind": "Kustomization"},
			}
			first, err := renderer.ApplicationReceipt(t.Context(), request, evidence, []string{"generator.yaml"})
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordEntry(t.Context(), first.Receipt, nil))
			assert.Equal(t, renderer.PreviousEntry(t.Context(), request).Producer, owner)
			prior := first.Receipt.Value().(entrySnapshot)
			var priorWire bytes.Buffer
			_, err = prior.publication.WriteTo(&priorWire)
			assert.NoError(t, err)
			if lifetime == "transferred" {
				assert.Equal(t, successor.Take(&preparation, renderer, &request), true)
				assert.Equal(t, preparation.complete || preparation.source != nil, false)
				request.Preparation = &successor
			}
			replacement := *source
			replacement.URL = "https://example.test/after"
			request.Source = &replacement
			after, err := renderer.prepareRender(t.Context(), &request)
			assert.NoError(t, err)
			assert.Equal(t, after.key, before.key)
			assert.Equal(t, after.binding.URL, replacement.URL)
			assert.Equal(t, after.binding.Revision, source.Revision)
			entryAfter, err := renderer.prepareEntry(t.Context(), &request)
			assert.NoError(t, err)
			assert.Equal(t, entryAfter.key == entryBefore.key, false)
			assert.Equal(t, renderer.PreviousEntry(t.Context(), request).Receipt == nil, true)
			current, err := renderer.ApplicationReceipt(t.Context(), request, evidence, []string{"generator.yaml"})
			assert.NoError(t, err)
			assert.NoError(t, renderer.RecordEntry(t.Context(), current.Receipt, nil))
			accepted := renderer.PreviousEntry(t.Context(), request)
			assert.Equal(t, accepted.Producer, owner)
			assert.Diff(t, accepted.Application, evidence.Application)
			snapshot := accepted.Receipt.Value().(entrySnapshot)
			assert.Equal(t, snapshot.Key, entryAfter.key)
			assert.Equal(t, snapshot.Binding, manifest.Fingerprint(after.binding))
			unchanged := first.Receipt.Value().(entrySnapshot)
			assert.Equal(t, unchanged.Key, entryBefore.key)
			assert.Equal(t, unchanged.Binding, manifest.Fingerprint(before.binding))
			var unchangedWire bytes.Buffer
			_, err = unchanged.publication.WriteTo(&unchangedWire)
			assert.NoError(t, err)
			assert.Equal(t, bytes.Equal(unchangedWire.Bytes(), priorWire.Bytes()), true)
			assert.Equal(t, source.URL, before.binding.URL)
		})
	}
}

func TestEntryApplication_RequiresCompleteFrame(t *testing.T) {
	t.Parallel()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	for _, application := range []entryApplication{{}, {Complete: true}} {
		receipt := manifest.NewRenderReceipt(entrySnapshot{Application: application}, equalEntryApplication)
		err := newTestTreeCache(t, t.TempDir()).RecordEntry(t.Context(), receipt, nil)
		assert.Equal(t, err != nil, persistent)
	}
}

func TestRecordEntry_RequiresCurrentAcceptance(t *testing.T) {
	t.Parallel()
	cache := newTestTreeCache(t, t.TempDir())
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	assert.Equal(t, cache.owner.CanPersistRenders(), persistent)
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
	wantOwner := manifest.NamedResource{}
	if persistent {
		wantOwner = owner
	}
	assert.Equal(t, cache.PreviousEntry(t.Context(), request).Producer, wantOwner)
}

func TestApplicationReceipt_FrameHistory(t *testing.T) {
	t.Parallel()
	root := testutil.WriteTree(t, map[string]string{"producer.yaml": "accepted"})
	cache := newTestTreeCache(t, "")
	request := RenderRequest{Source: localSource(t, root), SubPath: "."}
	owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "owner"}
	child := manifest.NamedResource{Kind: "ConfigMap", Name: "child"}
	evidence := EntryEvidence{
		Producer: owner, Root: owner, Owner: owner,
		Application: map[string]any{"kind": "Kustomization"},
		Records:     maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Path: "first"}}),
	}
	first, err := cache.ApplicationReceipt(t.Context(), request, evidence, []string{"producer.yaml"})
	assert.NoError(t, err)
	assert.Equal(t, maps.Collect(first.Records)[child].Path, "first")
	before := first.Receipt.Value().(entrySnapshot)
	_, complete := before.publication.Frame().History(owner, before)
	assert.Equal(t, complete, true)
	changed := before
	changed.Application.Application = map[string]any{"kind": "different"}
	_, complete = before.publication.Frame().History(owner, changed)
	assert.Equal(t, complete, false)
	assert.Equal(t, equalEntryApplication(before, before), true)
	assert.Equal(t, equalEntryApplication(before, changed), false)
	evidence.Records = maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{child: {Path: "second"}})
	second, err := cache.ApplicationReceipt(t.Context(), request, evidence, []string{"producer.yaml"})
	assert.NoError(t, err)
	after := second.Receipt.Value().(entrySnapshot)
	assert.Equal(t, manifest.Fingerprint(before), manifest.Fingerprint(after))
	assert.Equal(t, before.publication.Identity() == after.publication.Identity(), false)
	assert.Equal(t, equalEntryApplication(before, after), false)
	assert.Equal(t, maps.Collect(second.Records)[child].Path, "second")
	assert.Equal(t, maps.Collect(first.Records)[child].Path, "first")
	_, complete = before.publication.Frame().History(owner, before)
	assert.Equal(t, complete, true)
}
