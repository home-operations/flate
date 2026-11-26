package discovery

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdvance_RepeatedProducerPreparation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	root, err := filepath.Abs("../../../testdata/issue-1680-advance-loop/repo")
	assert.NoError(t, err)
	input := &store.SourceArtifact{
		Provider: store.ProviderInput, LocalPath: root, LocalRoot: root, FS: os.DirFS(root),
	}
	st := store.New()
	l := loader.New(st)
	t.Cleanup(l.Close)
	edited := "kubernetes/apps/demo/app/manifests/kustomization.yaml"
	l.Options.Changes = detectChanges(t, []string{edited}, []string{edited})
	request := selection.Request{Changed: true}
	configureSelection(t, l, request)
	d, err := beginTest(ctx, testConfig(t, Config{
		Input:     input,
		Path:      filepath.Join(root, "kubernetes", "flux", "cluster"),
		Store:     st,
		Loader:    l,
		Selection: request,
	}))
	assert.NoError(t, err)
	defer d.Close()
	st.SetDiscoveryIndex(l.Index)
	owner := (&manifest.Kustomization{Name: "cluster-apps", Namespace: "flux-system"}).Named()
	child := (&manifest.Kustomization{Name: "demo", Namespace: "demo"}).Named()
	consumer := (&manifest.HelmRelease{Name: "demo", Namespace: "demo"}).Named()

	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	reads.RequireData(consumer)
	reads.RequireData(owner)
	indexed, _ := l.Index.Lookup(consumer)
	production := manifest.Production{
		Owner: owner, Declaration: owner, Authored: consumer,
		Source: (&manifest.GitRepository{Name: "flux-system", Namespace: "flux-system"}).Named(),
		File:   indexed.File,
	}
	// Keep the pending HelmRelease relation from the paired comparison census.
	record := manifest.DiscoveryRecord{
		Builder:      edited,
		BuilderOwner: child,
		File:         indexed.File,
		Productions:  []manifest.Production{production},
	}
	l.Index.Records[consumer] = record
	var preparation *selection.Preparation
	defer func() { preparation.Retire() }()
	req := selection.PrepareRequest{
		Reads: &reads, Invocation: true, Preparation: &preparation,
	}
	accepted, err := admitSelection(ctx, t, l, req)
	assert.NoError(t, err)
	d.request = accepted.Request
	d.constructionStarted = true
	_, err = d.observeRequiredObjects(ctx, accepted)
	assert.NoError(t, err)
	_, err = d.prepareDemandedProducers(ctx, accepted.Demands, root)
	assert.NoError(t, err)

	initial, err := l.Options.Session.RefreshProducers(ctx, accepted, d.request)
	assert.NoError(t, err)
	progress, err := d.prepareDemandedProducers(ctx, slices.Collect(initial.Demands()), root)
	initial.Retire()
	assert.NoError(t, err)
	assert.Equal(t, progress, true)
	frame := d.prepared[owner]
	assert.FatalIf(t, frame == nil, "missing initial preparation of %s", owner)

	settings := (&manifest.ConfigMap{Name: "cluster-settings", Namespace: "flux-system"}).Named()
	var settingsReads store.ReadSet
	settingsReads.Init(st, owner, nil)
	defer settingsReads.Retire()
	settingsReads.RequireData(settings)
	_, err = admitSelection(ctx, t, l, selection.PrepareRequest{Reads: &settingsReads})
	assert.NoError(t, err)
	material := st.GetObject(settings)
	assert.FatalIf(t, material == nil, "missing prepared input %s", settings)
	assert.Equal(t, frame.support.Valid(), false)

	for pass := range 2 {
		frontier, refreshErr := l.Options.Session.RefreshProducers(ctx, accepted, accepted.Request)
		assert.NoError(t, refreshErr)
		demands := slices.Collect(frontier.Demands())
		assert.Equal(t, slices.ContainsFunc(demands, func(demand selection.Demand) bool {
			return demand.Identity == owner
		}), true)
		progress, prepareErr := d.prepareDemandedProducers(ctx, demands, root)
		frontier.Retire()
		assert.NoError(t, prepareErr)
		frame := d.prepared[owner]
		assert.FatalIf(t, frame == nil, "pass %d: producer %s missing for %s", pass+1, owner, consumer)
		preparedIndex := slices.IndexFunc(frame.children, func(doc store.Child) bool {
			return doc.Obj.Named() == child
		})
		assert.FatalIf(t, preparedIndex < 0, "pass %d: missing prepared child %s", pass+1, child)
		published, _ := l.PreparedObject(child, nil)
		assert.Equal(t, frame.children[preparedIndex].Obj, published.Obj)
		registered := len(l.PromotionDocuments(child)) != 0
		if progress != (pass == 0) || !frame.support.Valid() || !registered {
			t.Errorf("pass %d: %s prepared %s for %s from %s (complete=%v registered=%v current=%v progress=%v)",
				pass+1, owner, child, consumer, production.Source, frame.complete, registered,
				frame.support.Valid(), progress,
			)
		}
		assert.Equal(t, st.GetObject(settings), material)
	}
}

func TestRegisterProducer_KeepsReusedDeclarations(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: os.DirFS(root)}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, input)
	d := newDiscoverer(&Config{Input: input, Path: root, Store: st})
	t.Cleanup(d.Close)
	d.loader.SourceRoot = root
	d.prepared = make(map[manifest.NamedResource]*declarationPreparation)

	owner := &manifest.Kustomization{Name: "parent", Namespace: "apps", Path: "."}
	originals := []*manifest.Kustomization{
		{Name: "first", Namespace: "apps", Path: "./first"},
		{Name: "second", Namespace: "apps", Path: "./second"},
	}

	for pass := range 2 {
		frame := &declarationPreparation{
			owner: owner, source: input,
			complete: true,
		}
		for i, original := range originals {
			child := original
			if pass == 1 {
				child = original.Clone()
			}
			frame.children = append(frame.children, store.Child{Obj: child, Ordinal: i})
		}
		frame.captureSupport(st, owner.EffectiveSourceID())

		_, err := d.registerProducer(t.Context(), frame)
		assert.NoError(t, err)
		assert.Equal(t, frame.publication, loader.BuiltRegistered)
		assert.FatalIf(t, len(frame.children) != len(originals),
			"pass %d: prepared children = %v", pass+1, frame.children)

		for i, original := range originals {
			id := original.Named()
			indexed, ok := d.loader.Index.Lookup(id)
			assert.FatalIf(t, !ok || indexed.Declaration != original, "pass %d: loader did not retain %s", pass+1, id)
			assert.ErrorIf(t, frame.children[i].Obj != indexed.Declaration,
				"pass %d: prepared child %s does not use the indexed declaration", pass+1, id)
		}
	}
}
