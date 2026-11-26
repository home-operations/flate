package loader

import (
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSourceFacts_FrozenComparisonSide(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"same material", "changed declaration", "changed origin", "unknown side"} {
		t.Run("unfrozen/"+name, func(t *testing.T) {
			testUnfrozenComparison(t, name)
		})
	}
	for side := range 2 {
		t.Run(strconv.Itoa(side), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st, peer := store.New(), store.New()
			before := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				Content:   "before",
				FS:        fstest.MapFS{},
			}
			st.SetArtifact(manifest.BootstrapSourceID, before)
			peer.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{
					Provider:  store.ProviderInput,
					LocalPath: root,
					Content:   "peer",
					FS:        fstest.MapFS{},
				},
			)
			l := New(st)
			l.SourceRoot = root
			_, err := l.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			observed := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, observed.Input, before)
			assert.Equal(t, observed.InitialKnown, false)
			assert.Equal(t, observed.Initial, (*store.SourceArtifact)(nil))
			input := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				Content:   "initial",
				FS:        fstest.MapFS{},
			}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
			assert.Equal(t, l.SourceFacts(manifest.BootstrapSourceID, nil).Context.SideKnown, false)
			stores := [2]*store.Store{st, peer}
			if side == 1 {
				stores[0], stores[1] = stores[1], stores[0]
			}
			l.Options.Changes, err = change.Qualify(
				t.Context(),
				detectChanges(t, []string{"changed.yaml"}, []string{"changed.yaml"}),
				change.SourcePair{Stores: stores},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			assert.Equal(t, l.scopes == nil && l.documents.parsed == nil && l.Options.ParsedInputs == nil, true)
			prior := l.Index
			completed, completionErr := l.CompleteDeclarations(
				t.Context(),
				selection.DemandDeclarations(manifest.TreeSide(side), l.Options.Changes, l.Index),
			)
			assert.NoError(t, completionErr)
			assert.Equal(t, completed.Declarations[manifest.BootstrapSourceID]["changed.yaml"].Complete, true)
			assert.Equal(t, len(prior.Declarations[manifest.BootstrapSourceID]), 0)
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			facts := l.SourceFacts(manifest.BootstrapSourceID, &reads)
			assert.Equal(t, facts.Context.SideKnown, true)
			assert.Equal(t, facts.Context.Side, manifest.TreeSide(side))
			assert.Equal(t, facts.Paired, true)
			assert.Equal(t, facts.CurrentSnapshot, true)
			assert.Equal(t, facts.InitialKnown, true)
			assert.Equal(t, facts.Initial, input)
			assert.Equal(t, facts.ConfiguredInput, (*store.SourceArtifact)(nil))
			l.Options.ConfiguredInput = input
			configured := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, configured.ConfiguredInput, input)
			assert.Equal(t, configured.Initial, input)
			assert.Equal(t, configured.Derived, false)
			l.Options.ConfiguredInput = nil
			unconfigured := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, unconfigured.ConfiguredInput, (*store.SourceArtifact)(nil))
			assert.Equal(t, unconfigured.Initial, input)
			assert.Equal(t, unconfigured.Derived, false)
			l.side, l.sideKnown = manifest.TreeSide(side), true
			otherOrigin := *input
			otherOrigin.Metadata = map[string]string{"org.opencontainers.image.revision": "other"}
			assert.Equal(t, reads.Valid(), true)
			st.SetArtifact(manifest.BootstrapSourceID, &otherOrigin)
			assert.Equal(t, reads.Valid(), false)
			rebound := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, rebound.Paired, true)
			assert.Equal(t, rebound.CurrentSnapshot, false)
			after := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: root,
				Content:   "replacement",
				FS:        fstest.MapFS{},
			}
			st.SetArtifact(manifest.BootstrapSourceID, after)
			assert.Equal(t, reads.Valid(), false)
			replacement := l.SourceFacts(manifest.BootstrapSourceID, nil)
			assert.Equal(t, replacement.Paired, false)
			assert.Equal(t, replacement.CurrentSnapshot, false)
			assert.Equal(t, replacement.Input, after)
			assert.Equal(t, replacement.InitialKnown, true)
			assert.Equal(t, replacement.Initial, input)
		})
		t.Run(fmt.Sprintf("producer_retry=%d", side), func(t *testing.T) {
			t.Parallel()
			testOriginOnlyProducerRetry(t, manifest.TreeSide(side))
		})
	}
	for _, availableSide := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
		t.Run(fmt.Sprintf("available=%d", availableSide), func(t *testing.T) {
			t.Parallel()
			root, mount := t.TempDir(), t.TempDir()
			source := &manifest.OCIRepository{Name: "bundle", Namespace: "apps"}
			id := source.Named()
			stores := [2]*store.Store{store.New(), store.New()}
			input := &store.SourceArtifact{LocalPath: mount, Content: "remote", FS: fstest.MapFS{}}
			stores[availableSide].SetArtifact(id, input)
			for _, st := range stores {
				st.SetArtifact(
					manifest.BootstrapSourceID,
					&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
				)
			}
			changes, err := change.Qualify(
				t.Context(),
				detectChanges(t, []string{"apps/b/cm.yaml"}, []string{"apps/b/cm.yaml"}),
				change.SourcePair{Stores: stores},
				manifest.WorkScope{},
			)
			assert.NoError(t, err)
			for side, st := range stores {
				l := New(st)
				l.SourceRoot, l.Options.Changes = root, changes
				l.SourceProvider = func(
					manifest.NamedResource, *store.ReadSet, *manifest.DiscoveryIndex,
				) store.SourceProvider {
					return store.ProviderFetch
				}
				l.Index.Records[id] = manifest.DiscoveryRecord{
					File: "source.yaml", Path: filepath.Join(root, "source.yaml"), Authored: id, Declaration: source,
				}
				l.seedSourceAuthority()
				l.frozen = true
				facts := l.SourceFacts(id, nil)
				assert.Equal(t, facts.Context.SideKnown, true)
				assert.Equal(t, facts.Context.Side, manifest.TreeSide(side))
				assert.Equal(t, facts.External, true)
				assert.Equal(t, l.Store.GetObject(id), manifest.BaseManifest(nil))
				assert.Equal(t, l.Index.IsFileIndexed(id), true)
				assert.Equal(t, l.Index.Records[id].Declaration, manifest.BaseManifest(source))
				if manifest.TreeSide(side) == availableSide {
					assert.Equal(t, facts.Path, mount)
					assert.Equal(t, facts.Input, input)
				} else {
					assert.Equal(t, facts.Path, "")
					assert.Equal(t, facts.Input, (*store.SourceArtifact)(nil))
					assert.Equal(t, facts.Delta, (*change.Set)(nil))
					assert.Equal(t, len(facts.Files), 0)
				}
			}
		})
	}
}

func testUnfrozenComparison(t *testing.T, name string) {
	t.Helper()
	t.Parallel()
	root := t.TempDir()
	base, head := store.New(), store.New()
	for _, st := range []*store.Store{base, head} {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
			Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{},
		})
	}
	declaration := &manifest.GitRepository{Name: "bundle", Namespace: "apps"}
	id := declaration.Named()
	input := &store.SourceArtifact{LocalPath: root, Content: "bundle", FS: fstest.MapFS{}}
	active, activeInput := declaration, input
	switch name {
	case "changed declaration":
		active = new(*declaration)
		active.URL = "https://example.test/other"
	case "changed origin":
		activeInput = new(*input)
		activeInput.Metadata = map[string]string{"org.opencontainers.image.revision": "other"}
	}
	base.AddObject(declaration)
	base.SetArtifact(id, input)
	head.AddObject(active)
	head.SetArtifact(id, activeInput)
	changes, err := change.Qualify(t.Context(), nil,
		change.SourcePair{Stores: [2]*store.Store{base, head}}, manifest.WorkScope{})
	assert.NoError(t, err)

	head.AddObject(declaration)
	head.SetArtifact(id, input)
	if name == "unknown side" {
		head.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
			Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{},
		})
	}
	l := New(head)
	t.Cleanup(l.Close)
	l.SourceRoot, l.Options.Changes = root, changes
	facts := l.SourceFacts(id, nil)
	assert.Equal(t, facts.Context.SideKnown, name != "unknown side")
	assert.Equal(t, facts.Paired, true)
	assert.Equal(t, facts.CurrentSnapshot, name == "same material")
}

func testOriginOnlyProducerRetry(t *testing.T, side manifest.TreeSide) {
	t.Helper()
	root := t.TempDir()
	stores := [2]*store.Store{store.New(), store.New()}
	for _, st := range stores {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{
			Provider: store.ProviderInput, LocalPath: root, Content: "checkout", FS: fstest.MapFS{},
		})
	}
	st := stores[side]
	source := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	producer := &manifest.RawObject{
		APIVersion: manifest.SourceExtensionsDomain + "/v1beta1",
		Kind:       manifest.KindArtifactGenerator, Name: "generator", Namespace: "apps",
	}
	id := producer.Named()
	st.AddObject(producer)
	l := New(st)
	t.Cleanup(l.Close)
	l.SourceRoot = root
	changes, err := change.Qualify(t.Context(), nil, change.SourcePair{Stores: stores}, manifest.WorkScope{})
	assert.NoError(t, err)
	l.Options.Changes = changes
	_, err = l.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, l.FinalizeIndex(t.Context(), root, true))
	configureSelection(t, l, selection.Request{Changed: true})

	var captured store.ReadSet
	captured.Init(st, id, nil)
	t.Cleanup(captured.Retire)
	facts := l.SourceFacts(manifest.BootstrapSourceID, &captured)
	assert.Equal(t, facts.Context.Side, side)
	assert.Equal(t, facts.CurrentSnapshot, true)
	emission := &store.Emission{Complete: true}
	captured.Bind(emission)
	st.RecordEmitted(id, emission, nil)

	var preparation *selection.Preparation
	t.Cleanup(func() { preparation.Retire() })
	options := selection.AdmissionOptions{
		Request: selection.PrepareRequest{
			Changes: changes, Preparation: &preparation, Invocation: true,
			LookupSources: []selection.LookupSource{{Materializer: id}},
		},
		Stage: selection.Construction,
	}
	initial, committed, err := l.Options.Session.Admit(t.Context(), options)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, len(initial.Demands), 0)

	replacement := *source
	replacement.Metadata = map[string]string{"org.opencontainers.image.revision": "different"}
	st.SetArtifact(manifest.BootstrapSourceID, &replacement)
	retry, committed, err := l.Options.Session.Admit(t.Context(), options)
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.FatalIf(t, len(retry.Demands) != 1, "origin-only retry produced %d demands, want one", len(retry.Demands))
	assert.Equal(t, retry.Demands[0].Identity, id)
	assert.Equal(t, retry.Demands[0].Materialize, true)
}

func TestSourceFacts_PerSourcePublication(t *testing.T) {
	t.Parallel()
	t.Run("indexed-only", func(t *testing.T) {
		t.Parallel()
		l := New(store.New())
		l.SourceRoot = t.TempDir()
		source := manifest.NamedResource{
			Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Name: "chart", Namespace: "apps",
		}
		other := source
		other.Name += "-other"
		consumer := manifest.NamedResource{
			Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "consumer", Namespace: "apps",
		}
		data := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
		for i := range 3 {
			id := consumer
			id.Name += strconv.Itoa(i)
			l.Index.Records[id] = manifest.DiscoveryRecord{
				Refs: []manifest.NamedResource{source, source, data, other, other, source},
			}
		}
		l.seedSourceAuthority()
		peer := l.sourceBinding(other)
		assert.FatalIf(t, peer == nil, "indexed peer did not initialize evidence")
		assert.Equal(t, peer.initialKnown, true)
		assert.Equal(t, l.sourceBinding(data) == nil, true)
		initial := l.sourceBinding(source)
		assert.FatalIf(t, initial == nil, "indexed source did not initialize evidence")
		assert.Equal(t, initial.initialKnown, true)
		assert.Equal(t, initial.initial, (*store.SourceArtifact)(nil))
		assert.Equal(t, initial.input, (*store.SourceArtifact)(nil))
		l.seedSourceAuthority()
		assert.Equal(t, l.sourceBinding(source), initial)
		l.frozen = true
		var absent store.ReadSet
		absent.Init(l.Store, consumer, nil)
		defer absent.Retire()
		missing := l.SourceFacts(source, &absent)
		assert.Equal(t, missing.Input, (*store.SourceArtifact)(nil))
		assert.Equal(t, missing.InitialKnown, true)
		assert.Equal(t, absent.Valid(), true)
		assert.Equal(t, l.sourceBinding(data) == nil, true)
		bucket := l.sourceBucket(source).Load()
		input := &store.SourceArtifact{
			Provider:  store.ProviderInput,
			LocalPath: l.SourceRoot,
			Content:   "chart",
			FS:        fstest.MapFS{},
		}
		l.Store.SetArtifact(source, input)
		assert.Equal(t, absent.Valid(), false)
		var reads [2]store.ReadSet
		for i := range reads {
			id := consumer
			id.Name += strconv.Itoa(i)
			reads[i].Init(l.Store, id, nil)
			t.Cleanup(reads[i].Retire)
		}
		first := l.SourceFacts(source, &reads[0])
		assert.Equal(t, first.Input, input)
		assert.Equal(t, first.Derived, true)
		second := l.SourceFacts(source, &reads[1])
		assert.Equal(t, second.Input, input)
		assert.Equal(t, second.Derived, false)
		assert.Equal(t, first.InitialKnown, true)
		assert.Equal(t, second.InitialKnown, true)
		assert.Equal(t, first.Initial, (*store.SourceArtifact)(nil))
		assert.Equal(t, second.Initial, (*store.SourceArtifact)(nil))
		for i := range reads {
			assert.Equal(t, slices.Contains(slices.Collect(reads[i].Inputs()), source), true)
			assert.Equal(t, reads[i].Valid(), true)
		}
		assert.Equal(t, initial.input, (*store.SourceArtifact)(nil))
		assert.Equal(t, l.sourceBucket(source).Load() == bucket, true)
	})
	for _, changed := range []string{"comparison", "declaration", "input"} {
		t.Run(changed, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			declaration := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			source := declaration.Named()
			input := &store.SourceArtifact{
				Provider:  store.ProviderInput,
				LocalPath: l.SourceRoot,
				Content:   "initial",
				FS:        fstest.MapFS{},
			}
			l.Store.AddObject(declaration)
			l.Store.SetArtifact(source, input)
			l.Index.CompleteSources = map[manifest.NamedResource]bool{source: true}
			l.Options.Changes = detectChanges(t, []string{"before.yaml"}, []string{"before.yaml"})
			observed := l.SourceFacts(source, nil)
			assert.Equal(t, observed.Input, input)
			assert.Equal(t, observed.InitialKnown, false)
			assert.Equal(t, observed.Initial, (*store.SourceArtifact)(nil))
			ordinary := l.sourceBinding(source)
			l.seedSourceAuthority()
			previous := l.sourceBinding(source)
			assert.Equal(t, previous == ordinary, false)
			assert.Equal(t, previous.initialKnown, true)
			assert.Equal(t, previous.initial, input)
			switch changed {
			case "comparison":
				l.Options.Changes = detectChanges(t, []string{"after.yaml"}, []string{"after.yaml"})
			case "declaration":
				replacement := *declaration
				replacement.URL = "https://fixture.invalid/replacement"
				l.Store.AddObject(&replacement)
			case "input":
				replacement := *input
				replacement.Content = "replacement"
				l.Store.SetArtifact(source, &replacement)
			}
			l.seedSourceAuthority()
			current := l.sourceBinding(source)
			assert.Equal(t, current == previous, false)
			assert.Equal(t, current.initialKnown, true)
			assert.Equal(t, current.initial, input)
			assert.Equal(t, current.declaration, l.Store.GetObject(source))
			assert.Equal(t, current.input, l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact))
			assert.Equal(t, current.delta, l.Options.Changes)
			assert.Equal(t, previous.input, input)
			assert.Equal(t, previous.declaration, manifest.BaseManifest(declaration))
			l.seedSourceAuthority()
			assert.Equal(t, l.sourceBinding(source), current)
		})
	}
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	ids := []manifest.NamedResource{
		{Kind: manifest.KindGitRepository, Name: "first", Namespace: "apps"},
		{Kind: manifest.KindGitRepository, Name: "second", Namespace: "apps"},
	}
	for _, id := range ids {
		l.SourceFacts(id, nil)
	}
	var bindings [64]*sourceBinding
	for i := range l.bindings {
		bindings[i] = l.bindings[i].Load()
	}
	previous := make([]*admissionSource, len(ids))
	for i, id := range ids {
		previous[i] = l.sourceBinding(id)
	}
	l.frozen = true
	var workers sync.WaitGroup
	for _, id := range ids {
		workers.Go(func() {
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: l.SourceRoot, FS: fstest.MapFS{}}
			l.Store.SetArtifact(id, input)
			facts := l.SourceFacts(id, nil)
			assert.Equal(t, facts.Input, input)
			assert.Equal(t, l.SourceFacts(id, nil).Derived, false)
		})
	}
	workers.Wait()
	for i := range l.bindings {
		assert.Equal(t, l.bindings[i].Load() == bindings[i], true)
	}
	for i, id := range ids {
		assert.Equal(t, previous[i].input, (*store.SourceArtifact)(nil))
		assert.Equal(t, l.sourceBinding(id) == previous[i], false)
	}
	newSource := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "runtime", Namespace: "apps"}
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: l.SourceRoot, FS: fstest.MapFS{}}
	l.Store.SetArtifact(newSource, input)
	assert.Equal(t, l.SourceFacts(newSource, nil).Input, input)
	for i := range l.bindings {
		assert.Equal(t, l.bindings[i].Load() == bindings[i], true)
	}
	count := 0
	for _, bucket := range bindings {
		for binding := bucket; binding != nil; binding = binding.next {
			count++
		}
	}
	assert.Equal(t, count, len(ids))
}

func TestSourceFacts_InitializationRoutes(t *testing.T) {
	t.Parallel()
	declaration := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	for _, tc := range []struct {
		name                       string
		source                     manifest.NamedResource
		complete, changed, indexed bool
	}{
		{name: "bootstrap", source: manifest.BootstrapSourceID},
		{name: "indexed-only", source: declaration.Named(), indexed: true},
		{name: "complete-only", source: declaration.Named(), complete: true},
		{name: "changed-only", source: declaration.Named(), changed: true},
		{name: "overlapping", source: declaration.Named(), complete: true, changed: true, indexed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = t.TempDir()
			source := tc.source
			if tc.complete {
				l.Index.CompleteSources = map[manifest.NamedResource]bool{source: true}
			}
			if tc.changed {
				peer := store.New()
				input := &store.SourceArtifact{
					Provider:  store.ProviderInput,
					LocalPath: l.SourceRoot,
					FS:        fstest.MapFS{},
				}
				for _, st := range []*store.Store{l.Store, peer} {
					st.AddObject(declaration)
					st.SetArtifact(source, input)
				}
				var err error
				l.Options.Changes, err = change.Qualify(
					t.Context(),
					detectChanges(t, nil, nil),
					change.SourcePair{Stores: [2]*store.Store{l.Store, peer}},
					manifest.WorkScope{},
				)
				assert.NoError(t, err)
				l.Store.SetArtifact(source, nil)
			}
			if tc.indexed {
				for i := range 3 {
					consumer := manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: strconv.Itoa(i)}
					l.Index.Records[consumer] = manifest.DiscoveryRecord{Refs: []manifest.NamedResource{source}}
				}
			}
			l.seedSourceAuthority()
			initial := l.sourceBinding(source)
			assert.FatalIf(t, initial == nil, "source route %s did not initialize evidence", tc.name)
			assert.Equal(t, initial.initialKnown, true)
			assert.Equal(t, initial.initial, (*store.SourceArtifact)(nil))
			assert.Equal(t, initial.input, (*store.SourceArtifact)(nil))
			l.seedSourceAuthority()
			assert.Equal(t, l.sourceBinding(source) == initial, true)
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: l.SourceRoot, FS: fstest.MapFS{}}
			l.Store.SetArtifact(source, input)
			l.seedSourceAuthority()
			current := l.sourceBinding(source)
			assert.Equal(t, current == initial, false)
			assert.Equal(t, current.input, input)
			assert.Equal(t, current.initial, (*store.SourceArtifact)(nil))
			assert.Equal(t, initial.input, (*store.SourceArtifact)(nil))
			l.seedSourceAuthority()
			assert.Equal(t, l.sourceBinding(source) == current, true)
		})
	}
}

func TestSourceFacts_SweepPublication(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	declaration := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	source := declaration.Named()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: l.SourceRoot, FS: fstest.MapFS{}}
	l.Store.AddObject(declaration)
	l.Store.SetArtifact(source, input)
	l.Index.CompleteSources = map[manifest.NamedResource]bool{source: true}
	l.seedSourceAuthority()
	initial := l.sourceBinding(source)
	l.SweepPreparations()
	assert.Equal(t, l.sourceBinding(source) == initial, true)
	replacement := *input
	replacement.Content = "replacement"
	l.Store.SetArtifact(source, &replacement)
	l.SweepPreparations()
	cleared := l.sourceBinding(source)
	assert.Equal(t, cleared == initial, false)
	assert.Equal(t, cleared.bound, false)
	assert.Equal(t, cleared.input, (*store.SourceArtifact)(nil))
	assert.Equal(t, cleared.declaration, manifest.BaseManifest(nil))
	assert.Equal(t, cleared.files == nil, true)
	assert.Equal(t, cleared.delta, (*change.Set)(nil))
	assert.Equal(t, cleared.initial, input)
	assert.Equal(t, initial.input, input)
	assert.Equal(t, initial.declaration, manifest.BaseManifest(declaration))
	l.SweepPreparations()
	assert.Equal(t, l.sourceBinding(source) == cleared, true)
	facts := l.SourceFacts(source, nil)
	assert.Equal(t, facts.Input, &replacement)
	assert.Equal(t, facts.Initial, input)
}

func TestSourceFacts_ChangedFileCoordinates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, origin, file, indexed string
		paired, pinned, mutable     bool
	}{
		{name: "same_root", origin: "/workspace/selected", file: "input.txt", indexed: "input.txt", paired: true},
		{name: "ancestor_root", origin: "/workspace", file: "selected/input.txt", indexed: "input.txt", paired: true},
		{name: "other_root", origin: "/other", file: "input.txt", indexed: "../../other/input.txt", paired: true},
		{name: "unpaired", origin: "/workspace", file: "selected/input.txt", indexed: "selected/input.txt"},
		{name: "normalized", origin: "/workspace/selected", file: "dir/../input.txt", indexed: "input.txt", paired: true},
		{name: "failed", origin: "relative", file: "input.txt", paired: true},
		{name: "pinned", origin: "/workspace", file: "input.txt", indexed: "../../mounted/input.txt", paired: true, pinned: true},
		{name: "mutable", origin: "/workspace", file: "selected/input.txt", indexed: "input.txt", paired: true, mutable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := New(store.New())
			l.SourceRoot = "/workspace/selected"
			file := filepath.ToSlash(filepath.Clean(tc.file))
			evidence := &admissionSource{
				source: manifest.BootstrapSourceID,
				delta:  detectChanges(t, []string{file}, []string{file}),
				input: &store.SourceArtifact{
					LocalPath: "/mounted",
					LocalRoot: tc.origin,
					Provider:  store.ProviderAlias,
				},
				origin: tc.origin,
				paired: tc.paired,
			}
			if tc.pinned {
				evidence.input.Provider, evidence.input.Content = store.ProviderCommit, "pinned"
			} else if tc.mutable {
				evidence.input.Provider = store.ProviderInput
			}
			facts := l.sourceViewFacts(evidence, true)
			assert.Diff(t, facts.Files, []selection.SourceFile{{Path: file, Indexed: tc.indexed}})
			if file != tc.file {
				indexed, _ := evidence.input.IndexPath(l.SourceRoot, tc.file)
				assert.Equal(t, indexed, tc.indexed)
			}
			again := l.sourceViewFacts(evidence, false)
			assert.Equal(t, &facts.Files[0], &again.Files[0])
			assert.Diff(t, evidence.delta.Paths(), []string{file})
		})
	}
}

func TestSourceFacts_PartitionsConstruction(t *testing.T) {
	t.Parallel()
	l := New(store.New())
	l.SourceRoot = t.TempDir()
	first := manifest.NamedResource{Kind: manifest.KindGitRepository, Name: "first", Namespace: "apps"}
	l.SourceFacts(first, nil)
	bucket := l.sourceBucket(first)
	original := bucket.Load()
	var workers sync.WaitGroup
	start := make(chan struct{})
	for i := range 128 {
		id := first
		id.Name = strconv.Itoa(i)
		if l.sourceBucket(id) == bucket {
			continue
		}
		workers.Go(func() {
			<-start
			facts := l.SourceFacts(id, nil)
			assert.Equal(t, facts.Source, id)
			assert.Equal(t, l.SourceFacts(id, nil).Derived, false)
		})
	}
	close(start)
	workers.Wait()
	assert.Equal(t, bucket.Load(), original)
	assert.Equal(t, original.next == nil, true)
	for i := range 128 {
		id := first
		id.Name = strconv.Itoa(i)
		if l.sourceBucket(id) != bucket {
			assert.Equal(t, l.sourceBinding(id).source, id)
		}
	}
}
