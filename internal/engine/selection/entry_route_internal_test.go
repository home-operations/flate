package selection

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestEntryRoute_InheritedAndGuarded(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"source", "declaration", "route", "root", "withdrawal", "publication", "retired-declaration", "equivalent-winners", "changed-winners", "published-empty"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			session := newPublicationSession(t)
			st := session.store
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: t.TempDir()}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "application", Namespace: "apps", Path: "entry"}
			st.AddObject(owner)
			guard := &store.ReadSet{}
			guard.Init(st, manifest.BootstrapSourceID, nil)
			guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			guard.GetDeclaration(owner.Named())
			defer st.Retire(guard)
			guard.Retire()
			defer guard.ReleaseCapture()
			binding := &store.ReadSet{}
			binding.Init(st, manifest.BootstrapSourceID, nil)
			binding.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			binding.GetDeclaration(owner.Named())
			binding.ReleaseCapture()
			defer st.Retire(binding)
			assert.Equal(t, binding.Valid(), true)
			route := &EntryRoute{
				Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
				Producer:    owner.Named(), Root: owner.Named(), Reads: guard, Bound: true,
			}
			occurrence := OccurrenceID{File: "entry/workload.yaml"}
			other := owner.Named()
			other.Name = "other"
			winners := map[string]*map[OccurrenceID]manifest.NamedResource{
				"equivalent-winners": new(map[OccurrenceID]manifest.NamedResource{occurrence: owner.Named()}),
				"changed-winners":    new(map[OccurrenceID]manifest.NamedResource{occurrence: other}),
				"published-empty":    new(map[OccurrenceID]manifest.NamedResource{}),
			}
			switch change {
			case "equivalent-winners", "changed-winners":
				route.Winners = new(map[OccurrenceID]manifest.NamedResource{occurrence: owner.Named()})
			}
			session.PublishEntry(route)
			equal := *route
			session.PublishEntry(&equal)
			assert.Equal(t, session.entry.Load(), route)
			proposal, err := session.prepareTest(t.Context(), PrepareRequest{})
			assert.NoError(t, err)
			assert.Equal(t, proposal.entry, route)
			assert.Equal(t, proposal.entrySupported(), true)
			refreshed, err := session.refreshHistoryTest(t.Context(), new(proposal.result()), PrepareRequest{})
			assert.NoError(t, err)
			assert.Equal(t, refreshed.entry, route)
			refreshed.Retire()

			if change == "retired-declaration" {
				proposal.Retire()
				var committed bool
				_, committed, err = session.publishProposal(t.Context(), proposal, Construction)
				assert.NoError(t, err)
				assert.Equal(t, committed, true)
			}
			prepared, changed := make(chan struct{}), make(chan struct{})
			go func() {
				<-prepared
				switch change {
				case "publication":
					st.SetIncluded(owner.Named(), false, false)
					st.SetIncluded(owner.Named(), true, true)
				case "source":
					st.SetArtifact(
						manifest.BootstrapSourceID,
						&store.SourceArtifact{
							Provider:  store.ProviderInput,
							LocalPath: input.LocalPath + "-replacement",
						},
					)
				case "declaration", "retired-declaration":
					replacement := *owner
					replacement.Path = "elsewhere"
					st.AddObject(&replacement)
				case "route":
					replacement := *route
					replacement.Producer.Name = "other"
					session.PublishEntry(&replacement)
				case "root":
					replacement := *route
					replacement.Root.Name = "other"
					session.PublishEntry(&replacement)
				case "withdrawal":
					session.PublishEntry(nil)
				case "equivalent-winners", "changed-winners", "published-empty":
					replacement := *route
					replacement.Winners = winners[change]
					session.PublishEntry(&replacement)
				}
				close(changed)
			}()
			close(prepared)
			<-changed
			_, committed, err := session.publishProposal(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, change == "equivalent-winners")
			if change == "equivalent-winners" {
				return
			}
			assert.Equal(t, session.Snapshot().version(), uint64(0))
			current, err := session.prepareTest(t.Context(), PrepareRequest{})
			assert.NoError(t, err)
			defer current.Retire()
			assert.Equal(t, current.entry, session.entry.Load())
			switch change {
			case "source", "declaration", "retired-declaration", "withdrawal":
				assert.Equal(t, current.entrySupported(), false)
			default:
				assert.Equal(t, current.entrySupported(), true)
			}
			_, committed, err = session.publishProposal(t.Context(), current, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
		})
	}
}

func TestEntryRoute_BoundWithoutSupport(t *testing.T) {
	t.Parallel()
	var full *Session
	full.PublishEntry(&EntryRoute{Bound: true})
	assert.Equal(t, full.Snapshot(), (*Decision)(nil))
	session := newPublicationSession(t)
	session.PublishEntry(&EntryRoute{
		Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
		Bound:       true,
	})
	proposal, err := session.prepareTest(t.Context(), PrepareRequest{})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, proposal.entrySupported(), false)
}

func TestEntryRoute_DemandUsesDistinctCheckout(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		provider store.SourceProvider
		shared   bool
		stale    bool
		want     bool
	}{
		{name: "distinct alias", provider: store.ProviderAlias, want: true},
		{name: "configured input", provider: store.ProviderInput},
		{name: "same mount", provider: store.ProviderAlias, shared: true},
		{name: "stale alias", provider: store.ProviderAlias, stale: true},
		{name: "pinned source", provider: store.ProviderCommit},
		{name: "foreign source", provider: store.ProviderFetch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			source := (&manifest.GitRepository{Name: "sync", Namespace: "apps"}).Named()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			observed := &store.SourceArtifact{Provider: tc.provider, LocalPath: root, FS: input.FS}
			if tc.shared {
				input = observed
			}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			if tc.stale {
				settings := &manifest.ConfigMap{Name: "settings", Namespace: "apps"}
				st.AddObject(settings)
				var reads store.ReadSet
				reads.Init(st, source, nil)
				defer reads.Retire()
				reads.GetDeclaration(settings.Named())
				observed = observed.WithReads(&reads)
				changed := *settings
				changed.Data = map[string]any{"changed": true}
				st.AddObject(&changed)
			}
			st.SetArtifact(source, observed)
			path := filepath.Join(root, "entry")
			facts := &solverFacts{store: st, context: SourceContext{Root: root, EntryPath: path, PlainEntry: true}}
			c := solverClosure(t, st, facts)
			c.context = facts.context
			c.proposal.preparation = new(Preparation)
			c.proposal.entry = &EntryRoute{
				Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
				Projected:   true,
			}
			id := (&manifest.Kustomization{Name: "workload", Namespace: "apps"}).Named()
			record := &manifest.DiscoveryRecord{Path: filepath.Join(path, "workload.yaml"), File: "entry/workload.yaml"}
			assert.Equal(t, c.demandEntryPath(id, source, record), tc.want)
			if tc.want {
				entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{})
				assert.Diff(t, c.proposal.demands, []Demand{{
					Identity: entry.Named(),
					Root:     "entry", Source: manifest.BootstrapSourceID,
				}})
			} else {
				assert.Equal(t, len(c.proposal.demands), 0)
			}
		})
	}
}

func TestEntryRoute_SettledRouteSkipsApplicationDemand(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	application := &manifest.Kustomization{Name: "workload", Namespace: "apps", Path: "./workload"}
	id := application.Named()
	occurrence := applicationOccurrence(application, root)
	occurrence.Mounted = input
	facts := &solverFacts{
		store:       st,
		context:     SourceContext{Root: root, EntryPath: filepath.Join(root, "entry")},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{id: occurrence},
	}
	c := solverClosure(t, st, facts)
	c.session.request.Roots = []manifest.NamedResource{id}
	c.context = facts.context
	c.identities = []admissionIdentity{{id: id}}
	var reads store.ReadSet
	reads.Init(st, manifest.BootstrapSourceID, nil)
	defer reads.Close()
	c.proposal.reads, c.objects = &reads, &reads

	c.prepareApplicationDemand(&EntryRoute{Bound: true, Selected: true}, false)

	assert.Equal(t, len(c.proposal.demands), 0)
	assert.Equal(t, len(c.entryPending), 0)
	assert.Equal(t, len(slices.Collect(reads.Inputs())), 0)
}

func TestEntryRoute_IndexOnlyApplicationOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		frozen bool
	}{
		{name: "live"},
		{name: "frozen", frozen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			root := t.TempDir()
			st.SetArtifact(manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
			)
			owner := (&manifest.Kustomization{Name: "factory", Namespace: "apps", Path: "./factory"}).Named()
			declaration := manifest.NamedResource{
				Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Name: "instance", Namespace: "apps",
			}
			entry := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).
				Named()
			record := manifest.DiscoveryRecord{
				Productions: []manifest.Production{{Owner: owner, Declaration: declaration}},
			}
			facts := &solverFacts{
				store:   st,
				context: SourceContext{Root: root, Frozen: tc.frozen},
				index: manifest.DiscoveryIndex{
					Records: map[manifest.NamedResource]manifest.DiscoveryRecord{entry: record},
				},
			}
			c := solverClosure(t, st, facts)
			c.context = facts.context
			assert.Equal(t, st.GetDeclaration(owner), manifest.BaseManifest(nil))
			assert.Equal(t, c.selectProductions(
				entry, &record, admissionRole{needed: true}, &declarationEvidence{},
			), true)
			assert.Equal(t, c.role(owner).needed, true)
			assert.Equal(t, c.role(owner).selected, false)
			assert.Equal(t, c.role(owner).dirty, false)
		})
	}
}

func TestEntryRoute_ResourceSetApplicationOwner(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		namespace string
	}{
		{name: "authored namespace", namespace: "apps"},
		{name: "bound namespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			producer := &manifest.ResourceSet{Name: "factory", Namespace: tc.namespace}
			producer.ResourcesTemplate = `apiVersion: fluxcd.controlplane.io/v1
kind: FluxInstance
metadata: {name: flux}
spec:
  sync: {name: self, kind: GitRepository, url: https://fixture.invalid/repo, ref: refs/heads/main, path: ./flux}
`
			owner := producer.WithNamespace("apps").Named()
			projection, projected := manifest.ProjectProducer(producer, nil)
			assert.Equal(t, projected, true)
			occurrence := applicationOccurrence(producer, root)
			occurrence.Identity = owner
			occurrence.TemplateObjects, occurrence.TemplateInputs = projection.Objects, projection.Inputs
			occurrence.Targets = append(occurrence.Targets, projection.Targets...)
			facts := &solverFacts{
				store: st, context: SourceContext{Root: root},
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					owner: {Authored: producer.Named()},
				}},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{owner: occurrence},
			}
			c := solverClosure(t, st, facts)
			c.context = facts.context
			instance := manifest.NamedResource{
				Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Name: "flux", Namespace: "apps",
			}
			assert.Equal(t, c.materializers(materializerDemand{
				target: instance, source: root, required: true,
			}, manifest.NamedResource{}), true)
			assert.Equal(t, c.role(owner).needed, true)
			assert.Equal(t, c.role(owner).selected, false)
			assert.Equal(t, c.role(owner).dirty, false)
			owner.Namespace = ""
			assert.Equal(t, c.role(owner).needed, false)
		})
	}
}
