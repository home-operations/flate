package selection

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCaptureRoot_ProspectiveSupport(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		complete bool
		replace  bool
	}{
		{name: "replacement", complete: true, replace: true},
		{name: "complete omission", complete: true},
		{name: "incomplete omission"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			root := t.TempDir()
			oldInput := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}}
			st.SetArtifact(manifest.BootstrapSourceID, oldInput)
			owner := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
			st.AddObject(owner)
			old := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "old"}
			id := old.Named()
			st.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: old}}, Complete: true}, nil)
			index := manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Path: filepath.Join(root, "child.yaml"), File: "child.yaml", Authored: id},
			}}
			facts := &solverFacts{store: st, index: index}
			c := solverClosure(t, st, facts)
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			c.objects, c.proposal.reads = &reads, &reads
			newInput := &store.SourceArtifact{
				LocalPath: t.TempDir(),
				LocalRoot: root,
				Provider:  store.ProviderAlias,
				FS:        fstest.MapFS{},
			}
			c.sourceReads = &reads
			c.sourceFacts = map[manifest.NamedResource]*SourceFacts{
				manifest.BootstrapSourceID: {Input: newInput, Context: SourceContext{Root: root}},
			}
			c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
				owner.Named(): {},
			}
			c.replace = map[manifest.NamedResource]bool{owner.Named(): tc.complete}
			replacement := old.Clone()
			replacement.Path = "new"
			if tc.replace {
				application, err := json.Marshal(map[string]any{
					"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
					"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace},
					"spec":     map[string]any{"path": "new"},
				})
				assert.NoError(t, err)
				c.accepted[owner.Named()][id] = manifest.DiscoveryRecord{
					Application: string(
						application,
					),
					Productions: []manifest.Production{{Owner: owner.Named(), Source: manifest.BootstrapSourceID}},
				}
				c.generation = []store.Child{{Obj: replacement}}
				c.states[id] = admissionState{incoming: 1}
			}
			evidence := RootEvidence{
				Identity: id, Incoming: tc.replace, Record: RootRecordFrom(new(index.Records[id])),
				Occurrence: OccurrenceFacts{Declaration: OccurrenceID{Source: root, File: "child.yaml"}},
				Scope:      RootScopeFacts{NamespaceEligible: true, Initial: true},
			}
			c.capturedRoots = map[manifest.NamedResource]*capturedRootEvidence{id: {physical: evidence}}
			reader := &admissionRootReader{admissionReader: &reads, closure: &c}
			captured := CaptureRoot(evidence, reader, &index)
			assert.Equal(t, captured.Sources[0].Path, newInput.LocalPath)
			assert.Equal(t, captured.Sources[0].Origin, root)
			assert.Equal(t, slices.ContainsFunc(captured.LiveProductions, func(p manifest.Production) bool {
				return p.Owner == owner.Named()
			}), !tc.complete || tc.replace)
			if tc.replace {
				assert.Equal(t, captured.Declaration, manifest.BaseManifest(replacement))
				assert.Equal(t, len(captured.CoveringOwners), 1)
				assert.Equal(t, captured.CoveringOwners[0].Replaced, true)
				assert.Equal(
					t,
					captured.CoveringOwners[0].Applications[id].Application,
					c.accepted[owner.Named()][id].Application,
				)
			}
			assert.Equal(t, Root(captured).Independent, tc.complete && !tc.replace)
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, st.GetDeclaration(id), manifest.BaseManifest(old))
			if tc.replace {
				original := captured.CoveringOwners[0].Applications[id].Application
				revised := c.accepted[owner.Named()][id]
				revised.Application += "\n"
				c.accepted[owner.Named()] = map[manifest.NamedResource]manifest.DiscoveryRecord{id: revised}
				latest := CaptureRoot(evidence, reader, &index)
				assert.Equal(t, captured.CoveringOwners[0].Applications[id].Application, original)
				assert.Equal(t, latest.CoveringOwners[0].Applications[id].Application, revised.Application)
				assert.Equal(t, Root(latest).Independent, false)
			}
		})
	}
}

func TestCaptureRoot_ProspectiveComponentClaim(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	st.SetArtifact(
		manifest.BootstrapSourceID,
		&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
	)
	factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
	st.AddObject(factory)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "app"}
	id := manifest.NamedResource{
		Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "child", Namespace: "apps",
	}
	productions := []manifest.Production{{Owner: factory.Named(), Source: manifest.BootstrapSourceID}}
	facts := &solverFacts{store: st, index: manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			id: {Path: filepath.Join(root, "components", "child.yaml"), File: "components/child.yaml"},
		},
		Components: map[string][]string{filepath.Join(root, "app"): {"../components"}},
	}}
	facts.context.Root = root
	c := solverClosure(t, st, facts)
	var reads store.ReadSet
	reads.Init(st, manifest.NamedResource{}, nil)
	defer reads.Retire()
	c.objects, c.proposal.reads = &reads, &reads
	c.generation = []store.Child{{Obj: owner, Productions: productions}}
	c.states[owner.Named()] = admissionState{incoming: 1, incomingProductions: productions}
	application, err := json.Marshal(map[string]any{
		"apiVersion": manifest.FluxKustomizeDomain + "/v1", "kind": manifest.KindKustomization,
		"metadata": map[string]any{"name": owner.Name, "namespace": owner.Namespace},
		"spec":     map[string]any{"path": owner.Path},
	})
	assert.NoError(t, err)
	c.accepted = map[manifest.NamedResource]map[manifest.NamedResource]manifest.DiscoveryRecord{
		factory.Named(): {owner.Named(): {
			Application: string(application), Declaration: owner, Productions: productions,
		}},
	}
	c.replace = map[manifest.NamedResource]bool{factory.Named(): true}
	evidence := RootEvidence{
		Identity: id, Record: RootRecordFrom(new(facts.index.Records[id])),
		Occurrence: OccurrenceFacts{Declaration: OccurrenceID{Source: root, File: "components/child.yaml"}},
		Scope:      RootScopeFacts{NamespaceEligible: true, Initial: true},
	}
	captured := CaptureRoot(evidence, c.rootReader(), &facts.index)
	assert.Equal(t, len(captured.CoveringOwners), 2)
	assert.Equal(t, captured.CoveringOwners[1].Identity, factory.Named())
	assert.Diff(t, captured.CoveringOwners[0].Productions, productions)
	assert.Equal(t, captured.CoveringOwners[0].covering, true)
	assert.Equal(t, Root(captured).Owner, owner.Named())
	assert.Equal(t, Root(captured).Independent, false)
	assert.Equal(t, st.GetObject(owner.Named()), manifest.BaseManifest(nil))
	assert.Equal(t, reads.Valid(), true)
}

func TestCaptureRoot_ApplicationClaimPeers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		prefix  string
		indexed bool
		peer    bool
	}{
		{name: "no claims"},
		{name: "root claim belongs elsewhere", indexed: true},
		{name: "matching peers", prefix: "apps/", indexed: true, peer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			root := t.TempDir()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{}},
			)
			application := &manifest.Kustomization{Name: "application", Namespace: "apps", Path: "apps"}
			peer := &manifest.Kustomization{Name: "peer", Namespace: "apps"}
			index := manifest.DiscoveryIndex{Claims: map[string][]manifest.NamedResource{}}
			if tc.indexed {
				index.Claims[tc.prefix] = []manifest.NamedResource{application.Named(), peer.Named()}
			}
			facts := &solverFacts{store: st, index: index}
			facts.context.Root = root
			c := solverClosure(t, st, facts)
			evidence := RootEvidence{Identity: application.Named(), Scope: RootScopeFacts{Claims: index.Claims}}
			c.captureRootApplication(&evidence, application)
			assert.Equal(t, evidence.Scope.LocalApplication, true)
			assert.Equal(t, publicRootPeer(&evidence, peer.Named()), tc.peer)
		})
	}
}

func TestClaimContains_SourceOrigin(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"fleet", "flux-system"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := (&manifest.GitRepository{Name: name, Namespace: "flux-system"}).Named()
			local := (&manifest.Kustomization{Name: "checkout", Namespace: "apps"}).Named()
			pinned := (&manifest.Kustomization{Name: "pinned", Namespace: "apps"}).Named()
			index := &manifest.DiscoveryIndex{
				Claims: map[string][]manifest.NamedResource{
					"": {local}, "apps/": {pinned}, "../mount/apps/": {pinned},
				},
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{pinned: {Pinned: true}},
			}
			checkout := RootCoveringOwner{
				Identity: local, Source: source,
				Input: &store.SourceArtifact{Provider: store.ProviderAlias, FS: fstest.MapFS{}},
			}
			mounted := RootCoveringOwner{
				Identity: pinned, Source: source,
				Input: &store.SourceArtifact{Provider: store.ProviderCommit, FS: fstest.MapFS{}},
			}
			subject := manifest.NamedResource{}
			assert.Equal(t, claimContains(index, "apps/file.yaml", subject, &checkout), true)
			checkout.Identity = pinned
			assert.Equal(t, claimContains(index, "apps/file.yaml", subject, &checkout), false)
			assert.Equal(t, claimContains(index, "../mount/apps/file.yaml", subject, &mounted), true)
			mounted.Identity = local
			assert.Equal(t, claimContains(index, "../mount/apps/file.yaml", subject, &mounted), false)
		})
	}
}
