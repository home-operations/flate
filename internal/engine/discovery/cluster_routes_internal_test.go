package discovery

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPlan_IndirectAnchorReplay(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		provider store.SourceProvider
		anchors  []bootstrapAnchor
	}{
		{
			name:     "checkout",
			provider: store.ProviderAlias,
			anchors: []bootstrapAnchor{
				{target: "entry", source: (&manifest.GitRepository{Name: "content", Namespace: "apps"}).Named()},
				{target: "outside"},
			},
		},
		{name: "unavailable seed target", provider: store.ProviderAbsent},
		{name: "fetched seed target", provider: store.ProviderFetch, anchors: []bootstrapAnchor{{target: "outside"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, result := indirectAnchorReplay(t, tc.provider)
			d := p.session
			st := d.cfg.Store
			bootstrap := st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			assert.NoError(t, p.start(t.Context(), &result))
			err := p.reach(t.Context(), &result)
			if errors.Is(err, errPendingSources) {
				assert.NoError(t, constructTestSources(t.Context(), d))
				err = p.reach(t.Context(), &result)
			}
			assert.NoError(t, err)
			assert.Equal(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent), bootstrap)
			slices.SortFunc(
				result.anchors,
				func(a, b bootstrapAnchor) int { return strings.Compare(a.target, b.target) },
			)
			assert.FatalIf(
				t,
				len(result.anchors) != len(tc.anchors),
				"anchors = %+v, want %+v",
				result.anchors,
				tc.anchors,
			)
			for i, anchor := range result.anchors {
				assert.Equal(t, anchor, tc.anchors[i])
			}
			var members []manifest.NamedResource
			for _, occurrence := range result.Occurrences {
				if !occurrence.Identity.KindInfo().IsSource() {
					members = append(members, occurrence.Identity)
				}
			}
			slices.SortFunc(members, manifest.NamedResource.Compare)
			assert.Diff(t, slices.Compact(members), []manifest.NamedResource{
				{Kind: "ConfigMap", Name: "marker"},
				testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b"),
				testutil.KustomizationID("apps", "c"),
			})
		})
	}
}

func indirectAnchorReplay(t *testing.T, provider store.SourceProvider) (*entryPlanner, clusterRoot) {
	t.Helper()
	root := t.TempDir()
	const localURL = "https://fixture.invalid/local"
	const externalURL = "https://fixture.invalid/external"
	content := &manifest.GitRepository{Name: "content", Namespace: "apps", URL: externalURL}
	files := fstest.MapFS{
		"outside/a.yaml": {Data: []byte(testutil.KustomizationYAML("a", "apps",
			"  path: ./entry\n  sourceRef: {kind: GitRepository, name: content}\n"))},
		"entry/b.yaml":      {Data: []byte(testutil.KustomizationYAML("b", "apps", "  path: ./outside\n"))},
		"entry/marker.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker}\n")},
		"other/c.yaml":      {Data: []byte(testutil.KustomizationYAML("c", "apps", "  path: ./entry\n"))},
	}
	var declarations []store.Declaration
	if provider == store.ProviderAlias {
		const sourceYAML = "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\n" +
			"metadata: {name: content, namespace: apps}\nspec: {url: "
		files["catalog/source.yaml"] = &fstest.MapFile{Data: []byte(sourceYAML + localURL + "}\n")}
		files["outside/source.yaml"] = &fstest.MapFile{Data: []byte(sourceYAML + externalURL + "}\n")}
	} else {
		declarations = []store.Declaration{{Obj: content}}
	}
	st := store.New()
	if provider == store.ProviderFetch {
		// An independently supplied artifact can use the same declaration coordinates.
		st.SetArtifact(content.Named(), &store.SourceArtifact{
			Provider: store.ProviderFetch, LocalPath: root, FS: files,
		})
	}
	d, err := Begin(t.Context(), Config{
		Path: root, Input: &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files},
		Store: st, SelfURLs: []string{localURL}, Declarations: declarations,
	})
	assert.NoError(t, err)
	t.Cleanup(d.Close)
	assert.NoError(t, constructTestSources(t.Context(), d))
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	var result clusterRoot
	for _, dir := range []string{"outside", "entry", "other"} {
		occurrences, occurrenceErr := d.loader.Occurrences(t.Context(), filepath.Join(root, dir), "")
		assert.NoError(t, occurrenceErr)
		for i := range occurrences {
			if occurrences[i].Identity.Kind == manifest.KindKustomization {
				result.seeds = append(result.seeds, occurrences[i])
			}
		}
	}
	p := &entryPlanner{
		session: d, inputs: map[string]*store.SourceArtifact{root: d.cfg.Input},
		coordinates: map[selection.OccurrenceID]bool{}, seen: map[occurrenceKey]rootVisitState{},
	}
	if provider == store.ProviderAlias {
		p.catalog.occurrences, err = d.loader.Occurrences(t.Context(), filepath.Join(root, "catalog"), "")
		assert.NoError(t, err)
	}
	return p, result
}

func TestPlan_IndirectAnchorScopedSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const localURL = "https://fixture.invalid/local"
	files := fstest.MapFS{
		"entry/bootstrap/seed.yaml": {Data: []byte(testutil.KustomizationYAML("seed", "apps",
			"  path: ./fanout\n"))},
		"entry/kustomization.yaml": {Data: []byte("resources: [bootstrap/seed.yaml, marker.yaml]\n")},
		"entry/marker.yaml":        {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: marker}\n")},
		"shared/return.yaml": {Data: []byte(testutil.KustomizationYAML("return", "apps",
			"  path: ./entry\n  sourceRef: {kind: GitRepository, name: content}\n"))},
	}
	for _, branch := range []struct{ name, url string }{
		{name: "first", url: "https://fixture.invalid/external"},
		{name: "second", url: localURL},
	} {
		files["fanout/"+branch.name+".yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML(branch.name, "apps",
				"  path: ./"+branch.name+"\n")),
		}
		files[branch.name+"/source.yaml"] = &fstest.MapFile{Data: []byte(
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: content, namespace: apps}\nspec: {url: " + branch.url + "}\n",
		)}
		files[branch.name+"/gateway.yaml"] = &fstest.MapFile{
			Data: []byte(testutil.KustomizationYAML(branch.name+"-gateway", "apps",
				"  path: ./shared\n")),
		}
	}
	d, err := Begin(t.Context(), Config{
		Path: root, Input: &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: files},
		Store: store.New(), SelfURLs: []string{localURL},
	})
	assert.NoError(t, err)
	t.Cleanup(d.Close)
	assert.NoError(t, constructTestSources(t.Context(), d))
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	seeds, err := d.loader.Occurrences(t.Context(), filepath.Join(root, "entry", "bootstrap"), "")
	assert.NoError(t, err)
	assert.FatalIf(t, len(seeds) != 1, "seeds = %+v", seeds)
	d.planner = &entryPlanner{
		session: d, inputs: map[string]*store.SourceArtifact{root: d.cfg.Input},
		coordinates: map[selection.OccurrenceID]bool{}, seen: map[occurrenceKey]rootVisitState{},
		roots: []clusterRoot{{Path: "entry/bootstrap", seeds: seeds}},
	}
	_, err = d.clusterRoots(t.Context())
	assert.Equal(t, errors.Is(err, errPendingSources), true)
	assert.Diff(t, d.PendingSources(), []manifest.NamedResource{
		(&manifest.GitRepository{Name: "content", Namespace: "apps"}).Named(),
	})
	assert.NoError(t, constructTestSources(t.Context(), d))
	roots, err := d.clusterRoots(t.Context())
	assert.NoError(t, err)
	entries := clusterEntries(roots, d.cfg.Input, root)
	assert.FatalIf(t, len(entries) != 1, "entries = %+v", entries)
	assert.Equal(t, entries[0].ID, manifest.ClusterID("entry"))
	assert.Equal(t, entries[0].target, filepath.Join(root, "entry"))
	assert.Equal(t, slices.ContainsFunc(roots[0].Occurrences, func(occurrence selection.OccurrenceFacts) bool {
		return occurrence.Identity == (manifest.NamedResource{Kind: "ConfigMap", Name: "marker"})
	}), true)
}
