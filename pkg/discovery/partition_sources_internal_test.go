package discovery

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestPlan_SourceOccurrenceIsolation(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed_%t", changed), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{}
			for _, side := range []string{"left", "right"} {
				ref := map[string]string{"right": ", ref: {branch: right}"}[side]
				files[side+"/kustomization.yaml"] = &fstest.MapFile{
					Data: []byte("resources: [source.yaml, owner.yaml]\n"),
				}
				files[side+"/source.yaml"] = &fstest.MapFile{
					Data: []byte(
						"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://example.invalid/" + side + ref + "}\n",
					),
				}
				files["workload/"+side+"/kustomization.yaml"] = &fstest.MapFile{Data: []byte("resources: [cm.yaml]\n")}
				files["workload/"+side+"/cm.yaml"] = &fstest.MapFile{
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + side + "-marker}\n"),
				}
				files[side+"/owner.yaml"] = &fstest.MapFile{
					Data: []byte(
						testutil.KustomizationYAML(
							"owner",
							"apps",
							"  path: ./workload/"+side+"\n  sourceRef: {kind: GitRepository, name: cluster}\n",
						),
					),
				}
			}
			view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: files})
			assert.NoError(t, err)
			owner, err := cache.New(t.Context(), t.TempDir())
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			input := &store.SourceArtifact{LocalPath: root, FS: view}
			cfg := Config{
				Cache:     owner,
				Input:     input,
				Path:      root,
				SelfURLs:  []string{"https://example.invalid/left"},
				Selection: selection.Request{Changed: changed},
			}
			entries, err := Plan(t.Context(), cfg)
			assert.NoError(t, err)
			assert.Equal(t, len(entries), 2)
			restored, err := Plan(t.Context(), cfg)
			assert.NoError(t, err)
			assert.Equal(t, len(restored), len(entries))

			for i, side := range []string{"left", "right"} {
				entry := &restored[i]
				assert.Equal(t, entry.ID, entries[i].ID)
				assert.Equal(t, entry.Path, entries[i].Path)
				assert.Equal(t, entry.Path, filepath.Join(root, side))
				cfg.Path, cfg.Entry, cfg.Store = entry.Path, entry, store.New()
				session, err := beginTest(t.Context(), cfg)
				assert.NoError(t, err)
				t.Cleanup(session.Close)
				occurrences, err := session.partitionRootsTest(t.Context())
				assert.NoError(t, err)
				assertSourceScopeMarkers(t, occurrences, side == "left")
				var source *manifest.GitRepository
				for child, sourceErr := range session.loader.Sources(t.Context()) {
					assert.NoError(t, sourceErr)
					source = child.Obj.(*manifest.GitRepository)
				}
				assert.FatalIf(t, source == nil, "%s source missing", side)
				assert.Equal(t, source.URL, "https://example.invalid/"+side)
				artifact := cfg.Store.GetArtifact(source.Named(), store.ArtifactContent)
				if side == "left" {
					assert.FatalIf(
						t,
						artifact == nil || len(entry.prepared.aliasViews) != 1,
						"local source lost its view: %+v",
						entry,
					)
				} else {
					assert.Equal(t, source.Reference.Branch, "right")
					assert.FatalIf(
						t,
						artifact != nil || len(entry.prepared.aliasViews) != 0,
						"external source borrowed its sibling's view: %+v",
						entry,
					)
				}
			}
		})
	}
}

func TestPartitionRoots_SourceScope(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name                    string
		changed                 bool
		localRoot, externalRoot string
	}{
		{name: "changed_false/local_first", localRoot: "left", externalRoot: "right"},
		{name: "changed_false/local_last", localRoot: "right", externalRoot: "left"},
		{name: "changed_true/local_first", changed: true, localRoot: "left", externalRoot: "right"},
		{name: "changed_true/local_last", changed: true, localRoot: "right", externalRoot: "left"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			files := fstest.MapFS{
				tt.localRoot + "/kustomization.yaml": {Data: []byte("resources: [owner.yaml, source.yaml]\n")},
				tt.localRoot + "/source.yaml": {
					Data: []byte(
						"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://example.invalid/left}\n",
					),
				},
				tt.localRoot + "/owner.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							"left",
							"apps",
							"  path: ./workload/left\n  sourceRef: {kind: GitRepository, name: cluster}\n",
						),
					),
				},
				tt.externalRoot + "/kustomization.yaml": {Data: []byte("resources: [owner.yaml, source.yaml]\n")},
				tt.externalRoot + "/owner.yaml": {
					Data: []byte(
						testutil.KustomizationYAML(
							"right",
							"apps",
							"  path: ./workload/right\n  sourceRef: {kind: GitRepository, name: cluster}\n",
						),
					),
				},
				tt.externalRoot + "/source.yaml": {
					Data: []byte(
						"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://external.invalid/right, ref: {branch: right}}\n",
					),
				},
				"workload/left/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"workload/left/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: left-marker}\n"),
				},
				"workload/right/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"workload/right/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: right-marker}\n"),
				},
			}
			input := &store.SourceArtifact{LocalPath: root, FS: files}
			d := newDiscoverer(&Config{
				Path: root, Input: input, Store: store.New(),
				SelfURLs: []string{"https://example.invalid/left"}, Selection: selection.Request{Changed: tt.changed},
			})
			_, err := d.resolveTestSources(t.Context(), false)
			assert.NoError(t, err)
			d.loader.SourceRoot = root
			_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
			assert.NoError(t, err)
			roots, err := d.partitionRootsTest(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, len(roots), 2)
			assert.Equal(t, roots[0].Path, "left")
			assert.Equal(t, roots[1].Path, "right")
			restored, err := d.partitionRootsTest(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, len(restored), len(roots))
			for i, candidate := range roots {
				assert.Equal(t, restored[i].Path, candidate.Path)
				assert.Equal(t, len(restored[i].Occurrences), len(candidate.Occurrences))
				for j, occurrence := range candidate.Occurrences {
					assert.Equal(t, restored[i].Occurrences[j].Identity, occurrence.Identity)
					assert.Equal(t, restored[i].Occurrences[j].Declaration, occurrence.Declaration)
				}
				assertSourceScopeMarkers(t, []entryRoot{candidate}, candidate.Path == tt.localRoot)
			}
		})
	}
}

func assertSourceScopeMarkers(t *testing.T, roots []entryRoot, local bool) {
	t.Helper()
	leftMarker := false
	for i := range roots {
		for j := range roots[i].Occurrences {
			occurrence := &roots[i].Occurrences[j]
			leftMarker = leftMarker || occurrence.Identity.Name == "left-marker"
			assert.Equal(t, occurrence.Identity.Name == "right-marker", false)
		}
	}
	assert.Equal(t, leftMarker, local)
}

func TestPartitionRoots_SourceConstruction(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	files := fstest.MapFS{
		"left/kustomization.yaml": {Data: []byte("resources: [deferred.yaml, raw.yaml, owner.yaml, source.yaml]\n")},
		"left/deferred.yaml": {Data: []byte(testutil.KustomizationYAML("deferred", "apps",
			"  path: ./workload/right\n  sourceRef: {kind: GitRepository, name: raw}\n"))},
		"left/raw.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: raw, namespace: apps}\nspec: {url: https://example.invalid/left}\n",
			),
		},
		"left/owner.yaml": {Data: []byte(testutil.KustomizationYAML("left", "apps",
			"  path: ./workload/left\n  sourceRef: {kind: GitRepository, name: cluster}\n"))},
		"left/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://example.invalid/left}\n",
			),
		},
		"right/kustomization.yaml": {Data: []byte("resources: [owner.yaml, source.yaml]\n")},
		"right/owner.yaml": {Data: []byte(testutil.KustomizationYAML("right", "apps",
			"  path: ./workload/right\n  sourceRef: {kind: GitRepository, name: cluster}\n"))},
		"right/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://external.invalid/right, ref: {branch: right}}\n",
			),
		},
		"workload/left/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"workload/left/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: left-marker}\n"),
		},
		"workload/right/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"workload/right/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: right-marker}\n"),
		},
	}
	st := store.New()
	d, err := Begin(t.Context(), Config{
		Path: root, Input: &store.SourceArtifact{LocalPath: root, FS: files}, Store: st,
		SelfURLs: []string{"https://example.invalid/left"},
	})
	assert.NoError(t, err)
	t.Cleanup(d.Close)
	assert.NoError(t, constructTestSources(t.Context(), d))
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	occurrences, err := d.loader.Occurrences(t.Context(), filepath.Join(root, "left"), "")
	assert.NoError(t, err)
	var seeds []selection.OccurrenceFacts
	for _, name := range []string{"deferred", "raw", "left", "cluster"} {
		for _, occurrence := range occurrences {
			if occurrence.Identity.Name == name {
				occurrence.RequiresBuild = name == "deferred" || name == "raw"
				seeds = append(seeds, occurrence)
			}
		}
	}
	assert.Equal(t, len(seeds), 4)
	external, err := d.loader.Occurrences(t.Context(), filepath.Join(root, "right"), "")
	assert.NoError(t, err)
	planner := &entryPlanner{
		session: d, inputs: map[string]*store.SourceArtifact{root: d.cfg.Input},
		coordinates: map[selection.OccurrenceID]bool{}, seen: map[occurrenceKey]bool{},
		roots: []entryRoot{{Path: "left", seeds: seeds}, {Path: "right", seeds: external}},
	}
	d.planner = planner
	_, err = d.partitionRoots(t.Context())
	assert.Equal(t, errors.Is(err, errPendingSources), true)
	assert.Equal(t, d.planner == planner, true)
	assert.Equal(t, planner.head, 2)
	assert.Equal(t, len(planner.roots[0].Occurrences), 2)
	assert.Equal(t, planner.roots[0].Occurrences[0].Declaration, seeds[0].Declaration)
	assert.Equal(t, planner.roots[0].Occurrences[1].Declaration, seeds[1].Declaration)
	raw := seeds[1].Identity
	visit := partitionVisit{occurrence: &seeds[2], scope: &planner.scope}
	material, err := planner.source(t.Context(), &planner.roots[0], &visit, &manifest.Reference{NamedResource: raw})
	assert.NoError(t, err)
	assert.Equal(t, material == nil, true)
	_, acquired := d.resolvedSources.snapshot()[raw]
	assert.Equal(t, acquired, false)
	assert.Equal(t, st.GetArtifact(raw, store.ArtifactContent), nil)
	assert.Diff(t, d.PendingSources(), []manifest.NamedResource{seeds[3].Identity})
	assert.Equal(t, st.GetArtifact(seeds[3].Identity, store.ArtifactContent), nil)
	assert.NoError(t, constructTestSources(t.Context(), d))
	assert.Equal(t, st.GetArtifact(seeds[3].Identity, store.ArtifactContent) != nil, true)
	roots, err := d.partitionRoots(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, len(roots), 2)
	for i, candidate := range roots {
		counts := map[selection.OccurrenceID]int{}
		for _, occurrence := range candidate.Occurrences {
			counts[occurrence.Declaration]++
		}
		for _, count := range counts {
			assert.Equal(t, count, 1)
		}
		for _, seed := range [][]selection.OccurrenceFacts{seeds, external}[i] {
			assert.Equal(t, counts[seed.Declaration], 1)
		}
		assertSourceScopeMarkers(t, []entryRoot{candidate}, candidate.Path == "left")
	}
	_, acquired = d.resolvedSources.snapshot()[raw]
	assert.Equal(t, acquired, false)
	assert.Equal(t, st.GetArtifact(raw, store.ArtifactContent), nil)
}
