package discovery

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/fstest"

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
			input := &store.SourceArtifact{LocalPath: root, FS: files}
			cfg := Config{
				Input:     input,
				Path:      root,
				SelfURLs:  []string{"https://example.invalid/left"},
				Selection: selection.Request{Changed: changed},
			}
			entries, err := Plan(t.Context(), cfg)
			assert.NoError(t, err)
			assert.Equal(t, len(entries), 2)
			for i, side := range []string{"left", "right"} {
				entry := &entries[i]
				assert.Equal(t, entry.Path, filepath.Join(root, side))
				cfg.Path, cfg.Entry, cfg.Store = entry.Path, entry, store.New()
				session, err := Begin(t.Context(), cfg)
				assert.NoError(t, err)
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
	root := t.TempDir()
	files := fstest.MapFS{
		"left/kustomization.yaml": {Data: []byte("resources: [owner.yaml]\n")},
		"left/owner.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"left",
					"apps",
					"  path: ./workload/left\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				),
			),
		},
		"right/kustomization.yaml": {Data: []byte("resources: [owner.yaml, source.yaml]\n")},
		"right/owner.yaml": {
			Data: []byte(
				testutil.KustomizationYAML(
					"right",
					"apps",
					"  path: ./workload/right\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				),
			),
		},
		"right/source.yaml": {
			Data: []byte(
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: cluster, namespace: apps}\nspec: {url: https://external.invalid/right, ref: {branch: right}}\n",
			),
		},
		"workload/left/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"workload/left/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: left-marker}\n"),
		},
	}
	input := &store.SourceArtifact{LocalPath: root, FS: files}
	d := newDiscoverer(&Config{Path: root, Input: input, Store: store.New()})
	_, err := d.resolveEntrySources(t.Context(), false)
	assert.NoError(t, err)
	d.loader.SourceRoot = root
	_, err = d.loader.Load(t.Context(), root, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	roots, err := d.partitionRoots(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, len(roots), 2)
	for _, candidate := range roots {
		marker := false
		for _, occurrence := range candidate.Occurrences {
			marker = marker || occurrence.Identity.Name == "left-marker"
		}
		assert.Equal(t, marker, candidate.Path == "left")
	}
}
