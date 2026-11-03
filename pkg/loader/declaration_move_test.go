package loader

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type declarationReadFS struct {
	fs.FS
	opens atomic.Int64
}

func (f *declarationReadFS) Open(name string) (fs.File, error) {
	f.opens.Add(1)
	return f.FS.Open(name)
}

func TestWithReaders_ReusesCompleteDeclarations(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			t.Parallel()
			var loaders [2]*Loader
			var stores [2]*store.Store
			var inputs [2]*declarationReadFS
			id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "chart"}
			for side := range loaders {
				value := 0
				if changed {
					value = side
				}
				input := &declarationReadFS{FS: fstest.MapFS{
					"entry/source.yaml": {Data: fmt.Appendf(nil, "apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: apps}\nspec: {url: oci://fixture.invalid/chart, undiscovered: %d}\n", value)},
				}}
				root := fmt.Sprintf("/virtual/declaration-reuse/%d", side)
				st := store.New()
				st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
				l := New(st)
				l.SourceRoot = root
				l.Options.Selection.Changed = true
				_, err := l.Load(t.Context(), filepath.Join(root, "entry"))
				assert.NoError(t, err)
				l.Admit(root)
				assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
				loaders[side], stores[side], inputs[side] = l, st, input
			}
			changes, err := change.Qualify(t.Context(), change.NewSet([]string{"entry/source.yaml"}), stores[0], stores[1])
			assert.NoError(t, err)
			for _, input := range inputs {
				input.opens.Store(0)
			}
			for range 2 {
				changes, err = changes.WithReaders(t.Context(), loaders[0].Index, loaders[1].Index)
				assert.NoError(t, err)
				for _, input := range inputs {
					assert.Equal(t, input.opens.Load(), int64(0))
				}
				different, known := changes.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, id, nil)
				assert.Equal(t, known, true)
				assert.Equal(t, different, changed)
			}
		})
	}
}

func declarationMovePair(t *testing.T, scenario string, reverse bool) ([2]*Loader, *change.Set) {
	t.Helper()
	var loaders [2]*Loader
	var inputs [2]*store.SourceArtifact
	for side := range loaders {
		root := t.TempDir()
		testutil.WriteFile(t, root, "flux/owners.yaml", testutil.KustomizationYAML("old-owner", "apps", "  path: ./old\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")+"---\n"+testutil.KustomizationYAML("new-owner", "apps", "  path: ./new\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")+"---\n"+testutil.KustomizationYAML("reader-owner", "apps", "  path: ./reader\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
		for i, owner := range []string{"old", "new"} {
			resources := "resources: []\n"
			if side == i && (scenario != "added" || side != 0) && (scenario != "removed" || side != 1) {
				resources = "resources: [source.yaml]\n"
				namespace, tag := "apps", "same"
				if side == 1 && scenario == "changed" {
					tag = "different"
				}
				if side == 1 && scenario == "namespace" {
					namespace = "other"
				}
				body := fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: {name: chart, namespace: %s}\nspec: {url: oci://fixture.invalid/chart, ref: {tag: %s}}\n", namespace, tag)
				if scenario == "mixed" {
					body += fmt.Sprintf("---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {values.yaml: 'value: side-%d'}\n", side)
				}
				testutil.WriteFile(t, root, owner+"/source.yaml", body)
				if scenario == "build_input" {
					resources += fmt.Sprintf("patches:\n- target: {kind: OCIRepository, name: chart}\n  patch: '[{\"op\":\"add\",\"path\":\"/spec/interval\",\"value\":\"%dh\"}]'\n", side+1)
				}
			}
			testutil.WriteFile(t, root, owner+"/kustomization.yaml", resources)
		}
		testutil.WriteFile(t, root, "reader/kustomization.yaml", "resources: [release.yaml]\n")
		body := "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: reader, namespace: apps}\nspec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}}\n"
		if scenario == "mixed" {
			body += "---\n" + strings.ReplaceAll(strings.ReplaceAll(body, "name: reader", "name: data-reader"), "interval: 1h,", "interval: 1h, valuesFrom: [{kind: ConfigMap, name: settings}],")
		}
		testutil.WriteFile(t, root, "reader/release.yaml", body)
		st := store.New()
		inputs[side] = &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
		st.SetArtifact(manifest.BootstrapSourceID, inputs[side])
		l := New(st)
		l.SourceRoot = root
		l.Options.Selection.Changed = true
		_, err := l.Load(t.Context(), filepath.Join(root, "flux"))
		assert.NoError(t, err)
		for _, directory := range []string{"old", "new", "reader"} {
			_, err := l.Load(t.Context(), filepath.Join(root, directory))
			assert.NoError(t, err)
		}
		l.Admit(root)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
		loaders[side] = l
	}
	if scenario == "source" {
		id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "chart"}
		record := loaders[1].Index.Records[id]
		for i := range record.Productions {
			record.Productions[i].Source.Name = "foreign"
		}
		loaders[1].Index.Records[id] = record
	}
	if reverse {
		loaders[0], loaders[1] = loaders[1], loaders[0]
		inputs[0], inputs[1] = inputs[1], inputs[0]
	}
	if scenario == "partial" {
		loaders[1].Index.CompleteSources = nil
	}
	changes, err := change.Detect(t.Context(), inputs[0], inputs[1])
	assert.NoError(t, err)
	changes, err = changes.WithReaders(t.Context(), loaders[0].Index, loaders[1].Index)
	assert.NoError(t, err)
	for side, l := range loaders {
		l.side, l.sideKnown = manifest.TreeSide(side), true
		l.Options.Changes, l.Options.Filter = changes, change.NewFilter()
		assert.NoError(t, l.FinalizeIndex(t.Context(), l.SourceRoot, true))
	}
	return loaders, changes
}

func TestAdmission_UnchangedDeclarationMove(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			t.Parallel()
			loaders, changes := declarationMovePair(t, "move", reverse)
			for _, l := range loaders {
				for _, record := range l.Index.Files() {
					assert.Equal(t, record.Declaration == nil, true)
				}
				for range 2 {
					proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
					assert.NoError(t, err)
					var renderers []manifest.NamedResource
					for _, id := range proposal.Needed {
						if id.KindInfo().Render {
							renderers = append(renderers, id)
						}
					}
					assert.Diff(t, renderers, []manifest.NamedResource{testutil.KustomizationID("apps", "new-owner"), testutil.KustomizationID("apps", "old-owner")})
					changed, known := changes.DeclarationChanged(l.side, manifest.BootstrapSourceID, manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "chart"}, nil)
					assert.Equal(t, known && !changed, true)
				}
			}
		})
	}
}

func TestAdmission_DeclarationMoveChanges(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"changed", "added", "removed", "namespace", "mixed", "build_input", "partial", "source"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			loaders, _ := declarationMovePair(t, scenario, true)
			for _, l := range loaders {
				proposal, err := l.Prepare(t.Context(), store.Child{}, nil, nil)
				assert.NoError(t, err)
				assert.Equal(t, slices.Contains(proposal.Needed, testutil.HelmReleaseID("apps", "reader")), scenario != "mixed" && scenario != "build_input")
				if scenario == "build_input" {
					id := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindOCIRepository, Namespace: "apps", Name: "chart"}
					occurrence, ok := l.occurrence(id)
					assert.Equal(t, ok, true)
					input := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
					child, err := l.exactObject(t.Context(), occurrence, input)
					assert.NoError(t, err)
					child.Doc["spec"].(map[string]any)["interval"] = "2h"
					proposal, err = l.Prepare(t.Context(), child, nil, nil)
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Dirty, testutil.HelmReleaseID("apps", "reader")), true)
				}
				if scenario == "mixed" {
					assert.Equal(t, slices.Contains(proposal.Needed, testutil.HelmReleaseID("apps", "data-reader")), true)
				}
			}
		})
	}
}
