package loader

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
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
		t.Run(
			strconv.FormatBool(changed),
			func(t *testing.T) {
				t.Parallel()
				var loaders [2]*Loader
				var stores [2]*store.Store
				var inputs [2]*declarationReadFS
				id := manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindOCIRepository,
					Namespace: "apps",
					Name:      "chart",
				}
				for side := range loaders {
					value := 0
					if changed {
						value = side
					}
					input := &declarationReadFS{FS: fstest.MapFS{
						"entry/source.yaml": {
							Data: fmt.Appendf(
								nil,
								"apiVersion: source.toolkit.fluxcd.io/v1\nkind: OCIRepository\nmetadata: "+
									"{name: chart, namespace: apps}\nspec: {url: oci://fixture.invalid/chart"+
									", undiscovered: %d}\n",
								value,
							),
						},
					}}
					root := fmt.Sprintf("/virtual/declaration-reuse/%d", side)
					st := store.New()
					st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: input})
					l := New(st)
					l.SourceRoot = root
					configureSelection(t, l, selection.Request{Changed: true})
					_, err := l.Load(t.Context(), filepath.Join(root, "entry"), manifest.NamedResource{}, nil)
					assert.NoError(t, err)
					l.Admit(root)
					assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
					loaders[side], stores[side], inputs[side] = l, st, input
				}
				changes, err := change.Qualify(
					t.Context(),
					change.NewSet([]string{"entry/source.yaml"}), [2]*store.Store{stores[0], stores[1]},
					manifest.WorkScope{},
				)
				assert.NoError(t, err)
				for _, input := range inputs {
					input.opens.Store(0)
				}
				for range 2 {
					changes, err = changes.WithReaders(
						t.Context(),
						[2]*manifest.DiscoveryIndex{loaders[0].Index, loaders[1].Index},
						[2]map[manifest.NamedResource]inputprojection.Projection{
							loaders[0].InputContent,
							loaders[1].InputContent,
						},
						manifest.WorkScope{},
					)
					assert.NoError(t, err)
					for _, input := range inputs {
						assert.Equal(t, input.opens.Load(), int64(0))
					}
					declarationChangedResult := changes.DeclarationChanged(
						manifest.TreeHead,
						manifest.BootstrapSourceID,
						id,
						nil,
					)
					different, known := declarationChangedResult.Changed, declarationChangedResult.Known
					assert.Equal(t, known, true)
					assert.Equal(t, different, changed)
				}
			},
		)
	}
}

func declarationMovePair(t *testing.T, scenario string, reverse bool) ([2]*Loader, *change.Set) {
	t.Helper()
	var loaders [2]*Loader
	var inputs [2]*store.SourceArtifact
	for side := range loaders {
		root := declarationMoveFiles(t, scenario, side)
		st := store.New()
		inputs[side] = &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)}
		st.SetArtifact(manifest.BootstrapSourceID, inputs[side])
		l := New(st)
		l.SourceRoot = root
		configureSelection(t, l, selection.Request{Changed: true})
		_, err := l.Load(t.Context(), filepath.Join(root, "flux"), manifest.NamedResource{}, nil)
		assert.NoError(t, err)
		for _, directory := range []string{"old", "new", "reader"} {
			_, err := l.Load(t.Context(), filepath.Join(root, directory), manifest.NamedResource{}, nil)
			assert.NoError(t, err)
		}
		l.Admit(root)
		assert.NoError(t, l.FinalizeIndex(t.Context(), root, false))
		loaders[side] = l
	}
	if scenario == "source" {
		id := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "apps",
			Name:      "chart",
		}
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
	changes, err := change.Detect(t.Context(), inputs[0], inputs[1], change.DetectConfig{})
	assert.NoError(t, err)
	changes, err = changes.WithReaders(t.Context(),
		[2]*manifest.DiscoveryIndex{loaders[0].Index, loaders[1].Index},
		[2]map[manifest.NamedResource]inputprojection.Projection{loaders[0].InputContent, loaders[1].InputContent},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for side, l := range loaders {
		l.side, l.sideKnown = manifest.TreeSide(side), true
		l.Options.Changes = changes
		assert.NoError(t, l.FinalizeIndex(t.Context(), l.SourceRoot, true))
	}
	return loaders, changes
}

func TestAdmission_UnchangedDeclarationMove(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		t.Run(strconv.FormatBool(reverse), func(t *testing.T) {
			t.Parallel()
			testUnchangedDeclarationMove(t, reverse)
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
				proposal, err := prepareSelection(
					t.Context(),
					t,
					l,
					selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
				)
				assert.NoError(t, err)
				assert.Equal(
					t,
					slices.Contains(proposal.Needed, testutil.HelmReleaseID("apps", "reader")),
					scenario != "mixed" && scenario != "build_input",
				)
				if scenario == "build_input" {
					id := manifest.NamedResource{
						Group:     manifest.SourceDomain,
						Kind:      manifest.KindOCIRepository,
						Namespace: "apps",
						Name:      "chart",
					}
					occurrence, ok := l.occurrence(id)
					assert.Equal(t, ok, true)
					input := l.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
					child, err := l.OccurrenceObject(t.Context(), new(occurrenceView(&occurrence)), input)
					assert.NoError(t, err)
					child.Doc["spec"].(map[string]any)["interval"] = "2h"
					proposal, err = prepareSelection(
						t.Context(),
						t,
						l,
						selection.PrepareRequest{Changes: l.Options.Changes, Generation: []store.Child{child}},
					)
					assert.NoError(t, err)
					assert.Equal(t, slices.Contains(proposal.Dirty, testutil.HelmReleaseID("apps", "reader")), true)
				}
				if scenario == "mixed" {
					assert.Equal(
						t,
						slices.Contains(proposal.Needed, testutil.HelmReleaseID("apps", "data-reader")),
						true,
					)
				}
			}
		})
	}
}

func declarationMoveFiles(t *testing.T, scenario string, side int) string {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"flux/owners.yaml",
		testutil.KustomizationYAML(
			"old-owner",
			"apps",
			"  path: ./old\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		)+"---\n"+testutil.KustomizationYAML(
			"new-owner",
			"apps",
			"  path: ./new\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		)+"---\n"+testutil.KustomizationYAML(
			"reader-owner",
			"apps",
			"  path: ./reader\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	writeDeclarationMoveSources(t, root, scenario, side)
	testutil.WriteFile(t, root, "reader/kustomization.yaml", "resources: [release.yaml]\n")
	body := "apiVersion: helm.toolkit.fluxcd.io/v2\n" + "kind: HelmRelease\n" + "metadata: {name: reader, namespace: apps}\n" +
		"spec: {interval: 1h, chartRef: {kind: OCIRepository, name: chart}}\n"
	if scenario == "mixed" {
		body += "---\n" + strings.ReplaceAll(
			strings.ReplaceAll(body, "name: reader", "name: data-reader"),
			"interval: 1h,",
			"interval: 1h, valuesFrom: [{kind: ConfigMap, name: settings}],",
		)
	}
	testutil.WriteFile(t, root, "reader/release.yaml", body)
	return root
}

func writeDeclarationMoveSources(t *testing.T, root, scenario string, side int) {
	t.Helper()
	for i, owner := range []string{"old", "new"} {
		resources := "resources: []\n"
		if side == i && (scenario != "added" || side != 0) && (scenario != "removed" || side != 1) {
			resources = "resources: [source.yaml]\n"
			body := declarationMoveSource(scenario, side)
			testutil.WriteFile(t, root, owner+"/source.yaml", body)
			if scenario == "build_input" {
				resources += fmt.Sprintf("patches:\n"+"- target: {kind: OCIRepository, name: chart}\n"+
					"  patch: '[{\"op\":\"add\",\"path\":\"/spec/interval\",\"value\":\"%dh\"}]'\n", side+1)
			}
		}
		testutil.WriteFile(t, root, owner+"/kustomization.yaml", resources)
	}
}

func testUnchangedDeclarationMove(t *testing.T, reverse bool) {
	t.Helper()

	loaders, changes := declarationMovePair(t, "move", reverse)
	for _, l := range loaders {
		for id := range l.Index.Files() {
			record, _ := l.Index.Lookup(id)
			assert.Equal(t, record.Declaration == nil, true)
		}
		for range 2 {
			proposal, err := prepareSelection(
				t.Context(),
				t,
				l,
				selection.PrepareRequest{Changes: l.Options.Changes, Invocation: true},
			)
			assert.NoError(t, err)
			var renderers []manifest.NamedResource
			for _, id := range proposal.Needed {
				if id.KindInfo().Render {
					renderers = append(renderers, id)
				}
			}
			assert.Diff(
				t,
				renderers,
				[]manifest.NamedResource{
					testutil.KustomizationID("apps", "new-owner"),
					testutil.KustomizationID("apps", "old-owner"),
				},
			)
			declarationChangedResult2 := changes.DeclarationChanged(
				l.side,
				manifest.BootstrapSourceID,
				manifest.NamedResource{
					Group:     manifest.SourceDomain,
					Kind:      manifest.KindOCIRepository,
					Namespace: "apps",
					Name:      "chart",
				},
				nil,
			)
			changed, known := declarationChangedResult2.Changed, declarationChangedResult2.Known
			assert.Equal(t, known && !changed, true)
		}
	}
}

func declarationMoveSource(scenario string, side int) string {
	namespace, tag := "apps", "same"
	if side == 1 && scenario == "changed" {
		tag = "different"
	}
	if side == 1 && scenario == "namespace" {
		namespace = "other"
	}
	body := fmt.Sprintf("apiVersion: source.toolkit.fluxcd.io/v1\n"+"kind: OCIRepository\n"+
		"metadata: {name: chart, namespace: %s}\n"+
		"spec: {url: oci://fixture.invalid/chart, ref: {tag: %s}}\n", namespace, tag)
	if scenario == "mixed" {
		body += fmt.Sprintf(
			"---\n"+"apiVersion: v1\n"+"kind: ConfigMap\n"+"metadata: {name: settings, namespace: apps}\n"+
				"data: {values.yaml: 'value: side-%d'}\n",
			side,
		)
	}

	return body
}
