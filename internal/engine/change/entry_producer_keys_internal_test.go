package change

import (
	"fmt"
	"maps"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type entryProducerKeysCase struct {
	name        string
	names       [2]string
	roots       [2]string
	declaration bool
}

func TestWithReaders_EntryProducerKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []entryProducerKeysCase{
		{name: "owner_rename", names: [2]string{"before", "after"}, roots: [2]string{"cluster", "cluster"}},
		{name: "changed_declaration", names: [2]string{"owner", "owner"}, roots: [2]string{"cluster", "cluster"}, declaration: true},
		{name: "isolated_equal_names", names: [2]string{"owner", "owner"}, roots: [2]string{"first", "second"}},
		{name: "root_move", names: [2]string{"owner", "owner"}, roots: [2]string{"old", "new"}, declaration: true},
		{name: "absent_base", names: [2]string{"", "added"}, roots: [2]string{"", "cluster"}, declaration: true},
		{name: "absent_head", names: [2]string{"removed", ""}, roots: [2]string{"cluster", ""}, declaration: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, exchanged := range []bool{false, true} {
				t.Run(strconv.FormatBool(exchanged), func(t *testing.T) {
					t.Parallel()
					testWithReadersEntryProducerKeys(t, &tc, exchanged)
				})
			}
		})
	}
}

func testWithReadersEntryProducerKeys(t *testing.T, tc *entryProducerKeysCase, exchanged bool) {
	t.Helper()
	names, roots := tc.names, tc.roots
	if exchanged {
		names[0], names[1] = names[1], names[0]
		roots[0], roots[1] = roots[1], roots[0]
	}
	inputs, indexes, want := entryReaderFixture(names, roots, tc.declaration)
	changes, err := Detect(t.Context(), inputs[0], inputs[1], DetectConfig{})
	assert.NoError(t, err)
	for side, index := range indexes {
		if index != nil {
			completeTestInventory(t, changes, inputs[side], index)
		}
	}
	var earlyIndexes, unmappedIndexes [2]*manifest.DiscoveryIndex
	wantUnmapped := map[manifest.NamedResource]bool{}
	for side, index := range indexes {
		if index == nil {
			continue
		}
		early := *index
		early.Records, early.Declarations, early.CompleteSources = nil, nil, nil
		earlyIndexes[side] = &early
		unmapped := early
		unmapped.ProducerKeys = nil
		unmappedIndexes[side] = &unmapped
		wantUnmapped[testutil.KustomizationID("apps", names[side])] = true
	}
	unmapped, err := changes.WithReaders(
		t.Context(),
		unmappedIndexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	early, err := unmapped.WithReaders(
		t.Context(),
		earlyIndexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	settled, err := early.WithReaders(
		t.Context(),
		indexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	unmappedKeys := map[manifest.NamedResource]bool{}
	for id := range unmapped.Inputs(manifest.NamedResource{}) {
		unmappedKeys[id] = true
	}
	assert.Diff(t, unmappedKeys, wantUnmapped)
	earlyKeys := map[manifest.NamedResource]bool{}
	for id := range early.Inputs(manifest.NamedResource{}) {
		earlyKeys[id] = true
	}
	assert.Diff(t, earlyKeys, want)
	repeated, err := settled.WithReaders(
		t.Context(),
		indexes,
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(t,
		maps.Collect(repeated.Inputs(manifest.NamedResource{})),
		maps.Collect(settled.Inputs(manifest.NamedResource{})),
	)
	got := map[manifest.NamedResource]bool{}
	for id := range settled.Inputs(manifest.NamedResource{}) {
		got[id] = true
	}
	assert.Diff(t, got, want)
	for side, index := range indexes {
		if index == nil {
			continue
		}
		id := testutil.KustomizationID("apps", names[side])
		assert.Equal(t, index.Readers[fmt.Sprintf("input-%d.yaml", side)][0].Reader, id)
		if tc.declaration {
			assert.Equal(t, index.Records[id].File, roots[side]+"/owner.yaml")
			wantComparison := Comparison{Known: true, Changed: true}
			if names[1-side] == "" {
				wantComparison = Comparison{}
			}
			assert.Diff(t,
				settled.DeclarationChanged(manifest.TreeSide(side), manifest.BootstrapSourceID, id, nil),
				wantComparison,
			)
		}
	}
}

func entryReaderFixture(names, roots [2]string, declaration bool) (
	[2]*store.SourceArtifact, [2]*manifest.DiscoveryIndex, map[manifest.NamedResource]bool,
) {
	inputs := [2]*store.SourceArtifact{}
	indexes := [2]*manifest.DiscoveryIndex{}
	want := map[manifest.NamedResource]bool{}
	for side, name := range names {
		files := fstest.MapFS{}
		inputs[side] = &store.SourceArtifact{FS: files}
		if name == "" {
			continue
		}
		id := testutil.KustomizationID("apps", name)
		key := manifest.NewEntryKustomization(manifest.BootstrapSourceID, roots[side], manifest.NamedResource{}).Named()
		inputFile := fmt.Sprintf("input-%d.yaml", side)
		files[inputFile] = &fstest.MapFile{Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload}\n")}
		indexes[side] = &manifest.DiscoveryIndex{
			ProducerKeys: map[manifest.NamedResource]manifest.NamedResource{id: key},
			Readers: map[string][]manifest.FileReader{
				inputFile: {{Reader: id, Source: manifest.BootstrapSourceID, Exact: true}},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
		}
		if declaration {
			file := roots[side] + "/owner.yaml"
			files[file] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(
				name, "apps", fmt.Sprintf("  path: ./%s\n  targetNamespace: side-%d\n", roots[side], side),
			))}
			indexes[side].Records = map[manifest.NamedResource]manifest.DiscoveryRecord{id: {File: file, Authored: id}}
		}
		want[key] = true
	}
	return inputs, indexes, want
}
