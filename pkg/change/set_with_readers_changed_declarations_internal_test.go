package change

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestSet_WithReadersChangedDeclarations(t *testing.T) {
	t.Parallel()
	inputs := [2]*store.SourceArtifact{}
	indexes := [2]*manifest.DiscoveryIndex{}
	readers := [2]*digestReadFS{}
	changed := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "changed"}
	stable := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "stable"}
	for side := range inputs {
		readers[side] = &digestReadFS{FS: fstest.MapFS{
			"changed.yaml": {
				Data: fmt.Appendf(
					nil,
					"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: changed}\ndata: {value: side-%d}\n",
					side,
				),
			},
			"stable.yaml": {
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: stable}\ndata: {value: same}\n"),
			},
		}}
		inputs[side] = &store.SourceArtifact{FS: readers[side]}
		indexes[side] = &manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				changed: {File: "changed.yaml"}, stable: {File: "stable.yaml"},
			},
			CompleteSources: map[manifest.NamedResource]bool{manifest.BootstrapSourceID: true},
		}
	}
	changes, err := Detect(t.Context(), inputs[0], inputs[1], DetectConfig{})
	assert.NoError(t, err)
	for side, reader := range readers {
		reader.opens.Store(0)
		declared, localErr := collectInput(t.Context(), reader, "changed.yaml")
		assert.NoError(t, localErr)
		record := indexes[side].Records[changed]
		record.Declared = declared[changed]
		indexes[side].Records[changed] = record
	}
	qualified, err := changes.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			indexes[0],
			indexes[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for _, reader := range readers {
		assert.Equal(t, reader.opens.Load(), int64(1))
	}
	comparison := qualified.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, changed, nil)
	different, known := comparison.Changed, comparison.Known
	assert.Equal(t, different && known, true)
	known = qualified.DeclarationChanged(manifest.TreeHead, manifest.BootstrapSourceID, stable, nil).Known
	assert.Equal(t, known, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = changes.WithReaders(
		ctx, [2]*manifest.DiscoveryIndex{
			indexes[0],
			indexes[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.FatalIf(t, !errors.Is(err, context.Canceled), "WithReaders error = %v, want canceled", err)
}

func TestSet_InputVisibilityAvailability(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	foreign := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "other"}
	empty := manifest.InputVisibility{Source: manifest.BootstrapSourceID, Root: ".", Complete: true}
	visible := empty
	visible.Inputs = map[manifest.NamedResource][]string{id: {"z.yaml", "a.yaml"}}
	incomplete := visible
	incomplete.Complete = false
	moved := visible
	moved.Root = "other"
	otherEmpty := empty
	otherEmpty.Root = "other"
	otherSource := visible
	otherSource.Source = foreign
	for _, tc := range []struct {
		name          string
		before, after []manifest.InputVisibility
		want          bool
	}{
		{"hidden", []manifest.InputVisibility{visible}, []manifest.InputVisibility{empty}, true},
		{"revealed", []manifest.InputVisibility{empty}, []manifest.InputVisibility{visible}, true},
		{"unchanged", []manifest.InputVisibility{visible}, []manifest.InputVisibility{visible}, false},
		{"missing", []manifest.InputVisibility{visible}, nil, false},
		{"incomplete_absence", []manifest.InputVisibility{visible}, []manifest.InputVisibility{incomplete}, false},
		{"incomplete_contribution", []manifest.InputVisibility{incomplete}, []manifest.InputVisibility{empty}, false},
		{"completed_empty", []manifest.InputVisibility{empty}, []manifest.InputVisibility{empty}, false},
		{"domain_move", []manifest.InputVisibility{visible, otherEmpty}, []manifest.InputVisibility{empty, moved}, false},
		{"unmatched_domain", []manifest.InputVisibility{visible, otherEmpty}, []manifest.InputVisibility{empty}, false},
		{"available_with_incomplete_domain", []manifest.InputVisibility{visible, {Source: manifest.BootstrapSourceID, Root: "other"}}, []manifest.InputVisibility{empty, otherEmpty}, true},
		{"foreign_duplicate", []manifest.InputVisibility{visible, otherSource}, []manifest.InputVisibility{empty, otherSource}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			set := NewSet([]string{".krmignore"})
			indexes := [2]*manifest.DiscoveryIndex{{Visibility: tc.before}, {Visibility: tc.after}}
			resolved, err := set.WithReaders(
				t.Context(), [2]*manifest.DiscoveryIndex{
					indexes[0],
					indexes[1],
				},
				[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
			)
			assert.NoError(t, err)
			want := map[manifest.NamedResource]InputChange{}
			if tc.want {
				want[id] = InputChange{File: "a.yaml", Unbound: true}
			}
			assert.Diff(t, maps.Collect(resolved.Inputs(manifest.NamedResource{})), want)
			assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
			assert.Diff(t, resolved.Paths(), []string{".krmignore"})
		})
	}
}

func TestSet_WithReadersRequalification(t *testing.T) {
	t.Parallel()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	oldReader := testutil.KustomizationID("apps", "old")
	newReader := testutil.KustomizationID("apps", "new")
	set := NewSet([]string{"removed.yaml"})
	assert.NoError(
		t,
		set.CollectInputs(
			t.Context(),
			fstest.MapFS{
				"removed.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}\n")},
			},
			"removed.yaml",
			true,
		),
	)
	visible := manifest.InputVisibility{
		Source:   manifest.BootstrapSourceID,
		Root:     ".",
		Complete: true,
		Inputs:   map[manifest.NamedResource][]string{input: {"removed.yaml"}},
	}
	empty := manifest.InputVisibility{Source: manifest.BootstrapSourceID, Root: ".", Complete: true}
	before := &manifest.DiscoveryIndex{
		Visibility: []manifest.InputVisibility{visible},
		Readers: map[string][]manifest.FileReader{
			"removed.yaml": {{Reader: oldReader, Source: manifest.BootstrapSourceID}},
		},
	}
	after := &manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{empty}}
	first, err := set.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			before,
			after,
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		maps.Collect(first.Inputs(manifest.NamedResource{})),
		map[manifest.NamedResource]InputChange{input: {File: "removed.yaml"}, oldReader: {File: "removed.yaml"}},
	)
	next := &manifest.DiscoveryIndex{
		Visibility: []manifest.InputVisibility{visible},
		Readers: map[string][]manifest.FileReader{
			"removed.yaml": {{Reader: newReader, Source: manifest.BootstrapSourceID}},
		},
	}
	second, err := first.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			next,
			next,
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		maps.Collect(second.Inputs(manifest.NamedResource{})),
		map[manifest.NamedResource]InputChange{newReader: {File: "removed.yaml"}},
	)
	fresh, err := set.WithReaders(
		t.Context(),
		[2]*manifest.DiscoveryIndex{next, next},
		[2]map[manifest.NamedResource]inputprojection.Projection{},
		manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		maps.Collect(second.Inputs(manifest.NamedResource{})),
		maps.Collect(fresh.Inputs(manifest.NamedResource{})),
	)
	repeated, err := second.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			next,
			next,
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		maps.Collect(repeated.Inputs(manifest.NamedResource{})),
		maps.Collect(second.Inputs(manifest.NamedResource{})),
	)
	assert.Diff(
		t,
		maps.Collect(first.Inputs(manifest.NamedResource{})),
		map[manifest.NamedResource]InputChange{input: {File: "removed.yaml"}, oldReader: {File: "removed.yaml"}},
	)
	assert.Equal(t, before.Readers["removed.yaml"][0].Reader, oldReader)
	assert.Diff(t, before.Visibility[0].Inputs[input], []string{"removed.yaml"})
	frozen, err := first.WithDeclarations(
		t.Context(),
		DeclarationInputs{Stores: [2]*store.Store{nil, nil}, Prepared: [2]map[manifest.NamedResource]store.Child{}},
	)
	assert.NoError(t, err)
	qualified, err := frozen.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			next,
			next,
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		maps.Collect(qualified.Inputs(manifest.NamedResource{})),
		maps.Collect(second.Inputs(manifest.NamedResource{})),
	)
}

func TestSet_InputVisibilityDuplicateSourcePolicy(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	foreign := manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "foreign"}
	for _, reverse := range []bool{false, true} {
		for _, last := range []bool{false, true} {
			t.Run(fmt.Sprintf("reverse=%t/last=%t", reverse, last), func(t *testing.T) {
				t.Parallel()
				testSetInputVisibilityDuplicateSourcePolicyCase(
					t,
					testSetInputVisibilityDuplicateSourcePolicyCaseInputs{
						id:      &id,
						foreign: &foreign,
						reverse: &reverse,
						last:    &last,
					},
				)
			},
			)
		}
	}
}

func TestQualify_ReplacesBootstrapInputFacts(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	var stores [2]*store.Store
	for side := range stores {
		stores[side] = store.New()
		files := fstest.MapFS{".sourceignore": {}}
		if side == 0 {
			files["input.yaml"] = &fstest.MapFile{
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\n"),
			}
		}
		stores[side].SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: files})
	}
	raw, err := Qualify(t.Context(), nil, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	before := &manifest.DiscoveryIndex{
		Visibility: []manifest.InputVisibility{
			{
				Source:   manifest.BootstrapSourceID,
				Root:     ".",
				Complete: true,
				Inputs:   map[manifest.NamedResource][]string{id: {"input.yaml"}},
			},
		},
	}
	after := &manifest.DiscoveryIndex{
		Visibility: []manifest.InputVisibility{{Source: manifest.BootstrapSourceID, Root: ".", Complete: true}},
	}
	first, err := raw.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			before,
			after,
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(
		t,
		maps.Collect(first.Inputs(manifest.NamedResource{})),
		map[manifest.NamedResource]InputChange{id: {File: "input.yaml", Unbound: true}},
	)
	reused, err := Qualify(t.Context(), first, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	current, err := reused.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			after,
			after,
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	assert.Diff(t, maps.Collect(current.Inputs(manifest.NamedResource{})), map[manifest.NamedResource]InputChange{})
	for _, st := range stores {
		st.SetArtifact(
			manifest.BootstrapSourceID,
			&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{"unchanged.txt": {Data: []byte("same")}}},
		)
	}
	replaced, err := Qualify(t.Context(), first, [2]*store.Store{stores[0], stores[1]}, manifest.WorkScope{})
	assert.NoError(t, err)
	assert.Equal(t, replaced.Len(), 0)
	assert.Equal(t, len(replaced.oneSided[0])+len(replaced.oneSided[1]), 0)
	assert.Equal(t, len(maps.Collect(replaced.Inputs(manifest.NamedResource{}))), 0)
	assert.Diff(
		t,
		maps.Collect(first.Inputs(manifest.NamedResource{})),
		map[manifest.NamedResource]InputChange{id: {File: "input.yaml", Unbound: true}},
	)
}

type testSetInputVisibilityDuplicateSourcePolicyCaseInputs struct {
	id      *manifest.NamedResource
	foreign *manifest.NamedResource
	reverse *bool
	last    *bool
}

func testSetInputVisibilityDuplicateSourcePolicyCase(
	t *testing.T,
	testCaseInput testSetInputVisibilityDuplicateSourcePolicyCaseInputs,
) {
	t.Helper()

	body := []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\n")
	before := fstest.MapFS{"a/input.yaml": {Data: body}, "b/input.yaml": {Data: body}, ".sourceignore": {}}
	after := maps.Clone(before)
	ignore := "a/input.yaml\n"
	if *testCaseInput.last {
		ignore += "b/input.yaml\n"
	}
	after[".sourceignore"] = &fstest.MapFile{Data: []byte(ignore)}
	left := []manifest.InputVisibility{
		{
			Source:   manifest.BootstrapSourceID,
			Root:     "a",
			Complete: true,
			Inputs:   map[manifest.NamedResource][]string{(*testCaseInput.id): {"a/input.yaml"}},
		},
		{
			Source:   manifest.BootstrapSourceID,
			Root:     "b",
			Complete: true,
			Inputs:   map[manifest.NamedResource][]string{(*testCaseInput.id): {"b/input.yaml"}},
		},
		{
			Source:   (*testCaseInput.foreign),
			Root:     ".",
			Complete: true,
			Inputs:   map[manifest.NamedResource][]string{(*testCaseInput.id): {"foreign.yaml"}},
		},
	}
	right := slices.Clone(left)
	right[0].Inputs = nil
	if *testCaseInput.last {
		right[1].Inputs = nil
	}
	if *testCaseInput.reverse {
		before, after, left, right = after, before, right, left
	}
	set, err := Detect(
		t.Context(),
		&store.SourceArtifact{FS: before},
		&store.SourceArtifact{FS: after},
		DetectConfig{},
	)
	assert.NoError(t, err)
	assert.Equal(t, set.Contains("a/input.yaml"), true)
	assert.Equal(t, len(maps.Collect(set.Inputs(manifest.NamedResource{}))), 0)
	resolved, err := set.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			{Visibility: left},
			{Visibility: right},
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	want := map[manifest.NamedResource]InputChange{}
	if *testCaseInput.last {
		want[(*testCaseInput.id)] = InputChange{File: "a/input.yaml", Unbound: true}
	}
	assert.Diff(t, maps.Collect(resolved.Inputs(manifest.NamedResource{})), want)
}
