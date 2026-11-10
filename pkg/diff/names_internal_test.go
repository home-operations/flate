package diff

import (
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepareNames_CoreCollision(t *testing.T) {
	t.Parallel()
	core := manifest.NamedResource{Kind: "Widget", Name: "same"}
	custom := manifest.NamedResource{Group: "custom.example", Kind: "Widget", Name: "same"}
	short := manifest.NamedResource{Group: "other.example", Kind: "Other", Name: "other"}
	names := PrepareNames([]string{"alpha", "omega"}, [][]manifest.NamedResource{{core, short, {}}, {custom}}, nil)
	{
		_, ok := names.Groups[0].Subjects[manifest.NamedResource{}]
		assert.FatalIf(t, ok, "%v", "empty subjects must have no resource label")
	}
	assert.Equal(t, names.Groups[0].Subjects[core].Kind, "Widget")
	assert.Equal(t, names.Groups[1].Subjects[custom].Kind, "Widget.custom.example")
	assert.Equal(t, names.Groups[0].Subjects[short].Kind, "Other")
	assert.Equal(t, names.Groups[0].Subjects[core].Name, "alpha: same")
	single := PrepareNames([]string{"alpha"}, [][]manifest.NamedResource{{custom}}, nil)
	assert.Equal(t, single.Groups[0].Subjects[custom].Kind, "Widget")
	assert.Equal(t, single.Groups[0].Subjects[custom].Name, "same")

	entry := manifest.NamedResource{Kind: "Kustomization", Name: "entry"}
	path := manifest.NamedResource{Kind: "Kustomization", Name: "path"}
	otherEntry := manifest.NamedResource{Group: "custom.example", Kind: "entry", Name: "custom"}
	coreEntry := manifest.NamedResource{Kind: "entry", Name: "core"}
	for _, tc := range []struct {
		name      string
		inventory []manifest.NamedResource
		subjects  map[manifest.NamedResource]manifest.DiagnosticIdentity
		want      map[manifest.NamedResource]Subject
	}{
		{
			name:      "non-inventory captures do not collide",
			inventory: []manifest.NamedResource{core},
			subjects:  map[manifest.NamedResource]manifest.DiagnosticIdentity{custom: {ID: custom, Resource: custom}},
			want:      map[manifest.NamedResource]Subject{core: {ID: core, Kind: "Widget", Name: "same"}},
		},
		{
			name:      "zero raw capture contributes without a label",
			inventory: []manifest.NamedResource{core, {}},
			subjects:  map[manifest.NamedResource]manifest.DiagnosticIdentity{{}: {Resource: custom}, core: {ID: core, Resource: custom}},
			want:      map[manifest.NamedResource]Subject{core: {ID: core, Kind: "Widget", Name: "same"}},
		},
		{
			name:      "zero raw capture causes a collision",
			inventory: []manifest.NamedResource{custom, {}},
			subjects:  map[manifest.NamedResource]manifest.DiagnosticIdentity{{}: {Resource: core}},
			want:      map[manifest.NamedResource]Subject{custom: {ID: custom, Kind: "Widget.custom.example", Name: "same"}},
		},
		{
			name:      "entry and resource paths",
			inventory: []manifest.NamedResource{entry, path},
			subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{
				entry: {ID: entry, Path: "clusters/alpha"},
				path:  {ID: path, Resource: custom, Path: "apps/widget"},
			},
			want: map[manifest.NamedResource]Subject{
				entry: {ID: entry, Kind: "entry", Name: `path "clusters/alpha"`},
				path:  {ID: path, Kind: "Widget", Name: `same (entry path "apps/widget")`},
			},
		},
		{
			name:      "entry collision lookup follows fallback",
			inventory: []manifest.NamedResource{entry, coreEntry, otherEntry},
			subjects:  map[manifest.NamedResource]manifest.DiagnosticIdentity{entry: {ID: entry, Path: "clusters/alpha"}},
			want: map[manifest.NamedResource]Subject{
				entry:      {ID: entry, Name: `path "clusters/alpha"`},
				coreEntry:  {ID: coreEntry, Kind: "entry", Name: "core"},
				otherEntry: {ID: otherEntry, Kind: "entry.custom.example", Name: "custom"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := PrepareNames(
				[]string{"alpha"},
				[][]manifest.NamedResource{tc.inventory},
				[]map[manifest.NamedResource]manifest.DiagnosticIdentity{tc.subjects},
			)
			assert.Diff(t, got.Groups, []Names{{Subjects: tc.want}})
		})
	}
}

func TestNaming_RestoresContexts(t *testing.T) {
	t.Parallel()
	raw := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "source",
	}
	a := manifest.NamedResource{Group: "a.example", Kind: "Widget", Namespace: "apps", Name: "a"}
	b := manifest.NamedResource{Group: "b.example", Kind: "Widget", Namespace: "apps", Name: "b"}
	captured := map[manifest.NamedResource]manifest.DiagnosticIdentity{
		raw: {ID: raw, Resource: a},
		b:   {ID: b, Resource: a},
	}
	naming := PrepareNames(
		[]string{"alpha", "beta"},
		[][]manifest.NamedResource{{raw, raw, b}, {b}},
		[]map[manifest.NamedResource]manifest.DiagnosticIdentity{captured},
	)
	initial := []Names{
		{Prefix: "alpha: ", Subjects: map[manifest.NamedResource]Subject{
			raw: {ID: raw, Kind: "Widget.a.example", Name: "alpha: apps/a"},
			b:   {ID: b, Kind: "Widget.a.example", Name: "alpha: apps/a"},
		}},
		{Prefix: "beta: ", Subjects: map[manifest.NamedResource]Subject{
			b: {ID: b, Kind: "Widget.b.example", Name: "beta: apps/b"},
		}},
	}
	before := maps.Clone(captured)
	restored := func() {
		t.Helper()
		assert.Diff(t, naming.Groups, initial)
		assert.Diff(t, naming.Model(0, nil), initial[0])
		assert.Diff(t, naming.Model(1, map[manifest.NamedResource]manifest.DiagnosticIdentity{}), initial[1])
		assert.Diff(t, captured, before)
	}
	restored()
	empty := naming.Model(0, map[manifest.NamedResource]manifest.DiagnosticIdentity{})
	assert.Diff(t, empty, Names{Prefix: "alpha: ", Subjects: map[manifest.NamedResource]Subject{
		raw: {ID: raw, Kind: "OCIRepository", Name: "alpha: apps/source"},
		b:   {ID: b, Kind: "Widget", Name: "alpha: apps/b"},
	}})
	restored()
	assert.Diff(t, naming.Warning(0, manifest.Warning{
		Resource: raw, Subject: manifest.DiagnosticIdentity{ID: raw, Resource: b},
	}), Subject{ID: raw, Kind: "Widget", Name: "alpha: apps/b"})
	restored()
	model := naming.Model(0, map[manifest.NamedResource]manifest.DiagnosticIdentity{raw: {ID: raw, Resource: b}})
	assert.Diff(t, model, Names{Prefix: "alpha: ", Subjects: map[manifest.NamedResource]Subject{
		raw: {ID: raw, Kind: "Widget", Name: "alpha: apps/b"},
		b:   {ID: b, Kind: "Widget", Name: "alpha: apps/b"},
	}})
	restored()
	assert.Diff(t, naming.Warning(0, manifest.Warning{Resource: raw}), initial[0].Subjects[raw])
	restored()
	assert.Diff(t, naming.Warning(1, manifest.Warning{
		Resource: b, Subject: manifest.DiagnosticIdentity{ID: b, Resource: a},
	}), Subject{ID: b, Kind: "Widget", Name: "beta: apps/a"})
	restored()
	assert.Diff(t, naming.Warning(0, manifest.Warning{
		Resource: raw, Subject: manifest.DiagnosticFor(raw, nil),
	}), Subject{ID: raw, Kind: "OCIRepository", Name: "alpha: apps/source"})
	restored()
	assert.Diff(t, naming.Model(0, captured), initial[0])
	restored()
	assert.Diff(t, naming.Warning(0, manifest.Warning{
		Resource: raw, Subject: manifest.DiagnosticIdentity{ID: raw, Resource: a, Path: "warning/a"},
	}), Subject{ID: raw, Kind: "Widget.a.example", Name: `alpha: apps/a (entry path "warning/a")`})
	restored()
}
