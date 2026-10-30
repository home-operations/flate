package output

import (
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
	if _, ok := names[0].Subjects[manifest.NamedResource{}]; ok {
		t.Fatal("empty subjects must have no resource label")
	}
	assert.Equal(t, names[0].Subjects[core].Kind, "Widget")
	assert.Equal(t, names[1].Subjects[custom].Kind, "Widget.custom.example")
	assert.Equal(t, names[0].Subjects[short].Kind, "Other")
	assert.Equal(t, names[0].Subjects[core].Name, "alpha: same")
	single := PrepareNames([]string{"alpha"}, [][]manifest.NamedResource{{custom}}, nil)
	assert.Equal(t, single[0].Subjects[custom].Kind, "Widget")
	assert.Equal(t, single[0].Subjects[custom].Name, "same")

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
			got := PrepareNames([]string{"alpha"}, [][]manifest.NamedResource{tc.inventory}, []map[manifest.NamedResource]manifest.DiagnosticIdentity{tc.subjects})
			assert.Diff(t, got, []Names{{Subjects: tc.want}})
		})
	}
}
