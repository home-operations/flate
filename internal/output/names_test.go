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
}
