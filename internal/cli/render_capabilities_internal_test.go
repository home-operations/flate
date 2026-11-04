package cli

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRenderKinds_Capabilities(t *testing.T) {
	t.Parallel()
	var want []string
	for info := range manifest.Kinds() {
		if info.Render {
			want = append(want, info.Kind)
		}
	}
	assert.Diff(t, renderKinds, want)
}
