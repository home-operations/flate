package loader

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestProjectTargets_KeyCoverage(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		targets := []manifest.ProducerTarget{
			{
				Kind:      manifest.KindSecret,
				Namespace: "apps",
				Name:      "input",
				Keys:      manifest.KeySet{Names: []string{"${KEY}", "a", "z"}, Complete: complete},
			},
		}
		projected := projectTargets(
			targets,
			func(value string) string { return strings.ReplaceAll(value, "${KEY}", "a") },
		)
		assert.Diff(t, projected[0].Keys, manifest.KeySet{Names: []string{"a", "z"}, Complete: complete})
		assert.Diff(t, targets[0].Keys.Names, []string{"${KEY}", "a", "z"})
	}
}
