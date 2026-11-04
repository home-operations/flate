package git

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
)

func TestGitWarnings_ImmutableCoordinates(t *testing.T) {
	t.Parallel()
	raw := manifest.Warning{
		Category: manifest.WarnSkippedSubmodule,
		Message:  "skipping submodule",
		Detail:   []string{"module", "commit"},
	}
	input := &gittree.FS{Warnings: []manifest.Warning{raw}}
	id := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "source",
	}
	warnings := gitWarnings(
		input,
		id,
		"https://credential:password@example.test/repository.git?token=private#private",
		"main",
	)
	assert.Equal(t, warnings[0].Resource, id)
	assert.Diff(t, warnings[0].Detail, []string{"module", "commit", "https://example.test/repository.git", "main"})
	assert.Diff(t, input.Warnings, []manifest.Warning{raw})
}
