package manifest

import (
	"os"
	"path/filepath"

	yaml "go.yaml.in/yaml/v4"
)

// ReadKustomizeComponents returns the top-level `components:` field of
// the kustomization file at base (resolved relative to repoRoot).
// Returns nil when the file is missing / unreadable / malformed —
// pure best-effort, as the caller's claim graph is built from the
// union of declared sources and on-disk reads.
//
// Lives here so loader's parent index and change's ownership index
// share the same on-disk reader. Previously each module had its own
// copy; behavior must agree across them or change attribution and
// loader discovery silently disagree on which files belong to which
// Flux Kustomization.
func ReadKustomizeComponents(repoRoot, base string) []string {
	for _, name := range KustomizeBuilderFilenames {
		data, err := os.ReadFile(filepath.Join(repoRoot, base, name)) //nolint:gosec // path composed from known cluster layout
		if err != nil {
			continue
		}
		var doc struct {
			Components []string `yaml:"components"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			continue
		}
		return doc.Components
	}
	return nil
}
