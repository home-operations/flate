package kustomize

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
)

// dupDiagBuildLimit caps how many child paths the diagnostic rebuilds, so a
// pathological aggregate can't turn one failed build into thousands. Only ever
// reached on the failure path.
const dupDiagBuildLimit = 256

// duplicateProducers turns a krusty "already registered id" build failure into a
// report naming every accumulated path that emits the colliding resource.
//
// An aggregate build — most often a Flux Kustomization whose spec.path has no
// kustomization.yaml, so RenderFlux synthesizes one over the whole subtree —
// merges every child into one build. kustomize then names only the colliding id
// and the path it was re-added from, never the one that registered it first,
// which makes a localized duplicate (a copy-paste namespace, a component pulled
// in twice) hard to place.
//
// Each child builds cleanly on its own — the id only clashes once accumulated —
// so rebuilding the directory entries one at a time and inverting
// resource id → paths pinpoints every producer. It returns "" when a collision
// lies within one entry or against a leaf file, leaving the raw error unchanged.
//
// kustomizationYAML is the merged kustomization RenderFlux already built for
// subDir (absolute); subPath is the repo-relative spec.path, used to render
// producer paths the reader can open.
func duplicateProducers(memFS filesys.FileSystem, subDir, subPath string, kustomizationYAML []byte) string {
	var kus types.Kustomization
	if yaml.Unmarshal(kustomizationYAML, &kus) != nil {
		return ""
	}

	// Invert each child's resource ids into id → the paths that emit it.
	pathsByID := map[string][]string{}
	attempted := 0
	for ref := range kustomizeinput.Kustomization(&kus) {
		if ref.Role != kustomizeinput.Resource {
			continue
		}
		entry := ref.Path
		if attempted >= dupDiagBuildLimit {
			break
		}
		dir, local := kustpath.Resolve(subDir, entry, kustpath.Directory)
		if !local {
			continue
		}
		if !memFS.IsDir(dir) {
			continue
		}
		attempted++
		rel := "./" + path.Join(strings.TrimPrefix(subPath, "./"), entry)
		for _, id := range buildIDs(memFS, dir) {
			pathsByID[id] = append(pathsByID[id], rel)
		}
	}

	// Keep only the ids two or more paths emit — the collisions the merged build
	// rejects — then render them deterministically.
	maps.DeleteFunc(pathsByID, func(_ string, paths []string) bool { return len(paths) < 2 })
	if len(pathsByID) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  duplicate resource(s) produced by multiple accumulated paths:")
	for _, id := range slices.Sorted(maps.Keys(pathsByID)) {
		fmt.Fprintf(&b, "\n    %s", id)
		for _, p := range slices.Sorted(slices.Values(pathsByID[id])) {
			fmt.Fprintf(&b, "\n      - %s", p)
		}
	}
	return b.String()
}

// buildIDs builds one accumulation entry in isolation and returns the resource
// ids it emits, or nil when the entry doesn't build on its own (it then can't be
// a producer). BuildMutex guards krusty's process-global state — shared with scope lookup and Helm post-rendering.
func buildIDs(memFS filesys.FileSystem, dir string) []string {
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	rm, err := kustomizestate.Build(memFS, dir)
	if err != nil {
		return nil
	}
	allIDs := rm.AllIds()
	ids := make([]string, 0, len(allIDs))
	for _, id := range allIDs {
		ids = append(ids, id.String())
	}
	return ids
}
