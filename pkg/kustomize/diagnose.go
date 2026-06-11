package kustomize

import (
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	fluxkustomize "github.com/fluxcd/pkg/kustomize"
	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"
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
// resource id → paths pinpoints every producer. It returns "" for any error
// that isn't a duplicate id, or when the collision can't be attributed (it lives
// within one entry, or against a leaf file), leaving the caller to surface the
// raw error unchanged.
//
// kustomizationYAML is the merged kustomization RenderFlux already built for
// subDir (absolute); subPath is the repo-relative spec.path, used to render
// producer paths the reader can open.
func duplicateProducers(buildErr error, memFS filesys.FileSystem, subDir, subPath string, kustomizationYAML []byte) string {
	var kus kustypes.Kustomization
	if !strings.Contains(buildErr.Error(), "already registered id") ||
		yaml.Unmarshal(kustomizationYAML, &kus) != nil {
		return ""
	}

	// Invert each child's resource ids into id → the paths that emit it.
	pathsByID := map[string][]string{}
	for _, entry := range kus.Resources {
		dir := filepath.Join(subDir, entry)
		if len(pathsByID) >= dupDiagBuildLimit || !memFS.IsDir(dir) {
			continue
		}
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
// a producer). BuildMutex guards krusty's process-global state — see flux.go.
func buildIDs(memFS filesys.FileSystem, dir string) []string {
	BuildMutex.Lock()
	defer BuildMutex.Unlock()
	rm, err := fluxkustomize.Build(memFS, dir)
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(rm.AllIds()))
	for _, id := range rm.AllIds() {
		ids = append(ids, id.String())
	}
	return ids
}
