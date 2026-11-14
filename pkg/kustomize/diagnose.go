package kustomize

import (
	"cmp"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resmap"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/resid"
	"sigs.k8s.io/yaml"

	kustomizestate "github.com/home-operations/flate/internal/kustomize"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/internal/kustpath"
)

// Bound directory rebuilds so one failed aggregate cannot trigger thousands of builds.
const dupDiagBuildLimit = 256

const minimumDuplicateProducers = 2

func duplicateProducers(memFS filesys.FileSystem, subDir, subPath string, kustomizationYAML []byte) string {
	var kus types.Kustomization
	if yaml.Unmarshal(kustomizationYAML, &kus) != nil {
		return ""
	}

	type resourceKey struct {
		group, version, kind, name, namespace string
	}
	type producers struct {
		id    string
		paths []string
	}
	pathsByID := map[resourceKey]producers{}
	attempted := 0
	for ref := range kustomizeinput.Kustomization(&kus) {
		if ref.Role != kustomizeinput.Resource {
			continue
		}
		entry := ref.Path
		resolved, local := kustpath.Resolve(subDir, entry, kustpath.Directory)
		if !local {
			continue
		}
		rel := "./" + path.Join(strings.TrimPrefix(subPath, "./"), entry)
		for _, id := range entryIDs(memFS, resolved, &attempted) {
			// ResId.Equals ignores cached scope fields and compares effective namespaces.
			key := resourceKey{id.Group, id.Version, id.Kind, id.Name, id.EffectiveNamespace()}
			group := pathsByID[key]
			display := id.String()
			if group.id == "" || display < group.id {
				group.id = display
			}
			group.paths = append(group.paths, rel)
			pathsByID[key] = group
		}
	}

	maps.DeleteFunc(pathsByID, func(_ resourceKey, group producers) bool {
		return len(group.paths) < minimumDuplicateProducers
	})
	if len(pathsByID) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("  duplicate resource(s) produced by multiple accumulated paths:")
	for _, group := range slices.SortedFunc(maps.Values(pathsByID), func(a, b producers) int {
		return cmp.Compare(a.id, b.id)
	}) {
		fmt.Fprintf(&b, "\n    %s", group.id)
		for _, p := range slices.Sorted(slices.Values(group.paths)) {
			fmt.Fprintf(&b, "\n      - %s", p)
		}
	}
	return b.String()
}

func entryIDs(memFS filesys.FileSystem, entry string, attempted *int) []resid.ResId {
	isDir := memFS.IsDir(entry)
	var data []byte
	if isDir {
		if *attempted >= dupDiagBuildLimit {
			return nil
		}
		(*attempted)++
	} else {
		var err error
		data, err = memFS.ReadFile(entry)
		if err != nil {
			return nil
		}
	}

	// Parsing and AllIds also read krusty's process-global OpenAPI state.
	kustomizestate.BuildMutex.Lock()
	defer kustomizestate.BuildMutex.Unlock()
	var rm resmap.ResMap
	var err error
	if isDir {
		rm, err = kustomizestate.Build(memFS, entry)
	} else {
		rm, err = resmap.NewFactory(provider.NewDefaultDepProvider().GetResourceFactory()).NewResMapFromBytes(data)
	}
	if err != nil {
		return nil
	}
	return rm.AllIds()
}
