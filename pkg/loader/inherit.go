package loader

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	yaml "go.yaml.in/yaml/v4"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ApplyNamespaceInheritance fills empty metadata.namespace fields on
// loaded resources from the nearest enclosing namespace directive —
// either a Flux Kustomization's spec.targetNamespace or a
// kustomization.yaml `namespace:` field. This is the load-time analog
// of kustomize-controller's apply-time behavior, without which the
// store ends up with two copies of the same resource (one with the
// inherited namespace, one with namespace=""). repoRoot anchors the
// kustomization.yaml lookups; sourceFiles is mutated as ids are
// rewritten.
func ApplyNamespaceInheritance(s *store.Store, sourceFiles map[manifest.NamedResource]string, repoRoot string) {
	if len(sourceFiles) == 0 {
		return
	}

	// Index #1 — kustomize.yaml `namespace:` directives, keyed by
	// the directory containing the kustomization file. Indexed first
	// because indexFluxByPath consults it to project a Flux KS's
	// effective namespace before the parent's render fires.
	kustomizeByDir := indexKustomizeNamespaces(sourceFiles, repoRoot)

	// Index #2 — Flux Kustomizations by spec.path → effective
	// namespace (targetNamespace if set, otherwise metadata.namespace,
	// otherwise the kustomize.yaml directive that would patch the KS
	// itself once the parent renders).
	fluxByPath := indexFluxByPath(s, sourceFiles, kustomizeByDir)

	type update struct {
		old, new manifest.NamedResource
		file     string
	}
	var updates []update
	for id, file := range sourceFiles {
		if id.Namespace != "" {
			continue
		}
		ns := resolveNamespace(file, fluxByPath, kustomizeByDir)
		if ns == "" {
			continue
		}
		next := id
		next.Namespace = ns
		updates = append(updates, update{old: id, new: next, file: file})
	}
	for _, u := range updates {
		obj := s.GetObject(u.old)
		if obj == nil {
			continue
		}
		// Store immutability contract (pkg/store doc): clone before
		// mutating, then DeleteObject(old) + AddObject(new).
		updated := cloneWithNamespace(obj, u.new.Namespace)
		if updated == nil {
			continue
		}
		s.DeleteObject(u.old)
		s.AddObject(updated)
		delete(sourceFiles, u.old)
		sourceFiles[u.new] = u.file
	}
}

// pathEntry pairs a slash-suffixed directory prefix with the namespace
// to apply to anything underneath it.
type pathEntry struct {
	prefix string
	ns     string
}

// resolveNamespace returns the most-specific namespace that should
// apply to the resource at file. Flux Kustomizations win over
// kustomize.yaml directives only when their prefix is longer — the
// "deepest wins" rule.
func resolveNamespace(file string, flux, kust []pathEntry) string {
	slashFile := filepath.ToSlash(file)
	var best pathEntry
	for _, group := range [...][]pathEntry{flux, kust} {
		for _, e := range group {
			if !strings.HasPrefix(slashFile, e.prefix) {
				continue
			}
			if len(e.prefix) > len(best.prefix) {
				best = e
			}
		}
	}
	return best.ns
}

// indexFluxByPath returns one pathEntry per Flux Kustomization keyed
// by spec.path → the KS's effective namespace. Mirrors what real Flux
// renders into the cluster:
//   - spec.targetNamespace wins when set (kustomize-controller injects it).
//   - Otherwise metadata.namespace acts as the apply-context default —
//     resources rendered by the KS without explicit namespaces land
//     in the KS's own namespace.
//   - When metadata.namespace is also empty (i.e. the KS hasn't yet
//     had a kustomize directive applied at the time of indexing), fall
//     back to the kustomize.yaml directive that would patch the KS at
//     parent-render time. This handles the cross-tree base/ pattern,
//     where the parent's `replacements:` block injects targetNamespace
//     only at kustomize-build, but flate runs inheritance at load.
//
// resolveNamespace picks the longest-prefix match, so the slice can
// stay unsorted.
func indexFluxByPath(s *store.Store, sourceFiles map[manifest.NamedResource]string, kust []pathEntry) []pathEntry {
	var out []pathEntry
	for _, obj := range s.ListObjects(manifest.KindKustomization) {
		ks, ok := obj.(*manifest.Kustomization)
		if !ok || ks.Path == "" {
			continue
		}
		ns := ks.TargetNamespace
		if ns == "" {
			ns = ks.Namespace
		}
		if ns == "" {
			if file, ok := sourceFiles[ks.Named()]; ok {
				ns = resolveNamespace(file, nil, kust)
			}
		}
		if ns == "" {
			continue
		}
		out = append(out, pathEntry{
			prefix: normalizePrefix(ks.Path),
			ns:     ns,
		})
	}
	return out
}

// indexKustomizeNamespaces reads every ancestor kustomization.yaml of
// each source file and returns one pathEntry per `namespace:`
// directive. Slash-normalized dir keys match the sourceFiles
// coordinate; repoRoot anchors the on-disk reads.
func indexKustomizeNamespaces(sourceFiles map[manifest.NamedResource]string, repoRoot string) []pathEntry {
	dirs := map[string]struct{}{}
	for _, file := range sourceFiles {
		for d := path.Dir(file); d != "." && d != "/" && d != ""; d = path.Dir(d) {
			dirs[d] = struct{}{}
		}
	}
	var out []pathEntry
	for dir := range dirs {
		ns := readKustomizeNamespace(repoRoot, dir)
		if ns == "" {
			continue
		}
		out = append(out, pathEntry{prefix: strings.TrimSuffix(dir, "/") + "/", ns: ns})
	}
	return out
}

// readKustomizeNamespace returns the top-level `namespace:` value of
// a kustomization.yaml in dir (resolved relative to repoRoot), or ""
// if no kustomize file exists or the namespace key is absent.
func readKustomizeNamespace(repoRoot, dir string) string {
	for _, name := range []string{"kustomization.yaml", "kustomization.yml", "Kustomization"} {
		path := filepath.Join(repoRoot, dir, name)
		data, err := os.ReadFile(path) //nolint:gosec // path composed from known cluster layout
		if err != nil {
			continue
		}
		var doc struct {
			Namespace string `yaml:"namespace"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			continue
		}
		return doc.Namespace
	}
	return ""
}

// normalizePrefix turns a Kustomization spec.path into a slash-
// terminated repo-relative prefix suitable for HasPrefix matching.
func normalizePrefix(p string) string {
	p = strings.TrimPrefix(p, "./")
	return strings.TrimSuffix(p, "/") + "/"
}

// cloneWithNamespace returns a shallow copy of obj with metadata.namespace
// (and HelmRelease.Chart.RepoNamespace, when implicit) rewritten to ns.
// Returns nil for kinds the loader doesn't reposition. Honors the Store
// immutability contract — the caller AddObjects the returned pointer
// rather than mutating the stored object in place.
func cloneWithNamespace(obj manifest.BaseManifest, ns string) manifest.BaseManifest {
	switch o := obj.(type) {
	case *manifest.Kustomization:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.HelmRelease:
		c := *o
		c.Namespace = ns
		if c.Chart.RepoNamespace == "" {
			// chartRef.namespace wasn't explicit in the YAML so it
			// implicitly tracks the HR's namespace.
			c.Chart.RepoNamespace = ns
		}
		return &c
	case *manifest.HelmRepository:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.OCIRepository:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.GitRepository:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.HelmChartSource:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.ConfigMap:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.Secret:
		c := *o
		c.Namespace = ns
		return &c
	}
	return nil
}
