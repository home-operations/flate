package loader

import (
	"cmp"
	"path"
	"path/filepath"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// ApplyNamespaces projects discovery namespaces using the shared document reader.
func (l *Loader) ApplyNamespaces(repoRoot string) {
	l.stampTransformerTargetNamespaces(repoRoot)
	l.applyNamespaceInheritance(repoRoot)
	l.applyDefaultNamespaces()
}

func (l *Loader) applyNamespaceInheritance(repoRoot string) {
	s := l.Store
	if l.Index == nil || len(l.Index.Records) == 0 {
		return
	}
	kustomizeByDir := l.indexKustomizeNamespaces(repoRoot)
	fluxByPath := l.indexFluxByPath(kustomizeByDir, repoRoot)

	type update struct {
		old, new manifest.NamedResource
		file     string
	}
	var updates []update
	for id, record := range l.Index.Files() {
		file := record.File
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
		// Omitted reference namespaces track inheritance even for deferred consumers.
		record := l.Index.Records[u.old]
		refs := record.Refs
		for i := range refs {
			if refs[i].Namespace == "" {
				refs[i].Namespace = u.new.Namespace
			}
		}
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
		delete(l.Index.Records, u.old)
		record.File = u.file
		l.Index.Records[u.new] = record
	}
}

func (l *Loader) applyDefaultNamespaces() {
	applyDefaultNS(l, l.Store.ListAs[*manifest.ResourceSet](manifest.KindResourceSet))
	applyDefaultNS(l, l.Store.ListAs[*manifest.ResourceSetInputProvider](manifest.KindResourceSetInputProvider))
	applyDefaultNS(l, l.Store.ListAs[*manifest.HelmChartSource](manifest.KindHelmChart))
}

func applyDefaultNS[T manifest.BaseManifest](l *Loader, objs []T) {
	for _, obj := range objs {
		if obj.Named().Namespace != "" {
			continue
		}
		if updated := cloneWithNamespace(obj, manifest.DefaultNamespace); updated != nil {
			old := obj.Named()
			l.Store.DeleteObject(old)
			l.Store.AddObject(updated)
			if record, ok := l.Index.Lookup(old); ok {
				delete(l.Index.Records, old)
				l.Index.Records[updated.Named()] = record
			}
		}
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
func (l *Loader) indexFluxByPath(kust []pathEntry, repoRoot string) []pathEntry {
	s := l.Store
	var out []pathEntry
	for _, ks := range s.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		if ks.Path == "" && ks.SourceName == "" {
			continue
		}
		ns := cmp.Or(ks.TargetNamespace, ks.Namespace)
		if ns == "" {
			if record, ok := l.Index.Lookup(ks.Named()); ok {
				ns = resolveNamespace(record.File, nil, kust)
			}
		}
		if ns == "" {
			continue
		}
		out = append(out, pathEntry{
			prefix: manifest.NormalizeClaimBase(ks.Path) + "/",
			ns:     ns,
		})
		if root, pinned, _ := SourceView(s, ks, repoRoot); pinned {
			if prefix, err := filepath.Rel(repoRoot, filepath.Join(root, manifest.NormalizeClaimBase(ks.Path))); err == nil {
				out = append(out, pathEntry{prefix: filepath.ToSlash(prefix) + "/", ns: ns})
			}
		}
	}
	return out
}

// indexKustomizeNamespaces reads every ancestor kustomization.yaml of
// each source file and returns one pathEntry per `namespace:`
// directive. Slash-normalized dir keys match discovery file
// coordinates; repoRoot anchors the on-disk reads.
func (l *Loader) indexKustomizeNamespaces(repoRoot string) []pathEntry {
	dirs := map[string]struct{}{}
	for _, record := range l.Index.Files() {
		file := record.File
		for d := path.Dir(file); d != "." && d != "/" && d != ""; d = path.Dir(d) {
			dirs[d] = struct{}{}
		}
	}
	var out []pathEntry
	for dir := range dirs {
		k, _ := l.documents.read(filepath.Join(repoRoot, dir))
		if k == nil {
			continue
		}
		ns := k.Namespace
		if ns == "" {
			continue
		}
		out = append(out, pathEntry{prefix: strings.TrimSuffix(dir, "/") + "/", ns: ns})
	}
	return out
}

// cloneWithNamespace returns a shallow copy of obj with metadata.namespace
// (and HelmRelease.Chart.RepoNamespace, when implicit) rewritten to ns.
// Returns nil for kinds the loader doesn't reposition. Honors the Store
// immutability contract — the caller AddObjects the returned pointer
// rather than mutating the stored object in place.
func cloneWithNamespace(obj manifest.BaseManifest, ns string) manifest.BaseManifest {
	switch o := obj.(type) {
	case *manifest.Kustomization:
		c := o.Clone()
		c.Namespace = ns
		if c.SourceRef.Namespace == "" {
			c.SourceNamespace = ns
		}
		for i := range c.DependsOn {
			if c.DependsOn[i].Namespace == "" {
				c.DependsOn[i].Namespace = ns
			}
		}
		if c.Contents != nil {
			manifest.EnsureMetadata(c.Contents)["namespace"] = ns
		}
		return c
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
	case *manifest.ResourceSet:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.ResourceSetInputProvider:
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
