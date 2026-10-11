package loader

import (
	"cmp"
	"maps"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// ApplyNamespaces projects discovery namespaces using the shared document reader.
func (l *Loader) ApplyNamespaces(repoRoot string) {
	l.stampTransformerTargetNamespaces(repoRoot)
	l.applyNamespaceInheritance(repoRoot)
	l.applyDefaultNamespaces()
}

func (l *Loader) applyNamespaceInheritance(repoRoot string) {
	if l.Index == nil || len(l.Index.Records) == 0 || repoRoot == "" {
		return
	}
	b := &selfProduceBuilder{repoRoot: repoRoot, documents: l.documents,
		namespaces: map[string][]namespaceContext{}}
	b.walkGraph(l)
	l.Index.Claims = map[string][]manifest.NamedResource{}
	for _, claim := range l.Claims(repoRoot) {
		l.Index.Claims[claim.Prefix] = appendUniqueProducer(l.Index.Claims[claim.Prefix], claim.ID)
	}
	records := maps.Clone(l.Index.Records)
	for _, id := range slices.SortedFunc(maps.Keys(records), manifest.NamedResource.Compare) {
		record := records[id]
		if record.File == "" || id.Namespace != "" {
			continue
		}
		obj := l.Store.GetObject(id)
		scopes := b.namespaces[record.File]
		if len(scopes) == 0 {
			owners, _ := l.Index.OwnersOfFile(record.File)
			for _, owner := range owners {
				if ks, ok := l.Store.Get[*manifest.Kustomization](owner); ok {
					scopes = append(scopes, namespaceContext{fallback: cmp.Or(ks.TargetNamespace, ks.Namespace)})
				}
			}
		}
		for _, scope := range scopes {
			ns := cmp.Or(scope.namespace, id.Namespace, scope.fallback)
			if ns == "" || ns == id.Namespace {
				continue
			}
			next := id
			next.Namespace = ns
			projected := record
			if obj == nil {
				projected.Path = ""
			}
			projected.Refs = slices.Clone(record.Refs)
			for i := range projected.Refs {
				if projected.Refs[i].Namespace == "" {
					projected.Refs[i].Namespace = ns
				}
			}
			if obj != nil {
				if updated := cloneWithNamespace(obj, ns); updated != nil {
					l.Store.AddObject(updated)
				}
			}
			l.Index.Records[next] = projected
			if obj != nil || (id.Kind != manifest.KindConfigMap && id.Kind != manifest.KindSecret) {
				delete(l.Index.Records, id)
				l.Store.DeleteObject(id)
			}
		}
	}
}

type namespaceContext struct {
	namespace string
	fallback  string
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
