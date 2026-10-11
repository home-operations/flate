package loader

import (
	"cmp"
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
	var ids []manifest.NamedResource
	for id := range l.Index.Files() {
		if id.Namespace == "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	namespaces := make(map[string][]namespaceContext, len(ids))
	for _, id := range ids {
		namespaces[l.Index.Records[id].File] = nil
	}
	kss := l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization)
	for v := range l.graphVisits(repoRoot, kss, l.standaloneRoots(repoRoot, kss)) {
		file := v.coordinate
		if _, needed := namespaces[file]; needed {
			scope := namespaceContext{namespace: v.namespace, fallback: v.fallback}
			if !slices.Contains(namespaces[file], scope) {
				namespaces[file] = append(namespaces[file], scope)
			}
		}
	}
	claims := &manifest.DiscoveryIndex{}
	for _, id := range ids {
		record := l.Index.Records[id]
		obj := l.Store.GetObject(id)
		scopes, fallback := l.scopesFor(record, namespaces[record.File], repoRoot, claims)
		for _, scope := range scopes {
			ns := cmp.Or(scope.namespace, fallback, scope.fallback)
			if ns == "" {
				continue
			}
			if obj != nil {
				updated := cloneWithNamespace(obj, ns)
				if updated == nil {
					continue
				}
				l.Store.AddObject(updated)
			}
			next := id
			next.Namespace = ns
			projected := record
			if len(scopes) > 1 {
				projected.Refs = slices.Clone(record.Refs)
			}
			for i := range projected.Refs {
				if projected.Refs[i].Namespace == "" {
					projected.Refs[i].Namespace = ns
				}
			}
			l.Index.Records[next] = projected
			delete(l.Index.Records, id)
			l.Store.DeleteObject(id)
		}
	}
}

func (l *Loader) scopesFor(record manifest.DiscoveryRecord, scopes []namespaceContext, repoRoot string, claims *manifest.DiscoveryIndex) ([]namespaceContext, string) {
	if len(scopes) > 0 && !slices.ContainsFunc(scopes, func(scope namespaceContext) bool { return scope.namespace == "" }) {
		return scopes, ""
	}
	if claims.Claims == nil {
		claims.Claims = map[string][]manifest.NamedResource{}
		for _, claim := range l.Claims(repoRoot) {
			claims.Claims[claim.Prefix] = appendUniqueProducer(claims.Claims[claim.Prefix], claim.ID)
		}
	}
	owners, _ := claims.OwnersOfFile(record.File)
	if len(scopes) == 0 {
		for _, owner := range owners {
			if ks, ok := l.Store.Get[*manifest.Kustomization](owner); ok {
				scopes = append(scopes, namespaceContext{fallback: cmp.Or(ks.TargetNamespace, ks.Namespace)})
			}
		}
		return scopes, ""
	}
	if len(owners) == 1 {
		if ks, ok := l.Store.Get[*manifest.Kustomization](owners[0]); ok {
			return scopes, cmp.Or(ks.TargetNamespace, ks.Namespace)
		}
	}
	return scopes, ""
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
	case *manifest.Bucket:
		c := *o
		c.Namespace = ns
		return &c
	case *manifest.ExternalArtifact:
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
