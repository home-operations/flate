package loader

import (
	"slices"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

type inheritedNamespaces []string

func (n *inheritedNamespaces) collectVisits(
	l *Loader,
	path string,
	authored manifest.NamedResource,
	owners []manifest.NamedResource,
) {
	authoritative := false
	inactive := l.scopes.inactive
	for v := range l.scopes.fileVisits(path) {
		if inactive && v.input != 0 && !l.scopes.inputs[v.input-1].active {
			continue
		}
		if v.deferMaterialization || v.role != kustomizeinput.Resource || l.selfClaim(v.owner, authored) {
			continue
		}
		if v.membershipOnly {
			if authoritative {
				continue
			}
		} else if !authoritative {
			*n = (*n)[:0]
			authoritative = true
		}
		fallback := l.namespaceFallback(v.fallback, v.namespace, authored, owners)
		ns := manifest.Resolve(authored, v.namespace, fallback).Namespace
		if !slices.Contains(*n, ns) {
			*n = append(*n, ns)
		}
	}
}

func (l *Loader) namespaceFallback(
	fallback string,
	namespace string,
	authored manifest.NamedResource,
	owners []manifest.NamedResource,
) string {
	if authored.Namespace == "" && namespace == "" && len(owners) == 1 {
		if ks, ok := l.CatalogObject(owners[0]).(*manifest.Kustomization); ok {
			fallback = ks.TargetNamespace
		}
	}
	return fallback
}

func (n *inheritedNamespaces) finish(
	l *Loader,
	authored manifest.NamedResource,
	forced string,
	owners []manifest.NamedResource,
) []string {
	if len(*n) == 0 {
		if forced != "" {
			return []string{manifest.Resolve(authored, forced, "").Namespace}
		}
		if authored.Namespace != "" {
			return []string{authored.Namespace}
		}
		n.collectOwners(l, authored, owners)
	}
	if len(*n) == 0 {
		return []string{authored.Namespace}
	}
	slices.Sort(*n)
	return *n
}

func (n *inheritedNamespaces) collectOwners(
	l *Loader,
	authored manifest.NamedResource,
	owners []manifest.NamedResource,
) {
	for _, owner := range owners {
		if ks, ok := l.CatalogObject(owner).(*manifest.Kustomization); ok {
			ns := manifest.Resolve(authored, "", ks.TargetNamespace).Namespace
			if !slices.Contains(*n, ns) {
				*n = append(*n, ns)
			}
		}
	}
}
