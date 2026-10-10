package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// FinalizeIndex records immutable ownership facts and seeds the runtime producer registry.
// Decoded construction documents are discarded before the index is published.
func (l *Loader) FinalizeIndex(repoRoot string, producers *manifest.ProducerIndex) {
	if l.Index == nil {
		l.Index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
	}
	claims := l.Claims(repoRoot)
	l.Index.Claims = make(map[string][]manifest.NamedResource, len(claims))
	l.Index.Readers = map[string][]manifest.NamedResource{}
	own := map[manifest.NamedResource]map[string]struct{}{}
	for _, c := range claims {
		l.Index.Claims[c.Prefix] = appendUniqueProducer(l.Index.Claims[c.Prefix], c.ID)
		if own[c.ID] == nil {
			own[c.ID] = map[string]struct{}{}
		}
		own[c.ID][c.Prefix] = struct{}{}
	}
	for id, r := range l.Index.Files() {
		switch id.Kind {
		case manifest.KindKustomization, manifest.KindHelmRelease, manifest.KindResourceSet:
			r.Parent, _ = longestStrictParent(claims, r.File, id, own[id])
		}
		if r.Path != "" {
			if k, _ := l.documents.read(filepath.Dir(r.Path)); k != nil {
				r.Excluded = true
				for _, ref := range k.Resources {
					if resolved, ok := resolvePath(filepath.Dir(r.Path), ref); ok && resolved == r.Path {
						r.Excluded = false
						break
					}
				}
			}
		}
		l.Index.Records[id] = r
	}
	for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.KindKustomization) {
		_, pinned, _ := SourceView(l.Store, ks, repoRoot)
		r := l.Index.Records[ks.Named()]
		r.Pinned = pinned
		l.Index.Records[ks.Named()] = r
	}
	l.buildRenderers(repoRoot, producers)
	for id, r := range l.Index.Records {
		if id.Kind != manifest.KindConfigMap && id.Kind != manifest.KindSecret {
			continue
		}
		r.Owners, _ = l.Index.OwnersOfFile(r.File)
		for _, renderer := range r.Renderers {
			r.Owners = appendUniqueProducer(r.Owners, renderer)
		}
		l.Index.Records[id] = r
	}
	for id, r := range l.Index.Records {
		if (id.Kind == manifest.KindConfigMap || id.Kind == manifest.KindSecret) && id.Namespace != "" {
			raw := id
			raw.Namespace = ""
			for _, owner := range l.Index.Records[raw].Owners {
				r.Owners = appendUniqueProducer(r.Owners, owner)
			}
		}
		slices.SortFunc(r.Renderers, manifest.NamedResource.Compare)
		slices.SortFunc(r.DeclarationOwners, manifest.NamedResource.Compare)
		slices.SortFunc(r.Owners, manifest.NamedResource.Compare)
		l.Index.Records[id] = r
	}
	for _, ids := range l.Index.Claims {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
	for _, ids := range l.Index.Readers {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
	l.documents = nil
}
