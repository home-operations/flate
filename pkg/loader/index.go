package loader

import (
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// FinalizeIndex records immutable ownership facts and seeds the runtime producer registry.
func (l *Loader) FinalizeIndex(repoRoot string, producers *manifest.ProducerIndex) {
	if l.documents.cache == nil {
		l.documents.cache = map[string]document{}
	}
	l.documents.mountSources(l.Store)
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
	// Resolve each directory's resource declarations once, even when it contains many Flux objects.
	resources := map[string]map[string]struct{}{}
	for id, r := range l.Index.Files() {
		switch id.Kind {
		case manifest.KindKustomization, manifest.KindHelmRelease, manifest.KindResourceSet:
			r.Parent, _ = manifest.StrictParent(claims, r.File, id, own[id])
		}
		if id.Kind == manifest.KindKustomization {
			if ks, ok := l.Store.Get[*manifest.Kustomization](id); ok {
				_, r.Pinned, _ = SourceView(l.Store, ks, repoRoot)
			}
			if r.Path != "" {
				dir := filepath.Dir(r.Path)
				listed, seen := resources[dir]
				if !seen {
					if k, _ := l.documents.read(dir); k != nil {
						listed = make(map[string]struct{}, len(k.Resources))
						for _, ref := range k.Resources {
							if resolved, ok := resolvePath(dir, ref); ok {
								listed[resolved] = struct{}{}
							}
						}
					}
					resources[dir] = listed
				}
				if listed != nil {
					_, included := listed[r.Path]
					r.Excluded = !included
				}
			}
		}
		l.Index.Records[id] = r
	}
	// Graph traversal supplies readers before file-backed data ownership is frozen.
	l.buildRenderers(repoRoot, producers)
	l.fillOwners()
	for id, r := range l.Index.Records {
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
}

func (l *Loader) fillOwners() {
	for id, r := range l.Index.Files() {
		if id.Kind == manifest.KindConfigMap || id.Kind == manifest.KindSecret {
			r.Owners, _ = l.Index.OwnersOfFile(r.File)
			l.Index.Records[id] = r
		}
	}
	for id, r := range l.Index.Records {
		if (id.Kind == manifest.KindConfigMap || id.Kind == manifest.KindSecret) && id.Namespace != "" {
			raw := id
			raw.Namespace = ""
			for _, owner := range l.Index.Records[raw].Owners {
				r.Owners = appendUniqueProducer(r.Owners, owner)
			}
			l.Index.Records[id] = r
		}
	}
}
