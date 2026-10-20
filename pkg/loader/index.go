package loader

import (
	"maps"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/internal/kustpath"
	"github.com/home-operations/flate/pkg/manifest"
)

// FinalizeIndex freezes source-qualified graph contributions before reconciliation.
func (l *Loader) FinalizeIndex(repoRoot string) {
	l.mountSources()
	if l.scopes == nil || !l.scopes.complete {
		l.ensureScopes()
		for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
			l.scopes.ready[l.sourceState(ks.SourceID())] = struct{}{}
		}
		l.ApplyNamespaces(repoRoot)
	}
	var claims []manifest.KSClaim
	if l.scopes != nil {
		claims = l.scopes.claims
	}
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
		if origin := l.documents.localPath(r.Path); origin != r.Path {
			rel, _ := filepath.Rel(repoRoot, origin)
			r.File = filepath.ToSlash(rel)
		}
		if id.KindInfo().Parent {
			r.Parent, _ = manifest.StrictParent(claims, r.File, id, own[id])
		}
		if id.KindInfo().Structural {
			if ks, ok := l.Store.Get[*manifest.Kustomization](id); ok {
				_, r.Pinned, _ = SourceView(l.Store, ks.SourceID(), repoRoot)
			}
			if r.Path != "" {
				dir := filepath.Dir(r.Path)
				listed, seen := resources[dir]
				if !seen {
					if k, _ := l.documents.read(dir); k != nil {
						listed = make(map[string]struct{}, len(k.Resources))
						for _, ref := range k.Resources {
							if resolved, ok := kustpath.Resolve(dir, ref, kustpath.Directory); ok {
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
	emittedFiles := map[string][]manifest.NamedResource{}
	l.buildProductions(repoRoot, emittedFiles)
	l.finalizeRecords(repoRoot, emittedFiles)
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopes = nil
	for _, file := range slices.Sorted(maps.Keys(l.Index.Errors)) {
		origin, _ := filepath.Rel(repoRoot, l.documents.localPath(filepath.Join(repoRoot, filepath.FromSlash(file))))
		origin = filepath.ToSlash(origin)
		if origin != file {
			l.Index.Errors[origin] = l.Index.Errors[file]
			delete(l.Index.Errors, file)
		}
	}
	for _, ids := range l.Index.Claims {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
	for _, ids := range l.Index.Readers {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
}
