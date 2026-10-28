package loader

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// FinalizeIndex freezes source-qualified graph contributions before reconciliation.
func (l *Loader) FinalizeIndex(ctx context.Context, repoRoot string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mountSources()
	if l.scopes == nil || !l.scopes.complete {
		l.ensureScopes()
		if repoRoot != "" {
			for _, ks := range l.Store.ListAs[*manifest.Kustomization](manifest.FluxKustomizeDomain, manifest.KindKustomization) {
				state := l.sourceState(ks.SourceID(), repoRoot)
				scope := l.scopeSource(state)
				l.scopes.sources[scope].ready = true
			}
		}
		l.admit(repoRoot)
	}
	var claims []manifest.KSClaim
	if l.scopes != nil {
		claims = slices.DeleteFunc(l.scopes.claims, func(c manifest.KSClaim) bool {
			r, _ := l.Index.Lookup(c.ID)
			return l.documents.cache[r.Path].eligible == fileInactive
		})
		l.scopes.claims = claims
	}
	l.Index.Readers = map[string][]manifest.FileReader{}
	own := map[manifest.NamedResource]map[string]struct{}{}
	for prefix, ids := range l.Index.Claims {
		ids = slices.DeleteFunc(ids, func(id manifest.NamedResource) bool {
			r, _ := l.Index.Lookup(id)
			return l.documents.cache[r.Path].eligible == fileInactive
		})
		if len(ids) == 0 {
			delete(l.Index.Claims, prefix)
			continue
		}
		l.Index.Claims[prefix] = ids
		for _, id := range ids {
			if _, indexed := l.Index.Lookup(id); !indexed {
				continue
			}
			if own[id] == nil {
				own[id] = map[string]struct{}{}
			}
			own[id][prefix] = struct{}{}
		}
	}
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
		}
		l.Index.Records[id] = r
	}
	emittedFiles := map[string][]manifest.NamedResource{}
	if err := l.buildProductions(ctx, repoRoot, emittedFiles); err != nil {
		return err
	}

	if err := l.finalizeRecords(ctx, repoRoot, emittedFiles); err != nil {
		return err
	}
	for file, doc := range l.documents.cache {
		doc.terminal = l.terminalInput(file)
		l.documents.cache[file] = doc
	}
	for _, ids := range l.Index.Claims {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopes = nil
	return nil
}
