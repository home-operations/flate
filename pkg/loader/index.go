package loader

import (
	"cmp"
	"context"
	"maps"
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
				scope.ready = true
				l.scopes.sources[state] = scope
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
	l.Index.Claims = make(map[string][]manifest.NamedResource, len(claims))
	l.Index.Readers = map[string][]manifest.FileReader{}
	own := map[manifest.NamedResource]map[string]struct{}{}
	for _, c := range claims {
		l.Index.Claims[c.Prefix] = appendUniqueProducer(l.Index.Claims[c.Prefix], c.ID)
		if own[c.ID] == nil {
			own[c.ID] = map[string]struct{}{}
		}
		own[c.ID][c.Prefix] = struct{}{}
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
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopes = nil
	for _, file := range slices.Sorted(maps.Keys(l.Index.Errors)) {
		path := filepath.Join(repoRoot, filepath.FromSlash(file))
		if l.documents.cache[path].eligible == fileInactive {
			delete(l.Index.Errors, file)
			continue
		}
		origin, _ := filepath.Rel(repoRoot, l.documents.localPath(path))
		origin = filepath.ToSlash(origin)
		if origin != file {
			l.Index.Errors[origin] = l.Index.Errors[file]
			delete(l.Index.Errors, file)
		}
	}
	for _, ids := range l.Index.Claims {
		slices.SortFunc(ids, manifest.NamedResource.Compare)
	}
	for _, readers := range l.Index.Readers {
		slices.SortFunc(readers, func(a, b manifest.FileReader) int {
			if order := cmp.Or(a.Reader.Compare(b.Reader), a.Source.Compare(b.Source)); order != 0 {
				return order
			}
			if a.Exact == b.Exact {
				return 0
			}
			if a.Exact {
				return 1
			}
			return -1
		})
	}
	return nil
}
