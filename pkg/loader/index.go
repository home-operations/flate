package loader

import (
	"context"
	"iter"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// FinalizeIndex freezes source-qualified graph contributions before reconciliation.
func (l *Loader) FinalizeIndex(ctx context.Context, repoRoot string, freeze bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.documents.compact = l.Options.Selection.Changed || len(l.Options.Selection.Roots) != 0
	if l.documents.compact && l.frozen {
		return nil
	}
	l.mountSources()
	if l.scopes == nil || !l.scopes.complete || l.documents.compact && freeze {
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
	if l.documents.compact && l.indexDerived && l.derived == l.construction {
		if freeze {
			l.freezeConstruction()
		}
		return nil
	}
	if l.documents.compact && l.indexDerived {
		for id, record := range l.Index.Records {
			if record.Path == "" && record.Builder == "" {
				delete(l.Index.Records, id)
				delete(l.locations, id)
				continue
			}
			record.Productions = nil
			l.Index.Records[id] = record
		}
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
			if parent, ok := manifest.StrictParent(claims, r.File, id, own[id]); ok {
				r.Parent = parent
			}
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
	if l.documents.compact {
		l.derived, l.indexDerived = l.construction, true
		if !freeze {
			return nil
		}
		l.freezeConstruction()
		return nil
	}
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopes = nil
	return nil
}

func (l *Loader) freezeConstruction() {
	l.bindings = map[manifest.NamedResource]*store.SourceArtifact{}
	for source := range l.Options.Changes.Sources() {
		input, _ := l.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
		l.bindings[source] = input
	}
	l.inputReaders = map[manifest.NamedResource][]manifest.NamedResource{}
	for _, object := range l.Store.Objects() {
		id := object.Named()
		if _, indexed := l.Index.Lookup(id); indexed {
			continue
		}
		for ref := range manifest.References(object, manifest.ReferenceRelations) {
			if ref.ContentInput() && (ref.Name != "" || ref.Selector != nil) && !slices.Contains(l.inputReaders[ref.NamedResource], id) {
				l.inputReaders[ref.NamedResource] = append(l.inputReaders[ref.NamedResource], id)
			}
		}
	}
	for id, record := range l.Index.Records {
		var refs iter.Seq[manifest.Reference]
		if object := l.Store.GetDeclaration(id); object != nil {
			refs = manifest.References(object, manifest.ReferenceRelations)
		} else if child := l.candidates[id]; child.Obj != nil && record.Path == "" {
			refs = manifest.References(child.Obj, manifest.ReferenceRelations)
		} else if occurrence, ok := l.locations[id]; ok {
			refs = occurrence.references()
		}
		if refs == nil {
			continue
		}
		for ref := range refs {
			if !ref.ContentInput() || ref.Name == "" && ref.Selector == nil {
				continue
			}
			if !slices.Contains(l.inputReaders[ref.NamedResource], id) {
				l.inputReaders[ref.NamedResource] = append(l.inputReaders[ref.NamedResource], id)
			}
		}
	}
	l.inputObjects = map[manifest.NamedResource][]manifest.NamedResource{}
	for id, occurrence := range l.locations {
		if !occurrence.facts.labeled {
			continue
		}
		key := id
		key.Name = ""
		l.inputObjects[key] = append(l.inputObjects[key], id)
	}
	for id, child := range l.candidates {
		if _, located := l.locations[id]; located {
			continue
		}
		if _, labeled := child.Obj.(interface{ GetLabels() map[string]string }); !labeled {
			continue
		}
		key := id
		key.Name = ""
		l.inputObjects[key] = append(l.inputObjects[key], id)
	}
	l.frozen = true
	l.Store.SetDiscoveryIndex(l.Index)
	l.scopes, l.facts, l.Options.ParsedInputs, l.documents.parsed = nil, nil, nil, nil
}
