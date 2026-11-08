package loader

import (
	"iter"
	"path/filepath"

	"github.com/home-operations/flate/internal/selection"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// scopeConstruction is confined to sequential discovery. Physical walks are
// shared by equal contexts; each owner retains its own visits and claims.
type scopeConstruction struct {
	authored      []authoredRecord
	projections   [2]map[identityProjection]manifest.NamedResource
	walks         map[graphContext]scopeTraversal
	records       map[manifest.NamedResource]manifest.DiscoveryRecord
	candidates    map[manifest.NamedResource]store.Child
	scanned       map[string]manifest.DocumentRole
	sources       []sourceScope
	sourceBuf     [1]sourceScope
	inputs        []scopeInput
	complete      bool
	inactive      bool
	visits        map[string][]scopeVisit
	contributions []scopeContribution
	claims        []manifest.KSClaim
	roots         []string
	rootFacts     []selection.NativeRootFacts
	traversalWork map[string]*manifest.WorkRecord
}

type scopeInput struct {
	owner       *manifest.Kustomization
	source      int
	published   bool
	active      bool
	ownerClaims ownerClaims
	derived     bool
}

type sourceState struct {
	id       manifest.NamedResource
	object   manifest.BaseManifest
	artifact *store.SourceArtifact
	repoRoot string
}

type sourceScope struct {
	sourceState

	root                    string
	origin                  string
	pinned, external, ready bool
}

func (l *Loader) sourceState(id manifest.NamedResource, repoRoot string) sourceState {
	art, _ := l.Store.GetArtifact(id, store.ArtifactContent).(*store.SourceArtifact)
	return sourceState{id: id, object: l.Store.GetObject(id), artifact: art, repoRoot: repoRoot}
}

func (l *Loader) scopeSource(stateArg *sourceState) int {
	state := *stateArg

	for i := range l.scopes.sources {
		if l.scopes.sources[i].sourceState == state {
			return i
		}
	}
	location := SourceView(l.Store, state.id, state.repoRoot)
	l.scopes.sources = append(
		l.scopes.sources,
		sourceScope{
			sourceState: state,
			root:        location.Path,
			pinned:      location.Pinned,
			external:    location.External,
			origin:      new(l.documents).localBase(location.Path, state.repoRoot),
		},
	)
	return len(l.scopes.sources) - 1
}

func (l *Loader) ensureScopes() {
	if l.scopes == nil {
		l.scopes = &scopeConstruction{
			walks:   map[graphContext]scopeTraversal{},
			scanned: map[string]manifest.DocumentRole{},
			visits:  map[string][]scopeVisit{},
		}
		l.scopes.sources = l.scopes.sourceBuf[:0]
	}
}

func (l *Loader) refreshClaims(repoRoot string, kss []store.Child) {
	l.ensureScopes()
	if !l.scopesMatch(kss, repoRoot) {
		l.prepareClaimInputs(repoRoot, kss)
	}
	l.publishScopeClaims(repoRoot)
}

func (l *Loader) prepareClaimInputs(repoRoot string, kss []store.Child) {
	l.scopes.complete = false
	previous := l.scopes.inputs
	inputs := previous
	if len(inputs) != len(kss) {
		inputs = make([]scopeInput, len(kss))
	} else {
		for i := range kss {
			child := &kss[i]
			if inputs[i].owner.Named() != child.Obj.Named() {
				inputs = make([]scopeInput, len(kss))
				break
			}
		}
	}
	for i := range kss {
		child := &kss[i]
		//nolint:errcheck // Kustomization-only inputs must panic on an invalid object type.
		ks := child.Obj.(*manifest.Kustomization)
		view := l.scopeSource(new(l.sourceState(ks.SourceID(), repoRoot)))
		published := l.Store.GetObject(ks.Named()) != nil
		input := scopeInput{owner: ks, source: view, published: published}
		for len(previous) != 0 && previous[0].owner.Named().Compare(ks.Named()) < 0 {
			previous = previous[1:]
		}
		if len(previous) != 0 && previous[0].owner == ks && previous[0].source == view {
			input = previous[0]
			input.published = published
		}
		inputs[i] = input
	}
	l.scopes.inputs = inputs
}

func (l *Loader) claimView(repoRoot string, eligible func(scopeInput) bool) ([]manifest.KSClaim, bool) {
	var pending []*manifest.Kustomization
	for _, input := range l.scopes.inputs {
		if !l.scopes.sources[input.source].external && eligible(input) && !input.derived {
			pending = append(pending, input.owner)
		}
	}
	var claimBuf [16]ownerClaims
	built := new(l.documents).buildClaims(repoRoot, pending, func(id manifest.NamedResource) sourceScope {
		return l.scopes.sources[l.scopeSource(new(l.sourceState(id, repoRoot)))]
	}, claimBuf[:0])
	next := 0
	for i := range l.scopes.inputs {
		input := &l.scopes.inputs[i]
		if l.scopes.sources[input.source].external || !eligible(*input) || input.derived {
			continue
		}
		input.ownerClaims = built[next]
		input.derived = true
		next++
	}
	return projectClaims(len(l.scopes.inputs), func(i int) ownerClaims {
		input := l.scopes.inputs[i]
		if l.scopes.sources[input.source].external || !eligible(input) {
			return ownerClaims{}
		}
		return input.ownerClaims
	})
}

func (l *Loader) scopesMatch(kss []store.Child, repoRoot string) bool {
	if len(kss) != len(l.scopes.inputs) {
		return false
	}
	var previous manifest.NamedResource
	indexed := len(l.Index.Records) != 0
	for i := range kss {
		child := &kss[i]
		//nolint:errcheck // Kustomization-only inputs must panic on an invalid object type.
		ks := child.Obj.(*manifest.Kustomization)
		input := &l.scopes.inputs[i]
		if ks != input.owner {
			return false
		}
		published := true
		if indexed {
			published = l.Store.GetObject(ks.Named()) != nil
		}
		if published != input.published {
			return false
		}
		view := l.scopes.sources[input.source]
		ref := view.id
		if i == 0 || ref != previous {
			if l.sourceState(ref, repoRoot) != view.sourceState {
				return false
			}
			previous = ref
		}
	}
	return true
}

func (l *Loader) publishScopeClaims(repoRoot string) {
	for _, input := range l.scopes.inputs {
		view := &l.scopes.sources[input.source]
		if !view.external && !view.ready && len(l.scopes.scanned) != 0 {
			_, view.ready = l.scopes.scanned[filepath.Join(view.root, input.owner.Path)]
		}
	}
	l.scopes.claims, _ = l.claimView(repoRoot, func(input scopeInput) bool {
		view := l.scopes.sources[input.source]
		return repoRoot == "" || view.id.Name == "" || view.ready
	})
	l.Index.Claims = make(map[string][]manifest.NamedResource, len(l.scopes.claims))
	for _, claim := range l.scopes.claims {
		l.Index.Claims[claim.Prefix] = appendUniqueProducer(l.Index.Claims[claim.Prefix], claim.ID)
	}
}

// Scope references MUST borrow physical visits without retaining owner-specific copies.
type scopeVisit struct{ contribution, visit int }

func (s *scopeConstruction) fileVisits(file string) iter.Seq[graphVisit] {
	return func(yield func(graphVisit) bool) {
		for _, ref := range s.visits[file] {
			c := &s.contributions[ref.contribution]
			v := c.visits[ref.visit]
			if v.file != file && v.localFile != file {
				continue
			}
			v.owner, v.source, v.input = c.owner, c.source, c.input
			v.membershipOnly = c.membershipOnly
			if c.membershipOnly {
				v.owner, v.deferMaterialization = manifest.NamedResource{}, false
			}
			if !yield(v) {
				return
			}
		}
	}
}
