package loader

import (
	"iter"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

type staticScope struct{ loader *Loader }

func (s staticScope) ResetStatic() {
	l := s.loader
	l.resetAdmissions()
	for context, traversal := range l.scopes.walks {
		traversal.reached, traversal.controlled = selection.RootUnknown, false
		l.scopes.walks[context] = traversal
	}
}

func (l *Loader) resetAdmissions() {
	l.roots.retainIndependent()
	//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
	for file, doc := range l.documents.cache {
		doc.eligible, doc.rootKnown, doc.rootAdmitted, doc.terminal = selection.RootUnknown, false, false, false
		l.documents.cache[file] = doc
	}
}

func (s staticScope) Files() iter.Seq2[string, selection.StaticFile] {
	return func(yield func(string, selection.StaticFile) bool) {
		//nolint:gocritic // Map iteration supplies value snapshots without an additional lookup.
		for file, doc := range s.loader.documents.cache {
			if !yield(
				file,
				selection.StaticFile{
					Eligibility:  doc.eligible,
					Loaded:       doc.loaded,
					ObjectsKnown: doc.objects != nil,
					HasObjects:   len(doc.objects) != 0,
				},
			) {
				return
			}
		}
	}
}

func (s staticScope) Eligibility(file string) selection.RootEligibility {
	return s.loader.documents.cache[file].eligible
}

func (s staticScope) SetEligibility(file string, state selection.RootEligibility) {
	doc := s.loader.documents.cache[file]
	doc.eligible = state
	s.loader.documents.cache[file] = doc
}

func (s staticScope) PlainEntry() string {
	if s.loader.entry == entryPlain {
		return s.loader.entryPath
	}
	return ""
}
func (s staticScope) Terminal(file string) bool       { return s.loader.terminalInput(file) }
func (s staticScope) DirectoryFile(dir string) string { return s.loader.documents.cache[dir].file }
func (s staticScope) Coordinate(file string) string {
	return s.loader.fileCoordinate(new(s.loader.documents).localPath(file))
}
func (s staticScope) Claims() []manifest.KSClaim { return s.loader.scopes.claims }
func (s staticScope) Owner(id manifest.NamedResource) (string, bool) {
	r, indexed := s.loader.Index.Lookup(id)
	return r.Path, indexed
}

func (s staticScope) Replaced(id manifest.NamedResource) bool {
	object := s.loader.Store.GetObject(id)
	return object != nil && object != s.loader.admitted[id]
}

func (s staticScope) Declared(id manifest.NamedResource) bool {
	r, _ := s.loader.Index.Lookup(id)
	return s.loader.IndexedObject(id, r).Obj != nil
}
func (s staticScope) Contributions() int { return len(s.loader.scopes.contributions) }
func (s staticScope) Contribution(i int) selection.StaticContribution {
	c := s.loader.scopes.contributions[i]
	return selection.StaticContribution{
		Owner:          c.owner,
		Source:         c.source,
		Visits:         len(c.visits),
		Unshared:       c.unshared,
		MembershipOnly: c.membershipOnly,
	}
}

func (s staticScope) Visit(i, j int) selection.StaticVisit {
	v := s.loader.scopes.contributions[i].visits[j]
	return selection.StaticVisit{
		File:      v.file,
		LocalFile: v.localFile,
		Origin:    v.origin,
		Dir:       v.dir,
		Resource:  v.role == kustomizeinput.Resource,
	}
}

func (s staticScope) Traversal(i int) selection.StaticTraversal {
	c := s.loader.scopes.contributions[i]
	t := s.loader.scopes.walks[c.context]
	return selection.StaticTraversal{Reached: t.reached, Controlled: t.controlled}
}

func (s staticScope) SetTraversal(i int, state selection.StaticTraversal) {
	c := s.loader.scopes.contributions[i]
	t := s.loader.scopes.walks[c.context]
	t.reached, t.controlled = state.Reached, state.Controlled
	s.loader.scopes.walks[c.context] = t
}

func (s staticScope) SetActive(i int, active bool) {
	c := &s.loader.scopes.contributions[i]
	c.active = active
	if c.input != 0 {
		s.loader.scopes.inputs[c.input-1].active = active
	}
}
func (s staticScope) Inputs() int               { return len(s.loader.scopes.inputs) }
func (s staticScope) InputActive(i int) bool    { return s.loader.scopes.inputs[i].active }
func (s staticScope) SetInactive(inactive bool) { s.loader.scopes.inactive = inactive }
