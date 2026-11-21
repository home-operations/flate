package base

import (
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// Feedback history follows one scheduler node across changes to its declaration.
type renderFeedback struct {
	last  string
	seen  map[string]struct{}
	roots []manifest.NamedResource
}

// CheckRenderFeedback MUST precede fingerprint replay or Helm templating.
func (e *Execution) CheckRenderFeedback(key string) error {
	f := e.feedback
	if f == nil || key == "" || key == f.last {
		return nil
	}

	// A generator can consume its root through its own declaration or through a values or chart source.
	visited := make(map[manifest.NamedResource]bool)
	for input := range e.Reads.Inputs() {
		for id := input; id.Name != "" && !visited[id]; id = e.Store.EmissionOwner(id) {
			visited[id] = true
			if !slices.Contains(f.roots, id) {
				continue
			}
			if _, repeated := f.seen[key]; !repeated {
				return nil
			}
			const message = "generated sync root requests a previous Helm render key"
			e.Reads.AddWarning(manifest.Warning{
				Resource: e.Reads.ID(), Category: manifest.WarnDependencyCycle, Message: message,
			})
			return manifest.NewError(manifest.ErrInput, nil, message)
		}
	}
	e.independentRender = true
	return nil
}

// AcceptRenderFeedback MUST retain only roots from an accepted complete emission.
func (e *Execution) AcceptRenderFeedback(key string, output *store.Emission) {
	if len(e.childGeneration) == 0 && e.feedback == nil || key == "" || output.Footprint() == nil {
		return
	}
	var roots []manifest.NamedResource
	for _, generation := range e.childGeneration {
		for i := range generation.output.Children {
			child := &generation.output.Children[i]
			if child.VisibleGeneration && child.Obj != nil {
				if _, applies := manifest.ApplicationFor(child.Obj); applies {
					roots = append(roots, child.Obj.Named())
				}
			}
		}
	}
	if len(roots) == 0 {
		e.feedback = nil
		return
	}
	if e.feedback == nil || e.independentRender {
		e.feedback = &renderFeedback{seen: make(map[string]struct{})}
	}
	f := e.feedback
	slices.SortFunc(roots, manifest.NamedResource.Compare)
	f.last, f.roots = key, roots
	f.seen[key] = struct{}{}
}
