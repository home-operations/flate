package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entryApplication interface {
	EntryApplication() (manifest.EntryApplication, bool)
}

func (d *discoverer) applications() []store.Child {
	objects := d.loader.PathObjects()
	applications := objects[:0]
	seen := map[manifest.NamedResource]bool{}
	add := func(child store.Child) {
		if child.Obj == nil || !child.Obj.Named().KindInfo().Render || seen[child.Obj.Named()] {
			return
		}
		if record, indexed := d.loader.Index.Lookup(child.Obj.Named()); indexed && (record.Builder == "" && record.Authored != child.Obj.Named() || record.Builder != "" && record.File == record.Builder) {
			return
		}
		if _, ok := child.Obj.(entryApplication); ok {
			seen[child.Obj.Named()] = true
			applications = append(applications, child)
		}
	}
	for _, child := range objects {
		add(child)
	}
	clear(objects[len(applications):])
	for id, record := range d.loader.Index.Records {
		if !seen[id] {
			add(d.loader.IndexedObject(id, record))
		}
	}
	slices.SortFunc(applications, func(a, b store.Child) int { return a.Obj.Named().Compare(b.Obj.Named()) })
	return applications
}

type applicationEvidence struct {
	object      manifest.BaseManifest
	document    map[string]any
	application manifest.EntryApplication
	source      entryResolution
	owner       manifest.NamedResource
	history     bool
	next        *applicationEvidence
}

// AddApplication records a notifying object's accepted application declaration.
func (a *Admission) AddApplication(st *store.Store, obj manifest.BaseManifest) {
	if a == nil || obj == nil {
		return
	}
	if owner := st.EmissionOwner(obj.Named()); owner != (manifest.NamedResource{}) {
		obj = st.GetDeclaration(owner)
		if obj == nil {
			return
		}
	}
	output, _ := st.GetArtifact(obj.Named(), store.ArtifactContent).(*store.Emission)
	if _, ok := obj.(entryApplication); !ok && output == nil {
		return
	}
	_, doc, descriptor := st.GetEmission(obj.Named())
	descriptor.Finish()
	for {
		previous := a.applications.Load()
		for current := previous; current != nil; current = current.next {
			if current.object.Named() == obj.Named() && !current.history {
				if current.object == obj {
					return
				}
				break
			}
		}
		evidence := &applicationEvidence{object: obj, document: doc, next: previous}
		if a.applications.CompareAndSwap(previous, evidence) {
			return
		}
	}
}

// AddHistory admits application evidence only from validated accepted render history.
func (a *Admission) AddHistory(id manifest.NamedResource, record manifest.DiscoveryRecord) {
	if a == nil || record.Application == "" {
		return
	}
	var document map[string]any
	if err := json.Unmarshal([]byte(record.Application), &document); err != nil {
		return
	}
	obj, _, _, err := manifest.ParseDoc(document, manifest.ParseDocOptions{})
	if err != nil || obj == nil || obj.Named() != id {
		return
	}
	for {
		previous := a.applications.Load()
		evidence := &applicationEvidence{object: obj, document: document, owner: record.ApplicationOwner, history: true, next: previous}
		if a.applications.CompareAndSwap(previous, evidence) {
			return
		}
	}
}

func (a *Admission) applicationSource(st *store.Store, application manifest.EntryApplication) entryResolution {
	resolved := entryResolution{object: st.GetObject(application.Source)}
	resolved.artifact, _ = st.GetArtifact(application.Source, store.ArtifactContent).(*store.SourceArtifact)
	if resolved.artifact != nil && !resolved.artifact.Reads.Valid() {
		resolved.artifact = nil
	}
	if resolved.object == nil {
		resolved.object = a.sources[application.Source].object
	}
	if resolved.object == nil {
		resolved.object = application.Declaration
	}
	return resolved
}

func (a *Admission) entryApplications(st *store.Store, witnesses []manifest.BaseManifest) ([]applicationEvidence, []applicationEvidence) {
	declared := map[manifest.NamedResource]bool{}
	candidates := make(map[manifest.NamedResource]applicationEvidence)
	add := func(obj manifest.BaseManifest, doc map[string]any, history bool) {
		if obj == nil || !obj.Named().KindInfo().Render {
			return
		}
		applier, ok := obj.(entryApplication)
		if !ok {
			return
		}
		application, valid := applier.EntryApplication()
		if !valid {
			return
		}
		id := obj.Named()
		if _, exists := candidates[id]; !exists {
			candidates[id] = applicationEvidence{object: obj, document: doc, application: application, history: history}
		}
	}
	for _, child := range a.declarations {
		add(child.Obj, child.Doc, false)
		declared[child.Obj.Named()] = true
	}
	for _, obj := range witnesses {
		add(obj, nil, false)
	}
	seen := map[manifest.NamedResource]bool{}
	for evidence := a.applications.Load(); evidence != nil; evidence = evidence.next {
		id := evidence.object.Named()
		if seen[id] {
			continue
		}
		seen[id] = true
		if current := st.GetObject(id); current != nil {
			if st.EmissionOwner(id) != (manifest.NamedResource{}) {
				delete(candidates, id)
			}
			add(current, nil, false)
			if output, _ := st.GetArtifact(id, store.ArtifactContent).(*store.Emission); output != nil && output.OutputReads.Valid() {
				for _, child := range output.Children {
					if child.Obj != nil && st.EmissionOwner(child.Obj.Named()) == id {
						delete(candidates, child.Obj.Named())
						add(st.GetDeclaration(child.Obj.Named()), child.Doc, false)
					}
				}
			}
			continue
		}
		if evidence.history {
			add(evidence.object, evidence.document, true)
			candidate := candidates[id]
			candidate.owner = evidence.owner
			candidates[id] = candidate
		}
	}
	roots := make(map[manifest.NamedResource]applicationEvidence)
	var pending []applicationEvidence
	for id, candidate := range candidates {
		obj, doc, descriptor := st.GetEmission(id)
		descriptor.Finish()
		if obj != nil && (!declared[id] || st.EmissionOwner(id) != (manifest.NamedResource{})) {
			candidate.object, candidate.document = obj, doc
			candidate.application, _ = obj.(entryApplication).EntryApplication()
		} else if obj == nil && !candidate.history && st.GetObject(id) != nil {
			continue
		}
		resolved := a.applicationSource(st, candidate.application)
		if resolved.object == nil {
			resolved.object, _ = candidate.application.Source.BindLocal(a.input.LocalPath)
		}
		eligible := false
		if source, ok := resolved.object.(entrySource); ok {
			eligible = a.appliesEntry(candidate.application, source.BootstrapFacts(), ".")
		}
		if root, ok := candidate.object.(*manifest.Kustomization); ok {
			if !candidate.history {
				candidate.owner = st.EmissionOwner(id)
			}
			if candidate.owner != (manifest.NamedResource{}) {
				output, _ := st.GetArtifact(candidate.owner, store.ArtifactContent).(*store.Emission)
				if output == nil || !output.Complete || !output.OutputReads.Valid() || len(output.Failures) != 0 || !slices.ContainsFunc(output.Children, func(child store.Child) bool { return child.Obj != nil && manifest.Equal(child.Obj, root) }) {
					if candidate.history && eligible {
						pending = append(pending, candidate)
					}
					continue
				}
			}
			roots[id] = candidate
			continue
		}
		output, _ := st.GetArtifact(id, store.ArtifactContent).(*store.Emission)
		if output == nil || !output.Complete || !output.OutputReads.Valid() || len(output.Failures) != 0 {
			if eligible {
				pending = append(pending, candidate)
			}
		}
	}
	result := make([]applicationEvidence, 0, len(roots))
	for _, candidate := range roots {
		result = append(result, candidate)
	}
	slices.SortFunc(result, func(a, b applicationEvidence) int { return a.object.Named().Compare(b.object.Named()) })
	return result, pending
}

func (a *Admission) appliesEntry(application manifest.EntryApplication, facts manifest.BootstrapFacts, subtree string) bool {
	if facts.RefKind != "" && facts.RefKind != "branch" {
		return false
	}
	root := filepath.Join(a.input.LocalPath, subtree)
	if origin, err := url.Parse(facts.URL); err == nil && origin.Scheme == "file" {
		root = filepath.Clean(origin.Path)
	}
	target := filepath.Join(root, manifest.NormalizeClaimBase(application.Path))
	return safepath.Contains(root, target) && loader.OriginPath(a.input, target) == loader.OriginPath(a.input, a.path)
}

func (a *Admission) qualifyEntry(ctx context.Context, st *store.Store) (*applicationEvidence, error) {
	a.store = st
	candidates, _ := a.entryApplications(st, nil)
	var selected *applicationEvidence
	var conflicts []string
	for i := range candidates {
		candidate := &candidates[i]
		resolved := a.applicationSource(st, candidate.application)
		var err error
		if resolved.artifact == nil {
			resolved, err = a.resolve(ctx, nil, ResolveRequest{ID: candidate.application.Source, Object: resolved.object, Witnesses: []manifest.BaseManifest{candidate.object}})
		}
		if err != nil {
			return nil, err
		}
		if resolved.artifact == nil {
			continue
		}
		root, pinned, external := loader.SourceView(&resolved, candidate.application.Source, a.input.LocalPath)
		if pinned || external {
			continue
		}
		target := filepath.Join(root, manifest.NormalizeClaimBase(candidate.application.Path))
		if !safepath.Contains(root, target) || loader.OriginPath(resolved.artifact, target) != loader.OriginPath(a.input, a.path) {
			continue
		}
		if selected == nil {
			candidate.source = resolved
			selected = candidate
			continue
		}
		if len(conflicts) == 0 {
			conflicts = append(conflicts, selected.object.Named().String())
		}
		conflicts = append(conflicts, candidate.object.Named().String())
	}
	if len(conflicts) > 1 {
		st.AddWarning(manifest.Warning{Category: manifest.WarnConflictingProducers, Message: "multiple Kustomizations apply the entry; selected the first by resource identity", Detail: conflicts})
	}
	return selected, nil
}

func (r entryResolution) GetObject(manifest.NamedResource) manifest.BaseManifest {
	return r.object
}

func (r entryResolution) GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact {
	if r.artifact == nil {
		return nil
	}
	return r.artifact
}

// BindEntry classifies the entry and binds an accepted ordinary producer.
func (a *Admission) BindEntry(ctx context.Context, st *store.Store) (*manifest.Kustomization, Binding, error) {
	if a == nil {
		return nil, Binding{}, nil
	}
	a.store = st
	for _, obj := range st.ListObjects("", "") {
		a.AddApplication(st, obj)
	}
	selected, err := a.qualifyEntry(ctx, st)
	if err != nil {
		return nil, Binding{}, err
	}
	a.selected = selected
	if selected != nil && st.GetObject(selected.object.Named()) != selected.object {
		st.AddObject(selected.object, selected.document)
	}
	relative, _ := filepath.Rel(a.input.LocalPath, a.path)
	key := manifest.EntryKustomizationID(filepath.ToSlash(relative))
	var root *manifest.Kustomization
	if selected != nil {
		root, _ = selected.object.(*manifest.Kustomization)
	} else {
		if _, pending := a.entryApplications(st, nil); len(pending) != 0 {
			for _, candidate := range pending {
				if candidate.owner != (manifest.NamedResource{}) {
					continue
				}
				if st.GetObject(candidate.object.Named()) != candidate.object {
					st.AddObject(candidate.object, candidate.document)
				}
			}
			return nil, Binding{}, nil
		}
		found := false
		for _, filename := range manifest.KustomizeBuilderFilenames {
			_, err := fs.ReadFile(a.input.FS, filepath.ToSlash(filepath.Join(relative, filename)))
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, Binding{}, fmt.Errorf("%w: inspect entry package: %w", manifest.ErrInput, err)
			}
			if err == nil {
				found = true
				break
			}
		}
		if !found {
			return nil, Binding{}, nil
		}
		root, _ = st.GetObject(key).(*manifest.Kustomization)
		if root == nil {
			root = manifest.NewEntryKustomization(manifest.BootstrapSourceID, filepath.ToSlash(relative), manifest.NamedResource{})
		}
	}
	a.entryBound.Store(true)
	source := root.SourceID()
	if source.Name == "" || source.Kind == "" {
		source = manifest.BootstrapSourceID
	}
	return root, Binding{Producer: root.Named(), Source: source, Key: key, Path: manifest.NormalizeClaimBase(root.Path)}, nil
}
