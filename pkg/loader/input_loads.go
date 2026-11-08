package loader

import (
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

type cachedLoad struct {
	Path        string                `json:"Path,omitempty"`
	Ignore      string                `json:"Ignore,omitempty"`
	Coverage    manifest.DocumentRole `json:"Coverage,omitempty"`
	Files       []cachedAdmission     `json:"Files,omitempty"`
	Directories []cachedDirectory     `json:"Directories,omitempty"`
	Domains     []string              `json:"Domains,omitempty"`
	Inputs      []string              `json:"Inputs,omitempty"`
	Types       []string              `json:"Types,omitempty"`
}

type cachedAdmission struct {
	File       string        `json:"File,omitempty"`
	Kind       admissionKind `json:"Kind,omitempty"`
	Declared   bool          `json:"Declared,omitempty"`
	Coordinate string        `json:"Coordinate,omitempty"`
	Document   int           `json:"Document,omitempty"`
	Item       int           `json:"Item,omitempty"`
}

type cachedDirectory struct {
	Path      string `json:"Path,omitempty"`
	AdHoc     bool   `json:"AdHoc,omitempty"`
	Traversed bool   `json:"Traversed,omitempty"`
}

func inputRelative(root, file string) (string, bool) {
	name, err := filepath.Rel(root, file)
	name = filepath.ToSlash(name)
	return name, err == nil && fs.ValidPath(name)
}

func (l *Loader) cacheLoads(state *inputCache, force bool) ([]cachedLoad, bool) {
	facts := l.sourceFacts(state.input.LocalPath)
	pending := force
	for key, load := range facts.loadInputs() {
		coverage, known := state.loads[key]
		pending = pending || load.complete && load.err == nil && (!known || !covers(coverage, load.coverage))
	}
	if !pending {
		return nil, false
	}
	names := slices.Sorted(maps.Keys(state.types))
	var loads []cachedLoad
	changed := false
	for key, load := range facts.loadInputs() {
		if !load.complete || load.err != nil {
			continue
		}
		cached, valid := cacheLoad(state, key, &load, names)
		if !valid {
			continue
		}
		loads = append(loads, cached)
		coverage, known := state.loads[key]
		changed = changed || !known || !covers(coverage, load.coverage)
	}
	slices.SortFunc(loads, func(a, b cachedLoad) int {
		if a.Path != b.Path {
			return strings.Compare(a.Path, b.Path)
		}
		return strings.Compare(a.Ignore, b.Ignore)
	})
	return loads, changed
}

func cacheLoad(state *inputCache, key loadContext, facts *loadFacts, names []string) (cachedLoad, bool) {
	root := state.input.LocalPath
	name, valid := inputRelative(root, key.path)
	load := cachedLoad{Path: name, Coverage: facts.coverage}
	inputs := map[string]bool{}
	types := map[string]bool{}
	if !valid {
		return cachedLoad{}, false
	}
	if key.ignore != "" {
		load.Ignore, valid = inputRelative(root, key.ignore)
	}
	if !valid {
		return cachedLoad{}, false
	}
	for fileIndex := range facts.files {
		file := &facts.files[fileIndex]
		name, valid := inputRelative(root, file.file)
		if !valid || state.digests[name] == "" || file.kind == admissionWarning || !file.belongsToSource(root) {
			return cachedLoad{}, false
		}
		load.Files = append(load.Files, cachedAdmission{
			File: name, Kind: file.kind, Declared: file.coordinate.Source != "",
			Coordinate: file.coordinate.File, Document: file.coordinate.Document, Item: file.coordinate.item,
		})
		inputs[name] = true
		types[name] = true
	}
	for _, file := range slices.Sorted(maps.Keys(facts.directories)) {
		name, valid := inputRelative(root, file)
		if !valid {
			return cachedLoad{}, false
		}
		directory := facts.directories[file]
		load.Directories = append(
			load.Directories,
			cachedDirectory{Path: name, AdHoc: directory.adHoc, Traversed: directory.traversed},
		)
		if !load.contains(name) {
			load.Domains = append(load.Domains, name)
		}
	}
	ignore := load.Ignore
	if ignore == "" {
		ignore = path.Join(load.Path, ".krmignore")
	}
	inputs[ignore] = true
	load.captureMembership(state, names, inputs, types)
	load.Inputs, load.Types = slices.Sorted(maps.Keys(inputs)), slices.Sorted(maps.Keys(types))
	return load, true
}

func (f *fileAdmission) belongsToSource(root string) bool {
	return f.coordinate.Source == "" || f.coordinate.Source == root
}

func (l *cachedLoad) captureMembership(state *inputCache, names []string, inputs, types map[string]bool) {
	for _, domain := range l.Domains {
		start, _ := slices.BinarySearch(names, domain)
		for _, name := range names[start:] {
			if domain != "." && name != domain && !strings.HasPrefix(name, domain+"/") {
				if name > domain+"/" {
					break
				}
				continue
			}
			if l.contains(name) {
				types[name] = true
				if _, present := state.digests[name]; present {
					inputs[name] = true
				}
			}
		}
	}
}

func (l *cachedLoad) contains(name string) bool {
	for _, root := range l.Domains {
		if root != "." && name != root && !strings.HasPrefix(name, root+"/") {
			continue
		}
		for parent := path.Dir(name); parent != "." && parent != root; parent = path.Dir(parent) {
			if sourceignore.PruneDir(path.Base(parent), true) &&
				!slices.ContainsFunc(l.Directories, func(d cachedDirectory) bool { return d.Path == parent }) {
				return false
			}
		}
		return true
	}
	return false
}

func (l *cachedLoad) valid() bool {
	if !slices.IsSorted(l.Inputs) || !slices.IsSorted(l.Types) {
		return false
	}
	if !fs.ValidPath(l.Path) || l.Ignore != "" && !fs.ValidPath(l.Ignore) {
		return false
	}
	for _, file := range l.Files {
		if !fs.ValidPath(file.File) || file.Kind >= admissionWarning ||
			file.Declared && !fs.ValidPath(file.Coordinate) {
			return false
		}
	}
	for _, directory := range l.Directories {
		if !fs.ValidPath(directory.Path) {
			return false
		}
	}
	for _, names := range [][]string{l.Domains, l.Types, l.Inputs} {
		if slices.ContainsFunc(names, func(name string) bool { return !fs.ValidPath(name) }) {
			return false
		}
	}

	return true
}

func (l *cachedLoad) matches(changed inputChanges) bool {
	for name := range changed.paths {
		_, input := slices.BinarySearch(l.Inputs, name)
		_, node := slices.BinarySearch(l.Types, name)
		if input || node || l.contains(name) {
			return false
		}
	}
	return true
}

func (l *cachedLoad) context(root string) loadContext {
	key := loadContext{path: filepath.Join(root, filepath.FromSlash(l.Path))}
	if l.Ignore != "" {
		key.ignore = filepath.Join(root, filepath.FromSlash(l.Ignore))
	}
	return key
}

func (l *cachedLoad) restore(root string) loadFacts {
	facts := loadFacts{complete: true, imported: true, coverage: l.Coverage, directories: map[string]loadDirectory{}}
	for _, directory := range l.Directories {
		facts.directories[filepath.Join(root, filepath.FromSlash(directory.Path))] = loadDirectory{
			adHoc:     directory.AdHoc,
			traversed: directory.Traversed,
		}
	}
	for _, file := range l.Files {
		admission := fileAdmission{file: filepath.Join(root, filepath.FromSlash(file.File)), kind: file.Kind}
		if file.Declared {
			admission.coordinate = occurrenceID{
				Source:   root,
				File:     file.Coordinate,
				Document: file.Document,
				item:     file.Item,
			}
		}
		facts.files = append(facts.files, admission)
	}
	return facts
}

func (l *Loader) loadChangesScopes(root string, facts loadFacts, coverage manifest.DocumentRole) bool {
	if !facts.complete {
		return true
	}
	for file, directory := range facts.directories {
		doc := l.documents.cache[file]
		if directory.adHoc && !doc.adHoc || directory.traversed && !doc.traversed {
			return true
		}
	}
	for i := range facts.files {
		file := &facts.files[i]
		doc := l.documents.cache[file.file]
		if loadFileChangesScope(&doc, file.kind, coverage, root == l.entryPath) {
			return true
		}
	}

	for _, input := range l.scopes.inputs {
		scope := &l.scopes.sources[input.source]
		if !scope.ready && filepath.Join(scope.root, input.owner.Path) == root {
			return true
		}
	}
	return false
}

func loadFileChangesScope(doc *document, kind admissionKind, coverage manifest.DocumentRole, initial bool) bool {
	if !covers(doc.coverage, coverage) || initial && !doc.initial {
		return true
	}
	switch kind {
	case admissionData:
		if !doc.selected {
			return true
		}
	case admissionResource, admissionPackage:
		if !doc.loaded && !doc.selected {
			return true
		}
	case admissionBootstrap, admissionWarning:
		return true
	}
	return false
}
