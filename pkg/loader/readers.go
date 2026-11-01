package loader

import (
	"cmp"
	"context"
	"io/fs"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/chartinput"
	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
)

func (l *Loader) recordReader(file, local string, reader manifest.FileReader) {
	add := func(file string) {
		readers := l.Index.Readers[file]
		i, found := slices.BinarySearchFunc(readers, reader, func(a, b manifest.FileReader) int {
			if order := cmp.Or(a.Reader.Compare(b.Reader), a.Source.Compare(b.Source)); order != 0 {
				return order
			}
			if a.Materialize != b.Materialize {
				if a.Materialize {
					return 1
				}
				return -1
			}
			if a.Exact == b.Exact {
				return 0
			}
			if a.Exact {
				return 1
			}
			return -1
		})
		if !found {
			l.Index.Readers[file] = slices.Insert(readers, i, reader)
		}
	}
	add(file)
	if local != "" {
		add(local)
	}
}

func (l *Loader) finalizeRecords(ctx context.Context, repoRoot string, emittedFiles map[string][]manifest.NamedResource) error {
	controls := map[manifest.NamedResource][]string{}
	if l.documents.compact && l.scopes != nil {
		for _, contribution := range l.scopes.contributions {
			if contribution.owner == (manifest.NamedResource{}) || contribution.membershipOnly || !contribution.active {
				continue
			}
			for _, visit := range contribution.visits {
				if visit.role == kustomizeinput.Resource && visit.document == nil {
					continue
				}
				for _, file := range []string{visit.coordinate, visit.localCoordinate()} {
					if file != "" {
						controls[contribution.owner] = append(controls[contribution.owner], file)
					}
				}
			}
		}
	}
	for owner, files := range controls {
		slices.Sort(files)
		controls[owner] = slices.Compact(files)
	}

	objects := map[manifest.NamedResource]manifest.BaseManifest{}
	object := func(id manifest.NamedResource) manifest.BaseManifest {
		if record, ok := l.Index.Lookup(id); ok && record.Builder != "" {
			if child := l.IndexedObject(id, record); child.Obj != nil {
				return child.Obj
			}
		}
		if obj := l.Store.GetObject(id); obj != nil {
			return obj
		}
		if obj, seen := objects[id]; seen {
			return obj
		}
		r, _ := l.Index.Lookup(id)
		obj := l.IndexedObject(id, r).Obj
		objects[id] = obj
		return obj
	}
	sources := map[manifest.NamedResource]graphVisit{}
	charts := map[string][]string{}
	templateBodies := map[string][][]byte{}
	templateRefs := map[[2]string]manifest.TemplateProjection{}
	templateWalks := map[[2]string]bool{}
	visited := map[manifest.NamedResource]struct{}{}
	var collect func(manifest.NamedResource, manifest.NamedResource, bool) error
	var observe func(manifest.Reference, manifest.NamedResource, bool) error
	project := func(input manifest.TemplateInput, consumer manifest.NamedResource) error {
		if input.Reader.Namespace == "" && input.Reader.KindInfo().Namespaced() {
			input.Reader.Namespace = consumer.Namespace
		}
		if l.Index.IsFileIndexed(input.Reader) {
			return nil
		}
		ref := input.Reference
		if ref.NamespaceDefault && ref.Namespace == "" {
			ref.Namespace = consumer.Namespace
		}
		if ref.ContentInput() {
			if l.Store.ReferenceFacts(ref.NamedResource, l.Index).Supplied != nil {
				return nil
			}
			record, _ := l.Index.Lookup(ref.NamedResource)

			if record.File != "" {
				if len(record.Productions) == 0 {
					l.recordReader(record.File, "", manifest.FileReader{Reader: consumer, Source: manifest.BootstrapSourceID, Exact: true, Materialize: true})
				}
				for _, production := range record.Productions {
					source := production.Source
					if source == (manifest.NamedResource{}) {
						source = manifest.BootstrapSourceID
					}
					l.recordReader(record.File, "", manifest.FileReader{Reader: consumer, Source: source, Exact: true, Materialize: true})
				}
			}
		}
		return observe(ref, consumer, true)
	}

	observe = func(ref manifest.Reference, consumer manifest.NamedResource, materialize bool) error {
		if ref.ContentInput() && l.Store.ReferenceFacts(ref.NamedResource, l.Index).Supplied != nil {
			return nil
		}
		if ref.Content {
			record, _ := l.Index.Lookup(ref.NamedResource)
			for _, file := range record.Inputs {
				l.recordReader(file, "", manifest.FileReader{Reader: consumer, Source: manifest.BootstrapSourceID, Materialize: materialize})
			}
		}
		if ref.Use == "chart" {
			if err := collect(ref.NamedResource, consumer, materialize); err != nil {
				return err
			}
			return nil
		}
		if len(ref.Files) == 0 && ref.ChartPath == "" || ref.Name == "" || ref.Kind == "" {
			return nil
		}
		v, seen := sources[ref.NamedResource]
		if !seen {
			root, pinned, external := SourceView(l.Store, ref.NamedResource, repoRoot)
			if !external {
				v.root = root
				prefix, _ := filepath.Rel(repoRoot, root)
				if prefix != "." {
					v.prefix = filepath.ToSlash(prefix)
					if !pinned {
						v.localBase = l.documents.localBase(root, repoRoot)
					}
				}
			}
			sources[ref.NamedResource] = v
		}
		if v.root == "" {
			return nil
		}
		add := func(name string) {
			v.coordinate = path.Join(v.prefix, name)
			l.recordReader(v.coordinate, v.localCoordinate(), manifest.FileReader{Reader: consumer, Source: ref.NamedResource, Exact: true, Materialize: materialize})
		}
		for _, name := range ref.Files {
			clean := path.Clean(name)
			if ref.Subtree {
				var valid bool
				clean, valid = resolveRoot(l.Store, ref.NamedResource, cmp.Or(name, "."))
				if !valid {
					continue
				}
			} else if name == "" || !fs.ValidPath(clean) || clean == "." || strings.IndexByte(clean, 0) >= 0 {
				continue
			}
			v.coordinate = path.Join(v.prefix, clean)
			l.recordReader(v.coordinate, v.localCoordinate(), manifest.FileReader{Reader: consumer, Source: ref.NamedResource, Exact: !ref.Subtree, Materialize: materialize})
		}

		if ref.ChartPath == "" {
			return nil
		}
		ref.ChartPath = path.Clean(ref.ChartPath)
		location := filepath.Join(v.root, filepath.FromSlash(ref.ChartPath))
		names, seen := charts[location]
		if !seen {
			input, name := l.documents.input(location)
			info, err := fs.Stat(input, name)
			if err == nil {
				if info.IsDir() {
					names = append(names, path.Join(ref.ChartPath, ".helmignore"))
					if sub, err := fs.Sub(input, name); err == nil {
						// Reconciliation owns chart-local loading errors, including policy failures.
						_ = chartinput.Walk(ctx, sub, true, func(name string, _ fs.FileInfo, data []byte) error {
							names = append(names, path.Join(ref.ChartPath, name))
							if strings.HasPrefix(name, "templates/") || strings.Contains(name, "/templates/") {
								if data == nil {
									var err error
									data, err = fs.ReadFile(sub, name)
									if err != nil {
										return nil
									}
								}
								templateBodies[location] = append(templateBodies[location], data)
							}
							return nil
						})
					}
				} else if info.Mode().IsRegular() {
					names = append(names, ref.ChartPath)
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			charts[location] = names
		}
		for _, name := range names {
			add(name)
		}
		namespace := consumer.Namespace
		if producer, ok := object(consumer).(emissionProducer); ok {
			namespace = producer.EmissionPolicy().Namespace
		}
		key := [2]string{location, namespace}
		refs, seen := templateRefs[key]
		if !seen {
			templateRefs[key] = manifest.TemplateProjection{}
			for _, data := range templateBodies[location] {
				next := manifest.ProjectTemplate(data, namespace)
				refs.Inputs = append(refs.Inputs, next.Inputs...)
				refs.Targets = append(refs.Targets, next.Targets...)
			}
			templateRefs[key] = refs
		}
		if templateWalks[key] {
			return nil
		}
		templateWalks[key] = true
		for _, target := range refs.Targets {
			if l.templateTargets == nil {
				l.templateTargets = map[manifest.NamedResource][]manifest.ProducerTarget{}
			}
			targets := l.templateTargets[consumer]
			if !slices.ContainsFunc(targets, func(known manifest.ProducerTarget) bool { return known.NamedResource == target.NamedResource }) {
				l.templateTargets[consumer] = append(targets, target)
			}
		}
		for _, input := range refs.Inputs {
			if err := project(input, consumer); err != nil {
				return err
			}
		}

		return nil
	}
	collect = func(id, consumer manifest.NamedResource, materialize bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, seen := visited[id]; seen {
			return nil
		}
		visited[id] = struct{}{}
		if l.documents.compact {
			if occurrence, ok := l.locations[id]; ok {
				for _, input := range occurrence.facts.templateInputs {
					if err := project(input, consumer); err != nil {
						return err
					}
				}
				for ref := range occurrence.references() {
					if err := observe(ref, consumer, materialize); err != nil {
						return err
					}
				}
				return nil
			}
		}
		for ref := range manifest.References(object(id), manifest.ReferenceRelations) {
			if err := observe(ref, consumer, materialize); err != nil {
				return err
			}
		}
		return nil
	}
	for id, r := range l.Index.Records {
		occurrence, located := l.locations[id]
		if repoRoot == "" || len(r.Refs) == 0 && (!located || len(occurrence.facts.templateInputs) == 0) {
			continue
		}
		clear(visited)
		clear(templateWalks)
		if err := collect(id, id, false); err != nil {
			return err
		}
	}
	for id, r := range l.Index.Records {
		if r.Path != "" {
			if id.KindInfo().Discovery {
				if _, known := l.roots[id]; !known {
					if l.roots == nil {
						l.roots = map[manifest.NamedResource]bool{}
					}
					l.roots[id] = l.rootAdmission(r.Path, r.File, id.KindInfo().Structural).admits(l, id, r)
				}
			} else if doc := l.documents.cache[r.Path]; r.Authored != (manifest.NamedResource{}) && !doc.rootKnown {
				doc.rootAdmitted = l.rootAdmission(r.Path, r.File, false).admits(l, id, r)
				doc.rootKnown = true
				l.documents.cache[r.Path] = doc
			}
		}
		if r.File != "" {
			if parents := emittedFiles[r.File]; id.KindInfo().Structural && r.Parent == (manifest.NamedResource{}) && len(parents) == 1 && parents[0] != id {
				r.Parent = parents[0]
			}
			if len(r.Productions) == 0 && r.Authored != (manifest.NamedResource{}) {
				r.Productions = []manifest.Production{{Authored: r.Authored, Path: r.Path, File: r.File, Source: manifest.BootstrapSourceID}}
			}
		}
		// Owner evidence MUST survive compaction while provisional origins are removed.
		r.Productions = slices.DeleteFunc(r.Productions, func(p manifest.Production) bool {
			if p.Owner != (manifest.NamedResource{}) || p.Path == "" {
				return false
			}
			return slices.ContainsFunc(r.Productions, func(owner manifest.Production) bool {
				return owner.Owner != (manifest.NamedResource{}) && (owner.Source == p.Source && owner.Authored == p.Authored && owner.Path == p.Path && owner.File == p.File ||
					p.Source == manifest.BootstrapSourceID && p.Path == r.Path && !l.AdmitsRoot(l.CatalogObject(id), p.Path))
			})
		})
		if l.documents.compact && l.AdmitsRoot(l.CatalogObject(id), r.Path) {
			for _, p := range r.Productions {
				if p.Owner != (manifest.NamedResource{}) && p.Materializer(id) == (manifest.NamedResource{}) && p.Declaration == (manifest.NamedResource{}) && p.Authored != (manifest.NamedResource{}) {
					p.Owner = manifest.NamedResource{}
					if !slices.Contains(r.Productions, p) {
						r.Productions = append(r.Productions, p)
					}
				}
			}
		}
		slices.SortFunc(r.Productions, func(a, b manifest.Production) int { return a.Owner.Compare(b.Owner) })
		if !l.documents.compact {
			r.Declaration = l.IndexedObject(id, r).Obj
		}
		if l.documents.compact && id.KindInfo().Parent && r.Parent == (manifest.NamedResource{}) && !l.AdmitsRoot(l.CatalogObject(id), r.Path) {
			var owner manifest.NamedResource
			multiple := false
			for _, p := range r.Productions {
				if parent := p.Materializer(id); parent != (manifest.NamedResource{}) {
					multiple = multiple || owner != (manifest.NamedResource{}) && owner != parent
					owner = parent
				}
			}
			if !multiple && owner != id {
				r.Parent = owner
			}
		}
		if len(controls) != 0 {
			owners := []manifest.NamedResource{manifest.Resolve(r.Parent, "", id.Namespace)}
			for _, p := range r.Productions {
				owner := p.Materializer(id)
				if owner != (manifest.NamedResource{}) && !slices.Contains(owners, owner) {
					owners = append(owners, owner)
				}
			}
			for _, owner := range owners {
				files := controls[owner]
				if len(r.Inputs) == 0 {
					r.Inputs = files
				} else if len(files) != 0 && !slices.Equal(r.Inputs, files) {
					r.Inputs = append(slices.Clone(r.Inputs), files...)
					slices.Sort(r.Inputs)
					r.Inputs = slices.Compact(r.Inputs)
				}
			}
		}
		l.Index.Records[id] = r
	}
	return nil
}
