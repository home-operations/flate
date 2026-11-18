package discovery

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/internal/engine/change/inputprojection"
	"github.com/home-operations/flate/internal/engine/selection"

	kustypes "sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/source/safepath"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/pkg/manifest"
)

// Entry keeps structural identity separate from the path used for admission.
type Entry struct {
	ID        manifest.ClusterID
	Path      string
	Input     *store.SourceArtifact
	prepared  *entryInputs
	aggregate bool
	roots     []int
}

type entryInputs struct {
	input       store.SourceArtifact
	parsed      *loader.ParsedInputs
	aliasViews  map[string]*store.SourceArtifact
	sources     map[manifest.NamedResource]entryResolution
	projections *manifest.ProducerProjections
	packages    []*plannedPackage
}

func (e *Entry) matches(input *store.SourceArtifact, scan string) bool {
	if e.prepared == nil || input == nil || scan != e.Path {
		return false
	}
	prepared := e.prepared
	authored := inputprojection.Authored(input)
	return prepared.parsed.Matches(authored) &&
		input.LocalPath == prepared.input.LocalPath && input.LocalRoot == prepared.input.LocalRoot &&
		prepared.sameContent(authored)
}

func (p *entryInputs) sameContent(input *store.SourceArtifact) bool {
	return input.Key == p.input.Key && input.Content == p.input.Content &&
		input.Revision == p.input.Revision && input.Digest == p.input.Digest
}

// Plan requires an isolated session and resumes after PendingSources are ready.
func (d *Session) Plan(ctx context.Context) ([]Entry, error) {
	d.pendingSources = d.pendingSources[:0]
	switch err := d.loadPlan(ctx); {
	case errors.Is(err, errPendingSources):
		return nil, nil
	case err != nil:
		return nil, err
	}
	l := d.loader
	input := d.cfg.Input
	roots, err := d.clusterRoots(ctx)
	switch {
	case errors.Is(err, errPendingSources):
		return nil, nil
	case err != nil:
		return nil, err
	}
	entries := clusterEntries(roots, input, d.cfg.Path)
	for i := range entries {
		if entries[i].aggregate {
			entries[i].Input, err = aggregateInput(input, d.cfg.Path, d.packages)
			if err != nil {
				return nil, err
			}
			entries[i].aggregate = entries[i].Input != input
		}
	}
	if err := l.PersistInputs(ctx); err != nil {
		return nil, err
	}
	for i := range entries {
		if err := d.prepareEntry(ctx, &entries[i], roots); err != nil {
			return nil, err
		}
	}
	return entries, nil
}

func (p *entryInputs) merge(root *clusterRoot) {
	if root.packageBuild != nil {
		p.packages = append(p.packages, root.packageBuild)
	}
	p.parsed = root.parsed
	if p.aliasViews == nil {
		p.aliasViews = maps.Clone(root.views)
	} else {
		maps.Copy(p.aliasViews, root.views)
	}
	if p.sources == nil {
		p.sources = make(map[manifest.NamedResource]entryResolution, len(root.sources))
	}
	for id, resolution := range root.sources {
		previous := p.sources[id]
		previous.merge(&resolution)
		p.sources[id] = previous
	}
}

func (d *Session) prepareEntry(ctx context.Context, entry *Entry, roots []clusterRoot) error {
	input := d.cfg.Input
	prepared := &entryInputs{input: *input, projections: d.cfg.Projections}
	var scope *rootScope
	for _, j := range entry.roots {
		root := &roots[j]
		scope = &rootScope{occurrences: root.Occurrences, parent: scope}
		prepared.merge(root)
	}
	for _, id := range slices.SortedFunc(maps.Keys(prepared.sources), manifest.NamedResource.Compare) {
		resolution := prepared.sources[id]
		_, projected, producers := scope.source(id)
		if len(producers) == 0 {
			continue
		}
		object := resolution.object
		if resolution.synthetic {
			object = nil
		}
		projection := object == nil && projected != nil
		if projection {
			object = projected
		}
		resolved, resolveErr := d.admission().resolve(ctx, id, &sourceRequest{
			object: object, producers: producers, scoped: true,
			entry: resolution.entry, projected: resolution.projected || projection,
		})
		if resolveErr != nil {
			return resolveErr
		}
		prepared.sources[id] = resolved
	}
	if prepared.parsed == nil {
		prepared.parsed = d.loader.ParsedInputs()
	}
	entry.prepared = prepared
	return nil
}

func (d *Session) loadPlan(ctx context.Context) error {
	if d.planLoaded {
		return nil
	}
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return err
	}
	l := d.loader
	l.SourceRoot = d.cfg.Input.LocalPath
	l.IgnoreFile = d.cfg.KRMIgnoreFile
	if _, err := l.Load(
		ctx,
		d.cfg.Path,
		manifest.NamedResource{},
		d.cfg.Work.Record(manifest.WorkPreparation, d.cfg.Path),
	); err != nil {
		return err
	}
	l.IgnoreFile = ""
	l.PreferExisting = true
	d.planLoaded = true
	return nil
}

func clusterEntries(roots []clusterRoot, input *store.SourceArtifact, scan string) []Entry {
	parents := producerGroups(roots, input.LocalPath)
	groups := map[int]struct {
		coordinate string
		roots      []int
	}{}
	anchors := map[int][]string{}
	for i := range roots {
		root := &roots[i]
		if !root.bootstrap {
			continue
		}
		group := parents[i]
		members, exists := groups[group]
		coordinate := members.coordinate
		if !exists {
			coordinate = root.Path
		}
		for coordinate != "." && !safepath.Contains(filepath.FromSlash(coordinate), filepath.FromSlash(root.Path)) {
			coordinate = path.Dir(coordinate)
		}
		members.coordinate = coordinate
		groups[group] = members
		anchors[group] = append(anchors[group], root.Anchors...)
	}
	for i, group := range parents {
		if members, exists := groups[group]; exists {
			members.roots = append(members.roots, i)
			groups[group] = members
		}
	}
	entries := make([]Entry, 0, len(groups))
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		members := groups[group]
		coordinate := members.coordinate
		targets := anchors[group]
		slices.Sort(targets)
		targets = slices.Compact(targets)
		if len(targets) == 1 {
			coordinate = targets[0]
		}
		absolute := filepath.Join(input.LocalPath, filepath.FromSlash(coordinate))
		if !safepath.Contains(scan, absolute) {
			coordinate, absolute = members.coordinate, filepath.Join(
				input.LocalPath,
				filepath.FromSlash(members.coordinate),
			)
		}
		entries = append(entries, Entry{
			ID: manifest.ClusterID(coordinate), Path: absolute, Input: input, roots: members.roots,
		})
	}
	if len(entries) == 0 {
		coordinate, _ := filepath.Rel(input.LocalPath, scan)
		members := make([]int, len(roots))
		for i := range members {
			members[i] = i
		}
		return []Entry{
			{
				ID:        manifest.ClusterID(filepath.ToSlash(coordinate)),
				Path:      scan,
				Input:     input,
				aggregate: true,
				roots:     members,
			},
		}
	}
	if len(entries) == 1 {
		entries[0].Path = scan
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(string(a.ID), string(b.ID)) })
	return slices.CompactFunc(entries, func(a, b Entry) bool { return a.ID == b.ID })
}

func aggregateInput(
	input *store.SourceArtifact,
	requested string,
	packages []loader.PackageEvidence,
) (*store.SourceArtifact, error) {
	relative, _ := filepath.Rel(input.LocalPath, requested)
	authored, err := nativeEntry(input.FS, relative)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inspect entry package: %w", err), "")
	}
	if authored {
		return input, nil
	}
	resources, err := aggregateResources(input, requested, packages)
	if err != nil {
		return nil, err
	}
	if len(resources) == 0 {
		return input, nil
	}
	document := kustypes.Kustomization{
		APIVersion: kustypes.KustomizationVersion, Kind: kustypes.KustomizationKind,
		Resources: resources,
	}
	data, err := yaml.Marshal(document)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("encode entry package: %w", err), "")
	}
	const fileMode = 0o644
	overlay := tree.NewBuilder(input.FS)
	if err = overlay.AddFile(
		filepath.ToSlash(filepath.Join(relative, manifest.KustomizeBuilderFilenames[0])),
		data,
		fileMode,
	); err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("construct entry package: %w", err), "")
	}
	content, err := overlay.Build()
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("construct entry package: %w", err), "")
	}
	projected := *input
	projected.LocalRoot = input.OriginPath(input.LocalPath)
	projected.Content, projected.Digest, projected.ContentFingerprint = "", "", nil
	projected.FS = &inputprojection.Overlay{Tree: content, Authored: input}
	return &projected, nil
}

func producerGroups(roots []clusterRoot, root string) []int {
	parents := make([]int, len(roots))
	declarations := map[selection.OccurrenceID]int{}
	for i := range roots {
		parents[i] = i
		for j := range roots[i].seeds {
			occurrence := &roots[i].seeds[j]
			if safepath.Contains(filepath.Join(root, roots[i].Path),
				filepath.Join(occurrence.Declaration.Source, occurrence.Declaration.File)) {
				declarations[occurrence.Declaration] = i
			}
		}
	}
	var find func(int) int
	find = func(i int) int {
		if parents[i] != i {
			parents[i] = find(parents[i])
		}
		return parents[i]
	}
	join := func(left, right int) {
		a, b := find(left), find(right)
		parents[max(a, b)] = min(a, b)
	}
	joinRootSources(roots, declarations, join)
	joinRootOccurrences(roots, declarations, join)
	for i := range parents {
		parents[i] = find(i)
	}
	return parents
}

func aggregateResources(
	input *store.SourceArtifact,
	requested string,
	packages []loader.PackageEvidence,
) ([]string, error) {
	var resources []string
	for i := range packages {
		pkg := &packages[i]
		absolute := filepath.Join(input.LocalPath, filepath.FromSlash(pkg.Path))
		if !pkg.Independent || !safepath.Contains(requested, absolute) {
			continue
		}
		exists, err := nativeEntry(input.FS, pkg.Path)
		if err != nil {
			return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("inspect aggregate package: %w", err), "")
		}
		if exists {
			resource, _ := filepath.Rel(requested, absolute)
			resources = append(resources, filepath.ToSlash(resource))
			continue
		}
		for j := range pkg.Occurrences {
			occurrence := &pkg.Occurrences[j]
			file := filepath.Join(occurrence.Declaration.Source, filepath.FromSlash(occurrence.Declaration.File))
			if !safepath.Contains(requested, file) {
				continue
			}
			resource, _ := filepath.Rel(requested, file)
			resources = append(resources, filepath.ToSlash(resource))
		}
	}
	slices.Sort(resources)
	resources = slices.Compact(resources)
	return resources, nil
}

func nativeEntry(input fs.FS, dir string) (bool, error) {
	for _, name := range manifest.KustomizeBuilderFilenames {
		_, err := fs.Stat(input, filepath.ToSlash(filepath.Join(dir, name)))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("inspect builder in %q: %w", dir, err)
		}
		return true, nil
	}
	return false, nil
}

func joinRootSources(roots []clusterRoot, declarations map[selection.OccurrenceID]int, join func(int, int)) {
	for i := range roots {
		for _, coordinate := range roots[i].sourceDeclarations {
			owner, known := declarations[coordinate]
			if known && roots[owner].bootstrap {
				join(i, owner)
			}
		}
	}
	for i := range roots {
		for _, resolution := range roots[i].sources {
			for j := range resolution.producers {
				producer := &resolution.producers[j]
				if owner, known := declarations[producer.declaration]; known {
					join(i, owner)
				}
			}
		}
	}
}

func joinRootOccurrences(roots []clusterRoot, declarations map[selection.OccurrenceID]int, join func(int, int)) {
	for i := range roots {
		if !roots[i].bootstrap {
			continue
		}
		for j := range roots[i].Occurrences {
			occurrence := &roots[i].Occurrences[j]
			owner, known := declarations[occurrence.Declaration]
			if known && roots[owner].bootstrap {
				join(i, owner)
			}
		}
	}
}
