package discovery

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Entry keeps structural identity separate from the path used for admission.
type Entry struct {
	ID       manifest.ClusterID
	Path     string
	Input    *store.SourceArtifact
	prepared *entryInputs
}

type entryInputs struct {
	input      store.SourceArtifact
	parsed     *loader.ParsedInputs
	aliasViews map[string]*store.SourceArtifact
	sources    map[manifest.NamedResource]entryResolution
}

func (e *Entry) matches(input *store.SourceArtifact, scan string) bool {
	if e.prepared == nil || input == nil || scan != e.Path {
		return false
	}
	prepared := e.prepared
	return prepared.parsed.Matches(input) &&
		input.LocalPath == prepared.input.LocalPath && input.LocalRoot == prepared.input.LocalRoot &&
		prepared.sameContent(input)
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
	roots, err := d.partitionRoots(ctx)
	switch {
	case errors.Is(err, errPendingSources):
		return nil, nil
	case err != nil:
		return nil, err
	}
	entries, err := partitionEntries(roots, input, d.cfg.Path)
	if err != nil {
		return nil, err
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

func (p *entryInputs) merge(root *entryRoot) {
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

func (d *Session) prepareEntry(ctx context.Context, entry *Entry, roots []entryRoot) error {
	input := d.cfg.Input
	prepared := &entryInputs{input: *input}
	var scope *partitionScope
	for j := range roots {
		root := &roots[j]
		if !safepath.Contains(entry.Path, filepath.Join(input.LocalPath, root.Path)) {
			continue
		}
		scope = &partitionScope{occurrences: root.Occurrences, parent: scope}
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

func partitionEntries(roots []entryRoot, input *store.SourceArtifact, scan string) ([]Entry, error) {
	roots = slices.Clone(roots)
	slices.SortFunc(roots, func(a, b entryRoot) int { return strings.Compare(a.Path, b.Path) })
	edges, anyEdge := separationEdges(roots)
	if !anyEdge {
		structural := manifest.ClusterID(".")
		if len(roots) == 1 {
			structural = manifest.ClusterID(roots[0].Path)
			if len(roots[0].Anchors) == 1 {
				structural = manifest.ClusterID(roots[0].Anchors[0])
			}
		}
		return []Entry{{ID: structural, Path: scan, Input: input}}, nil
	}

	groups, err := partitionGroups(roots, edges)
	if err != nil {
		return nil, err
	}
	var entries []Entry
	for _, group := range groups {
		label, targets := groupPath(roots, group), groupTargets(roots, group)
		var native string
		switch len(targets) {
		case 0:
			candidates := groupScans(roots, group, groups)
			if len(candidates) == 0 {
				return nil, partitionError(roots, edges, "conflicting entry coordinates")
			}
			native = candidates[0]
		case 1:
			label, native = targets[0], targets[0]
		default:
			return nil, partitionError(roots, edges, "conflicting bootstrap targets")
		}
		if err := manifest.ClusterID(label).Validate(); err != nil {
			return nil, err
		}
		absolute := filepath.Join(input.LocalPath, filepath.FromSlash(native))
		if !safepath.Contains(scan, absolute) {
			return nil, partitionError(roots, edges, "entry escapes requested scan")
		}
		entries = append(entries, Entry{ID: manifest.ClusterID(label), Path: absolute, Input: input})
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(string(a.ID), string(b.ID)) })
	for i := 1; i < len(entries); i++ {
		if entries[i-1].ID == entries[i].ID {
			return nil, partitionError(roots, edges, "conflicting entry coordinates")
		}
	}
	return entries, nil
}

func groupTargets(roots []entryRoot, group []int) []string {
	var targets []string
	for _, i := range group {
		for _, target := range roots[i].Anchors {
			if !slices.Contains(targets, target) {
				targets = append(targets, target)
			}
		}
	}
	return targets
}

func groupPath(roots []entryRoot, group []int) string {
	label := roots[group[0]].Path
	for _, i := range group {
		for label != "." && !safepath.Contains(filepath.FromSlash(label), filepath.FromSlash(roots[i].Path)) {
			label = path.Dir(label)
		}
	}
	return label
}

func groupScans(roots []entryRoot, group []int, groups [][]int) []string {
	candidates := append(slices.Clone(roots[group[0]].Scans), groupPath(roots, group))
	for _, i := range group {
		candidates = slices.DeleteFunc(candidates, func(candidate string) bool {
			return !safepath.Contains(filepath.FromSlash(candidate), filepath.FromSlash(roots[i].Path))
		})
	}
	for _, other := range groups {
		if slices.Contains(other, group[0]) {
			continue
		}
		for _, i := range other {
			candidates = slices.DeleteFunc(candidates, func(candidate string) bool {
				return safepath.Contains(filepath.FromSlash(candidate), filepath.FromSlash(roots[i].Path))
			})
		}
	}
	return candidates
}

func separationEdges(roots []entryRoot) ([][]string, bool) {
	type witness struct {
		occurrence *selection.OccurrenceFacts
		root       int
	}
	edges := make([][]string, len(roots))
	for i := range roots {
		edges[i] = make([]string, len(roots))
	}
	byIdentity := map[manifest.NamedResource][]witness{}
	anyEdge := false
	for i := range roots {
		for j := range roots[i].Occurrences {
			occurrence := &roots[i].Occurrences[j]
			for _, previous := range byIdentity[occurrence.Identity] {
				if previous.root == i || previous.occurrence.Declaration == occurrence.Declaration {
					continue
				}
				left, right := fmt.Sprint(previous.occurrence.Declaration), fmt.Sprint(occurrence.Declaration)
				left, right = min(left, right), max(left, right)
				label := fmt.Sprintf("%s at %s and %s", occurrence.Identity, left, right)
				if edges[previous.root][i] == "" || label < edges[previous.root][i] {
					edges[previous.root][i], edges[i][previous.root] = label, label
				}
				anyEdge = true
			}
			byIdentity[occurrence.Identity] = append(
				byIdentity[occurrence.Identity],
				witness{occurrence: occurrence, root: i},
			)
		}
	}
	return edges, anyEdge
}

func partitionGroups(roots []entryRoot, edges [][]string) ([][]int, error) {
	for i := range roots {
		root := &roots[i]
		for j := i + 1; j < len(roots); j++ {
			if edges[i][j] != "" &&
				slices.ContainsFunc(
					root.Anchors,
					func(target string) bool { return slices.Contains(roots[j].Anchors, target) },
				) {
				return nil, partitionError(roots, edges, "incompatible mandatory entry group")
			}
		}
	}
	components := collisionComponents(edges)
	partitions := make([][][]int, len(components))
	// Only one collision component can determine the required number of isolated entries.
	strongest, width := -1, 1
	for i, component := range components {
		groups := partitionComponent(roots, edges, component)
		if slices.ContainsFunc(groups, func(group []int) bool { return groupConflict(edges, group) }) {
			return nil, partitionError(roots, edges, "ambiguous collision constraints")
		}
		partitions[i] = groups
		switch {
		case len(groups) > width:
			strongest, width = i, len(groups)
		case len(groups) == width:
			strongest = -1
		}
	}
	if strongest < 0 {
		return nil, partitionError(roots, edges, "ambiguous collision constraints")
	}
	var groups [][]int
	for i, partition := range partitions {
		for _, group := range partition {
			if i == strongest || len(groupTargets(roots, group)) != 0 {
				groups = append(groups, group)
			}
		}
	}
	return groups, nil
}

func collisionComponents(edges [][]string) [][]int {
	var components [][]int
	seen := make([]bool, len(edges))
	for i := range edges {
		if seen[i] {
			continue
		}
		component := []int{i}
		seen[i] = true
		for start := 0; start < len(component); start++ {
			for j, witness := range edges[component[start]] {
				if !seen[j] && witness != "" {
					seen[j] = true
					component = append(component, j)
				}
			}
		}
		components = append(components, component)
	}
	return components
}

func partitionComponent(roots []entryRoot, edges [][]string, component []int) [][]int {
	assigned := make([]bool, len(roots))
	for i := range assigned {
		assigned[i] = true
	}
	for _, i := range component {
		assigned[i] = false
	}
	var groups [][]int
	for _, i := range component {
		if assigned[i] || len(component) == 1 && len(roots[i].Anchors) == 0 {
			continue
		}
		groups = append(groups, partitionGroup(roots, edges, i, assigned))
	}
	return groups
}

func groupConflict(edges [][]string, group []int) bool {
	for _, a := range group {
		for _, b := range group {
			if edges[a][b] != "" {
				return true
			}
		}
	}
	return false
}

func partitionGroup(roots []entryRoot, edges [][]string, first int, assigned []bool) []int {
	group := []int{first}
	assigned[first] = true
	for start := 0; start < len(group); start++ {
		for j := range roots {
			if assigned[j] || edges[group[start]][j] != "" {
				continue
			}
			if !slices.ContainsFunc(edges[j], func(w string) bool { return w != "" }) {
				continue
			}
			if !slices.Equal(roots[j].Anchors, roots[first].Anchors) {
				continue
			}
			assigned[j] = true
			group = append(group, j)
		}
	}
	return group
}

func partitionError(roots []entryRoot, edges [][]string, reason string) error {
	candidates := make([]string, 0, len(roots))
	var witnesses []string
	for i := range roots {
		root := &roots[i]
		candidates = append(candidates, root.Path)
		for j := i + 1; j < len(roots); j++ {
			if edges[i][j] != "" {
				witnesses = append(witnesses, edges[i][j])
			}
		}
	}
	slices.Sort(candidates)
	slices.Sort(witnesses)
	return manifest.NewError(
		manifest.ErrInput,
		nil,
		fmt.Sprintf(
			"%s: candidates [%s]; witnesses [%s]; select an entry with --path",
			reason,
			strings.Join(candidates, ", "),
			strings.Join(slices.Compact(witnesses), "; "),
		),
	)
}
