package discovery

import (
	"context"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Entry keeps structural identity separate from native scan and source coordinates.
type Entry struct {
	ID         manifest.ClusterID
	Structural manifest.ClusterID
	NativePath string
	Path       string
	Input      *store.SourceArtifact
	prepared   *entryInputs
}

type entryInputs struct {
	input       store.SourceArtifact
	parsed      *loader.ParsedInputs
	aliasViews  map[string]*gittree.FS
	wipeSecrets bool
}

// Plan establishes entries without publishing seeds into the caller's store.
func Plan(ctx context.Context, cfg Config) ([]Entry, error) {
	if cfg.Input == nil && cfg.Store != nil {
		cfg.Input, _ = cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	}
	cfg.Store = store.New()
	d := newDiscoverer(cfg)
	l := d.loader
	if _, err := d.resolveEntrySources(ctx, false); err != nil {
		return nil, err
	}
	input := cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	l.SourceRoot = input.LocalPath
	l.IgnoreFile = cfg.KRMIgnoreFile
	if _, err := l.Load(ctx, d.cfg.Path); err != nil {
		return nil, err
	}
	l.IgnoreFile = ""
	l.PreferExisting = true
	var roots []entryRoot
	for {
		if _, err := d.resolveEntrySources(ctx, true); err != nil {
			return nil, err
		}
		before := l.PathObjects()
		var err error
		roots, err = d.entryRoots(ctx)
		if err != nil {
			return nil, err
		}
		if slices.EqualFunc(before, l.PathObjects(), func(a, b store.Child) bool {
			sa, pa, _ := a.Obj.SourcePath()
			sb, pb, _ := b.Obj.SourcePath()
			return a.Obj.Named() == b.Obj.Named() && sa == sb && pa == pb && manifest.RenderNamespace(a.Obj) == manifest.RenderNamespace(b.Obj)
		}) {
			break
		}
	}
	prepared := &entryInputs{input: *input, parsed: l.ParsedInputs(), aliasViews: maps.Clone(d.aliasViews), wipeSecrets: cfg.WipeSecrets}
	d.cfg.Entry = &Entry{prepared: prepared}
	roots, err := d.admittedRoots(ctx, roots)
	if err != nil {
		return nil, err
	}
	entries, err := planEntries(roots, input, d.cfg.Path)
	for i := range entries {
		entries[i].prepared = prepared
	}
	return entries, err
}

func planEntries(roots []entryRoot, input *store.SourceArtifact, scan string) ([]Entry, error) {
	edges := make([][]string, len(roots))
	for i := range roots {
		edges[i] = make([]string, len(roots))
	}
	anyEdge := false
	for i, a := range roots {
		for j := i + 1; j < len(roots); j++ {
			for _, x := range a.Occurrences {
				for _, y := range roots[j].Occurrences {
					if x.Identity == y.Identity && x.Declaration != y.Declaration {
						witness := fmt.Sprintf("%s at %s#%d and %s#%d", x.Identity, x.Declaration.File, x.Declaration.Document, y.Declaration.File, y.Declaration.Document)
						if edges[i][j] == "" || witness < edges[i][j] {
							edges[i][j], edges[j][i] = witness, witness
						}
						anyEdge = true
					}
				}
			}
		}
	}
	if !anyEdge {
		structural := manifest.ClusterID(".")
		native := scan
		if len(roots) == 1 {
			root := roots[0]
			structural = manifest.ClusterID(root.Path)
			coordinate := root.Path
			for _, candidate := range root.Scans {
				if candidate != "." || root.Path == "." {
					coordinate = candidate
					break
				}
			}
			if len(root.Anchors) == 1 {
				structural = manifest.ClusterID(root.Anchors[0])
				coordinate = root.Anchors[0]
			}
			native = filepath.Join(input.LocalPath, filepath.FromSlash(coordinate))
		}
		return []Entry{{ID: ".", Structural: structural, Path: scan, NativePath: native, Input: input}}, nil
	}

	for i, root := range roots {
		for j := i + 1; j < len(roots); j++ {
			if edges[i][j] != "" && slices.ContainsFunc(root.Anchors, func(target string) bool { return slices.Contains(roots[j].Anchors, target) }) {
				return nil, partitionError(roots, edges, "incompatible mandatory entry group")
			}
		}
	}
	var groups [][]int
	assigned := make([]bool, len(roots))
	for i := range roots {
		if assigned[i] {
			continue
		}
		active := slices.ContainsFunc(edges[i], func(w string) bool { return w != "" })
		if !active && len(roots[i].Anchors) == 0 {
			continue
		}
		group := []int{i}
		assigned[i] = true
		for start := 0; start < len(group); start++ {
			for j := range roots {
				if assigned[j] || edges[group[start]][j] != "" {
					continue
				}
				if !slices.ContainsFunc(edges[j], func(w string) bool { return w != "" }) {
					continue
				}
				if len(roots[j].Anchors) != 0 && !slices.Equal(roots[j].Anchors, roots[i].Anchors) {
					continue
				}
				assigned[j] = true
				group = append(group, j)
			}
		}
		for _, a := range group {
			for _, b := range group {
				if edges[a][b] != "" {
					return nil, partitionError(roots, edges, "ambiguous collision constraints")
				}
			}
		}
		groups = append(groups, group)
	}
	var entries []Entry
	for _, group := range groups {
		var targets []string
		label := roots[group[0]].Path
		for _, i := range group {
			for _, target := range roots[i].Anchors {
				if !slices.Contains(targets, target) {
					targets = append(targets, target)
				}
			}
			for label != "." && !safepath.Contains(filepath.FromSlash(label), filepath.FromSlash(roots[i].Path)) {
				label = path.Dir(label)
			}
		}
		if len(targets) > 1 {
			return nil, partitionError(roots, edges, "conflicting bootstrap targets")
		}
		native := label
		if len(targets) == 0 {
			for _, candidate := range roots[group[0]].Scans {
				admitted := true
				for _, i := range group {
					if !slices.Contains(roots[i].Scans, candidate) {
						admitted = false
					}
				}
				for _, other := range groups {
					if slices.Contains(other, group[0]) {
						continue
					}
					for _, i := range other {
						if safepath.Contains(filepath.FromSlash(candidate), filepath.FromSlash(roots[i].Path)) {
							admitted = false
						}
					}
				}
				if admitted {
					native = candidate
					break
				}
			}
		}
		if len(targets) == 1 {
			label, native = targets[0], targets[0]
		}
		if err := manifest.ClusterID(label).Validate(); err != nil {
			return nil, err
		}
		absolute := filepath.Join(input.LocalPath, filepath.FromSlash(native))
		if !safepath.Contains(scan, absolute) {
			return nil, partitionError(roots, edges, "entry escapes requested scan")
		}
		entries = append(entries, Entry{ID: manifest.ClusterID(label), Structural: manifest.ClusterID(label), Path: absolute, NativePath: absolute, Input: input})
	}
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(string(a.ID), string(b.ID)) })
	for i := 1; i < len(entries); i++ {
		if entries[i-1].ID == entries[i].ID {
			return nil, partitionError(roots, edges, "conflicting entry coordinates")
		}
	}
	return entries, nil
}

func partitionError(roots []entryRoot, edges [][]string, reason string) error {
	var candidates, witnesses []string
	for i, root := range roots {
		candidates = append(candidates, root.Path)
		for j := i + 1; j < len(roots); j++ {
			if edges[i][j] != "" {
				witnesses = append(witnesses, edges[i][j])
			}
		}
	}
	slices.Sort(candidates)
	slices.Sort(witnesses)
	return fmt.Errorf("%w: %s: candidates [%s]; witnesses [%s]; select an entry with --path", manifest.ErrInput, reason, strings.Join(candidates, ", "), strings.Join(slices.Compact(witnesses), "; "))
}

func (d *discoverer) admittedRoots(ctx context.Context, roots []entryRoot) ([]entryRoot, error) {
	var contexts []entryRoot
	for _, root := range roots {
		if root.nativeContext {
			contexts = append(contexts, root)
		}
	}
	input := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID).(*store.SourceArtifact)
	plan, err := planEntries(contexts, input, d.cfg.Path)
	if err != nil {
		return nil, err
	}
	if len(plan) == 1 && plan[0].ID == "." {
		return roots, nil
	}
	keep := make([]bool, len(roots))
	for _, entry := range plan {
		cfg := d.cfg
		cfg.Store = store.New()
		cfg.Input = input
		cfg.Path = entry.Path
		native := newDiscoverer(cfg)
		l := native.loader
		if _, err := native.resolveEntrySources(ctx, false); err != nil {
			return nil, err
		}
		if err := native.loadManifests(ctx, input.LocalPath, false); err != nil {
			return nil, err
		}
		for i, root := range roots {
			if root.nativeContext {
				keep[i] = true
				continue
			}
			for _, occurrence := range root.Occurrences {
				if !root.seeds[occurrence.Declaration] {
					continue
				}
				if l.AdmitsOccurrence(occurrence) {
					keep[i] = true
					break
				}
			}
		}
	}
	var admitted []entryRoot
	for i, root := range roots {
		if keep[i] {
			admitted = append(admitted, root)
		}
	}
	return admitted, nil
}
