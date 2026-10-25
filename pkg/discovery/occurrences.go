package discovery

import (
	"context"
	"path/filepath"
	"slices"

	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

type entryRoot struct {
	Path          string
	Scans         []string
	Occurrences   []loader.Occurrence
	Anchors       []string
	seeds         map[loader.OccurrenceID]bool
	nativeContext bool
}

func (d *discoverer) entryRoots(ctx context.Context) ([]entryRoot, error) {
	packages, err := d.loader.Packages(ctx)
	if err != nil {
		return nil, err
	}
	input := d.cfg.Store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	var pinned []manifest.KSClaim
	if d.hasPins {
		var occurrences []loader.Occurrence
		for _, p := range packages {
			occurrences = append(occurrences, p.Occurrences...)
		}
		pinned, _ = d.pinnedWorkingTreeClaims(input.LocalPath, occurrences)
	}
	type coordinate struct{ path, namespace string }
	childrenByPath := map[coordinate][]loader.Occurrence{}
	var roots []entryRoot
	for _, p := range packages {
		root := entryRoot{Path: p.Path, Scans: p.Scans, seeds: map[loader.OccurrenceID]bool{}}
		for _, o := range p.Occurrences {
			root.seeds[o.Declaration] = true
		}
		seen := map[loader.Occurrence]bool{}
		queue := p.Occurrences
		for len(queue) != 0 {
			o := queue[0]
			queue = queue[1:]
			if len(pinned) != 0 {
				file := filepath.Join(o.Declaration.Source, filepath.FromSlash(o.Declaration.File))
				coordinate, _ := filepath.Rel(input.LocalPath, file)
				if d.pinnedWorkingTreeFile(input.LocalPath, pinned, o.Identity, manifest.DiscoveryRecord{File: filepath.ToSlash(coordinate), Path: file}) {
					continue
				}
			}
			if seen[o] {
				continue
			}
			seen[o] = true
			root.Occurrences = append(root.Occurrences, o)
			obj := o.Object
			if obj.Named().Namespace != o.Identity.Namespace {
				obj = obj.WithNamespace(o.Identity.Namespace)
			}
			source, path, ok := obj.SourcePath()
			if !ok || path == "" && source.Name == "" {
				continue
			}
			sourceRoot, _, external := loader.SourceView(d.cfg.Store, source, input.LocalPath)
			if external {
				continue
			}
			if root.seeds[o.Declaration] {
				root.nativeContext = true
			}
			target := manifest.NormalizeClaimBase(path)
			absolute := filepath.Join(sourceRoot, target)
			if !safepath.Contains(sourceRoot, absolute) {
				continue
			}
			count := 0
			art, _ := d.cfg.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact)
			loaded, err := d.loadSourcePath(ctx, absolute, art, &count)
			if err != nil {
				return nil, err
			}
			if !loaded {
				continue
			}
			key := coordinate{absolute, manifest.RenderNamespace(obj)}
			children, known := childrenByPath[key]
			if !known {
				children, err = d.loader.Occurrences(ctx, absolute, key.namespace)
				if err != nil {
					return nil, err
				}
				childrenByPath[key] = children
			}
			queue = append(queue, children...)
			local := sourceRoot == input.LocalPath
			if art, ok := d.cfg.Store.GetArtifact(source, store.ArtifactContent).(*store.SourceArtifact); ok {
				local = local || art.InputAlias && art.LocalRoot == input.LocalPath
			}
			if local && root.seeds[o.Declaration] && slices.ContainsFunc(children, func(child loader.Occurrence) bool { return child.Declaration == o.Declaration }) && !slices.Contains(root.Anchors, target) {
				root.Anchors = append(root.Anchors, target)
			}
		}
		if !slices.ContainsFunc(root.Occurrences, func(o loader.Occurrence) bool { return manifest.KindOf(o.Object).Runnable }) {
			continue
		}
		slices.Sort(root.Anchors)
		roots = append(roots, root)
	}
	var independent []entryRoot
	for i, root := range roots {
		owned := false
		for j, other := range roots {
			if i == j {
				continue
			}
			reaches := func(a, b entryRoot) bool {
				return slices.ContainsFunc(a.Occurrences, func(o loader.Occurrence) bool { return b.seeds[o.Declaration] })
			}
			if reaches(other, root) && (!reaches(root, other) || other.Path < root.Path) {
				owned = true
				break
			}
		}
		if !owned {
			independent = append(independent, root)
		}
	}
	return independent, nil
}
