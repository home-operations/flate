package gittree

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/tree"
)

func (g *gitView) shape(ctx context.Context, optsValue *ViewOptions, input *FS) error {
	opts := *optsValue
	mutable := g.identity == "" && (opts.Current != nil || opts.Worktree != nil)
	routed := g.commit != plumbing.ZeroHash && opts.Current != nil
	if mutable || routed {
		refresh := opts
		refresh.policy = input.snapshot
		refresh.Sparse, refresh.Changes = slices.Clone(opts.Sparse), slices.Clone(opts.Changes)
		g.refresh = &refresh
	}
	names := slices.Sorted(maps.Keys(g.nodes))
	matcher := input.policy
	var links *tree.Builder
	var err error
	switch opts.Mode {
	case Direct:
	case Source:
		opts.Sparse = NormalizeSparsePaths(opts.Sparse)
		if matcher == nil {
			matcher, err = input.SourcePolicy(ctx)
			if err == nil && opts.Ignore != "" {
				matcher = matcher.WithExtra(opts.Ignore)
			}
		}
		input.policy = matcher
	case RemoteBase:
		links, err = g.resolveLinks(names)
	}
	if err != nil {
		return err
	}
	kept, err := input.selectNodes(names, &opts, matcher, links)
	if err != nil {
		return err
	}
	maps.DeleteFunc(g.nodes, func(name string, _ *gitNode) bool { return !kept[name] })
	g.list()
	return nil
}

func directVisible(_ []string, _ string, mode fs.FileMode, _ *sourceignore.Matcher) bool {
	return mode&fs.ModeSymlink == 0
}

func sourceVisible(sparse []string, name string, mode fs.FileMode, matcher *sourceignore.Matcher) bool {
	return mode&fs.ModeSymlink == 0 &&
		(len(sparse) == 0 || slices.ContainsFunc(sparse, func(dir string) bool { return strings.HasPrefix(name, dir) })) &&
		!matcher.Match(filepath.FromSlash(name), false)
}

func visible(_ []string, name string, _ fs.FileMode, _ *sourceignore.Matcher) bool {
	for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
		if sourceignore.PruneDir(path.Base(parent), false) {
			return false
		}
	}
	return true
}

func (g *gitView) resolveLinks(names []string) (*tree.Builder, error) {
	raw := &gitView{nodes: g.nodes, backing: g.backing, current: g.current, changes: g.changes}
	raw.storers.New = g.storers.New
	links := tree.NewBuilder(raw)
	for _, name := range names {
		if g.nodes[name].mode&fs.ModeSymlink == 0 {
			continue
		}
		target, err := g.ReadLink(name)
		if err != nil {
			return nil, err
		}
		if err := links.RemoveAll(name); err != nil {
			return nil, err
		}
		if err := links.AddSymlink(name, target); err != nil {
			return nil, err
		}
	}
	return links, nil
}

func (g *FS) selectNodes(
	names []string,
	opts *ViewOptions,
	matcher *sourceignore.Matcher,
	links *tree.Builder,
) (map[string]bool, error) {
	v := g.view.(*gitView) //nolint:errcheck // Only native Git views select their own nodes.
	selectVisible := visible
	switch opts.Mode {
	case Direct:
		selectVisible = directVisible
	case Source:
		selectVisible = sourceVisible
	case RemoteBase:
	}
	kept := map[string]bool{".": true}
	for _, name := range names {
		node := v.nodes[name]
		if node.mode&fs.ModeSymlink != 0 && opts.Mode != RemoteBase {
			if v.denied == nil {
				v.denied = make(map[string]bool)
			}
			v.denied[name] = true
		}
		if g.excludes(name) || node.mode.IsDir() || !selectVisible(opts.Sparse, name, node.mode, matcher) {
			continue
		}
		selected, err := v.resolveSelectedNode(name, node, links)
		if err != nil {
			return nil, err
		}
		if !selected {
			continue
		}
		for parent := name; parent != "."; parent = path.Dir(parent) {
			kept[parent] = true
		}
	}
	return kept, nil
}

func (g *gitView) resolveSelectedNode(name string, node *gitNode, links *tree.Builder) (bool, error) {
	if node.mode&fs.ModeSymlink == 0 {
		return true, nil
	}
	resolved, err := links.Resolve(name)
	if errors.Is(err, tree.ErrEscape) || errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	target := g.nodes[resolved]
	if !target.mode.IsRegular() {
		return false, nil
	}
	g.nodes[name] = &gitNode{name: path.Base(name), hash: target.hash, mode: target.mode}
	return true, nil
}
