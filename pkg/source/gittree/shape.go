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

	"github.com/go-git/go-git/v6/plumbing"

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
	case Checkout:
		opts.Sparse = NormalizeSparsePaths(opts.Sparse)
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

func checkoutVisible(sparse []string, name string, _ fs.FileMode, _ *sourceignore.Matcher) bool {
	return len(sparse) == 0 ||
		slices.ContainsFunc(sparse, func(dir string) bool { return strings.HasPrefix(name, dir) })
}

func sourceVisible(match func(string, bool) bool) func([]string, string, fs.FileMode, *sourceignore.Matcher) bool {
	directories := map[string]bool{}
	matchDirectory := func(name string) bool {
		excluded, known := directories[name]
		if !known {
			excluded = match(filepath.FromSlash(name), true)
			directories[name] = excluded
		}
		return excluded
	}
	return func(sparse []string, name string, mode fs.FileMode, _ *sourceignore.Matcher) bool {
		if mode.IsDir() {
			for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
				if matchDirectory(parent) {
					return false
				}
			}
		}
		if mode&fs.ModeSymlink != 0 || !checkoutVisible(sparse, name, mode, nil) {
			return false
		}
		if mode.IsDir() {
			return !matchDirectory(name)
		}
		return !match(filepath.FromSlash(name), false)
	}
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
	raw.newStorer = g.newStorer
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
	case Checkout:
		selectVisible = checkoutVisible
	case Source:
		selectVisible = sourceVisible(matcher.Match)
	case RemoteBase:
	}
	kept := map[string]bool{".": true}
	for _, name := range names {
		node := v.nodes[name]
		if node.mode.IsDir() && opts.Mode != Checkout && opts.Mode != Source {
			continue
		}
		selected := !g.excludes(name) && selectVisible(opts.Sparse, name, node.mode, matcher)
		var err error
		if opts.Mode != Checkout {
			selected, err = v.resolveSelectedNode(name, node, links, selected)
		}
		if err != nil {
			return nil, err
		}
		if !selected {
			continue
		}
		for parent := name; parent != "." && !kept[parent]; parent = path.Dir(parent) {
			kept[parent] = true
		}
	}
	return kept, nil
}

func (g *gitView) resolveSelectedNode(name string, node *gitNode, links *tree.Builder, selected bool) (bool, error) {
	if node.mode&fs.ModeSymlink == 0 {
		return selected, nil
	}
	if links == nil {
		if g.denied == nil {
			g.denied = make(map[string]bool)
		}
		g.denied[name] = true
		return false, nil
	}
	if !selected {
		return false, nil
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
