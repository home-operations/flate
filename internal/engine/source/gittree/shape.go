package gittree

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"

	"github.com/home-operations/flate/internal/engine/source/sourceignore"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/pkg/manifest"
)

// DefaultSourcePolicyKey lets discovery reuse a view without reapplying the default exclusions.
const DefaultSourcePolicyKey = "default"

func (g *gitView) shape(ctx context.Context, optsValue *ViewOptions, input *FS) error {
	opts := *optsValue
	mutable := g.identity == "" && (opts.Current != nil || opts.Worktree != nil)
	routed := g.commit != plumbing.ZeroHash && opts.Current != nil
	if mutable || routed {
		refresh := opts
		refresh.CurrentObserved = false
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
		matcher, err = input.sourcePolicy(ctx, opts.Ignore, true)
		input.policy = matcher
		if err == nil && len(opts.Sparse) == 0 && matcher.UsesDefaults() {
			input.policyKey = DefaultSourcePolicyKey
		} else if err == nil {
			input.policyKey = manifest.Fingerprint(struct {
				Ignore string
				Sparse []string
				Files  any
			}{opts.Ignore, opts.Sparse, input.PolicyEvidence()})
		}
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
	g.bindMembership(input, kept)
	return nil
}

func (g *gitView) bindMembership(input *FS, kept map[string]bool) {
	if len(kept) == len(g.nodes) && !g.pruned && input.sameFiles != nil {
		if _, local := input.sameFiles.view.(*localView); local {
			return
		}
	} else {
		input.sameFiles = nil
	}
	maps.DeleteFunc(g.nodes, func(name string, _ *gitNode) bool { return !kept[name] })
	g.list()
}

func checkoutVisible(sparse []string, name string) bool {
	return len(sparse) == 0 ||
		slices.ContainsFunc(sparse, func(dir string) bool { return strings.HasPrefix(name, dir) })
}

func (g *gitView) resolveLinks(names []string) (*tree.Builder, error) {
	raw := &gitView{nodes: g.nodes, backing: g.backing, current: g.current, worktree: g.worktree, changes: g.changes}
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
	v := g.view.(*gitView) //nolint:errcheck // finishGitView selects nodes only after New constructs *gitView; corruption MUST panic.
	kept := map[string]bool{".": true}
	for _, name := range names {
		node := v.nodes[name]
		if node.mode.IsDir() && opts.Mode != Checkout && opts.Mode != Source {
			continue
		}
		selected := !g.excludes(name) && checkoutVisible(opts.Sparse, name)
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
		matcher.Admit(kept, name, node.mode.IsDir())
	}
	return kept, nil
}

func (g *gitView) missingInfo(name string) (fs.FileInfo, error) {
	var err error
	for parent := name; len(g.denied) > 0 && parent != "."; parent = path.Dir(parent) {
		if g.denied[parent] {
			err = errRejectedLink
			break
		}
	}
	indexed := err != nil
	if !indexed && g.backing == nil && g.current != nil {
		_, err = fs.Lstat(g.current, name)
	}
	switch {
	case err == nil, errors.Is(err, fs.ErrNotExist), g.mode == Source && errors.Is(err, errRejectedLink):
		err = fs.ErrNotExist
	case !indexed:
		return nil, err
	}
	return nil, &fs.PathError{Op: statOperation, Path: name, Err: err}
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

func (g *FS) sourcePolicy(ctx context.Context, ignore string, prepared bool) (*sourceignore.Matcher, error) {
	matcher := g.policy
	if matcher == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		matcher, err = g.snapshot.load(g)
		if err != nil {
			return nil, err
		}
	} else if prepared {
		return matcher, nil
	}
	if ignore != "" {
		matcher = matcher.WithExtra(ignore)
	}
	return matcher, nil
}
