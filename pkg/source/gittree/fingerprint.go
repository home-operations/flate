package gittree

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	merklefs "github.com/go-git/go-git/v5/utils/merkletrie/filesystem"
	"github.com/go-git/go-git/v5/utils/merkletrie/noder"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/sourceignore"
)

type contentFingerprint struct {
	files atomic.Pointer[map[string]string]
	types map[string]fs.FileMode
	once  sync.Once
	key   string
	err   error
}

// ContentFingerprint identifies selected bytes once per stable admitted view.
// Immutable views retain their acquisition identity; mutable identities remain separate.
func ContentFingerprint(ctx context.Context, input fs.FS) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	g, ok := input.(*FS)
	if !ok {
		return "", nil
	}
	for {
		content := g.content.Load()
		if content == nil {
			content = &contentFingerprint{}
			if !g.content.CompareAndSwap(nil, content) {
				continue
			}
		}
		run := false
		content.once.Do(func() {
			run = true
			content.initialize(ctx, g, false, nil)
		})
		if errors.Is(content.err, context.Canceled) || errors.Is(content.err, context.DeadlineExceeded) {
			g.content.CompareAndSwap(content, nil)
			if !run && ctx.Err() == nil {
				continue
			}
		}
		if err := ctx.Err(); err != nil {
			return "", err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
		}
		return content.key, content.err
	}
}

func (g *FS) identifyFiles(ctx context.Context, inventory fingerprintInventory, fresh bool) error {
	files := inventory.files
	var v *gitView
	subdir, origin := ".", "."
	nodes := map[string]noder.Noder{".": nil}
	prefix := "."
	root := ""
	g.fingerprintOrigin(&v, &subdir, &origin, &root)
	if !fresh && root != "" && g.location != nil && g.location.Repository != nil && (v == nil || v.identity == "") {
		idx, err := g.location.Repository.Storer.Index()
		if err != nil {
			return err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
		}
		prefix, err = filepath.Rel(g.location.RepositoryRoot, root)
		if err != nil {
			return err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
		}
		prefix = filepath.ToSlash(prefix)
		nodes["."] = merklefs.NewRootNodeWithOptions(
			osfs.New(g.location.RepositoryRoot, osfs.WithBoundOS()),
			nil,
			merklefs.Options{Index: idx},
		)
	}
	var lookup func(string) (noder.Noder, error)
	lookup = func(name string) (noder.Noder, error) {
		if node, known := nodes[name]; known {
			return node, nil
		}
		parent, err := lookup(path.Dir(name))
		if parent == nil || err != nil {
			return nil, err
		}
		children, err := parent.Children()
		if err != nil {
			return nil, err //nolint:wrapcheck // The filesystem view preserves upstream filesystem and cancellation error contracts.
		}
		for _, child := range children {
			nodes[path.Join(path.Dir(name), child.Name())] = child
		}
		return nodes[name], nil
	}
	selected := fileIdentity{git: v, origin: origin, prefix: prefix, lookup: lookup, types: inventory.types}
	for _, name := range slices.Sorted(maps.Keys(files)) {
		identity, err := selected.identify(ctx, g, name)
		switch {
		case fresh && errors.Is(err, fs.ErrNotExist):
			delete(files, name)
		case err != nil:
			return err
		default:
			files[name] = manifest.Fingerprint(identity)
		}
	}
	return nil
}

// FileDigests borrows completed selected-file evidence without reading content.
// Returned maps MUST remain read-only.
func (g *FS) FileDigests() map[string]string {
	if content := g.content.Load(); content != nil {
		if files := content.files.Load(); files != nil {
			return *files
		}
	}
	return nil
}

// PolicyEvidence returns the ordered ignore-file content used to shape the view.
// Policy evidence MUST NOT be used as immutable source acquisition authority.
func (g *FS) PolicyEvidence() any {
	if g.policy == nil {
		return nil
	}
	files := g.policy.Evidence()
	content := make([]struct {
		Path string
		Data []byte
	}, 0, len(files))
	for _, file := range files {
		content = append(content, struct {
			Path string
			Data []byte
		}{file.Path, []byte(file.Data)})
	}
	return content
}

// SourcePolicy returns the admission's captured Flux policy without reshaping its files.
func (g *FS) SourcePolicy(ctx context.Context) (*sourceignore.Matcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	if g.policy != nil {
		return g.policy, nil
	}
	return g.snapshot.load(g)
}

func (c *contentFingerprint) initialize(ctx context.Context, g *FS, fresh bool, requested []string) {
	policy, err := g.SourcePolicy(ctx)
	if err != nil {
		c.err = err
		return
	}
	v, native := g.view.(*gitView)
	inventory, err := g.fingerprintFiles(ctx, v, native, fresh || native && v.identity != "")

	c.err = err
	if c.err != nil {
		return
	}

	selectedPolicy := selectedSourcePolicy(policy, v, native, inventory.files)
	if requested != nil {
		selected := make(map[string]string, len(requested))
		for _, name := range requested {
			if _, known := inventory.files[name]; known {
				selected[name] = ""
			}
		}
		inventory.files = selected
	}
	if !selectedPolicy {
		sourceignore.Select(policy, inventory.files, inventory.ignores)
	}
	if c.err = g.identifyFiles(ctx, inventory, fresh); c.err != nil {
		return
	}
	if !fresh {
		if v, ok := g.view.(*gitView); ok && v.identity != "" {
			c.key = v.identity
			if g.excluded != "" {
				c.key = manifest.Fingerprint(struct{ Content, Excluded string }{v.identity, g.excluded})
			}
		} else {
			c.key = manifest.Fingerprint(inventory.files)
		}
	}
	files := inventory.files
	c.types = inventory.types
	c.files.Store(&files)
}

func (g *FS) fingerprintFiles(ctx context.Context, v *gitView, native, observe bool) (fingerprintInventory, error) {
	var err error

	files := map[string]string{}
	var types map[string]fs.FileMode
	if observe {
		types = map[string]fs.FileMode{}
	}
	var ignores []string
	visit := func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if observe {
			types[name] = entry.Type()
		}
		if err := ctx.Err(); err != nil || entry.IsDir() {
			return err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
		}
		if entry.Name() == ".sourceignore" {
			ignores = append(ignores, name)
		}
		files[name] = ""
		return nil
	}
	if native {
		files = make(map[string]string, len(v.nodes))
		for name := range v.nodes {
			if g.excludes(name) {
				continue
			}
			if err = visit(name, gitEntry{v, name}, nil); err != nil {
				break
			}
		}
	} else {
		err = fs.WalkDir(g, ".", visit)
	}
	//nolint:wrapcheck // Filesystem adapters preserve fs.PathError, I/O sentinel, and cancellation error contracts.
	return fingerprintInventory{files: files, ignores: ignores, types: types}, err
}

func selectedSourcePolicy(policy *sourceignore.Matcher, v *gitView, native bool, files map[string]string) bool {
	selectedPolicy := native && v.mode == Source
	for _, file := range policy.Evidence() {
		if _, admitted := files[file.Path]; !admitted {
			selectedPolicy = false
			break
		}
	}
	return selectedPolicy
}

func (g *FS) fingerprintOrigin(v **gitView, subdir, origin, root *string) {
	for input := g; input != nil; {
		switch current := input.view.(type) {
		case *localView:
			(*root) = filepath.Join(current.root, filepath.FromSlash((*subdir)))
			input = nil
		case *scopedView:
			(*subdir) = path.Join(current.path, (*subdir))
			input = current.parent
		case *gitView:
			if (*v) == nil && current.commit != plumbing.ZeroHash {
				(*v), (*origin) = current, (*subdir)
			}
			input, _ = current.current.(*FS)
		default:
			input = nil
		}
	}
}

type fileIdentity struct {
	types          map[string]fs.FileMode
	git            *gitView
	origin, prefix string
	lookup         func(string) (noder.Noder, error)
}

func (s *fileIdentity) identify(ctx context.Context, g *FS, name string) (plumbing.Hash, error) {
	if err := ctx.Err(); err != nil {
		return plumbing.ZeroHash, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	var identity plumbing.Hash
	member := path.Join(s.origin, name)
	if s.git != nil && (s.git.identity != "" || s.git.changes[member]) {
		identity = s.git.nodes[member].hash
	}
	if identity == plumbing.ZeroHash {
		node, err := s.lookup(path.Join(s.prefix, name))
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if node != nil {
			identity = plumbing.Hash(node.Hash()[:len(identity)])
		}
	}
	if identity != plumbing.ZeroHash {
		return identity, nil
	}
	var data []byte
	var err error
	mode, observed := s.types[name]
	if observed && mode.IsRegular() {
		file, openErr := g.OpenRegular(name)
		if openErr != nil {
			return plumbing.ZeroHash, openErr
		}
		data, err = io.ReadAll(file)
		err = errors.Join(err, file.Close())
	} else {
		data, err = g.ReadFile(name)
	}
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return plumbing.ComputeHash(plumbing.BlobObject, data), nil
}

type fingerprintInventory struct {
	types   map[string]fs.FileMode
	files   map[string]string
	ignores []string
}

// InputObservations borrows current file identities and node kinds from one source observation.
// Both maps MUST remain read-only.
type InputObservations struct {
	Files map[string]string
	Types map[string]fs.FileMode
}

// FreshInputs MUST observe mutable bytes and membership without prior operation memos or Git stat evidence.
// A nil request observes every file. An explicit request reads only those bodies while retaining complete node membership.
func (g *FS) FreshInputs(ctx context.Context, requested []string) (InputObservations, error) {
	if view, ok := g.view.(*gitView); ok && view.identity != "" {
		if _, err := ContentFingerprint(ctx, g); err != nil {
			return InputObservations{}, err
		}
		evidence := g.content.Load()
		files := g.FileDigests()
		if requested != nil {
			selected := make(map[string]string, len(requested))
			for _, name := range requested {
				if digest, known := files[name]; known {
					selected[name] = digest
				}
			}
			files = selected
		}
		return InputObservations{Files: files, Types: evidence.types}, nil
	}
	input, err := g.freshInput(ctx)
	if err != nil {
		return InputObservations{}, err
	}
	var evidence contentFingerprint
	evidence.initialize(ctx, input, true, requested)
	if evidence.err != nil {
		return InputObservations{}, evidence.err
	}
	return InputObservations{Files: *evidence.files.Load(), Types: evidence.types}, nil
}

func (g *FS) freshInput(ctx context.Context) (*FS, error) {
	switch v := g.view.(type) {
	case *localView:
		input := newLocal(v.root, true, &localPolicy{}, v.visibility)
		input.location = g.location
		input.excluded = g.excluded
		return input, nil
	case *scopedView:
		parent, err := v.parent.freshInput(ctx)
		if err != nil {
			return nil, err
		}
		input, err := Sub(parent, v.path)
		if err != nil {
			return nil, err
		}
		return input.(*FS), nil //nolint:errcheck // Sub preserves the concrete owner for a native parent.
	case *gitView:
		if v.refresh == nil {
			return g, nil
		}
		opts := *v.refresh
		opts.excluded = g.excluded
		opts.policy = nil
		if current, ok := opts.Current.(*FS); ok {
			var err error
			opts.Current, err = current.freshInput(ctx)
			if err != nil {
				return nil, err
			}
		}
		input, _, err := New(ctx, v.backing, v.commit, opts)
		return input, err
	default:
		return &FS{view: g.view, location: g.location, excluded: g.excluded, snapshot: &localPolicy{}}, nil
	}
}
