package baseline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	objectcache "github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/memory"
	"github.com/go-git/go-git/v6/storage/transactional"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
)

const fullBaseRef plumbing.ReferenceName = "refs/flate/base-full"

type gitClient interface {
	Fetch(context.Context, storage.Storer, string, *git.FetchOptions) error
}

type baseRemote struct {
	client             gitClient
	url                string
	pin                plumbing.Hash
	ref                plumbing.ReferenceName
	full               bool
	noExact, noShallow bool
}

func fetchBase(ctx context.Context, url, rev string, cfg Config) (*resolution, error) {
	transportClient, err := sourcegit.NewClient(
		sourcegit.ClientConfig{Transports: cfg.GitTransports, Network: cfg.Network},
	)
	if err != nil {
		return nil, err
	}
	refs, err := transportClient.List(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("handshake: %w", err)
	}
	pin, err := remotePin(refs, rev)
	if err != nil {
		return nil, err
	}
	remote := &baseRemote{client: transportClient, pin: pin, url: url}
	for _, ref := range refs {
		matchesPin := ref.Type() == plumbing.HashReference && ref.Hash() == pin
		preferredName := remote.ref == "" || ref.Name() < remote.ref
		if matchesPin && !ref.Name().IsPeeled() && preferredName {
			remote.ref = ref.Name()
		}
	}
	key := sourcecache.GitKey{Content: struct{ URL, Commit string }{url, pin.String()}, Ref: pin.String()}
	generation, _, err := cfg.Cache.Git(
		ctx,
		key,
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		func(ctx context.Context, upper storage.Storer, _ billy.Filesystem) (plumbing.Hash, error) {
			if localErr := remote.fetch(ctx, upper, nil); localErr != nil {
				return plumbing.ZeroHash, localErr
			}
			if tag, localErr := object.GetTag(upper, pin); localErr == nil {
				commit, innerLocalErr := tag.Commit()
				if innerLocalErr != nil {
					return plumbing.ZeroHash, fmt.Errorf("commit: %w", innerLocalErr)
				}
				remote.pin = commit.Hash
			}
			return remote.pin, nil
		},
	)
	if err != nil {
		return nil, err
	}
	remote.pin = generation.Commit
	r := &resolution{Hash: generation.Commit, backing: generation.Backing, remote: remote}
	if err := r.adopt(nil); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *resolution) adopt(lower storage.Storer) error {
	upper := filesystem.NewStorageWithOptions(r.backing, objectcache.NewObjectLRU(0), filesystem.Options{})
	_, err := upper.Reference(fullBaseRef)
	switch {
	case err == nil:
		r.remote.full = true
	case !errors.Is(err, plumbing.ErrReferenceNotFound):
		return fmt.Errorf("reference: %w", err)
	}
	var storer storage.Storer = upper
	if lower != nil {
		storer = transactional.NewStorage(lower, upper)
	}
	r.Repo = &git.Repository{Storer: storer}
	return nil
}

func remotePin(refs []*plumbing.Reference, rev string) (plumbing.Hash, error) {
	if plumbing.IsHash(rev) {
		return plumbing.NewHash(rev), nil
	}
	names := []string{rev}
	if !strings.HasPrefix(rev, "refs/") {
		name := strings.TrimPrefix(rev, "origin/")
		names = []string{plumbing.NewBranchReferenceName(name).String(), plumbing.NewTagReferenceName(name).String()}
	}
	for _, name := range names {
		for _, ref := range refs {
			if ref.Name().String() == name {
				return ref.Hash(), nil
			}
		}
	}
	return plumbing.ZeroHash, fmt.Errorf("origin has no ref named %s", rev)
}

//nolint:gocognit // History recovery and capability retries MUST share one invocation budget.
func (r *baseRemote) fetch(ctx context.Context, upper storage.Storer, wants []plumbing.Hash) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	commitWant := len(wants) == 0 || len(wants) == 1 && wants[0] == r.pin
	options := &git.FetchOptions{Tags: git.NoTags, Force: true}
	if len(wants) == 0 {
		wants = []plumbing.Hash{r.pin}
		options.Depth = 1
		if !r.full {
			options.Filter = packp.FilterBlobNone()
		}
	}
	for _, hash := range wants {
		options.RefSpecs = append(options.RefSpecs, config.RefSpec("+"+hash.String()+":"+hash.String()))
	}
	if commitWant {
		r.pinFetch(options)
	}
	history := false
	for {
		if r.noShallow || history {
			options.Depth = 0
		}
		acquired, transferErr := r.fetchObjects(ctx, upper, options, history)
		if acquired {
			if errors.Is(transferErr, plumbing.ErrObjectNotFound) && options.Depth > 0 {
				history = true
				continue
			}
			return transferErr
		}
		if ctx.Err() != nil {
			return fmt.Errorf("context: %w", ctx.Err())
		}
		var err error
		commitWant, err = r.retryFetch(options, transferErr, commitWant)
		if err != nil {
			return err
		}
	}
}

func (r *baseRemote) fetchObjects(
	ctx context.Context,
	upper storage.Storer,
	options *git.FetchOptions,
	history bool,
) (bool, error) {
	target := upper
	if r.full || history {
		// Present metadata MUST not suppress acquisition of missing bodies or history.
		target = memory.NewStorage()
	}
	err := r.client.Fetch(ctx, target, r.url, options)
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		err = nil
	}
	if err != nil {
		return false, err
	}
	if r.full || !options.RefSpecs[0].IsExactSHA1() {
		if err := target.HasEncodedObject(r.pin); err != nil {
			return true, fmt.Errorf("base %s: %w", r.pin, err)
		}
	}
	if target != upper {
		if err := transactional.NewObjectStorage(upper, target).Commit(); err != nil {
			return true, fmt.Errorf("commit fetched objects: %w", err)
		}
		if err := transactional.NewShallowStorage(upper, target).Commit(); err != nil {
			return true, fmt.Errorf("commit fetched shallow: %w", err)
		}
	}
	if r.full {
		if err := upper.SetReference(plumbing.NewHashReference(fullBaseRef, r.pin)); err != nil {
			return true, fmt.Errorf("setreference: %w", err)
		}
	}
	return true, nil
}

func (r *baseRemote) retryFetch(options *git.FetchOptions, transferErr error, commitWant bool) (bool, error) {
	if errors.Is(transferErr, transport.ErrAuthenticationRequired) ||
		errors.Is(transferErr, transport.ErrAuthorizationFailed) {
		return commitWant, transferErr
	}
	switch {
	case (errors.Is(transferErr, transport.ErrFilterNotSupported) || errors.Is(transferErr, packfile.ErrPromisorPacksUnsupported)) && options.Filter != "":
		r.full, options.Filter = true, ""
		return commitWant, nil
	case errors.Is(transferErr, transport.ErrShallowNotSupported) && !r.noShallow:
		r.noShallow = true
		return commitWant, nil
	case errors.Is(transferErr, git.ErrExactSHA1NotSupported):
		if !commitWant && r.full {
			return commitWant, fmt.Errorf("base %s is missing objects after full fetch", r.pin)
		}
		if commitWant && r.noExact {
			return commitWant, transferErr
		}
		r.noExact = true
		if commitWant {
			r.pinFetch(options)
			return true, nil
		}
	default:
		if _, refusal := errors.AsType[*pktline.ErrorLine](transferErr); !refusal || r.full {
			return commitWant, transferErr
		}
	}
	r.full, options.Filter, options.Depth = true, "", 1
	r.pinFetch(options)
	return true, nil
}

func (r *baseRemote) pinFetch(options *git.FetchOptions) {
	options.RefSpecs = []config.RefSpec{config.RefSpec("+" + r.pin.String() + ":" + r.pin.String())}
	if !r.noExact {
		return
	}
	options.RefSpecs = []config.RefSpec{config.RefSpec("+" + r.ref.String() + ":refs/flate/base")}
	if r.ref != "" {
		return
	}
	options.RefSpecs = []config.RefSpec{"+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*"}
	options.Depth = 0
}

func copyMetadata(ctx context.Context, source, upper storage.Storer, hash plumbing.Hash) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	commit, err := source.EncodedObject(plumbing.CommitObject, hash)
	if err != nil {
		return fmt.Errorf("encodedobject: %w", err)
	}
	if _, err = upper.SetEncodedObject(commit); err != nil {
		return fmt.Errorf("setencodedobject: %w", err)
	}
	c, err := object.GetCommit(source, hash)
	if err != nil {
		return fmt.Errorf("getcommit: %w", err)
	}
	tree, err := c.Tree()
	if err != nil {
		return fmt.Errorf("tree: %w", err)
	}
	walker := object.NewTreeWalker(tree, true, nil)
	defer walker.Close()
	root, err := source.EncodedObject(plumbing.TreeObject, tree.Hash)
	if err != nil {
		return fmt.Errorf("encodedobject: %w", err)
	}
	if _, err = upper.SetEncodedObject(root); err != nil {
		return fmt.Errorf("setencodedobject: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context: %w", err)
		}
		_, entry, err := walker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("next: %w", err)
		}
		if entry.Mode != filemode.Dir {
			continue
		}
		obj, err := source.EncodedObject(plumbing.TreeObject, entry.Hash)
		if err != nil {
			return fmt.Errorf("encodedobject: %w", err)
		}
		if _, err = upper.SetEncodedObject(obj); err != nil {
			return fmt.Errorf("setencodedobject: %w", err)
		}
	}
	return nil
}

// originURL is the checkout's origin remote URL, when it has one.
func originURL(cfg *config.Config) (string, bool) {
	if cfg == nil {
		return "", false
	}
	remote, ok := cfg.Remotes[git.DefaultRemoteName]
	if !ok || len(remote.URLs) == 0 {
		return "", false
	}
	return remote.URLs[0], true
}
