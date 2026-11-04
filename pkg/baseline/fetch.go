package baseline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	objectcache "github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/storage/transactional"

	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

const fullBaseRef plumbing.ReferenceName = "refs/flate/base-full"

type baseRemote struct {
	client       transport.Transport
	endpoint     *transport.Endpoint
	capabilities *capability.List
	url          string
	pin          plumbing.Hash
	full         bool
}

func fetchBase(ctx context.Context, url, rev string, owner *sourcecache.Cache) (*resolution, error) {
	ep, err := transport.NewEndpoint(url)
	if err != nil {
		return nil, fmt.Errorf("newendpoint: %w", err)
	}
	transportClient, err := client.NewClient(ep)
	if err != nil {
		return nil, fmt.Errorf("newclient: %w", err)
	}
	session, err := transportClient.NewUploadPackSession(ep, nil)
	if err != nil {
		return nil, fmt.Errorf("newuploadpacksession: %w", err)
	}
	defer func() { _ = session.Close() }()
	refs, err := session.AdvertisedReferencesContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("advertisedreferencescontext: %w", err)
	}
	pin, err := remotePin(refs, rev)
	if err != nil {
		return nil, err
	}
	remote := &baseRemote{client: transportClient, endpoint: ep, capabilities: refs.Capabilities, pin: pin, url: url}
	key := sourcecache.GitKey{Content: struct{ URL, Commit string }{url, pin.String()}, Ref: pin.String()}
	generation, _, err := owner.Git(
		ctx,
		key,
		sourcecache.Policy{MaxAge: sourcecache.Forever},
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

func remotePin(refs *packp.AdvRefs, rev string) (plumbing.Hash, error) {
	if plumbing.IsHash(rev) {
		return plumbing.NewHash(rev), nil
	}
	names := []string{rev}
	if !strings.HasPrefix(rev, "refs/") {
		name := strings.TrimPrefix(rev, "origin/")
		names = []string{plumbing.NewBranchReferenceName(name).String(), plumbing.NewTagReferenceName(name).String()}
	}
	for _, name := range names {
		if pin, ok := refs.References[name]; ok {
			return pin, nil
		}
	}
	return plumbing.ZeroHash, fmt.Errorf("origin has no ref named %s", rev)
}

// fetch requests only missing body wants after the metadata phase. Unsupported
// partial capabilities permit one full fetch of the same resolved commit.
func (r *baseRemote) fetch(ctx context.Context, upper storage.Storer, wants []plumbing.Hash) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context: %w", err)
	}
	req, err := r.prepareRequest(wants)
	if err != nil {
		return err
	}
	for {
		transferErr := r.uploadPack(ctx, upper, req)
		if transferErr == nil {
			if !r.full {
				return nil
			}
			if err := upper.SetReference(plumbing.NewHashReference(fullBaseRef, r.pin)); err != nil {
				return fmt.Errorf("setreference: %w", err)
			}
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("context: %w", ctx.Err())
		}
		if errors.Is(transferErr, transport.ErrAuthenticationRequired) ||
			errors.Is(transferErr, transport.ErrAuthorizationFailed) {
			return transferErr
		}
		if _, refusal := errors.AsType[*pktline.ErrorLine](transferErr); !refusal || r.full {
			return transferErr
		}
		r.full = true
		req = r.newRequest(true)
	}
}

func (r *baseRemote) uploadPack(ctx context.Context, upper storage.Storer, req *packp.UploadPackRequest) error {
	session, err := r.client.NewUploadPackSession(r.endpoint, nil)
	if err != nil {
		return fmt.Errorf("newuploadpacksession: %w", err)
	}
	response, err := session.UploadPack(ctx, req)
	if err == nil {
		err = errors.Join(packfile.UpdateObjectStorage(upper, response), response.Close())
	}
	err = errors.Join(err, session.Close())
	return err
}

func (r *baseRemote) prepareRequest(wants []plumbing.Hash) (*packp.UploadPackRequest, error) {
	req := r.newRequest(len(wants) == 0)
	switch {
	case len(wants) == 0:
		if !r.full && r.capabilities.Supports(capability.Filter) {
			if err := req.Capabilities.Set(capability.Filter); err != nil {
				return nil, fmt.Errorf("set: %w", err)
			}
			req.Filter = packp.FilterBlobNone()
		} else {
			r.full = true
		}
	case r.capabilities.Supports(capability.AllowReachableSHA1InWant):
		req.Wants = wants
	case r.full:
		return nil, fmt.Errorf("base %s is missing objects after full fetch", r.pin)
	default:
		r.full = true
		req = r.newRequest(true)
	}
	return req, nil
}

func (r *baseRemote) newRequest(wantCommit bool) *packp.UploadPackRequest {
	req := packp.NewUploadPackRequestFromCapabilities(r.capabilities)
	req.Capabilities.Delete(capability.Sideband64k)
	req.Capabilities.Delete(capability.Sideband)
	if wantCommit {
		req.Wants = []plumbing.Hash{r.pin}
		if r.capabilities.Supports(capability.Shallow) {
			req.Depth = packp.DepthCommits(1)
			_ = req.Capabilities.Set(capability.Shallow)
		}
	}
	return req
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
