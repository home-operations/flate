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
		return nil, err
	}
	client, err := client.NewClient(ep)
	if err != nil {
		return nil, err
	}
	session, err := client.NewUploadPackSession(ep, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = session.Close() }()
	refs, err := session.AdvertisedReferencesContext(ctx)
	if err != nil {
		return nil, err
	}
	pin, err := remotePin(refs, rev)
	if err != nil {
		return nil, err
	}
	remote := &baseRemote{client: client, endpoint: ep, capabilities: refs.Capabilities, pin: pin, url: url}
	key := sourcecache.GitKey{Content: struct{ URL, Commit string }{url, pin.String()}, Ref: pin.String()}
	generation, _, err := owner.Git(ctx, key, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(ctx context.Context, upper storage.Storer, _ billy.Filesystem) (plumbing.Hash, error) {
		if err := remote.fetch(ctx, upper, nil); err != nil {
			return plumbing.ZeroHash, err
		}
		if tag, err := object.GetTag(upper, pin); err == nil {
			commit, err := tag.Commit()
			if err != nil {
				return plumbing.ZeroHash, err
			}
			remote.pin = commit.Hash
		}
		return remote.pin, nil
	})
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
	if _, err := upper.Reference(fullBaseRef); err == nil {
		r.remote.full = true
	} else if !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return err
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
		return err
	}
	req := r.newRequest(len(wants) == 0)
	if len(wants) == 0 {
		if !r.full && r.capabilities.Supports(capability.Filter) {
			if err := req.Capabilities.Set(capability.Filter); err != nil {
				return err
			}
			req.Filter = packp.FilterBlobNone()
		} else {
			r.full = true
		}
	} else if r.capabilities.Supports(capability.AllowReachableSHA1InWant) {
		req.Wants = wants
	} else if r.full {
		return fmt.Errorf("base %s is missing objects after full fetch", r.pin)
	} else {
		r.full = true
		req = r.newRequest(true)
	}
	for {
		session, err := r.client.NewUploadPackSession(r.endpoint, nil)
		if err != nil {
			return err
		}
		response, err := session.UploadPack(ctx, req)
		if err == nil {
			err = errors.Join(packfile.UpdateObjectStorage(upper, response), response.Close())
		}
		err = errors.Join(err, session.Close())
		if err == nil {
			if !r.full {
				return nil
			}
			return upper.SetReference(plumbing.NewHashReference(fullBaseRef, r.pin))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, transport.ErrAuthenticationRequired) || errors.Is(err, transport.ErrAuthorizationFailed) {
			return err
		}
		if _, refusal := errors.AsType[*pktline.ErrorLine](err); !refusal || r.full {
			return err
		}
		r.full = true
		req = r.newRequest(true)
	}
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

func copyMetadata(ctx context.Context, source storage.Storer, upper storage.Storer, hash plumbing.Hash) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	commit, err := source.EncodedObject(plumbing.CommitObject, hash)
	if err != nil {
		return err
	}
	if _, err = upper.SetEncodedObject(commit); err != nil {
		return err
	}
	c, err := object.GetCommit(source, hash)
	if err != nil {
		return err
	}
	tree, err := c.Tree()
	if err != nil {
		return err
	}
	walker := object.NewTreeWalker(tree, true, nil)
	defer walker.Close()
	root, err := source.EncodedObject(plumbing.TreeObject, tree.Hash)
	if err != nil {
		return err
	}
	if _, err = upper.SetEncodedObject(root); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, entry, err := walker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if entry.Mode != filemode.Dir {
			continue
		}
		obj, err := source.EncodedObject(plumbing.TreeObject, entry.Hash)
		if err != nil {
			return err
		}
		if _, err = upper.SetEncodedObject(obj); err != nil {
			return err
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
