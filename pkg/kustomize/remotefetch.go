package kustomize

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type remoteOperation uint8

const (
	remoteHTTP remoteOperation = iota
	remoteGit
)

type remoteInput struct {
	Operation remoteOperation `json:"op"`
	URL       string          `json:"url"`
	Ref       string          `json:"ref,omitempty"`
}

type remoteIdentity struct {
	HTTP     string `json:"http,omitempty"`
	Key      string `json:"key,omitempty"`
	Revision string `json:"revision,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

func (r remoteInput) acquire(ctx context.Context, c *TreeCache, input *treeFS) (fs.FS, remoteIdentity, error) {
	switch r.Operation {
	case remoteHTTP:
		acquired, err := c.remoteInputs.Acquire(ctx, r.URL, input.retry, input.slot)
		if err != nil {
			return nil, remoteIdentity{}, err
		}
		return acquired, remoteIdentity{HTTP: cache.AcquisitionIdentity(acquired)}, nil
	case remoteGit:
		if c.gitBase == nil {
			return nil, remoteIdentity{}, fmt.Errorf("%w: kustomization references remote git base %q but no git fetcher is wired", manifest.ErrInput, r.URL)
		}
		var artifact *store.SourceArtifact
		var err error
		fetch := func() {
			if err = ctx.Err(); err == nil {
				artifact, err = c.gitBase(ctx, GitBaseRequest{URL: r.URL, Ref: r.Ref, MaxAge: input.maxAge}, input.retry)
			}
		}
		if input.slot == nil {
			done := make(chan struct{})
			c.tasks.Go(context.WithoutCancel(ctx), "kustomize/git-base", func(context.Context) {
				defer close(done)
				fetch()
			})
			<-done
		} else {
			fetch()
		}
		if err != nil {
			return nil, remoteIdentity{}, err
		}
		if artifact == nil || artifact.FS == nil || artifact.Key == "" || (artifact.Revision == "" && artifact.Digest == "") {
			return nil, remoteIdentity{}, fmt.Errorf("%w: remote git base has no immutable content", manifest.ErrInput)
		}
		return artifact.FS, remoteIdentity{Key: artifact.Key, Revision: artifact.Revision, Digest: artifact.Digest}, nil
	default:
		return nil, remoteIdentity{}, fmt.Errorf("%w: unknown remote operation %d", manifest.ErrInput, r.Operation)
	}
}
