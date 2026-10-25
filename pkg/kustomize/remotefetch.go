package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/home-operations/flate/internal/fetcherr"
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
	Operation  remoteOperation `json:"op"`
	URL        string          `json:"url"`
	Ref        string          `json:"ref,omitempty"`
	Timeout    time.Duration   `json:"timeout,omitempty"`
	Submodules bool            `json:"submodules,omitempty"`
}

type remoteIdentity struct {
	store.ArtifactInput
	HTTPStatus int `json:"httpStatus,omitempty"`
}

func (r remoteInput) acquire(ctx context.Context, c *TreeCache, input *treeFS) (fs.FS, remoteIdentity, error) {
	switch r.Operation {
	case remoteHTTP:
		acquired, err := c.remoteInputs.Acquire(ctx, r.URL, input.retry, input.slot)
		if err != nil {
			return nil, httpFailureIdentity(err), err
		}
		return acquired, remoteIdentity{ArtifactInput: (&store.SourceArtifact{FS: acquired, Content: cache.AcquisitionIdentity(acquired)}).Input(store.ArtifactContent)}, nil
	case remoteGit:
		if c.gitBase == nil {
			return nil, remoteIdentity{}, fmt.Errorf("%w: kustomization references remote git base %q but no git fetcher is wired", manifest.ErrInput, r.URL)
		}
		var artifact *store.SourceArtifact
		var err error
		fetch := func() {
			if err = ctx.Err(); err == nil {
				artifact, err = c.gitBase(ctx, GitBaseRequest{URL: r.URL, Ref: r.Ref, MaxAge: input.maxAge, Timeout: r.Timeout, Submodules: r.Submodules}, input.retry)
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
		if artifact == nil || artifact.FS == nil || artifact.Content == "" {
			return nil, remoteIdentity{}, fmt.Errorf("%w: remote git base has no immutable content", manifest.ErrInput)
		}
		return artifact.FS, remoteIdentity{ArtifactInput: artifact.Input(store.ArtifactContent)}, nil
	default:
		return nil, remoteIdentity{}, fmt.Errorf("%w: unknown remote operation %d", manifest.ErrInput, r.Operation)
	}
}

func httpFailureIdentity(err error) remoteIdentity {
	response, ok := errors.AsType[*fetcherr.ResponseError](err)
	if !ok || response.StatusCode != http.StatusNotFound {
		return remoteIdentity{}
	}
	if _, ok := errors.AsType[*cache.CacheError](err); ok {
		return remoteIdentity{}
	}
	var stable func(error) bool
	stable = func(err error) bool {
		switch wrapped := err.(type) { //nolint:errorlint // Each node must be checked without matching an independent joined sibling.
		case *fetcherr.ResponseError:
			return wrapped.StatusCode == http.StatusNotFound && (wrapped.Err == nil || stable(wrapped.Err))
		case interface{ Unwrap() []error }:
			children := wrapped.Unwrap()
			for _, child := range children {
				if !stable(child) {
					return false
				}
			}
			return len(children) > 0
		case interface{ Unwrap() error }:
			return stable(wrapped.Unwrap())
		default:
			return err == manifest.ErrFlux //nolint:errorlint // Only this concrete domain leaf is allowed in a stable response tree.
		}
	}
	if stable(err) {
		return remoteIdentity{HTTPStatus: http.StatusNotFound}
	}
	return remoteIdentity{}
}
