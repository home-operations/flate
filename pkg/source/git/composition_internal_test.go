package git

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	gogit "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestResolveLocal_CompositionCheckout(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "hidden.yaml", "pinned")
	testutil.WriteFile(t, root, ".sourceignore", "hidden.yaml\n")
	first := testutil.GitCommitAll(
		t,
		repo,
		"pinned",
		&gogit.CommitOptions{
			Author: &object.Signature{Name: "test", Email: "test@example.invalid", When: time.Unix(0, 0)},
		},
	)
	testutil.WriteFile(t, root, "hidden.yaml", "working")
	testutil.GitCommitAll(
		t,
		repo,
		"working",
		&gogit.CommitOptions{
			Author: &object.Signature{Name: "test", Email: "test@example.invalid", When: time.Unix(1, 0)},
		},
	)
	ignore := "hidden.yaml\n"
	source := &manifest.GitRepository{
		Name:           "source",
		Namespace:      "ns",
		Reference:      &sourcev1.GitRepositoryRef{Commit: first.String()},
		Ignore:         &ignore,
		SparseCheckout: []string{"hidden.yaml"},
		Include: []sourcev1.GitRepositoryInclude{
			{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}},
		},
	}
	before := gitCompositionSnapshot(t, root)
	artifacts, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{source},
		LocalOptions{},
	)
	assert.NoError(t, err)
	artifact := artifacts[source.Named()]
	assert.FatalIf(t, artifact == nil, "pinned checkout not acquired")
	data, err := fs.ReadFile(artifact.FS, "hidden.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "pinned")
	assert.Diff(t, gitCompositionSnapshot(t, root), before)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = ResolveLocal(canceled, localLocation(t, root), []*manifest.GitRepository{source}, LocalOptions{})
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Diff(t, gitCompositionSnapshot(t, root), before)
}

type gitCompositionNode struct {
	Mode fs.FileMode
	Data string
}

func gitCompositionSnapshot(t *testing.T, root string) map[string]gitCompositionNode {
	t.Helper()
	result := map[string]gitCompositionNode{}
	err := fs.WalkDir(os.DirFS(root), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		node := gitCompositionNode{Mode: info.Mode()}
		if info.Mode().IsRegular() {
			data, readErr := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
			if readErr != nil {
				return readErr
			}
			node.Data = string(data)
		}
		result[name] = node
		return nil
	})
	assert.NoError(t, err)
	return result
}

func TestFetcher_CompositionCheckout(t *testing.T) {
	t.Parallel()
	fixture := testfixture.Materialize(t, "remote-inputs", "git-source")
	server := file.NewTransport(file.Options{Loader: fixture})
	fetcher := &Fetcher{
		Network: networktest.New(t),
		Cache:   cachetest.New(t, "", sourcecache.New),
		GitTransports: Transports{"git": func(ctx context.Context, request TransportRequest) (Connection, error) {
			return server.Connect(
				ctx,
				&transport.Request{
					URL:      request.URL,
					Command:  request.Command,
					Protocol: protocol.Version(request.Protocol),
				},
			)
		}},
	}
	ignore := "app/cm.yaml\n"
	repository := &manifest.GitRepository{
		Name:      "remote",
		Namespace: "ns",
		URL:       "git://remote.fixture.invalid/source",
		Ignore:    &ignore,
		Include:   []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}}},
	}
	artifact, err := fetchtest.Fetch(
		t.Context(),
		fetcher.Fetch,
		repository,
		(testInputs{}).readSet(repository),
		&sourcecache.Policy{MaxAge: sourcecache.Forever},
		nil,
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(artifact.FS, "app/cm.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), fixture.Read(t, "remote/app/cm.yaml", ""))
}
