package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"
	"github.com/go-git/go-git/v6/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/store"
)

type includeRepository struct {
	repository *gogit.Repository
	calls      atomic.Int64
	fail       bool
}

//nolint:ireturn // The go-git transport requires the storage.Storer interface.
func (f *includeRepository) Load(_ *url.URL) (storage.Storer, error) {
	f.calls.Add(1)
	if f.fail {
		return nil, fs.ErrNotExist
	}
	return f.repository.Storer, nil
}

//nolint:paralleltest // RenderTrees changes process-global source policy.
func TestPlanTree_IncludeConstruction(t *testing.T) {
	for _, scenario := range []string{"success", "nested", "missing", "acquisition failure", "cycle", "long cycle", "cancellation", "sparse", "copied policy"} {
		t.Run(scenario, func(t *testing.T) { testPlanTreeIncludeConstruction(t, scenario) })
	}
}

type includeConstructionCase struct {
	root, remote, scenario, want, cluster string
	tree                                  Tree
	cfg                                   *Config
	backing                               *includeRepository
	beforeRoot, beforeRemote              map[string]includeNode
	successful                            bool
}

func testPlanTreeIncludeConstruction(t *testing.T, scenario string) {
	t.Helper()
	root, remote := t.TempDir(), t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	want := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: included\ndata:\n  value: remote\n"
	testutil.WriteFile(t, remote, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, remote, "app/cm.yaml", want)
	testutil.WriteFile(t, remote, "policy", "")
	repo := testutil.GitInit(t, remote)
	testutil.GitCommitAll(
		t,
		repo,
		"remote",
		&gogit.CommitOptions{
			Author: &object.Signature{Name: "test", Email: "test@example.invalid", When: time.Unix(0, 0)},
		},
	)
	backing := &includeRepository{repository: repo, fail: scenario == "acquisition failure"}
	server := file.NewTransport(file.Options{Loader: backing})
	owner := cachetest.New(t, "", cache.New)
	cfg := &Config{
		Cache:       owner,
		Network:     networktest.New(t),
		Concurrency: 2,
		missing:     depwait.NewMissingPolicy(nil),
		SourceRetry: cache.RetryConfig{Attempts: 1},
		GitTransports: sourcegit.Transports{
			"git": func(ctx context.Context, request sourcegit.TransportRequest) (sourcegit.Connection, error) {
				if scenario == "cancellation" {
					cancel()
					return nil, context.Canceled
				}
				return server.Connect(
					ctx,
					&transport.Request{
						URL:      request.URL,
						Command:  request.Command,
						Protocol: protocol.Version(request.Protocol),
					},
				)
			},
		},
	}
	inputName := "remote"
	if scenario == "missing" {
		inputName = "absent"
	}
	if scenario == "cycle" {
		inputName = "cluster"
	}
	cluster := fmt.Sprintf(
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata:\n  name: cluster\n  namespace: flux-system\nspec:\n  url: https://self.invalid/repo\n  interval: 1m\n  include:\n  - repository:\n      name: %s\n    fromPath: app\n    toPath: included\n",
		inputName,
	)
	if scenario == "sparse" {
		cluster += "  sparseCheckout:\n  - cluster.yaml\n  - entry.yaml\n  - kustomization.yaml\n  - remote.yaml\n"
	}
	if scenario == "copied policy" {
		cluster += "  - repository:\n      name: remote\n    fromPath: policy\n    toPath: .sourceignore\n"
		testutil.WriteFile(t, root, ".sourceignore", "reinclude.txt\n")
		testutil.WriteFile(t, root, "reinclude.txt", "admitted before filtering")
	}
	testutil.WriteFile(t, root, "cluster.yaml", cluster)
	remoteSource := "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata:\n  name: remote\n  namespace: flux-system\nspec:\n  url: git://remote.invalid/repo\n  interval: 1m\n"
	if scenario == "long cycle" {
		remoteSource += "  include:\n  - repository:\n      name: cluster\n    fromPath: app\n    toPath: cyclic\n"
	}
	if scenario == "nested" {
		remoteSource += "  include:\n  - repository:\n      name: leaf\n    fromPath: app/cm.yaml\n    toPath: nested.yaml\n"
		testutil.WriteFile(
			t,
			root,
			"leaf.yaml",
			"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata:\n  name: leaf\n  namespace: flux-system\nspec:\n  url: git://remote.invalid/repo\n  interval: 1m\n",
		)
	}
	testutil.WriteFile(t, root, "remote.yaml", remoteSource)
	testutil.WriteFile(
		t,
		root,
		"entry.yaml",
		testutil.KustomizationYAML(
			"entry",
			"flux-system",
			"  path: ./included\n  sourceRef:\n    kind: GitRepository\n    name: cluster\n",
		),
	)
	rootResources := "resources: [cluster.yaml, remote.yaml, entry.yaml]\n"
	if scenario == "nested" {
		rootResources = "resources: [cluster.yaml, remote.yaml, leaf.yaml, entry.yaml]\n"
	}
	testutil.WriteFile(t, root, "kustomization.yaml", rootResources)
	tree := Tree{
		ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
		Path:     root,
		RepoRoot: root,
		SelfURLs: []string{"https://self.invalid/repo"},
	}
	assert.NoError(t, tree.prepareInput(t.Context(), map[string]*store.SourceArtifact{}))
	beforeRoot, beforeRemote := includeSnapshot(t, root), includeSnapshot(t, remote)
	successful := scenario == "success" || scenario == "nested" || scenario == "sparse" ||
		scenario == "copied policy"
	count := 1
	if successful {
		count = 2
	}
	c := &includeConstructionCase{
		root: root, remote: remote, scenario: scenario, want: want, cluster: cluster,
		tree: tree, cfg: cfg, backing: backing, beforeRoot: beforeRoot, beforeRemote: beforeRemote,
		successful: successful,
	}
	for range count {
		testIncludeConstructionRun(ctx, t, c)
	}
}

func testIncludeConstructionRun(ctx context.Context, t *testing.T, c *includeConstructionCase) {
	t.Helper()
	runs, err := RenderTrees(ctx, []Tree{c.tree}, *c.cfg)
	for _, run := range runs {
		if run.Result != nil {
			t.Cleanup(run.Result.Close)
		}
	}

	if c.successful {
		assert.NoError(t, err)
		assert.FatalIf(t, len(runs) != 1 || runs[0].Orchestrator == nil, "entry did not render")
		assert.FatalIf(t, c.backing.calls.Load() == 0, "entry planned without remote acquisition")
		clusterID := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindGitRepository,
			Namespace: "flux-system",
			Name:      "cluster",
		}
		artifact, _ := runs[0].Store().GetArtifact(clusterID, store.ArtifactContent).(*store.SourceArtifact)
		assert.FatalIf(t, artifact == nil, "no completed including source")
		data, readErr := fs.ReadFile(artifact.FS, "included/cm.yaml")
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), c.want)
		if c.scenario == "copied policy" {
			data, readErr := fs.ReadFile(artifact.FS, "reinclude.txt")
			assert.NoError(t, readErr)
			assert.Equal(t, string(data), "admitted before filtering")
		}
	} else {
		assert.FatalIf(t, err == nil, "%s returned successful entry planning", c.scenario)
		if c.scenario == "cancellation" {
			assert.Equal(t, errors.Is(err, context.Canceled), true)
		} else {
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		}
	}
	assert.Diff(t, includeSnapshot(t, c.root), c.beforeRoot)
	assert.Diff(t, includeSnapshot(t, c.remote), c.beforeRemote)
	_, err = fs.Stat(c.tree.Input.FS, filepath.ToSlash("included/cm.yaml"))
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	data, err := fs.ReadFile(c.tree.Input.FS, "cluster.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), c.cluster)
}

type includeNode struct {
	Mode fs.FileMode
	Data string
}

func includeSnapshot(t *testing.T, root string) map[string]includeNode {
	t.Helper()
	result := map[string]includeNode{}
	err := fs.WalkDir(os.DirFS(root), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		node := includeNode{Mode: info.Mode()}
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
