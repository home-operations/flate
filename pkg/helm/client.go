package helm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"helm.sh/helm/v4/pkg/chart/v2/loader"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// LocalGitRepository couples a GitRepository CRD with the cached working
// tree produced by SourceController. The helm Client uses these to
// resolve charts whose sourceRef.kind is GitRepository.
type LocalGitRepository struct {
	Repo     *manifest.GitRepository
	Artifact *store.SourceArtifact
}

// RepoName mirrors HelmRepository.RepoName for convenience in lookups.
func (l LocalGitRepository) RepoName() string {
	if l.Repo == nil {
		return ""
	}
	return l.Repo.RepoName()
}

// Client renders HelmReleases. Construct with NewClient.
type Client struct {
	tmpDir   string
	cacheDir string

	mu       sync.RWMutex
	repos    map[string]*manifest.HelmRepository
	ociRepos map[string]*manifest.OCIRepository
	gitRepos map[string]LocalGitRepository
	registry *registry.Client
}

// NewClient constructs a Client. tmpDir and cacheDir are used for
// scratch chart downloads. Both will be created if absent.
func NewClient(tmpDir, cacheDir string) (*Client, error) {
	tmpDir = cmp.Or(tmpDir, filepath.Join(os.TempDir(), "flate-helm"))
	cacheDir = cmp.Or(cacheDir, filepath.Join(os.TempDir(), "flate-helm-cache"))
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cacheDir, 0o750); err != nil {
		return nil, err
	}
	reg, err := registry.NewClient(registry.ClientOptCredentialsFile(""))
	if err != nil {
		return nil, fmt.Errorf("helm registry: %w", err)
	}
	return &Client{
		tmpDir:   tmpDir,
		cacheDir: cacheDir,
		repos:    map[string]*manifest.HelmRepository{},
		ociRepos: map[string]*manifest.OCIRepository{},
		gitRepos: map[string]LocalGitRepository{},
		registry: reg,
	}, nil
}

// AddRepo registers a HelmRepository so chart lookups can resolve it.
func (c *Client) AddRepo(repo *manifest.HelmRepository) {
	if repo == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.repos[repo.RepoName()] = repo
}

// AddOCIRepo registers an OCIRepository.
func (c *Client) AddOCIRepo(repo *manifest.OCIRepository) {
	if repo == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ociRepos[repo.RepoName()] = repo
}

// AddLocalGit registers a GitRepository / artifact pair so charts
// referenced via sourceRef.kind=GitRepository can be loaded.
func (c *Client) AddLocalGit(g LocalGitRepository) {
	if g.Repo == nil || g.Artifact == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gitRepos[g.RepoName()] = g
}

// LocateChart returns a filesystem path to the chart referenced by hr.
// The caller is responsible for cleanup (chart paths inside the cache
// are reused across calls; paths inside the tmp dir are not).
func (c *Client) LocateChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	if hr == nil {
		return "", errors.New("nil HelmRelease")
	}
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository:
		return c.locateGitChart(hr)
	case manifest.KindOCIRepository:
		return c.locateOCIChart(ctx, hr)
	case manifest.KindHelmRepository, "":
		return c.locateHelmRepoChart(ctx, hr)
	}
	return "", fmt.Errorf("%w: unsupported chart repo kind %s", manifest.ErrInput, hr.Chart.RepoKind)
}

// LoadChart resolves and loads the chart into helm's in-memory model.
func (c *Client) LoadChart(ctx context.Context, hr *manifest.HelmRelease) (ChartLoadResult, error) {
	path, err := c.LocateChart(ctx, hr)
	if err != nil {
		return ChartLoadResult{}, err
	}
	ch, err := loader.Load(path)
	if err != nil {
		return ChartLoadResult{}, fmt.Errorf("load chart %s: %w", path, err)
	}
	return ChartLoadResult{Path: path, Chart: ch}, nil
}
