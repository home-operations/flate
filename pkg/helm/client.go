package helm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	chart "helm.sh/helm/v4/pkg/chart/v2"
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

// SecretGetter resolves a Secret CR by namespace + name. The helm
// Client uses this to look up HelmRepository.SecretRef credentials at
// pull time. Orchestrator wires it to Store.GetByName; nil is OK when
// no HelmRepositories use SecretRef.
type SecretGetter func(namespace, name string) *manifest.Secret

// Client renders HelmReleases. Construct with NewClient.
type Client struct {
	tmpDir   string
	cacheDir string

	mu       sync.RWMutex
	repos    map[string]*manifest.HelmRepository
	ociRepos map[string]*manifest.OCIRepository
	gitRepos map[string]LocalGitRepository
	registry *registry.Client
	secrets  SecretGetter

	// chartCache memoizes parsed *chart.Chart by on-disk path. Helm's
	// loader.Load reparses the entire tgz on every call — for repos
	// where many HelmReleases share a base chart (e.g. bjw-s
	// app-template referenced by 30+ HRs), the same chart was being
	// re-parsed once per HR. Cache by path; the upstream cache key is
	// already content-addressed (name-version-digest in the filename).
	chartMu    sync.Mutex
	chartCache map[string]*chart.Chart
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

// SetSecretGetter installs a Secret lookup function so HelmRepository
// SecretRef credentials can be resolved at pull time. Safe to call
// before any Add* — typically once at orchestrator construction.
func (c *Client) SetSecretGetter(g SecretGetter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.secrets = g
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
// Parsed *chart.Chart values are cached by path — Helm's loader.Load
// reparses the tgz (and recompiles values.schema.json) on every call,
// which is a significant render-time hot spot when many HelmReleases
// share a base chart (bjw-s app-template, podinfo, common-library, …).
// Path is content-addressed by Helm's own cacher (name-version-digest),
// so this is safe across reconciles.
func (c *Client) LoadChart(ctx context.Context, hr *manifest.HelmRelease) (ChartLoadResult, error) {
	path, err := c.LocateChart(ctx, hr)
	if err != nil {
		return ChartLoadResult{}, err
	}
	c.chartMu.Lock()
	if c.chartCache == nil {
		c.chartCache = make(map[string]*chart.Chart)
	}
	ch, ok := c.chartCache[path]
	c.chartMu.Unlock()
	if ok {
		return ChartLoadResult{Path: path, Chart: ch}, nil
	}
	ch, err = loader.Load(path)
	if err != nil {
		return ChartLoadResult{}, fmt.Errorf("load chart %s: %w", path, err)
	}
	c.chartMu.Lock()
	c.chartCache[path] = ch
	c.chartMu.Unlock()
	return ChartLoadResult{Path: path, Chart: ch}, nil
}
