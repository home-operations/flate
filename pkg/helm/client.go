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

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// LocalSource couples a source CR (GitRepository / Bucket /
// ExternalArtifact — any fetched artifact that lives as a directory on
// disk) with the fetcher-produced SourceArtifact. The helm Client uses
// these to resolve charts whose sourceRef.kind points at one of those
// three kinds; the chart sits at `<Artifact.LocalPath>/<hr.Chart.Name>`
// in every case. Name+Namespace mirror the source CR so RepoFullName
// matches what hr.Chart.RepoFullName() produces.
type LocalSource struct {
	Name      string
	Namespace string
	Artifact  *store.SourceArtifact
}

// RepoFullName is the `<namespace>-<name>` lookup key. Matches
// manifest.HelmChart.RepoFullName so listing the helm client's
// local sources resolves cleanly against the HR's chartRef.
func (l LocalSource) RepoFullName() string {
	return l.Namespace + "-" + l.Name
}

// SecretGetter is the same shape as source.SecretGetter; aliased so
// the helm Client and the source Fetchers consume one canonical type.
// The orchestrator wires the same closure into both.
type SecretGetter = source.SecretGetter

// Client renders HelmReleases. Construct with NewClient.
type Client struct {
	tmpDir   string
	cacheDir string

	mu sync.RWMutex
	// resolver is the canonical source-lookup surface. When non-nil it
	// wins over the legacy Add*-driven maps. The orchestrator wires
	// NewStoreSourceResolver(store) at construction; embedders driving
	// helm.Client directly can either set their own resolver or stay on
	// the Add* API for back-compat.
	resolver SourceResolver
	// repos/ociRepos/localSources are the legacy push-registries. Kept
	// for tests / standalone embedders that haven't migrated to the
	// resolver. When the resolver is set, these maps are not consulted.
	repos        map[string]*manifest.HelmRepository
	ociRepos     map[string]*manifest.OCIRepository
	localSources map[string]LocalSource
	registry     *registry.Client
	secrets      SecretGetter

	// chartCache memoizes parsed *chart.Chart by on-disk path. Helm's
	// loader.Load reparses the entire tgz on every call — for repos
	// where many HelmReleases share a base chart (e.g. bjw-s
	// app-template referenced by 30+ HRs), the same chart was being
	// re-parsed once per HR. Cache by path; the upstream cache key is
	// already content-addressed (name-version-digest in the filename).
	//
	// chartLoadLocks serializes first-time loads per-path so N parallel
	// reconciles of the same chart issue exactly one loader.Load
	// (thundering-herd coalesce); the rest hit the populated cache.
	// Distinct paths still parse in parallel.
	chartMu        sync.Mutex
	chartCache     map[string]*chart.Chart
	chartLoadLocks *keylock.KeyMap[string]
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
		tmpDir:         tmpDir,
		cacheDir:       cacheDir,
		repos:          map[string]*manifest.HelmRepository{},
		ociRepos:       map[string]*manifest.OCIRepository{},
		localSources:   map[string]LocalSource{},
		registry:       reg,
		chartCache:     map[string]*chart.Chart{},
		chartLoadLocks: keylock.New[string](),
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

// SetSourceResolver installs the canonical lookup surface for
// HelmRepository / OCIRepository / local-artifact sources. When set,
// helm.Client reads through the resolver instead of consulting its
// own Add*-populated maps — eliminating the duplicate-state hazard
// where a source CR's spec change after registration could leave the
// helm.Client looking at stale credentials. Safe to call before any
// Add* / template call; typically once at orchestrator construction.
// Pass nil to revert to the legacy push-API behavior.
func (c *Client) SetSourceResolver(r SourceResolver) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resolver = r
}

// resolveHelmRepo returns the HelmRepository at <namespace>-<name>,
// reading from the resolver when present and falling back to the
// legacy Add*-populated map otherwise.
func (c *Client) resolveHelmRepo(hr *manifest.HelmRelease) *manifest.HelmRepository {
	c.mu.RLock()
	resolver := c.resolver
	c.mu.RUnlock()
	if resolver != nil {
		return resolver.HelmRepository(hr.Chart.RepoNamespace, hr.Chart.RepoName)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.repos[hr.Chart.RepoFullName()]
}

func (c *Client) resolveOCIRepo(hr *manifest.HelmRelease) *manifest.OCIRepository {
	c.mu.RLock()
	resolver := c.resolver
	c.mu.RUnlock()
	if resolver != nil {
		return resolver.OCIRepository(hr.Chart.RepoNamespace, hr.Chart.RepoName)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ociRepos[hr.Chart.RepoFullName()]
}

func (c *Client) resolveLocalSource(hr *manifest.HelmRelease) *store.SourceArtifact {
	c.mu.RLock()
	resolver := c.resolver
	c.mu.RUnlock()
	if resolver != nil {
		return resolver.LocalSourceArtifact(hr.Chart.RepoKind, hr.Chart.RepoNamespace, hr.Chart.RepoName)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ls, ok := c.localSources[hr.Chart.RepoFullName()]; ok {
		return ls.Artifact
	}
	return nil
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

// AddLocalSource registers a fetched-artifact source — GitRepository,
// Bucket, or ExternalArtifact — so charts referenced via the
// corresponding sourceRef.kind can be resolved on disk.
func (c *Client) AddLocalSource(s LocalSource) {
	if s.Name == "" || s.Artifact == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.localSources[s.RepoFullName()] = s
}

// LocateChart returns a filesystem path to the chart referenced by hr.
// The caller is responsible for cleanup (chart paths inside the cache
// are reused across calls; paths inside the tmp dir are not).
func (c *Client) LocateChart(ctx context.Context, hr *manifest.HelmRelease) (string, error) {
	if hr == nil {
		return "", errors.New("nil HelmRelease")
	}
	switch hr.Chart.RepoKind {
	case manifest.KindGitRepository, manifest.KindBucket, manifest.KindExternalArtifact:
		return c.locateLocalChart(hr)
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
	// Fast path: already parsed.
	c.chartMu.Lock()
	if ch, ok := c.chartCache[path]; ok {
		c.chartMu.Unlock()
		return ChartLoadResult{Path: path, Chart: ch}, nil
	}
	c.chartMu.Unlock()

	// Coalesce parallel first-loads of the same chart so N concurrent
	// reconciles of the same base chart (bjw-s app-template referenced
	// by 30+ HRs, podinfo across multiple test envs) issue exactly one
	// loader.Load instead of N. Distinct paths still parse in parallel.
	release, err := c.chartLoadLocks.Acquire(ctx, path)
	if err != nil {
		return ChartLoadResult{}, err
	}
	defer release()

	// Re-check under the per-path lock — another goroutine may have
	// populated the cache while we waited.
	c.chartMu.Lock()
	if ch, ok := c.chartCache[path]; ok {
		c.chartMu.Unlock()
		return ChartLoadResult{Path: path, Chart: ch}, nil
	}
	c.chartMu.Unlock()

	ch, err := loader.Load(path)
	if err != nil {
		// A truncated/corrupt chart tgz left on disk (process killed
		// mid-download, fs fault, manual delete-then-recreate) would
		// otherwise stay sticky-broken — `os.Stat`-based cache-hit
		// checks in LocateChart see the file, return its path, and we
		// re-error here on every subsequent run. Removing the file
		// lets the next reconcile re-pull cleanly.
		_ = os.Remove(path)
		return ChartLoadResult{}, fmt.Errorf("load chart %s: %w", path, err)
	}
	c.chartMu.Lock()
	c.chartCache[path] = ch
	c.chartMu.Unlock()
	return ChartLoadResult{Path: path, Chart: ch}, nil
}
