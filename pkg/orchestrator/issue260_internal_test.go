package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	src "github.com/home-operations/flate/internal/source"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/testutil"

	"github.com/home-operations/flate/internal/change"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type fakeOCIFetcher struct{ localPath string }

func (f fakeOCIFetcher) Fetch(
	_ context.Context,
	_ manifest.BaseManifest,
	_ *store.ReadSet,
	_ *sourcecache.Policy,
) (src.PreparedFetch, error) {
	return src.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			if local != nil {
				return local, ctx.Err()
			}

			return &store.SourceArtifact{
				Kind:      manifest.KindOCIRepository,
				URL:       "oci://example.test/homepage-config",
				LocalPath: f.localPath,
				Revision:  "test",
				Key:       "test",
				FS:        os.DirFS(f.localPath),
			}, nil
		},
	}, nil
}

// A newly admitted emitted consumer MUST admit its source before publication.
//
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestIssue260_OCIRepoFetchedWhenConsumerJoinsKeepSetAtRuntime(t *testing.T) {
	clusterKS := "---\n" +
		`apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
` + "---\n" +
		testutil.KustomizationYAML("cluster-apps", "flux-system", `  interval: 10m
  path: ./kubernetes/apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
  targetNamespace: default
`)

	homepageKS := "---\n" +
		testutil.KustomizationYAML("homepage-config", "", `  interval: 10m
  path: ./kubernetes/apps/default/homepage/config
  prune: true
  sourceRef:
    kind: OCIRepository
    name: homepage-config
`) + "---\n" +
		testutil.KustomizationYAML("homepage", "", `  interval: 10m
  path: ./kubernetes/apps/default/homepage
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`)

	dir := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes", "flux", "cluster", "ks.yaml"), clusterKS)
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes", "apps", "default", "homepage", "ks.yaml"), homepageKS)
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "kubernetes", "apps", "default", "homepage", "repos", "ocirepository.yaml"),
		ociRepo,
	)
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "kubernetes", "apps", "default", "homepage", "config", "kustomization.yaml"),
		"resources: []\n",
	)
	testutil.WriteFileAt(
		t,
		filepath.Join(dir, "kubernetes", "apps", "default", "homepage", "app", "kustomization.yaml"),
		"resources: []\n",
	)
	testutil.WriteFileAt(t, filepath.Join(dir, "kubernetes", "apps", "networking", "echo.yaml"), echoCM)

	o, err := newTestOrchestrator(t, Tree{
		Path:            dir,
		ExternalChanges: change.NewSet([]string{"kubernetes/apps/networking/echo.yaml"}),
	}, Config{Concurrency: 2})
	assert.FatalIf(t, err != nil, "New: %v", err)
	o.src.Fetchers[manifest.KindOCIRepository] = fakeOCIFetcher{localPath: dir}

	res, err := o.render(t.Context(), t)
	if err != nil {
		t.Logf("Render returned err: %v", err)
	}
	for id, info := range res.Failed {
		assert.ErrorIf(t, strings.Contains(info.String(), "artifact not found"),
			"issue #260 regressed: %s reports %s", id, info.String())
	}
}

const ociRepo = `---
apiVersion: source.toolkit.fluxcd.io/v1beta2
kind: OCIRepository
metadata:
  name: homepage-config
spec:
  interval: 10m
  url: oci://example.test/homepage-config
  ref:
    tag: v1.0.0
`

const echoCM = `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: echo
data:
  msg: hello
`
