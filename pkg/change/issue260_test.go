package change_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/orchestrator"
	"github.com/home-operations/flate/pkg/store"
)

// fakeOCIFetcher always returns a populated SourceArtifact so we can
// exercise the change-filter / source-controller interplay without
// hitting the network. The LocalPath points back at the working tree
// so kustomize render against the OCI artifact resolves to local
// files (the test fixture has an empty kustomization.yaml under the
// path; kustomize accepts an empty resources list).
type fakeOCIFetcher struct{ localPath string }

func (f fakeOCIFetcher) Fetch(_ context.Context, _ manifest.BaseManifest) (*store.SourceArtifact, error) {
	return &store.SourceArtifact{
		Kind:      manifest.KindOCIRepository,
		URL:       "oci://example.test/homepage-config",
		LocalPath: f.localPath,
		Revision:  "test",
	}, nil
}

// TestIssue260_OCIRepoFetchedWhenConsumerJoinsKeepSetAtRuntime
// reproduces https://github.com/home-operations/flate/issues/260.
//
// Setup:
//
//	cluster-apps Kustomization (spec.path: ./kubernetes/apps,
//	                            targetNamespace: default)
//	  ↓ renders + emits
//	homepage-config Kustomization (sourceRef = OCIRepository)
//	  ↑ sourceRef
//	OCIRepository (file-loaded under homepage tree)
//
// Diff-mode change: only an UNRELATED file changed. The keep set
// resolves at construction with neither homepage-config nor the
// OCIRepository in scope — both files sit under homepage's spec.path
// and homepage isn't directly affected by the change.
//
// At runtime, cluster-apps renders ./kubernetes/apps and emits
// homepage-config. The KS controller's keepEmitted calls
// Filter.AddEmitted(cluster-apps, homepage-config) and then
// AddObject(homepage-config). The child KS reconciles — and its
// sourceRef points at the OCIRepository that was PreGate-skipped
// at startup with no artifact.
//
// Bug: the runtime add was a one-shot add — it didn't walk
// transitiveDeps, so the OCIRepository never entered the keep set,
// the source controller never re-fetched it, and homepage-config's
// resolveSourceRoot surfaced "source ... artifact not found".
//
// Fix: addRecursive (called via AddEmitted) now walks transitiveDeps
// recursively AND the orchestrator wires Filter.OnAdd to
// Store.Refire so source kinds that newly join keep at runtime get
// their listener re-fired with the updated filter state.
func TestIssue260_OCIRepoFetchedWhenConsumerJoinsKeepSetAtRuntime(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "kubernetes/flux/cluster/ks.yaml"), clusterKS)
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/default/homepage/ks.yaml"), homepageKS)
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/default/homepage/repos/ocirepository.yaml"), ociRepo)
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/default/homepage/config/kustomization.yaml"), "resources: []\n")
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/default/homepage/app/kustomization.yaml"), "resources: []\n")
	mustWrite(t, filepath.Join(dir, "kubernetes/apps/networking/echo.yaml"), echoCM)

	o, err := orchestrator.New(orchestrator.Config{
		Path:        dir,
		WipeSecrets: true,
		EnableOCI:   true,
		ExternalChanges: change.NewSet([]string{
			"kubernetes/apps/networking/echo.yaml",
		}),
	})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	o.WithFetcher(manifest.KindOCIRepository, fakeOCIFetcher{localPath: dir})

	res, err := o.Render(context.Background())
	if err != nil {
		t.Logf("Render returned err: %v", err)
	}
	for id, info := range res.Failed {
		if strings.Contains(info.Message, "artifact not found") {
			t.Errorf("issue #260 regressed: %s reports %s", id, info.Message)
		}
	}
}

const clusterKS = `---
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: flux-system
  namespace: flux-system
spec:
  url: https://example.test/cluster.git
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: cluster-apps
  namespace: flux-system
spec:
  interval: 10m
  path: ./kubernetes/apps
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
  targetNamespace: default
`

const homepageKS = `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: homepage-config
spec:
  interval: 10m
  path: ./kubernetes/apps/default/homepage/config
  prune: true
  sourceRef:
    kind: OCIRepository
    name: homepage-config
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: homepage
spec:
  interval: 10m
  path: ./kubernetes/apps/default/homepage
  prune: true
  sourceRef:
    kind: GitRepository
    name: flux-system
    namespace: flux-system
`

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

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
