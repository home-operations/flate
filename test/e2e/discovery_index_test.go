package e2e

import (
	"github.com/home-operations/flate/internal/testutil"
	"strings"
	"testing"
)

func TestE2E_DiscoveryIndex_ScopedRawFileFallback(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}}
`)
			testutil.WriteFile(t, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings}]}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: raw, namespace: flux-system}
spec: {interval: 10m, path: ./shared, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: nsprod, namespace: flux-system}
spec: {interval: 10m, path: ./overlay, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: other, namespace: other}
spec:
  interval: 10m
  path: ./shared
  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
  patches:
    - target: {kind: ConfigMap}
      patch: |-
        - op: replace
          path: /does/not/exist
          value: x
`)
			testutil.WriteFile(t, root, "shared/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - settings.yaml
`)
			testutil.WriteFile(t, root, "shared/settings.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: settings}
data: {K: v}
`)
			testutil.WriteFile(t, root, "overlay/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: flux-system
resources:
  - ../shared
`)
			testutil.WriteFile(t, root, "consumer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`)
			testutil.WriteFile(t, root, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: out, namespace: default}
data: {v: "${K}"}
`)
			_, stderr, code := runCLIBuffers("build", "ks", "consumer", "--path", root, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if code != 1 || !strings.Contains(stderr, "other") {
				t.Fatalf("exit=%d stderr=%s", code, stderr)
			}
		})
	}
}

func TestE2E_DiscoveryIndex_ChangedOnlyFileBackedProducers(t *testing.T) {
	for _, workers := range []string{"2", "4"} {
		t.Run(workers, func(t *testing.T) {
			root := t.TempDir()
			orig := t.TempDir()
			for _, dir := range []string{root, orig} {
				testutil.WriteFile(t, dir, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: flux-system, namespace: flux-system}
spec: {url: "https://example.test/cluster.git", ref: {branch: main}}
`)
				testutil.WriteFile(t, dir, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: consumer, namespace: flux-system}
spec:
  interval: 10m
  path: ./consumer
  sourceRef: {kind: GitRepository, name: flux-system}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: othercm}]}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: producer, namespace: flux-system}
spec: {interval: 10m, path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}
`)
				testutil.WriteFile(t, dir, "apps/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: flux-system
resources:
  - other
`)
				testutil.WriteFile(t, dir, "apps/other/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`)
				testutil.WriteFile(t, dir, "apps/other/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: othercm, namespace: default}
data: {K: v}
`)
				testutil.WriteFile(t, dir, "consumer/kustomization.yaml", `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - cm.yaml
`)
				testutil.WriteFile(t, dir, "consumer/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: out, namespace: default}
data: {v: "${K}"}
`)
			}
			testutil.WriteFile(t, root, "consumer/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: out2, namespace: default}\ndata: {v: \"${K}\"}\n")
			_, stderr, code := runCLIBuffers("build", "all", "--path", root, "--path-orig", orig, "--cache-dir", t.TempDir(), "--concurrency", workers, "--no-progress", "--log-level", "warn")
			if code != 1 || !strings.Contains(stderr, "othercm") {
				t.Fatalf("exit=%d stderr=%s", code, stderr)
			}
		})
	}
}
