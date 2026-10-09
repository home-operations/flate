package discovery

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func BenchmarkRunParentGates(b *testing.B) {
	root := b.TempDir()
	testutil.WriteGeneratedValuesCluster(b, root)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(b.Context(), Config{Path: root, RepoRoot: root, Store: store.New(), WipeSecrets: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRun_UnpinnedFollowedChain(b *testing.B) {
	root := b.TempDir()
	const depth, width = 8, 40
	testutil.WriteFile(b, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: top, namespace: flux-system}
spec: {path: ./level0, sourceRef: {kind: GitRepository, name: flux-system}}
`)
	for level := range depth {
		var resources strings.Builder
		for child := range width {
			name := fmt.Sprintf("ks%d-%d", level, child)
			fmt.Fprintf(&resources, "  - %s.yaml\n", name)
			testutil.WriteFile(b, root, fmt.Sprintf("level%d/%s.yaml", level, name), fmt.Sprintf(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: %s}
spec: {path: ./level%d, sourceRef: {kind: GitRepository, name: flux-system}}
`, name, level+1))
		}
		testutil.WriteFile(b, root, fmt.Sprintf("level%d/kustomization.yaml", level),
			"namespace: flux-system\nresources:\n"+resources.String())
	}
	testutil.WriteFile(b, root, fmt.Sprintf("level%d/kustomization.yaml", depth), "resources: []\n")
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(b.Context(), Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: store.New(), WipeSecrets: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRun_StandaloneSecretProducer(b *testing.B) {
	root := b.TempDir()
	testutil.WriteFile(b, root, "flux/ks.yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: owner, namespace: flux-system}
spec: {path: ./apps, sourceRef: {kind: GitRepository, name: flux-system}}
`)
	testutil.WriteFile(b, root, "apps/kustomization.yaml", "namespace: secure\nresources: [producer.yaml]\n")
	testutil.WriteFile(b, root, "apps/producer.yaml", `apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata: {name: producer, namespace: secure}
spec:
 target: {name: app-secret}
 data: [{secretKey: HOST, remoteRef: {key: fixture}}]
`)
	testutil.WriteFile(b, root, "secret.yaml", `apiVersion: v1
kind: Secret
metadata: {name: app-secret, namespace: secure}
stringData: {HOST: fixture-value}
`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Run(b.Context(), Config{Path: root, RepoRoot: root, Store: store.New(), WipeSecrets: true}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRun_HelmReleaseObserver(b *testing.B) {
	root := b.TempDir()
	testutil.WriteGeneratedValuesCluster(b, root)
	testutil.WriteFile(b, root, "inline-gate.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: flux-instance, namespace: flux-system}
spec:
  suspend: true
  chartRef: {kind: OCIRepository, name: fixture}
  values:
    instance:
      kustomize:
        patches:
          - patch: "DisableChartDigestTracking=true"
`)
	for _, observe := range []bool{false, true} {
		b.Run(fmt.Sprintf("observe_%t", observe), func(b *testing.B) {
			cfg := Config{Path: root, RepoRoot: root, WipeSecrets: true}
			var seen int
			if observe {
				cfg.OnHelmRelease = func(*manifest.HelmRelease) {
					seen++
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				seen = 0
				cfg.Store = store.New()
				if _, err := Run(b.Context(), cfg); err != nil {
					b.Fatal(err)
				}
			}
			if observe && seen == 0 {
				b.Fatal("no release observed")
			}
		})
	}
}
