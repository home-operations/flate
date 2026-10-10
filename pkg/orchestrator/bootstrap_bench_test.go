package orchestrator

import (
	"fmt"
	"github.com/home-operations/flate/internal/testutil"
	"testing"
)

func BenchmarkBootstrap_ChartDigestTracking(b *testing.B) {
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
	for _, auto := range []bool{false, true} {
		b.Run(fmt.Sprintf("auto_%t", auto), func(b *testing.B) {
			cfg := Config{Path: root, RepoRoot: root, CacheDir: b.TempDir(), Concurrency: 2}
			if !auto {
				cfg.DisableChartDigestTracking = new(false)
			}
			b.ReportAllocs()
			for b.Loop() {
				o, err := New(cfg)
				if err != nil {
					b.Fatal(err)
				}
				err = o.Bootstrap(b.Context())
				o.Stop()
				if err != nil {
					b.Fatal(err)
				}
				if o.hrc.Options.DisableChartDigestTracking != auto {
					b.Fatal("incorrect detection result")
				}
			}
		})
	}
}
