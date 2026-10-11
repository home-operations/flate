package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_SubstitutionProducerOrdering(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		for _, changedOnly := range []bool{false, true} {
			for _, producerFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/changed_%t/producer_first_%t", kind, changedOnly, producerFirst), func(t *testing.T) {
					root := t.TempDir()
					testutil.WriteSubstitutionProducerCluster(t, root, kind)
					cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2}
					if changedOnly {
						cfg.ExternalChanges = change.NewSet([]string{"app/cm.yaml"})
					}
					o, err := New(cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(o.Stop)
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					if err := o.Bootstrap(ctx); err != nil {
						t.Fatal(err)
					}
					o.configureControllers()
					d := dagDispatcher{o}
					producer, consumer := ksID("producer"), ksID("consumer")
					release := make(chan struct{})
					o.tasks.Go(ctx, "producer", func(ctx context.Context) {
						select {
						case <-release:
							d.Dispatch(ctx, producer, manifest.DrainNone)
						case <-ctx.Done():
						}
					})
					if producerFirst {
						close(release)
						o.tasks.BlockTillDone()
					}
					attempt := make(chan []manifest.NamedResource, 1)
					o.tasks.Go(ctx, "consumer", func(ctx context.Context) {
						_, blocked, _ := d.Dispatch(ctx, consumer, manifest.DrainNone)
						attempt <- blocked
					})
					var blocked []manifest.NamedResource
					select {
					case blocked = <-attempt:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if !producerFirst {
						close(release)
					}
					o.tasks.BlockTillDone()
					if slices.Contains(blocked, producer) == producerFirst {
						t.Fatalf("consumer blockers = %v, producer first = %t", blocked, producerFirst)
					}
					o.tasks.Go(ctx, "consumer", func(ctx context.Context) { d.Dispatch(ctx, consumer, manifest.DrainNone) })
					o.tasks.BlockTillDone()
					output, _ := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "output"})
					if output == nil || output.Data["version"] != "4.6.2" {
						t.Fatalf("consumer output = %+v, want rendered version 4.6.2", output)
					}
				})
			}
		}
	}
}

func TestRender_SubstitutionProducerFailures(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteSubstitutionProducerCluster(t, root, kind)
			testutil.WriteFile(t, root, "data/version.env", "VERSION=4.6.2\n")
			testutil.WriteFile(t, root, "data/kustomization.yaml", "namespace: flux-system\nresources: [missing.yaml]\ngeneratorOptions: {disableNameSuffixHash: true}\n"+map[string]string{manifest.KindConfigMap: "configMapGenerator", manifest.KindSecret: "secretGenerator"}[kind]+":\n- name: settings\n  envs: [version.env]\n")
			o := newSubstitutionOrchestrator(t, Config{Path: root})
			res, err := o.Render(t.Context())
			consumer, producer := ksID("consumer"), ksID("producer")
			if err == nil || !slices.Contains(res.Blocked[consumer], producer) || len(res.Manifests[consumer]) != 0 {
				t.Fatalf("failed producer did not block consumer: error=%v blocked=%v manifests=%v", err, res.Blocked, res.Manifests[consumer])
			}
		})
	}
}
