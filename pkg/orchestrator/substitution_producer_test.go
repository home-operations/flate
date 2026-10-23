package orchestrator

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
					o, err := New(t.Context(), cfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := o.Stop(); err != nil {
							t.Errorf("Stop: %v", err)
						}
					})
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					if err := o.Bootstrap(ctx); err != nil {
						t.Fatal(err)
					}
					o.configureControllers()
					d := dagDispatcher{o}
					producer, consumer := testutil.KustomizationID("flux-system", "producer"), testutil.KustomizationID("flux-system", "consumer")
					release := make(chan struct{})
					o.tasks.Go(ctx, "producer", func(ctx context.Context) {
						select {
						case <-release:
							d.Dispatch(ctx, producer, manifest.ReconcileAttempt{Drain: manifest.DrainNone})
						case <-ctx.Done():
						}
					})
					if producerFirst {
						close(release)
						o.tasks.BlockTillDone()
					}
					attempt := make(chan []manifest.NamedResource, 1)
					o.tasks.Go(ctx, "consumer", func(ctx context.Context) {
						_, outWait := d.Dispatch(ctx, consumer, manifest.ReconcileAttempt{Drain: manifest.DrainNone})
						blocked := outWait
						attempt <- blocked.Deps
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
					o.tasks.Go(ctx, "consumer", func(ctx context.Context) {
						d.Dispatch(ctx, consumer, manifest.ReconcileAttempt{Drain: manifest.DrainNone})
					})
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
			consumer, producer := testutil.KustomizationID("flux-system", "consumer"), testutil.KustomizationID("flux-system", "producer")
			if err == nil || !slices.Contains(res.Blocked[consumer], producer) || len(res.Manifests[consumer]) != 0 {
				t.Fatalf("failed producer did not block consumer: error=%v blocked=%v manifests=%v", err, res.Blocked, res.Manifests[consumer])
			}
		})
	}
}

func TestRender_ResourceSetConflictingDataConsumers(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/skip=%t", kind, skip), func(t *testing.T) {
				t.Parallel()

				run := func(reverse bool) *Result {
					root := t.TempDir()
					testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "apps", "  path: ./producers\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
					testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", "  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  postBuild: {substituteFrom: [{kind: "+kind+", name: shared}]}\n"))
					testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [a.yaml, z.yaml]\n")
					for _, producer := range []struct{ name, value string }{{"a", "alpha"}, {"z", "zulu"}} {
						value := producer.value
						if kind == manifest.KindSecret {
							value = base64.StdEncoding.EncodeToString([]byte(value))
						}
						testutil.WriteFile(t, root, "producers/"+producer.name+".yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: apps}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: v1
      kind: %s
      metadata: {name: shared, namespace: apps}
      data: {VALUE: %s}
`, producer.name, kind, value))
					}
					testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [output.yaml]\n")
					testutil.WriteFile(t, root, "consumer/output.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output, namespace: apps}\ndata: {value: '${VALUE}'}\n")
					cfg := Config{Path: root, RepoRoot: root, CacheDir: t.TempDir(), Concurrency: 2}
					if skip {
						cfg.HelmOptions = helm.Options{SkipKinds: []string{kind}}
					}
					o, err := New(t.Context(), cfg)
					assert.NoError(t, err)
					t.Cleanup(func() { assert.NoError(t, o.Stop()) })
					assert.NoError(t, o.Bootstrap(t.Context()))
					o.configureControllers()
					d := dagDispatcher{o}
					parent := testutil.KustomizationID("apps", "parent")
					o.tasks.Go(t.Context(), "parent", func(ctx context.Context) {
						d.Dispatch(ctx, parent, manifest.ReconcileAttempt{Drain: manifest.DrainNone})
					})
					o.tasks.BlockTillDone()
					owners := []manifest.NamedResource{
						{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "a"},
						{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "z"},
					}
					order := slices.Clone(owners)
					if reverse {
						slices.Reverse(order)
					}
					release, done := make(chan struct{}), make(chan struct{})
					o.tasks.Go(t.Context(), "first", func(ctx context.Context) {
						d.Dispatch(ctx, order[0], manifest.ReconcileAttempt{Drain: manifest.DrainNone})
						close(release)
					})
					o.tasks.Go(t.Context(), "second", func(ctx context.Context) {
						select {
						case <-release:
							d.Dispatch(ctx, order[1], manifest.ReconcileAttempt{Drain: manifest.DrainNone})
						case <-ctx.Done():
						}
						close(done)
					})
					<-done
					o.tasks.BlockTillDone()
					consumer := testutil.KustomizationID("apps", "consumer")
					o.tasks.Go(t.Context(), "consumer", func(ctx context.Context) {
						d.Dispatch(ctx, consumer, manifest.ReconcileAttempt{Drain: manifest.DrainNone})
					})
					o.tasks.BlockTillDone()
					if failures := o.store.FailedResources(); len(failures) != 0 {
						t.Fatalf("render failed: %v", failures)
					}
					for _, id := range append(owners, parent, consumer) {
						info, ok := o.store.GetStatus(id)
						if !ok || info.Status != store.StatusReady {
							t.Fatalf("%s did not render successfully: %+v", id, info)
						}
					}
					shared := manifest.NamedResource{Kind: kind, Namespace: "apps", Name: "shared"}
					var stored string
					if kind == manifest.KindConfigMap {
						cm, ok := o.store.Get[*manifest.ConfigMap](shared)
						if !ok {
							t.Fatal("missing generated ConfigMap")
						}
						stored = cm.Data["VALUE"].(string)
					} else {
						secret, ok := o.store.Get[*manifest.Secret](shared)
						if !ok {
							t.Fatal("missing generated Secret")
						}
						var found bool
						stored, found, err = secret.Value("VALUE")
						assert.NoError(t, err)
						assert.Equal(t, found, true)
					}
					output, ok := o.store.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "consumer-output"})
					if !ok {
						t.Fatal("missing rendered consumer")
					}
					consumed := output.Data["value"]
					assert.Equal(t, stored, "zulu")
					assert.Diff(t, consumed, "zulu")
					res := &Result{Manifests: o.collectManifests(), Warnings: o.store.Warnings()}
					count := 0
					for owner, docs := range res.Manifests {
						for _, doc := range docs {
							name, ns := manifest.DocMetadata(doc)
							if manifest.DocKind(doc) != kind || name != "shared" || ns != "apps" {
								continue
							}
							count++
							value := doc["data"].(map[string]any)["VALUE"].(string)
							if kind == manifest.KindSecret {
								decoded, err := base64.StdEncoding.DecodeString(value)
								assert.NoError(t, err)
								value = string(decoded)
							}
							assert.Equal(t, owner, parent)
							t.Logf("generated=%s store=%s consumer=%v", value, stored, consumed)
							assert.Equal(t, value, stored)
						}
					}
					wantCount := 1
					if skip {
						wantCount = 0
					}
					assert.Equal(t, count, wantCount)
					if len(res.Warnings) != 1 {
						t.Fatalf("warnings = %+v", res.Warnings)
					}
					w := res.Warnings[0]
					assert.Equal(t, w.Resource, shared)
					assert.Equal(t, w.Category, manifest.WarnConflictingProducers)
					assert.Equal(t, w.Count, 1)
					assert.Diff(t, w.Detail, []string{owners[0].String(), owners[1].String()})
					if !strings.Contains(w.Message, "using "+owners[1].String()) {
						t.Errorf("wrong winner: %s", w.Message)
					}
					return res
				}
				first, second := run(false), run(true)
				assert.Diff(t, second.Manifests, first.Manifests)
				assert.Diff(t, second.Warnings, first.Warnings)
			})
		}
	}
}
