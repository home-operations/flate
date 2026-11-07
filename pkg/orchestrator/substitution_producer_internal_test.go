package orchestrator

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionProducerOrdering(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		for _, changedOnly := range []bool{false, true} {
			for _, producerFirst := range []bool{false, true} {
				t.Run(
					fmt.Sprintf("%s/changed_%t/producer_first_%t", kind, changedOnly, producerFirst),
					func(t *testing.T) {
						testRenderSubstitutionProducerOrderingCase(
							t,
							testRenderSubstitutionProducerOrderingCaseInputs{
								kind:          &kind,
								changedOnly:   &changedOnly,
								producerFirst: &producerFirst,
							},
						)
					},
				)
			}
		}
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_SubstitutionProducerFailureRejectsPredictions(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			testutil.WriteSubstitutionProducerCluster(t, root, kind)
			testutil.WriteFile(t, root, "data/version.env", "VERSION=4.6.2\n")
			testutil.WriteFile(
				t,
				root,
				"data/kustomization.yaml",
				"namespace: flux-system\nresources: [missing.yaml]\ngeneratorOptions: {disableNameSuffixHash: true}\n"+map[string]string{manifest.KindConfigMap: "configMapGenerator", manifest.KindSecret: "secretGenerator"}[kind]+":\n- name: settings\n  envs: [version.env]\n",
			)
			o := newSubstitutionOrchestrator(t, treeConfig{Config: Config{}, Path: root})
			res, err := o.render(t.Context(), t)
			consumer, producer := testutil.KustomizationID(
				"flux-system",
				"consumer",
			), testutil.KustomizationID(
				"flux-system",
				"producer",
			)
			if err == nil || res.Failed[producer].Status != store.StatusFailed {
				t.Fatalf("materializer failure missing: error=%v failures=%v", err, res.Failed)
			}
			target := manifest.NamedResource{Kind: kind, Namespace: consumer.Namespace, Name: "settings"}
			if res.Failed[consumer].Status != store.StatusFailed ||
				!slices.Equal(res.Blocked[consumer], []manifest.NamedResource{target}) ||
				len(outputByParent(res.Manifests)[consumer]) != 0 {
				t.Fatalf(
					"unaccepted prediction supplied required content: failures=%v blocked=%v manifests=%v",
					res.Failed,
					res.Blocked,
					outputByParent(res.Manifests)[consumer],
				)
			}
		})
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_ResourceSetConflictingDataConsumers(t *testing.T) {
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		for _, skip := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/skip=%t", kind, skip), func(t *testing.T) {
				testRenderResourceSetConflictingDataConsumersCase(
					t,
					testRenderResourceSetConflictingDataConsumersCaseInputs{kind: &kind, skip: &skip},
				)
			},
			)
		}
	}
}

type testRenderSubstitutionProducerOrderingCaseInputs struct {
	kind          *string
	changedOnly   *bool
	producerFirst *bool
}

func testRenderSubstitutionProducerOrderingCase(
	t *testing.T,
	testCaseInput testRenderSubstitutionProducerOrderingCaseInputs,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteSubstitutionProducerCluster(t, root, (*testCaseInput.kind))
	cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, Path: root, RepoRoot: root}
	if *testCaseInput.changedOnly {
		cfg.ExternalChanges = change.NewSet([]string{"app/cm.yaml"})
	}
	o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
	assert.FatalIf(t, err != nil, "%v", err)
	t.Cleanup(func() {
		o.stop()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	{
		err := o.bootstrap(ctx)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	reconcileEntryForTest(t, o)
	o.configureControllers()
	d := dagDispatcher{o}
	producer, consumer := testutil.KustomizationID(
		"flux-system",
		"producer",
	), testutil.KustomizationID(
		"flux-system",
		"consumer",
	)
	release := make(chan struct{})
	o.tasks.Go(ctx, "producer", func(ctx context.Context) {
		select {
		case <-release:
			d.Dispatch(ctx, producer, manifest.DrainNone, schedule.Result{})
		case <-ctx.Done():
		}
	})
	if *testCaseInput.producerFirst {
		close(release)
		o.tasks.BlockTillDone()
	}
	attempt := make(chan *base.Execution, 1)
	o.tasks.Go(ctx, "consumer", func(ctx context.Context) {
		result := d.Dispatch(ctx, consumer, manifest.DrainNone, schedule.Result{})
		attempt <- result.Assessment.(*base.Execution)
	})
	var execution *base.Execution
	select {
	case execution = <-attempt:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !(*testCaseInput.producerFirst) {
		close(release)
	}
	o.tasks.BlockTillDone()
	defer execution.Close()
	if slices.Contains(execution.Blocked, producer) == (*testCaseInput.producerFirst) {
		t.Fatalf("consumer blockers = %v, producer first = %t", execution.Blocked, (*testCaseInput.producerFirst))
	}
	o.tasks.Go(
		ctx,
		"consumer",
		func(ctx context.Context) { d.Dispatch(ctx, consumer, manifest.DrainNone, schedule.Result{}) },
	)
	o.tasks.BlockTillDone()
	output, _ := o.store.Get[*manifest.ConfigMap](
		manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: "flux-system",
			Name:      "output",
		},
	)
	assert.FatalIf(
		t,
		output == nil || output.Data["version"] != "4.6.2",
		"consumer output = %+v, want rendered version 4.6.2",
		output,
	)
}

type testRenderResourceSetConflictingDataConsumersCaseInputs struct {
	kind *string
	skip *bool
}

func testRenderResourceSetConflictingDataConsumersCase(
	t *testing.T,
	testCaseInput testRenderResourceSetConflictingDataConsumersCaseInputs,
) {
	t.Helper()

	run := func(reverse bool) *Result {
		root := t.TempDir()
		testutil.WriteFile(
			t,
			root,
			"kustomization.yaml",
			"resources: [flux/parent.yaml, flux/consumer.yaml]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"flux/parent.yaml",
			testutil.KustomizationYAML(
				"parent",
				"apps",
				"  path: ./producers\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"flux/consumer.yaml",
			testutil.KustomizationYAML(
				"consumer",
				"apps",
				"  path: ./consumer\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  postBuild: {substituteFrom: [{kind: "+(*testCaseInput.kind)+", name: shared}]}\n",
			),
		)
		testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [a.yaml, z.yaml]\n")
		for _, producer := range []struct{ name, value string }{{"a", "alpha"}, {"z", "zulu"}} {
			value := producer.value
			if (*testCaseInput.kind) == manifest.KindSecret {
				value = base64.StdEncoding.EncodeToString([]byte(value))
			}
			testutil.WriteFile(
				t,
				root,
				"producers/"+producer.name+".yaml",
				fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: %s, namespace: apps}
spec:
  inputs: [{name: one}]
  resources:
    - apiVersion: v1
      kind: %s
      metadata: {name: shared, namespace: apps}
      data: {VALUE: %s}
`, producer.name, (*testCaseInput.kind), value),
			)
		}
		testutil.WriteFile(t, root, "consumer/kustomization.yaml", "resources: [output.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"consumer/output.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: consumer-output, namespace: apps}\ndata: {value: '${VALUE}'}\n",
		)
		cfg := treeConfig{CacheDir: t.TempDir(), Concurrency: 2, Path: root, RepoRoot: root}
		if *testCaseInput.skip {
			cfg.HelmOptions = helm.Options{SkipKinds: []string{(*testCaseInput.kind)}}
		}
		o, err := newTestOrchestrator(t, cfg.Tree, cfg.Config)
		assert.NoError(t, err)
		t.Cleanup(func() { o.stop() })
		assert.NoError(t, o.bootstrap(t.Context()))
		reconcileEntryForTest(t, o)
		o.configureControllers()
		d := dagDispatcher{o}
		parent := testutil.KustomizationID("apps", "parent")
		o.tasks.Go(
			t.Context(),
			"parent",
			func(ctx context.Context) { d.Dispatch(ctx, parent, manifest.DrainNone, schedule.Result{}) },
		)
		o.tasks.BlockTillDone()
		owners := []manifest.NamedResource{
			{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSet,
				Namespace: "apps",
				Name:      "a",
			},
			{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSet,
				Namespace: "apps",
				Name:      "z",
			},
		}
		order := slices.Clone(owners)
		if reverse {
			slices.Reverse(order)
		}
		release, done := make(chan struct{}), make(chan struct{})
		o.tasks.Go(t.Context(), "first", func(ctx context.Context) {
			d.Dispatch(ctx, order[0], manifest.DrainNone, schedule.Result{})
			close(release)
		})
		o.tasks.Go(t.Context(), "second", func(ctx context.Context) {
			select {
			case <-release:
				d.Dispatch(ctx, order[1], manifest.DrainNone, schedule.Result{})
			case <-ctx.Done():
			}
			close(done)
		})
		<-done
		o.tasks.BlockTillDone()
		return testCaseInput.finishConflictingDataConsumers(t, d, parent, owners)
	}
	first, second := run(false), run(true)
	assert.Diff(t, outputByParent(second.Manifests), outputByParent(first.Manifests))
	assert.Diff(t, second.Warnings, first.Warnings)
}

func (i testRenderResourceSetConflictingDataConsumersCaseInputs) finishConflictingDataConsumers(
	t *testing.T,
	d dagDispatcher,
	parent manifest.NamedResource,
	owners []manifest.NamedResource,
) *Result {
	t.Helper()
	o := d.o
	var err error
	consumer := testutil.KustomizationID("apps", "consumer")
	o.tasks.Go(
		t.Context(),
		"consumer",
		func(ctx context.Context) { d.Dispatch(ctx, consumer, manifest.DrainNone, schedule.Result{}) },
	)
	o.tasks.BlockTillDone()
	{
		failures := o.store.FailedResources()
		assert.FatalIf(t, len(failures) != 0, "render failed: %v", failures)
	}
	for _, id := range append(owners, parent, consumer) {
		info, ok := o.store.GetStatus(id)
		assert.FatalIf(
			t,
			!ok || info.Status != store.StatusReady,
			"%s did not render successfully: %+v",
			id,
			info,
		)
	}
	shared := manifest.NamedResource{Kind: (*i.kind), Namespace: "apps", Name: "shared"}
	var stored string
	if (*i.kind) == manifest.KindConfigMap {
		cm, ok := o.store.Get[*manifest.ConfigMap](shared)
		assert.FatalIf(t, !ok, "%v", "missing generated ConfigMap")
		stored = cm.Data["VALUE"].(string)
	} else {
		secret, ok := o.store.Get[*manifest.Secret](shared)
		assert.FatalIf(t, !ok, "%v", "missing generated Secret")
		var found bool
		valueResult, valueResultErr := secret.Value("VALUE")
		stored, found, err = valueResult.Value, valueResult.Found, valueResultErr
		assert.NoError(t, err)
		assert.Equal(t, found, true)
	}
	output, ok := o.store.Get[*manifest.ConfigMap](
		manifest.NamedResource{
			Kind:      manifest.KindConfigMap,
			Namespace: "apps",
			Name:      "consumer-output",
		},
	)
	assert.FatalIf(t, !ok, "%v", "missing rendered consumer")
	consumed := output.Data["value"]
	assert.Equal(t, stored, "zulu")
	assert.Diff(t, consumed, "zulu")
	res := &Result{Manifests: o.collectManifests(), Warnings: o.store.Warnings()}
	count := 0
	for owner, docs := range outputByParent(res.Manifests) {
		for _, doc := range docs {
			metadata := manifest.DocMetadata(doc)
			name, ns := metadata.Name, metadata.Namespace
			if manifest.DocKind(doc) != (*i.kind) || name != "shared" || ns != "apps" {
				continue
			}
			count++
			value := doc["data"].(map[string]any)["VALUE"].(string)
			if (*i.kind) == manifest.KindSecret {
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
	if *i.skip {
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
	assert.ErrorIf(t, !strings.Contains(w.Message, "using "+owners[1].String()), "wrong winner: %s", w.Message)
	return res
}
