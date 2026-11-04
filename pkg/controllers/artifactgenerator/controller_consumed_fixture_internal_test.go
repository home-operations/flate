package artifactgenerator

import (
	"archive/tar"
	"context"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/controllers/kustomization"

	sourcecontroller "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"

	sources "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type consumedFixture struct {
	publication, change                                   string
	st                                                    *store.Store
	bodies                                                *task.Service
	owner                                                 *cache.Cache
	s1, s2                                                *store.SourceArtifact
	obj                                                   *manifest.RawObject
	doc                                                   map[string]any
	repo, rebound                                         manifest.NamedResource
	consumer                                              manifest.BaseManifest
	driver                                                *manifest.Kustomization
	generator                                             *Controller
	ks                                                    *kustomization.Controller
	src                                                   *sourcecontroller.Controller
	publish                                               func(*store.SourceArtifact)
	firstGenerator, firstConsumer, startPublication       chan struct{}
	committed, release, settled, finishDriver, driverDone chan struct{}
	paused                                                atomic.Bool
	genSignaled, consumerSignaled, successorSignaled      atomic.Bool
	work                                                  generatorWork
	tasks                                                 *convergenceTasks
}

func (f *consumedFixture) preparePublication(t *testing.T, producer *base.Execution) {
	t.Helper()
	st, generator, repo, s1 := f.st, f.generator, f.repo, f.s1
	publication := f.publication
	publish := func(input *store.SourceArtifact) { st.SetArtifact(repo, input) }
	if publication == "emitted" {
		supplier := &manifest.ResourceSet{Name: "supplier", Namespace: "apps"}
		publish = func(input *store.SourceArtifact) {
			_, _, next := producer.Reads.GetEmission(supplier.Named())
			defer next.Finish()
			childDoc := map[string]any{
				"apiVersion": manifest.SourceDomain + "/v1",
				"kind":       repo.Kind,
				"metadata":   map[string]any{"name": repo.Name, "namespace": repo.Namespace},
			}
			parsed := (&base.Execution{Controller: generator.Controller}).ParseChildren(
				supplier,
				testutil.DocumentOccurrences([]map[string]any{childDoc}),
				manifest.ParseDocOptions{},
			)
			assert.NoError(t, manifest.DocumentFailure(parsed.Failures))
			next.Children, next.Manifests, next.Complete = parsed.Children, parsed.Visible, true
			{
				err := producer.Emit(
					t.Context(),
					supplier.Named(),
					&next,
					map[manifest.NamedResource]*store.SourceArtifact{repo: input},
				)
				assert.ErrorIf(t, err != nil, "%v", err)
			}
			producer.Reads.Accept()
		}
		publish(s1)
		inventory, ok := st.GetArtifact(supplier.Named(), store.ArtifactContent).(*store.Emission)
		assert.FatalIf(
			t,
			!ok || !inventory.Complete || st.EmissionOwner(repo) != supplier.Named(),
			"%v",
			"S1 supplying emission is not complete and visible",
		)
		st.UpdateStatus(supplier.Named(), store.StatusInfo{Status: store.StatusReady})
	} else {
		st.AddObject(&manifest.GitRepository{Name: repo.Name, Namespace: repo.Namespace})
		publish(s1)
	}
	assert.FatalIf(
		t,
		st.GetArtifact(repo, store.ArtifactContent) != s1,
		"%v",
		"S1 source is not visible before scheduler start",
	)
	f.publish = publish
}

func (f *consumedFixture) prepareGenerator(t *testing.T) {
	t.Helper()
	change, publication := f.change, f.publication
	st, s1 := f.st, f.s1
	generatorForTestResult := generatorForTest(
		t,
		[]any{map[string]any{"name": "output", "revision": "@repo", "originRevision": "@repo", "copy": []any{
			map[string]any{"from": "@repo/app/kustomization.yaml", "to": "@artifact/app/kustomization.yaml"},
			map[string]any{"from": "@repo/app/cm.yaml", "to": "@artifact/app/cm.yaml"},
		}}},
	)
	obj, doc := generatorForTestResult.object, generatorForTestResult.document
	if strings.HasPrefix(change, "unselected-") || change == "origin-without-revision" ||
		change == "selected-origin-fallback" {
		output := doc["spec"].(map[string]any)["artifacts"].([]any)[0].(map[string]any)
		delete(output, "revision")
		if strings.HasPrefix(change, "unselected-") {
			delete(output, "originRevision")
		}
		parseDocResult, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		parsed, authoritative := parseDocResult.Object, parseDocResult.Document
		assert.FatalIf(t, err != nil, "%v", err)
		obj, doc = parsed.(*manifest.RawObject), authoritative
	}
	repo := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "repo",
	}
	if publication == "emitted" {
		repo.Kind = manifest.KindExternalArtifact
		doc["spec"].(map[string]any)["sources"].([]any)[0].(map[string]any)["kind"] = repo.Kind
		parseDocResult2, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{})
		parsed, authoritative := parseDocResult2.Object, parseDocResult2.Document
		assert.FatalIf(t, err != nil, "%v", err)
		obj, doc = parsed.(*manifest.RawObject), authoritative
	}
	st.AddObject(obj, doc)
	rebound := repo
	rebound.Name = "other"
	if change == "selected-binding" {
		st.SetArtifact(rebound, s1)
	}
	f.obj, f.doc, f.repo, f.rebound = obj, doc, repo, rebound
}

func (f *consumedFixture) wait(ctx context.Context, ch <-chan struct{}) bool {
	bodies := f.bodies
	var received bool
	bodies.YieldSlot(func() {
		select {
		case <-ch:
			received = true
		case <-ctx.Done():
		}
	})
	return received
}

func acquireConsumedInput(ctx context.Context, t *testing.T, owner *cache.Cache, value string) *store.SourceArtifact {
	t.Helper()
	input, descriptor, id, err := owner.Acquire(
		ctx,
		cache.ArchiveKey{Policy: "fixed", Content: value},
		&cache.Policy{MaxAge: cache.Forever},
		func(
			_ context.Context,
			target cache.ProducerInput,
		) (
			cache.ProducerResult,
			error,
		) {
			archive := tar.NewWriter(target.Writer)
			files := fstest.MapFS{
				"app/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				"app/cm.yaml": {
					Data: []byte(
						"apiVersion: v1\nkind: ConfigMap\n" +
							"metadata: {name: generated-config, namespace: apps}\n" +
							"data: {value: " + value + "}\n",
					),
				},
			}
			if err := archive.AddFS(files); err != nil {
				return cache.ProducerResult{}, err
			}
			return cache.ProducerResult{}, archive.Close()
		},
	)
	assert.FatalIf(t, err != nil, "%v", err)
	return &store.SourceArtifact{
		FS:       input,
		Key:      id,
		Content:  cache.AcquisitionIdentity(input),
		Digest:   descriptor.Digest.String(),
		Revision: value + "-revision",
		Metadata: map[string]string{"org.opencontainers.image.revision": value + "-origin"},
	}
}

func (f *consumedFixture) drive(ctx context.Context, t *testing.T) {
	t.Helper()
	st, bodies, obj, doc, rebound := f.st, f.bodies, f.obj, f.doc, f.rebound
	change, publish, s2, wait := f.change, f.publish, f.s2, f.wait
	firstGenerator, firstConsumer, startPublication := f.firstGenerator, f.firstConsumer, f.startPublication
	settled, finishDriver := f.settled, f.finishDriver
	if !wait(ctx, firstGenerator) || !wait(ctx, firstConsumer) || !wait(ctx, startPublication) {
		return
	}
	bodies.YieldSlot(func() {
		declarationChanged := change == "ignored-declaration-metadata" || change == "selected-uid" ||
			change == "selected-binding"
		if !declarationChanged {
			publish(s2)
			return
		}
		updated := manifest.DeepCopyMap(doc)
		updated["metadata"].(map[string]any)["annotations"] = map[string]any{"ignored": "new"}
		if change == "selected-uid" {
			updated["metadata"].(map[string]any)["uid"] = "next"
		}
		selected := manifest.BaseManifest(obj)
		if change == "selected-binding" {
			updated["spec"].(map[string]any)["sources"].([]any)[0].(map[string]any)["name"] = rebound.Name
			parseDocResult3, err := manifest.ParseDoc(updated, manifest.ParseDocOptions{})
			parsed, authoritative := parseDocResult3.Object, parseDocResult3.Document
			assert.FatalIf(t, err != nil, "%v", err)
			selected, updated = parsed, authoritative
		}
		st.AddObject(selected, updated)
	})
	if change == "selected-union" {
		wait(ctx, settled)
	}
	wait(ctx, finishDriver)
}

func (f *consumedFixture) startControllers(t *testing.T) *kustomize.TreeCache {
	t.Helper()
	st, bodies, owner := f.st, f.bodies, f.owner
	consumerDoc := map[string]any{
		"apiVersion": manifest.FluxKustomizeDomain + "/v1",
		"kind":       manifest.KindKustomization,
		"metadata":   map[string]any{"name": "consumer", "namespace": "apps"},
		"spec": map[string]any{
			"interval":  "1m",
			"path":      "./app",
			"sourceRef": map[string]any{"kind": manifest.KindExternalArtifact, "name": "output"},
		},
	}
	parseDocResult4, err := manifest.ParseDoc(consumerDoc, manifest.ParseDocOptions{})
	consumer, authoritative := parseDocResult4.Object, parseDocResult4.Document
	assert.FatalIf(t, err != nil, "%v", err)
	st.AddObject(consumer, authoritative)
	driver := &manifest.Kustomization{Name: "driver", Namespace: "apps"}
	st.AddObject(driver)
	generator := New(st, bodies, owner)
	generator.StartLifecycle()
	trees := kustomize.NewTreeCache(owner, nil, bodies)
	ks := kustomization.New(st, bodies, trees)
	ks.StartLifecycle()
	src := sourcecontroller.New(st, bodies)
	src.Fetchers[manifest.KindExternalArtifact] = sources.Wrap(manifest.KindExternalArtifact, &external.Fetcher{})
	src.StartLifecycle()
	f.consumer, f.driver = consumer, driver
	f.generator, f.ks, f.src = generator, ks, src
	return trees
}

func (f *consumedFixture) observeWork() {
	bodies, obj := f.bodies, f.obj
	firstGenerator, firstConsumer, startPublication := make(chan struct{}), make(chan struct{}), make(chan struct{})
	committed, release, settled, finishDriver := make(
		chan struct{},
	), make(
		chan struct{},
	), make(
		chan struct{},
	), make(
		chan struct{},
	)
	driverDone := make(chan struct{})
	work := &f.work
	tasks := &convergenceTasks{Service: bodies, generator: obj.Named(), work: work}
	tasks.completed = f.completeWork
	f.firstGenerator, f.firstConsumer, f.startPublication = firstGenerator, firstConsumer, startPublication
	f.committed, f.release, f.settled, f.finishDriver, f.driverDone = committed, release, settled, finishDriver, driverDone
	f.tasks = tasks
}

func (f *consumedFixture) prepareInputs(ctx context.Context, t *testing.T) {
	t.Helper()
	change := f.change
	s1, s2 := acquireConsumedInput(ctx, t, f.owner, "S1"), acquireConsumedInput(ctx, t, f.owner, "S2")
	if change == "selected-origin-presence" || change == "selected-origin-fallback" {
		delete(s1.Metadata, "org.opencontainers.image.revision")
	}
	if change != "selected-union" {
		clone := *s1
		clone.Metadata = maps.Clone(s1.Metadata)
		s2 = &clone
		if change == "ignored-artifact-metadata" {
			s2.Metadata["ignored"] = "new"
		}
		if change == "unselected-revision" || change == "selected-revision" || change == "origin-without-revision" ||
			change == "selected-origin-fallback" {
			s2.Revision = "next-revision"
		}
		if change == "unselected-origin" {
			s2.Metadata["org.opencontainers.image.revision"] = "next-origin"
		}
		if change == "selected-origin-presence" {
			s2.Metadata["org.opencontainers.image.revision"] = ""
		}
	}
	f.s1, f.s2 = s1, s2
}

func (f *consumedFixture) completeWork(name string) {
	if name == "schedule/"+f.driver.Named().String() {
		close(f.driverDone)
	}
	if name == "schedule/"+f.obj.Named().String() || name == "schedule/settle/"+f.obj.Named().String() {
		status, _ := f.st.GetStatus(f.obj.Named())
		if status.Status == store.StatusReady && f.genSignaled.CompareAndSwap(false, true) {
			close(f.firstGenerator)
		}
	}
	consumerCompleted := name == "schedule/"+f.consumer.Named().String() ||
		name == "schedule/settle/"+f.consumer.Named().String()
	if !consumerCompleted {
		return
	}
	inventory, ok := f.st.GetArtifact(f.consumer.Named(), store.ArtifactContent).(*store.Emission)
	if !ok || !inventory.Complete || len(inventory.Manifests) != 1 {
		return
	}
	value := inventory.Manifests[0].Document["data"].(map[string]any)["value"]
	switch value {
	case "S1":
		if f.consumerSignaled.CompareAndSwap(false, true) {
			close(f.firstConsumer)
		}
	case "S2":
		if f.successorSignaled.CompareAndSwap(false, true) {
			close(f.settled)
		}
	}
}
