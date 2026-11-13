package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type completionCancellationData struct {
	*manifest.ConfigMap

	onNamed func()
}

func (m *completionCancellationData) Named() manifest.NamedResource {
	if m.onNamed != nil {
		onNamed := m.onNamed
		m.onNamed = nil
		onNamed()
	}
	return m.ConfigMap.Named()
}

type completionCancellationFacts struct {
	*loader.Loader

	data *completionCancellationData
}

func (f completionCancellationFacts) OccurrenceObject(
	ctx context.Context, occurrence *selection.OccurrenceFacts, input *store.SourceArtifact,
) (store.Child, error) {
	child, err := f.Loader.OccurrenceObject(ctx, occurrence, input)
	if child.Obj != nil && child.Obj.Named() == f.data.ConfigMap.Named() {
		child.Obj = f.data
	}
	return child, err
}

//nolint:paralleltest // Orchestrator setup changes process-global source policy.
func TestFinishInitialAdmission_CancellationDuringRound(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/owner.yaml", testutil.KustomizationYAML("owner", "apps",
		"  path: ./work\n  postBuild: {substituteFrom: [{kind: ConfigMap, name: vars}]}\n"))
	testutil.WriteFile(
		t,
		root,
		"flux/vars.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {VALUE: known}\n",
	)
	testutil.WriteFile(t, root, "work/kustomization.yaml", "resources: []\n")
	request := selection.Request{Roots: []manifest.NamedResource{testutil.KustomizationID("apps", "owner")}}
	o, err := newTestOrchestrator(t, Tree{
		Path: filepath.Join(root, "flux"), RepoRoot: root,
		Input: &store.SourceArtifact{LocalPath: root, FS: os.DirFS(root)},
	}, Config{Concurrency: 2, Selection: request})
	assert.NoError(t, err)
	t.Cleanup(o.stop)
	assert.NoError(t, o.discover(t.Context()))
	data := &completionCancellationData{ConfigMap: &manifest.ConfigMap{
		Name: "vars", Namespace: "apps", Data: map[string]any{"VALUE": "known"},
	}}
	o.selection, err = selection.New(o.store, completionCancellationFacts{Loader: o.loader, data: data}, request)
	assert.NoError(t, err)
	o.loader.Options.Session = o.selection
	o.store.SetArtifact(manifest.BootstrapSourceID, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	published := false
	id := data.ConfigMap.Named()
	unsubscribe := o.store.AddListener(store.EventObjectAdded, func(current manifest.NamedResource, _ any) {
		if current == id {
			published = true
			data.onNamed = cancel
		}
	}, false)
	defer unsubscribe()
	err = o.finishInitialAdmission(ctx)
	assert.Equal(t, published, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, err, ctx.Err())
}

func TestConstructDiscovery_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	l := loader.New(st)
	full, err := selection.New(st, l, selection.Request{})
	assert.NoError(t, err)
	l.Options.Session = full
	session, err := discovery.Begin(t.Context(), discovery.Config{
		Store: st, Loader: l, Path: filepath.Join(root, "flux"),
		Input: &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"flux/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps",
				"  path: ./work\n  sourceRef: {kind: OCIRepository, name: missing}\n"))},
		}},
	})
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(st, tasks, nil)
	controller.Configure(sourcectrl.FetchOptions{Admission: session.Admission()}, cache.RetryConfig{})
	o := &Orchestrator{store: st, loader: l, selection: full, session: session, src: controller, tasks: tasks}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	published := false
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind == manifest.KindOCIRepository && id.Name == "missing" {
			published = true
			cancel()
		}
	}, false)
	defer unsubscribe()
	result, err := o.constructDiscovery(ctx)
	assert.Equal(t, published, true)
	assert.Equal(t, result == nil, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, err, ctx.Err())
}

func TestBeginDiscovery_CancellationDuringRound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	l := loader.New(st)
	request := selection.Request{Changed: true}
	selected, err := selection.New(st, l, request)
	assert.NoError(t, err)
	l.Options.Session = selected
	tasks := task.NewBounded(2)
	o := &Orchestrator{store: st, loader: l, selection: selected, src: sourcectrl.New(st, tasks, nil), tasks: tasks}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	published := false
	unsubscribe := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind == manifest.KindOCIRepository && id.Name == "missing" {
			published = true
			cancel()
		}
	}, false)
	defer unsubscribe()
	err = o.beginDiscovery(ctx, &discovery.Config{
		Store: st, Loader: l, Selection: request, Path: filepath.Join(root, "flux"),
		Input: &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"flux/owner.yaml": {Data: []byte(testutil.KustomizationYAML("owner", "apps",
				"  path: ./work\n  sourceRef: {kind: OCIRepository, name: missing}\n"))},
		}},
	})
	if o.session != nil {
		t.Cleanup(o.session.Close)
	}
	assert.Equal(t, published, true)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, err, ctx.Err())
}

//nolint:gocritic // Test admission MUST preserve the caller configuration snapshot.
func runDiscovery(ctx context.Context, cfg discovery.Config) (*discovery.Result, error) {
	session, err := discovery.Begin(ctx, cfg)
	if err != nil {
		return nil, err
	}
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(cfg.Store, tasks, nil)
	controller.Configure(sourcectrl.FetchOptions{Admission: session.Admission()}, cache.RetryConfig{})
	for {
		if err := reconcileConstruction(ctx, session.PendingSources(), controller, tasks); err != nil {
			return nil, err
		}
		result, err := session.Run(ctx)
		if err != nil || result != nil {
			return result, err
		}
	}
}

func constructDiscoveredSource(
	ctx context.Context,
	st *store.Store,
	admission *discovery.Admission,
	id manifest.NamedResource,
) error {
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(st, tasks, nil)
	controller.Configure(sourcectrl.FetchOptions{Admission: admission}, cache.RetryConfig{})
	var err error
	tasks.Go(ctx, "construct source", func(ctx context.Context) { err = controller.Construct(ctx, id) })
	tasks.BlockTillDone()
	return err
}

type panickingConstructionSources struct{}

func (panickingConstructionSources) Resolve(
	context.Context,
	manifest.NamedResource,
	manifest.BaseManifest,
	*store.ReadSet,
) (store.Child, *store.SourceArtifact, error) {
	panic("construction source failure")
}

func TestReconcileConstruction_PanicCannotSucceed(t *testing.T) {
	t.Parallel()
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(store.New(), tasks, nil)
	controller.Configure(sourcectrl.FetchOptions{Admission: panickingConstructionSources{}}, cache.RetryConfig{})
	err := reconcileConstruction(t.Context(), []manifest.NamedResource{manifest.BootstrapSourceID}, controller, tasks)
	assert.ErrorIf(t, err == nil && tasks.Failures() == 0, "%v",
		"construction panic returned success and disappeared from invocation failure accounting")
	assert.FatalIf(t, err == nil, "%v", "construction panic returned no reconciliation error")
}

func TestConstructDiscovery_UnrestrictedPendingSources(t *testing.T) {
	t.Parallel()
	st := store.New()
	l := loader.New(st)
	full, err := selection.New(st, l, selection.Request{})
	assert.NoError(t, err)
	assert.FatalIf(t, full == nil, "unrestricted request has no session")
	l.Options.Session = full
	root := t.TempDir()
	session, err := discovery.Begin(t.Context(), discovery.Config{
		Store: st, Loader: l, Path: root,
		Input: &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"cm.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: eager, namespace: apps}\n")},
		}},
	})
	assert.NoError(t, err)
	t.Cleanup(session.Close)
	assert.Diff(t, session.PendingSources(), []manifest.NamedResource{manifest.BootstrapSourceID})
	assert.Equal(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent), nil)
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(st, tasks, nil)
	controller.Configure(sourcectrl.FetchOptions{Admission: session.Admission()}, cache.RetryConfig{})
	o := &Orchestrator{store: st, loader: l, selection: full, session: session, src: controller, tasks: tasks}
	result, err := o.constructDiscovery(t.Context())
	assert.NoError(t, err)
	assert.FatalIf(t, result == nil, "unrestricted discovery has no result")
	assert.Equal(t, len(session.PendingSources()), 0)
	assert.Equal(t, st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent) != nil, true)
	status, exists := st.GetStatus(manifest.BootstrapSourceID)
	assert.Equal(t, exists, true)
	assert.Equal(t, status.Status, store.StatusReady)
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "eager"}
	assert.Equal(t, st.GetDeclaration(id) != nil, true)
	assert.Equal(t, full.Snapshot().Output(id), true)
}
