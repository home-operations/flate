package orchestrator

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

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
