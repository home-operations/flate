package discovery

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResult_PreparationTransfer(t *testing.T) {
	t.Parallel()
	fixture := newPreparedConstructionFixture(t, false)
	cfg, st := fixture.cfg, fixture.cfg.Store
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	defer result.Close()
	id := fixture.parent.Named()
	assert.Equal(t, (*Result)(nil).Preparation(id) == nil, true)
	assert.Equal(t, result.Preparation(testutil.KustomizationID("apps", "missing")) == nil, true)
	slot := result.Preparation(id)
	assert.FatalIf(t, slot == nil, "frozen owner has no preparation slot")
	owner, ok := st.GetObject(id).(*manifest.Kustomization)
	assert.Equal(t, ok, true)
	source, _, err := kustomize.SourceInput(st, owner)
	assert.NoError(t, err)
	request := kustomize.RenderRequest{
		Source: source, Spec: owner, SubPath: owner.Path,
		ApplyIgnore: kustomize.ApplySourceIgnore(owner, source),
	}
	assert.Equal(t, slot.Matches(cfg.Trees, &request), true)
	var destination base.Preparation[kustomize.Preparation]
	var execution base.Execution
	execution.Init(base.New(st, task.NewBounded(2)), id)
	defer execution.Close()
	request.Preparation = execution.Prepare(&destination, slot, func(p *kustomize.Preparation) bool {
		return p.Matches(cfg.Trees, &request)
	})
	assert.Equal(t, result.Preparation(id), slot)
	assert.Equal(t, slot.Matches(cfg.Trees, &request), false)
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), true)
	result.Close()
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), true)
	docs, _, err := kustomize.Render(t.Context(), cfg.Trees, request, cfg.RemoteRetry)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0].Document["kind"], any(manifest.KindKustomization))
	execution.Retire()
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), false)
}

func TestResult_AggregatePreparationTransfer(t *testing.T) {
	t.Parallel()
	var recorder manifest.WorkRecorder
	root := t.TempDir()
	cfg := testConfig(t, Config{
		Path: root, Work: recorder.Scope(),
		Input: &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
			"package/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
			"package/cm.yaml": {
				Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: package}\n"),
			},
			"plain.yaml": {Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: plain}\n")},
		}},
	})
	entries, err := Plan(t.Context(), cfg)
	assert.NoError(t, err)
	assert.Equal(t, len(entries), 1)
	cfg.Input, cfg.Entry, cfg.Path, cfg.Store = entries[0].Input, &entries[0], entries[0].Path, store.New()
	result, err := Run(t.Context(), cfg)
	assert.NoError(t, err)
	defer result.Close()
	producer := result.Binding.Producer
	owner, ok := cfg.Store.GetObject(producer).(*manifest.Kustomization)
	assert.Equal(t, ok, true)
	input, _, err := kustomize.SourceInput(cfg.Store, owner)
	assert.NoError(t, err)
	var destination base.Preparation[kustomize.Preparation]
	var execution base.Execution
	execution.Init(base.New(cfg.Store, task.NewBounded(2)), producer)
	defer execution.Close()
	request := kustomize.RenderRequest{
		Work: cfg.Work.RenderWork(), Source: input, Spec: owner, SubPath: owner.Path,
		ApplyIgnore: kustomize.ApplySourceIgnore(owner, input),
	}
	request.Preparation = execution.Prepare(
		&destination,
		result.Preparation(producer),
		func(p *kustomize.Preparation) bool {
			return p.Matches(cfg.Trees, &request)
		},
	)
	result.Close()
	docs, _, err := kustomize.Render(t.Context(), cfg.Trees, request, cfg.RemoteRetry)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 2)
	var after manifest.WorkCounts
	for _, record := range recorder.Scope().Snapshot(0) {
		if record.Key.Phase == manifest.WorkKustomize {
			after.Add(&record.Counts)
		}
	}
	assert.Equal(t, after.Attempts, uint64(1))
	execution.Retire()
}

func TestResult_PlannedBootstrapPreparationTransfer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target string
		register     bool
	}{
		{name: "independent workload", target: "./workload"},
		{name: "unchanged bootstrap declarations", target: "./entry"},
		{name: "patched bootstrap declaration", target: "./entry", register: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var recorder manifest.WorkRecorder
			root := t.TempDir()
			kind, patchPath, value := "ConfigMap", "/data/value", "'new'"
			if tc.register {
				kind, patchPath, value = manifest.KindKustomization, "/spec/prune", "true"
			}
			patch := "resources: [cluster.yaml, cm.yaml]\npatches:\n- target: {kind: " + kind +
				"}\n  patch: |-\n    - op: replace\n      path: " + patchPath + "\n      value: " + value + "\n"
			input := &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{
				"entry/kustomization.yaml": {Data: []byte(patch)},
				"entry/cluster.yaml": {Data: []byte(testutil.KustomizationYAML("cluster", "apps",
					"  path: "+tc.target+"\n  prune: false\n"))},
				"entry/cm.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: package}\ndata: {value: old}\n"),
				},
				"workload/input.yaml": {
					Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload}\n"),
				},
			}}
			cfg := testConfig(t, Config{
				Input: input, Path: filepath.Join(root, "entry"), Work: recorder.Scope(),
				Selection: selection.Request{Changed: true}, Changes: new(change.Set),
			})
			entries, err := Plan(t.Context(), cfg)
			assert.NoError(t, err)
			assert.FatalIf(t, len(entries) != 1, "entries = %+v", entries)
			packages := entries[0].prepared.packages
			if tc.target == "./workload" {
				assert.Equal(t, len(packages), 0)
				for _, record := range recorder.Scope().Snapshot(0) {
					assert.FatalIf(
						t,
						record.Key.Phase == manifest.WorkKustomize,
						"unreached package built: %+v",
						record,
					)
				}
				return
			}
			assert.FatalIf(t, len(packages) != 1, "packages = %+v", packages)
			pkg := packages[0]
			assert.Equal(t, pkg.register, tc.register)

			cfg.Entry, cfg.Store = &entries[0], store.New()
			cfg.Trees = kustomize.NewTreeCache(cfg.Cache, nil, task.NewBounded(2))
			t.Cleanup(cfg.Trees.Close)
			session, err := BeginTest(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			ready, err := session.Load(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, ready, true)
			id := pkg.render.owner.Named()
			frame := session.prepared[id]
			assert.FatalIf(t, frame == nil, "construction discarded the planned build")
			assert.Equal(t, frame.registered, tc.register)
			assert.Equal(t, frame.receipt == pkg.render.receipt, true)

			result, err := session.Freeze(t.Context())
			assert.NoError(t, err)
			assert.FatalIf(t, result == nil, "construction did not freeze")
			request := kustomize.RenderRequest{
				Work: cfg.Work.RenderWork(), Source: input, Spec: frame.owner, SubPath: frame.owner.Path,
				ApplyIgnore: true, Preparation: result.Preparation(id),
			}
			assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), true)
			docs, _, err := kustomize.Render(t.Context(), cfg.Trees, request, cfg.RemoteRetry)
			assert.NoError(t, err)
			assert.Equal(t, len(docs), 2)
			var counts manifest.WorkCounts
			for _, record := range recorder.Scope().Snapshot(0) {
				if record.Key.Phase == manifest.WorkKustomize {
					counts.Add(&record.Counts)
				}
			}
			assert.Equal(t, counts.Attempts, uint64(1))
		})
	}
}
