package discovery

import (
	"bytes"
	"path/filepath"
	"testing"
	"testing/fstest"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/change"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResult_PreparationTransfer(t *testing.T) {
	t.Parallel()
	fixture := newPreparedConstructionFixture(t, false)
	cfg, st := fixture.cfg, fixture.cfg.Store
	parent := fixture.parent.Clone()
	const value = "example.net/value"
	const substitute = manifest.SubstituteAnnotationKey
	parent.CommonMetadata = &kustomizev1.CommonMetadata{Annotations: map[string]string{substitute: "disabled"}}
	st.AddObject(parent)
	applier := cfg.Input.FS.(fstest.MapFS)["parent/applier.yaml"]
	applier.Data = bytes.Replace(applier.Data, []byte("spec:\n"),
		[]byte("  annotations: {"+value+": '${TEAM}'}\nspec:\n"), 1)
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
	source, err := kustomize.SourceInput(st, owner)
	assert.NoError(t, err)
	request := kustomize.RenderRequest{
		Source: source, Spec: owner, SubPath: owner.Path,
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
	frame := result.prepared[id]
	assert.Equal(t, len(frame.children), 1)
	discovered := manifest.DeepCopyMap(frame.children[0].Doc)
	wantFingerprint := manifest.Fingerprint(frame.content)
	result.Close()
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), true)
	docs, _, err := kustomize.Render(t.Context(), cfg.Trees, request, cfg.RemoteRetry)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 1)
	assert.Equal(t, docs[0].Document["kind"], any(manifest.KindKustomization))
	rawAnnotations := docs[0].Document["metadata"].(map[string]any)["annotations"].(map[string]any)
	assert.Equal(t, rawAnnotations[substitute], nil)
	base.PrepareDocuments(owner, docs)
	shaping := kustomize.PrepareShape(&request, docs)
	defer shaping.Retire()
	assert.Equal(t, manifest.Fingerprint(shaping.Content()), wantFingerprint)
	assert.NoError(t, shaping.Apply(t.Context(), kustomize.ShapeOptions{}))
	children := execution.ParseChildren(owner, docs, manifest.ParseDocOptions{})
	assert.Equal(t, len(children.Failures), 0)
	assert.Equal(t, len(children.Children), 1)
	assert.Diff(t, children.Children[0].Doc, discovered)
	annotations := discovered["metadata"].(map[string]any)["annotations"].(map[string]any)
	assert.Equal(t, annotations[substitute], any("disabled"))
	assert.Equal(t, annotations[value], any("${TEAM}"))
	execution.Retire()
	assert.Equal(t, request.Preparation.Matches(cfg.Trees, &request), false)
}

func TestResult_AggregatePreparationTransfer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		value  string
		builds uint64
	}{
		{name: "unchanged input", value: "before", builds: 1},
		{name: "changed input", value: "after", builds: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testAggregatePreparationTransfer(t, tc.value, tc.builds)
		})
	}
}

func testAggregatePreparationTransfer(t *testing.T, value string, builds uint64) {
	t.Helper()
	var recorder manifest.WorkRecorder
	root := t.TempDir()
	const plain = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: plain}\ndata: {value: "
	inputFS := fstest.MapFS{
		"package/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
		"package/cm.yaml": {
			Data: []byte("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: package}\n"),
		},
		"plain.yaml": {Data: []byte(plain + "before}\n")},
	}
	cfg := testConfig(t, Config{
		Path: root, Work: recorder.Scope(),
		Input: &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: inputFS},
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
	input, err := kustomize.SourceInput(cfg.Store, owner)
	assert.NoError(t, err)
	var destination base.Preparation[kustomize.Preparation]
	var execution base.Execution
	execution.Init(base.New(cfg.Store, task.NewBounded(2)), producer)
	defer execution.Close()
	request := kustomize.RenderRequest{
		Work: cfg.Work.RenderWork(), Source: input, Spec: owner, SubPath: owner.Path,
	}
	request.Preparation = execution.Prepare(
		&destination,
		result.Preparation(producer),
		func(p *kustomize.Preparation) bool {
			return p.Matches(cfg.Trees, &request)
		},
	)
	result.Close()
	inputFS["plain.yaml"] = &fstest.MapFile{Data: []byte(plain + value + "}\n")}
	docs, _, err := kustomize.Render(t.Context(), cfg.Trees, request, cfg.RemoteRetry)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 2)
	var plainValue any
	for i := range docs {
		if manifest.DocMetadata(docs[i].Document).Name == "plain" {
			data, _ := docs[i].Document["data"].(map[string]any)
			plainValue = data["value"]
		}
	}
	assert.Equal(t, plainValue, any(value))
	var after manifest.WorkCounts
	summary := recorder.Scope().Snapshot(0)
	for i := range summary {
		record := &summary[i]
		if record.Key.Phase == manifest.WorkKustomize {
			after.Add(&record.Counts)
		}
	}
	assert.Equal(t, after.Attempts, builds)
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
			input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root, FS: fstest.MapFS{
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

			session, err := BeginTest(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			ready, err := session.Load(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, ready, true)
			id := pkg.render.owner.Named()
			frame := session.prepared[id]
			assert.FatalIf(t, frame == nil, "construction discarded the planned build")
			assert.Equal(t, frame.publication == loader.BuiltRegistered, tc.register)
			assert.Equal(t, frame.receipt == pkg.render.receipt, true)

			result, err := session.Freeze(t.Context())
			assert.NoError(t, err)
			assert.FatalIf(t, result == nil, "construction did not freeze")
			request := kustomize.RenderRequest{
				Work: cfg.Work.RenderWork(), Source: input, Spec: frame.owner, SubPath: frame.owner.Path,
				Preparation: result.Preparation(id),
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

func TestResult_BootstrapRecipeTransfer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		same bool
	}{
		{name: "same material", same: true},
		{name: "different material"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			const bootstrap = "entry/bootstrap"
			const configMap = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: inventory}\n"
			files := fstest.MapFS{
				bootstrap + "/kustomization.yaml": {Data: []byte("resources: [cm.yaml]\n")},
				bootstrap + "/cm.yaml":            {Data: []byte(configMap + "data: {version: old}\n")},
			}
			original := &store.SourceArtifact{
				LocalPath: root, FS: files, Provider: store.ProviderInput, Content: "old",
			}
			cfg := testConfig(t, Config{Input: original, Path: filepath.Join(root, "entry"), Store: store.New()})
			planner, err := BeginTest(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(planner.Close)
			owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, bootstrap, manifest.NamedResource{})
			planned, err := planner.preparePackageInventory(t.Context(), owner, original, nil)
			assert.NoError(t, err)
			recipe := &plannedPackage{render: planned.declarationRender}
			entry := &Entry{
				Path: cfg.Path, Input: original, target: cfg.Path, packagePath: bootstrap,
				prepared: &entryInputs{
					input: *original, parsed: new(loader.ParsedInputs),
					packages: []*plannedPackage{recipe},
				},
			}
			current := *original
			if !tc.same {
				current.Content = "new"
				current.FS = fstest.MapFS{
					bootstrap + "/kustomization.yaml": files[bootstrap+"/kustomization.yaml"],
					bootstrap + "/cm.yaml":            {Data: []byte(configMap + "data: {version: new}\n")},
				}
			}
			cfg.Input, cfg.Entry, cfg.Store = &current, entry, store.New()
			session, err := BeginTest(t.Context(), cfg)
			assert.NoError(t, err)
			t.Cleanup(session.Close)
			assert.Equal(t, session.loader.Options.ParsedInputs == nil, true)
			if !tc.same {
				session.retainPackage(recipe)
			}
			ready, err := session.Load(t.Context())
			assert.NoError(t, err)
			assert.Equal(t, ready, true)
			assert.Equal(t, session.prepared[owner.Named()] != nil, tc.same)
			frame, err := session.preparePackageInventory(t.Context(), owner, &current, nil)
			assert.NoError(t, err)
			assert.FatalIf(t, frame == nil || !frame.complete, "inventory did not settle")
			assert.Equal(t, frame.source, &current)
			assert.Equal(t, manifest.ValueEqual(frame.children[0].Doc, planned.children[0].Doc), tc.same)
			assert.Equal(t, cfg.Store.GetDeclaration(owner.Named()) == nil, true)
			session.Close()
			assert.Equal(t, session.prepared == nil, true)
			assert.Equal(t, frame.owner == nil, true)
			assert.Equal(t, frame.children == nil, true)
		})
	}
}
