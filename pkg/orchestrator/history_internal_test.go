package orchestrator

import (
	"context"
	"errors"
	"io/fs"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_EntryOwnerDependsOnSibling(t *testing.T) {
	for _, mode := range []string{"direct", "deferred_required", "deferred_optional", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			testRenderEntryOwnerDependsOnSiblingCase(t, &mode)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_EntryReceiptDependencySubstitutions(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, declaration := range []string{"authored", "patched", "independent"} {
		t.Run(declaration, func(t *testing.T) {
			testBootstrapEntryReceiptDependencySubstitutionsCase(
				t,
				&declaration,
			)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_EntryHistoryMatchesEmission(t *testing.T) {
	t.Run("partial_application_history", testBuildChangeFilterPartialApplicationHistory)
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	for _, mode := range []string{"authored", "literal", "empty"} {
		t.Run(mode, func(t *testing.T) {
			testBootstrapEntryHistoryMatchesEmissionCase(
				t,
				&mode,
			)
		},
		)
	}
}

type entryBuildFS struct {
	fs.FS

	builds atomic.Int64
}

func (f *entryBuildFS) Open(name string) (fs.File, error) {
	if name == "entry/child.yaml" {
		var callers [64]uintptr
		frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
		for {
			frame, more := frames.Next()
			if frame.Function == "github.com/home-operations/flate/internal/kustomize.Build" {
				f.builds.Add(1)
				break
			}
			if !more {
				break
			}
		}
	}
	return f.FS.Open(name)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestBootstrap_AcceptedLiteralChildRoot(t *testing.T) {
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	root := t.TempDir()
	cacheDir := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", `  path: ./producers
  postBuild:
    substitute:
      ROOT: apps/red
`))
	testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [child.yaml]\n")
	testutil.WriteFile(t, root, "producers/child.yaml", testutil.KustomizationYAML("child", "apps", `  path: ./${ROOT}
  sourceRef:
    kind: GitRepository
    name: content
`))
	testutil.WriteFile(t, root, "apps/red/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/red/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: before}\n",
	)
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}
	cfg := Config{CacheDir: cacheDir, Concurrency: 2}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	assert.NoError(t, warm.run(t.Context()))
	owner := testutil.KustomizationID("apps", "emitter")
	child := testutil.KustomizationID("apps", "child")
	assert.Equal(t, warm.store.GetObject(child).(*manifest.Kustomization).Path, "./apps/red")
	testutil.WriteFile(
		t,
		root,
		"apps/red/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: payload, namespace: apps}\ndata: {value: after}\n",
	)
	tree.ExternalChanges = change.NewSet([]string{"apps/red/cm.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { fresh.stop() })
	assert.Equal(t, fresh.store.GetObject(child) == nil, true)
	assert.NoError(t, fresh.bootstrap(t.Context()))
	assert.Equal(t, fresh.filter.ShouldReconcile(child), true)
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Equal(t, fresh.store.GetArtifact(owner, store.ArtifactContent) == nil, true)
	assert.Equal(t, fresh.store.GetArtifact(child, store.ArtifactContent) == nil, true)
	assert.Equal(
		t,
		fresh.loader.IndexedObject(child, fresh.index.Records[child]).Obj.(*manifest.Kustomization).Path,
		"./${ROOT}",
	)
	for range fresh.store.Sweep() {
		t.Fatal("Bootstrap left probe registrations waiting for retirement")
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestCompleteBootstrap_Cancellation(t *testing.T) {
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/emitter.yaml", testutil.KustomizationYAML("emitter", "apps", "  path: ./apps\n"))
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, Config{Concurrency: 2})
	assert.NoError(t, err)
	t.Cleanup(func() { o.stop() })
	err = o.discover(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	o.completeBootstrap()
	o.cfg.ExternalChanges = change.NewSet([]string{"flux/emitter.yaml"})
	err = o.buildChangeFilter(ctx)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, len(o.filter.KeepNames()) == 0, true)
	for range o.store.Sweep() {
		t.Fatal("canceled preparation left probe registrations")
	}
	assert.Equal(
		t,
		o.store.GetArtifact(testutil.KustomizationID("apps", "emitter"), store.ArtifactContent) == nil,
		true,
	)
	assert.Equal(
		t,
		o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact).FS != nil,
		true,
	)
}

func testRenderEntryOwnerDependsOnSiblingCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(
		t,
		root,
		"entry/kustomization.yaml",
		"resources: [owner.yaml, sibling.yaml, settings.yaml]\n",
	)
	extra := ""
	if (*testCaseInput) == "deferred_required" || (*testCaseInput) == "deferred_optional" {
		extra = "  postBuild: {substituteFrom: [{kind: ConfigMap, name: settings, optional: " + strconv.FormatBool(
			(*testCaseInput) ==
				"deferred_optional",
		) + "}]}\n"
	}
	testutil.WriteFile(
		t,
		root,
		"entry/owner.yaml",
		testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n  dependsOn: [{name: sibling}]\n"+extra),
	)
	testutil.WriteFile(
		t,
		root,
		"entry/settings.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {SIBLING: sibling}\n",
	)
	siblingExtra := ""
	if (*testCaseInput) == "cycle" {
		siblingExtra = "  dependsOn: [{name: owner}]\n"
	}
	siblingDeclaration := testutil.KustomizationYAML("sibling", "apps", "  path: ./sibling\n"+siblingExtra)
	if (*testCaseInput) == "deferred_required" || (*testCaseInput) == "deferred_optional" {
		siblingDeclaration = testutil.KustomizationYAML(
			"declare-sibling",
			"apps",
			"  path: ./declarations\n"+extra,
		)
		testutil.WriteFile(t, root, "declarations/kustomization.yaml", "resources: [sibling.yaml]\n")
		testutil.WriteFile(
			t,
			root,
			"declarations/sibling.yaml",
			testutil.KustomizationYAML("${SIBLING}", "apps", "  path: ./sibling\n"),
		)
	}
	testutil.WriteFile(t, root, "entry/sibling.yaml", siblingDeclaration)
	testutil.WriteFile(t, root, "sibling/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"sibling/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: workload, namespace: apps}\n",
	)
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join(root, "entry"), RepoRoot: root},
		Config{Concurrency: 2},
	)
	assert.NoError(t, err)
	t.Cleanup(o.stop)
	assert.NoError(t, o.bootstrap(t.Context()))
	if !o.entryBinding.Producer.ReportHidden() {
		t.Fatalf("entry prerequisite reused its owner: %+v", o.entryBinding)
	}
	owner := testutil.KustomizationID("apps", "owner")
	if (*testCaseInput) == "deferred_required" || (*testCaseInput) == "deferred_optional" {
		record, _ := o.index.Lookup(owner)
		{
			assert.FatalIf(t, record.Declaration == nil, "%v", "deferred owner lost its declaration")
		}
		o.configureControllers()
		var reads store.ReadSet
		reads.Init(o.store, owner, nil)
		records, _ := o.controllers.PreviousRender(t.Context(), record.Declaration, &reads)
		reads.Retire()
		{
			assert.FatalIf(
				t,
				records != nil,
				"%v",
				"deferred owner accepted history before referenced substitution",
			)
		}
		{
			assert.FatalIf(
				t,
				o.store.GetObject(testutil.KustomizationID("apps", "sibling")) != nil,
				"%v",
				"deferred substitution published the effective sibling during discovery",
			)
		}
	}
	result, err := o.collectResult(o.run(t.Context()))
	if (*testCaseInput) == "cycle" {
		{
			assert.FatalIf(t, err == nil, "%v", "authored cycle rendered successfully")
		}
		for _, name := range []string{"owner", "sibling"} {
			{
				{
					message, ok := o.preflightFailure(
						testutil.KustomizationID("apps", name),
					)
					assert.FatalIf(t, !ok ||
						!strings.Contains(
							message,
							"dependency cycle detected",
						), "authored cycle lost its ordinary diagnosis: %s", message)
				}
			}
		}
		return
	}
	assert.NoError(t, err)
	sibling := testutil.KustomizationID("apps", "sibling")
	if (*testCaseInput) == "deferred_required" || (*testCaseInput) == "deferred_optional" {
		assert.Equal(t, o.store.EmissionOwner(sibling), testutil.KustomizationID("apps", "declare-sibling"))
	}
	for _, id := range []manifest.NamedResource{owner, sibling} {
		status, ok := o.store.GetStatus(id)
		{
			assert.FatalIf(
				t,
				!ok || status.Status != store.StatusReady,
				"entry prerequisite did not reconcile: %s: %+v",
				id,
				status,
			)
		}
	}
	docs := outputByParent(result.Manifests)[sibling]
	{
		assert.FatalIf(
			t,
			len(docs) != 1 || docs[0]["metadata"].(map[string]any)["name"] != "workload",
			"sibling did not render its own path: %v",
			docs,
		)
	}
}

func testBootstrapEntryReceiptDependencySubstitutionsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	cacheDir := t.TempDir()
	entry := "resources: [child.yaml]\n"
	inline := "  postBuild: {substitute: {VALUE: frozen}}\n"
	if (*testCaseInput) == "patched" {
		entry += "patches:\n- target: {kind: Kustomization, name: child}\n  patch: '[{\"op\":\"add\",\"path\":\"/spec/postBuild\",\"value\":{\"substitute\":{\"VALUE\":\"frozen\"}}}]'\n"
		inline = ""
	}
	if (*testCaseInput) == "independent" {
		entry = "resources: [child.yaml, owner.yaml]\n"
		inline = ""
		testutil.WriteFile(
			t,
			root,
			"entry/owner.yaml",
			testutil.KustomizationYAML(
				"owner",
				"apps",
				"  path: ./entry\n  dependsOn: [{name: '${PREFIX:-dev-}ready', namespace: '${TEAM:-dev}'}]\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"leaf/supplier.yaml",
			testutil.KustomizationYAML(
				"supplier",
				"control",
				"  path: ./empty\n  postBuild: {substitute: {PREFIX: prod-, TEAM: control}}\n",
			),
		)
		testutil.WriteFile(t, root, "empty/kustomization.yaml", "resources: []\n")
	}
	testutil.WriteFile(t, root, "entry/kustomization.yaml", entry)
	testutil.WriteFile(
		t,
		root,
		"entry/child.yaml",
		testutil.KustomizationYAML("child", "apps", "  path: ./leaf\n"+inline),
	)
	leaf := "resources: []\n"
	if (*testCaseInput) == "independent" {
		leaf = "resources: [supplier.yaml]\n"
	}
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", leaf)
	input := &entryBuildFS{FS: os.DirFS(root)}
	for _, lifetime := range []string{"cold", "warm"} {
		t.Run(lifetime, func(t *testing.T) {
			testBootstrapEntryReceiptDependencySubstitutionsCaseCase(
				t,
				testBootstrapEntryReceiptDependencySubstitutionsCaseCaseInputs{
					testCaseInput: &testCaseInput,
					root:          &root,
					cacheDir:      &cacheDir,
					input:         &input,
					lifetime:      &lifetime,
				},
			)
		},
		)
	}
}

func testBootstrapEntryHistoryMatchesEmissionCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	resources := "[child.yaml, bucket.yaml, generator.yaml, consumer.yaml]"
	if (*testCaseInput) == "authored" {
		resources = "[owner.yaml, child.yaml, bucket.yaml, generator.yaml, consumer.yaml]"
	}
	if (*testCaseInput) == "empty" {
		resources = "[owner.yaml]"
	}
	testutil.WriteFile(t, root, "entry/kustomization.yaml", "resources: "+resources+"\n")
	if (*testCaseInput) != "literal" {
		testutil.WriteFile(
			t,
			root,
			"entry/owner.yaml",
			testutil.KustomizationYAML("owner", "apps", "  path: ./entry\n"),
		)
	}
	if (*testCaseInput) != "empty" {
		testutil.WriteFile(
			t,
			root,
			"entry/child.yaml",
			testutil.KustomizationYAML(
				"child",
				"apps",
				"  path: ./leaf\n  dependsOn: [{name: '${DEP}', namespace: apps}]\n  postBuild: {substitute: {DEP: ready}}\n",
			),
		)
		testutil.WriteFile(
			t,
			root,
			"entry/bucket.yaml",
			"apiVersion: objectbucket.io/v1alpha1\nkind: ObjectBucketClaim\nmetadata: {name: bucket, namespace: apps}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/generator.yaml",
			"apiVersion: source.extensions.fluxcd.io/v1beta1\nkind: ArtifactGenerator\nmetadata: {name: generator, namespace: apps}\nspec:\n  pathPattern: '@inputs/{team}'\n  artifacts: [{name: 'artifact-{team}'}]\n",
		)
		testutil.WriteFile(
			t,
			root,
			"entry/consumer.yaml",
			testutil.KustomizationYAML(
				"consumer",
				"apps",
				"  path: .\n  sourceRef: {kind: ExternalArtifact, name: artifact-blue, namespace: apps}\n",
			),
		)
	}
	testutil.WriteFile(t, root, "leaf/kustomization.yaml", "resources: []\n")
	o, err := newTestOrchestrator(
		t,
		Tree{Path: filepath.Join(root, "entry"), RepoRoot: root},
		Config{CacheDir: t.TempDir(), Concurrency: 2},
	)
	assert.NoError(t, err)
	assert.NoError(t, o.bootstrap(t.Context()))
	o.configureControllers()
	owner := o.entryBinding.Producer
	readHistory := func() map[manifest.NamedResource]manifest.DiscoveryRecord {
		t.Helper()
		var reads store.ReadSet
		reads.Init(o.store, owner, nil)
		defer reads.Retire()
		records, complete := o.controllers.PreviousRender(t.Context(), o.store.GetObject(owner), &reads)
		{
			assert.FatalIf(t, records == nil || !complete, "%v", "entry lacks complete controller history")
		}
		return maps.Collect(records)
	}
	before := readHistory()
	reopened := cachetest.New(t, o.cfg.CacheDir, sourcecache.New)
	bootstrap := o.store.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	query := []manifest.SourceEvidence{
		{Source: bootstrap.HistoryIdentity(manifest.BootstrapSourceID), Path: "entry/kustomization.yaml"},
	}
	owners, err := reopened.ProducersForPaths(t.Context(), query)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(owners, owner), true)
	{
		{
			_, self := before[owner]
			assert.FatalIf(t, self, "%v", "accepted entry history includes its own owner")
		}
	}
	if (*testCaseInput) == "empty" {
		assert.Equal(t, len(before), 0)
	} else {
		child := testutil.KustomizationID("apps", "child")
		assert.Diff(t, before[child].Refs, []manifest.NamedResource{testutil.KustomizationID("apps", "ready")})
		assert.Diff(
			t,
			before[child].ContentRoots,
			map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"leaf"}},
		)
		consumer := testutil.KustomizationID("apps", "consumer")
		source := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindExternalArtifact,
			Name:      "artifact-blue",
			Namespace: "apps",
		}
		assert.Diff(t, before[consumer].ContentRoots, map[manifest.NamedResource][]string{source: {"."}})
		generator := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Name:      "generator",
			Namespace: "apps",
		}
		assert.Diff(
			t,
			before[source].Productions,
			[]manifest.Production{{Owner: owner, Declaration: generator, Source: o.entryBinding.Source}},
		)
		secret := manifest.NamedResource{Kind: manifest.KindSecret, Name: "bucket", Namespace: "apps"}
		claim := manifest.NamedResource{
			Group:     "objectbucket.io",
			Kind:      "ObjectBucketClaim",
			Name:      "bucket",
			Namespace: "apps",
		}
		assert.Diff(
			t,
			before[secret].Productions,
			[]manifest.Production{{Owner: owner, Declaration: claim, Source: o.entryBinding.Source}},
		)
		for _, record := range before { //nolint:gocritic // Map iteration preserves the value snapshot without an extra lookup.
			{
				assert.FatalIf(
					t,
					record.Authored != (manifest.NamedResource{}) || record.Parent != (manifest.NamedResource{}) ||
						record.BuilderOwner != (manifest.NamedResource{}) ||
						record.Path != "" ||
						record.File != "" ||
						record.Builder != "" ||
						len(record.Inputs) != 0 ||
						record.Pinned ||
						record.Declaration != nil,
					"%v",
					"accepted history contains static-only evidence",
				)
			}
		}
	}
	paths := []string{"leaf/value.yaml", "entry/child.yaml", "unrelated/value.yaml"}
	selection := func(records map[manifest.NamedResource]manifest.DiscoveryRecord) [][]bool {
		selected := make([][]bool, len(paths))
		for i, path := range paths {
			fixture := admissiontest.New(
				t,
				admissiontest.Config{
					Changes:   change.NewSet([]string{path}),
					Index:     o.index,
					Inventory: o.store,
					Root:      root,
				},
			)
			proposal, failure := fixture.Prepare(
				t.Context(),
				store.Child{},
				nil,
				map[manifest.NamedResource]struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{owner: {Records: maps.All(records), Complete: true}},
			)
			assert.NoError(t, failure)
			admissiontest.Commit(t, fixture, proposal)
			filter := fixture.Options.Filter
			for _, id := range append(slices.SortedFunc(maps.Keys(before), manifest.NamedResource.Compare), owner, testutil.KustomizationID("apps", "ready")) {
				selected[i] = append(selected[i], filter.ShouldReconcile(id))
			}
		}
		return selected
	}
	selected := selection(before)
	var execution *base.Execution
	o.tasks.Go(t.Context(), "entry history", func(ctx context.Context) {
		execution = o.controllers.ReconcileNode(ctx, owner, manifest.DrainNone)
	})
	o.tasks.BlockTillDone()
	assert.Equal(t, o.tasks.Failures(), int64(0))
	{
		assert.FatalIf(t, execution == nil, "%v", "entry reconciliation did not run")
	}
	defer execution.Close()
	status, ok := o.store.GetStatus(owner)
	{
		assert.FatalIf(t, !ok || status.Status != store.StatusReady, "entry emission failed: %+v", status)
	}
	_, _, emitted := o.store.GetEmission(owner)
	defer emitted.Finish()
	assert.Equal(t, o.store.GetArtifact(owner, store.ArtifactContent).(*store.Emission).Complete, true)
	after := readHistory()
	assert.Diff(t, after, before)
	assert.Diff(t, selection(after), selected)
}

type testBootstrapEntryReceiptDependencySubstitutionsCaseCaseInputs struct {
	testCaseInput **string
	root          *string
	cacheDir      *string
	input         **entryBuildFS
	lifetime      *string
}

func testBootstrapEntryReceiptDependencySubstitutionsCaseCase(
	t *testing.T,
	testCaseInput testBootstrapEntryReceiptDependencySubstitutionsCaseCaseInputs,
) {
	t.Helper()
	(*testCaseInput.input).builds.Store(0)
	o, err := newTestOrchestrator(
		t,
		Tree{
			Path:     filepath.Join((*testCaseInput.root), "entry"),
			RepoRoot: (*testCaseInput.root),
			Input:    &store.SourceArtifact{LocalPath: (*testCaseInput.root), FS: (*testCaseInput.input)},
		},
		Config{CacheDir: (*testCaseInput.cacheDir), Concurrency: 2},
	)
	assert.NoError(t, err)
	defer o.stop()
	if (*(*testCaseInput.testCaseInput)) == "independent" {
		o.store.AddObject(
			&manifest.Kustomization{Name: "prod-ready", Namespace: "control", Path: "./empty"},
		)
	}
	assert.NoError(t, o.bootstrap(t.Context()))
	o.configureControllers()
	if (*(*testCaseInput.testCaseInput)) == "independent" {
		assert.Equal(t, o.entryBinding.Producer, testutil.KustomizationID("apps", "owner"))
		owner := o.store.GetObject(o.entryBinding.Producer).(*manifest.Kustomization)
		assert.Equal(t, owner.DependsOn[0].Name, "prod-ready")
		assert.Equal(t, owner.DependsOn[0].Namespace, "control")
		assert.Equal(t, o.dependencySubstitutions.Resolve("${PREFIX}"), "prod-")
	} else {
		assert.Equal(t, o.dependencySubstitutions.Resolve("${VALUE}"), "frozen")
	}
	assert.Equal(t, (*testCaseInput.input).builds.Load() == 0, (*testCaseInput.lifetime) == "warm")
	owner := o.entryBinding.Producer
	var reads store.ReadSet
	reads.Init(o.store, owner, nil)
	defer reads.Retire()
	records, narrow := o.controllers.PreviousRender(t.Context(), o.store.GetObject(owner), &reads)
	{
		assert.FatalIf(
			t,
			records == nil || !narrow,
			"%v",
			"controller rejected the receipt discovery saved",
		)
	}
	found := false
	for id := range records {
		found = found || id == testutil.KustomizationID("apps", "child")
	}
	{
		assert.FatalIf(t, !found, "%v", "entry receipt lost its child inventory")
	}
	o.ksc.Trees.Close()
	reopened, err := sourcecache.New(t.Context(), (*testCaseInput.cacheDir))
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, reopened.Close()) })
	o.ksc.Trees = kustomize.NewTreeCache(reopened, &o.inputs, o.tasks)
	bootstrap := o.cfg.Input
	owners, err := reopened.ProducersForPaths(
		t.Context(),
		[]manifest.SourceEvidence{
			{
				Source: bootstrap.HistoryIdentity(manifest.BootstrapSourceID),
				Path:   "entry/kustomization.yaml",
			},
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(owners, owner), true)
	again, complete := o.controllers.PreviousRender(t.Context(), o.store.GetObject(owner), &reads)
	{
		assert.FatalIf(
			t,
			again == nil || !complete,
			"%v",
			"controller rejected the receipt after reopening persistent cache",
		)
	}
	assert.Diff(t, maps.Collect(again), maps.Collect(records))
	assert.NoError(t, o.run(t.Context()))
}

func testBuildChangeFilterPartialApplicationHistory(t *testing.T) {
	t.Helper()
	if reason := sourcecache.RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/factory.yaml", testutil.KustomizationYAML("factory", "apps", `  path: ./producers
  postBuild: {substituteFrom: [{kind: ConfigMap, name: supplied}]}
`))
	testutil.WriteFile(t, root, "flux/consumer.yaml", testutil.KustomizationYAML("consumer", "apps", `  path: ./workload
  postBuild: {substituteFrom: [{kind: ConfigMap, name: target}]}
`))
	testutil.WriteFile(t, root, "producers/kustomization.yaml", "resources: [applier.yaml, target.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"producers/applier.yaml",
		testutil.KustomizationYAML("applier", "apps", "  path: ./${ENTRY}\n"),
	)
	testutil.WriteFile(t, root, "producers/target.yaml", `apiVersion: v1
kind: ConfigMap
metadata: {name: target, namespace: apps}
data: {VALUE: rendered}
`)
	testutil.WriteFile(t, root, "workload/kustomization.yaml", "resources: [result.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"workload/result.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: result, namespace: apps}\ndata: {value: '${VALUE}'}\n",
	)
	tree := Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}
	cfg := Config{
		CacheDir:            t.TempDir(),
		Concurrency:         2,
		StrictSubstitutions: true,
		Supply: []store.Declaration{
			{
				Doc: testutil.ObjectDocument(&manifest.ConfigMap{
					Name: "supplied", Namespace: "apps", Data: map[string]any{"ENTRY": "flux"},
				}),
				Origin: manifest.SuppliedDeclaration,
			},
		},
	}
	warm, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	assert.NoError(t, warm.bootstrap(t.Context()))
	warm.configureControllers()
	owner := testutil.KustomizationID("apps", "factory")
	var reads store.ReadSet
	reads.Init(warm.store, owner, nil)
	prepared, err := kustomize.Prepare(
		warm.store.GetObject(owner).(*manifest.Kustomization),
		values.NewStoreProvider(&reads, false),
	)
	assert.NoError(t, err)
	source := reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact)
	request := kustomize.RenderRequest{
		Source:              source,
		ApplyIgnore:         true,
		SubPath:             prepared.Path,
		Spec:                prepared,
		StrictSubstitutions: true,
	}
	documents, receipt, err := kustomize.Render(t.Context(), warm.ksc.Trees, request, cfg.SourceRetry)
	assert.NoError(t, err)
	content := request.Content(documents)
	assert.NoError(t, kustomize.Shape(prepared, documents, true))
	children, _, failures := warm.ksc.ParseChildren(prepared, documents, manifest.ParseDocOptions{})
	assert.Equal(t, len(failures), 0)
	assert.NoError(
		t,
		warm.ksc.Trees.RecordRender(
			t.Context(),
			kustomize.RenderAcceptance{
				Owner:   owner,
				Receipt: receipt,
				Content: content,
				Records: warm.store.ChildFootprint(owner, children, warm.index),
			},
		),
	)
	reads.Retire()
	warm.stop()
	tree.ExternalChanges = change.NewSet([]string{"flux/consumer.yaml"})
	fresh, err := newTestOrchestrator(t, tree, cfg)
	assert.NoError(t, err)
	t.Cleanup(fresh.stop)
	result, err := fresh.render(t.Context())
	assert.NoError(t, err)
	assert.Equal(t, fresh.entryBinding.Producer, testutil.KustomizationID("apps", "applier"))
	assert.Equal(t, fresh.filter.ShouldReconcile(owner), true)
	assert.Diff(t, configMapNames(result), map[string]bool{"target": true, "result": true})
	consumer := outputByParent(result.Manifests)[testutil.KustomizationID("apps", "consumer")]
	assert.Equal(t, len(consumer), 1)
	assert.Diff(t, consumer[0]["data"], any(map[string]any{"value": "rendered"}))
}
