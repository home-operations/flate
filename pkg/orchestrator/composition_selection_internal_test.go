package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	gogit "github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/file"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	sourcectrl "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/depwait"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type includeClosureCase struct {
	name, prior, baseOutput, headOutput, baseValue, headValue, baseFile, headFile string
	filtered                                                                      bool
}

type includeClosureExpectation struct {
	value, output, selectedFile string
}

func (tc *includeClosureCase) expectation(side manifest.TreeSide) includeClosureExpectation {
	expected := includeClosureExpectation{value: tc.baseValue, output: tc.baseOutput, selectedFile: tc.baseFile}
	if side == manifest.TreeHead {
		expected = includeClosureExpectation{value: tc.headValue, output: tc.headOutput, selectedFile: tc.headFile}
	}
	if expected.selectedFile == "" {
		expected.selectedFile = "backup-values.yaml"
	}
	return expected
}

//nolint:paralleltest // RenderTrees changes process-global source policy.
func TestChangedOnly_IndexedIncludeClosure(t *testing.T) {
	for _, tc := range []includeClosureCase{
		{name: "independent-diff", prior: "current-before", baseOutput: "before", headOutput: "after", baseValue: "before", headValue: "after"},
		{name: "outside-copied-path", prior: "current-before", baseOutput: "before", headOutput: "before", baseValue: "before", headValue: "before", filtered: true},
		{name: "include-added", prior: "current-noinclude", baseOutput: "no-include", headOutput: "before", headValue: "before"},
		{name: "include-removed", prior: "current-before", baseOutput: "before", headOutput: "no-include", baseValue: "before"},
		{name: "include-destination", prior: "current-destination-before", baseOutput: "before", headOutput: "destination", baseValue: "before", headValue: "before", baseFile: "previous.yaml", headFile: "renamed.yaml"},
		{name: "include-reordered", prior: "current-order-before", baseOutput: "before", headOutput: "before", baseValue: "alternate", headValue: "before"},
		{name: "shallow-authored-pin", prior: "current-pin-before", baseOutput: "before", headOutput: "after", baseValue: "before", headValue: "after"},
		{name: "nested-includes", prior: "current-nested-before", baseOutput: "before", headOutput: "after", baseValue: "before", headValue: "after"},
		{name: "filtered-input-change", prior: "current-filtered-before", baseOutput: "no-include", headOutput: "no-include", filtered: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, route := range []string{"path-orig", "base"} {
				t.Run(route, func(t *testing.T) { testChangedOnlyIncludeClosure(t, &tc, route) })
			}
		})
	}
}

func testChangedOnlyIncludeClosure(t *testing.T, tc *includeClosureCase, route string) {
	t.Helper()
	fixture := testfixture.Materialize(t, "artifact-composition", tc.name)
	owner := cachetest.New(t, "", cache.New)
	network := networktest.New(t)
	server := file.NewTransport(file.Options{Loader: fixture})
	transports := sourcegit.Transports{
		"git": func(ctx context.Context, request sourcegit.TransportRequest) (sourcegit.Connection, error) {
			return server.Connect(
				ctx,
				&transport.Request{
					URL:      request.URL,
					Command:  request.Command,
					Protocol: protocol.Version(request.Protocol),
				},
			)
		},
	}
	root := fixture.Repo
	original := fixture.Resolve(t, "@@baseline@@", "")
	trees := []Tree{
		{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
			RepoRoot: original,
			Path:     filepath.Join(original, "entry"),
			SelfURLs: []string{"git://cluster.fixture.invalid/repo"},
		},
		{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			RepoRoot: root,
			Path:     filepath.Join(root, "entry"),
			SelfURLs: []string{"git://cluster.fixture.invalid/repo"},
		},
	}
	if route == "base" {
		admitted, err := baseline.AutoResolve(
			t.Context(),
			baseline.Config{
				Path:          trees[1].Path,
				Base:          fixture.Resolve(t, "@@commit:"+tc.prior+"@@", ""),
				Cache:         owner,
				Network:       network,
				GitTransports: transports,
			},
		)
		assert.NoError(t, err)
		trees[0].Input, trees[0].Path, trees[0].RepoRoot = admitted.BaseInput, admitted.BaseScanPath, admitted.BaseInput.LocalPath
		trees[1].Input, trees[1].Path, trees[1].RepoRoot = admitted.Source, admitted.ScanPath, admitted.Source.LocalPath
		trees[0].ExternalChanges, trees[1].ExternalChanges = admitted.Changes, admitted.Changes
	}
	before, beforeOriginal := includeSnapshot(t, root), includeSnapshot(t, original)
	results, err := RenderTrees(
		t.Context(),
		trees,
		Config{
			Cache:         owner,
			Network:       network,
			Concurrency:   2,
			Selection:     selection.Request{Changed: true},
			missing:       depwait.NewMissingPolicy(nil),
			SourceRetry:   cache.RetryConfig{Attempts: 1},
			GitTransports: transports,
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, len(results), 2)
	for i := range results {
		testIncludeClosureResult(t, fixture, tc, &results[i])
	}

	assert.Diff(t, includeSnapshot(t, root), before)
	assert.Diff(t, includeSnapshot(t, original), beforeOriginal)
}

func testIncludeClosureResult(t *testing.T, fixture *testfixture.Fixture, tc *includeClosureCase, result *Rendered) {
	t.Helper()
	input := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "input",
	}
	including := input
	including.Name = "cluster"
	consumer, unrelated := testutil.KustomizationID(
		"apps",
		"consumer",
	), testutil.KustomizationID(
		"apps",
		"unrelated",
	)
	assert.NoError(t, result.Err)
	assert.FatalIf(
		t,
		result.Orchestrator == nil || result.Result == nil,
		"missing rendered side %v",
		result.ID,
	)
	t.Cleanup(result.Result.Close)
	decision := result.selection.Snapshot()
	assert.Equal(t, result.index.IsFileIndexed(input), true)
	assert.Equal(t, result.Store().GetDeclaration(input) != nil, true)
	expectation := tc.expectation(result.ID.Side)
	hasInclude := expectation.value != ""
	assert.Equal(t, decision.Reconcile(input), hasInclude || tc.filtered)
	assert.Equal(t, decision.Reconcile(including), true)
	assert.Equal(t, decision.Reconcile(consumer), true)
	assert.Equal(t, decision.Reconcile(unrelated), false)
	artifact, ok := result.Store().GetArtifact(including, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok, "missing completed source %v", including)
	assert.Equal(t, artifact.InputAlias, !hasInclude && !tc.filtered)
	assert.Equal(t, artifact.Reads.Valid(), true)

	value, selectedFile := expectation.value, expectation.selectedFile
	selected := "kubernetes/components/cnpg-common/" + selectedFile
	path := filepath.Join(artifact.LocalPath, filepath.FromSlash(selected))
	if hasInclude || tc.filtered {
		assert.Equal(t, artifact.OriginPath(path), path)
	}
	data, readErr := fs.ReadFile(artifact.FS, selected)
	if value != "" {
		assert.NoError(t, readErr)
		assert.Equal(
			t,
			string(data),
			fmt.Sprintf(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: backup-values, namespace: apps}\ndata: {value: %s}\n",
				value,
			),
		)
	} else {
		assert.Equal(t, errors.Is(readErr, fs.ErrNotExist), true)
	}
	assertIncludeClosureDocuments(t, fixture, result, expectation)
	if tc.name == "independent-diff" {
		assertIncludeAcceptedHistory(t, result, artifact, expectation)
	}
}

func assertIncludeClosureDocuments(
	t *testing.T,
	fixture *testfixture.Fixture,
	result *Rendered,
	expectation includeClosureExpectation,
) {
	t.Helper()
	value, expected, selectedFile := expectation.value, expectation.output, expectation.selectedFile
	var documents []map[string]any
	for _, batch := range result.Result.Manifests {
		for _, document := range batch.Documents {
			if manifest.DocKind(document) == manifest.KindConfigMap {
				documents = append(documents, document)
			}
		}
	}
	slices.SortFunc(documents, func(a, b map[string]any) int {
		return manifest.DocIdentity(a).Compare(manifest.DocIdentity(b))
	})
	var expectedBytes string
	if expected != "" {
		expectedBytes = fixture.Read(t, "expect/"+expected+".stdout", "")
	}
	if value == "alternate" {
		expectedBytes = strings.ReplaceAll(expectedBytes, "value: before", "value: alternate")
	}
	if selectedFile == "previous.yaml" {
		expectedBytes = strings.ReplaceAll(
			expectedBytes,
			"cnpg-common/backup-values.yaml",
			"cnpg-common/previous.yaml",
		)
	}
	parsed, parseErr := manifest.DecodeDocs(
		strings.NewReader(expectedBytes),
		manifest.RequiredDocuments,
	)
	assert.NoError(t, parseErr)
	want := make([]map[string]any, len(parsed))
	for i := range parsed {
		want[i] = parsed[i].Document
	}
	assert.Diff(t, documents, want)
}

func assertIncludeAcceptedHistory(
	t *testing.T,
	result *Rendered,
	artifact *store.SourceArtifact,
	expectation includeClosureExpectation,
) {
	t.Helper()
	input := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "input",
	}
	including := input
	including.Name = "cluster"
	consumer := testutil.KustomizationID("apps", "consumer")
	value := expectation.value
	selected := "kubernetes/components/cnpg-common/" + expectation.selectedFile
	previous, ok := result.Store().GetArtifact(consumer, store.ArtifactContent).(*store.Emission)
	assert.FatalIf(t, !ok || !previous.Complete, "consumer has no complete accepted emission")
	var retained store.ReadSet
	retained.Init(result.Store(), consumer, nil)
	defer retained.Retire()
	_, _, history := retained.GetEmission(consumer)
	defer history.Finish()
	history.Complete = previous.Complete
	result.Store().RecordEmitted(consumer, &history, nil)
	assert.Equal(t, history.Footprint() != nil, true)
	paths := slices.Clone(history.SourcePaths())
	family := artifact.HistoryIdentity(including)
	assert.Equal(
		t,
		slices.ContainsFunc(
			paths,
			func(path manifest.SourceEvidence) bool { return path.Source == family },
		),
		true,
	)
	previousInput := result.Store().GetArtifact(input, store.ArtifactContent).(*store.SourceArtifact)
	replacement := *previousInput
	replacement.Content += "-input-generation"
	result.Store().SetArtifact(input, &replacement)
	var refreshed *base.Execution
	result.tasks.Go(t.Context(), "refresh included generation", func(ctx context.Context) {
		refreshed = result.src.ReconcileNode(
			ctx,
			including,
			manifest.DrainNone,
			schedule.Result{},
		)
	})
	result.tasks.BlockTillDone()
	assert.Equal(t, refreshed.Outcome(), schedule.Terminal)
	defer refreshed.Close()
	next := result.Store().GetArtifact(including, store.ArtifactContent).(*store.SourceArtifact)
	assert.Equal(t, next.Key == artifact.Key, false)
	assert.Equal(t, next.HistoryIdentity(including), family)
	assert.Equal(t, history.SourcePaths() == nil, true)
	var rerendered *base.Execution
	result.tasks.Go(t.Context(), "refresh consumer", func(ctx context.Context) {
		rerendered = result.ksc.ReconcileNode(
			ctx,
			consumer,
			manifest.DrainNone,
			schedule.Result{},
		)
	})
	result.tasks.BlockTillDone()
	assert.Equal(t, rerendered.Outcome(), schedule.Terminal)
	defer rerendered.Close()
	current := result.Store().GetArtifact(consumer, store.ArtifactContent).(*store.Emission)
	var rebound store.ReadSet
	rebound.Init(result.Store(), consumer, nil)
	defer rebound.Retire()
	_, _, accepted := rebound.GetEmission(consumer)
	defer accepted.Finish()
	accepted.Complete = current.Complete
	result.Store().RecordEmitted(consumer, &accepted, nil)
	assert.Diff(t, accepted.SourcePaths(), paths)
	data, err := fs.ReadFile(next.FS, selected)
	assert.NoError(t, err)
	assert.Equal(
		t,
		string(data),
		fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: backup-values, namespace: apps}\ndata: {value: %s}\n",
			value,
		),
	)
}

//nolint:paralleltest // Discovery changes process-global source policy.
func TestChangedOnly_IncludeAdmissionBeforePruning(t *testing.T) {
	for _, scenario := range []string{"independent-diff", "shallow-authored-pin"} {
		t.Run(scenario, func(t *testing.T) {
			for _, route := range []string{"path-orig", "base"} {
				t.Run(route, func(t *testing.T) { testIncludeAdmissionBeforePruning(t, scenario, route) })
			}
		})
	}
}

func testIncludeAdmissionBeforePruning(t *testing.T, scenario, route string) {
	t.Helper()
	fixture := testfixture.Materialize(t, "artifact-composition", scenario)
	root, original := fixture.Repo, fixture.Resolve(t, "@@baseline@@", "")
	var baseCommit string
	for _, path := range []string{root, original} {
		declaration, err := os.ReadFile(filepath.Join(path, "entry", "input.yaml"))
		assert.NoError(t, err)
		testutil.WriteFile(
			t,
			path,
			"declarations/input.yaml",
			strings.ReplaceAll(string(declaration), "tag: v2", "tag: v1"),
		)
		assert.NoError(t, os.Remove(filepath.Join(path, "entry", "input.yaml")))
		testutil.WriteFile(
			t,
			path,
			"entry/kustomization.yaml",
			"resources: [cluster.yaml, consumer.yaml, unrelated.yaml]\n",
		)
		repository, err := gogit.PlainOpen(path)
		assert.NoError(t, err)
		options := &gogit.CommitOptions{
			Author: &object.Signature{
				Name:  "fixture",
				Email: "fixture@example.invalid",
				When:  time.Unix(0, 0),
			},
		}
		before := testutil.GitCommitAll(t, repository, "indexed include before", options)
		if path == root {
			baseCommit = before.String()
			testutil.WriteFile(t, path, "declarations/input.yaml", string(declaration))
			testutil.GitCommitAll(t, repository, "indexed include after", options)
		}
	}
	owner := cachetest.New(t, "", cache.New)
	server := file.NewTransport(file.Options{Loader: fixture})
	cfg := Config{
		Cache:       owner,
		Network:     networktest.New(t),
		Concurrency: 2,
		missing:     depwait.NewMissingPolicy(nil),
		GitTransports: sourcegit.Transports{
			"git": func(ctx context.Context, request sourcegit.TransportRequest) (sourcegit.Connection, error) {
				return server.Connect(
					ctx,
					&transport.Request{
						URL:      request.URL,
						Command:  request.Command,
						Protocol: protocol.Version(request.Protocol),
					},
				)
			},
		},
	}
	trees := []Tree{
		{
			ID:       manifest.TreeID{Side: manifest.TreeBase},
			RepoRoot: original,
			Path:     filepath.Join(original, "entry"),
			SelfURLs: []string{"git://cluster.fixture.invalid/repo"},
		},
		{
			ID:       manifest.TreeID{Side: manifest.TreeHead},
			RepoRoot: root,
			Path:     filepath.Join(root, "entry"),
			SelfURLs: []string{"git://cluster.fixture.invalid/repo"},
		},
	}
	if route == "base" {
		admitted, err := baseline.AutoResolve(
			t.Context(),
			baseline.Config{
				Path:          trees[1].Path,
				Base:          baseCommit,
				Cache:         owner,
				Network:       cfg.Network,
				GitTransports: cfg.GitTransports,
			},
		)
		assert.NoError(t, err)
		trees[0].Input, trees[0].Path, trees[0].RepoRoot = admitted.BaseInput, admitted.BaseScanPath, admitted.BaseInput.LocalPath
		trees[1].Input, trees[1].Path, trees[1].RepoRoot = admitted.Source, admitted.ScanPath, admitted.Source.LocalPath
	}
	for _, tree := range trees {
		t.Run(fmt.Sprint(tree.ID.Side), func(t *testing.T) {
			testIncludeAdmissionSide(t, &tree, &cfg)
		})
	}
}

func testIncludeAdmissionSide(t *testing.T, tree *Tree, cfg *Config) {
	t.Helper()
	value := "before"
	if tree.ID.Side == manifest.TreeHead {
		value = "after"
	}
	o, err := newTestOrchestrator(t, *tree, *cfg)
	assert.NoError(t, err)

	o.cfg.Selection = selection.Request{Changed: true}
	o.cfg.ExternalChanges = change.NewSet([]string{"declarations/input.yaml"})
	o.store.SetArtifact(manifest.BootstrapSourceID, o.cfg.Input)
	o.loader.SourceRoot = o.cfg.Input.LocalPath
	session, err := selection.New(o.store, o.loader, o.cfg.Selection)
	assert.NoError(t, err)
	o.loader.Options.Session = session
	_, err = o.loader.Load(t.Context(), o.cfg.Input.LocalPath, manifest.NamedResource{}, nil)
	assert.NoError(t, err)
	assert.NoError(t, o.loader.FinalizeIndex(t.Context(), o.cfg.Input.LocalPath, false))
	input := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindGitRepository,
		Namespace: "apps",
		Name:      "input",
	}
	assert.Equal(t, o.store.GetDeclaration(input), manifest.BaseManifest(nil))
	assert.Equal(t, o.loader.IsFileIndexed(input), true)
	assert.NoError(t, o.discover(t.Context()))
	including := input
	including.Name = "cluster"
	assert.Equal(t, o.loader.IsFileIndexed(input), true)
	record, indexed := o.index.Lookup(input)
	assert.Equal(t, indexed, true)
	assert.Equal(t, record.File, "declarations/input.yaml")
	assert.Equal(t, o.store.GetDeclaration(input) != nil, true)
	artifact, ok := o.store.GetArtifact(including, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok, "completed source unavailable before selection pruning")
	assert.Equal(t, artifact.Reads.Valid(), true)
	data, err := fs.ReadFile(
		artifact.FS,
		"kubernetes/components/cnpg-common/backup-values.yaml",
	)
	assert.NoError(t, err)
	assert.Equal(
		t,
		string(data),
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: backup-values, namespace: apps}\ndata: {value: "+value+"}\n",
	)
	assert.NoError(t, o.prepareBootstrap(t.Context()))
	snapshot := o.selection.Snapshot()
	assert.Equal(t, snapshot.Reconcile(input), true)
	assert.Equal(t, snapshot.Reconcile(including), true)
	assert.Equal(t, snapshot.Reconcile(testutil.KustomizationID("apps", "consumer")), true)
}

func TestConstruction_RequiredDeclarationPromotion(t *testing.T) {
	t.Parallel()
	st := store.New()
	including := &manifest.GitRepository{
		Name:      "including",
		Namespace: "ns",
		Include:   []sourcev1.GitRepositoryInclude{{GitRepositoryRef: manifest.LocalObjectReference{Name: "input"}}},
	}
	input := &manifest.GitRepository{Name: "input", Namespace: "ns", URL: "https://input.invalid/repo"}
	st.AddObject(including)
	st.AddObject(input)
	root := t.TempDir()
	session, err := discovery.Begin(
		t.Context(),
		discovery.Config{
			Store:    st,
			Path:     root,
			RepoRoot: root,
			Input:    &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}},
		},
	)
	assert.NoError(t, err)
	defer session.Close()
	tasks := task.NewBounded(2)
	controller := sourcectrl.New(st, tasks, cachetest.New(t, "", cache.New))
	controller.Configure(sourcectrl.FetchOptions{Admission: session.Admission()}, cache.RetryConfig{})
	assert.NoError(t, reconcileConstruction(t.Context(), session.PendingSources(), controller, tasks))
	st.AddDeclaration(store.Declaration{Obj: including, Origin: manifest.SuppliedDeclaration})
	st.AddDeclaration(store.Declaration{Obj: input, Origin: manifest.SuppliedDeclaration})
	e := orchestratorExistence{o: &Orchestrator{store: st, admission: session.Admission()}, construction: true}
	var reads store.ReadSet
	reads.Init(st, including.Named(), nil)
	defer reads.Retire()
	accepted, err := e.promoteAdmission(t.Context(), selection.PrepareRequest{Reads: &reads}, input.Named())
	assert.NoError(t, err)
	assert.Equal(t, accepted, true)
	assert.Equal(t, st.GetDeclaration(input.Named()), manifest.BaseManifest(input))
	assert.Equal(t, st.GetArtifact(input.Named(), store.ArtifactContent), store.Artifact(nil))
}
