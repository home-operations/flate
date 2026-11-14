package orchestrator

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/selection"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/opencontainers/go-digest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
	"github.com/home-operations/flate/pkg/store"
)

func warmGeneratedChartCache(t *testing.T, owner *sourcecache.Cache, network *source.Network, url string) {
	t.Helper()
	chartID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-selected",
	}
	repository := &manifest.HelmRepository{
		Name: "generated-charts", Namespace: "apps", URL: url, Interval: metav1.Duration{Duration: time.Hour},
	}
	hc := &manifest.HelmChartSource{
		Name: chartID.Name, Namespace: chartID.Namespace, Chart: "demo", Version: "1.0.0",
		SourceRef: sourcev1.LocalHelmChartSourceReference{Kind: manifest.KindHelmRepository, Name: repository.Name},
	}
	st := store.New()
	st.AddObject(repository)
	st.AddObject(hc)
	reads := new(store.ReadSet)
	reads.Init(st, chartID, nil)
	fetcher := helmchart.New(nil, helmchart.Config{Cache: owner, Network: network, Concurrency: 2})
	_, err := fetchtest.Fetch(t.Context(), fetcher.Fetch, hc, reads, &sourcecache.Policy{}, nil)
	assert.NoError(t, err)
}

func assertGeneratedChartRecords(t *testing.T, work manifest.WorkSummary, treeID manifest.TreeID) {
	t.Helper()
	hr := testutil.HelmReleaseID("apps", "selected")
	chartID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-selected",
	}
	for recordIndex := range work {
		record := &work[recordIndex]
		assert.Equal(t, record.Key.Tree, treeID)
		if record.Key.Owner == chartID && record.Key.Phase != manifest.WorkAdmission {
			assert.Equal(t, record.Key.Source, chartID)
		}
		if record.Key.Phase == manifest.WorkHelm || record.Key.Phase == manifest.WorkChartDecode {
			assert.Equal(t, record.Key.Owner, hr)
			assert.Equal(t, record.Key.Source, chartID)
			assert.Equal(t, record.Key.Input != "", true)
		}
		if record.Key.Phase == manifest.WorkEvaluation || record.Key.Phase == manifest.WorkKustomize {
			assert.Equal(t, record.Key.Source, manifest.BootstrapSourceID)
		}
		if record.Counts.RemoteTransfers != 0 {
			assert.Equal(t, record.Key.Owner, chartID)
			assert.Equal(t, record.Key.Source, chartID)
			assert.Equal(t, record.Key.Phase, manifest.WorkAcquisition)
		}
	}
}

func newGeneratedChartFixture(t *testing.T) generatedChartFixture {
	t.Helper()
	chart := testutil.TarGz(
		t,
		map[string]string{
			"demo/Chart.yaml": "apiVersion: v2\nname: demo\nversion: 1.0.0\n",

			"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: chosen-output, namespac" +
				"e: apps}\ndata: {payload: {{ .Values.payload | quote }}}\n",
		},
		0o644,
		gzip.DefaultCompression,
	)
	index := fmt.Sprintf(
		"apiVersion: v1\nentries:\n  demo:\n    - name: demo\n      version: 1.0.0"+"\n      digest: %s\n      urls: [demo.tgz]\n",
		digest.FromBytes(chart).Encoded(),
	)
	f := generatedChartFixture{chart: chart, index: index}
	serverResult := f.server(t)
	server := serverResult.server
	hr := testutil.HelmReleaseID("apps", "selected")
	var full [2][]map[string]any
	for side, value := range []string{"before", "after"} {
		tree := generatedChartTree(t, server.URL, value, manifest.TreeHead)
		runs, err := renderTreesForTest(t.Context(), t, []Tree{tree}, Config{RecordWork: true, Concurrency: 2})
		assert.NoError(t, err)
		full[side] = outputByParent(runs[0].Result.Manifests)[hr]
		assert.Equal(t, len(full[side]), 1)
		assert.Equal(
			t,
			manifest.DocIdentity(full[side][0]),
			manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "chosen-output"},
		)
		assert.Diff(t, full[side][0]["data"], map[string]any{"payload": value})
	}
	f.full = full
	return f
}

func generatedSourceSelectedWork(
	t *testing.T,
	result *Result,
	treeValue *Tree,
	sourceID manifest.NamedResource,
) manifest.WorkSummary {
	t.Helper()
	tree := *treeValue
	kind := sourceID.Kind
	var selected manifest.WorkSummary
	for recordIndex := range result.Work {
		record := &result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, tree.ID)
		if record.Key.Owner == sourceID && record.Key.Phase != manifest.WorkAdmission {
			assert.Equal(t, record.Key.Source, sourceID)
			isArtifactPreparation := kind == manifest.KindExternalArtifact &&
				record.Key.Phase == manifest.WorkPreparation
			if isArtifactPreparation && record.Counts.Failures == 0 {
				assert.Equal(t, record.Counts.Attempts, record.Counts.Completed)
				assert.Equal(t, record.Counts.OwnerPrepares, record.Counts.Attempts)
				record.Counts.Attempts, record.Counts.Completed, record.Counts.OwnerPrepares = 1, 1, 1
			}
			selected = append(selected, *record)
		}
	}
	return selected
}

type generatedChartFixture struct {
	chart []byte
	index string
	full  [2][]map[string]any
}

func assertGeneratedSourcePhases(
	t *testing.T,
	result *Result,
	sourceID manifest.NamedResource,
	persistent bool,
) {
	t.Helper()
	engines, evaluations := map[manifest.NamedResource]uint64{}, map[manifest.NamedResource]uint64{}
	acquisitions := map[manifest.NamedResource]manifest.WorkCounts{
		{}: {Attempts: 1, Completed: 1, LocalAcquisitions: 1},
	}
	caches := map[manifest.NamedResource]manifest.WorkCounts{}
	switch sourceID.Kind {
	case manifest.KindHelmChart:
		owner := testutil.KustomizationID("apps", "owner")
		engines[owner] = 1
		evaluations[manifest.NamedResource{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSet,
			Namespace: "apps",
			Name:      "selected",
		}] = 1
		caches[owner] = manifest.WorkCounts{Attempts: 4, Completed: 2, Failures: 2}
		if !persistent {
			caches[owner] = manifest.WorkCounts{Attempts: 1, Completed: 1}
		}
	case manifest.KindExternalArtifact:
		generator := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Namespace: "apps",
			Name:      "generator",
		}
		evaluations[generator] = 1
		acquisitions[generator] = manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1}
		caches[generator] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	case manifest.KindOCIRepository:
		acquisitions[sourceID] = manifest.WorkCounts{Attempts: 1, Completed: 1, RemoteTransfers: 1}
		caches[sourceID] = manifest.WorkCounts{Attempts: 1, Completed: 1}
	}
	assert.Diff(t, witnessWork(result.Work, manifest.WorkKustomize), engines)
	assert.Diff(t, witnessWork(result.Work, manifest.WorkEvaluation), evaluations)
	assert.Diff(t, witnessPhaseCounts(result.Work, manifest.WorkAcquisition), acquisitions)
	assert.Diff(t, witnessPhaseCounts(result.Work, manifest.WorkCache), caches)
}

type generatedChartServer struct {
	server    *httptest.Server
	indexHits *atomic.Uint64
	chartHits *atomic.Uint64
}

func (fr *generatedChartFixture) server(t *testing.T) generatedChartServer {
	t.Helper()
	f := *fr
	var indexHits, chartHits atomic.Uint64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/index.yaml":
			indexHits.Add(1)
			_, _ = io.WriteString(w, f.index)
		case "/demo.tgz":
			chartHits.Add(1)
			_, _ = w.Write(f.chart)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return generatedChartServer{server: server, indexHits: &indexHits, chartHits: &chartHits}
}

func (fr *generatedChartFixture) assertRuns(
	t *testing.T,
	runs []Rendered,
	tc generatedChartCase,
) generatedChartWitness {
	t.Helper()
	f := *fr
	hr := testutil.HelmReleaseID("apps", "selected")
	supplier := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "supplier",
	}
	unrelated := manifest.NamedResource{
		Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "unrelated",
	}
	chartID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-selected",
	}
	full := f.full
	var joint manifest.WorkSummary
	output := make([]map[string][]map[string]any, 0, len(runs))
	var acquisitionID string
	for side, run := range runs {
		result := run.Result
		assert.Diff(t, witnessWork(result.Work, manifest.WorkKustomize), map[manifest.NamedResource]uint64{{}: 1})
		evaluation, helm := map[manifest.NamedResource]uint64{}, map[manifest.NamedResource]uint64{}
		if tc.outcome == "success" {
			evaluation[supplier], helm[hr] = 1, 1
			wantSide := side
			if !tc.paired {
				wantSide = 1
			}
			assert.Diff(t, outputByParent(run.Result.Manifests)[hr], full[wantSide])
			assert.Equal(t, len(result.Failed), 0)
			assert.Equal(t, len(result.Blocked), 0)
			artifact, ok := run.Store().GetArtifact(chartID, store.ArtifactContent).(*store.SourceArtifact)
			assert.Equal(t, ok, true)
			assert.Equal(t, artifact.Key != "", true)
			if side == 0 {
				acquisitionID = artifact.Key
			} else {
				assert.Equal(t, artifact.Key, acquisitionID)
			}
		} else {
			assert.Equal(t, len(outputByParent(result.Manifests)[hr]), 0)

			assert.Equal(t, len(result.Failed), 0)
			assert.Equal(t, len(result.Blocked), 0)
			assert.Equal(t, len(result.Manifests), 0)
		}
		assert.Diff(t, witnessWork(result.Work, manifest.WorkEvaluation), evaluation)
		assert.Diff(t, witnessWork(result.Work, manifest.WorkHelm), helm)
		assert.Equal(
			t,
			workCounts(result.Work, manifest.NamedResource{}, manifest.WorkKustomize),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
		assert.Equal(t, workCounts(result.Work, hr, manifest.WorkPostRender), manifest.WorkCounts{})
		wantEngine := manifest.WorkCounts{}
		if tc.outcome == "success" {
			wantEngine = manifest.WorkCounts{Attempts: 1, Completed: 1}
		}
		assert.Equal(t, workCounts(result.Work, hr, manifest.WorkHelm), wantEngine)
		assert.Equal(t, workCounts(result.Work, supplier, manifest.WorkEvaluation), wantEngine)
		assert.Equal(t, workCounts(result.Work, unrelated, manifest.WorkEvaluation), manifest.WorkCounts{})
		assert.Equal(t, len(result.Warnings), 0)
		joint = append(joint, result.Work...)
		documents := map[string][]map[string]any{}
		for id, docs := range outputByParent(result.Manifests) {
			documents[id.String()] = docs
		}
		output = append(output, documents)
	}
	return generatedChartWitness{joint: joint, output: output, acquisitionID: acquisitionID, runs: len(runs)}
}

type generatedChartCase struct {
	name, outcome string
	roots         []manifest.NamedResource
	paired        bool
}

func TestRenderTrees_ResourceSetChartSourceWork(t *testing.T) {
	t.Parallel()
	fixture := newGeneratedChartFixture(t)
	hr := testutil.HelmReleaseID("apps", "selected")
	for _, tc := range []generatedChartCase{
		{name: "full-diff", outcome: "success", paired: true},
		{name: "named", outcome: "success", roots: []manifest.NamedResource{hr}},
		{name: "named-diff", outcome: "omitted-generated-edit", roots: []manifest.NamedResource{hr}, paired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, regime := range []string{"cold", "source-warm"} {
				t.Run(regime, func(t *testing.T) {
					t.Parallel()
					testGeneratedChartWork(t, &fixture, tc, regime)
				})
			}
		})
	}
}

type generatedChartWitness struct {
	joint         manifest.WorkSummary
	output        []map[string][]map[string]any
	acquisitionID string
	runs          int
}

func TestRenderTrees_GeneratedSourceSelectionWork(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindHelmChart, manifest.KindOCIRepository, manifest.KindExternalArtifact} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			persistent := sourcecache.RenderPersistenceDisabledReason() == ""
			tree, sourceID := generatedSourceWorkTree(t, kind)
			var previous manifest.WorkSummary
			for repeat := range 2 {
				invocation := prepareOrderingTrees(t, []Tree{tree}, &Config{
					RecordWork: true, Concurrency: 2, CacheDir: t.TempDir(),
					Selection: selection.Request{Roots: []manifest.NamedResource{sourceID}},
				})
				o := invocation.results[0].Orchestrator
				if kind == manifest.KindExternalArtifact {
					published := make(chan struct{})
					o.controllers.ArtifactGenerator = &orderedPublisherController{
						reconcileController: o.agc,
						publisher: manifest.NamedResource{
							Group:     "source.extensions.fluxcd.io",
							Kind:      "ArtifactGenerator",
							Namespace: "apps",
							Name:      "generator",
						},
						published: published,
						t:         t,
					}
					o.controllers.Source = &afterPublicationController{
						reconcileController: o.src, published: published, only: sourceID, tasks: o.tasks,
					}
				}
				invocation.renderTrees(t.Context())
				clusterAdvisories(invocation.results, invocation.stores)
				runs := invocation.results
				err := runs[0].Err
				t.Cleanup(runs[0].Result.Close)
				assert.NoError(t, err)
				if kind != manifest.KindHelmChart {
					assert.NoError(t, err)
				}
				assert.Equal(t, len(runs), 1)
				result := runs[0].Result
				assert.Equal(t, runs[0].Store().GetObject(sourceID) != nil, true)
				assert.Equal(
					t,
					runs[0].Store().GetArtifact(sourceID, store.ArtifactContent) != nil,
					kind != manifest.KindHelmChart,
				)
				assert.Diff(t, witnessWork(result.Work, manifest.WorkHelm), map[manifest.NamedResource]uint64{})
				assertGeneratedSourcePhases(t, result, sourceID, persistent)
				assertGeneratedSourcePreparation(t, runs[0], &tree, sourceID)
				selected := generatedSourceSelectedWork(t, runs[0].Result, &tree, sourceID)
				if repeat == 0 {
					previous = selected
				} else {
					assert.Diff(t, selected, previous)
				}
			}
		})
	}
}

func assertGeneratedChartSelection(t *testing.T, runs []Rendered, full [2][]map[string]any, paired bool) {
	t.Helper()
	hr := testutil.HelmReleaseID("apps", "selected")
	projectedResults0, projectionErrors0 := Select(runs, nil, []manifest.NamedResource{hr}, nil)
	selected, selectionErr, remainder := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
	assert.NoError(t, selectionErr)
	assert.NoError(t, remainder)
	for side := range selected {
		wantSide := side
		if !paired {
			wantSide = 1
		}
		assert.Diff(
			t,
			outputByParent(selected[side].Manifests),
			map[manifest.NamedResource][]map[string]any{hr: full[wantSide]},
		)
		assert.Equal(t, len(selected[side].Objects), 1)
		assert.Equal(t, selected[side].Objects[0].Named(), hr)
	}
}

func (tc generatedChartCase) assertTransfers(t *testing.T, w generatedChartWitness, regime string) uint64 {
	t.Helper()
	hr := testutil.HelmReleaseID("apps", "selected")
	chartID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindHelmChart,
		Namespace: "apps",
		Name:      "apps-selected",
	}
	joint, acquisitionID := w.joint, w.acquisitionID
	wantAcquisition, wantCache, wantDecode := manifest.WorkCounts{}, manifest.WorkCounts{}, manifest.WorkCounts{}
	var transfers uint64
	if tc.outcome == "success" {
		wantCache = manifest.WorkCounts{
			Attempts:     uint64(w.runs),
			Completed:    uint64(w.runs),
			CacheService: uint64(w.runs),
		}
		if regime == "cold" {
			wantAcquisition = manifest.WorkCounts{Attempts: 2, Completed: 2, RemoteTransfers: 2}
			wantCache = manifest.WorkCounts{
				Attempts:     uint64(w.runs + 1),
				Completed:    uint64(w.runs + 1),
				CacheService: uint64(w.runs - 1),
			}
			wantDecode = manifest.WorkCounts{Attempts: 1, Completed: 1}
			transfers = 1
		}
	}
	assert.Equal(t, workCounts(joint, chartID, manifest.WorkAcquisition), wantAcquisition)
	assert.Equal(t, workCounts(joint, chartID, manifest.WorkCache), wantCache)
	assert.Equal(t, workCounts(joint, chartID, manifest.WorkIndexDecode), wantDecode)
	acquired := map[string]uint64{}
	for recordIndex := range joint {
		record := &joint[recordIndex]
		if record.Key.Owner != chartID {
			continue
		}
		if record.Key.Phase == manifest.WorkAcquisition {
			assert.Equal(t, record.Key.Input != "", true)
			assert.Equal(t, record.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1, RemoteTransfers: 1})
			acquired[record.Key.Input] += record.Counts.Attempts
		}
		if record.Key.Phase == manifest.WorkCache && record.Counts.CacheService != 0 {
			assert.Equal(t, record.Key.Input, acquisitionID)
		}
	}
	wantChartDecode := manifest.WorkCounts{}
	if tc.outcome == "success" {
		wantChartDecode = manifest.WorkCounts{Attempts: 1, Completed: 1, PeakConcurrent: 1}
	}
	assert.Equal(t, workCounts(joint, hr, manifest.WorkChartDecode), wantChartDecode)
	if transfers != 0 {
		assert.Equal(t, len(acquired), 2)
		assert.Equal(t, acquired[acquisitionID], uint64(1))
	}
	return transfers
}

func assertGeneratedSourcePreparation(t *testing.T, run Rendered, treeValue *Tree, sourceID manifest.NamedResource) {
	t.Helper()
	tree := *treeValue
	kind := sourceID.Kind
	result := run.Result
	preparation := manifest.WorkCounts{Attempts: 1, Completed: 1, OwnerPrepares: 1}
	acquisition, cache := manifest.WorkCounts{}, manifest.WorkCounts{}
	switch kind {
	case manifest.KindExternalArtifact:
		generator := manifest.NamedResource{
			Group:     "source.extensions.fluxcd.io",
			Kind:      "ArtifactGenerator",
			Namespace: "apps",
			Name:      "generator",
		}
		assert.Equal(t, run.Store().EmissionOwner(sourceID), generator)
		assert.Equal(
			t,
			workCounts(result.Work, generator, manifest.WorkAcquisition),
			manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1},
		)
		assert.Equal(
			t,
			workCounts(result.Work, generator, manifest.WorkCache),
			manifest.WorkCounts{Attempts: 1, Completed: 1},
		)
	case manifest.KindOCIRepository:
		preparation.RawKeyPrepares = 1
		acquisition = manifest.WorkCounts{Attempts: 1, Completed: 1, RemoteTransfers: 1}
		cache = manifest.WorkCounts{Attempts: 1, Completed: 1}
	}
	gotPreparation := workCounts(result.Work, sourceID, manifest.WorkPreparation)
	key := manifest.WorkKey{Tree: tree.ID, Owner: sourceID, Source: sourceID, Phase: manifest.WorkPreparation}
	assertWitnessPreparations(
		t,
		map[manifest.WorkKey]manifest.WorkCounts{key: gotPreparation},
		map[manifest.WorkKey]manifest.WorkCounts{key: preparation},
	)
	assert.Equal(t, workCounts(result.Work, sourceID, manifest.WorkAcquisition), acquisition)
	assert.Equal(t, workCounts(result.Work, sourceID, manifest.WorkCache), cache)
}

func testGeneratedChartWork(t *testing.T, fixtureValue *generatedChartFixture, tc generatedChartCase, regime string) {
	t.Helper()
	fixture := *fixtureValue
	t.Logf("requirements=REQ-003,REQ-005,REQ-010 scenarios=G03,G04,V03 current=%s", tc.outcome)
	serverResult2 := fixture.server(t)
	server, indexHits, chartHits := serverResult2.server, serverResult2.indexHits, serverResult2.chartHits
	var previous []byte
	for repeat := range 2 {
		owner := cachetest.New(t, "", sourcecache.New)
		network := new(source.Network)
		t.Cleanup(network.Close)
		if regime == "source-warm" {
			warmGeneratedChartCache(t, owner, network, server.URL)
		}
		trees := []Tree{
			generatedChartTree(t, server.URL, "before", manifest.TreeBase),
			generatedChartTree(t, server.URL, "after", manifest.TreeHead),
		}
		if !tc.paired {
			trees = trees[1:]
		}
		beforeIndex, beforeChart := indexHits.Load(), chartHits.Load()
		runs, err := renderTreesForTest(t.Context(), t, trees, Config{
			RecordWork: true, Concurrency: 2, Cache: owner, Network: network,
			Selection: selection.Request{Roots: tc.roots},
		})
		assert.NoError(t, err)
		if tc.outcome != "missing-generated-inputs" {
			assert.NoError(t, err)
		}
		assert.Equal(t, len(runs), len(trees))
		for side, run := range runs {
			assertGeneratedChartRecords(t, run.Result.Work, trees[side].ID)
		}
		witness := fixture.assertRuns(t, runs, tc)
		if tc.outcome == "success" {
			assertGeneratedChartSelection(t, runs, fixture.full, tc.paired)
		}
		transfers := tc.assertTransfers(t, witness, regime)
		assert.Equal(t, indexHits.Load()-beforeIndex, transfers)
		assert.Equal(t, chartHits.Load()-beforeChart, transfers)
		body, err := json.Marshal(witness.output)
		assert.NoError(t, err)
		if repeat == 0 {
			previous = body
		} else {
			assert.Equal(t, string(body), string(previous))
		}
	}
}

func generatedSourceWorkTree(t *testing.T, kind string) (Tree, manifest.NamedResource) {
	t.Helper()
	var tree Tree
	var sourceID manifest.NamedResource
	switch kind {
	case manifest.KindHelmChart:
		tree = crdWorkTree(t, true, false)
		sourceID = manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      kind,
			Namespace: "apps",
			Name:      "generated",
		}
	case manifest.KindExternalArtifact:
		tree = artifactWorkTree(t, "old", manifest.TreeHead)
		sourceID = manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      kind,
			Namespace: "apps",
			Name:      "generated",
		}
	case manifest.KindOCIRepository:
		registry := testutil.NewChartRegistry(t, []map[string]string{inlineChartFiles(t)}, false)
		inlineChartRepositoryResult := inlineChartRepository(t, registry.URL, nil)
		root, release := inlineChartRepositoryResult.root, inlineChartRepositoryResult.document
		spec := release["spec"].(map[string]any)
		sourceSpec := manifest.DeepCopyMap(spec["chart"].(map[string]any)["spec"].(map[string]any))
		sourceSpec["layerSelector"] = map[string]any{
			"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			"operation": "copy",
		}
		sourceID = manifest.NamedResource{Group: manifest.SourceDomain, Kind: kind, Namespace: "apps", Name: "charts"}
		body, err := yaml.Marshal(
			map[string]any{
				"apiVersion": "source.toolkit.fluxcd.io/v1",
				"kind":       kind,
				"metadata":   map[string]any{"name": sourceID.Name, "namespace": sourceID.Namespace},
				"spec":       sourceSpec,
			},
		)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "apps/source.yaml", string(body))
		delete(spec, "chart")
		spec["chartRef"] = map[string]any{"kind": kind, "name": sourceID.Name}
		body, err = yaml.Marshal(release)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "apps/release.yaml", string(body))
		testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [release.yaml, source.yaml]\n")
		tree = Tree{
			ID:       manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
			RepoRoot: root,
			Path:     filepath.Join(root, "cluster"),
		}
	}
	return tree, sourceID
}

func generatedChartTree(t *testing.T, url, value string, side manifest.TreeSide) Tree {
	t.Helper()
	root := t.TempDir()
	testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [release.yaml, supplier.yaml, unrelated.yaml]\n")
	testutil.WriteFile(t, root, "flux/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: selected, namespace: apps}
spec:
  interval: 1h
  releaseName: chosen
  chart:
    spec:
      chart: demo
      version: 1.0.0
      sourceRef: {kind: HelmRepository, name: generated-charts}
  valuesFrom: [{kind: ConfigMap, name: generated-values}]
`)
	testutil.WriteFile(t, root, "flux/supplier.yaml", fmt.Sprintf(`apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: supplier, namespace: apps}
spec:
  inputs: [{value: %s}]
  resources:
    - apiVersion: source.toolkit.fluxcd.io/v1
      kind: HelmRepository
      metadata: {name: generated-charts, namespace: apps}
      spec: {url: %s, interval: 1h}
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: generated-values, namespace: apps}
      data:
        values.yaml: |
          payload: << inputs.value >>
`, value, url))
	testutil.WriteFile(t, root, "flux/unrelated.yaml", `apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSet
metadata: {name: unrelated, namespace: apps}
spec:
  resources:
    - apiVersion: v1
      kind: ConfigMap
      metadata: {name: unrelated-output, namespace: apps}
`)
	return Tree{ID: manifest.TreeID{Cluster: "cluster", Side: side}, Path: filepath.Join(root, "flux"), RepoRoot: root}
}
