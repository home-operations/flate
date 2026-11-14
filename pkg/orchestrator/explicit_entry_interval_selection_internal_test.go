package orchestrator

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/selection"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testfixture"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The work observations replace process-global logging.
func TestRenderTrees_ExplicitEntryIntervalSelection(t *testing.T) {
	for _, kind := range []string{"fluxinstance", "kustomization"} {
		t.Run(kind, func(t *testing.T) {
			for _, mode := range []string{"cold", "warm", "workload", "invalidated"} {
				t.Run(mode, func(t *testing.T) {
					testExplicitEntrySelection(t, kind, mode)
				})
			}
		})
	}
}

//nolint:gocognit // Each selection mode MUST retain both resource sides and the independent two-run byte comparison.
func testExplicitEntrySelection(t *testing.T, kind, mode string) {
	t.Helper()
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	name := "entry-application-rendered-" + kind
	head := testfixture.Materialize(t, "entry-applicator", name)
	var oracle [2][]byte
	for range 2 {
		head.Restore(t)
		cache, err := sourcecache.New(t.Context(), t.TempDir())
		assert.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, cache.Close()) })
		selected := Config{Cache: cache, Concurrency: 2}
		tree := Tree{
			Path: filepath.Join(head.Repo, "kubernetes", "apps"), RepoRoot: head.Repo,
			SelfURLs: []string{"https://example.test/entry/repo"},
		}
		if mode != "cold" {
			prime, primeErr := newTestOrchestrator(t, tree, selected)
			assert.NoError(t, primeErr)
			_, primeErr = prime.render(t.Context(), t)
			assert.NoError(t, primeErr)
			assert.Equal(t, cache.CanPersistRenders(), persistent)
		}
		editExplicitEntry(t, head, mode)
		inputs, err := baseline.AutoResolve(
			t.Context(),
			baseline.Config{Network: networktest.New(t), Path: tree.Path, Base: "HEAD", Cache: cache},
		)
		assert.NoError(t, err)
		selected.Selection = selection.Request{Changed: mode != "invalidated"}
		var admissions [2]atomic.Int64
		generator := testutil.HelmReleaseID("apps", "generator")
		selected.StatusListener = func(id manifest.TreeID, subject manifest.DiagnosticIdentity, info store.StatusInfo) {
			if subject.ID == generator && info.String() == "rendering" {
				admissions[id.Side].Add(1)
			}
		}
		var logs bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		trees := []Tree{
			{
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Path:            inputs.BaseScanPath,
				SelfURLs:        inputs.SelfURLs,
				Input:           inputs.BaseInput,
				BaseInput:       inputs.Source,
				ExternalChanges: inputs.Changes,
			},
			{
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Path:            inputs.ScanPath,
				SelfURLs:        inputs.SelfURLs,
				Input:           inputs.Source,
				BaseInput:       inputs.BaseInput,
				ExternalChanges: inputs.Changes,
			},
		}
		if mode == "invalidated" {
			trees = trees[1:]
		}
		results, err := renderTreesForTest(t.Context(), t, trees, selected)
		slog.SetDefault(previous)
		assert.NoError(t, err)
		wantTemplates := 0
		if mode == "cold" || mode == "invalidated" {
			wantTemplates = 1
		}
		assert.Equal(t, entryReceiptTemplateCount(logs.String(), generator), wantTemplates)
		wantAdmissions := int64(1)
		if mode == "workload" {
			wantAdmissions = 0
		}
		for _, result := range results {
			assert.Equal(t, admissions[result.ID.Side].Load(), wantAdmissions)
		}
		output := assertExplicitEntryResults(t, results, mode, persistent)
		for side := range output {
			if oracle[side] == nil {
				oracle[side] = output[side]
			} else {
				assert.Equal(t, bytes.Equal(output[side], oracle[side]), true)
			}
		}
		assertExplicitEntryWork(t, mode, persistent, logs.String())
	}
}

func assertExplicitEntryResults(t *testing.T, results []Rendered, mode string, persistent bool) [2][]byte {
	t.Helper()
	var outputs [2][]byte
	entry := manifest.NewEntryKustomization(
		manifest.BootstrapSourceID, "kubernetes/apps", manifest.NamedResource{},
	).Named()
	for _, result := range results {
		side := int(result.ID.Side)
		output, err := json.Marshal(result.Result.Manifests)
		assert.NoError(t, err)
		outputs[side] = output
		names := configMapNames(result.Result)
		assert.Equal(t, names["unapplied"], false)
		if mode == "workload" || mode == "invalidated" {
			assert.Equal(t, names["workload"], true)
		}
		if mode == "invalidated" {
			assert.Equal(t, names["literal-layer"], true)
		}
		if mode == "workload" {
			builds := witnessWork(result.Result.Work, manifest.WorkKustomize)[entry]
			var wantBuilds uint64
			if persistent && result.ID.Side == manifest.TreeBase {
				wantBuilds = 1
			}
			assert.Equal(t, builds, wantBuilds)
		}
	}
	return outputs
}

func editExplicitEntry(t *testing.T, head *testfixture.Fixture, mode string) {
	t.Helper()
	for _, step := range head.Plan.Steps {
		if step.Edit == nil {
			continue
		}
		switch step.ID {
		case "workload-edit":
			if mode == "workload" || mode == "invalidated" {
				head.Edit(t, step)
			}
		case "interval-edit":
			if mode != "workload" {
				head.Edit(t, step)
			}
		case "application-path-edit":
			if mode == "invalidated" {
				head.Edit(t, step)
			}
		}
	}
}

func assertExplicitEntryWork(t *testing.T, mode string, persistent bool, logs string) {
	t.Helper()
	if mode == "workload" {
		assert.Equal(t, strings.Count(logs, `"msg":"discovery: entry application reused"`), 0)
	}
	var entryBuilds int
	for line := range strings.SplitSeq(logs, "\n") {
		var event struct {
			Msg  string `json:"msg"`
			Path string `json:"path"`
		}
		if json.Unmarshal([]byte(line), &event) == nil && event.Msg == "kustomize: build" {
			path := manifest.NormalizeClaimBase(event.Path)
			assert.Equal(t, path == "kubernetes/apps/unapplied", false)
			if path == "kubernetes/apps" {
				entryBuilds++
			}
		}
	}
	if mode == "workload" {
		wantBuilds := 0
		if persistent {
			wantBuilds = 1
		}
		assert.Equal(t, entryBuilds, wantBuilds)
	}
}
