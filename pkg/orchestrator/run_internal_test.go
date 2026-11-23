package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/object"

	enginesource "github.com/home-operations/flate/internal/engine/source"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/source/ssrfguard"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_RestrictEgress(t *testing.T) {
	t.Cleanup(func() { ssrfguard.Restrict(false) })
	const value = "remote-sdk-value"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprint(w,
			"apiVersion: v1\nkind: ConfigMap\n"+
				"metadata: {name: output, namespace: apps}\ndata: {value: "+value+"}\n",
		)
	}))
	t.Cleanup(server.Close)
	root := t.TempDir()
	writeRunCluster(t, root, "", "local")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: ["+server.URL+"/remote.yaml]\n")
	tree := Tree{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}, RepoRoot: root}

	blocked, err := RenderTrees(t.Context(), []Tree{tree}, Config{
		Concurrency: 2, CacheDir: t.TempDir(), RestrictEgress: true,
	})
	t.Cleanup(func() { assert.NoError(t, blocked.Close()) })
	assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "blocked by egress restriction"),
		"missing guard diagnostic: %v", err)
	assert.Equal(t, requests.Load(), 0)

	invalid, err := RenderTrees(t.Context(), []Tree{tree, tree}, Config{Concurrency: 2, CacheDir: t.TempDir()})
	t.Cleanup(func() { assert.NoError(t, invalid.Close()) })
	assert.Equal(t, invalid == nil, true)
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	transport := enginesource.NewHTTPTransport(enginesource.TransportMaterial{})
	conn, err := transport.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
	if conn != nil {
		assert.NoError(t, conn.Close())
	}
	assert.FatalIf(t, err == nil || !strings.Contains(err.Error(), "blocked by egress restriction"),
		"input rejection changed the egress policy: %v", err)
	assert.Equal(t, requests.Load(), 0)

	allowed, err := RenderTrees(t.Context(), []Tree{tree}, Config{Concurrency: 2, CacheDir: t.TempDir()})
	t.Cleanup(func() { assert.NoError(t, allowed.Close()) })
	assert.NoError(t, err)
	assert.FatalIf(t, requests.Load() == 0, "missing request to %s", server.URL)
	assert.FatalIf(
		t,
		allowed == nil || len(allowed.Results) != 1 || !allowed.Results[0].Complete,
		"missing render: %v",
		allowed,
	)
	assert.Equal(t, runValue(t, allowed.Results[0].Result.Manifests), value)
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_ResultsDoNotControlOwnership(t *testing.T) {
	for _, mutation := range []string{"replace", "reorder", "clear", "fields"} {
		t.Run(mutation, func(t *testing.T) {
			roots := []string{t.TempDir(), t.TempDir()}
			trees := make([]Tree, 0, len(roots))
			for i, root := range roots {
				writeRunCluster(t, root, "", strconv.Itoa(i))
				trees = append(
					trees,
					Tree{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeSide(i)}, RepoRoot: root},
				)
			}
			run, err := RenderTrees(t.Context(), trees, Config{Concurrency: 2, CacheDir: t.TempDir()})
			assert.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, run.Close()) })
			ids := []manifest.TreeID{trees[0].ID, trees[1].ID}
			switch mutation {
			case "replace":
				run.Results = []Rendered{{ID: manifest.TreeID{Cluster: "fake", Side: manifest.TreeHead}}}
			case "reorder":
				slices.Reverse(run.Results)
			case "clear":
				run.Results = nil
			case "fields":
				for i := range run.Results {
					run.Results[i] = Rendered{}
				}
			}
			selected, projection := run.Select(
				ids,
				[]manifest.NamedResource{testutil.KustomizationID("apps", "owner")},
				nil,
			)
			assert.NoError(t, projection.Selected)
			assert.NoError(t, projection.Remaining)
			assert.Equal(t, len(selected), 2)
			for i, s := range selected {
				assert.Equal(t, runValue(t, s.Manifests), strconv.Itoa(i))
			}
			fake, projection := run.Select([]manifest.TreeID{{Cluster: "fake", Side: manifest.TreeHead}}, nil, nil)
			assert.Equal(t, fake == nil, true)
			assert.Equal(t, errors.Is(projection.Selected, manifest.ErrInput), true)
			cacheDir := run.invocation.Config.CacheDir
			assert.NoError(t, run.Close())
			assert.NoError(t, run.Close())
			result, sweepErr := sourcecache.Sweep(t.Context(), cacheDir, sourcecache.SweepOpts{})
			assert.NoError(t, sweepErr)
			assert.Equal(t, len(result.Errors), 0)
		})
	}
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_LazySelectionLifetime(t *testing.T) {
	for _, stage := range []string{"completed", "partial_failure", "discovery_failure", "canceled"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			writeRunCluster(t, root, "", "retained")
			path := ""
			switch stage {
			case "partial_failure":
				testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [missing.yaml]\n")
			case "discovery_failure":
				path = filepath.Join(root, "missing")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if stage == "canceled" {
				cancel()
			}
			run, err := RenderTrees(
				ctx,
				[]Tree{{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}, RepoRoot: root, Path: path}},
				Config{Concurrency: 2, CacheDir: t.TempDir(), RecordWork: true},
			)
			assert.FatalIf(t, run == nil, "lost %s invocation: %v", stage, err)
			t.Cleanup(func() { assert.NoError(t, run.Close()) })
			assert.Equal(t, err != nil, stage != "completed")
			assert.Equal(t, len(run.Results), 1)
			rendered := run.Results[0]
			switch stage {
			case "discovery_failure":
				assert.Equal(t, rendered.Complete, false)
				assert.Equal(t, rendered.Absent, false)
				assert.Equal(t, rendered.ID, (manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}))
				assert.FatalIf(t, rendered.Err == nil || rendered.Result == nil, "lost discovery diagnostics: %v", err)
				assert.Equal(t, errors.Is(err, rendered.Err), true)
				assert.Equal(t, len(rendered.Result.Work) > 0, true)
			case "completed", "partial_failure":
				assert.Equal(t, rendered.Complete, true)
			case "canceled":
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			}
			var archive bytes.Buffer
			testutil.WriteTar(t, &archive, map[string]string{"lazy.txt": "retained disk content"}, 0o644)
			input, _, _, acquireErr := run.invocation.Config.Cache.Acquire(
				t.Context(),
				sourcecache.ArchiveKey{Content: stage},
				&sourcecache.Policy{},
				func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					_, writeErr := w.Write(archive.Bytes())
					return sourcecache.ProducerResult{}, writeErr
				},
			)
			assert.NoError(t, acquireErr)
			body, readErr := fs.ReadFile(input, "lazy.txt")
			assert.NoError(t, readErr)
			assert.Equal(t, string(body), "retained disk content")
			selected, projection := run.Select(
				[]manifest.TreeID{{Cluster: "a", Side: manifest.TreeHead}},
				[]manifest.NamedResource{testutil.KustomizationID("apps", "owner")},
				nil,
			)
			assert.Equal(t, len(selected), 1)
			if stage == "completed" {
				assert.NoError(t, projection.Selected)
				assert.Equal(t, runValue(t, selected[0].Manifests), "retained")
			}
			assert.NoError(t, run.Close())
			assert.NoError(t, run.Close())
			_, readErr = fs.ReadFile(input, "lazy.txt")
			assert.Equal(t, errors.Is(readErr, fs.ErrClosed), true)
		})
	}
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_PairedSelectionMatchesHeadOnly(t *testing.T) {
	root, err := filepath.Abs("../../testdata/producer-moves/pr91")
	assert.NoError(t, err)
	head := manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}
	base := manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}
	roots := []manifest.NamedResource{
		testutil.KustomizationID("", ""),
		testutil.HelmReleaseID("", ""),
	}
	cfg := Config{Concurrency: 2, CacheDir: t.TempDir(), Selection: SelectionRequest{Roots: roots}}
	single, err := RenderTrees(t.Context(), []Tree{{ID: head, RepoRoot: root}}, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, single.Close()) })
	pair, err := RenderTrees(t.Context(), []Tree{{ID: base, RepoRoot: root}, {ID: head, RepoRoot: root}}, cfg)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, pair.Close()) })

	want, projection := single.Select([]manifest.TreeID{head}, roots, nil)
	assert.NoError(t, projection.Selected)
	assert.NoError(t, projection.Remaining)
	got, projection := pair.Select([]manifest.TreeID{base, head}, roots, nil)
	assert.NoError(t, projection.Selected)
	assert.NoError(t, projection.Remaining)
	assert.Equal(t, len(want), 1)
	assert.Equal(t, len(got), 2)
	assert.FatalIf(t, len(want[0].Manifests) == 0, "expected rendered documents from the fixture")
	for _, side := range got {
		batches := want[0].Manifests
		assert.FatalIf(
			t,
			len(side.Manifests) != len(batches),
			"got %d batches, want %d",
			len(side.Manifests),
			len(batches),
		)
		for i, batch := range side.Manifests {
			assert.Equal(t, batch.Parent.Named(), batches[i].Parent.Named())
			assert.Diff(t, batch.Identities, batches[i].Identities)
			assert.Diff(t, batch.Documents, batches[i].Documents)
		}
	}
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_StatusSnapshots(t *testing.T) {
	st := store.New()
	id := testutil.KustomizationID("apps", "owner")
	st.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	st.UpdateStatus(id, store.ApplyingStatus(1234))
	var replay StatusInfo
	unsubscribe := st.AddListener(
		store.EventStatusUpdated,
		func(_ manifest.NamedResource, value any) { replay = statusInfo(value.(store.StatusInfo)) },
		true,
	)
	unsubscribe()
	assert.Equal(t, replay.Message, "applying 1234 objects")
	assert.Equal(t, replay.Outcome, OutcomePending)
	st.UpdateFailure(
		id,
		&manifest.DependencyFailedError{
			Parent: id,
			Failed: []manifest.NamedResource{testutil.KustomizationID("apps", "dependency")},
		},
	)
	info, _ := st.GetStatus(id)
	assert.Equal(t, statusInfo(info).Outcome, OutcomeBlocked)
	assert.Equal(t, statuses(map[manifest.NamedResource]store.StatusInfo{id: info})[id], statusInfo(info))
	assert.Equal(t, replay.Message, "applying 1234 objects")
	for _, reason := range []store.StatusReason{store.ReasonSkipped, store.ReasonUnchanged, store.ReasonSuspended, ""} {
		value := store.StatusInfo{Status: store.StatusReady, Reason: reason}
		assert.Equal(t, statusInfo(value).Outcome, Outcome(value.Outcome()))
		assert.Equal(t, statusInfo(value).Reason, StatusReason(reason))
	}
	var mu sync.Mutex
	var events []StatusInfo
	root := t.TempDir()
	writeRunCluster(t, root, "", "rendered")
	run, err := RenderTrees(
		t.Context(),
		[]Tree{{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}, RepoRoot: root}},
		Config{Concurrency: 2, StatusListener: func(_ manifest.TreeID, _ manifest.DiagnosticIdentity, info StatusInfo) {
			mu.Lock()
			events = append(events, info)
			mu.Unlock()
		}},
	)
	assert.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, run.Close()) })
	assert.Equal(t, len(events) > 0, true)
	applying := slices.ContainsFunc(events, func(info StatusInfo) bool {
		return info.Message == "applying 1 objects" && info.Outcome == OutcomePending
	})
	assert.Equal(t, applying, true)
	selected, projection := run.Select(
		[]manifest.TreeID{{Cluster: "a", Side: manifest.TreeHead}},
		[]manifest.NamedResource{id},
		nil,
	)
	assert.NoError(t, projection.Selected)
	assert.Equal(t, selected[0].Statuses[id].Outcome, OutcomePassed)
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_SuppliedDocuments(t *testing.T) {
	for _, scenario := range []string{"replacement", "repository_precedence", "encrypted", "malformed", "malformed_data"} {
		t.Run(scenario, func(t *testing.T) {
			testRunSuppliedDocuments(t, scenario)
		})
	}
}

func testRunSuppliedDocuments(t *testing.T, scenario string) {
	t.Helper()
	root := t.TempDir()
	writeRunCluster(t, root, "settings", "${VALUE}-${OLD:-gone}")
	doc := func(data map[string]any) map[string]any {
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "settings", "namespace": "apps"},
			"data":       data,
		}
	}
	first := doc(map[string]any{"VALUE": "first", "OLD": "old"})
	last := doc(map[string]any{"VALUE": "last"})
	if scenario == "encrypted" {
		last = doc(map[string]any{"VALUE": "ENC[AES256_GCM,data:encrypted,iv:invalid,tag:invalid,type:str]"})
		last["sops"] = map[string]any{"mac": "ENC[AES256_GCM,data:encrypted]"}
	}
	if scenario == "repository_precedence" {
		testutil.WriteFile(
			t,
			root,
			"settings.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: apps}\ndata: {VALUE: repository}\n",
		)
	}
	if scenario == "malformed_data" {
		last = doc(map[string]any{"VALUE": map[string]any{"nested": "invalid"}})
	}
	if scenario == "malformed" {
		last = map[string]any{"kind": "ConfigMap"}
	}
	cfg := Config{
		Concurrency: 2,
		Supply: []SuppliedDocument{
			{Doc: first, InputPath: "first.yaml", Ordinal: 0},
			{Doc: last, InputPath: "last.yaml", Ordinal: 1},
		},
	}
	run, err := RenderTrees(
		t.Context(),
		[]Tree{{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}, RepoRoot: root}},
		cfg,
	)
	if scenario == "malformed" || scenario == "malformed_data" {
		assert.Equal(t, run == nil, true)
		assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
		return
	}
	assert.FatalIf(t, run == nil, "lost run: %v", err)
	t.Cleanup(func() { assert.NoError(t, run.Close()) })
	if scenario == "encrypted" {
		assert.NoError(t, err)
		facts := run.invocation.Results[0].Store().
			ReferenceFacts(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}, nil, manifest.NamedResource{})
		encrypted, readErr := facts.Data.Value("VALUE")
		assert.NoError(t, readErr)
		assert.Equal(t, encrypted.Encrypted, true)
		assert.Equal(t, runValue(t, run.Results[0].Result.Manifests), "-gone")
		assert.Equal(t, len(run.Results[0].Result.Warnings), 0)
		return
	}
	assert.NoError(t, err)
	last["data"].(map[string]any)["VALUE"] = "caller mutation"
	selected, projection := run.Select(
		[]manifest.TreeID{{Cluster: "a", Side: manifest.TreeHead}},
		[]manifest.NamedResource{testutil.KustomizationID("apps", "owner")},
		nil,
	)
	assert.NoError(t, projection.Selected)
	want := "last-gone"
	if scenario == "repository_precedence" {
		want = "repository-gone"
		assert.Equal(
			t,
			slices.ContainsFunc(
				run.Results[0].Result.ClusterWarnings,
				func(w manifest.Warning) bool { return w.Category == manifest.WarnSuppliedPrecedence },
			),
			true,
		)
	}
	assert.Equal(t, runValue(t, selected[0].Manifests), want)
	assert.Equal(t, len(run.invocation.Config.Supply), 1)
	assert.Equal(t, run.invocation.Config.Supply[0].InputPath, "last.yaml")
	assert.Equal(t, run.invocation.Config.Supply[0].Origin, manifest.SuppliedDeclaration)
}

func writeRunCluster(t *testing.T, root, reference, value string) {
	t.Helper()
	postBuild := ""
	if reference != "" {
		postBuild = "  postBuild: {substituteFrom: [{kind: ConfigMap, name: " + reference + "}]}\n"
	}
	testutil.WriteFile(t, root, "flux.yaml", testutil.KustomizationYAML("owner", "apps", "  path: ./app\n"+postBuild))
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"app/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output, namespace: apps}\ndata: {value: '"+value+"'}\n",
	)
}

func runValue(t *testing.T, batches []manifest.DocumentBatch) string {
	t.Helper()
	for _, batch := range batches {
		for _, doc := range batch.Documents {
			if data, ok := doc["data"].(map[string]any); ok {
				if value, ok := data["value"].(string); ok {
					return value
				}
			}
		}
	}
	t.Fatal("missing output value")
	return ""
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_LazySourceReads(t *testing.T) {
	for _, stage := range []string{"completed", "partial_failure", "canceled"} {
		t.Run(stage, func(t *testing.T) {
			repositoryRoot, root := t.TempDir(), t.TempDir()
			repo := testutil.GitInit(t, repositoryRoot)
			writeRunCluster(t, repositoryRoot, "", "remote")
			testutil.WriteFile(t, repositoryRoot, "lazy.txt", "lazy git content")
			if stage == "partial_failure" {
				testutil.WriteFile(t, repositoryRoot, "app/kustomization.yaml", "resources: [absent.yaml]\n")
			}
			testutil.GitCommitAll(
				t,
				repo,
				"fixture",
				&git.CommitOptions{
					Author: &object.Signature{Name: "Test", Email: "test@example.com", When: time.Unix(1, 0)},
				},
			)
			source := manifest.NamedResource{
				Group:     manifest.SourceDomain,
				Kind:      manifest.KindGitRepository,
				Namespace: "apps",
				Name:      "remote",
			}
			owner := testutil.KustomizationID("apps", "owner")
			testutil.WriteFile(
				t,
				root,
				"source.yaml",
				"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: remote, namespace: apps}\nspec: {url: file://"+repositoryRoot+"}\n",
			)
			testutil.WriteFile(
				t,
				root,
				"flux.yaml",
				testutil.KustomizationYAML(
					"owner",
					"apps",
					"  path: ./app\n  sourceRef: {kind: GitRepository, name: remote}\n",
				),
			)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			run, err := RenderTrees(
				ctx,
				[]Tree{{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}, RepoRoot: root}},
				Config{
					Concurrency: 2,
					CacheDir:    t.TempDir(),
					StatusListener: func(_ manifest.TreeID, subject manifest.DiagnosticIdentity, info StatusInfo) {
						if stage == "canceled" && subject.ID == owner && info.Message == "applying 1 objects" {
							cancel()
						}
					},
				},
			)
			assert.FatalIf(t, run == nil || len(run.Results) != 1, "missing retained source run: %v", err)
			t.Cleanup(func() { assert.NoError(t, run.Close()) })
			if stage == "completed" {
				assert.NoError(t, err)
			} else {
				assert.FatalIf(t, err == nil, "missing %s error", stage)
			}
			if stage == "canceled" {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			}
			content := run.invocation.Results[0].Store().GetArtifact(source, store.ArtifactContent)
			artifact, ok := content.(*store.SourceArtifact)
			assert.FatalIf(t, !ok, "missing lazy git source: %v", err)
			data, readErr := fs.ReadFile(artifact.FS, "lazy.txt")
			assert.NoError(t, readErr)
			assert.Equal(t, string(data), "lazy git content")
			selected, projection := run.Select(
				[]manifest.TreeID{run.Results[0].ID},
				[]manifest.NamedResource{owner},
				nil,
			)
			assert.Equal(t, len(selected), 1)
			if stage == "completed" {
				assert.NoError(t, projection.Selected)
				assert.Equal(t, runValue(t, selected[0].Manifests), "remote")
			}
			cacheDir := run.invocation.Config.CacheDir
			assert.NoError(t, run.Close())
			assert.NoError(t, run.Close())
			result, sweepErr := sourcecache.Sweep(t.Context(), cacheDir, sourcecache.SweepOpts{})
			assert.NoError(t, sweepErr)
			assert.Equal(t, len(result.Errors), 0)
		})
	}
}

//nolint:paralleltest // RenderTrees sets the process-wide egress policy.
func TestRun_StatusDependencySnapshots(t *testing.T) {
	root := t.TempDir()
	writeRunCluster(t, root, "", "unused")
	testutil.WriteFile(t, root, "app/kustomization.yaml", "resources: [missing.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"blocked.yaml",
		testutil.KustomizationYAML("blocked", "apps", "  path: ./app\n  dependsOn: [{name: owner}]\n"),
	)
	var mu sync.Mutex
	callbacks := map[manifest.NamedResource]StatusInfo{}
	run, err := RenderTrees(
		t.Context(),
		[]Tree{{ID: manifest.TreeID{Cluster: "a", Side: manifest.TreeHead}, RepoRoot: root}},
		Config{
			Concurrency: 2,
			StatusListener: func(_ manifest.TreeID, subject manifest.DiagnosticIdentity, info StatusInfo) {
				mu.Lock()
				callbacks[subject.ID] = info
				mu.Unlock()
			},
		},
	)
	assert.FatalIf(t, run == nil || err == nil, "missing failed dependency run: %v", err)
	t.Cleanup(func() { assert.NoError(t, run.Close()) })
	blocked := testutil.KustomizationID("apps", "blocked")
	assert.Equal(t, callbacks[blocked].Outcome, OutcomeBlocked)
	assert.Equal(t, run.Results[0].Result.Failed[blocked], callbacks[blocked])
	selected, projection := run.Select(
		[]manifest.TreeID{run.Results[0].ID},
		[]manifest.NamedResource{testutil.KustomizationID("apps", "")},
		nil,
	)
	assert.FatalIf(t, projection.Selected == nil, "selected failure lost")
	assert.NoError(t, projection.Remaining)
	assert.Equal(t, selected[0].Statuses[blocked].Outcome, OutcomeBlocked)
	assert.Equal(t, selected[0].Failed[blocked].Outcome, OutcomeBlocked)
	assert.Equal(t, selected[0].BlockedCount, 1)
}
