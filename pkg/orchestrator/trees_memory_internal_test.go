package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/networktest"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func writeMemoryCluster(t *testing.T, producers int) (string, []manifest.NamedResource) {
	t.Helper()
	root := t.TempDir()
	payload := strings.Repeat("x", 1024)
	var resources, templates strings.Builder
	for n := range 10 {
		name := fmt.Sprintf("object-%02d", n)
		fmt.Fprintf(&resources, "- %s.yaml\n", name)
		testutil.WriteFile(
			t,
			root,
			"shared/"+name+".yaml",
			fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s}\ndata: {payload: %s}\n", name, payload),
		)
		fmt.Fprintf(
			&templates,
			"---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-%s\ndata: {payload: %s}\n",
			name,
			payload,
		)
	}
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources:\n"+resources.String())
	testutil.WriteFile(t, root, "charts/shared/Chart.yaml", "apiVersion: v2\nname: shared\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/shared/templates/objects.yaml", templates.String())
	var declarations []manifest.NamedResource
	for n := range producers {
		name := fmt.Sprintf("producer-%04d", n)
		var declaration string
		if n%2 == 0 {
			testutil.WriteFile(
				t,
				root,
				"apps/"+name+"/kustomization.yaml",
				"resources: [../../shared]\nnamePrefix: "+name+"-\n",
			)
			declaration = fmt.Sprintf(
				"apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: %s, namespace: flux-system}\nspec:\n  interval: 10m\n  path: ./apps/%s\n  sourceRef: {kind: GitRepository, name: cluster}\n",
				name,
				name,
			)
			declarations = append(declarations, testutil.KustomizationID("flux-system", name))
		} else {
			declaration = fmt.Sprintf(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: %s, namespace: flux-system}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/shared\n      sourceRef: {kind: GitRepository, name: cluster}\n",
				name,
			)
			declarations = append(declarations, testutil.HelmReleaseID("flux-system", name))
		}
		testutil.WriteFile(t, root, "flux/"+name+".yaml", declaration)
	}
	return root, declarations
}

func memoryWorkloadBatches(
	t *testing.T,
	result *Result,
	o *Orchestrator,
	want []manifest.NamedResource,
) []manifest.DocumentBatch {
	t.Helper()
	var workloads []manifest.DocumentBatch
	var declarations []manifest.NamedResource
	for _, batch := range result.Manifests {
		if batch.Parent == nil || !batch.Parent.Named().ReportHidden() {
			workloads = append(workloads, batch)
			continue
		}
		assert.Equal(t, batch.Parent.Named(), o.entryBinding.Producer)
		for _, doc := range batch.Documents {
			declarations = append(declarations, manifest.DocIdentity(doc))
		}
	}
	slices.SortFunc(declarations, manifest.NamedResource.Compare)
	want = slices.Clone(want)
	slices.SortFunc(want, manifest.NamedResource.Compare)
	if o.entryBinding.Producer == (manifest.NamedResource{}) {
		assert.Equal(t, len(declarations), 0)
		for _, id := range want {
			assert.Equal(t, o.store.GetObject(id) != nil, true)
		}
	} else {
		assert.Diff(t, declarations, want)
	}
	return workloads
}

func TestRender_MemorySharedOwner(t *testing.T) {
	t.Parallel()
	t.Run("local", func(t *testing.T) {
		t.Parallel()
		root, declarations := writeMemoryCluster(t, 2)
		var calls int
		var fingerprints [2][32]byte
		func() {
			owner, ownerErr := sourcecache.New(t.Context(), "")
			assert.NoError(t, ownerErr)
			defer func() { assert.NoError(t, owner.Close()) }()
			for run := range 2 {
				render, err := newTestOrchestrator(
					t,
					Tree{Path: filepath.Join(root, "flux"), RepoRoot: root},
					Config{Cache: owner, Concurrency: 2},
				)
				assert.NoError(t, err)
				result, renderErr := render.render(t.Context(), t)
				assert.NoError(t, renderErr)
				count := 0
				for _, batch := range memoryWorkloadBatches(t, result, render, declarations) {
					count += len(batch.Documents)
				}
				assert.Equal(t, count, 20)
				data, err := json.Marshal(result.Manifests)
				assert.NoError(t, err)
				fingerprints[run] = sha256.Sum256(data)
				reader, _, _, err := owner.Blob(
					t.Context(),
					sourcecache.BlobKey{Content: "E8 shared-owner lifecycle"},
					&sourcecache.Policy{MaxAge: sourcecache.Forever},
					func(_ context.Context, out sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
						calls++
						_, writeErr := io.WriteString(out, "shared-owner payload")
						return sourcecache.ProducerResult{Descriptor: ocispec.Descriptor{}}, writeErr
					},
				)
				assert.NoError(t, err)
				body, err := fs.ReadFile(reader, "content")
				assert.NoError(t, err)
				assert.Equal(t, string(body), "shared-owner payload")
			}
		}()
		assert.Equal(t, calls, 1)
		assert.Equal(t, fingerprints[0], fingerprints[1])
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		t.Logf(
			"SDK reuse: two fresh render instances, one owner, 2 producers/20 objects, reacquisitions=%d; after result release/Stop/Close/GC heap_alloc=%d heap_inuse=%d; process-wide samples, not attributed RSS",
			calls,
			stats.HeapAlloc,
			stats.HeapInuse,
		)
	})
	t.Run(
		"home_plex",
		func(t *testing.T) {
			t.Parallel()
			if os.Getenv("FLATE_E8_SLOW") != "1" {
				t.Skip(
					"MEM-3-WARM SDK not-run: env FLATE_E8_SLOW=1 FLATE_E8_HOME=<pinned-home" +
						"-ops> go test ./pkg/orchestrator -run '^TestRender_MemorySharedOwner/h" +
						"ome_plex$' -count=1 -v",
				)
			}
			root := os.Getenv("FLATE_E8_HOME")
			assert.FatalIf(t, root == "", "%v", "MEM-3-WARM SDK requested without FLATE_E8_HOME pinned corpus input")
			func() {
				owner, err := sourcecache.New(t.Context(), "")
				assert.NoError(t, err)
				defer func() { assert.NoError(t, owner.Close()) }()
				var digests [2][32]byte
				for run := range 2 {
					o, err := newTestOrchestrator(
						t,
						Tree{
							Path: filepath.Join(
								root,
								"kubernetes", "apps",
							),
							RepoRoot: root,
						},
						Config{
							RecordWork: true,

							Cache: owner,

							Concurrency: runtime.NumCPU() * 4,

							GitDepth: 1,

							HelmOptions: helm.Options{
								DisableChartDigestTracking: true,
								KubeVersion:                helm.BundledKubeVersion(),
								SkipTests:                  true,
								SkipCRDs:                   true,
								SkipKinds: []string{
									manifest.KindCustomResourceDefinition,
									manifest.KindSecret,
								},
							},

							SourceRetry: sourcecache.RetryConfig{
								Attempts: 3, MinWait: 200 * time.Millisecond, MaxWait: 3 * time.Second, Jitter: 0.1,
							},
						},
					)
					assert.NoError(t, err)
					result, renderErr := o.render(t.Context(), t)
					o.stop()
					projectedResults0, projectionErrors0 := Select(
						[]Rendered{
							{
								Orchestrator: o,
								Result:       result,
								Err:          renderErr,
							},
						},
						renderErr,
						[]manifest.NamedResource{
							{
								Group:     "helm.toolkit.fluxcd.io",
								Kind:      "HelmRelease",
								Namespace: "media",
								Name:      "plex",
							},
						},
						nil,
					)
					selected, selectedErr, _ := projectedResults0, projectionErrors0.Selected, projectionErrors0.Remaining
					assert.NoError(t, selectedErr)
					assert.FatalIf(
						t,
						len(selected) != 1 || len(selected[0].Objects) != 1 || len(selected[0].Manifests) == 0,
						"%v",
						"pinned corpus did not supply rendered media/plex",
					)
					data, err := json.Marshal(selected[0].Manifests)
					assert.NoError(t, err)
					digests[run] = sha256.Sum256(data)
					t.Logf(
						"MEM-3-WARM SDK run=%d digest=%x; full SDK render then public selection"+
							"; SDK startup/output encoding differ from CLI",
						run,
						digests[run],
					)
				}
				assert.Equal(t, digests[0], digests[1])
			}()
			runtime.GC()
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			t.Logf(
				"MEM-3-WARM SDK released results/Stop/Close/GC heap_alloc=%d heap_inuse=%d; no process RSS verdict",
				stats.HeapAlloc,
				stats.HeapInuse,
			)
		},
	)
}

func TestRender_MemorySparseGit(t *testing.T) {
	t.Parallel()
	for _, size := range []struct {
		name  string
		files int
		bytes int64
	}{
		{"routine", 24, 4096}, {"sparse-git", 12000, 256 << 20},
	} {
		t.Run(size.name, func(t *testing.T) {
			t.Parallel()
			testRenderMemorySparseGitCase(t, &size)
		},
		)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_MemoryContained(t *testing.T) {
	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	previousGC := debug.SetGCPercent(100)
	previousLimit := debug.SetMemoryLimit(math.MaxInt64)
	defer debug.SetGCPercent(previousGC)
	defer debug.SetMemoryLimit(previousLimit)
	root := t.TempDir()
	for _, name := range []string{"large-a", "large-b", "large-c", "large-d"} {
		testutil.WriteFile(
			t,
			root,
			"flux/"+name+".yaml",
			fmt.Sprintf(
				"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name"+
					": %s, namespace: apps}\nspec:\n  interval: 1h\n  releaseName: %s\n  chart"+
					":\n    spec:\n      chart: charts/large\n      sourceRef: {kind: GitRepos"+
					"itory, name: cluster}\n",
				name,
				name,
			),
		)
	}
	testutil.WriteFile(t, root, "charts/large/Chart.yaml", "apiVersion: v2\nname: large\nversion: 1.0.0\n")
	cacheDir := t.TempDir()
	writePayload := func(size int) {
		testutil.WriteFile(
			t,
			root,
			"charts/large/templates/cm.yaml",
			fmt.Sprintf(
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: {{ .Release.Name }}}\nd"+
					"ata:\n  payload: |-\n    {{ .Release.Name }}{{ repeat %d \"x\" }}\n",
				size/4-len(
					"large-a",
				),
			),
		)
	}
	run := func(payloadSize int, templates uint64) {
		runs, err := RenderTrees(t.Context(),
			[]Tree{
				witnessTree(
					root,
					manifest.TreeHead,
				),
			},
			Config{
				RecordWork:  true,
				Concurrency: 2,
				CacheDir:    cacheDir,
			},
		)
		assert.NoError(t, err)
		defer func() {
			for _, run := range runs {
				run.Result.Close()
			}
		}()
		memoryPayloadsResult := memoryPayloads(t, runs[0].Result)
		size, payloads := memoryPayloadsResult.size, memoryPayloadsResult.payloads
		assert.Equal(t, size, payloadSize)
		assert.Diff(t, payloads, map[string]int{
			"large-a": payloadSize / 4, "large-b": payloadSize / 4,
			"large-c": payloadSize / 4, "large-d": payloadSize / 4,
		})
		assert.Equal(
			t,
			workCounts(runs[0].Result.Work, manifest.NamedResource{}, manifest.WorkHelm).Attempts,
			templates,
		)
		owners := map[manifest.NamedResource]uint64{}
		if templates != 0 {
			owners = map[manifest.NamedResource]uint64{
				testutil.HelmReleaseID("apps", "large-a"): 1, testutil.HelmReleaseID("apps", "large-b"): 1,
				testutil.HelmReleaseID("apps", "large-c"): 1, testutil.HelmReleaseID("apps", "large-d"): 1,
			}
		}
		assert.Diff(t, witnessWork(runs[0].Result.Work, manifest.WorkHelm), owners)
		runtime.KeepAlive(runs)
	}
	heap := func() uint64 {
		// Two cycles discard sync.Pool victim caches before sampling.
		runtime.GC()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		return stats.HeapAlloc
	}
	writePayload(1024)
	run(1024, 4)
	const payloadSize = 16 << 20
	writePayload(payloadSize)
	baselineTree := heap()
	tolerance := max(uint64(1<<20), baselineTree/50)
	assert.FatalIf(
		t,
		uint64(payloadSize/4) <= 2*tolerance,
		"each payload=%d must exceed twice the retained-heap tolerance=%d",
		payloadSize/4,
		tolerance,
	)
	for _, name := range []string{"control_one", "control_two", "one_cold_operation", "two_warm_operations"} {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "one_cold_operation":
				run(payloadSize, 4)
			case "two_warm_operations":
				templates := uint64(0)
				if !persistent {
					templates = 4
				}
				run(payloadSize, templates)
				run(payloadSize, templates)
			}
			after := heap()
			assert.FatalIf(
				t,
				after > baselineTree+tolerance || strings.HasPrefix(name, "control") && baselineTree > after+tolerance,
				"retained heap or invalid isolation: baseline=%d after=%d tolerance=%d",
				baselineTree,
				after,
				tolerance,
			)
			t.Logf("baseline=%d after=%d tolerance=%d payload=%d", baselineTree, after, tolerance, payloadSize)
		})
	}
}

func testRenderMemorySparseGitCase(t *testing.T, testCaseInput *struct {
	name string

	files int
	bytes int64
},
) {
	t.Helper()

	if testCaseInput.name == "sparse-git" && os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip(
			"MEM-SPARSE not-run: env FLATE_E8_SLOW=1 go test ./pkg/orchestrator -run '^TestRender_MemorySparseGit/sparse-git$' -count=1 -v",
		)
	}
	fixture := testutil.NewSparseGit(t, testCaseInput.files, testCaseInput.bytes)
	owner, err := sourcecache.New(t.Context(), "")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, owner.Close()) }()
	render := func(wantPaths []string) [2][32]byte {
		inputs, err := baseline.AutoResolve(
			t.Context(),
			baseline.Config{
				Network: networktest.New(t),
				Path:    filepath.Join(fixture.Root, "flux"),
				Base:    fixture.Base.String(),
				Cache:   owner,
			},
		)
		assert.NoError(t, err)
		assert.Diff(t, inputs.Changes.Paths(), wantPaths)
		runs, err := renderTreesForTest(t.Context(), t, []Tree{
			{
				RepoRoot:        inputs.BaseInput.LocalPath,
				Path:            inputs.BaseScanPath,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeBase},
				Input:           inputs.BaseInput,
				BaseInput:       inputs.Source,
				ExternalChanges: inputs.Changes,
			},
			{
				RepoRoot:        inputs.Source.LocalPath,
				Path:            inputs.ScanPath,
				ID:              manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
				Input:           inputs.Source,
				BaseInput:       inputs.BaseInput,
				ExternalChanges: inputs.Changes,
			},
		}, Config{Cache: owner, Concurrency: 2})
		assert.NoError(t, err)
		var digests [2][32]byte
		for n, side := range runs {
			app, sibling := testutil.KustomizationID(
				"flux-system",
				"app",
			), testutil.KustomizationID(
				"flux-system",
				"sibling",
			)
			assert.Equal(t, side.selection.Snapshot().Reconcile(app), true)
			assert.Equal(t, side.selection.Snapshot().Reconcile(sibling), false)
			status, ok := side.store.GetStatus(app)
			assert.Equal(t, ok, true)
			assert.Equal(t, status.Status, store.StatusReady)
			siblingStatus, _ := side.store.GetStatus(sibling)

			assert.FatalIf(
				t,
				siblingStatus.Outcome() == store.OutcomePassed,
				"%v",
				"unrelated sibling reconciled",
			)

			assert.Equal(t, side.store.GetObject(sibling) == nil, true)
			count := 0
			for _, batch := range memoryWorkloadBatches(t, side.Result, side.Orchestrator, []manifest.NamedResource{app}) {
				assert.FatalIf(
					t,
					batch.Parent == nil || batch.Parent.Named().Name != "app",
					"unrelated output parent=%v",
					batch.Parent,
				)

				for _, doc := range batch.Documents {
					count++
					want := []string{"before", "after!"}[n]
					assert.Equal(t, doc["data"].(map[string]any)["value"], any(want))
				}
			}
			assert.Equal(t, count, 1)
			data, err := json.Marshal(side.Result.Manifests)
			assert.NoError(t, err)
			digests[n] = sha256.Sum256(data)
		}
		return digests
	}
	initial := render([]string{"app/input.yaml"})
	testutil.WriteFile(t, fixture.Root, "disjoint/growth.txt", "unconsumed growth\n")
	assert.Equal(t, render([]string{"app/input.yaml", "disjoint/growth.txt"}), initial)
	t.Logf(
		"MEM-SPARSE files=%d blob_bytes=%d blob=%s: exact app admission/terminal/output; sibling excluded; public admission/fetch Reader counters unavailable",
		testCaseInput.files,
		testCaseInput.bytes,
		fixture.Large,
	)
}

type memoryPayloadSummary struct {
	size     int
	payloads map[string]int
}

func memoryPayloads(t *testing.T, result *Result) memoryPayloadSummary {
	t.Helper()
	var size int
	payloads := map[string]int{}
	for _, batch := range result.Manifests {
		for _, doc := range batch.Documents {
			if data, ok := doc["data"].(map[string]any); ok {
				if payload, ok := data["payload"].(string); ok {
					size += len(payload)
					name := manifest.DocIdentity(doc).Name
					assert.Equal(t, strings.HasPrefix(payload, name), true)
					payloads[name] = len(payload)
				}
			}
		}
	}

	return memoryPayloadSummary{size: size, payloads: payloads}
}
