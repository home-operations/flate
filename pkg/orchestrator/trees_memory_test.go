package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/baseline"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func writeMemoryCluster(t *testing.T, producers int) string {
	t.Helper()
	root := t.TempDir()
	payload := strings.Repeat("x", 1024)
	var resources, templates strings.Builder
	for n := range 10 {
		name := fmt.Sprintf("object-%02d", n)
		fmt.Fprintf(&resources, "- %s.yaml\n", name)
		testutil.WriteFile(t, root, "shared/"+name+".yaml", fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata: {name: %s}\ndata: {payload: %s}\n", name, payload))
		fmt.Fprintf(&templates, "---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}-%s\ndata: {payload: %s}\n", name, payload)
	}
	testutil.WriteFile(t, root, "shared/kustomization.yaml", "resources:\n"+resources.String())
	testutil.WriteFile(t, root, "charts/shared/Chart.yaml", "apiVersion: v2\nname: shared\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "charts/shared/templates/objects.yaml", templates.String())
	for n := range producers {
		name := fmt.Sprintf("producer-%04d", n)
		var declaration string
		if n%2 == 0 {
			testutil.WriteFile(t, root, "apps/"+name+"/kustomization.yaml", "resources: [../../shared]\nnamePrefix: "+name+"-\n")
			declaration = fmt.Sprintf("apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata: {name: %s, namespace: flux-system}\nspec:\n  interval: 10m\n  path: ./apps/%s\n  sourceRef: {kind: GitRepository, name: cluster}\n", name, name)
		} else {
			declaration = fmt.Sprintf("apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: %s, namespace: flux-system}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/shared\n      sourceRef: {kind: GitRepository, name: cluster}\n", name)
		}
		testutil.WriteFile(t, root, "flux/"+name+".yaml", declaration)
	}
	return root
}

func TestRender_MemorySharedOwner(t *testing.T) {
	t.Parallel()
	t.Run("local", func(t *testing.T) {
		t.Parallel()
		root := writeMemoryCluster(t, 2)
		var calls int
		var fingerprints [2][32]byte
		func() {
			owner, err := sourcecache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			for run := range 2 {
				render, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, Config{Cache: owner, Concurrency: 2})
				assert.NoError(t, err)
				result, renderErr := render.render(t.Context())
				assert.NoError(t, renderErr)
				count := 0
				for _, batch := range result.Manifests {
					count += len(batch.Documents)
				}
				assert.Equal(t, count, 20)
				data, err := json.Marshal(result.Manifests)
				assert.NoError(t, err)
				fingerprints[run] = sha256.Sum256(data)
				reader, _, _, err := owner.Blob(t.Context(), sourcecache.BlobKey{Content: "E8 shared-owner lifecycle"}, sourcecache.Policy{MaxAge: sourcecache.Forever}, func(_ context.Context, out sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					calls++
					_, err := io.WriteString(out, "shared-owner payload")
					return sourcecache.ProducerResult{Descriptor: ocispec.Descriptor{}}, err
				})
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
		t.Logf("SDK reuse: two fresh render instances, one owner, 2 producers/20 objects, reacquisitions=%d; after result release/Stop/Close/GC heap_alloc=%d heap_inuse=%d; process-wide samples, not attributed RSS", calls, stats.HeapAlloc, stats.HeapInuse)
	})
	t.Run("home_plex", func(t *testing.T) {
		t.Parallel()
		if os.Getenv("FLATE_E8_SLOW") != "1" {
			t.Skip("MEM-3-WARM SDK not-run: env FLATE_E8_SLOW=1 FLATE_E8_HOME=<pinned-home-ops> go test ./pkg/orchestrator -run '^TestRender_MemorySharedOwner/home_plex$' -count=1 -v")
		}
		root := os.Getenv("FLATE_E8_HOME")
		if root == "" {
			t.Fatal("MEM-3-WARM SDK requested without FLATE_E8_HOME pinned corpus input")
		}
		func() {
			owner, err := sourcecache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			var digests [2][32]byte
			for run := range 2 {
				o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "kubernetes/apps"), RepoRoot: root}, Config{
					Cache:       owner,
					WipeSecrets: true, Concurrency: runtime.NumCPU() * 4,
					GitDepth:    1,
					HelmOptions: helm.Options{DisableChartDigestTracking: true, KubeVersion: helm.BundledKubeVersion(), SkipTests: true, SkipCRDs: true, SkipKinds: []string{manifest.KindSecret}},
					SourceRetry: sourcecache.RetryConfig{Attempts: 3, MinWait: 200 * time.Millisecond, MaxWait: 3 * time.Second, Jitter: 0.1},
				})
				assert.NoError(t, err)
				result, renderErr := o.render(t.Context())
				o.stop()
				selected, selectedErr, _ := Select([]Rendered{{Orchestrator: o, Result: result, Err: renderErr}}, renderErr, []manifest.NamedResource{{Group: "helm.toolkit.fluxcd.io", Kind: "HelmRelease", Namespace: "media", Name: "plex"}}, nil)
				assert.NoError(t, selectedErr)
				if len(selected) != 1 || len(selected[0].Objects) != 1 || len(selected[0].Manifests) == 0 {
					t.Fatal("pinned corpus did not supply rendered media/plex")
				}
				data, err := json.Marshal(selected[0].Manifests)
				assert.NoError(t, err)
				digests[run] = sha256.Sum256(data)
				t.Logf("MEM-3-WARM SDK run=%d digest=%x; full SDK render then public selection; SDK startup/output encoding differ from CLI", run, digests[run])
			}
			assert.Equal(t, digests[0], digests[1])
		}()
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		t.Logf("MEM-3-WARM SDK released results/Stop/Close/GC heap_alloc=%d heap_inuse=%d; no process RSS verdict", stats.HeapAlloc, stats.HeapInuse)
	})
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
			if size.name == "sparse-git" && os.Getenv("FLATE_E8_SLOW") != "1" {
				t.Skip("MEM-SPARSE not-run: env FLATE_E8_SLOW=1 go test ./pkg/orchestrator -run '^TestRender_MemorySparseGit/sparse-git$' -count=1 -v")
			}
			fixture := testutil.NewSparseGit(t, size.files, size.bytes)
			owner, err := sourcecache.New(t.Context(), "")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, owner.Close()) }()
			render := func(wantPaths []string) [2][32]byte {
				inputs, err := baseline.AutoResolve(t.Context(), filepath.Join(fixture.Root, "flux"), fixture.Base.String(), owner)
				assert.NoError(t, err)
				assert.Diff(t, inputs.Changes.Paths(), wantPaths)
				runs, err := RenderTrees(t.Context(), []Tree{
					{RepoRoot: inputs.BaseInput.LocalPath, Path: inputs.BaseScanPath, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeBase}, Input: inputs.BaseInput, BaseInput: inputs.Source, ExternalChanges: inputs.Changes},
					{RepoRoot: inputs.Source.LocalPath, Path: inputs.ScanPath, ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead}, Input: inputs.Source, BaseInput: inputs.BaseInput, ExternalChanges: inputs.Changes},
				}, Config{Cache: owner, Concurrency: 2})
				assert.NoError(t, err)
				var digests [2][32]byte
				for n, side := range runs {
					app, sibling := testutil.KustomizationID("flux-system", "app"), testutil.KustomizationID("flux-system", "sibling")
					assert.Equal(t, side.filter.ShouldReconcile(app), true)
					assert.Equal(t, side.filter.ShouldReconcile(sibling), false)
					status, ok := side.store.GetStatus(app)
					assert.Equal(t, ok, true)
					assert.Equal(t, status.Status, store.StatusReady)
					siblingStatus, _ := side.store.GetStatus(sibling)
					if siblingStatus.Outcome() == store.OutcomePassed {
						t.Fatal("unrelated sibling reconciled")
					}
					count := 0
					for _, batch := range side.Result.Manifests {
						if batch.Parent == nil || batch.Parent.Named().Name != "app" {
							t.Fatalf("unrelated output parent=%v", batch.Parent)
						}
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
			t.Logf("MEM-SPARSE files=%d blob_bytes=%d blob=%s: exact app admission/terminal/output; sibling excluded; public admission/fetch Reader counters unavailable", size.files, size.bytes, fixture.Large)
		})
	}
}

func TestRender_MemoryContained(t *testing.T) {
	t.Parallel()
	if os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip("MEM-CONTAINED not-run: env FLATE_E8_SLOW=1 go test ./pkg/orchestrator -run '^TestRender_MemoryContained$' -count=1 -v")
	}
	root := writeMemoryCluster(t, 1000)
	o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "flux"), RepoRoot: root}, Config{Concurrency: 16})
	assert.NoError(t, err)
	result, renderErr := o.render(t.Context())
	assert.NoError(t, renderErr)
	parents := make(map[manifest.NamedResource]int)
	objects := make(map[string]bool)
	for _, batch := range result.Manifests {
		if batch.Parent == nil {
			t.Fatal("ownerless workload output")
		}
		id := batch.Parent.Named()
		for _, doc := range batch.Documents {
			parents[id]++
			metadata := doc["metadata"].(map[string]any)
			key := fmt.Sprint(metadata["namespace"], "/", metadata["name"])
			if objects[key] {
				t.Fatalf("duplicate workload object %s", key)
			}
			objects[key] = true
			assert.Equal(t, doc["kind"], any("ConfigMap"))
			assert.Equal(t, doc["data"].(map[string]any)["payload"], any(strings.Repeat("x", 1024)))
		}
	}
	assert.Equal(t, len(parents), 1000)
	assert.Equal(t, len(objects), 10000)
	for id, count := range parents {
		assert.Equal(t, count, 10)
		status, ok := o.store.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, status.Status, store.StatusReady)
	}
	data, err := json.Marshal(result.Manifests)
	assert.NoError(t, err)
	t.Logf("MEM-CONTAINED producers=%d objects=%d payload_bytes=1024 concurrency=16 digest=%x; RSS/cgroup verdict requires external clean-child collector", len(parents), len(objects), sha256.Sum256(data))
}
