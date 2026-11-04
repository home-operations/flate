package orchestrator

import (
	"encoding/json"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func inlineChartFiles(t *testing.T) map[string]string {
	t.Helper()
	root := os.DirFS("../../testdata/hr-oci-template/chart")
	files := map[string]string{}
	assert.NoError(t, fs.WalkDir(root, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		body, err := fs.ReadFile(root, name)
		files[name] = string(body)
		return err
	}))
	return files
}

func inlineChartRepository(t *testing.T, url string, fields map[string]any) inlineChartFixture {
	t.Helper()
	root := t.TempDir()
	testutil.CopyTree(t, "../../testdata/hr-oci-template", root)
	body, err := os.ReadFile(filepath.Join(root, "apps", "release.yaml"))
	assert.NoError(t, err)
	var doc map[string]any
	assert.NoError(t, yaml.Unmarshal(body, &doc))
	spec := doc["spec"].(map[string]any)["chart"].(map[string]any)["spec"].(map[string]any)
	spec["url"] = url
	maps.Copy(spec, fields)
	return inlineChartFixture{root: root, document: doc}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestRender_InlineOCIRegistryParity(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		digest, extract, auth, disable, fail bool
	}{
		{name: "tag"},
		{name: "digest", digest: true},
		{name: "extract", extract: true},
		{name: "credentials", auth: true},
		{name: "tracking_disabled", disable: true},
		{name: "registry_failure", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testRenderInlineOCIRegistryParityCase(t, &tc)
		})
	}
}

func TestRenderTrees_InlineOCIIndependentGenerations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		helm               uint64
		cache, acquisition manifest.WorkCounts
	}{
		{
			name: "tag",

			helm: 2,

			cache: manifest.WorkCounts{
				Attempts: 2,

				Completed: 2,
			},

			acquisition: manifest.WorkCounts{
				Attempts:        2,
				Completed:       2,
				RemoteTransfers: 2,
			},
		},

		{
			name: "digest",
			helm: 2,
			cache: manifest.WorkCounts{
				Attempts:  2,
				Completed: 2,
			},
			acquisition: manifest.WorkCounts{
				Attempts:        2,
				Completed:       2,
				RemoteTransfers: 2,
			},
		},

		{
			name: "failed_head",

			helm: 1,

			cache: manifest.WorkCounts{
				Attempts:  2,
				Completed: 1,
				Failures:  1,
			},

			acquisition: manifest.WorkCounts{
				Attempts: 2,

				Completed: 1,

				Failures: 1,

				RemoteTransfers: 1,
			},
		},

		{
			name: "url",
			helm: 1,
			cache: manifest.WorkCounts{
				Attempts:  2,
				Completed: 2,
			},
			acquisition: manifest.WorkCounts{
				Attempts:        2,
				Completed:       2,
				RemoteTransfers: 2,
			},
		},

		{
			name: "layer_selector",
			helm: 2,
			cache: manifest.WorkCounts{
				Attempts:  2,
				Completed: 2,
			},
			acquisition: manifest.WorkCounts{
				Attempts:        2,
				Completed:       2,
				RemoteTransfers: 2,
			},
		},

		{
			name: "metadata",

			helm: 1,

			cache: manifest.WorkCounts{
				Attempts:     2,
				Completed:    2,
				CacheService: 1,
			},

			acquisition: manifest.WorkCounts{
				Attempts: 1,

				Completed: 1,

				RemoteTransfers: 1,
			},
		},

		{
			name: "interval",

			helm: 1,

			cache: manifest.WorkCounts{
				Attempts:     2,
				Completed:    2,
				CacheService: 1,
			},

			acquisition: manifest.WorkCounts{
				Attempts: 1,

				Completed: 1,

				RemoteTransfers: 1,
			},
		},

		{
			name: "helm_chart",

			helm: 2,

			cache: manifest.WorkCounts{
				Attempts:  1,
				Completed: 1,
			},

			acquisition: manifest.WorkCounts{
				Attempts: 1,

				Completed: 1,

				RemoteTransfers: 1,
			},
		},

		{
			name: "chart_ref", helm: 1,
			cache:       manifest.WorkCounts{Attempts: 2, Completed: 2, CacheService: 1},
			acquisition: manifest.WorkCounts{Attempts: 1, Completed: 1, RemoteTransfers: 1},
		},

		{name: "no_op"},

		{name: "upstream"},
	} {
		t.Run(

			tc.name,

			func(t *testing.T) {
				t.Parallel()
				testRenderTreesInlineOCIIndependentGenerationsCase(t, &tc)
			},
		)
	}
}

func testRenderInlineOCIRegistryParityCase(t *testing.T, tc *struct {
	name                                 string
	digest, extract, auth, disable, fail bool
},
) {
	t.Helper()

	registry := testutil.NewChartRegistry(t, []map[string]string{inlineChartFiles(t)}, tc.auth)
	fields := map[string]any{}
	if tc.digest {
		fields["ref"] = map[string]any{"digest": registry.Digest}
	}
	if tc.extract {
		fields["layerSelector"] = map[string]any{
			"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			"operation": "extract",
		}
	}
	if tc.auth {
		fields["secretRef"] = map[string]any{"name": "pull"}
	}
	url := registry.URL
	if tc.fail {
		url += "/missing"
	}
	var expected []map[string]any
	for _, entry := range []string{"template", "chartRef"} {
		inlineChartRepositoryResult := inlineChartRepository(t, url, fields)
		root, doc := inlineChartRepositoryResult.root, inlineChartRepositoryResult.document
		spec := doc["spec"].(map[string]any)
		template := spec["chart"].(map[string]any)["spec"].(map[string]any)
		resources := "resources: [release.yaml"
		if entry == "chartRef" {
			writeInlineChartReference(t, root, template)
			resources += ", source.yaml"
			delete(spec, "chart")
			spec["chartRef"] = map[string]any{"kind": "OCIRepository", "name": "chart"}
		}
		if tc.auth {
			resources += ", pull.yaml"
			host := strings.TrimSuffix(strings.TrimPrefix(registry.URL, "oci://"), "/demo")
			credentials, err := json.Marshal(
				map[string]any{
					"auths": map[string]any{
						host: map[string]any{"username": "fixture", "password": "fixture-password"},
					},
				},
			)
			assert.NoError(t, err)
			secret := map[string]any{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata":   map[string]any{"name": "pull", "namespace": "apps"},
				"type":       "kubernetes.io/dockerconfigjson",
				"stringData": map[string]any{".dockerconfigjson": string(credentials)},
			}
			secretBody, err := yaml.Marshal(secret)
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "apps/pull.yaml", string(secretBody))
		}
		testutil.WriteFile(t, root, "apps/kustomization.yaml", resources+"]\n")
		body, err := yaml.Marshal(doc)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "apps/release.yaml", string(body))
		cfg := Config{
			Concurrency: 2,
			CacheDir:    t.TempDir(),
			HelmOptions: helm.Options{DisableChartDigestTracking: tc.disable},
			SourceRetry: sourcecache.RetryConfig{Attempts: 1},
		}
		o, err := newTestOrchestrator(t, Tree{Path: filepath.Join(root, "cluster"), RepoRoot: root}, cfg)
		assert.NoError(t, err)
		result, renderErr := o.render(t.Context())
		id := testutil.HelmReleaseID("apps", "demo")
		sourceID := manifest.NamedResource{
			Group:     manifest.SourceDomain,
			Kind:      manifest.KindOCIRepository,
			Namespace: "apps",
			Name:      "apps-demo",
		}
		if entry == "chartRef" {
			sourceID.Name = "chart"
		}
		assert.FatalIf(
			t,
			o.store.ReportHidden(sourceID) != (entry == "template"),
			"%v",
			"source visibility lost publication provenance",
		)
		if tc.fail {
			assert.FatalIf(
				t,
				renderErr == nil || result.Failed[id].Status != store.StatusFailed,
				"registry failure lost release diagnostic: %v %+v",
				renderErr,
				result.Failed,
			)
			continue
		}
		assert.NoError(t, renderErr)
		docs := outputByParent(result.Manifests)[id]
		assert.FatalIf(t, len(docs) != 1, "chart output=%+v", docs)
		data := docs[0]["data"].(map[string]any)
		version := "0.1.0"
		if !tc.disable {
			version += "+" + strings.TrimPrefix(registry.Digest, "sha256:")[:12]
		}
		assert.Equal(t, data["version"], any(version))
		assert.Equal(t, data["release"], any("visible-release"))
		assert.Equal(t, data["payload"], any("first"))
		if entry == "template" {
			expected = docs
		} else {
			assert.Diff(t, docs, expected)
		}
		assert.FatalIf(
			t,
			len(o.store.ListObjects(manifest.SourceDomain, manifest.KindHelmChart)) != 0,
			"%v",
			"OCI render synthesized a HelmChart",
		)
	}
	assert.FatalIf(
		t,
		tc.auth && registry.Authenticated.Load() == 0,
		"%v",
		"inline credentials were not used by acquisition",
	)
}

func testRenderTreesInlineOCIIndependentGenerationsCase(t *testing.T, tc *struct {
	name               string
	helm               uint64
	cache, acquisition manifest.WorkCounts
},
) {
	t.Helper()

	mode := tc.name

	first := inlineChartFiles(t)
	second := maps.Clone(first)
	second["demo/templates/configmap.yaml"] = strings.Replace(
		second["demo/templates/configmap.yaml"],
		"payload: first",
		"payload: second",
		1,
	)
	registry := testutil.NewChartRegistry(t, []map[string]string{first, second}, false)
	trees := inlineGenerationTrees(t, mode, registry)
	id := testutil.HelmReleaseID("apps", "demo")
	sourceID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "apps-demo",
	}
	var expected [2][]map[string]any
	for i, tree := range trees {
		if mode == "upstream" && i == 1 {
			registry.TagGeneration.Store(1)
		}
		tree.ID.Side = manifest.TreeHead
		full, err := RenderTrees(
			t.Context(),
			[]Tree{
				tree,
			},
			Config{
				RecordWork:  true,
				Concurrency: 2,
				CacheDir:    t.TempDir(),
				SourceRetry: sourcecache.RetryConfig{
					Attempts: 1,
				},
			},
		)
		assert.Equal(t, err != nil, mode == "failed_head" && i == 1)
		expected[i] = outputByParent(full[0].Result.Manifests)[id]
	}
	if mode == "upstream" {
		assert.Equal(t, expected[0][0]["data"].(map[string]any)["payload"], any("first"))
		assert.Equal(t, expected[1][0]["data"].(map[string]any)["payload"], any("second"))
	}
	assertions := inlineGenerationAssertions{
		mode:     mode,
		registry: registry,
		owner:    id,
		source:   sourceID,
		expected: expected,
	}
	var previous []byte
	for round := range 2 {
		requests := registry.Requests.Load()
		runs, err := RenderTrees(
			t.Context(),
			trees,
			Config{
				RecordWork:  true,
				Concurrency: 2,
				CacheDir:    t.TempDir(),
				SourceRetry: sourcecache.RetryConfig{
					Attempts: 1,
				},
			},
		)
		assert.FatalIf(t, (err != nil) != (mode == "failed_head"), "paired render error=%v", err)
		assert.Equal(t, len(runs), 2)
		var joint manifest.WorkSummary
		for i, run := range runs {
			joint = append(joint, run.Result.Work...)
			assertions.assertSide(t, &run, i)
		}
		switch mode {
		case "no_op", "upstream":
			assert.Equal(t, registry.Requests.Load(), requests)
		}
		assert.Equal(
			t,
			workCounts(joint, id, manifest.WorkHelm),
			manifest.WorkCounts{Attempts: tc.helm, Completed: tc.helm},
		)
		assertions.assertSourceWork(t, joint, &tc.cache, &tc.acquisition)
		body, err := json.Marshal(
			[][]manifest.DocumentBatch{runs[0].Result.Manifests, runs[1].Result.Manifests},
		)
		assert.NoError(t, err)
		if round != 0 {
			assert.Equal(t, string(body), string(previous))
		}
		previous = body
		if mode == "metadata" || mode == "interval" {
			assert.Diff(t, expected[0], expected[1])
		}
		assert.FatalIf(t, mode != "no_op" && mode != "upstream" &&
			runs[0].Store().
				GetObject(sourceID) ==
				runs[1].Store().
					GetObject(sourceID),
			"%v", "comparison sides shared a generated declaration")
	}
}

func writeInlineChartReference(t *testing.T, root string, template map[string]any) {
	t.Helper()
	sourceSpec := manifest.DeepCopyMap(template)
	if sourceSpec["layerSelector"] == nil {
		sourceSpec["layerSelector"] = map[string]any{
			"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip", "operation": "copy",
		}
	}
	source := map[string]any{
		"apiVersion": "source.toolkit.fluxcd.io/v1",
		"kind":       "OCIRepository",
		"metadata":   map[string]any{"name": "chart", "namespace": "apps"},
		"spec":       sourceSpec,
	}
	body, err := yaml.Marshal(source)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "apps/source.yaml", string(body))
}

func inlineGenerationTrees(t *testing.T, mode string, registry *testutil.ChartRegistry) []Tree {
	t.Helper()
	trees := make([]Tree, 0, 2)
	for i, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
		inlineChartRepositoryResult2 := inlineChartRepository(
			t,
			registry.URL,
			map[string]any{"ref": map[string]any{"tag": "0.1.0"}},
		)
		root, doc := inlineChartRepositoryResult2.root, inlineChartRepositoryResult2.document
		sibling := manifest.DeepCopyMap(doc)
		sibling["metadata"].(map[string]any)["name"] = "sibling"
		siblingSpec := sibling["spec"].(map[string]any)
		siblingSpec["releaseName"] = "unchanged"
		siblingSpec["chart"].(map[string]any)["spec"].(map[string]any)["ref"] = map[string]any{
			"tag": "0.1.0",
		}
		body, err := yaml.Marshal(sibling)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "cluster/sibling.yaml", string(body))
		spec := doc["spec"].(map[string]any)
		chart := spec["chart"].(map[string]any)
		sourceSpec := chart["spec"].(map[string]any)
		switch mode {
		case "tag":
			sourceSpec["ref"] = map[string]any{"tag": []string{"0.1.0", "next"}[i]}
		case "digest":
			sourceSpec["ref"] = map[string]any{"digest": registry.Digests[i]}
		case "layer_selector":
			sourceSpec["layerSelector"] = map[string]any{
				"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
				"operation": []string{
					"copy",
					"extract",
				}[i],
			}
		case "helm_chart":
			repository := map[string]any{
				"apiVersion": "source.toolkit.fluxcd.io/v1",

				"kind": "HelmRepository",

				"metadata": map[string]any{
					"name": "repository",

					"namespace": "apps",
				},

				"spec": map[string]any{
					"url": strings.TrimSuffix(
						registry.URL,
						"/demo",
					),

					"type": "oci",

					"insecure": true,
				},
			}
			body, err = yaml.Marshal(repository)
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "apps/source.yaml", string(body))
			testutil.WriteFile(
				t,
				root,
				"apps/kustomization.yaml",
				"resources: [release.yaml, source.yaml]\n",
			)
			if side == manifest.TreeBase {
				chart["kind"] = manifest.KindHelmChart
				chart["spec"] = map[string]any{
					"chart":    "demo",
					"version":  "0.1.0",
					"interval": "1m",
					"sourceRef": map[string]any{
						"kind": "HelmRepository",
						"name": "repository",
					},
				}
			}
		}
		if side == manifest.TreeHead {
			switch mode {
			case "failed_head":
				sourceSpec["ref"] = map[string]any{"tag": "missing"}
			case "url":
				sourceSpec["url"] = registry.URL + "-other"
			case "metadata":
				chart["metadata"].(map[string]any)["labels"] = map[string]any{"custom": "changed"}
			case "interval":
				sourceSpec["interval"] = "2m"
			case "chart_ref":
				sourceSpec["layerSelector"] = map[string]any{
					"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip", "operation": "copy",
				}
				source := map[string]any{
					"apiVersion": "source.toolkit.fluxcd.io/v1",
					"kind":       "OCIRepository",
					"metadata": map[string]any{
						"name":      "chart",
						"namespace": "apps",
					},
					"spec": sourceSpec,
				}
				body, err = yaml.Marshal(source)
				assert.NoError(t, err)
				testutil.WriteFile(t, root, "apps/source.yaml", string(body))
				testutil.WriteFile(
					t,
					root,
					"apps/kustomization.yaml",
					"resources: [release.yaml, source.yaml]\n",
				)
				delete(spec, "chart")
				spec["chartRef"] = map[string]any{"kind": "OCIRepository", "name": "chart"}
			}
		}
		body, err = yaml.Marshal(doc)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "apps/release.yaml", string(body))
		trees = append(trees, Tree{
			ID: manifest.TreeID{
				Cluster: ".",
				Side:    side,
			},
			Path:     filepath.Join(root, "cluster"),
			RepoRoot: root,
		})
	}
	return trees
}

type inlineGenerationAssertions struct {
	mode          string
	registry      *testutil.ChartRegistry
	owner, source manifest.NamedResource
	expected      [2][]map[string]any
}

func (f *inlineGenerationAssertions) assertSourceWork(
	t *testing.T,
	summary manifest.WorkSummary,
	wantCache, wantAcquisition *manifest.WorkCounts,
) {
	t.Helper()
	sources := []manifest.NamedResource{f.source}
	if f.mode == "chart_ref" {
		reference := f.source
		reference.Name = "chart"
		sources = append(sources, reference)
	}
	var cached, acquired manifest.WorkCounts
	for _, source := range sources {
		lookup := workCounts(summary, source, manifest.WorkCache)
		acquisition := workCounts(summary, source, manifest.WorkAcquisition)
		cached.Add(&lookup)
		acquired.Add(&acquisition)
		if f.mode != "chart_ref" {
			continue
		}
		// Both owners share one content key, so the first caller owns the acquisition.
		if acquisition.Attempts == 0 {
			assert.Equal(t, acquisition, manifest.WorkCounts{})
			assert.Equal(t, lookup, manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1})
		} else {
			assert.Equal(t, acquisition, manifest.WorkCounts{Attempts: 1, Completed: 1, RemoteTransfers: 1})
			assert.Equal(t, lookup, manifest.WorkCounts{Attempts: 1, Completed: 1})
		}
	}
	assert.Equal(t, cached, *wantCache)
	assert.Equal(t, acquired, *wantAcquisition)
}

func (f *inlineGenerationAssertions) assertSide(t *testing.T, run *Rendered, i int) {
	t.Helper()
	mode, registry, id, sourceID, expected := f.mode, f.registry, f.owner, f.source, f.expected

	assert.Equal(t, run.filter.ShouldReconcile(id), mode != "no_op" && mode != "upstream")
	assert.FatalIf(
		t,
		run.filter.ShouldReconcile(testutil.HelmReleaseID("apps", "sibling")),
		"%v",
		"template declaration selected unchanged sibling",
	)
	if mode == "no_op" || mode == "upstream" {
		f.assertSkipped(t, run)
		return
	}
	currentSource := sourceID
	if mode == "helm_chart" && i == 0 {
		currentSource.Kind = manifest.KindHelmChart
	}
	if mode == "chart_ref" && i == 1 {
		currentSource.Name = "chart"
	}
	source := run.Store().GetObject(currentSource)
	assert.FatalIf(
		t,
		source == nil || run.Store().ReportHidden(currentSource) != (mode != "chart_ref" || i != 1),
		"%v",
		"side lost generated declaration or provenance",
	)
	if mode == "failed_head" && i == 1 {
		assert.FatalIf(
			t,
			run.Err == nil || run.Result.Failed[id].Status != store.StatusFailed,
			"%v",
			"head failure did not remain side-local",
		)
		return
	}
	assert.NoError(t, run.Err)
	content := 0
	if mode == "tag" || mode == "digest" {
		content = i
	}
	artifact, ok := run.Store().GetArtifact(currentSource, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok ||
		currentSource.Kind == manifest.KindOCIRepository &&
			artifact.Digest != registry.Digests[content], "side %d borrowed another generation: %+v", i, artifact)
	docs := outputByParent(run.Result.Manifests)[id]
	assert.FatalIf(t, len(docs) != 1, "side %d output=%v", i, docs)
	data := docs[0]["data"].(map[string]any)
	assert.Equal(t, data["payload"], any([]string{"first", "second"}[content]))
	version := "0.1.0"
	if currentSource.Kind == manifest.KindOCIRepository {
		version += "+" + strings.TrimPrefix(registry.Digests[content], "sha256:")[:12]
	}
	assert.Equal(t, data["version"], any(version))
	assert.Diff(t, docs, expected[i])
	f.assertRecords(t, run, currentSource)
	assert.Equal(
		t,
		workCounts(
			run.Result.Work,
			testutil.HelmReleaseID(
				"apps",
				"sibling",
			),
			manifest.WorkHelm,
		),
		manifest.WorkCounts{},
	)
}

func (f *inlineGenerationAssertions) assertRecords(t *testing.T, run *Rendered, currentSource manifest.NamedResource) {
	t.Helper()
	id := f.owner
	for recordIndex := range run.Result.Work {
		record := &run.Result.Work[recordIndex]
		assert.Equal(t, record.Key.Tree, run.ID)
		if f.mode == "chart_ref" && record.Key.Owner == currentSource &&
			(record.Key.Phase == manifest.WorkCache || record.Key.Phase == manifest.WorkAcquisition) {
			assert.Equal(t, record.Key.Source, currentSource)
			assert.Equal(t, record.Key.Input != "", true)
		}
		if record.Key.Phase == manifest.WorkHelm ||
			record.Key.Phase == manifest.WorkChartDecode ||
			record.Key.Phase == manifest.WorkPostRender {
			assert.Equal(t, record.Key.Owner, id)
			assert.Equal(t, record.Key.Source, currentSource)
			assert.Equal(t, record.Key.Input != "", true)
		}
	}
}

func (f *inlineGenerationAssertions) assertSkipped(t *testing.T, run *Rendered) {
	t.Helper()
	id := f.owner

	assert.FatalIf(t, len(outputByParent(run.Result.Manifests)[id]) != 0 ||
		run.Store().GetObject(f.source) != nil, "%v", "unchanged template published generated or rendered output")
	for _, phase := range []manifest.WorkPhase{
		manifest.WorkHelm,
		manifest.WorkPostRender,
		manifest.WorkChartDecode,
		manifest.WorkIndexDecode,
		manifest.WorkEvaluation,
	} {
		assert.Diff(t, witnessWork(run.Result.Work, phase), map[manifest.NamedResource]uint64{})
	}
	for recordIndex := range run.Result.Work {
		record := &run.Result.Work[recordIndex]
		assert.Equal(t, record.Counts.RemoteTransfers, uint64(0))
	}
}

type inlineChartFixture struct {
	root     string
	document map[string]any
}
