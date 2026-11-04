package orchestrator

import (
	"compress/gzip"
	"encoding/json"
	"io/fs"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"sigs.k8s.io/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type inlineChartRegistry struct {
	url, digest   string
	authenticated atomic.Int64
	digests       []string
}

func newInlineChartRegistry(t *testing.T, files []map[string]string, auth bool) *inlineChartRegistry {
	t.Helper()
	blobs := map[string][]byte{}
	manifests := map[string][]byte{}
	registry := &inlineChartRegistry{}
	descriptor := func(body []byte, mediaType string) ocispec.Descriptor {
		hash := digest.FromBytes(body)
		blobs[hash.String()] = body
		return ocispec.Descriptor{MediaType: mediaType, Digest: hash, Size: int64(len(body))}
	}
	for i, files := range files {
		chart := testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)
		image := ocispec.Manifest{
			SchemaVersion: 2,
			MediaType:     ocispec.MediaTypeImageManifest,
			Config:        descriptor([]byte(`{}`), "application/vnd.cncf.helm.config.v1+json"),
			Layers: []ocispec.Descriptor{
				descriptor([]byte("unrelated provenance layer"), "application/vnd.cncf.helm.chart.provenance.v1.prov"),
				descriptor(chart, "application/vnd.cncf.helm.chart.content.v1.tar+gzip"),
			},
		}
		body, err := json.Marshal(image)
		assert.NoError(t, err)
		hash := digest.FromBytes(body).String()
		manifests[hash] = body
		registry.digests = append(registry.digests, hash)
		if i == 0 {
			registry.digest = hash
			manifests["0.1.0"] = body
		} else {
			manifests["next"] = body
		}
	}
	return newInlineChartRegistryTail(
		t,
		&newInlineChartRegistryTailInputs{auth: &auth, registry: &registry, manifests: &manifests, blobs: &blobs},
	)
}

func newInlineChartRegistryTail(t *testing.T, tailCaseInput *newInlineChartRegistryTailInputs) *inlineChartRegistry {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if *tailCaseInput.auth {
			user, password, ok := r.BasicAuth()
			if !ok || user != "fixture" || password != "fixture-password" {
				w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			(*tailCaseInput.registry).
				authenticated.Add(1)
		}
		switch {
		case strings.Contains(r.URL.Path, "/missing/"):
			http.NotFound(w, r)
		case r.URL.Path == "/v2/":
		case strings.Contains(r.URL.Path, "/manifests/"):
			_, ref, _ := strings.CutLast(r.URL.Path, "/")
			body, ok := (*tailCaseInput.manifests)[ref]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			if r.Method != http.MethodHead {
				_, _ = w.Write(body)
			}
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, key, _ := strings.CutLast(r.URL.Path, "/")
			if blob, ok := (*tailCaseInput.blobs)[key]; ok {
				_, _ = w.Write(blob)
			} else {
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	(*tailCaseInput.registry).
		url = "oci://" + strings.TrimPrefix(server.URL, "https://") + "/demo"
	return (*tailCaseInput.registry)
}

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

type inlineChartFixture struct {
	root     string
	document map[string]any
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
		},
		)
	}
}

func TestRenderTrees_InlineOCIIndependentGenerations(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"tag", "digest", "failed_head", "url", "layer_selector", "metadata", "interval", "helm_chart", "chart_ref", "no_op"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testRenderTreesInlineOCIIndependentGenerationsCase(
				t,
				&mode,
			)
		},
		)
	}
}

func testRenderInlineOCIRegistryParityCase(t *testing.T, testCaseInput *struct {
	name string

	digest  bool
	extract bool
	auth    bool
	disable bool
	fail    bool
},
) {
	t.Helper()

	registry := newInlineChartRegistry(t, []map[string]string{inlineChartFiles(t)}, testCaseInput.auth)
	fields := map[string]any{}
	if testCaseInput.digest {
		fields["ref"] = map[string]any{"digest": registry.digest}
	}
	if testCaseInput.extract {
		fields["layerSelector"] = map[string]any{
			"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			"operation": "extract",
		}
	}
	if testCaseInput.auth {
		fields["secretRef"] = map[string]any{"name": "pull"}
	}
	url := registry.url
	if testCaseInput.fail {
		url += "/missing"
	}
	var expected []map[string]any
	testRenderInlineOCIRegistryParityCaseSegment(
		t,
		&testRenderInlineOCIRegistryParityCaseSegmentInputs{
			testCaseInput: &testCaseInput,
			registry:      &registry,
			fields:        &fields,
			url:           &url,
			expected:      &expected,
		},
	)
	assert.FatalIf(
		t, testCaseInput.auth && registry.authenticated.Load() == 0, "%v",
		"inline credentials were not used by acquisition",
	)
}

func testRenderTreesInlineOCIIndependentGenerationsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	first := inlineChartFiles(t)
	second := maps.Clone(first)
	second["demo/templates/configmap.yaml"] = strings.Replace(
		second["demo/templates/configmap.yaml"],
		"payload: first",
		"payload: second",
		1,
	)
	registry := newInlineChartRegistry(t, []map[string]string{first, second}, false)
	trees := make([]Tree, 0, 2)
	for i, side := range []manifest.TreeSide{manifest.TreeBase, manifest.TreeHead} {
		inlineChartRepositoryResult1 := inlineChartRepository(
			t,
			registry.url,
			map[string]any{"ref": map[string]any{"tag": "0.1.0"}},
		)
		root, doc := inlineChartRepositoryResult1.root, inlineChartRepositoryResult1.document
		sibling := manifest.DeepCopyMap(doc)
		sibling["metadata"].(map[string]any)["name"] = "sibling"
		siblingSpec := sibling["spec"].(map[string]any)
		siblingSpec["releaseName"] = "unchanged"
		siblingSpec["chart"].(map[string]any)["spec"].(map[string]any)["ref"] = map[string]any{"tag": "0.1.0"}
		body, err := yaml.Marshal(sibling)
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "cluster/sibling.yaml", string(body))
		spec := doc["spec"].(map[string]any)
		chart := spec["chart"].(map[string]any)
		sourceSpec := chart["spec"].(map[string]any)
		switch *testCaseInput {
		case "tag":
			sourceSpec["ref"] = map[string]any{"tag": []string{"0.1.0", "next"}[i]}
		case "digest":
			sourceSpec["ref"] = map[string]any{"digest": registry.digests[i]}
		case "layer_selector":
			sourceSpec["layerSelector"] = map[string]any{
				"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
				"operation": []string{"copy", "extract"}[i],
			}
		case "helm_chart":
			repository := map[string]any{
				"apiVersion": "source.toolkit.fluxcd.io/v1",
				"kind":       "HelmRepository",
				"metadata":   map[string]any{"name": "repository", "namespace": "apps"},
				"spec": map[string]any{
					"url":      strings.TrimSuffix(registry.url, "/demo"),
					"type":     "oci",
					"insecure": true,
				},
			}
			body, err = yaml.Marshal(repository)
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "apps/source.yaml", string(body))
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [release.yaml, source.yaml]\n")
			if side == manifest.TreeBase {
				chart["kind"] = manifest.KindHelmChart
				chart["spec"] = map[string]any{
					"chart":     "demo",
					"version":   "0.1.0",
					"interval":  "1m",
					"sourceRef": map[string]any{"kind": "HelmRepository", "name": "repository"},
				}
			}
		}
		if side == manifest.TreeHead {
			switch *testCaseInput {
			case "failed_head":
				sourceSpec["ref"] = map[string]any{"tag": "missing"}
			case "url":
				sourceSpec["url"] = registry.url + "-other"
			case "metadata":
				chart["metadata"].(map[string]any)["labels"] = map[string]any{"custom": "changed"}
			case "interval":
				sourceSpec["interval"] = "2m"
			case "chart_ref":
				sourceSpec["layerSelector"] = map[string]any{
					"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
					"operation": "copy",
				}
				source := map[string]any{
					"apiVersion": "source.toolkit.fluxcd.io/v1",
					"kind":       "OCIRepository",
					"metadata":   map[string]any{"name": "chart", "namespace": "apps"},
					"spec":       sourceSpec,
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
		trees = append(
			trees,
			Tree{
				ID:       manifest.TreeID{Cluster: ".", Side: side},
				Path:     filepath.Join(root, "cluster"),
				RepoRoot: root,
			},
		)
	}
	id := testutil.HelmReleaseID("apps", "demo")
	sourceID := manifest.NamedResource{
		Group:     manifest.SourceDomain,
		Kind:      manifest.KindOCIRepository,
		Namespace: "apps",
		Name:      "apps-demo",
	}
	var expected [2][]map[string]any
	testRenderTreesInlineOCIIndependentGenerationsCaseSegment(
		t,
		&testRenderTreesInlineOCIIndependentGenerationsCaseSegmentInputs{
			testCaseInput: &testCaseInput,
			registry:      &registry,
			trees:         &trees,
			id:            &id,
			sourceID:      &sourceID,
			expected:      &expected,
		},
	)
}

type newInlineChartRegistryTailInputs struct {
	auth      *bool
	registry  **inlineChartRegistry
	manifests *map[string][]byte
	blobs     *map[string][]byte
}

type testRenderTreesInlineOCIIndependentGenerationsCaseSegmentInputs struct {
	testCaseInput **string
	registry      **inlineChartRegistry
	trees         *[]Tree
	id            *manifest.NamedResource
	sourceID      *manifest.NamedResource
	expected      *[2][]map[string]any
}

func testRenderTreesInlineOCIIndependentGenerationsCaseSegment(
	t *testing.T,
	statementInput *testRenderTreesInlineOCIIndependentGenerationsCaseSegmentInputs,
) {
	t.Helper()
	for round := range 2 {
		runs, err := RenderTrees(
			t.Context(),
			(*statementInput.trees),
			Config{Concurrency: 2, CacheDir: t.TempDir(), SourceRetry: sourcecache.RetryConfig{Attempts: 1}},
		)
		assert.FatalIf(
			t,
			(err != nil) != ((*(*statementInput.testCaseInput)) == "failed_head"),
			"paired render error=%v",
			err,
		)
		assert.Equal(t, len(runs), 2)
		for i, run := range runs {
			assert.Equal(
				t,
				run.filter.ShouldReconcile((*statementInput.id)),
				(*(*statementInput.testCaseInput)) != "no_op",
			)
			assert.FatalIf(
				t,
				run.filter.ShouldReconcile(testutil.HelmReleaseID("apps", "sibling")),
				"%v",
				"template declaration selected unchanged sibling",
			)
			if (*(*statementInput.testCaseInput)) == "no_op" {
				assert.FatalIf(t, len(outputByParent(run.Result.Manifests)[(*statementInput.id)]) != 0 ||
					run.Store().
						GetObject((*statementInput.sourceID)) !=
						nil, "%v", "unchanged template published generated or rendered output")
				continue
			}
			currentSource := statementInput.assertSourceGeneration(t, i, &run)
			if (*(*statementInput.testCaseInput)) == "failed_head" && i == 1 {
				assert.FatalIf(
					t,
					run.Err == nil || run.Result.Failed[(*statementInput.id)].Status != store.StatusFailed,
					"%v",
					"head failure did not remain side-local",
				)
				continue
			}
			assert.NoError(t, run.Err)
			docs := statementInput.assertGenerationContent(t, i, &run, currentSource)
			if round == 0 {
				statementInput.expected[i] = docs
			} else {
				assert.Diff(t, docs, statementInput.expected[i])
			}
		}
		statementInput.assertPair(t, runs)
	}
}

type testRenderInlineOCIRegistryParityCaseSegmentInputs struct {
	testCaseInput **struct {
		name    string
		digest  bool
		extract bool
		auth    bool
		disable bool
		fail    bool
	}
	registry **inlineChartRegistry
	fields   *map[string]any
	url      *string
	expected *[]map[string]any
}

func testRenderInlineOCIRegistryParityCaseSegment(
	t *testing.T,
	statementInput *testRenderInlineOCIRegistryParityCaseSegmentInputs,
) {
	t.Helper()
	for _, entry := range []string{"template", "chartRef"} {
		inlineChartRepositoryResult2 := inlineChartRepository(t, (*statementInput.url), (*statementInput.fields))
		root, doc := inlineChartRepositoryResult2.root, inlineChartRepositoryResult2.document
		spec := doc["spec"].(map[string]any)
		template := spec["chart"].(map[string]any)["spec"].(map[string]any)
		resources := "resources: [release.yaml"
		if entry == "chartRef" {
			sourceSpec := manifest.DeepCopyMap(template)
			if sourceSpec["layerSelector"] == nil {
				sourceSpec["layerSelector"] = map[string]any{
					"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
					"operation": "copy",
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
			resources += ", source.yaml"
			delete(spec, "chart")
			spec["chartRef"] = map[string]any{"kind": "OCIRepository", "name": "chart"}
		}
		if (*statementInput.testCaseInput).auth {
			resources += ", pull.yaml"
			host := strings.TrimSuffix(strings.TrimPrefix((*statementInput.registry).url, "oci://"), "/demo")
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
			HelmOptions: helm.Options{DisableChartDigestTracking: (*statementInput.testCaseInput).disable},
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
		if (*statementInput.testCaseInput).fail {
			if renderErr == nil || result.Failed[id].Status != store.StatusFailed {
				t.Fatalf("registry failure lost release diagnostic: %v %+v",
					renderErr,
					result.Failed,
				)
			}
			continue
		}
		assert.NoError(t, renderErr)
		docs := outputByParent(result.Manifests)[id]
		statementInput.assertChartOutput(t, docs)
		if entry == "template" {
			(*statementInput.expected) = docs
		} else {
			assert.Diff(t, docs, (*statementInput.expected))
		}
		assert.FatalIf(
			t,
			len(o.store.ListObjects(manifest.SourceDomain, manifest.KindHelmChart)) != 0,
			"%v",
			"OCI render synthesized a HelmChart",
		)
	}
}

func (s *testRenderTreesInlineOCIIndependentGenerationsCaseSegmentInputs) assertSourceGeneration(
	t *testing.T,
	i int,
	run *Rendered,
) manifest.NamedResource {
	t.Helper()
	currentSource := (*s.sourceID)
	if (*(*s.testCaseInput)) == "helm_chart" && i == 0 {
		currentSource.Kind = manifest.KindHelmChart
	}
	if (*(*s.testCaseInput)) == "chart_ref" && i == 1 {
		currentSource.Name = "chart"
	}
	source := run.Store().GetObject(currentSource)
	assert.FatalIf(
		t,
		source == nil ||
			run.Store().
				ReportHidden(currentSource) !=
				((*(*s.testCaseInput)) != "chart_ref" || i != 1),
		"%v",
		"side lost generated declaration or provenance",
	)
	return currentSource
}

func (s *testRenderTreesInlineOCIIndependentGenerationsCaseSegmentInputs) assertGenerationContent(
	t *testing.T,
	i int,
	run *Rendered,
	currentSource manifest.NamedResource,
) []map[string]any {
	t.Helper()
	content := 0
	if (*(*s.testCaseInput)) == "tag" || (*(*s.testCaseInput)) == "digest" {
		content = i
	}
	artifact, ok := run.Store().GetArtifact(currentSource, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(t, !ok ||
		currentSource.Kind == manifest.KindOCIRepository &&
			artifact.Digest != (*s.registry).digests[content], "side %d borrowed another generation: %+v", i, artifact)
	docs := outputByParent(run.Result.Manifests)[(*s.id)]
	assert.FatalIf(t, len(docs) != 1, "side %d output=%v", i, docs)
	data := docs[0]["data"].(map[string]any)
	assert.Equal(t, data["payload"], any([]string{"first", "second"}[content]))
	version := "0.1.0"
	if currentSource.Kind == manifest.KindOCIRepository {
		version += "+" + strings.TrimPrefix((*s.registry).digests[content], "sha256:")[:12]
	}
	assert.Equal(t, data["version"], any(version))
	return docs
}

func (s *testRenderTreesInlineOCIIndependentGenerationsCaseSegmentInputs) assertPair(t *testing.T, runs []Rendered) {
	t.Helper()
	if (*(*s.testCaseInput)) == "metadata" || (*(*s.testCaseInput)) == "interval" {
		assert.Diff(t, s.expected[0], s.expected[1])
	}
	assert.FatalIf(
		t, (*(*s.testCaseInput)) !=
			"no_op" && runs[0].Store().GetObject((*s.sourceID)) == runs[1].Store().GetObject((*s.sourceID)), "%v",
		"comparison sides shared a generated declaration",
	)
}

func (s *testRenderInlineOCIRegistryParityCaseSegmentInputs) assertChartOutput(t *testing.T, docs []map[string]any) {
	t.Helper()
	assert.FatalIf(t, len(docs) != 1, "chart output=%+v", docs)
	data := docs[0]["data"].(map[string]any)
	version := "0.1.0"
	if !(*s.testCaseInput).disable {
		version += "+" + strings.TrimPrefix((*s.registry).digest, "sha256:")[:12]
	}
	assert.Equal(t, data["version"], any(version))
	assert.Equal(t, data["release"], any("visible-release"))
	assert.Equal(t, data["payload"], any("first"))
}
