package scope_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	digest "github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_ChildKustomizationIdentity(t *testing.T) {
	for _, tc := range []struct{ name, target, authored, namespace string }{
		{"bare", "", "", ""},
		{"target", "  targetNamespace: flux-system\n", "", "flux-system"},
		{"authored", "", ", namespace: flux-system", "flux-system"},
	} {
		t.Run(tc.name, func(t *testing.T) { e2EChildKustomizationIdentityName(t, &tc) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_RuntimeOwnerNamespace(t *testing.T) {
	for _, namespace := range []string{"", "apps"} {
		name := "omitted"
		if namespace != "" {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) { e2ERuntimeOwnerNamespaceName(t, &namespace) })
	}
}

func runtimeOwnerNamespaceOutput(t *testing.T, args ...string) string {
	t.Helper()
	requireCLIOKResult1 := harness.RequireCLIOK(t, args...)
	out, stderr := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
	requireCLIOKResult2 := harness.RequireCLIOK(t, args...)
	again, diagnostics := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
	assert.Equal(t, again, out)
	assert.Equal(t, stderr, "")
	assert.Equal(t, diagnostics, "")
	return out
}

func runtimeOwnerNamespaceFixture(t *testing.T, namespace, value string) string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/repo.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: flux-system, namespace: flux-system}\nspec: {url: 'https://example.invalid/cluster.git'}\n",
	)
	owner := testutil.KustomizationYAML(
		"apps",
		"flux-system",
		"  path: ./apps\n  targetNamespace: apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"flux/owners.yaml",
		"apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: owners, namespace: flux-system}\nspec:\n  resourcesTemplate: |\n    "+strings.ReplaceAll(
			strings.TrimSpace(owner),
			"\n",
			"\n    ",
		)+"\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/ks.yaml",
		testutil.KustomizationYAML(
			"demo",
			namespace,
			"  path: ./workload\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "workload/kustomization.yaml", "namespace: apps\nresources: [hr.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"workload/hr.yaml",
		"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  values: {value: "+value+"}\n",
	)
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/demo/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-chart\n  namespace: {{ .Release.Namespace }}\ndata: {value: {{ .Values.value }}}\n",
	)
	return root
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_NamespaceOrphans(t *testing.T) {
	for _, scenario := range []string{"sibling", "explicit", "standalone", "shared"} {
		t.Run(scenario, func(t *testing.T) { e2ENamespaceOrphansScenario(t, &scenario) })
	}
	t.Run("terminal replay", func(t *testing.T) { e2ENamespaceOrphansTerminalReplay(t) })
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_NamespaceOrphans_FailingOwner(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprintf("concurrency%d", concurrency), func(t *testing.T) {
			namespaceOrphanFixtureResult1 := namespaceOrphanFixture(t, "failing")
			root, cache, _ := namespaceOrphanFixtureResult1.root, namespaceOrphanFixtureResult1.cache, namespaceOrphanFixtureResult1.namespaces
			st := store.New()
			{
				_, err := discovery.Run(
					t.Context(),
					discovery.Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st},
				)
				assert.FatalIf(t, err != nil, "%v", err)
			}
			{
				releases := st.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease)
				assert.FatalIf(
					t,
					len(releases) != 0,
					"failed owner's raw releases admitted during discovery: %+v",
					releases,
				)
			}
			flags := []string{
				"--path",
				filepath.Join(root, "flux"),
				"--cache-dir",
				cache,
				"--concurrency",
				strconv.Itoa(concurrency),
				"--no-progress",
			}
			runCLIBuffersResult3 := harness.RunCLIBuffers(append([]string{"test", "all"}, flags...)...)
			out, stderr, code := runCLIBuffersResult3.Stdout, runCLIBuffersResult3.Stderr, runCLIBuffersResult3.Code
			assert.ErrorIf(t, code != 1 || !strings.Contains(out+stderr, "flux-system/owner") ||
				!strings.Contains(
					out+stderr,
					"no matches",
				), "owner build failure must remain visible: exit=%d stdout=%s stderr=%s", code, out, stderr)
			runCLIBuffersResult4 := harness.RunCLIBuffers(append([]string{"get", "hr", "-o", "json"}, flags...)...)
			out, stderr, code = runCLIBuffersResult4.Stdout, runCLIBuffersResult4.Stderr, runCLIBuffersResult4.Code
			assert.ErrorIf(t, code != 1, "failed owner get exit=%d stderr=%s", code, stderr)
			var releases []any
			assert.NoError(t, json.Unmarshal([]byte(out), &releases))
			assert.ErrorIf(t, len(releases) != 0, "failed owner's raw releases admitted in CLI collection: %s", out)
		})
	}
}

func namespaceOrphanSourceNamespace(scenario, namespace string) string {
	if scenario == "shared" {
		return "charts"
	}
	return namespace
}

type namespaceFixture struct {
	root       string
	cache      string
	namespaces []string
}

func namespaceOrphanFixture(t *testing.T, scenario string) namespaceFixture {
	t.Helper()
	for _, name := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"} {
		t.Setenv(name, "")
	}
	t.Setenv("NO_PROXY", "*")
	t.Setenv("no_proxy", "*")
	root, cache := t.TempDir(), t.TempDir()
	testutil.GitInit(t, root)
	namespaces := []string{"demo"}
	rawNS := ""
	if scenario == "explicit" {
		rawNS = ", namespace: demo"
	}
	bundle := `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: demo` + rawNS + `}
spec:
 interval: 10m
 chart:
  spec:
   chart: demo
   version: 0.1.0
   sourceRef: {kind: HelmRepository, name: demo}
---
apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: demo` + rawNS + `}
spec: {url: "https://charts.example.invalid", interval: 24h}
`
	if scenario == "shared" {
		bundle, _, _ = strings.Cut(bundle, "\n---\n")
		bundle = strings.Replace(
			bundle,
			"sourceRef: {kind: HelmRepository, name: demo}",
			"sourceRef: {kind: HelmRepository, name: demo, namespace: charts}",
			1,
		)
		testutil.WriteFile(t, root, "flux/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: demo, namespace: charts}
spec: {url: "https://charts.example.invalid", interval: 24h}
`)
	}
	testutil.WriteFile(t, root, "cluster/shared/demo/bundle.yaml", bundle)
	if scenario == "standalone" {
		testutil.WriteFile(t, root, "flux/kustomization.yaml", "resources: [../cluster/shared/demo]\n")
	}
	base := "namespace: demo\nresources: [bundle.yaml]\n"
	if scenario == "shared" {
		base = "resources: [bundle.yaml]\n"
		namespaces = []string{"demo-a", "demo-b"}
	}
	if scenario == "failing" {
		base += "patches:\n- patch: |-\n    apiVersion: v1\n    kind: ConfigMap\n    metadata: {name: absent}\n    data: {value: fixture}\n"
	}
	testutil.WriteFile(t, root, "cluster/shared/demo/kustomization.yaml", base)
	if scenario != "standalone" {
		for _, ns := range namespaces {
			owner, path := "owner", "cluster/per-cluster"
			overlay := "resources: [../shared/demo]\n"
			if scenario == "shared" {
				owner += "-" + ns
				path += "-" + ns
				overlay = "namespace: " + ns + "\n" + overlay
			}
			testutil.WriteFile(
				t,
				root,
				"flux/"+owner+".yaml",
				testutil.KustomizationYAML(
					owner,
					"flux-system",
					" interval: 10m\n path: ./"+path+"\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
				),
			)
			testutil.WriteFile(t, root, path+"/kustomization.yaml", overlay)
		}
	}
	archive := testutil.TarGz(t, map[string]string{
		"demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 0.1.0\n",
		"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-chart\n  namespace: {{ .Release.Namespace }}\ndata: {value: fixture}\n",
	}, 0o600, gzip.DefaultCompression)
	c, err := sourcecache.New(t.Context(), cache)
	assert.NoError(t, err)
	repository := &manifest.HelmRepository{URL: "https://charts.example.invalid"}
	chart := &manifest.HelmChartSource{Chart: "demo", Version: "0.1.0"}
	_, _, _, err = c.Blob(
		t.Context(),
		sourcecache.BlobKey{
			Content: struct {
				Repository any `json:"Repository"`
				Chart      any `json:"Chart"`
				Transport  any `json:"Transport"`
			}{repository.Content(), chart.Content(), nil},
			Required: []string{"chart-version", "chart-url"},
		},
		sourcecache.Policy{MaxAge: time.Hour},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, writeErr := w.Write(archive)
			return sourcecache.ProducerResult{
				Digest: digest.FromBytes(archive),
				Size:   int64(len(archive)),
				Annotations: map[string]string{
					"flate.chart-version": "0.1.0",
					"flate.chart-url":     "https://charts.example.invalid/demo-0.1.0.tgz",
				},
			}, writeErr
		},
	)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	return namespaceFixture{root: root, cache: cache, namespaces: namespaces}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_RuntimeOwnerNamespace_ChangedOnly(t *testing.T) {
	for _, transition := range []struct{ name, namespace, value string }{
		{"chart value", "", "after"},
		{"authored namespace", "apps", "before"},
	} {
		t.Run(transition.name, func(t *testing.T) { e2ERuntimeOwnerNamespaceChangedOnlyName(t, &transition) })
	}
}

func e2EChildKustomizationIdentityName(t *testing.T, tc *struct {
	name      string
	target    string
	authored  string
	namespace string
},
) {
	t.Helper()

	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(
		t,
		root,
		"flux/repo.yaml",
		"apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: flux-system, namespace: flux-system}\nspec: {url: 'https://example.invalid/cluster.git'}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"flux/parent.yaml",
		testutil.KustomizationYAML(
			"parent",
			"flux-system",
			"  path: ./apps\n",
		)+tc.target+"  sourceRef: {kind: GitRepository, name: flux-system}\n",
	)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [child.yaml, raw.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/child.yaml",
		testutil.KustomizationYAML(
			"child",
			strings.TrimPrefix(tc.authored, ", namespace: "),
			"  path: ./apps/child\n  sourceRef: {kind: GitRepository, name: flux-system}\n",
		),
	)
	rawVersions := []string{"example.test/v1", "other.test/v1"}
	rawBodies := []string{
		"apiVersion: example.test/v1\nkind: Kustomization\nmetadata: {name: child" + tc.authored + "}\nspec: {value: first}\n",
		"apiVersion: other.test/v1\nkind: Kustomization\nmetadata: {name: child" + tc.authored + "}\nspec: {value: second}\n",
	}
	testutil.WriteFile(t, root, "apps/raw.yaml", strings.Join(rawBodies, "---\n"))
	testutil.WriteFile(t, root, "apps/child/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/child/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output}\n",
	)
	flags := []string{
		"--path",
		filepath.Join(root, "flux"),
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"--no-progress",
	}
	wantCode, wantMaps := 0, 1
	if tc.namespace == "" {
		wantCode, wantMaps = 1, 0
	}
	runCLIBuffersResult5 := harness.RunCLIBuffers(append([]string{"get", "ks", "-o", "json"}, flags...)...)
	out, stderr, code := runCLIBuffersResult5.Stdout, runCLIBuffersResult5.Stderr, runCLIBuffersResult5.Code
	assert.ErrorIf(t, code != wantCode, "get exit=%d want=%d stderr=%s", code, wantCode, stderr)
	var objects []struct {
		Name      string `json:"Name"`
		Namespace string `json:"Namespace"`
		SourceRef struct {
			Kind      string `json:"Kind"`
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
		} `json:"SourceRef"`
	}
	{
		err := json.Unmarshal([]byte(out), &objects)
		assert.FatalIf(t, err != nil, "get output=%s: %v", out, err)
	}
	children := 0
	for _, obj := range objects {
		if obj.Name == "child" {
			children++
			assert.ErrorIf(
				t,
				obj.Namespace != tc.namespace,
				"child identity namespace=%q want=%q",
				obj.Namespace,
				tc.namespace,
			)
			assert.ErrorIf(
				t,
				obj.SourceRef.Kind != manifest.KindGitRepository || obj.SourceRef.Name != "flux-system" ||
					obj.SourceRef.Namespace != tc.namespace,
				"implicit source=%+v want GitRepository.source.toolkit.fluxcd.io/%s/flux-system",
				obj.SourceRef,
				tc.namespace,
			)
		}
	}
	assert.ErrorIf(t, children != 1, "child identities=%d want=1; output=%s", children, out)
	runCLIBuffersResult6 := harness.RunCLIBuffers(append([]string{"build", "all", "-o", "yaml"}, flags...)...)
	out, stderr, code = runCLIBuffersResult6.Stdout, runCLIBuffersResult6.Stderr, runCLIBuffersResult6.Code
	assert.ErrorIf(t, code != wantCode, "build exit=%d want=%d stderr=%s", code, wantCode, stderr)
	docs, err := testutil.SplitDocs([]byte(out))
	assert.NoError(t, err)
	children, maps := 0, 0
	raw := make(map[string]string)
	e2EChildKustomizationIdentityNameLoop(t, struct {
		docs     *[]map[string]any
		children *int
		raw      *map[string]string
		tc       **struct {
			name      string
			target    string
			authored  string
			namespace string
		}
		maps *int
	}{docs: &docs, children: &children, raw: &raw, tc: &tc, maps: &maps})
	assert.ErrorIf(
		t,
		len(raw) != 2 || raw["example.test/v1"] != "first" || raw["other.test/v1"] != "second",
		"raw stdout lost group-specific authored content: %v; output=%s",
		raw,
		out,
	)
	assert.ErrorIf(
		t,
		children != 1 || maps != wantMaps,
		"rendered children=%d ConfigMaps=%d want=1/%d; output=%s",
		children,
		maps,
		wantMaps,
		out,
	)
	runCLIBuffersResult7 := harness.RunCLIBuffers(append([]string{"test", "all"}, flags...)...)
	out, stderr, code = runCLIBuffersResult7.Stdout, runCLIBuffersResult7.Stderr, runCLIBuffersResult7.Code
	assert.ErrorIf(
		t,
		code != wantCode || strings.Contains(out+stderr, "blocked") != (wantCode == 1),
		"test exit=%d want=%d output=%s stderr=%s",
		code,
		wantCode,
		out,
		stderr,
	)
	assert.ErrorIf(t, strings.Contains(out+stderr, "Kustomization.example.test") ||
		strings.Contains(
			out+stderr,
			"Kustomization.other.test",
		), "raw lookalikes entered test status: output=%s stderr=%s", out, stderr)
	assert.ErrorIf(
		t,
		wantCode == 1 && !strings.Contains(out+stderr, "blocked by flux-system (not found)"),
		"bare child must block on its missing implicit source: output=%s stderr=%s",
		out,
		stderr,
	)
	if tc.name != "authored" {
		return
	}
	for _, order := range [][2]int{{0, 1}, {1, 0}} {
		for _, removed := range []int{0, 1} {
			t.Run(fmt.Sprintf("insert%d%d/remove%d", order[0], order[1], removed), func(t *testing.T) {
				e2EChildKustomizationIdentityNameCase2(t, &flags, &tc, struct {
					root        *string
					rawBodies   *[]string
					order       *[2]int
					rawVersions *[]string
					removed     *int
				}{root: &root, rawBodies: &rawBodies, order: &order, rawVersions: &rawVersions, removed: &removed})
			})
		}
	}
}

func e2ERuntimeOwnerNamespaceName(t *testing.T, namespace *string) {
	t.Helper()

	root := runtimeOwnerNamespaceFixture(t, (*namespace), "fixture")
	var rootReleases string
	for _, entry := range []string{root, filepath.Join(root, "flux")} {
		flags := []string{"--path", entry, "--cache-dir", "", "--concurrency", "2", "--no-progress"}
		out := runtimeOwnerNamespaceOutput(t, append([]string{"get", "hr", "-o", "json"}, flags...)...)
		var releases []struct {
			Name      string `json:"Name"`
			Namespace string `json:"Namespace"`
			SourceRef struct {
				Kind      string `json:"Kind"`
				Name      string `json:"Name"`
				Namespace string `json:"Namespace"`
			} `json:"SourceRef"`
		}
		assert.NoError(t, json.Unmarshal([]byte(out), &releases))
		assert.FatalIf(t, len(releases) != 1, "want exactly one release: %s", out)
		assert.Equal(t, releases[0].Name, "demo")
		assert.Equal(t, releases[0].Namespace, "apps")
		assert.Equal(t, releases[0].SourceRef.Kind, manifest.KindGitRepository)
		assert.Equal(t, releases[0].SourceRef.Name, "flux-system")
		assert.Equal(t, releases[0].SourceRef.Namespace, "flux-system")
		if rootReleases == "" {
			rootReleases = out
		} else {
			assert.Equal(t, out, rootReleases)
		}
		out = runtimeOwnerNamespaceOutput(t, append([]string{"build", "all"}, flags...)...)
		docs, err := testutil.SplitDocs([]byte(out))
		assert.NoError(t, err)
		counts := map[string]int{}
		for _, doc := range docs {
			kind := manifest.DocKind(doc)
			counts[kind]++
			metadata := manifest.DocMetadata(doc)
			name, ns := metadata.Name, metadata.Namespace
			if kind == manifest.KindKustomization && name == "demo" {
				assert.Equal(t, ns, "apps")
			}
			if kind == manifest.KindHelmRelease || kind == manifest.KindConfigMap {
				assert.Equal(t, ns, "apps")
				if kind == manifest.KindConfigMap {
					assert.Equal(t, name, "demo-chart")
					assert.Equal(t, doc["data"].(map[string]any)["value"], any("fixture"))
				}
			}
		}
		assert.Equal(t, counts[manifest.KindHelmRelease], 1)
		assert.Equal(t, counts[manifest.KindConfigMap], 1)
	}
}

func e2EChildKustomizationIdentityNameLoop(t *testing.T, loopInputs struct {
	docs     *[]map[string]any
	children *int
	raw      *map[string]string
	tc       **struct {
		name      string
		target    string
		authored  string
		namespace string
	}
	maps *int
},
) {
	t.Helper()

	for _, doc := range *loopInputs.docs {
		metadata2 := manifest.DocMetadata(doc)
		name, ns := metadata2.Name, metadata2.Namespace
		if manifest.DocKind(doc) == manifest.KindKustomization && name == "child" {
			version := manifest.DocAPIVersion(doc)
			if version == "kustomize.toolkit.fluxcd.io/v1" {
				(*loopInputs.children)++
			} else {
				spec, _ := doc["spec"].(map[string]any)
				value, _ := spec["value"].(string)
				{
					_, duplicate := (*loopInputs.raw)[version]
					assert.ErrorIf(t, duplicate, "duplicate raw document for %s", version)
				}
				(*loopInputs.raw)[version] = value
			}
			assert.ErrorIf(
				t,
				ns != (*loopInputs.tc).namespace,
				"child document namespace=%q want=%q",
				ns, (*loopInputs.tc).namespace,
			)
		}
		if manifest.DocKind(doc) == manifest.KindConfigMap && name == "child-output" {
			(*loopInputs.maps)++
		}
	}
}
