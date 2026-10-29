package scope

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
	digest "github.com/opencontainers/go-digest"
)

func TestE2E_ChildKustomizationIdentity(t *testing.T) {
	for _, tc := range []struct{ name, target, authored, namespace string }{
		{"bare", "", "", ""},
		{"target", "  targetNamespace: flux-system\n", "", "flux-system"},
		{"authored", "", ", namespace: flux-system", "flux-system"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "flux/repo.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: flux-system, namespace: flux-system}\nspec: {url: 'https://example.invalid/cluster.git'}\n")
			testutil.WriteFile(t, root, "flux/parent.yaml", testutil.KustomizationYAML("parent", "flux-system", "  path: ./apps\n")+tc.target+"  sourceRef: {kind: GitRepository, name: flux-system}\n")
			testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [child.yaml, raw.yaml]\n")
			testutil.WriteFile(t, root, "apps/child.yaml", testutil.KustomizationYAML("child", strings.TrimPrefix(tc.authored, ", namespace: "), "  path: ./apps/child\n  sourceRef: {kind: GitRepository, name: flux-system}\n"))
			rawVersions := []string{"example.test/v1", "other.test/v1"}
			rawBodies := []string{
				"apiVersion: example.test/v1\nkind: Kustomization\nmetadata: {name: child" + tc.authored + "}\nspec: {value: first}\n",
				"apiVersion: other.test/v1\nkind: Kustomization\nmetadata: {name: child" + tc.authored + "}\nspec: {value: second}\n",
			}
			testutil.WriteFile(t, root, "apps/raw.yaml", strings.Join(rawBodies, "---\n"))
			testutil.WriteFile(t, root, "apps/child/kustomization.yaml", "resources: [cm.yaml]\n")
			testutil.WriteFile(t, root, "apps/child/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: child-output}\n")
			flags := []string{"--path", filepath.Join(root, "flux"), "--cache-dir", t.TempDir(), "--concurrency", "2", "--no-progress"}
			wantCode, wantMaps := 0, 1
			if tc.namespace == "" {
				wantCode, wantMaps = 1, 0
			}
			out, stderr, code := harness.RunCLIBuffers(append([]string{"get", "ks", "-o", "json"}, flags...)...)
			if code != wantCode {
				t.Errorf("get exit=%d want=%d stderr=%s", code, wantCode, stderr)
			}
			var objects []struct {
				Name, Namespace string
				SourceRef       struct{ Kind, Name, Namespace string }
			}
			if err := json.Unmarshal([]byte(out), &objects); err != nil {
				t.Fatalf("get output=%s: %v", out, err)
			}
			children := 0
			for _, obj := range objects {
				if obj.Name == "child" {
					children++
					if obj.Namespace != tc.namespace {
						t.Errorf("child identity namespace=%q want=%q", obj.Namespace, tc.namespace)
					}
					if obj.SourceRef.Kind != manifest.KindGitRepository || obj.SourceRef.Name != "flux-system" || obj.SourceRef.Namespace != tc.namespace {
						t.Errorf("implicit source=%+v want GitRepository.source.toolkit.fluxcd.io/%s/flux-system", obj.SourceRef, tc.namespace)
					}
				}
			}
			if children != 1 {
				t.Errorf("child identities=%d want=1; output=%s", children, out)
			}
			out, stderr, code = harness.RunCLIBuffers(append([]string{"build", "all", "-o", "yaml"}, flags...)...)
			if code != wantCode {
				t.Errorf("build exit=%d want=%d stderr=%s", code, wantCode, stderr)
			}
			docs, err := testutil.SplitDocs([]byte(out))
			if err != nil {
				t.Fatal(err)
			}
			children, maps := 0, 0
			raw := make(map[string]string)
			for _, doc := range docs {
				name, ns := manifest.DocMetadata(doc)
				if manifest.DocKind(doc) == manifest.KindKustomization && name == "child" {
					version := manifest.DocAPIVersion(doc)
					if version == "kustomize.toolkit.fluxcd.io/v1" {
						children++
					} else {
						spec, _ := doc["spec"].(map[string]any)
						value, _ := spec["value"].(string)
						if _, duplicate := raw[version]; duplicate {
							t.Errorf("duplicate raw document for %s", version)
						}
						raw[version] = value
					}
					if ns != tc.namespace {
						t.Errorf("child document namespace=%q want=%q", ns, tc.namespace)
					}
				}
				if manifest.DocKind(doc) == manifest.KindConfigMap && name == "child-output" {
					maps++
				}
			}
			if len(raw) != 2 || raw["example.test/v1"] != "first" || raw["other.test/v1"] != "second" {
				t.Errorf("raw stdout lost group-specific authored content: %v; output=%s", raw, out)
			}
			if children != 1 || maps != wantMaps {
				t.Errorf("rendered children=%d ConfigMaps=%d want=1/%d; output=%s", children, maps, wantMaps, out)
			}
			out, stderr, code = harness.RunCLIBuffers(append([]string{"test", "all"}, flags...)...)
			if code != wantCode || strings.Contains(out+stderr, "blocked") != (wantCode == 1) {
				t.Errorf("test exit=%d want=%d output=%s stderr=%s", code, wantCode, out, stderr)
			}
			if strings.Contains(out+stderr, "Kustomization.example.test") || strings.Contains(out+stderr, "Kustomization.other.test") {
				t.Errorf("raw lookalikes entered test status: output=%s stderr=%s", out, stderr)
			}
			if wantCode == 1 && !strings.Contains(out+stderr, "blocked by flux-system (not found)") {
				t.Errorf("bare child must block on its missing implicit source: output=%s stderr=%s", out, stderr)
			}
			if tc.name != "authored" {
				return
			}
			for _, order := range [][2]int{{0, 1}, {1, 0}} {
				for _, removed := range []int{0, 1} {
					t.Run(fmt.Sprintf("insert%d%d/remove%d", order[0], order[1], removed), func(t *testing.T) {
						renderRaw := func() map[string]string {
							t.Helper()
							out, stderr, code := harness.RunCLIBuffers(append([]string{"build", "all", "-o", "yaml"}, flags...)...)
							if code != 0 {
								t.Fatalf("build exit=%d stderr=%s", code, stderr)
							}
							raw := make(map[string]string)
							for body := range strings.SplitSeq(out, "---\n") {
								docs, err := testutil.SplitDocs([]byte(body))
								if err != nil {
									t.Fatal(err)
								}
								for _, doc := range docs {
									version := manifest.DocAPIVersion(doc)
									name, ns := manifest.DocMetadata(doc)
									if manifest.DocKind(doc) != manifest.KindKustomization || name != "child" || version == "kustomize.toolkit.fluxcd.io/v1" {
										continue
									}
									if _, duplicate := raw[version]; duplicate || ns != tc.namespace {
										t.Fatalf("duplicate or wrong namespace for raw %s: %s", version, body)
									}
									raw[version] = body
								}
							}
							return raw
						}
						testutil.WriteFile(t, root, "apps/raw.yaml", rawBodies[order[0]]+"---\n"+rawBodies[order[1]])
						before := renderRaw()
						if len(before) != 2 || before[rawVersions[0]] == "" || before[rawVersions[1]] == "" {
							t.Fatalf("raw groups did not coexist: %v", before)
						}
						remaining := 1 - removed
						version := rawVersions[remaining]
						testutil.WriteFile(t, root, "apps/raw.yaml", rawBodies[remaining])
						after := renderRaw()
						if len(after) != 1 || after[version] != before[version] {
							t.Fatalf("withdrawal changed surviving bytes or retained stale output: before=%v after=%v", before, after)
						}
						nextVersion := strings.TrimSuffix(version, "/v1") + "/v2"
						testutil.WriteFile(t, root, "apps/raw.yaml", strings.Replace(rawBodies[remaining], version, nextVersion, 1))
						versioned := renderRaw()
						if len(versioned) != 1 || versioned[nextVersion] != strings.Replace(before[version], version, nextVersion, 1) {
							t.Fatalf("version-only change split identity or changed content: before=%v after=%v", before, versioned)
						}
					})
				}
			}
		})
	}
}

func TestE2E_RuntimeOwnerNamespace(t *testing.T) {
	for _, namespace := range []string{"", "apps"} {
		name := "omitted"
		if namespace != "" {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			root := runtimeOwnerNamespaceFixture(t, namespace)
			var rootReleases string
			for _, entry := range []string{root, filepath.Join(root, "flux")} {
				flags := []string{"--path", entry, "--cache-dir", "", "--concurrency", "2", "--no-progress"}
				out := runtimeOwnerNamespaceOutput(t, append([]string{"get", "hr", "-o", "json"}, flags...)...)
				var releases []struct {
					Name, Namespace string
					SourceRef       struct{ Kind, Name, Namespace string }
				}
				assert.NoError(t, json.Unmarshal([]byte(out), &releases))
				if len(releases) != 1 {
					t.Fatalf("want exactly one release: %s", out)
				}
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
					name, ns := manifest.DocMetadata(doc)
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
		})
	}
}

func runtimeOwnerNamespaceOutput(t *testing.T, args ...string) string {
	t.Helper()
	out, stderr := harness.RequireCLIOK(t, args...)
	again, diagnostics := harness.RequireCLIOK(t, args...)
	assert.Equal(t, again, out)
	assert.Equal(t, stderr, "")
	assert.Equal(t, diagnostics, "")
	return out
}

func runtimeOwnerNamespaceFixture(t *testing.T, namespace string) string {
	t.Helper()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "flux/repo.yaml", "apiVersion: source.toolkit.fluxcd.io/v1\nkind: GitRepository\nmetadata: {name: flux-system, namespace: flux-system}\nspec: {url: 'https://example.invalid/cluster.git'}\n")
	owner := testutil.KustomizationYAML("apps", "flux-system", "  path: ./apps\n  targetNamespace: apps\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n")
	testutil.WriteFile(t, root, "flux/owners.yaml", "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name: owners, namespace: flux-system}\nspec:\n  resourcesTemplate: |\n    "+strings.ReplaceAll(strings.TrimSpace(owner), "\n", "\n    ")+"\n")
	testutil.WriteFile(t, root, "apps/ks.yaml", testutil.KustomizationYAML("demo", namespace, "  path: ./workload\n  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
	testutil.WriteFile(t, root, "workload/kustomization.yaml", "namespace: apps\nresources: [hr.yaml]\n")
	testutil.WriteFile(t, root, "workload/hr.yaml", "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name: demo}\nspec:\n  interval: 10m\n  chart:\n    spec:\n      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n  values: {value: fixture}\n")
	testutil.WriteFile(t, root, "charts/demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 1.0.0\n")
	testutil.WriteFile(t, root, "charts/demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-chart\n  namespace: {{ .Release.Namespace }}\ndata: {value: {{ .Values.value }}}\n")
	return root
}

func TestE2E_NamespaceOrphans(t *testing.T) {
	for _, scenario := range []string{"sibling", "explicit", "standalone", "shared"} {
		t.Run(scenario, func(t *testing.T) {
			root, cache, namespaces := namespaceOrphanFixture(t, scenario)
			flags := []string{"--path", filepath.Join(root, "flux"), "--cache-dir", cache, "--concurrency", "2", "--no-progress"}
			getArgs := append([]string{"get", "hr", "-o", "json"}, flags...)
			hr, _ := harness.RequireCLIOK(t, getArgs...)
			var releases []struct {
				Name, Namespace string
				SourceRef       struct{ Name, Namespace string }
			}
			if err := json.Unmarshal([]byte(hr), &releases); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, release := range releases {
				if release.Name != "demo" || release.SourceRef.Name != release.Namespace+"-demo" || release.SourceRef.Namespace != namespaceOrphanSourceNamespace(scenario, release.Namespace) {
					t.Errorf("unexpected release/source: %+v", release)
				}
				got = append(got, release.Namespace)
			}
			if !slices.Equal(got, namespaces) {
				t.Fatalf("release namespaces = %v, want %v; full output: %s", got, namespaces, hr)
			}
			buildArgs := append([]string{"build", "all", "-o", "yaml"}, flags...)
			build, _ := harness.RequireCLIOK(t, buildArgs...)
			docs, err := testutil.SplitDocs([]byte(build))
			if err != nil {
				t.Fatal(err)
			}
			var repos []string
			for _, doc := range docs {
				if doc["kind"] != manifest.KindHelmRepository {
					continue
				}
				metadata, _ := doc["metadata"].(map[string]any)
				if metadata["name"] != "demo" {
					t.Errorf("unexpected repository: %+v", metadata)
				}
				ns, _ := metadata["namespace"].(string)
				repos = append(repos, ns)
			}
			slices.Sort(repos)
			wantRepos := namespaces
			if scenario == "standalone" {
				counts := make(map[string]int)
				for _, doc := range docs {
					name, namespace := manifest.DocMetadata(doc)
					kind := manifest.DocKind(doc)
					if namespace != "demo" || kind == manifest.KindConfigMap && name != "demo-chart" {
						t.Fatalf("standalone release lost its rendered namespace: %s", build)
					}
					counts[kind]++
				}
				if len(docs) != 3 || counts[manifest.KindHelmRelease] != 1 || counts[manifest.KindHelmRepository] != 1 || counts[manifest.KindConfigMap] != 1 {
					t.Fatalf("standalone root declarations or workload missing: %s", build)
				}
			}
			if scenario != "shared" && !slices.Equal(repos, wantRepos) {
				t.Errorf("repository namespaces = %v, want %v; full output: %s", repos, wantRepos, build)
			}
			out, _ := harness.RequireCLIOK(t, append([]string{"test", "all"}, flags...)...)
			if strings.Contains(out, "failed") || strings.Contains(out, "blocked") {
				t.Errorf("unexpected test result: %s", out)
			}
			wantPassed := 3 * len(namespaces)
			if scenario == "shared" {
				wantPassed = 5
			}
			if scenario == "standalone" {
				wantPassed = 2
			}
			if !strings.Contains(out, fmt.Sprintf("%d passed", wantPassed)) {
				t.Errorf("want %d real resources passed: %s", wantPassed, out)
			}
		})
	}
	t.Run("terminal replay", func(t *testing.T) {
		root, cache, _ := namespaceOrphanFixture(t, "explicit")
		flags := []string{"--path", filepath.Join(root, "flux"), "--cache-dir", cache, "--concurrency", "2", "--no-progress"}
		out, _ := harness.RequireCLIOK(t, append([]string{"build", "all"}, flags...)...)
		if !strings.Contains(out, "name: demo-chart") {
			t.Fatalf("last-good chart did not render: %s", out)
		}
		repository := &manifest.HelmRepository{URL: "https://charts.example.invalid"}
		chart := &manifest.HelmChartSource{Chart: "demo", Version: "0.1.0"}
		key := sourcecache.BlobKey{Content: struct{ Repository, Chart, Transport any }{repository.Content(), chart.Content(), nil}, Required: []string{"chart-version", "chart-url"}}
		completed := time.Now()
		// CLI signal handlers MUST stay outside the bubble. Seed a capped
		// deadline at the real wall clock so CLI scheduling cannot expire it.
		synctest.Test(t, func(t *testing.T) {
			time.Sleep(time.Until(completed.Add(-11 * 15 * time.Minute)))
			c, err := sourcecache.New(t.Context(), cache)
			if err != nil {
				t.Fatal(err)
			}
			for attempt := range 12 {
				_, _, _, err = c.Blob(t.Context(), key, sourcecache.Policy{}, func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
					return sourcecache.ProducerResult{}, &sourcecache.TerminalError{Err: syscall.ECONNRESET, Attempted: true}
				})
				if terminal, ok := errors.AsType[*sourcecache.TerminalError](err); !ok || !terminal.Attempted || !errors.Is(err, syscall.ECONNRESET) {
					t.Fatalf("seeded failure lost its classification: %v", err)
				}
				if attempt < 11 {
					time.Sleep(15 * time.Minute)
				}
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		})
		for _, command := range [][]string{{"build", "all"}, {"test", "all"}} {
			out, stderr, code := harness.RunCLIBuffers(append(command, flags...)...)
			if code != 1 || !strings.Contains(out+stderr, syscall.ECONNRESET.Error()) || strings.Contains(out, "name: demo-chart") || strings.Contains(out+stderr, "[OrphanedResource]") {
				t.Fatalf("terminal replay became success or warning: exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
		}
		c, err := sourcecache.New(t.Context(), cache)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _, err = c.Blob(t.Context(), key, sourcecache.Policy{}, func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			t.Fatal("fresh replay invoked producer")
			return sourcecache.ProducerResult{}, nil
		})
		if terminal, ok := errors.AsType[*sourcecache.TerminalError](err); !ok || terminal.Attempted || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("fresh replay lost its non-attempt classification: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	})

}

func TestE2E_NamespaceOrphans_FailingOwner(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprintf("concurrency%d", concurrency), func(t *testing.T) {
			root, cache, _ := namespaceOrphanFixture(t, "failing")
			st := store.New()
			if _, err := discovery.Run(t.Context(), discovery.Config{Path: filepath.Join(root, "flux"), RepoRoot: root, Store: st, WipeSecrets: true}); err != nil {
				t.Fatal(err)
			}
			if releases := st.ListObjects(manifest.HelmReleaseDomain, manifest.KindHelmRelease); len(releases) != 0 {
				t.Fatalf("failed owner's raw releases admitted during discovery: %+v", releases)
			}
			flags := []string{"--path", filepath.Join(root, "flux"), "--cache-dir", cache, "--concurrency", fmt.Sprint(concurrency), "--no-progress"}
			out, stderr, code := harness.RunCLIBuffers(append([]string{"test", "all"}, flags...)...)
			if code != 1 || !strings.Contains(out+stderr, "flux-system/owner") || !strings.Contains(out+stderr, "no matches") {
				t.Errorf("owner build failure must remain visible: exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
			out, stderr, code = harness.RunCLIBuffers(append([]string{"get", "hr", "-o", "json"}, flags...)...)
			if code != 1 {
				t.Errorf("failed owner get exit=%d stderr=%s", code, stderr)
			}
			var releases []any
			if err := json.Unmarshal([]byte(out), &releases); err != nil {
				t.Fatal(err)
			}
			if len(releases) != 0 {
				t.Errorf("failed owner's raw releases admitted in CLI collection: %s", out)
			}
		})
	}
}

func namespaceOrphanSourceNamespace(scenario, namespace string) string {
	if scenario == "shared" {
		return "charts"
	}
	return namespace
}

func namespaceOrphanFixture(t *testing.T, scenario string) (string, string, []string) {
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
		bundle = strings.Replace(bundle, "sourceRef: {kind: HelmRepository, name: demo}", "sourceRef: {kind: HelmRepository, name: demo, namespace: charts}", 1)
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
			testutil.WriteFile(t, root, "flux/"+owner+".yaml", testutil.KustomizationYAML(owner, "flux-system", " interval: 10m\n path: ./"+path+"\n sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n"))
			testutil.WriteFile(t, root, path+"/kustomization.yaml", overlay)
		}
	}
	archive := testutil.TarGz(t, map[string]string{
		"demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 0.1.0\n",
		"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-chart\n  namespace: {{ .Release.Namespace }}\ndata: {value: fixture}\n",
	}, 0o600, gzip.DefaultCompression)
	c, err := sourcecache.New(t.Context(), cache)
	if err != nil {
		t.Fatal(err)
	}
	repository := &manifest.HelmRepository{URL: "https://charts.example.invalid"}
	chart := &manifest.HelmChartSource{Chart: "demo", Version: "0.1.0"}
	_, _, _, err = c.Blob(t.Context(), sourcecache.BlobKey{Content: struct{ Repository, Chart, Transport any }{repository.Content(), chart.Content(), nil}, Required: []string{"chart-version", "chart-url"}}, sourcecache.Policy{MaxAge: time.Hour}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		_, err := w.Write(archive)
		return sourcecache.ProducerResult{Digest: digest.FromBytes(archive), Size: int64(len(archive)), Annotations: map[string]string{"flate.chart-version": "0.1.0", "flate.chart-url": "https://charts.example.invalid/demo-0.1.0.tgz"}}, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	return root, cache, namespaces
}
