package e2e

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestE2E_NamespaceOrphans(t *testing.T) {
	for _, scenario := range []string{"sibling", "explicit", "standalone", "shared"} {
		t.Run(scenario, func(t *testing.T) {
			for _, concurrency := range []int{2, 4} {
				t.Run(fmt.Sprintf("concurrency%d", concurrency), func(t *testing.T) {
					root, cache, namespaces := namespaceOrphanFixture(t, scenario)
					if scenario == "shared" || scenario == "standalone" {
						st := store.New()
						if _, err := discovery.Run(t.Context(), discovery.Config{Path: root, RepoRoot: root, Store: st, WipeSecrets: true}); err != nil {
							t.Fatal(err)
						}
						repos := st.ListAs[*manifest.HelmRepository](manifest.KindHelmRepository)
						if len(repos) != 1 || repos[0].Name != "demo" || repos[0].Namespace != namespaceOrphanSourceNamespace(scenario, "demo") {
							t.Errorf("standalone source collection = %+v", repos)
						}
					}
					flags := []string{"--path", root, "--cache-dir", cache, "--concurrency", fmt.Sprint(concurrency), "--no-progress"}
					getArgs := append([]string{"get", "hr", "-o", "json"}, flags...)
					hr, _ := requireCLIOK(t, getArgs...)
					var releases []struct {
						Name, Namespace string
						SourceRef       struct{ Name, Namespace string }
					}
					if err := json.Unmarshal([]byte(hr), &releases); err != nil {
						t.Fatal(err)
					}
					var got []string
					for _, release := range releases {
						if release.Name != "demo" || release.SourceRef.Name != "demo" || release.SourceRef.Namespace != namespaceOrphanSourceNamespace(scenario, release.Namespace) {
							t.Errorf("unexpected release/source: %+v", release)
						}
						got = append(got, release.Namespace)
					}
					if !slices.Equal(got, namespaces) {
						t.Fatalf("release namespaces = %v, want %v; full output: %s", got, namespaces, hr)
					}
					again, _ := requireCLIOK(t, getArgs...)
					if hr != again {
						t.Error("get hr payload differs between runs")
					}
					buildArgs := append([]string{"build", "all", "-o", "yaml"}, flags...)
					build, _ := requireCLIOK(t, buildArgs...)
					again, _ = requireCLIOK(t, buildArgs...)
					if build != again {
						t.Error("build all payload differs between runs")
					}
					docs, err := manifest.SplitDocs([]byte(build))
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
					if scenario != "standalone" && scenario != "shared" && !slices.Equal(repos, namespaces) {
						t.Errorf("repository namespaces = %v, want %v; full output: %s", repos, namespaces, build)
					}
					out, _ := requireCLIOK(t, append([]string{"test", "all"}, flags...)...)
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
		})
	}
}

func TestE2E_NamespaceOrphans_FailingOwner(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprintf("concurrency%d", concurrency), func(t *testing.T) {
			root, cache, _ := namespaceOrphanFixture(t, "failing")
			st := store.New()
			if _, err := discovery.Run(t.Context(), discovery.Config{Path: root, RepoRoot: root, Store: st, WipeSecrets: true}); err != nil {
				t.Fatal(err)
			}
			if releases := st.ListObjects(manifest.KindHelmRelease); len(releases) != 0 {
				t.Fatalf("failed owner's raw releases admitted during discovery: %+v", releases)
			}
			flags := []string{"--path", root, "--cache-dir", cache, "--concurrency", fmt.Sprint(concurrency), "--no-progress"}
			out, stderr, code := runCLIBuffers(append([]string{"test", "all"}, flags...)...)
			if code != 1 || !strings.Contains(out+stderr, "flux-system/owner") || !strings.Contains(out+stderr, "no matches") {
				t.Errorf("owner build failure must remain visible: exit=%d stdout=%s stderr=%s", code, out, stderr)
			}
			out, stderr, code = runCLIBuffers(append([]string{"get", "hr", "-o", "json"}, flags...)...)
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
	root, cache := t.TempDir(), t.TempDir()
	gitInit(t, root)
	namespaces := []string{"demo"}
	rawNS := ""
	if scenario == "explicit" || scenario == "standalone" {
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
		testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: HelmRepository
metadata: {name: demo, namespace: charts}
spec: {url: "https://charts.example.invalid", interval: 24h}
`)
	}
	testutil.WriteFile(t, root, "cluster/shared/demo/bundle.yaml", bundle)
	base := "namespace: demo\nresources: [bundle.yaml]\n"
	if scenario == "shared" {
		base = "resources: [bundle.yaml]\n"
		namespaces = []string{"demo-a", "demo-b"}
	}
	if scenario == "failing" {
		base += "patches:\n- patch: |-\n    apiVersion: v1\n    kind: ConfigMap\n    metadata: {name: absent}\n    data: {value: fixture}\n"
	}
	if scenario != "standalone" {
		testutil.WriteFile(t, root, "cluster/shared/demo/kustomization.yaml", base)
		for _, ns := range namespaces {
			owner, path := "owner", "cluster/per-cluster"
			overlay := "resources: [../shared/demo]\n"
			if scenario == "shared" {
				owner += "-" + ns
				path += "-" + ns
				overlay = "namespace: " + ns + "\n" + overlay
			}
			testutil.WriteFile(t, root, "flux/"+owner+".yaml", `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: `+owner+`, namespace: flux-system}
spec:
 interval: 10m
 path: ./`+path+`
 sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
			testutil.WriteFile(t, root, path+"/kustomization.yaml", overlay)
		}
	}
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	for _, file := range []struct{ name, body string }{
		{"demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n"},
		{"demo/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-chart\n  namespace: {{ .Release.Namespace }}\ndata: {value: fixture}\n"},
	} {
		if err := tw.WriteHeader(&tar.Header{Name: file.name, Mode: 0o600, Size: int64(len(file.body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(file.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	c := source.NewCache(cacheroot.New(cache))
	_, digest, err := c.PutBytes(t.Context(), archive.Bytes(), "chart.tgz")
	if err != nil {
		t.Fatal(err)
	}
	slot, err := c.Slot(t.Context(), "https://charts.example.invalid", "helm-resolve:demo@0.1.0", source.AuthIdentityFromRefs("demo", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer slot.Release()
	if err := slot.PersistMeta(func(m *source.SlotMeta) {
		m.ChartVersion = "0.1.0"
		m.ChartDigest = digest
		m.ChartURL = "https://charts.example.invalid/demo-0.1.0.tgz"
	}); err != nil {
		t.Fatal(err)
	}
	return root, cache, namespaces
}
