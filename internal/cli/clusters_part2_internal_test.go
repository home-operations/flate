package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/homeport/dyff/pkg/dyff"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/diffprep"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestClusterReport_NativeAggregateIndexes(t *testing.T) {
	t.Parallel()
	doc := func(name, value string) diffprep.Doc {
		return diffprep.Doc{Manifest: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name}, "data": map[string]any{"value": value},
		}}
	}
	groups := make([]dyff.Report, 2)
	var err error
	groups[0], err = diffprep.Prepare([]diffprep.Doc{doc("same", "old"), doc("removed", "old")}, []diffprep.Doc{doc("same", "new")}, diffprep.Options{}).
		Native()
	assert.NoError(t, err)
	groups[1], err = diffprep.Prepare([]diffprep.Doc{doc("same", "old")}, []diffprep.Doc{doc("same", "new"), doc("added", "new")}, diffprep.Options{}).
		Native()
	assert.NoError(t, err)
	aggregate := aggregateNative(groups)
	assert.Equal(t, len(aggregate.From.Documents), 3)
	assert.Equal(t, len(aggregate.To.Documents), 3)
	assert.Equal(t, len(aggregate.Diffs), 4)
	assert.Diff(t, aggregate.From.Names, []string{"v1/ConfigMap/same", "v1/ConfigMap/removed", "v1/ConfigMap/same"})
	assert.Diff(t, aggregate.To.Names, []string{"v1/ConfigMap/same", "v1/ConfigMap/same", "v1/ConfigMap/added"})
	for i, want := range []struct {
		to    bool
		index int
	}{{false, 0}, {false, 1}, {false, 2}, {true, 2}} {
		path := aggregate.Diffs[i].Path
		root := &aggregate.From
		if want.to {
			root = &aggregate.To
		}
		assert.FatalIf(
			t,
			path.Root != root || path.DocumentIdx != want.index,
			"diff %d has wrong side or offset: %+v",
			i,
			path,
		)
	}
	assert.Equal(t, groups[1].Diffs[0].Path.DocumentIdx, 0)
	assert.FatalIf(t, groups[1].Diffs[0].Path.Root == &aggregate.From, "%v", "aggregation mutated its input paths")
}

func writeGetRelease(t *testing.T, root, dir string) {
	t.Helper()
	testutil.WriteFile(t, root, "charts/app/Chart.yaml", "apiVersion: v2\nname: app\nversion: 0.1.0\n")
	testutil.WriteFile(
		t,
		root,
		"charts/app/templates/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: installed, namespace: workloads}\ndata: {value: rendered}\n",
	)
	testutil.WriteFile(t, root, dir+"/app/kustomization.yaml", "resources: [pod.yaml, release.yaml]\n")
	testutil.WriteFile(t, root, dir+"/app/release.yaml", `apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: shared, namespace: apps}
spec:
  targetNamespace: workloads
  releaseName: installed
  chart:
    spec:
      chart: ./charts/app
      version: 0.1.0
      sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}
`)
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_Get_Formats(t *testing.T) {
	root := writeClusterGroupsFixture(t, map[string]string{"alpha": "first", "omega": "second"})
	for _, dir := range []string{"alpha", "omega"} {
		appendScopeSpec(
			t,
			root,
			dir+"/flux.yaml",
			"  sourceRef: {kind: GitRepository, name: flux-system, namespace: flux-system}\n",
		)
		writeGetRelease(t, root, dir)
		testutil.WriteFile(
			t,
			root,
			dir+"/app/images.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: images, namespace: apps}\ndata: {shared: 'example.test/shared:v1', unusual: 'count:up0'}\n",
		)
		testutil.WriteFile(t, root, dir+"/app/kustomization.yaml", "resources: [pod.yaml, release.yaml, images.yaml]\n")
	}
	for _, scope := range []struct {
		name, path string
		clusters   []string
	}{
		{"single", filepath.Join(root, "alpha"), []string{"alpha"}},
		{"multiple", root, []string{"alpha", "omega"}},
	} {
		t.Run(scope.name, func(t *testing.T) { runGetFormatsName(t, &scope) })
	}
}

func decodeGetDocuments(t *testing.T, out, format string, clusters []string) []output.DocumentGroup {
	t.Helper()
	if len(clusters) > 1 && format == "json" {
		var groups []output.DocumentGroup
		assert.NoError(t, json.Unmarshal([]byte(out), &groups))
		assert.Equal(t, len(groups), len(clusters))
		for i, cluster := range clusters {
			assert.Equal(t, groups[i].Cluster, cluster)
		}
		return groups
	}
	groups := make([]output.DocumentGroup, len(clusters))
	for i, cluster := range clusters {
		groups[i] = output.DocumentGroup{Cluster: cluster, Documents: []map[string]any{}}
	}
	if format == "json" {
		assert.NoError(t, json.Unmarshal([]byte(out), &groups[0].Documents))
		return groups
	}
	streams := []string{out}
	if len(clusters) > 1 {
		streams = nil
		for i, cluster := range clusters {
			marker := "# Cluster: " + cluster + "\n"
			assert.FatalIf(t, !strings.HasPrefix(out, marker), "missing cluster marker %q: %s", marker, out)
			out = strings.TrimPrefix(out, marker)
			stream, rest, found := strings.Cut(out, "# Cluster: ")
			streams = append(streams, stream)
			if found {
				out = "# Cluster: " + rest
			} else {
				out = ""
			}
			assert.FatalIf(t, i == len(clusters)-1 && out != "", "extra groups: %s", out)
		}
	}
	decodeGetDocumentsLoop(t, struct {
		streams *[]string
		groups  *[]output.DocumentGroup
	}{streams: &streams, groups: &groups})
	return groups
}

func TestPrepareGetTables_SharedColumns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups int
	}{
		{name: "single", groups: 1},
		{name: "multiple", groups: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			columns := []output.Column{{Header: "NAME", Key: "name"}}
			rows := []map[string]string{{"cluster": "alpha", "name": "shared"}}
			original := []output.Table{{Columns: columns, Rows: rows}}
			prepared, multi := prepareGetTables(&selectedInvocation{groups: make([]selectedGroup, tc.groups)}, original)
			assert.Equal(t, multi, tc.groups == 2)
			want := columns
			if multi {
				want = []output.Column{{Header: "CLUSTER", Key: "cluster"}, {Header: "NAME", Key: "name"}}
			}
			assert.Diff(t, prepared[0].Columns, want)
			assert.Diff(t, original, []output.Table{{
				Columns: []output.Column{{Header: "NAME", Key: "name"}},
				Rows:    []map[string]string{{"cluster": "alpha", "name": "shared"}},
			}})
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_EmptyKinds(t *testing.T) {
	for _, kinds := range []string{"ks", "hr", "empty"} {
		t.Run(kinds, func(t *testing.T) { runGetAllEmptyKindsKinds(t, &kinds) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_GetAll_ChangedOnly(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "same", "omega": "same"})
	head := writeClusterGroupsFixture(t, map[string]string{"alpha": "same", "omega": "same"})
	for _, root := range []string{base, head} {
		writeGetRelease(t, root, "alpha")
		writeGetRelease(t, root, "omega")
		testutil.WriteFile(
			t,
			root,
			"alpha/app/private.yaml",
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private, namespace: apps}\ndata: {value: original}\n",
		)
		testutil.WriteFile(
			t,
			root,
			"alpha/app/kustomization.yaml",
			"resources: [pod.yaml, release.yaml, private.yaml]\n",
		)
	}
	testutil.WriteFile(
		t,
		head,
		"alpha/app/private.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: private, namespace: apps}\ndata: {value: changed}\n",
	)
	testutil.WriteFile(
		t,
		base,
		"alpha/removed.yaml",
		testutil.KustomizationYAML("removed", "flux-system", "  path: ./alpha/removed\n"),
	)
	testutil.WriteFile(t, base, "alpha/kustomization.yaml", "resources: [flux.yaml, removed.yaml]\n")
	testutil.WriteFile(t, base, "alpha/removed/kustomization.yaml", "resources: [cm.yaml]\n")
	testutil.WriteFile(t, base, "alpha/removed/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: removed}\n")
	flags := []string{"--path", head, "--path-orig", base, "--cache-dir", t.TempDir(), "--concurrency", "2"}
	var selected [][]output.DocumentGroup
	runGetAllChangedOnlyLoop(t, struct {
		flags    *[]string
		selected *[][]output.DocumentGroup
	}{flags: &flags, selected: &selected})
	for _, command := range []string{"get all", "get"} {
		t.Run(command, func(t *testing.T) { runGetAllChangedOnlyBody(t, &command, &flags, &selected) })
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_ClusterGroups_SuppliedPrecedence(t *testing.T) {
	base := writeClusterGroupsFixture(t, map[string]string{"alpha": "old-alpha", "omega": "old-omega"})
	head := writeClusterGroupsFixture(t, map[string]string{"alpha": "new-alpha", "omega": "new-omega"})
	for _, root := range []string{base, head} {
		for _, cluster := range []string{"alpha", "omega"} {
			testutil.WriteFile(
				t,
				root,
				cluster+"/flux.yaml",
				testutil.KustomizationYAML(
					"shared",
					"flux-system",
					"  path: ./"+cluster+"/app\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: settings}]\n",
				),
			)
			value := "old-" + cluster
			if root == head {
				value = "new-" + cluster
			}
			testutil.WriteFile(
				t,
				root,
				cluster+"/settings.yaml",
				"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: "+value+"}\n",
			)
			testutil.WriteFile(t, root, cluster+"/kustomization.yaml", "resources: [flux.yaml, settings.yaml]\n")
		}
	}
	input := substitutionInput(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: flux-system}\ndata: {CLUSTER: supplied}\n",
	)
	stdout, stderr, code := runClusterInputs(
		t,
		"diff",
		"all",
		"--path",
		head,
		"--path-orig",
		base,
		"--supply",
		input,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
		"-o",
		"diff",
	)
	assert.FatalIf(t, code != 0 || strings.Count(stderr, "[SuppliedPrecedence]") != 2 ||
		!strings.Contains(stderr, "[SuppliedPrecedence] alpha:") ||
		!strings.Contains(stderr, "[SuppliedPrecedence] omega:") ||
		strings.Contains(stderr, "Cluster:") ||
		strings.Contains(
			stdout,
			"supplied",
		), "cluster supply reduction: exit=%d stdout=%s stderr=%s", code, stdout, stderr)
	for _, cluster := range []string{"alpha", "omega"} {
		assert.Equal(t, strings.Count(stderr, "[SuppliedPrecedence] "+cluster+":"), 1)
	}
	for _, value := range []string{"old-alpha", "new-alpha", "old-omega", "new-omega"} {
		assert.FatalIf(t, !strings.Contains(stdout, value), "cluster precedence lost %q: %s", value, stdout)
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestRun_SyntheticDiagnosticsAcrossCommands(t *testing.T) {
	server := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }),
	)
	t.Cleanup(server.Close)
	fixture := func() string {
		root := t.TempDir()
		testutil.WriteFile(t, root, "kustomization.yaml", "resources: [release.yaml]\n")
		testutil.WriteFile(t, root, "release.yaml", fmt.Sprintf(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: release, namespace: apps}
spec:
  interval: 5m
  chart:
    kind: OCIRepository
    spec:
      insecure: true
      url: oci://%s/charts/app
      ref: {tag: 1.0.0}
      secretRef: {name: auth}
`, strings.TrimPrefix(server.URL, "https://")))
		return root
	}
	current, original := fixture(), fixture()
	testutil.WriteFile(t, original, "kustomization.yaml", "resources: []\n")
	assert.NoError(t, os.Remove(filepath.Join(original, "release.yaml")))
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, "{}")
	for _, verb := range []string{"build", "diff", "get", "test"} {
		t.Run(verb, func(t *testing.T) {
			args := []string{
				verb,
				"all",
				"--path",
				current,
				"--allow-missing-kinds",
				"Secret",
				"--registry-config",
				config,
				"--cache-dir",
				t.TempDir(),
				"--concurrency",
				"2",
			}
			if verb == "diff" {
				args = append(args, "--path-orig", original, "-o", "diff")
			}
			if verb == "build" || verb == "get" {
				args = append(args, "-o", "json")
			}
			out, diagnostic, code := runCLI(t, args...)
			assert.FatalIf(t, code != 0 || strings.Contains(diagnostic, "OCIRepository") ||
				!strings.Contains(
					diagnostic,
					"HelmRelease",
				), "%s diagnostic: code=%d stdout=%s stderr=%s", verb, code, out, diagnostic)
			if verb == "build" || verb == "get" {
				var documents []map[string]any
				assert.NoError(t, json.Unmarshal([]byte(out), &documents))
				for _, document := range documents {
					assert.FatalIf(
						t,
						document["kind"] == manifest.KindOCIRepository,
						"%s exposed generated source: %s",
						verb,
						out,
					)
				}
			}
		})
	}
}

func runClusterGroupsPartialFailureCase(t *testing.T, command *string, root *string, format *string) {
	t.Helper()

	args := append(
		strings.Fields((*command)),
		"--path",
		(*root),
		"-o",
		(*format),
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
	)
	out, diagnostics, status := runClusterInputs(t, args...)
	assert.FatalIf(t, status != 1 || !strings.Contains(diagnostics, "alpha: flux-system/missing") ||
		strings.Contains(
			diagnostics,
			"Cluster:",
		), "lost qualified failure: exit=%d stdout=%s stderr=%s", status, out, diagnostics)
	if (*command) == "get images" {
		if (*format) == "table" {
			assert.Equal(t, out, "CLUSTER    IMAGE\nomega      example.test/app:second\n")
		}
		if (*format) == "name" {
			assert.Equal(t, out, "omega: example.test/app:second\n")
		}
		if (*format) == "json" || (*format) == "yaml" {
			var images []output.ImageGroup
			assert.NoError(t, yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&images))
			assert.Equal(t, len(images), 2)
			assert.Equal(t, images[0].Cluster, "alpha")
			assert.Equal(t, len(images[0].Images), 0)
			assert.Equal(t, images[1].Cluster, "omega")
			assert.Diff(t, images[1].Images, []string{"example.test/app:second"})
		}
		return
	}
	switch *format {
	case "table":
		assert.FatalIf(t, strings.Count(out, "CLUSTER") != 2 || !strings.Contains(out, "omega") ||
			!strings.Contains(out, "Kustomization/shared") ||
			!strings.Contains(out, "HelmRelease/shared"), "partial table lost healthy resources: %s", out)
	case "name":
		assert.Equal(
			t,
			out,
			"alpha:Kustomization/shared\nomega:Kustomization/shared\nomega:HelmRelease/shared\n",
		)
	default:
		groups := decodeGetDocuments(t, out, (*format), []string{"alpha", "omega"})
		assert.Equal(t, len(groups[0].Documents), 1)
		assert.Equal(t, groups[0].Documents[0]["kind"], any("Kustomization"))
		assert.Equal(t, len(groups[1].Documents), 2)
		assert.Equal(t, groups[1].Documents[0]["kind"], any("Kustomization"))
		assert.Equal(t, groups[1].Documents[1]["kind"], any("HelmRelease"))
		assert.Equal(t, groups[1].Documents[1]["name"], any("shared"))
	}
}

func decodeGetDocumentsLoop(t *testing.T, loopInputs struct {
	streams *[]string
	groups  *[]output.DocumentGroup
},
) {
	t.Helper()

	for i, stream := range *loopInputs.streams {
		decoder := yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(stream), 4096)
		for {
			var doc map[string]any
			err := decoder.Decode(&doc)
			if errors.Is(err, io.EOF) {
				break
			}
			assert.NoError(t, err)
			if doc != nil {
				(*loopInputs.groups)[i].Documents = append((*loopInputs.groups)[i].Documents, doc)
			}
		}
	}
}

func runGetAllChangedOnlyBody(t *testing.T, command *string, flags *[]string, selected *[][]output.DocumentGroup) {
	t.Helper()

	out, stderr, code := runClusterInputs(t, append(append(strings.Fields((*command)), "-o", "json"), (*flags)...)...)
	assert.FatalIf(t, code != 0, "exit=%d stdout=%s stderr=%s", code, out, stderr)
	groups := decodeGetDocuments(t, out, "json", []string{"alpha", "omega"})
	for i, group := range groups {
		want := append(slices.Clone((*selected)[0][i].Documents), (*selected)[1][i].Documents...)
		assert.Diff(t, group.Documents, want)
		var identities []string
		for _, doc := range group.Documents {
			identities = append(identities, doc["kind"].(string)+"/"+doc["name"].(string))
		}
		if i == 0 {
			assert.Diff(t, identities, []string{"Kustomization/shared", "HelmRelease/shared"})
			assert.Equal(t, group.Documents[1]["version"], any("0.1.0"))
		} else {
			assert.Diff(t, identities, []string{"Kustomization/shared"})
		}
	}
	out, stderr, code = runClusterInputs(t, append(strings.Fields((*command)), (*flags)...)...)
	assert.FatalIf(t, code != 0 || strings.Count(out, "CLUSTER") != 2 || !strings.Contains(out, "omega") ||
		strings.Contains(out, "removed") ||
		!strings.Contains(out, "alpha") ||
		!strings.Contains(
			out,
			"HelmRelease/shared",
		), "changed table: exit=%d stdout=%s stderr=%s", code, out, stderr)
	for _, format := range []string{"json", "yaml"} {
		args := append(append(strings.Fields((*command)), "-o", format, "-n", "apps"), (*flags)...)
		out, stderr, code := runClusterInputs(t, args...)
		assert.FatalIf(t, code != 0, "namespace selection: exit=%d stdout=%s stderr=%s", code, out, stderr)
		groups := decodeGetDocuments(t, out, format, []string{"alpha", "omega"})
		assert.Equal(t, len(groups[0].Documents), 1)
		assert.Equal(t, groups[0].Documents[0]["kind"], any("HelmRelease"))
		assert.Equal(t, len(groups[1].Documents), 0)
		assert.FatalIf(
			t,
			format == "json" && groups[1].Documents == nil,
			"%v",
			"empty retained cluster documents must be []",
		)
	}
}

func runGetAllChangedOnlyLoop(t *testing.T, loopInputs struct {
	flags    *[]string
	selected *[][]output.DocumentGroup
},
) {
	t.Helper()

	for _, kind := range []string{"ks", "hr"} {
		out, stderr, code := runClusterInputs(t, append([]string{"get", kind, "-o", "json"}, (*loopInputs.flags)...)...)
		assert.FatalIf(t, code != 0, "get %s: exit=%d stdout=%s stderr=%s", kind, code, out, stderr)
		if (*loopInputs.selected) == nil {
			(*loopInputs.selected) = make([][]output.DocumentGroup, 0, 2)
		}
		(*loopInputs.selected) = append(
			(*loopInputs.selected),
			decodeGetDocuments(t, out, "json", []string{"alpha", "omega"}),
		)
	}
}
