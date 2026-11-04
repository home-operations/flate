package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	yamlutil "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/output"
	"github.com/home-operations/flate/internal/testutil"
)

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

func runGetFormatsName(t *testing.T, scope *struct {
	name     string
	path     string
	clusters []string
},
) {
	t.Helper()

	for _, format := range []string{"", "table", "yaml", "json", "name"} {
		t.Run("format="+format, func(t *testing.T) { runGetFormatsNameCase(t, &scope, &format) })
	}
}

func runGetFormatsNameCase(t *testing.T, scope **struct {
	name     string
	path     string
	clusters []string
}, format *string,
) {
	t.Helper()

	var combined string
	for _, command := range []string{"get all", "get", "get ks", "get hr", "get images"} {
		t.Run(command, func(t *testing.T) {
			runGetFormatsNameCaseCommand(t, &command, &scope, struct {
				format   **string
				combined *string
			}{format: &format, combined: &combined})
		})
	}
}

func runGetFormatsNameCaseCommand(t *testing.T, command *string, scope ***struct {
	name     string
	path     string
	clusters []string
}, capturedInputs struct {
	format   **string
	combined *string
},
) {
	t.Helper()

	args := append(
		strings.Fields((*command)),
		"--path",
		(*(*scope)).path,
		"--cache-dir",
		t.TempDir(),
		"--concurrency",
		"2",
	)
	if (*(*capturedInputs.format)) != "" {
		args = append(args, "-o", (*(*capturedInputs.format)))
	}
	cluster := runClusterInputs(t, args...)
	out, diagnostics, status := cluster.stdout, cluster.stderr, cluster.code
	assert.FatalIf(t, status != 0, "exit=%d stdout=%s stderr=%s", status, out, diagnostics)
	cluster2 := runClusterInputs(t, args...)
	again, stderr, code := cluster2.stdout, cluster2.stderr, cluster2.code
	assert.Equal(t, code, status)
	assert.Equal(t, again, out)
	assert.Equal(t, stderr, diagnostics)
	if (*command) == "get all" {
		(*capturedInputs.combined) = out
	}
	if (*command) == "get" {
		assert.Equal(t, out, (*capturedInputs.combined))
	}
	expectation := getFormatExpectation{
		command: *command, format: **capturedInputs.format, clusters: (**scope).clusters,
	}
	if *command == "get images" {
		expectation.assertImages(t, out)
		return
	}
	kinds := []string{"Kustomization", "HelmRelease"}
	if *command == "get ks" {
		kinds = kinds[:1]
	}
	if *command == "get hr" {
		kinds = kinds[1:]
	}
	if expectation.format == "yaml" || expectation.format == "json" {
		expectation.assertDocuments(t, out, kinds)
		return
	}
	expectation.assertRows(t, out, kinds)
}

type getFormatExpectation struct {
	command, format string
	clusters        []string
}

func (e *getFormatExpectation) assertImages(t *testing.T, out string) {
	t.Helper()

	want := []string{"example.test/app:first", "example.test/shared:v1"}
	if e.format != "json" && e.format != "yaml" {
		assert.Equal(t, out, e.imageRows())
		return
	}
	{
		var images []string
		if len(e.clusters) == 1 {
			assert.NoError(
				t,
				yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&images),
			)
			assert.Diff(t, images, want)
		} else {
			var groups []output.ImageGroup
			assert.NoError(
				t,
				yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(out), 4096).Decode(&groups),
			)
			assert.Equal(t, len(groups), 2)
			for i, cluster := range e.clusters {
				assert.Equal(t, groups[i].Cluster, cluster)
				if i == 1 {
					want[0] = "example.test/app:second"
				}
				assert.Diff(t, groups[i].Images, want)
			}
		}
	}
}

func (e *getFormatExpectation) imageRows() string {
	want := []string{"example.test/app:first", "example.test/shared:v1"}

	expected := ""
	if e.format != "name" {
		if len(e.clusters) == 2 {
			expected = "CLUSTER    "
		}
		expected += "IMAGE\n"
	}
	var expectedSb82 strings.Builder
	for i, cluster := range e.clusters {
		if i == 1 {
			want[0] = "example.test/app:second"
		}
		var expectedSb1145 strings.Builder
		for _, image := range want {
			prefix := ""
			if len(e.clusters) == 2 {
				prefix = fmt.Sprintf("%-11s", cluster)
				if e.format == "name" {
					prefix = cluster + ": "
				}
			}
			expectedSb1145.WriteString(prefix + image + "\n")
		}
		expectedSb82.WriteString(expectedSb1145.String())
	}
	expected += expectedSb82.String()
	return expected
}

func (e *getFormatExpectation) assertDocuments(t *testing.T, out string, kinds []string) {
	t.Helper()

	groups := decodeGetDocuments(t, out, e.format, e.clusters)
	for i, group := range groups {
		assert.Equal(t, len(group.Documents), len(kinds))
		for j, kind := range kinds {
			doc := group.Documents[j]
			assert.Equal(t, doc["kind"], any(kind))
			assert.Equal(t, doc["name"], any("shared"))
			assert.Diff(
				t,
				doc["sourceRef"],
				map[string]any{
					"kind":      "GitRepository",
					"name":      "flux-system",
					"namespace": "flux-system",
				},
			)
			if kind == "Kustomization" {
				assert.Equal(t, doc["path"], any("./"+e.clusters[i]+"/app"))
				assert.Equal(t, doc["prune"], any(false))
				assert.Equal(t, doc["wait"], any(false))
			} else {
				assert.Equal(t, doc["chart"], any("flux-system-flux-system/./charts/app"))
				assert.Equal(t, doc["version"], any("0.1.0"))
				assert.Equal(t, doc["releaseName"], any("installed"))
				assert.Equal(t, doc["targetNamespace"], any("workloads"))
			}
		}
	}
}

func (e *getFormatExpectation) assertRows(t *testing.T, out string, kinds []string) {
	t.Helper()
	var expected strings.Builder
	for j, kind := range kinds {
		expected.WriteString(e.header(kind, j))
		expected.WriteString(e.rows(kind))
	}
	assert.Equal(t, out, expected.String())
}

func (e *getFormatExpectation) header(kind string, j int) string {
	var expected strings.Builder
	all := e.command == "get" || e.command == "get all"
	if e.format == "name" {
		return ""
	}
	if j > 0 {
		expected.WriteString("\n")
	}
	if len(e.clusters) == 2 {
		expected.WriteString("CLUSTER    ")
	}
	if kind == "Kustomization" {
		if all {
			expected.WriteString("NAMESPACE      NAME                    PATH\n")
		} else {
			expected.WriteString("NAMESPACE      NAME      PATH\n")
		}
		return expected.String()
	}
	if all {
		expected.WriteString("NAMESPACE    NAME                  ")
	} else {
		expected.WriteString("NAMESPACE    NAME      ")
	}
	expected.WriteString("CHART                                   VERSION    SOURCE\n")

	return expected.String()
}

func (e *getFormatExpectation) rows(kind string) string {
	var expected strings.Builder
	all := e.command == "get" || e.command == "get all"

	for _, cluster := range e.clusters {
		name := "shared"
		if all {
			name = kind + "/" + name
		}
		if e.format == "name" {
			if len(e.clusters) == 2 {
				name = cluster + ":" + name
			}
			expected.WriteString(name + "\n")
			continue
		}
		if len(e.clusters) == 2 {
			fmt.Fprintf(&expected, "%-11s", cluster)
		}
		if kind == "Kustomization" {
			fmt.Fprintf(&expected, "flux-system    %-*s./%s/app\n",
				map[bool]int{false: 10, true: 24}[all],
				name,
				cluster)
		} else {
			fmt.Fprintf(
				&expected,
				"apps         %-*sflux-system-flux-system/./charts/app    0.1.0      flux-system\n",
				map[bool]int{false: 10, true: 22}[all],
				name,
			)
		}
	}
	return expected.String()
}
