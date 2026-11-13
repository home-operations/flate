package diff

import (
	"bytes"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestComparison_WriteTwice(t *testing.T) {
	t.Parallel()
	for _, format := range []Format{FormatGitHub, FormatDiff, FormatHTML} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			for _, supplied := range []bool{false, true} {
				name := "default names"
				if supplied {
					name = "supplied names"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					testComparisonWriteTwice(t, format, supplied)
				})
			}
		})
	}
}

func testComparisonWriteTwice(t *testing.T, format Format, supplied bool) {
	t.Helper()
	groups := make([]Group, 2)
	originals := make([]map[string]any, 0, 4)
	inputs := make([]map[string]any, 0, 4)
	for i, cluster := range []string{"east", "west"} {
		groups[i].Cluster = cluster
		parent := &manifest.Kustomization{Name: "render", Namespace: "apps"}
		snapshots := [2]*Snapshot{&groups[i].Orig, &groups[i].Current}
		for side, value := range []string{"old", "new"} {
			doc := map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": map[string]any{"namespace": "apps", "name": "config"},
				"data":     map[string]any{"key": value},
			}
			snapshots[side].Manifests = []manifest.DocumentBatch{{
				Parent: parent, Documents: []map[string]any{doc},
			}}
			inputs = append(inputs, doc)
			originals = append(originals, manifest.DeepCopyMap(doc))
		}
	}
	calls := 0
	comparison, err := Compare(groups, Options{Format: format, Normalize: func(doc map[string]any) {
		calls++
		metadata := doc["metadata"].(map[string]any)
		metadata["name"] = metadata["name"].(string) + "-normalized"
	}})
	assert.NoError(t, err)
	assert.Equal(t, calls, 4)
	var names, beforeNames []Names
	clusters := []string{"east", "west"}
	if supplied {
		clusters = []string{"review-east", "review-west"}
		names = PrepareNames(clusters, [][]manifest.NamedResource{
			comparison.Groups[0].Subjects, comparison.Groups[1].Subjects,
		}, nil).Groups
		beforeNames = make([]Names, len(names))
		for i := range names {
			beforeNames[i] = Names{Prefix: names[i].Prefix, Subjects: maps.Clone(names[i].Subjects)}
		}
	}
	var first, second bytes.Buffer
	assert.NoError(t, comparison.Write(&first, names))
	writeErr := errors.New("destination failed")
	err = comparison.Write(writerError{writeErr}, names)
	assert.FatalIf(t, !errors.Is(err, writeErr), "destination error lost: %v", err)
	assert.NoError(t, comparison.Write(&second, names))
	assert.Diff(t, second.String(), first.String())
	assert.FatalIf(t, first.Len() == 0, "changed groups produced empty %s output", format)
	for _, cluster := range clusters {
		prefix := cluster + ": "
		label := prefix + "ConfigMap apps/config-normalized"
		if format == FormatGitHub {
			label = prefix + "v1/ConfigMap/apps/config-normalized"
		}
		if format == FormatHTML {
			label = prefix + "apps/config-normalized"
		}
		assert.ErrorIf(t, !strings.Contains(first.String(), label), "missing label %q in %s", label, first.String())
		assert.ErrorIf(t, strings.Contains(second.String(), prefix+prefix), "repeated prefix in %s", second.String())
	}
	assert.Equal(t, calls, 4)
	assert.Diff(t, inputs, originals)
	assert.Diff(t, names, beforeNames)
}
