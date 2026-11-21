package diff

import (
	"bytes"
	"errors"
	"html"
	"maps"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestComparison_WriteEntryProducer(t *testing.T) {
	t.Parallel()
	entryID := testutil.KustomizationID("apps", "entry")
	ownerID := testutil.KustomizationID("apps", "owner")
	doc := ncm("root-output", "key", "value")
	old, current := doc, doc
	old.Producer = manifest.DiagnosticIdentity{ID: entryID, Path: "app"}
	current.Producer = manifest.DiagnosticFor(ownerID, nil)
	for _, format := range []Format{FormatHuman, FormatDiff} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			out, _, err := renderDocuments([]Doc{old}, []Doc{current}, &Options{Format: format})
			assert.NoError(t, err)
			assert.Equal(
				t,
				string(out),
				"ConfigMap apps/root-output producer: entry path \"app\" -> Kustomization apps/owner\n",
			)
		})
	}
}

func TestComparison_WriteTwice(t *testing.T) {
	t.Parallel()
	for _, format := range []Format{FormatGitHub, FormatDiff, FormatHTML, FormatJSON} {
		t.Run(string(format), func(t *testing.T) {
			t.Parallel()
			for _, supplied := range []bool{false, true} {
				name := "default names"
				if supplied {
					name = "supplied names"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					testComparisonWriteTwice(t, format, supplied, false)
					testComparisonWriteTwice(t, format, supplied, true)
				})
			}
		})
	}
}

func testComparisonWriteTwice(t *testing.T, format Format, supplied, generated bool) {
	t.Helper()
	fixture := repeatedComparisonGroups(t, generated)
	calls := 0
	comparison, err := Compare(fixture.groups, Options{Format: format, Normalize: func(doc map[string]any) {
		calls++
		metadata := doc["metadata"].(map[string]any)
		if generated {
			doc["data"].(map[string]any)["normalized"] = "yes"
		} else {
			metadata["name"] = metadata["name"].(string) + "-normalized"
		}
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
	name := repeatedComparisonResource(&fixture.groups[0], format, generated)
	for i, cluster := range clusters {
		prefix := cluster + ": "
		label := repeatedComparisonLabel(format, generated, prefix, name)
		assert.ErrorIf(t, !strings.Contains(first.String(), label), "missing label %q in %s", label, first.String())
		if !generated {
			assertRepeatedComparisonMove(t, format, fixture.groups[i].Cluster, first.String())
		}
		assert.ErrorIf(t, strings.Contains(second.String(), prefix+prefix), "repeated prefix in %s", second.String())
	}
	assert.Equal(t, calls, 4)
	assert.Diff(t, fixture.inputs, fixture.originals)
	assert.Diff(t, names, beforeNames)
}

type repeatedComparisonFixture struct {
	groups            []Group
	inputs, originals []map[string]any
}

func repeatedComparisonGroups(t *testing.T, generated bool) repeatedComparisonFixture {
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
			if generated {
				doc = withGeneratorHash(t, new(Doc{Manifest: doc})).Manifest
			}
			snapshots[side].Manifests = []manifest.DocumentBatch{testutil.DocumentBatch(parent, []map[string]any{doc})}
			if !generated {
				id := testutil.KustomizationID("apps", value)
				snapshots[side].Producers = map[int]map[int]manifest.DiagnosticIdentity{
					0: {0: manifest.DiagnosticFor(id, nil)},
				}
				snapshots[side].Subjects = map[manifest.NamedResource]manifest.DiagnosticIdentity{
					id: {ID: id, Resource: parent.Named(), Path: cluster + "/" + value},
				}
			}
			inputs = append(inputs, doc)
			originals = append(originals, manifest.DeepCopyMap(doc))
		}
	}
	return repeatedComparisonFixture{groups: groups, inputs: inputs, originals: originals}
}

func repeatedComparisonResource(group *Group, format Format, generated bool) string {
	if !generated {
		return "config-normalized"
	}
	snapshot := &group.Current
	if format == FormatGitHub {
		snapshot = &group.Orig
	}
	return manifest.DocIdentity(snapshot.Manifests[0].Documents[0]).Name
}

func repeatedComparisonLabel(format Format, generated bool, prefix, name string) string {
	switch {
	case format == FormatGitHub && generated:
		return prefix + "v1/ConfigMap/apps/" + name
	case format == FormatHTML:
		return prefix + "apps/" + name
	case format == FormatJSON:
		return `"name": "` + name + `"`
	default:
		return prefix + "ConfigMap apps/" + name
	}
}

func assertRepeatedComparisonMove(t *testing.T, format Format, entryPath, output string) {
	t.Helper()
	if format == FormatJSON {
		assert.Equal(t, strings.Count(output, `"producerChange":`), 2)
		for _, side := range []string{"old", "new"} {
			assert.Equal(t, strings.Count(output, `"path": "`+entryPath+"/"+side+`"`), 1)
		}
		return
	}
	move := `producer: Kustomization apps/render (entry path "` + entryPath + `/old") -> Kustomization apps/render (entry path "` + entryPath + `/new")`
	assert.Equal(t, strings.Count(html.UnescapeString(output), move), 1)
}
