package diff

import (
	"github.com/home-operations/flate/internal/assert"

	"bytes"
	"encoding/json"
	"html"
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCompare_OwnerlessDocuments(t *testing.T) {
	t.Parallel()
	owner := &manifest.Kustomization{Name: "actual", Namespace: "apps"}
	left := selectedSnapshot(Parent{NamedResource: owner.Named()}, nil, store.StatusInfo{})
	right := selectedSnapshot(
		Parent{NamedResource: owner.Named()},
		nil,
		store.StatusInfo{Status: store.StatusFailed, Message: "real driver failed"},
	)
	makeDoc := func(name, image string) map[string]any {
		return map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": name, "namespace": "apps"},
			"spec": map[string]any{
				"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"image": image}}}},
			},
		}
	}
	left.Manifests = append(
		left.Manifests,
		manifest.DocumentBatch{
			Documents: []map[string]any{
				makeDoc("changed", "example.test/app:v1"),
				makeDoc("removed", "example.test/removed:v1"),
			},
		},
	)
	right.Manifests = append(
		right.Manifests,
		manifest.DocumentBatch{
			Documents: []map[string]any{
				makeDoc("changed", "example.test/app:v2"),
				makeDoc("added", "example.test/added:v1"),
			},
		},
	)
	out, _, _, err := Compare(left, right, nil, Options{Format: FormatJSON})
	{
		assert.FatalIf(t, err != nil, "%v", err)
	}
	var result struct {
		Changes    []Change      `json:"changes"`
		Suppressed []Suppression `json:"suppressed"`
	}
	{
		err := json.Unmarshal(out, &result)
		assert.FatalIf(t, err != nil, "%v", err)
	}
	{
		assert.FatalIf(t, len(result.Changes) != 3 || len(result.Suppressed) != 0, "ownerless changes suppressed: %s", out)
	}
	statuses := map[string]ChangeStatus{}
	for _, changed := range result.Changes {
		{
			assert.FatalIf(t, changed.Parent != (Parent{}) || changed.Kind != "Deployment" || changed.Namespace != "apps", "ownerless identity changed: %+v", changed)
		}
		statuses[changed.Name] = changed.Status
	}
	{
		assert.FatalIf(t, statuses["changed"] != StatusChanged || statuses["removed"] != StatusRemoved ||
			statuses["added"] != StatusAdded, "changes = %v", statuses)
	}
	images, notes, err := CompareImages(left, right, nil, true)
	{
		assert.FatalIf(t, err != nil || len(notes) != 0 ||
			!slices.Equal(
				images,
				[]string{"example.test/added:v1", "example.test/app:v1", "example.test/app:v2", "example.test/removed:v1"},
			), "images = %v, notes = %v, err = %v", images, notes, err)
	}
	out, _, _, err = Compare(left, right, nil, Options{Format: FormatDiff})
	{
		assert.FatalIf(t, err != nil || bytes.Contains(out, []byte("Kustomization")) ||
			!bytes.Contains(out, []byte("Deployment apps/changed")), "ownerless heading = %q, err = %v", out, err)
	}
}

func TestCompare_EntryParentDisplay(t *testing.T) {
	t.Parallel()
	for _, applier := range []manifest.NamedResource{{}, {Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Namespace: "apps", Name: "cluster"}} {
		t.Run(applier.Name, func(t *testing.T) {
			t.Parallel()
			root := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "cluster", applier)
			subject := manifest.DiagnosticFor(root.Named(), root)
			parent := Parent{NamedResource: root.Named(), Path: "cluster"}
			before := cmDoc(parent, manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "changed"}, "before", "")
			after := cmDoc(parent, manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "changed"}, "after", "")
			left := selectedSnapshot(parent, []Doc{before, before}, store.StatusInfo{})
			right := selectedSnapshot(parent, []Doc{after, after}, store.StatusInfo{})
			left.Subjects = map[manifest.NamedResource]manifest.DiagnosticIdentity{root.Named(): subject}
			right.Subjects = maps.Clone(left.Subjects)
			originalLeft := manifest.DeepCopyMap(before.Manifest)
			originalRight := manifest.DeepCopyMap(after.Manifest)
			originalSubjects := maps.Clone(left.Subjects)
			out, warnings, _, err := Compare(left, right, nil, Options{Format: FormatJSON})
			{
				assert.FatalIf(t, err != nil, "%v", err)
			}
			var result struct{ Changes []Change }
			{
				err := json.Unmarshal(out, &result)
				assert.FatalIf(t, err != nil, "%v", err)
			}
			{
				assert.FatalIf(t, len(result.Changes) != 1 ||
					result.Changes[0].Parent != (Parent{NamedResource: subject.Resource, Path: subject.Path}) ||
					bytes.Contains(out, []byte(`\u0000`)), "JSON leaked execution attribution: %s", out)
			}
			{
				assert.FatalIf(t, len(warnings) != 2, "duplicate warnings lost: %+v", warnings)
			}
			for _, warning := range warnings {
				{
					assert.FatalIf(t, !slices.Contains(warning.Detail, "parent: "+subject.String()), "warning lost captured parent: %+v", warning)
				}
			}
			for _, format := range []Format{FormatHTML, FormatDiff} {
				out, _, _, err := Compare(left, right, nil, Options{Format: format})
				{
					assert.FatalIf(t, err != nil, "%v", err)
				}
				want := "--- ConfigMap changed (cluster)\n+++ ConfigMap changed (cluster)\n"
				if format == FormatHTML {
					label := `entry path "cluster"`
					if applier != (manifest.NamedResource{}) {
						label = `FluxInstance apps/cluster (entry path "cluster")`
					}
					want = `<div class="rparent">` + html.EscapeString(label+" (cluster)") + `</div>`
				}
				{
					assert.ErrorIf(t, !bytes.Contains(out, []byte(want)), "%s missing heading %q", format, want)
				}
			}
			{
				assert.ErrorIf(t, !reflect.DeepEqual(left.Manifests[0].Documents[0], originalLeft) ||
					!reflect.DeepEqual(right.Manifests[0].Documents[0], originalRight), "%v", "display projection changed snapshot documents")
			}
			{
				assert.ErrorIf(t, !maps.Equal(left.Subjects, originalSubjects) || !maps.Equal(right.Subjects, originalSubjects), "%v", "display projection changed snapshot subjects")
			}
			{
				assert.FatalIf(t, left.Manifests[0].Parent.Named() != root.Named() || right.Manifests[0].Parent.Named() != root.Named(), "%v", "display projection changed raw snapshot parents")
			}
		})
	}
}
