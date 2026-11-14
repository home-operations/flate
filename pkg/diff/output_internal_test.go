package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	_ "github.com/home-operations/flate/internal/kustomize" // The SDK scope resolver must be registered before emitted identity queries.
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // Native diff writers change process-global color settings.
func TestCompare_OutputMembership(t *testing.T) {
	parent := kustomizationParent()
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "config"}
	oldDoc := cmDoc(&parent, id, "old", "")
	newDoc := cmDoc(&parent, id, "new", "")
	old := selectedSnapshot(&parent, []Doc{oldDoc}, store.StatusInfo{})
	current := selectedSnapshot(&parent, []Doc{newDoc}, store.StatusInfo{})
	failed := selectedSnapshot(&parent, []Doc{newDoc}, store.StatusInfo{Status: store.StatusFailed, Message: "failed"})
	partial := errors.New("partial render")
	for _, format := range Formats() {
		t.Run(string(format), func(t *testing.T) {
			for _, tc := range []struct {
				name      string
				group     Group
				normalize bool
				want      [2][]manifest.NamedResource
			}{
				{name: "changed pair", group: Group{Orig: old, Current: current}, want: [2][]manifest.NamedResource{{id}, {id}}},
				{name: "added", group: Group{Current: current}, want: [2][]manifest.NamedResource{nil, {id}}},
				{name: "removed", group: Group{Orig: old}, want: [2][]manifest.NamedResource{{id}, nil}},
				{name: "unchanged", group: Group{Orig: old, Current: old}},
				{name: "normalization only", group: Group{Orig: old, Current: current}, normalize: true},
				{name: "suppressed current", group: Group{Orig: old, Current: failed}},
				{name: "suppressed original", group: Group{Orig: failed, Current: current}},
				{name: "both unavailable", group: Group{Orig: failed, Current: failed}},
				{name: "partial failure", group: Group{Orig: old, Current: current, SelectedErr: partial}, want: [2][]manifest.NamedResource{{id}, {id}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					opts := Options{Format: format}
					if tc.normalize {
						opts.Normalize = func(doc map[string]any) { delete(doc, "data") }
					}
					comparison, err := Compare([]Group{tc.group}, opts)
					assert.ErrorIf(
						t,
						!errors.Is(err, tc.group.SelectedErr),
						"error = %v, want %v",
						err,
						tc.group.SelectedErr,
					)
					want := tc.want
					if format == FormatBrief {
						want = [2][]manifest.NamedResource{}
					}
					assert.Diff(t, comparison.Groups[0].Output, want)
					var first, second bytes.Buffer
					assert.NoError(t, comparison.Write(&first, nil))
					assert.NoError(t, comparison.Write(&second, nil))
					assert.Diff(t, second.String(), first.String())
					assert.Diff(t, comparison.Groups[0].Output, want)
				})
			}
		})
	}
}

//nolint:paralleltest // Native diff writers change process-global color settings.
func TestCompare_OutputGroupsAndPositions(t *testing.T) {
	parent := kustomizationParent()
	old := []Doc{
		cmDoc(&parent, manifest.NamedResource{Name: "removed", Namespace: "apps"}, "old", ""),
		cmDoc(&parent, manifest.NamedResource{Name: "changed", Namespace: "apps"}, "old", ""),
		cmDoc(&parent, manifest.NamedResource{Name: "unchanged", Namespace: "apps"}, "same", ""),
	}
	current := []Doc{
		cmDoc(&parent, manifest.NamedResource{Name: "unchanged", Namespace: "apps"}, "same", ""),
		cmDoc(&parent, manifest.NamedResource{Name: "changed", Namespace: "apps"}, "new", ""),
		cmDoc(&parent, manifest.NamedResource{Name: "added", Namespace: "apps"}, "new", ""),
	}
	left, right := selectedSnapshot(
		&parent,
		old,
		store.StatusInfo{},
	), selectedSnapshot(
		&parent,
		current,
		store.StatusInfo{},
	)
	for _, format := range Formats() {
		t.Run(string(format), func(t *testing.T) {
			comparison, err := Compare([]Group{
				{Cluster: "changes", Orig: left, Current: right},
				{Cluster: "removed", Orig: right, Removed: true},
				{Cluster: "added", Current: left, Added: true},
			}, Options{Format: format})
			assert.NoError(t, err)
			want := [][2][]manifest.NamedResource{
				{
					{manifest.DocIdentity(old[0].Manifest), manifest.DocIdentity(old[1].Manifest)},
					{manifest.DocIdentity(current[1].Manifest), manifest.DocIdentity(current[2].Manifest)},
				},
				{
					{
						manifest.DocIdentity(current[0].Manifest),
						manifest.DocIdentity(current[1].Manifest),
						manifest.DocIdentity(current[2].Manifest),
					},
					nil,
				},
				{
					nil,
					{
						manifest.DocIdentity(old[0].Manifest),
						manifest.DocIdentity(old[1].Manifest),
						manifest.DocIdentity(old[2].Manifest),
					},
				},
			}
			var out bytes.Buffer
			assert.NoError(t, comparison.Write(&out, nil))
			for group, result := range comparison.Groups {
				for side, ids := range result.Output {
					expected := want[group][side]
					if format == FormatBrief {
						expected = nil
					}
					got := slices.Clone(ids)
					slices.SortFunc(got, manifest.NamedResource.Compare)
					slices.SortFunc(expected, manifest.NamedResource.Compare)
					assert.Diff(t, got, expected)
				}
			}
			t.Run("invalid group", func(t *testing.T) {
				testCompareInvalidOutputGroup(t, []Group{
					{Cluster: "changes", Orig: left, Current: right},
					{Cluster: "removed", Orig: right, Removed: true},
					{Cluster: "added", Current: left, Added: true},
				}, want, comparison)
			})
		})
	}
}

//nolint:paralleltest // Native diff writers change process-global color settings.
func TestCompare_OutputInternalNamespaces(t *testing.T) {
	parents := [2]*manifest.ResourceSet{
		{Name: "first", Namespace: "apps"},
		{Name: "second", Namespace: "other"},
	}
	for _, format := range Formats() {
		t.Run(string(format), func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				changed    [2]bool
				namespace  string
				identities string
			}{
				{name: "first producer", changed: [2]bool{true, false}},
				{name: "second producer", changed: [2]bool{false, true}},
				{name: "both producers", changed: [2]bool{true, true}},
				{name: "explicit namespace", changed: [2]bool{true, false}, namespace: "explicit"},
				{name: "policy derived", changed: [2]bool{true, true}, identities: "omitted"},
				{name: "empty identities", changed: [2]bool{true, true}, identities: "empty"},
				{name: "mixed identities", changed: [2]bool{true, true}, identities: "mixed"},
				{name: "authoritative emitter", changed: [2]bool{true, true}, identities: "supplied"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					snapshots, want := internalNamespaceSnapshots(parents, tc.changed, tc.namespace)
					setNamespaceBatchIdentities(&snapshots, tc.identities, want)
					before, err := json.Marshal(
						[2][]manifest.DocumentBatch{snapshots[0].Manifests, snapshots[1].Manifests},
					)
					assert.NoError(t, err)
					if format == FormatBrief {
						want = nil
					}
					comparison, err := Compare(
						[]Group{{Orig: snapshots[0], Current: snapshots[1]}},
						Options{Format: format},
					)
					assert.NoError(t, err)
					for side, got := range comparison.Groups[0].Output {
						assert.Diff(t, got, want)
						for _, batch := range snapshots[side].Manifests {
							assert.Equal(t, manifest.DocMetadata(batch.Documents[0]).Namespace, tc.namespace)
						}
					}
					after, err := json.Marshal(
						[2][]manifest.DocumentBatch{snapshots[0].Manifests, snapshots[1].Manifests},
					)
					assert.NoError(t, err)
					assert.Diff(t, after, before)
				})
			}
		})
	}
}

func internalNamespaceSnapshots(
	parents [2]*manifest.ResourceSet, changed [2]bool, namespace string,
) ([2]Snapshot, []manifest.NamedResource) {
	var snapshots [2]Snapshot
	var want []manifest.NamedResource
	for producer, parent := range parents {
		for side := range snapshots {
			metadata := map[string]any{"name": "config"}
			if namespace != "" {
				metadata["namespace"] = namespace
			}
			value := "before"
			if side == 1 && changed[producer] {
				value = "after"
			}
			doc := map[string]any{
				"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata,
				"data": map[string]any{"value": value},
			}
			snapshots[side].Manifests = append(
				snapshots[side].Manifests,
				testutil.DocumentBatch(parent, []map[string]any{doc}),
			)
		}
		if changed[producer] {
			expectedNamespace := parent.Namespace
			if namespace != "" {
				expectedNamespace = namespace
			}
			want = append(
				want,
				manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: expectedNamespace, Name: "config"},
			)
		}
	}
	return snapshots, want
}

func setNamespaceBatchIdentities(snapshots *[2]Snapshot, mode string, want []manifest.NamedResource) {
	if mode == "supplied" {
		for i := range want {
			want[i].Namespace = "emitter-" + want[i].Namespace
		}
	}
	for side := range snapshots {
		for producer := range snapshots[side].Manifests {
			batch := &snapshots[side].Manifests[producer]
			switch mode {
			case "omitted":
				batch.Identities = nil
			case "empty":
				batch.Identities = []manifest.NamedResource{}
			case "mixed":
				if producer == side {
					batch.Identities = nil
				}
			case "supplied":
				batch.Identities[0].Namespace = "emitter-" + batch.Parent.Named().Namespace
			}
		}
	}
}

func testCompareInvalidOutputGroup(
	t *testing.T,
	groups []Group,
	want [][2][]manifest.NamedResource,
	control *Comparison,
) {
	t.Helper()
	format := control.format
	left, right := groups[0].Orig, groups[0].Current
	old := documents(left.Manifests)
	current := documents(right.Manifests)
	producer := &manifest.ResourceSet{Name: "invalid", Namespace: "apps"}
	subject := manifest.DiagnosticIdentity{
		ID: producer.Named(), Path: "clusters/apps",
		Resource: manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: "FluxInstance", Name: "cluster"},
	}
	invalid := Snapshot{
		Manifests: []manifest.DocumentBatch{{
			Parent: producer, Documents: []map[string]any{old[0].Manifest},
			Identities: []manifest.NamedResource{{}, {}},
		}},
		Unavailable: map[manifest.NamedResource]store.StatusInfo{
			producer.Named(): {Status: store.StatusFailed, Message: "failed"},
		},
		Subjects: map[manifest.NamedResource]manifest.DiagnosticIdentity{producer.Named(): subject},
	}
	healthy := Snapshot{Manifests: []manifest.DocumentBatch{{
		Parent: producer, Documents: []map[string]any{old[0].Manifest},
	}}}
	selectedErr := errors.New("partial render")
	groups = slices.Insert(slices.Clone(groups), 1, Group{
		Cluster: "invalid", Orig: invalid, Current: healthy, SelectedErr: selectedErr, Removed: true,
	})
	before, err := json.Marshal(
		[][]manifest.DocumentBatch{left.Manifests, right.Manifests, invalid.Manifests, healthy.Manifests},
	)
	assert.NoError(t, err)
	partial, err := Compare(groups, Options{Format: format})
	assert.FatalIf(t, !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) ||
		!errors.Is(err, selectedErr), "classified or selected error lost: %v", err)
	assert.Equal(t, len(partial.Groups), 4)
	assert.Diff(t, partial.Groups[1].Output, [2][]manifest.NamedResource{})
	note := Suppression{Parent: objectParent(producer), Subject: subject, Side: SideOrig, Count: 1, Reason: "failed"}
	assert.Diff(t, partial.Groups[1].Suppressed, []Suppression{note})
	positions := []int{0, 2, 3}
	for original, position := range positions {
		for side, ids := range partial.Groups[position].Output {
			expected := slices.Clone(want[original][side])
			if format == FormatBrief {
				expected = nil
			}
			got := slices.Clone(ids)
			slices.SortFunc(got, manifest.NamedResource.Compare)
			slices.SortFunc(expected, manifest.NamedResource.Compare)
			assert.Diff(t, got, expected)
		}
	}
	for i := range groups {
		group := &groups[i]
		assert.Equal(t, partial.output[i].cluster, group.Cluster)
		assert.Equal(t, partial.output[i].added, group.Added)
		assert.Equal(t, partial.output[i].removed, group.Removed)
		assert.Equal(t, partial.names()[i].Prefix, group.Cluster+": ")
	}
	var first, second bytes.Buffer
	assert.NoError(t, partial.Write(&first, nil))
	assert.NoError(t, partial.Write(&second, nil))
	assert.Diff(t, second.String(), first.String())
	if format != FormatJSON {
		var valid bytes.Buffer
		assert.NoError(t, control.Write(&valid, nil))
		assert.Diff(t, first.String(), valid.String())
	} else {
		assert.Diff(t, first.String(), expectedPartialGroupJSON(t, old, current, &note))
	}
	after, err := json.Marshal(
		[][]manifest.DocumentBatch{left.Manifests, right.Manifests, invalid.Manifests, healthy.Manifests},
	)
	assert.NoError(t, err)
	assert.Diff(t, after, before)
}

func expectedPartialGroupJSON(t *testing.T, old, current []Doc, note *Suppression) string {
	t.Helper()
	displayed := *note
	displayed.Parent = Parent{NamedResource: note.Subject.Resource, Path: note.Subject.Path}
	expected := []map[string]any{
		{"cluster": "changes", "suppressed": []Suppression{}, "changes": []Change{}},
		{"cluster": "invalid", "removed": true, "suppressed": []Suppression{displayed}, "changes": []Change{}},
		{"cluster": "removed", "removed": true, "suppressed": []Suppression{}, "changes": []Change{}},
		{"cluster": "added", "added": true, "suppressed": []Suppression{}, "changes": []Change{}},
	}
	for _, change := range []struct {
		group  int
		status ChangeStatus
		old    map[string]any
		new    map[string]any
	}{
		{group: 0, status: StatusAdded, new: current[2].Manifest},
		{group: 0, status: StatusChanged, old: old[1].Manifest, new: current[1].Manifest},
		{group: 0, status: StatusRemoved, old: old[0].Manifest},
		{group: 2, status: StatusRemoved, old: current[2].Manifest},
		{group: 2, status: StatusRemoved, old: current[1].Manifest},
		{group: 2, status: StatusRemoved, old: current[0].Manifest},
		{group: 3, status: StatusAdded, new: old[1].Manifest},
		{group: 3, status: StatusAdded, new: old[0].Manifest},
		{group: 3, status: StatusAdded, new: old[2].Manifest},
	} {
		doc := change.new
		if doc == nil {
			doc = change.old
		}
		id := manifest.DocIdentity(doc)
		expected[change.group]["changes"] = append(expected[change.group]["changes"].([]Change), Change{
			Parent: old[0].Parent, APIVersion: "v1", Kind: id.Kind, Namespace: id.Namespace, Name: id.Name,
			Status: change.status, Old: change.old, New: change.new,
		})
	}
	wantBytes, err := json.MarshalIndent(expected, "", "  ")
	assert.NoError(t, err)
	return string(wantBytes) + "\n"
}
