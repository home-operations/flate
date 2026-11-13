package cache

import (
	"encoding/json"
	"maps"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAcceptedRender_ExactValue(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Group: "group", Kind: "kind", Namespace: "namespace", Name: "name"}
	original := acceptedRender{
		Version: frameVersion, Final: manifest.Fingerprint("final"), Complete: true, Owner: id, Raw: "raw",
		Records: []acceptedRecord{{
			ID: id, Record: manifest.DiscoveryRecord{
				Authored: id,
				Path:     "path",
				File:     "file",
				Parent:   id,
				Refs:     []manifest.NamedResource{id},
				Productions: []manifest.Production{
					{Owner: id, Declaration: id, Authored: id, Source: id, Path: "path", File: "file"},
				},
				Builder: "builder",
				Inputs:  []string{"input"},
				Pinned:  true,
			}, Roots: []acceptedRoot{{Source: id, Paths: []string{"path"}}},
		}},
	}
	copyValue := func(t *testing.T, a acceptedRender) acceptedRender {
		t.Helper()
		data, err := json.Marshal(a)
		assert.NoError(t, err)
		var b acceptedRender
		assert.NoError(t, json.Unmarshal(data, &b))
		return b
	}
	assert.FatalIf(
		t,
		!original.equal(&original) || original.equal(nil) || (*acceptedRender)(nil).equal(&original),
		"%s",
		"accepted-value presence comparison differs",
	)
	assert.FatalIf(t, !(*acceptedRender)(nil).equal(nil), "%s", "nil accepted values differ")
	for _, tc := range []struct {
		name   string
		mutate func(*acceptedRender)
	}{
		{"version", func(a *acceptedRender) { a.Version++ }},
		{"complete", func(a *acceptedRender) { a.Complete = false }},
		{"raw", func(a *acceptedRender) { a.Raw = "different" }},
		{"final inputs", func(a *acceptedRender) { a.Final = "different" }},
		{"application", func(a *acceptedRender) { a.Records[0].Record.Application = "different" }},
		{"static output", func(a *acceptedRender) { a.Records[0].Record.StaticOutput = true }},
		{"record count", func(a *acceptedRender) { a.Records = nil }},
		{"record length", func(a *acceptedRender) { a.Records = a.Records[:0] }},
		{"authored", func(a *acceptedRender) { a.Records[0].Record.Authored.Name = "different" }},
		{"path", func(a *acceptedRender) { a.Records[0].Record.Path = "different" }},
		{"file", func(a *acceptedRender) { a.Records[0].Record.File = "different" }},
		{"parent", func(a *acceptedRender) { a.Records[0].Record.Parent.Name = "different" }},
		{"builder", func(a *acceptedRender) { a.Records[0].Record.Builder = "different" }},
		{"inputs", func(a *acceptedRender) { a.Records[0].Record.Inputs[0] = "different" }},
		{"pinned", func(a *acceptedRender) { a.Records[0].Record.Pinned = false }},
		{"ref count", func(a *acceptedRender) { a.Records[0].Record.Refs = nil }},
		{"production count", func(a *acceptedRender) { a.Records[0].Record.Productions = nil }},
		{"production path", func(a *acceptedRender) { a.Records[0].Record.Productions[0].Path = "different" }},
		{"production file", func(a *acceptedRender) { a.Records[0].Record.Productions[0].File = "different" }},
		{"root count", func(a *acceptedRender) { a.Records[0].Roots = nil }},
		{"root length", func(a *acceptedRender) { a.Records[0].Roots = a.Records[0].Roots[:0] }},
		{"root path count", func(a *acceptedRender) { a.Records[0].Roots[0].Paths = nil }},
		{"root path", func(a *acceptedRender) { a.Records[0].Roots[0].Paths[0] = "different" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := copyValue(t, original)
			tc.mutate(&changed)
			assert.FatalIf(
				t,
				original.equal(&changed) || changed.equal(&original),
				"%s",
				"changed accepted value compared equal",
			)
		},
		)
	}
	for _, location := range []struct {
		name     string
		selectID func(*acceptedRender) *manifest.NamedResource
	}{
		{"owner", func(a *acceptedRender) *manifest.NamedResource { return &a.Owner }},
		{"record", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].ID }},
		{"authored", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Authored }},
		{"parent", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Parent }},
		{"reference", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Refs[0] }},
		{"production owner", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Productions[0].Owner }},
		{"production declaration", func(a *acceptedRender) *manifest.NamedResource {
			return &a.Records[0].Record.Productions[0].Declaration
		}},
		{"production authored", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Productions[0].Authored }},
		{"production source", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Record.Productions[0].Source }},
		{"root source", func(a *acceptedRender) *manifest.NamedResource { return &a.Records[0].Roots[0].Source }},
	} {
		for _, component := range []struct {
			name   string
			mutate func(*manifest.NamedResource)
		}{
			{"group", func(n *manifest.NamedResource) { n.Group = "different" }},
			{"kind", func(n *manifest.NamedResource) { n.Kind = "different" }},
			{"namespace", func(n *manifest.NamedResource) { n.Namespace = "different" }},
			{"name", func(n *manifest.NamedResource) { n.Name = "different" }},
		} {
			t.Run(location.name+"/"+component.name, func(t *testing.T) {
				t.Parallel()
				changed := copyValue(t, original)
				component.mutate(location.selectID(&changed))
				assert.FatalIf(
					t,
					original.equal(&changed) || changed.equal(&original),
					"%s",
					"changed full identity compared equal",
				)
			},
			)
		}
	}
	for _, tc := range []struct {
		name     string
		setNil   func(*acceptedRender)
		setEmpty func(*acceptedRender)
	}{
		{"records", func(a *acceptedRender) { a.Records = nil }, func(a *acceptedRender) { a.Records = []acceptedRecord{} }},
		{"inputs", func(a *acceptedRender) {
			a.Records[0].Record.Inputs = nil
		}, func(a *acceptedRender) {
			a.Records[0].Record.Inputs = []string{}
		}},
		{"references", func(a *acceptedRender) {
			a.Records[0].Record.Refs = nil
		}, func(a *acceptedRender) {
			a.Records[0].Record.Refs = []manifest.NamedResource{}
		}},
		{"productions", func(a *acceptedRender) {
			a.Records[0].Record.Productions = nil
		}, func(a *acceptedRender) {
			a.Records[0].Record.Productions = []manifest.Production{}
		}},
		{"roots", func(a *acceptedRender) { a.Records[0].Roots = nil }, func(a *acceptedRender) { a.Records[0].Roots = []acceptedRoot{} }},
		{"paths", func(a *acceptedRender) {
			a.Records[0].Roots[0].Paths = nil
		}, func(a *acceptedRender) {
			a.Records[0].Roots[0].Paths = []string{}
		}},
	} {
		t.Run("nil-empty/"+tc.name, func(t *testing.T) {
			t.Parallel()
			a, b := copyValue(t, original), copyValue(t, original)
			tc.setNil(&a)
			tc.setEmpty(&b)
			assert.FatalIf(t, a.equal(&b) || b.equal(&a), "%s", "nil and empty serialized slices compared equal")
		},
		)
	}
}

func TestAcceptedRender_OneShotCanonicalInventory(t *testing.T) {
	t.Parallel()
	a := manifest.NamedResource{Kind: "kind", Name: "a"}
	b := a
	b.Name = "b"
	record := manifest.DiscoveryRecord{
		Refs:         []manifest.NamedResource{b, a, b},
		Productions:  []manifest.Production{{Owner: b}, {Owner: a}, {Owner: b}},
		ContentRoots: map[manifest.NamedResource][]string{b: {"z", "a", "z"}, a: nil},
	}
	calls := 0
	oneShot := func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
		calls++
		assert.FatalIf(t, calls > 1, "%s", "accepted inventory was traversed more than once")
		if yield(b, record) {
			yield(a, record)
		}
	}
	frame := Frame{raw: "raw"}
	got, err := prepareAccepted(a, &frame, manifest.Fingerprint("final"), oneShot)
	assert.NoError(t, err)
	want, err := prepareAccepted(
		a,
		&frame,
		manifest.Fingerprint("final"),
		maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{a: record, b: record}),
	)
	assert.NoError(t, err)
	assert.FatalIf(t, !got.equal(&want) || calls != 1, "%s", "one-shot unordered inventory did not canonicalize")
	assert.Equal(t, got.Records[0].ID, a)
	assert.Diff(t, got.Records[0].Record.Refs, []manifest.NamedResource{a, b})
	assert.Diff(t, got.Records[0].Roots[1].Paths, []string{"a", "z"})
	record.Refs[0].Name = "mutated"
	record.Productions[0].Path = "mutated"
	record.ContentRoots[b][0] = "mutated"
	assert.FatalIf(t, !got.equal(&want), "%s", "caller mutation changed the prepared inventory")
	_, err = prepareAccepted(a, &frame, manifest.Fingerprint("final"), func(
		yield func(
			manifest.NamedResource,
			manifest.DiscoveryRecord,
		) bool,
	) {
		if yield(a, record) {
			yield(a, record)
		}
	})
	assert.FatalIf(t, err == nil, "%s", "duplicate accepted identity was accepted")
}
