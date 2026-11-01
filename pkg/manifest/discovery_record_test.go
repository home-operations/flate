package manifest

import (
	"slices"
	"testing"
)

func discoveryRecordFixture(t *testing.T) DiscoveryRecord {
	t.Helper()
	id := NamedResource{Group: "group", Kind: "kind", Namespace: "namespace", Name: "name"}
	other := id
	other.Name = "other"
	return DiscoveryRecord{
		Application: "application", Authored: id, Path: "path", File: "file", Parent: id, Builder: "builder",
		Pinned: true, StaticOutput: true, Inputs: []string{"first", "second"}, Refs: []NamedResource{id, other},
		Productions: []Production{
			{Owner: id, Declaration: id, Authored: id, Source: id, Path: "path", File: "file"},
			{Owner: other, Path: "other"},
		},
	}
}

func TestDiscoveryRecord_Equal(t *testing.T) {
	t.Parallel()
	original := discoveryRecordFixture(t)
	if !original.Equal(discoveryRecordFixture(t)) || !(DiscoveryRecord{}).Equal(DiscoveryRecord{}) {
		t.Fatal("equal records differ")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*DiscoveryRecord)
	}{
		{"application", func(r *DiscoveryRecord) { r.Application = "other" }},
		{"path", func(r *DiscoveryRecord) { r.Path = "other" }},
		{"file", func(r *DiscoveryRecord) { r.File = "other" }},
		{"builder", func(r *DiscoveryRecord) { r.Builder = "other" }},
		{"pinned", func(r *DiscoveryRecord) { r.Pinned = false }},
		{"static output", func(r *DiscoveryRecord) { r.StaticOutput = false }},
		{"input value", func(r *DiscoveryRecord) { r.Inputs[0] = "other" }},
		{"input length", func(r *DiscoveryRecord) { r.Inputs = r.Inputs[:1] }},
		{"input order", func(r *DiscoveryRecord) { slices.Reverse(r.Inputs) }},
		{"reference length", func(r *DiscoveryRecord) { r.Refs = r.Refs[:1] }},
		{"reference order", func(r *DiscoveryRecord) { slices.Reverse(r.Refs) }},
		{"production length", func(r *DiscoveryRecord) { r.Productions = r.Productions[:1] }},
		{"production order", func(r *DiscoveryRecord) { slices.Reverse(r.Productions) }},
		{"production path", func(r *DiscoveryRecord) { r.Productions[0].Path = "other" }},
		{"production file", func(r *DiscoveryRecord) { r.Productions[0].File = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := discoveryRecordFixture(t)
			tc.mutate(&changed)
			if original.Equal(changed) || changed.Equal(original) {
				t.Fatal("changed record compared equal")
			}
		})
	}
}

func TestDiscoveryRecord_EqualIdentities(t *testing.T) {
	t.Parallel()
	for _, location := range []struct {
		name     string
		selectID func(*DiscoveryRecord) *NamedResource
	}{
		{"authored", func(r *DiscoveryRecord) *NamedResource { return &r.Authored }},
		{"parent", func(r *DiscoveryRecord) *NamedResource { return &r.Parent }},
		{"reference", func(r *DiscoveryRecord) *NamedResource { return &r.Refs[0] }},
		{"production owner", func(r *DiscoveryRecord) *NamedResource { return &r.Productions[0].Owner }},
		{"production declaration", func(r *DiscoveryRecord) *NamedResource { return &r.Productions[0].Declaration }},
		{"production authored", func(r *DiscoveryRecord) *NamedResource { return &r.Productions[0].Authored }},
		{"production source", func(r *DiscoveryRecord) *NamedResource { return &r.Productions[0].Source }},
	} {
		for _, component := range []struct {
			name   string
			mutate func(*NamedResource)
		}{
			{"group", func(n *NamedResource) { n.Group = "other" }},
			{"kind", func(n *NamedResource) { n.Kind = "other" }},
			{"namespace", func(n *NamedResource) { n.Namespace = "other" }},
			{"name", func(n *NamedResource) { n.Name = "other" }},
		} {
			t.Run(location.name+"/"+component.name, func(t *testing.T) {
				t.Parallel()
				original, changed := discoveryRecordFixture(t), discoveryRecordFixture(t)
				component.mutate(location.selectID(&changed))
				if original.Equal(changed) || changed.Equal(original) {
					t.Fatal("changed identity compared equal")
				}
			})
		}
	}
}

func TestDiscoveryRecord_EqualPresence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                 string
		nilValue, emptyValue DiscoveryRecord
	}{
		{name: "inputs", emptyValue: DiscoveryRecord{Inputs: []string{}}},
		{name: "references", emptyValue: DiscoveryRecord{Refs: []NamedResource{}}},
		{name: "productions", emptyValue: DiscoveryRecord{Productions: []Production{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilValue.Equal(tc.emptyValue) || tc.emptyValue.Equal(tc.nilValue) {
				t.Fatal("nil and empty collections compared equal")
			}
		})
	}
}

func TestDiscoveryRecord_EqualExcludedFields(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*DiscoveryRecord)
	}{
		{"declaration", func(r *DiscoveryRecord) { r.Declaration = &Kustomization{Name: "other"} }},
		{"declared fingerprint", func(r *DiscoveryRecord) { r.Declared = "other" }},
		{"builder owner", func(r *DiscoveryRecord) { r.BuilderOwner = NamedResource{Name: "other"} }},
		{"reference data", func(r *DiscoveryRecord) { r.ReferenceData = []ReferenceData{{DeclaredKeys: []string{"other"}}} }},
		{"content roots", func(r *DiscoveryRecord) { r.ContentRoots = map[NamedResource][]string{{Name: "other"}: {"path"}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original, changed := discoveryRecordFixture(t), discoveryRecordFixture(t)
			tc.mutate(&changed)
			if !original.Equal(changed) || !changed.Equal(original) {
				t.Fatal("excluded live state changed accepted-record equality")
			}
		})
	}
}
