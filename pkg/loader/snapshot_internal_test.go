package loader

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

func snapshotDiff(want, got any) string {
	// DiscoveryRecord.Equal omits facts that immutable snapshots MUST retain.
	type record manifest.DiscoveryRecord
	return cmp.Diff(
		want,
		got,
		cmp.AllowUnexported(
			rootEvidence{},
			authoredOccurrence{},
			occurrenceID{},
			occurrenceFacts{},
			manifest.Reference{},
		),
		cmp.Transformer("CompleteRecord", func(r manifest.DiscoveryRecord) record { return record(r) }),
		// Immutable declarations MUST retain their exact object identity.
		cmp.Comparer(func(a, b manifest.BaseManifest) bool { return a == b }),
	)
}

func TestSnapshotDiff_CompleteRecords(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		record manifest.DiscoveryRecord
	}{
		{name: "declaration", record: manifest.DiscoveryRecord{Declaration: &manifest.ConfigMap{Name: "values"}}},
		{name: "builder owner", record: manifest.DiscoveryRecord{BuilderOwner: manifest.BootstrapSourceID}},
		{name: "reference data", record: manifest.DiscoveryRecord{ReferenceData: []manifest.ReferenceData{{Keys: manifest.KeySet{Names: []string{"key"}, Complete: true}}}}},
		{name: "origin", record: manifest.DiscoveryRecord{Origin: manifest.SuppliedDeclaration}},
		{name: "content roots", record: manifest.DiscoveryRecord{ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {"app"}}}},
		{name: "nil reference data", record: manifest.DiscoveryRecord{ReferenceData: []manifest.ReferenceData{}}},
		{name: "nil content roots", record: manifest.DiscoveryRecord{ContentRoots: map[manifest.NamedResource][]string{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var before manifest.DiscoveryRecord
			assert.Equal(t, before.Equal(tc.record), true)
			want := rootEvidence{scope: selection.RootScopeFacts{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{manifest.BootstrapSourceID: before},
			}}
			got := rootEvidence{scope: selection.RootScopeFacts{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{manifest.BootstrapSourceID: tc.record},
			}}
			assert.Equal(t, snapshotDiff(want, want), "")
			assert.Equal(t, snapshotDiff(want, got) != "", true)
		})
	}
}

func TestSnapshotDiff_PrivateFacts(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		facts occurrenceFacts
	}{
		{name: "nil projection", facts: occurrenceFacts{projection: []string{}}},
		{name: "nil labels", facts: occurrenceFacts{labels: map[string]string{}}},
		{name: "nil references", facts: occurrenceFacts{refs: []manifest.Reference{}}},
		{name: "nil reference elements", facts: occurrenceFacts{refs: []manifest.Reference{{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := occurrenceView(&authoredOccurrence{facts: &occurrenceFacts{}})
			got := occurrenceView(&authoredOccurrence{facts: &tc.facts})
			assert.Equal(t, snapshotDiff(want, want), "")
			assert.Equal(t, snapshotDiff(want, got) != "", true)
		})
	}
}

func TestDocumentSnapshot_DeclarationEvidence(t *testing.T) {
	t.Parallel()
	id := manifest.NamedResource{Kind: "Deployment", Name: "work"}
	doc := document{declarations: &fileObservation{
		Present: true, Objects: map[manifest.NamedResource]string{id: "narrow"},
	}}
	doc.snapshot()
	before := doc.parsed
	doc.declarations = &fileObservation{
		Present: true, Complete: true, Objects: map[manifest.NamedResource]string{id: "complete"},
	}
	doc.snapshot()
	assert.Equal(t, before.declarations.Complete, false)
	assert.Equal(t, before.declarations.Objects[id], "narrow")
	assert.Equal(t, doc.parsed.declarations.Complete, true)
	assert.Equal(t, doc.parsed.declarations.Objects[id], "complete")
	assert.Equal(
		t,
		snapshotDiff(before.declarations.FileDeclarations, doc.parsed.declarations.FileDeclarations) != "",
		true,
	)
	data, err := json.Marshal(cachedFileDeclarations{
		Present: true, Complete: true, Objects: []cachedDeclaration{{Identity: id, Fingerprint: "complete"}},
	})
	assert.NoError(t, err)
	deferred := document{declarationsData: data}
	deferred.snapshot()
	assert.NoError(t, deferred.decodeErr)
	assert.Equal(t, deferred.declarationsData == nil, true)
	assert.Diff(t, deferred.parsed.declarations.FileDeclarations, doc.parsed.declarations.FileDeclarations)
	failed := document{declarationsData: []byte("[]")}
	assert.Equal(t, errors.Is(failed.restoreDeclarations(), manifest.ErrFlux), true)
	failed.snapshot()
	assert.Equal(t, errors.Is(failed.parsed.decodeErr, manifest.ErrFlux), true)
}
