package kustomize

import (
	"slices"
	"testing"

	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPreparation_EntryMembershipContributors(t *testing.T) {
	t.Parallel()
	const localDocument = `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: local
  annotations: {config.kubernetes.io/local-config: 'true'}
`
	const list = `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: ConfigMap
  metadata: {name: inside}
  data: {k: v}
`
	for _, tc := range []struct {
		name, inside, extra string
		allInside           bool
		count               int
		want                map[string]bool
	}{
		{name: "flat files", inside: cmYAML("inside"), count: 2, want: map[string]bool{"inside": true, "outside": false}},
		{name: "multiple documents", inside: cmYAML("inside") + localDocument, count: 2},
		{name: "list", inside: list, count: 2},
		{
			name: "generator beside inside files", inside: cmYAML("inside"), allInside: true, count: 3,
			extra: "configMapGenerator: [{name: generated, literals: [k=v]}]\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": existingKustomization("outside.yaml", "inside.yaml") + tc.extra,
				"inside.yaml":        tc.inside,
				"outside.yaml":       cmYAML("outside"),
			})
			var preparation Preparation
			request := RenderRequest{Source: localSource(t, root), SubPath: ".", Preparation: &preparation}
			docs, receipt, err := Render(t.Context(), newTestTreeCache(t, ""), request, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			assert.Equal(t, len(docs), tc.count)
			membership := preparation.EntryDocumentMembership(t.Context(), receipt, func(files []string) []bool {
				assert.Diff(t, files, []string{"outside.yaml", "inside.yaml"})
				return []bool{tc.allInside, true}
			})
			var names map[string]bool
			if membership != nil {
				names = make(map[string]bool, len(membership))
				for _, doc := range docs {
					if inside, ok := membership[doc.Ordinal]; ok {
						names[manifest.DocIdentity(doc.Document).Name] = inside
					}
				}
			}
			assert.Diff(t, names, tc.want)
		})
	}
}

func TestPreparation_EntryMembershipReadSupport(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"missing preflight read", "changed observation header"} {
		t.Run(tc, func(t *testing.T) {
			t.Parallel()
			recipe := existingKustomization("outside.yaml", "inside.yaml")
			if tc == "missing preflight read" {
				recipe += "sortOptions: {order: fifo}\n"
			}
			outside := cmYAML("outside")
			root := testutil.WriteTree(t, map[string]string{
				"kustomization.yaml": recipe,
				"inside.yaml":        cmYAML("inside"),
				"outside.yaml":       outside,
			})
			var preparation Preparation
			request := RenderRequest{Source: localSource(t, root), SubPath: ".", Preparation: &preparation}
			docs, receipt, err := Render(t.Context(), newTestTreeCache(t, ""), request, sourcecache.RetryConfig{})
			assert.NoError(t, err)
			outsidePos := slices.IndexFunc(docs, func(doc manifest.DocOccurrence) bool {
				return manifest.DocIdentity(doc.Document).Name == "outside"
			})
			assert.FatalIf(t, outsidePos < 0, "full render did not contain the outside match")
			project := func(files []string) []bool {
				assert.Diff(t, files, []string{"outside.yaml", "inside.yaml"})
				return []bool{false, true}
			}
			membership := preparation.EntryDocumentMembership(t.Context(), receipt, project)
			inside, known := membership[docs[outsidePos].Ordinal]
			assert.Equal(t, known, true)
			assert.Equal(t, inside, false)

			if tc == "missing preflight read" {
				frame := receipt.Value().(renderFrame)
				frame.Header.Reads.Files = slices.DeleteFunc(
					slices.Clone(frame.Header.Reads.Files),
					func(read localEntry) bool {
						return read.Path == "kustomization.yaml"
					},
				)
				receipt = renderDomain.Receipt(frame)
			} else {
				project = func(_ []string) []bool {
					testutil.WriteFile(t, root, "outside.yaml", outside+"# changed during the probe\n")
					return []bool{false, true}
				}
			}
			membership = preparation.EntryDocumentMembership(t.Context(), receipt, project)
			_, known = membership[docs[outsidePos].Ordinal]
			assert.Equal(t, known, false)
			assert.Diff(t, membership, map[int]bool(nil))
		})
	}
}

func TestPairEntryDocuments_RejectsUnmatchedSlots(t *testing.T) {
	t.Parallel()
	docs := make([]manifest.DocOccurrence, 0, 2)
	for i, name := range []string{"inside", "outside"} {
		docs = append(docs, manifest.DocOccurrence{Ordinal: i, Document: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": name}, "data": "original",
		}})
	}
	changed := manifest.DocOccurrence{Ordinal: 0, Document: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "inside"}, "data": "changed",
	}}
	for _, tc := range []struct {
		name          string
		raw, observed []manifest.DocOccurrence
	}{
		{name: "duplicate raw identity", raw: []manifest.DocOccurrence{docs[0], docs[0]}, observed: docs},
		{name: "duplicate observed identity", raw: docs, observed: []manifest.DocOccurrence{docs[0], docs[0]}},
		{
			name: "duplicates in both builds", raw: []manifest.DocOccurrence{docs[0], docs[0]},
			observed: []manifest.DocOccurrence{docs[0], docs[0]},
		},
		{name: "different content", raw: docs, observed: []manifest.DocOccurrence{changed, docs[1]}},
		{name: "different count", raw: docs, observed: docs[:1]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Diff(t, pairEntryDocuments(tc.raw, tc.observed, []bool{true, false}), map[int]bool(nil))
		})
	}
}
