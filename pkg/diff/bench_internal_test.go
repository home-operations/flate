package diff

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func BenchmarkDiff_LargeTree(b *testing.B) {
	const n = 100
	buildDiffCorpusResult1 := buildDiffCorpus(n, 5)
	left, right := buildDiffCorpusResult1.left, buildDiffCorpusResult1.right
	opts := Options{Format: FormatGitHub}
	snapshots := documentSnapshots(left, right, &opts)
	groups := []Group{{Orig: snapshots[0], Current: snapshots[1]}}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		comparison, err := Compare(groups, opts)
		var out bytes.Buffer
		if err == nil {
			err = comparison.Write(&out, nil)
		}
		if err != nil {
			b.Fatalf("Compare/Write: %v", err)
		}
		if out.Len() == 0 {
			b.Fatalf("expected at least one diff")
		}
	}
}

func BenchmarkDiff_PreparedSubjects(b *testing.B) {
	for _, format := range []Format{FormatJSON, FormatHTML} {
		b.Run(string(format), func(b *testing.B) {
			buildDiffCorpusResult2 := buildDiffCorpus(3, 1)
			left, right := buildDiffCorpusResult2.left, buildDiffCorpusResult2.right
			left = left[:2]
			right = append(right[:1], right[2:]...)
			id := left[0].Parent.NamedResource
			opts := Options{Format: format, subjects: [2]map[manifest.NamedResource]manifest.DiagnosticIdentity{
				{id: {ID: id, Resource: testutil.KustomizationID("apps", "original"), Path: "original/path"}},
				{id: {ID: id, Resource: testutil.KustomizationID("apps", "current"), Path: "current/path"}},
			}}
			snapshots := documentSnapshots(left, right, &opts)
			groups := []Group{{Orig: snapshots[0], Current: snapshots[1]}}
			b.ReportAllocs()
			for b.Loop() {
				comparison, err := Compare(groups, opts)
				var out bytes.Buffer
				if err == nil {
					err = comparison.Write(&out, nil)
				}
				if err != nil {
					b.Fatalf("Compare/Write: %v", err)
				}
				if out.Len() == 0 {
					b.Fatal("expected at least one diff")
				}
			}
		})
	}
}

type diffCorpus struct {
	left  []Doc
	right []Doc
}

// buildDiffCorpus generates two doc sets of size n. The first
// changeCount entries differ in a single field; the rest are
// identical.
func buildDiffCorpus(n, changeCount int) diffCorpus {
	left := make([]Doc, 0, n)
	right := make([]Doc, 0, n)
	for i := range n {
		name := fmt.Sprintf("cm-%d", i)
		parent := Parent{NamedResource: testutil.HelmReleaseID("default", fmt.Sprintf("hr-%d", i/10))}
		baseValue := fmt.Sprintf("value-%d", i)
		other := baseValue
		if i < changeCount {
			other = fmt.Sprintf("value-%d-changed", i)
		}
		left = append(left, Doc{
			Manifest: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      name,
					"namespace": "default",
				},
				"data": map[string]any{"k": baseValue},
			},
			Parent: parent,
		})
		right = append(right, Doc{
			Manifest: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      name,
					"namespace": "default",
				},
				"data": map[string]any{"k": other},
			},
			Parent: parent,
		})
	}
	return diffCorpus{left: left, right: right}
}
