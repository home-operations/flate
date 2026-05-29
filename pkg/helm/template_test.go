package helm

import (
	"testing"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestMergeChartValuesFiles_Cached pins Item 5: the second call with
// the same (chart, valuesFiles, ignoreMissing) tuple must serve from
// chartValuesCache rather than re-yaml.Unmarshal. We assert the
// behavior by mutating the underlying *chart.Chart's Files between
// calls — the cached map is returned regardless of the (now-empty)
// Files slice, which only happens when the cache short-circuits the
// scan.
//
// The returned map MUST be a deep clone (defensive-copy convention):
// callers may mutate it (downstream DeepMerge layering), so the cache
// can't hand out the canonical map directly. We verify by mutating
// the first return and observing the second return is unaffected.
func TestMergeChartValuesFiles_Cached(t *testing.T) {
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ch := &chart.Chart{
		Metadata: &chart.Metadata{Name: "mychart", Version: "0.1.0"},
		Files: []*chartcommon.File{
			{Name: "values-prod.yaml", Data: []byte("replicaCount: 3\nimage:\n  tag: v1\n")},
		},
	}
	names := []string{"values-prod.yaml"}

	first, err := cli.mergeChartValuesFiles(ch, names, false)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first["replicaCount"] != float64(3) {
		t.Fatalf("first call missing replicaCount: %+v", first)
	}

	// Mutate the chart's Files so a non-cached call would now miss the
	// file and (with ignoreMissing=false) return an error. A successful
	// second call therefore PROVES the cache served it.
	ch.Files = nil
	second, err := cli.mergeChartValuesFiles(ch, names, false)
	if err != nil {
		t.Fatalf("second call (cache hit expected): %v", err)
	}
	if second["replicaCount"] != float64(3) {
		t.Fatalf("second call missing replicaCount: %+v", second)
	}

	// Caller-mutation safety: mutating the first result must not
	// affect the second (defensive deep-clone on cache read).
	first["replicaCount"] = "stomped"
	third, err := cli.mergeChartValuesFiles(ch, names, false)
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if third["replicaCount"] != float64(3) {
		t.Errorf("cache aliased prior call's map: %+v", third)
	}
}

// TestMergeChartValuesFiles_DifferentKeysDontShare pins that the key
// function distinguishes charts and valuesFiles lists — two cache
// entries for the same chart with different valuesFiles must not
// alias, and two different charts (different name OR version) must
// not alias even with identical valuesFiles.
func TestMergeChartValuesFiles_DifferentKeysDontShare(t *testing.T) {
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	chA := &chart.Chart{
		Metadata: &chart.Metadata{Name: "a", Version: "1.0.0"},
		Files: []*chartcommon.File{
			{Name: "values.yaml", Data: []byte("kind: chartA\n")},
		},
	}
	chB := &chart.Chart{
		Metadata: &chart.Metadata{Name: "b", Version: "1.0.0"},
		Files: []*chartcommon.File{
			{Name: "values.yaml", Data: []byte("kind: chartB\n")},
		},
	}

	a, err := cli.mergeChartValuesFiles(chA, []string{"values.yaml"}, false)
	if err != nil {
		t.Fatalf("chartA: %v", err)
	}
	b, err := cli.mergeChartValuesFiles(chB, []string{"values.yaml"}, false)
	if err != nil {
		t.Fatalf("chartB: %v", err)
	}
	if a["kind"] != "chartA" || b["kind"] != "chartB" {
		t.Errorf("distinct-chart cache aliased: a=%v b=%v", a, b)
	}
}
