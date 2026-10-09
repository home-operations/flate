package helm

import (
	"testing"

	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/source/cacheroot"
)

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
	loaded := ChartLoadResult{Chart: ch, Fingerprint: chartFingerprint(ch)}
	names := []string{"values-prod.yaml"}

	first, err := cli.mergeChartValuesFiles(loaded, names, false)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first["replicaCount"] != float64(3) {
		t.Fatalf("first call missing replicaCount: %+v", first)
	}

	second, err := cli.mergeChartValuesFiles(loaded, names, false)
	if err != nil {
		t.Fatalf("second call (cache hit expected): %v", err)
	}
	if second["replicaCount"] != float64(3) {
		t.Fatalf("second call missing replicaCount: %+v", second)
	}

	// Caller-mutation safety: mutating the first result must not
	// affect the second (defensive deep-clone on cache read).
	first["replicaCount"] = "stomped"
	third, err := cli.mergeChartValuesFiles(loaded, names, false)
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

	a, err := cli.mergeChartValuesFiles(ChartLoadResult{Chart: chA, Fingerprint: chartFingerprint(chA)}, []string{"values.yaml"}, false)
	if err != nil {
		t.Fatalf("chartA: %v", err)
	}
	b, err := cli.mergeChartValuesFiles(ChartLoadResult{Chart: chB, Fingerprint: chartFingerprint(chB)}, []string{"values.yaml"}, false)
	if err != nil {
		t.Fatalf("chartB: %v", err)
	}
	if a["kind"] != "chartA" || b["kind"] != "chartB" {
		t.Errorf("distinct-chart cache aliased: a=%v b=%v", a, b)
	}
}

func TestMergeChartValuesFiles_OrderedPolicy(t *testing.T) {
	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ch := &chart.Chart{Metadata: &chart.Metadata{Name: "fixture", Version: "1.0.0"}, Files: []*chartcommon.File{
		{Name: "first.yaml", Data: []byte("marker: first\n")},
		{Name: "second.yaml", Data: []byte("marker: second\n")},
	}}
	loaded := ChartLoadResult{Chart: ch, Fingerprint: chartFingerprint(ch)}
	for _, tc := range []struct {
		name   string
		files  []string
		ignore bool
		want   string
	}{
		{"ordered", []string{"first.yaml", "second.yaml"}, false, "second"},
		{"reversed", []string{"second.yaml", "first.yaml"}, false, "first"},
		{"ignore missing", []string{"first.yaml", "missing.yaml"}, true, "first"},
		{"require missing", []string{"first.yaml", "missing.yaml"}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			merged, err := cli.mergeChartValuesFiles(loaded, tc.files, tc.ignore)
			if tc.want == "" {
				if err == nil {
					t.Fatal("required missing file was served from cache")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if merged["marker"] != tc.want {
				t.Fatalf("marker = %v, want %s", merged["marker"], tc.want)
			}
		})
	}
}
