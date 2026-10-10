package helm

import (
	"errors"
	"testing"

	"helm.sh/helm/v4/pkg/action"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/store"
)

func TestTemplate_CRDsPolicy(t *testing.T) {
	const crd = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names:
    kind: Widget
    plural: widgets
  scope: Namespaced
  versions:
    - name: v1
      served: true
      storage: true
      schema:
        openAPIV3Schema:
          type: object
`
	const cm = `apiVersion: v1
kind: ConfigMap
metadata:
  name: demo-cm
  namespace: default
data:
  greeting: hello
`
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/crds/widgets.yaml", crd)
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", cm)
	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{TemplateCacheBytes: 0})
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	wantAll, err := manifest.SplitDocs([]byte(crd + "---\n" + cm))
	if err != nil {
		t.Fatal(err)
	}
	skipHR := newHR()
	skipHR.CRDsPolicy = "Skip"
	wantRaw, err := cli.Template(t.Context(), skipHR, nil, Options{SkipCRDs: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		policy      string
		skip        bool
		wantInclude bool
	}{
		{"Create skip", "Create", true, false},
		{"Create include", "Create", false, true},
		{"CreateReplace skip", "CreateReplace", true, false},
		{"CreateReplace include", "CreateReplace", false, true},
		{"Skip skip", "Skip", true, false},
		{"Skip include", "Skip", false, false},
		{"unset skip", "", true, false},
		{"unset include", "", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hr := &manifest.HelmRelease{
				Name: "demo", Namespace: "default", CRDsPolicy: tc.policy,
				Chart: manifest.HelmChart{
					Name: "mychart", RepoName: "chart-repo", RepoNamespace: "flux-system", RepoKind: manifest.KindGitRepository,
				},
			}
			opts := Options{SkipCRDs: tc.skip}
			inst, _, err := newInstallAction(new(action.Configuration), hr, opts, chartcommon.DefaultCapabilities.Copy())
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, inst.IncludeCRDs, tc.wantInclude)
			raw, err := cli.Template(t.Context(), hr, nil, opts)
			if err != nil {
				t.Fatal(err)
			}
			wantOutput := wantRaw
			if tc.wantInclude {
				wantOutput = "---\n# Source: mychart/crds/widgets.yaml\n" + crd + "\n" + wantRaw
			}
			assert.Equal(t, raw, wantOutput)
			docs, err := cli.TemplateDocs(t.Context(), hr, nil, opts)
			if err != nil {
				t.Fatal(err)
			}
			want := wantAll
			if !tc.wantInclude {
				want = wantAll[1:]
			}
			assert.Diff(t, docs, want)
			if tc.skip {
				want = wantAll[1:]
			}
			assert.Diff(t, manifest.DropKinds(docs, opts.SkipResourceKinds()), want)
		})
	}
}

func TestTemplate_MalformedCRDsPolicy(t *testing.T) {
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-cm\n"
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/crds/a-valid.yaml", cm)
	testutil.WriteFile(t, dir, "mychart/crds/bad.yaml", "apiVersion: [\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", cm)
	cli, err := NewClientWithOptions(cacheroot.New(t.TempDir()), ClientOptions{TemplateCacheBytes: 0})
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	want, err := manifest.SplitDocs([]byte(cm))
	if err != nil {
		t.Fatal(err)
	}
	_, wantErr := cli.TemplateDocs(t.Context(), newHR(), nil, Options{})
	if !errors.Is(wantErr, manifest.ErrInput) {
		t.Fatalf("got %v, want malformed CRD input error", wantErr)
	}
	for _, tc := range []struct {
		name   string
		policy string
		skip   bool
	}{
		{"Create skip", "Create", true},
		{"Create include", "Create", false},
		{"CreateReplace skip", "CreateReplace", true},
		{"CreateReplace include", "CreateReplace", false},
		{"unset skip", "", true},
		{"unset include", "", false},
		{"Skip skip", "Skip", true},
		{"Skip include", "Skip", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hr := newHR()
			hr.CRDsPolicy = tc.policy
			docs, err := cli.TemplateDocs(t.Context(), hr, nil, Options{SkipCRDs: tc.skip})
			if tc.policy != "Skip" {
				if !errors.Is(err, manifest.ErrInput) {
					t.Fatalf("got %v, want malformed CRD input error", err)
				}
				assert.Equal(t, err.Error(), wantErr.Error())
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, docs, want)
		})
	}
}

func TestTemplate_MalformedSubchartCRDs(t *testing.T) {
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-cm\n"
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/crds/valid.yaml", cm)
	testutil.WriteFile(t, dir, "mychart/charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/charts/child/crds/bad.yaml", cm+"---\napiVersion: [\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", cm)
	cli, err := NewClient(cacheroot.New(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cli.SetSourceResolver(localChartResolver(t, "chart-repo", "flux-system", dir))
	_, wantErr := cli.TemplateDocs(t.Context(), newHR(), nil, Options{})
	if !errors.Is(wantErr, manifest.ErrInput) {
		t.Fatalf("got %v, want malformed CRD input error", wantErr)
	}
	for range 2 {
		_, err := cli.TemplateDocs(t.Context(), newHR(), nil, Options{SkipCRDs: true})
		if !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("got %v, want malformed CRD input error", err)
		}
		assert.Equal(t, err.Error(), wantErr.Error())
	}
}

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

	loaded.Chart.Files = nil
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
	first["image"].(map[string]any)["tag"] = "stomped"
	third, err := cli.mergeChartValuesFiles(loaded, names, false)
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if third["replicaCount"] != float64(3) {
		t.Errorf("cache aliased prior call's map: %+v", third)
	}
	assert.Equal(t, third["image"].(map[string]any)["tag"], "v1")
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

func TestChartValuesCacheKey_DistinctInputs(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second string
	}{
		{
			"name boundaries",
			chartValuesCacheKey("f", []string{"ab"}, false),
			chartValuesCacheKey("f", []string{"a", "b"}, false),
		},
		{
			"empty trailing name",
			chartValuesCacheKey("f", []string{"a", ""}, false),
			chartValuesCacheKey("f", []string{"a"}, false),
		},
		{
			"missing file policy",
			chartValuesCacheKey("f", []string{"a"}, true),
			chartValuesCacheKey("f", []string{"a"}, false),
		},
		{
			"name order",
			chartValuesCacheKey("f", []string{"a", "b"}, false),
			chartValuesCacheKey("f", []string{"b", "a"}, false),
		},
		{
			"chart fingerprint",
			chartValuesCacheKey("f", []string{"a"}, false),
			chartValuesCacheKey("g", []string{"a"}, false),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.first != tc.second, true)
		})
	}
}

func TestOCIChartFingerprint_DistinctInputs(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second string
	}{
		{
			"digest revision boundary",
			ociChartFingerprint("f", &store.SourceArtifact{Digest: "xy", Revision: "z"}, false),
			ociChartFingerprint("f", &store.SourceArtifact{Digest: "x", Revision: "yz"}, false),
		},
		{
			"digest tracking mode",
			ociChartFingerprint("f", &store.SourceArtifact{Digest: "xy", Revision: "z"}, true),
			ociChartFingerprint("f", &store.SourceArtifact{Digest: "xy", Revision: "z"}, false),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.first != tc.second, true)
		})
	}
}
