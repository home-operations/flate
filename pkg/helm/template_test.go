package helm

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"
	"helm.sh/helm/v4/pkg/action"
	chartcommon "helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/cli-runtime/pkg/resource"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestTemplate_CRDsPolicy(t *testing.T) {
	t.Parallel()
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
	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	wantAll, err := testutil.SplitDocs([]byte(crd + "---\n" + cm))
	if err != nil {
		t.Fatal(err)
	}
	for _, isUpgrade := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			policy  *helmv2.CRDsPolicy
			include bool
		}{
			{"nil", nil, true},
			{"empty", new(helmv2.CRDsPolicy("")), true},
			{"create", new(helmv2.Create), true},
			{"replace", new(helmv2.CreateReplace), true},
			{"skip", new(helmv2.Skip), false},
		} {
			for _, skip := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/upgrade=%t/skip=%t", tc.name, isUpgrade, skip), func(t *testing.T) {
					t.Parallel()
					hr := newHR()
					inactive := helmv2.Skip
					if !tc.include {
						inactive = helmv2.Create
					}
					if isUpgrade {
						hr.Install = &helmv2.Install{CRDs: inactive, DisableOpenAPIValidation: true}
						if tc.policy != nil {
							hr.Upgrade = &helmv2.Upgrade{CRDs: *tc.policy}
						}
					} else {
						hr.Upgrade = &helmv2.Upgrade{CRDs: inactive, DisableOpenAPIValidation: true}
						if tc.policy != nil {
							hr.Install = &helmv2.Install{CRDs: *tc.policy}
						}
					}
					opts := Options{SkipCRDs: skip, IsUpgrade: isUpgrade}
					inst, _ := newInstallAction(new(action.Configuration), hr, opts, chartcommon.DefaultCapabilities.Copy())
					assert.Equal(t, inst.IncludeCRDs, tc.include && !skip)
					assert.Equal(t, inst.DisableOpenAPIValidation, false)
					want := wantAll[1:]
					if tc.include && !skip {
						want = wantAll
					}
					for range 2 {
						docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
						assert.NoError(t, err)
						assert.Diff(t, docs, want)
					}
					fresh := NewClient(cachetest.New(t, "", sourcecache.New))
					cold, _, err := testutil.RenderMaps(fresh.Template(t.Context(), hr, nil, RenderOptions{Options: opts, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
					assert.NoError(t, err)
					assert.Diff(t, cold, want)
				})
			}
		}
	}
}

func TestTemplate_ReleaseName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}', namespace: '{{ .Release.Namespace }}'}\n")
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	for _, tc := range []struct{ name, target, explicit, want string }{
		{"metadata namespace only", "", "", "demo"},
		{"target namespace", "apps", "", "apps-demo"},
		{"override", "apps", "chosen", "chosen"},
		{"long composed", "namespace-with-a-very-long-name-for-release-composition", "", "namespace-with-a-very-long-name-for-rele-e5340b09d048"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hr := newHR()
			hr.TargetNamespace, hr.HelmReleaseSpec.ReleaseName = tc.target, tc.explicit
			for range 2 {
				docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, nil, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
				assert.NoError(t, err)
				assert.Equal(t, len(docs), 1)
				name, _ := manifest.DocMetadata(docs[0])
				assert.Equal(t, name, tc.want)
			}
		})
	}
}

func TestTemplate_SelectedActionPolicy(t *testing.T) {
	t.Parallel()
	for _, isUpgrade := range []bool{false, true} {
		for _, tc := range []struct {
			name                           string
			install, upgrade               helmv2.CRDsPolicy
			installOpenAPI, upgradeOpenAPI bool
		}{
			{"opposite install create", helmv2.Create, helmv2.Skip, false, true},
			{"opposite upgrade create", helmv2.Skip, helmv2.CreateReplace, true, false},
		} {
			t.Run(fmt.Sprintf("%s/upgrade=%t", tc.name, isUpgrade), func(t *testing.T) {
				t.Parallel()
				parsed, _, _, err := manifest.ParseDoc(map[string]any{
					"apiVersion": "helm.toolkit.fluxcd.io/v2", "kind": "HelmRelease",
					"metadata": map[string]any{"name": "demo", "namespace": "default"},
					"spec": map[string]any{
						"chart":   map[string]any{"spec": map[string]any{"chart": "mychart", "sourceRef": map[string]any{"kind": "GitRepository", "name": "chart-repo"}}},
						"install": map[string]any{"crds": string(tc.install), "disableOpenAPIValidation": tc.installOpenAPI},
						"upgrade": map[string]any{"crds": string(tc.upgrade), "disableOpenAPIValidation": tc.upgradeOpenAPI},
					},
				}, manifest.ParseDocOptions{})
				assert.NoError(t, err)
				hr := parsed.(*manifest.HelmRelease)
				policy, openAPI := tc.install, tc.installOpenAPI
				if isUpgrade {
					policy, openAPI = tc.upgrade, tc.upgradeOpenAPI
				}
				inst, _ := newInstallAction(new(action.Configuration), hr, Options{IsUpgrade: isUpgrade}, chartcommon.DefaultCapabilities.Copy())
				assert.Equal(t, inst.IncludeCRDs, policy != helmv2.Skip)
				assert.Equal(t, inst.DisableOpenAPIValidation, openAPI)
			})
		}
	}
}

func TestTemplate_MalformedCRDsPolicy(t *testing.T) {
	t.Parallel()
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-cm\n"
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/crds/a-valid.yaml", cm)
	testutil.WriteFile(t, dir, "mychart/crds/bad.yaml", "apiVersion: [\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", cm)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	want, err := testutil.SplitDocs([]byte(cm))
	if err != nil {
		t.Fatal(err)
	}
	_, _, wantErr := testutil.RenderMaps(cli.Template(t.Context(), newHR(), nil, RenderOptions{Options: Options{}, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
	if !errors.Is(wantErr, manifest.ErrInput) {
		t.Fatalf("got %v, want malformed CRD input error", wantErr)
	}
	for _, isUpgrade := range []bool{false, true} {
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
			t.Run(fmt.Sprintf("%s/upgrade=%t", tc.name, isUpgrade), func(t *testing.T) {
				t.Parallel()
				hr := newHR()
				if isUpgrade {
					hr.Install = &helmv2.Install{CRDs: helmv2.Skip}
					hr.Upgrade = &helmv2.Upgrade{CRDs: helmv2.CRDsPolicy(tc.policy)}
				} else {
					hr.Install = &helmv2.Install{CRDs: helmv2.CRDsPolicy(tc.policy)}
					hr.Upgrade = &helmv2.Upgrade{CRDs: helmv2.Skip}
				}
				docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, nil, RenderOptions{SkipCRDs: tc.skip, IsUpgrade: isUpgrade, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
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
}

func TestTemplate_MalformedSubchartCRDs(t *testing.T) {
	t.Parallel()
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-cm\n"
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/crds/valid.yaml", cm)
	testutil.WriteFile(t, dir, "mychart/charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/charts/child/crds/bad.yaml", cm+"---\napiVersion: [\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", cm)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	_, _, wantErr := testutil.RenderMaps(cli.Template(t.Context(), newHR(), nil, RenderOptions{Options: Options{}, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
	if !errors.Is(wantErr, manifest.ErrInput) {
		t.Fatalf("got %v, want malformed CRD input error", wantErr)
	}
	for range 2 {
		_, _, err := testutil.RenderMaps(cli.Template(t.Context(), newHR(), nil, RenderOptions{SkipCRDs: true, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
		if !errors.Is(err, manifest.ErrInput) {
			t.Fatalf("got %v, want malformed CRD input error", err)
		}
		assert.Equal(t, err.Error(), wantErr.Error())
	}
}

func TestTemplate_MalformedDisabledSubchartCRDs(t *testing.T) {
	t.Parallel()
	const cm = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: demo-cm\n"
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", `apiVersion: v2
name: mychart
version: 0.1.0
dependencies:
  - name: child
    version: 0.1.0
    condition: child.enabled
`)
	testutil.WriteFile(t, dir, "mychart/values.yaml", "child:\n  enabled: false\n")
	testutil.WriteFile(t, dir, "mychart/charts/child/Chart.yaml", "apiVersion: v2\nname: child\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/charts/child/crds/bad.yaml", "apiVersion: [\n")
	testutil.WriteFile(t, dir, "mychart/templates/configmap.yaml", cm)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	want, err := testutil.SplitDocs([]byte(cm))
	if err != nil {
		t.Fatal(err)
	}
	for _, isUpgrade := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			policy  string
			enabled bool
		}{
			{"disabled unset", "", false},
			{"disabled Create", "Create", false},
			{"enabled unset", "", true},
			{"enabled Create", "Create", true},
		} {
			t.Run(fmt.Sprintf("%s/upgrade=%t", tc.name, isUpgrade), func(t *testing.T) {
				t.Parallel()
				hr := newHR()
				if isUpgrade {
					hr.Install = &helmv2.Install{CRDs: helmv2.Skip}
					hr.Upgrade = &helmv2.Upgrade{CRDs: helmv2.CRDsPolicy(tc.policy)}
				} else {
					hr.Install = &helmv2.Install{CRDs: helmv2.CRDsPolicy(tc.policy)}
					hr.Upgrade = &helmv2.Upgrade{CRDs: helmv2.Skip}
				}
				values := map[string]any{"child": map[string]any{"enabled": tc.enabled}}
				included, _, wantErr := testutil.RenderMaps(cli.Template(t.Context(), hr, values, RenderOptions{IsUpgrade: isUpgrade, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
				docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, values, RenderOptions{SkipCRDs: true, IsUpgrade: isUpgrade, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
				if tc.enabled {
					if !errors.Is(wantErr, manifest.ErrInput) || !errors.Is(err, manifest.ErrInput) {
						t.Fatalf("included: %v, omitted: %v; want malformed CRD input errors", wantErr, err)
					}
					assert.Equal(t, err.Error(), wantErr.Error())
					return
				}
				if wantErr != nil || err != nil {
					t.Fatalf("included: %v, omitted: %v; want success", wantErr, err)
				}
				assert.Diff(t, included, want)
				assert.Diff(t, docs, want)
			})
		}
	}
}

func TestMergeChartValuesFiles_Cached(t *testing.T) {
	t.Parallel()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	ch := &chart.Chart{
		Metadata: &chart.Metadata{Name: "mychart", Version: "0.1.0"},
		Values:   map[string]any{"image": map[string]any{"tag": "default", "repository": "canonical"}},
		Files: []*chartcommon.File{
			{Name: "values-prod.yaml", Data: []byte("replicaCount: 3\nimage:\n  tag: v1\n")},
		},
	}
	loaded := chartDescription{chart: ch, fingerprint: chartFixtureDescription(t, ch).fingerprint}
	names := []string{"values.yaml", "values-prod.yaml"}

	first, err := cli.mergeChartValuesFiles(t.Context(), loaded, names, false)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if first["replicaCount"] != float64(3) {
		t.Fatalf("first call missing replicaCount: %+v", first)
	}

	loaded.chart.Files = nil
	second, err := cli.mergeChartValuesFiles(t.Context(), loaded, names, false)
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
	third, err := cli.mergeChartValuesFiles(t.Context(), loaded, names, false)
	if err != nil {
		t.Fatalf("third call: %v", err)
	}
	if third["replicaCount"] != float64(3) {
		t.Errorf("cache aliased prior call's map: %+v", third)
	}
	assert.Equal(t, third["image"].(map[string]any)["tag"], "v1")
	assert.Equal(t, ch.Values["image"].(map[string]any)["tag"], "default")
	assert.Equal(t, third["image"].(map[string]any)["repository"], "canonical")
}

// TestMergeChartValuesFiles_DifferentKeysDontShare pins that the key
// function distinguishes charts and valuesFiles lists — two cache
// entries for the same chart with different valuesFiles must not
// alias, and two different charts (different name OR version) must
// not alias even with identical valuesFiles.
func TestMergeChartValuesFiles_DifferentKeysDontShare(t *testing.T) {
	t.Parallel()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	chA := &chart.Chart{
		Metadata: &chart.Metadata{Name: "a", Version: "1.0.0"},
		Files: []*chartcommon.File{
			{Name: "prod.yaml", Data: []byte("kind: chartA\n")},
		},
	}
	chB := &chart.Chart{
		Metadata: &chart.Metadata{Name: "b", Version: "1.0.0"},
		Files: []*chartcommon.File{
			{Name: "prod.yaml", Data: []byte("kind: chartB\n")},
		},
	}

	a, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: chA, fingerprint: chartFixtureDescription(t, chA).fingerprint}, []string{"prod.yaml"}, false)
	if err != nil {
		t.Fatalf("chartA: %v", err)
	}
	b, err := cli.mergeChartValuesFiles(t.Context(), chartDescription{chart: chB, fingerprint: chartFixtureDescription(t, chB).fingerprint}, []string{"prod.yaml"}, false)
	if err != nil {
		t.Fatalf("chartB: %v", err)
	}
	if a["kind"] != "chartA" || b["kind"] != "chartB" {
		t.Errorf("distinct-chart cache aliased: a=%v b=%v", a, b)
	}
}

func TestMergeChartValuesFiles_UnfingerprintedChartsDontShare(t *testing.T) {
	t.Parallel()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	for _, name := range []string{"A", "B"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ch := &chart.Chart{
				Metadata: &chart.Metadata{Name: name, Version: "1.0.0"},
				Values:   map[string]any{"n": math.NaN()},
				Files: []*chartcommon.File{
					{Name: "prod.yaml", Data: []byte("k: " + name + "\n")},
				},
			}
			loaded := chartDescription{chart: ch, fingerprint: ""}
			assert.Equal(t, chartValuesCacheKey(loaded, []string{"prod.yaml"}, false, ""), "")
			values, err := cli.mergeChartValuesFiles(t.Context(), loaded, []string{"prod.yaml"}, false)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, values["k"], any(name))
		})
	}
}

func TestMergeChartValuesFiles_OrderedPolicy(t *testing.T) {
	t.Parallel()
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	ch := &chart.Chart{Metadata: &chart.Metadata{Name: "fixture", Version: "1.0.0"}, Files: []*chartcommon.File{
		{Name: "first.yaml", Data: []byte("marker: first\n")},
		{Name: "second.yaml", Data: []byte("marker: second\n")},
	}}
	loaded := chartDescription{chart: ch, fingerprint: chartFixtureDescription(t, ch).fingerprint}
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
			t.Parallel()
			merged, err := cli.mergeChartValuesFiles(t.Context(), loaded, tc.files, tc.ignore)
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
	t.Parallel()
	loaded := chartDescription{fingerprint: manifest.Fingerprint([]byte("k: f"))}
	changed := chartDescription{fingerprint: manifest.Fingerprint([]byte("k: g"))}
	source := loaded
	source.valuesFS = fstest.MapFS{}
	source.valuesPin = "pin"
	otherSource := source
	otherSource.valuesPin = "other-pin"
	unpinned := source
	unpinned.valuesPin = ""
	for _, tc := range []struct {
		name          string
		first, second string
	}{
		{
			"name boundaries",
			chartValuesCacheKey(loaded, []string{"ab"}, false, ""),
			chartValuesCacheKey(loaded, []string{"a", "b"}, false, ""),
		},
		{
			"empty trailing name",
			chartValuesCacheKey(loaded, []string{"a", ""}, false, ""),
			chartValuesCacheKey(loaded, []string{"a"}, false, ""),
		},
		{
			"missing file policy",
			chartValuesCacheKey(loaded, []string{"a"}, true, ""),
			chartValuesCacheKey(loaded, []string{"a"}, false, ""),
		},
		{
			"name order",
			chartValuesCacheKey(loaded, []string{"a", "b"}, false, ""),
			chartValuesCacheKey(loaded, []string{"b", "a"}, false, ""),
		},
		{
			"lookup domain",
			chartValuesCacheKey(loaded, []string{"a"}, false, ""),
			chartValuesCacheKey(unpinned, []string{"a"}, false, ""),
		},
		{
			"acquired source pin",
			chartValuesCacheKey(source, []string{"a"}, false, ""),
			chartValuesCacheKey(otherSource, []string{"a"}, false, ""),
		},
		{
			"missing versus empty source file",
			chartValuesCacheKey(unpinned, []string{"a"}, true, manifest.Fingerprint([]chartValuesFile{{Missing: true}})),
			chartValuesCacheKey(unpinned, []string{"a"}, true, manifest.Fingerprint([]chartValuesFile{{}})),
		},
		{
			"mutable source bytes",
			chartValuesCacheKey(unpinned, []string{"a"}, false, manifest.Fingerprint([]chartValuesFile{{Data: []byte("one")}})),
			chartValuesCacheKey(unpinned, []string{"a"}, false, manifest.Fingerprint([]chartValuesFile{{Data: []byte("two")}})),
		},
		{
			"chart fingerprint",
			chartValuesCacheKey(loaded, []string{"a"}, false, ""),
			chartValuesCacheKey(changed, []string{"a"}, false, ""),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.first != tc.second, true)
		})
	}
}

func TestOCIChartContent_DistinctInputs(t *testing.T) {
	t.Parallel()
	initial := ociChartContent{Digest: "xy", Revision: "z"}
	for _, tc := range []struct {
		name string
		next ociChartContent
	}{
		{"digest revision boundary", ociChartContent{Digest: "x", Revision: "yz"}},
		{"digest", ociChartContent{Digest: "x", Revision: "z"}},
		{"revision", ociChartContent{Digest: "xy", Revision: "zz"}},
		{"tracking", ociChartContent{Digest: "xy", Revision: "z", DisableTracking: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch := minimalChart()
			first := chartDescription{chart: ch, fingerprint: chartFixtureDescription(t, ch).fingerprint, oci: &initial}
			second := first
			second.oci = &tc.next
			assert.Equal(t, computeTemplateKey(first, nil, Options{}, minimalHR()) != computeTemplateKey(second, nil, Options{}, minimalHR()), true)
			assert.Equal(t, chartValuesCacheKey(first, []string{"values.yaml"}, false, "") != chartValuesCacheKey(second, []string{"values.yaml"}, false, ""), true)
		})
	}
}

func TestTemplate_KubeVersion(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", `apiVersion: v1
kind: ConfigMap
metadata:
  name: capabilities
data:
  version: {{ .Capabilities.KubeVersion.Version | quote }}
`)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	for _, tc := range []struct {
		name, version, want string
		fail                bool
	}{
		{name: "default", want: chartcommon.DefaultCapabilities.KubeVersion.Version},
		{name: "override", version: "1.30.2", want: "v1.30.2"},
		{name: "invalid", version: "invalid", fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), newHR(), nil, RenderOptions{KubeVersion: tc.version, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
			if tc.fail {
				if err == nil {
					t.Fatal("invalid override accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, docs[0]["data"].(map[string]any)["version"], any(tc.want))
		})
	}
}

func TestTemplate_ActionSchemaPolicy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n")
	testutil.WriteFile(t, dir, "mychart/values.schema.json", `{"type":"object","properties":{"name":{"type":"string","pattern":"^valid$"}},"required":["name"]}`)
	for _, tc := range []struct {
		name                                               string
		install, upgrade, isUpgrade, placeholder, wantSkip bool
	}{
		{name: "install validation disabled, upgrade enabled", install: true, wantSkip: true},
		{name: "install validation enabled, upgrade disabled", upgrade: true},
		{name: "upgrade validation enabled, install disabled", install: true, isUpgrade: true},
		{name: "upgrade validation disabled, install enabled", upgrade: true, isUpgrade: true, wantSkip: true},
		{name: "nil install"},
		{name: "nil upgrade", isUpgrade: true},
		{name: "placeholder install", placeholder: true, wantSkip: true},
		{name: "placeholder upgrade", isUpgrade: true, placeholder: true, wantSkip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cli := NewClient(cachetest.New(t, "", sourcecache.New))

			hr := newHR()
			hr.DisableSchemaValidation = tc.install
			if tc.install {
				hr.Install = &helmv2.Install{DisableSchemaValidation: true}
			}
			if tc.upgrade {
				hr.Upgrade = &helmv2.Upgrade{DisableSchemaValidation: true}
			}
			opts := Options{IsUpgrade: tc.isUpgrade}
			values := map[string]any{"name": "invalid"}
			if tc.placeholder {
				values["name"] = "..PLACEHOLDER_name.."
			}
			original := hr.Clone()
			warm := hr.Clone()
			warm.DisableSchemaValidation = true
			warm.Upgrade = &helmv2.Upgrade{DisableSchemaValidation: true}
			if _, _, err := testutil.RenderMaps(cli.Template(t.Context(), warm, values, RenderOptions{Options: opts, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})); err != nil {
				t.Fatalf("warm cache: %v", err)
			}
			for range 2 {
				_, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, values, RenderOptions{Options: opts, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
				assert.Diff(t, hr.HelmReleaseSpec, original.HelmReleaseSpec)
				assert.Equal(t, hr.DisableSchemaValidation, original.DisableSchemaValidation)
				if tc.wantSkip {
					if err != nil {
						t.Fatal(err)
					}
					continue
				}
				if err == nil {
					t.Fatal("invalid schema accepted after warmed skipped render")
				}
				assert.Equal(t, err.Error(), "helm template default/demo: values don't meet the specifications of the schema(s) in the following chart(s):\nmychart:\n- at '/name': 'invalid' does not match pattern '^valid$'\n")
			}
		})
	}
}

func TestTemplate_SchemaErrorOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n")
	testutil.WriteFile(t, dir, "mychart/values.schema.json", `{
  "type":"object",
  "properties":{
    "alpha":{"type":"string"},
    "middle":{"type":"string"},
    "zeta":{"type":"string"},
    "controllers":{"type":"object","properties":{
      "3ds":{"allOf":[{"properties":{"timeZone":{"type":"string"},"enabled":{"type":"boolean"}}}]},
      "ds":{"allOf":[{"properties":{"timeZone":{"type":"string"},"enabled":{"type":"boolean"}}}]},
      "omega":{"allOf":[{"properties":{"timeZone":{"type":"string"},"enabled":{"type":"boolean"}}}]}
    }}
  }
}`)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	hr := newHR()
	values := map[string]any{
		"alpha": nil, "middle": nil, "zeta": nil,
		"controllers": map[string]any{
			"3ds":   map[string]any{"timeZone": nil, "enabled": nil},
			"ds":    map[string]any{"timeZone": nil, "enabled": nil},
			"omega": map[string]any{"timeZone": nil, "enabled": nil},
		},
	}
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}
	_, _, first := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
	_, _, second := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
	if first == nil || second == nil {
		t.Fatalf("schema failures: %v; %v", first, second)
	}
	const want = `helm template default/demo: values don't meet the specifications of the schema(s) in the following chart(s):
mychart:
- at '/alpha': got null, want string
- at '/controllers': validation failed
  - at '/controllers/3ds': 'allOf' failed
    - at '/controllers/3ds': validation failed
      - at '/controllers/3ds/enabled': got null, want boolean
      - at '/controllers/3ds/timeZone': got null, want string
  - at '/controllers/ds': 'allOf' failed
    - at '/controllers/ds': validation failed
      - at '/controllers/ds/enabled': got null, want boolean
      - at '/controllers/ds/timeZone': got null, want string
  - at '/controllers/omega': 'allOf' failed
    - at '/controllers/omega': validation failed
      - at '/controllers/omega/enabled': got null, want boolean
      - at '/controllers/omega/timeZone': got null, want string
- at '/middle': got null, want string
- at '/zeta': got null, want string
`
	assert.Equal(t, first.Error(), want)
	assert.Equal(t, second.Error(), want)
	assert.Equal(t, first.Error(), second.Error())
}

func TestTemplate_SchemaPropertyOrder(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/cm.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: demo}\n")
	testutil.WriteFile(t, dir, "mychart/values.schema.json", `{"type":"object","additionalProperties":false}`)
	cli := NewClient(cachetest.New(t, "", sourcecache.New))
	hr := newHR()
	values := map[string]any{"zulu": true, "alpha": true, "mu": true, "beta": true, "theta": true, "omega": true}
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}
	_, _, first := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
	_, _, second := testutil.RenderMaps(cli.Template(t.Context(), hr, values, input))
	if first == nil || second == nil {
		t.Fatalf("schema failures: %v; %v", first, second)
	}
	const want = `helm template default/demo: values don't meet the specifications of the schema(s) in the following chart(s):
mychart:
- at '': additional properties 'alpha', 'beta', 'mu', 'omega', 'theta', 'zulu' not allowed
`
	assert.Equal(t, first.Error(), want)
	assert.Equal(t, second.Error(), want)
	assert.Equal(t, first.Error(), second.Error())
}

func TestTemplate_ShowOnlyFraming(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "demo/Chart.yaml", "apiVersion: v2\nname: demo\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "demo/templates/a.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a}")
	testutil.WriteFile(t, dir, "demo/templates/b.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: b}")
	cli := NewClient(cachetest.New(t, "", sourcecache.New))

	const a = "---\n# Source: demo/templates/a.yaml\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: a}\n"
	const b = "---\n# Source: demo/templates/b.yaml\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: b}\n"
	for _, tc := range []struct {
		name  string
		paths []string
		want  string
		names []string
	}{
		{"both", []string{"demo/templates/a.yaml", "demo/templates/b.yaml"}, a + b, []string{"ConfigMap/a", "ConfigMap/b"}},
		{"reversed-paths", []string{"demo/templates/b.yaml", "demo/templates/a.yaml"}, a + b, []string{"ConfigMap/a", "ConfigMap/b"}},
		{"first", []string{"demo/templates/a.yaml"}, a, []string{"ConfigMap/a"}},
		{"second", []string{"demo/templates/b.yaml"}, b, []string{"ConfigMap/b"}},
		{"unmatched", []string{"demo/templates/missing.yaml"}, "", []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var first []map[string]any
			for i := range 2 {
				hr := newHR()
				hr.Chart.Name = "demo"
				out, _, err := testutil.RenderMaps(cli.Template(t.Context(), hr, nil, RenderOptions{ShowOnly: tc.paths, Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
				assert.NoError(t, err)
				want, err := testutil.SplitDocs([]byte(tc.want))
				assert.NoError(t, err)
				assert.Diff(t, out, want)
				assert.Diff(t, templateResources(t, out), tc.names)
				if i == 0 {
					first = out
				} else {
					assert.Diff(t, out, first)
				}
			}
		})
	}
}

func TestTemplate_EmittedLists(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, input string
		wantError   bool
	}{
		{"generic", `apiVersion: v1
kind: List
items:
- {apiVersion: v1, kind: ConfigMap, metadata: {name: a}}
- {apiVersion: v1, kind: Secret, metadata: {name: b}}
`, false},
		{"typed both absent", "apiVersion: v1\nkind: ConfigMapList\nitems: [{metadata: {name: bare}}]\n", false},
		{"typed secret", "apiVersion: v1\nkind: SecretList\nitems: [{metadata: {name: secret}}]\n", false},
		{"missing kind", "apiVersion: v1\nkind: ConfigMapList\nitems: [{apiVersion: v1, metadata: {name: partial}}]\n", true},
		{"missing version", "apiVersion: v1\nkind: ConfigMapList\nitems: [{kind: ConfigMap, metadata: {name: partial}}]\n", true},
		{"nested", `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: List
  items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]
- {apiVersion: v1, kind: ConfigMap, metadata: {name: sibling}}
`, true},
		{"empty", "apiVersion: v1\nkind: ConfigMapList\nitems: []\n", false},
		{"null items", "apiVersion: v1\nkind: ConfigMapList\nitems: null\n", false},
		{"null member", "apiVersion: v1\nkind: ConfigMapList\nitems: [null]\n", false},
		{"scalar member", "apiVersion: v1\nkind: ConfigMapList\nitems: [scalar]\n", true},
		{"object items", "apiVersion: v1\nkind: ConfigMapList\nitems: {metadata: {name: bad}}\n", true},
		{"scalar items", "apiVersion: v1\nkind: ConfigMapList\nitems: scalar\n", true},
		{"custom kind", "apiVersion: example.test/v1\nkind: Bundle\nmetadata: {name: bundle}\nitems: [{apiVersion: v1, kind: ConfigMap, metadata: {name: child}}]\n", false},
		{"scalar AllowList", "apiVersion: example.test/v1\nkind: AllowList\nmetadata: {name: acl}\nitems: [scalar]\n", true},
		{"ordinary AllowList", "apiVersion: example.test/v1\nkind: AllowList\nmetadata: {name: acl}\nspec: {items: [scalar]}\n", false},
		{"constructed scalars", `apiVersion: v1
kind: List
items:
- apiVersion: example.test/v1
  kind: Example
  metadata: {name: nested-keys}
  spec:
    mapping: {true: value}
    string: 'yes'
    date: '2001-01-01'
    number: 9007199254740993
`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			infos, oracleErr := resource.NewLocalBuilder().ContinueOnError().Unstructured().Flatten().Stream(strings.NewReader(tc.input), "").Do().Infos()
			if (oracleErr != nil) != tc.wantError {
				t.Fatalf("upstream error = %v, wantError = %t", oracleErr, tc.wantError)
			}
			want := make([]map[string]any, 0, len(infos))
			for _, info := range infos {
				want = append(want, info.Object.(*unstructured.Unstructured).Object)
			}
			if tc.name == "ordinary AllowList" {
				var err error
				want, err = testutil.SplitDocs([]byte(tc.input))
				assert.NoError(t, err)
			}
			dir := t.TempDir()
			testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
			testutil.WriteFile(t, dir, "mychart/templates/list.yaml", tc.input)
			cli := NewClient(cachetest.New(t, "", sourcecache.New))
			docs, _, err := testutil.RenderMaps(cli.Template(t.Context(), newHR(), nil, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)}))
			if oracleErr != nil {
				if !errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || !strings.Contains(err.Error(), oracleErr.Error()) {
					t.Fatalf("Template error = %v, want upstream: %v", err, oracleErr)
				}
				if docs != nil {
					t.Fatalf("failed producer returned partial output: %#v", docs)
				}
				return
			}
			assert.NoError(t, err)
			assert.Diff(t, docs, want)
		})
	}
}

func TestNormalizeLists_OwnershipAndOrder(t *testing.T) {
	t.Parallel()
	input := []byte(`apiVersion: v1
kind: ConfigMap
metadata: {name: before}
---
apiVersion: v1
kind: ConfigMapList
items: []
---
apiVersion: v1
kind: ConfigMapList
items:
- metadata: {name: expanded}
  data: {key: value}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: after}
`)
	docs, err := manifest.DecodeDocs(bytes.NewReader(input), manifest.RequiredDocuments)
	assert.NoError(t, err)
	snapshot, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	out := normalizeLists(docs)
	var names []string
	for _, doc := range out {
		name, _ := manifest.DocMetadata(doc.Document)
		names = append(names, name)
	}
	assert.Diff(t, names, []string{"before", "expanded", "after"})
	ordinals := make([]int, len(out))
	for i, doc := range out {
		ordinals[i] = doc.Ordinal
	}
	assert.Diff(t, ordinals, []int{0, 2, 3})
	cached, err := manifest.DecodeRender(snapshot)
	assert.NoError(t, err)
	replayed := normalizeLists(cached)
	assert.Diff(t, replayed, out)

	out[1].Document["data"].(map[string]any)["key"] = "changed"
	unchanged, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	assert.Diff(t, unchanged, snapshot)
	out[0].Document["retained"] = true
	assert.Equal(t, docs[0].Document["retained"], any(true))
	out[2].Document["retained"] = true
	assert.Equal(t, docs[3].Document["retained"], any(true))
}

func TestNormalizeLists_NoItems(t *testing.T) {
	docs := testutil.DocumentOccurrences([]map[string]any{{"kind": "AllowList", "spec": map[string]any{"items": []any{"value"}}}, {"kind": "ConfigMap"}})
	out := normalizeLists(docs)
	assert.Equal(t, &out[0] == &docs[0], true)
	allocations := testing.AllocsPerRun(100, func() {
		out = normalizeLists(docs)
	})
	assert.Equal(t, allocations, float64(0))
}

func TestTemplate_PostRenderedLists(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFile(t, dir, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, dir, "mychart/templates/list.yaml", `apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: List
  items: [{apiVersion: v1, kind: ConfigMap, metadata: {name: nested}}]
- {apiVersion: v1, kind: ConfigMap, metadata: {name: sibling}}
`)
	hr := newHR()
	hr.PostRenderers = []helmv2.PostRenderer{{Kustomize: &helmv2.Kustomize{}}}
	out, _, err := NewClient(cachetest.New(t, "", sourcecache.New)).Template(t.Context(), hr, nil, RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", dir)})
	assert.NoError(t, err)
	var names []string
	for _, doc := range out {
		name, _ := manifest.DocMetadata(doc.Document)
		names = append(names, name)
	}
	assert.Diff(t, names, []string{"nested", "sibling"})
}

func TestNormalizeLists_RetainsFailedOutcome(t *testing.T) {
	t.Parallel()
	docs, err := manifest.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ConfigMap
metadata: {name: ordinary}
---
apiVersion: v1
kind: ConfigMapList
items:
- {apiVersion: v1, kind: ConfigMap, metadata: {name: valid}}
- {apiVersion: v1, metadata: {name: invalid}}
`), manifest.RequiredDocuments)
	assert.NoError(t, err)
	before, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	out := normalizeLists(docs)
	failure := manifest.DocumentFailure(out)
	if len(out) != 2 || out[0].Err != nil || out[1].Ordinal != 1 || out[1].Document == nil || !errors.Is(failure, manifest.ErrInput) || !errors.Is(failure, manifest.ErrFlux) {
		t.Fatalf("invalid stream lost its outcome: %#v, %v", out, failure)
	}
	after, err := manifest.EncodeRender(docs)
	assert.NoError(t, err)
	assert.Diff(t, after, before)
}

func TestTemplate_RetainsFailedStream(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "mychart/Chart.yaml", "apiVersion: v2\nname: mychart\nversion: 0.1.0\n")
	testutil.WriteFile(t, root, "mychart/templates/output.yaml", "null\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: invalid}\ndata: {key: first, key: duplicate}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: sibling}\n")
	client := NewClient(cachetest.New(t, "", sourcecache.New))
	input := RenderOptions{Sources: localChartResolver(t, "chart-repo", "flux-system", root)}
	var diagnostic string
	for _, phase := range []string{"cold", "repeat"} {
		docs, receipt, err := client.Template(t.Context(), newHR(), nil, input)
		assert.NoError(t, err)
		if len(docs) != 2 || docs[0].Ordinal != 0 || docs[0].Err == nil || docs[0].Document != nil || docs[1].Ordinal != 1 || manifest.DocIdentity(docs[1].Document).Name != "sibling" || receipt != nil {
			t.Fatalf("%s lost outcomes or cached subset: %+v, %v", phase, docs, receipt)
		}
		if diagnostic == "" {
			diagnostic = manifest.DocumentFailure(docs).Error()
		}
		assert.Equal(t, manifest.DocumentFailure(docs).Error(), diagnostic)
	}
}
