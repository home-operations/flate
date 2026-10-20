package helm

import (
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"

	chart "helm.sh/helm/v4/pkg/chart/v2"
)

// allow is the default top-level allowlist for walker tests (no chart deps).
func allow(keys ...string) map[string]struct{} {
	m := map[string]struct{}{}
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

func vals(keys ...string) map[string]any {
	m := map[string]any{}
	for _, k := range keys {
		m[k] = true
	}
	return m
}

func TestUnreferencedValuePaths(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		src    string
		values map[string]any
		allow  map[string]struct{}
		want   []string
	}{
		{
			name:   "key no template references is flagged",
			src:    "data:\n  g: {{ .Values.greeting }}\n  r: {{ .Values.replicas }}\n",
			values: vals("greeting", "replicas", "oldKey"),
			want:   []string{"oldKey"},
		},
		{
			name:   "all keys referenced",
			src:    "{{ .Values.greeting }}{{ .Values.replicas }}",
			values: vals("greeting", "replicas"),
			want:   nil,
		},
		{
			name:   "quoted index access counts as a reference",
			src:    `{{ index .Values "greeting" }}{{ if hasKey .Values "extra" }}x{{ end }}`,
			values: vals("greeting", "extra", "gone"),
			want:   []string{"gone"},
		},
		{
			name:   "nested field reference marks the top-level key used",
			src:    "{{ .Values.image.repository }}:{{ .Values.image.tag }}",
			values: vals("image", "stale"),
			want:   []string{"stale"},
		},
		{
			name:   "identifier boundary: .Values.foobar does not satisfy foo",
			src:    "{{ .Values.foobar }}",
			values: vals("foo"),
			want:   []string{"foo"},
		},
		{
			name:   "allowlisted global + dependency are never flagged",
			src:    "{{ .Values.greeting }}",
			values: vals("greeting", "global", "mariadb"),
			allow:  allow("global", "mariadb"),
			want:   nil,
		},
		{
			name:   "opaque toYaml .Values bails (reports nothing)",
			src:    "config.yaml: |\n  {{- toYaml .Values | nindent 4 }}",
			values: vals("anything", "more"),
			want:   nil,
		},
		{
			name:   "opaque range over whole .Values bails",
			src:    "{{- range $k, $v := .Values }}{{ $k }}{{- end }}",
			values: vals("anything"),
			want:   nil,
		},
		{
			name:   "opaque bind of whole .Values bails",
			src:    "{{- $v := .Values }}{{ $v.anything }}",
			values: vals("anything"),
			want:   nil,
		},
		{
			name:   "opaque pass of .Values to include bails",
			src:    `{{ include "lib.all" .Values }}`,
			values: vals("anything"),
			want:   nil,
		},
		{
			name:   "passing whole context (.) to include is NOT opaque — .Values.x still resolves",
			src:    `{{ include "lib.all" . }}{{ define "lib.all" }}{{ .Values.greeting }}{{ end }}`,
			values: vals("greeting", "gone"),
			want:   []string{"gone"},
		},
		{
			name:   "multiple unreferenced keys returned sorted",
			src:    "{{ .Values.keep }}",
			values: vals("keep", "zeta", "alpha"),
			want:   []string{"alpha", "zeta"},
		},
		{
			name:   "empty template source reports nothing",
			src:    "",
			values: vals("anything"),
			want:   nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			al := tc.allow
			if al == nil {
				al = allow()
			}
			got := unreferencedValuePaths(tc.src, tc.values, al)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("unreferencedValuePaths = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOpaqueValuesAccess(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"{{ .Values.foo }}":              false, // field selector
		`{{ index .Values "foo" }}`:      false, // literal index
		"{{ toYaml .Values }}":           true,  // whole-map dump
		"{{- range .Values }}{{ end }}":  true,  // range whole map
		"{{ $v := .Values }}":            true,  // bind whole map
		`{{ include "x" .Values }}`:      true,  // passed to a template
		"{{ .Values | toYaml }}":         true,  // piped whole map
		"{{ index .Values $key }}":       true,  // non-literal index
		"no values here":                 false,
		"{{ .Values }}":                  true, // bare at brace
		"{{ .Values.a }}{{ .Values.b }}": false,
	}
	for src, want := range cases {
		if got := opaqueValuesAccess(src); got != want {
			t.Errorf("opaqueValuesAccess(%q) = %v, want %v", src, got, want)
		}
	}
}

func TestValuesKeyReferenced(t *testing.T) {
	t.Parallel()
	src := `{{ .Values.greeting }} {{ index .Values "weird-key" }} {{ .Values.image.tag }}`
	cases := map[string]bool{
		"greeting":  true,  // dotted
		"weird-key": true,  // quoted (not a valid Go field, accessed via index)
		"image":     true,  // dotted prefix of .Values.image.tag
		"greet":     false, // boundary: not .Values.greet<boundary>
		"absent":    false,
	}
	for key, want := range cases {
		if got := valuesKeyReferenced(src, key); got != want {
			t.Errorf("valuesKeyReferenced(%q) = %v, want %v", key, got, want)
		}
	}
}

func TestKnownTopLevelKeys(t *testing.T) {
	t.Parallel()
	sub := &chart.Chart{
		Metadata: &chart.Metadata{
			Name: "operator",
			Dependencies: []*chart.Dependency{
				{Name: "inner-crd", Condition: "innerCrds.enabled"},
			},
		},
	}
	tagsOnlyNested := &chart.Chart{Metadata: &chart.Metadata{Name: "parent"}}
	tagsOnlyNested.AddDependency(&chart.Chart{
		Metadata: &chart.Metadata{
			Name: "child",
			Dependencies: []*chart.Dependency{
				{Name: "grand", Tags: []string{"group"}},
			},
		},
	})
	tagsOnlyNested.AddDependency(&chart.Chart{Metadata: &chart.Metadata{Name: "later"}})
	deep := &chart.Chart{Metadata: &chart.Metadata{
		Name:         "child",
		Dependencies: []*chart.Dependency{{Condition: "innerCrds.enabled"}},
	}}
	deep.AddDependency(&chart.Chart{Metadata: &chart.Metadata{
		Name:         "deeper",
		Dependencies: []*chart.Dependency{{Name: "grand", Condition: "deep.enabled", Tags: []string{"group"}}},
	}})
	missingMetadata := &chart.Chart{}
	missingMetadata.AddDependency(deep, &chart.Chart{})
	noTags := &chart.Chart{Metadata: &chart.Metadata{
		Name: "parent",
		Dependencies: []*chart.Dependency{
			{Name: "database", Alias: "db", Condition: " , crds.enabled, , .ignored, metricsEnabled, extra.enabled, "},
			{},
			nil,
		},
	}}
	noTags.AddDependency(sub)
	ch := &chart.Chart{
		Metadata: &chart.Metadata{
			Name: "parent",
			Dependencies: []*chart.Dependency{
				{Name: "alloy-crd", Condition: "crds.deployAlloyCRD", Tags: []string{"observability"}},
				{Name: "podlogs-crd", Alias: "podlogs", Condition: "crds.deployPodLogsCRD, extra.enabled"},
				{Name: "metrics", Condition: "metricsEnabled"},
				nil,
			},
		},
	}
	ch.AddDependency(sub)

	cases := []struct {
		name string
		ch   *chart.Chart
		want []string
	}{
		{
			name: "nil chart still allows global",
			ch:   nil,
			want: []string{"global"},
		},
		{
			// operator's nested innerCrds.enabled condition resolves at
			// operator.innerCrds.enabled, so innerCrds must NOT leak into
			// the root allowlist.
			name: "names, aliases, root condition segments, and tags",
			ch:   ch,
			want: []string{
				"alloy-crd", "crds", "extra", "global",
				"metrics", "metricsEnabled", "operator", "podlogs",
				"podlogs-crd", "tags",
			},
		},
		{
			name: "tags on a nested dependency still allowlist the shared tags block",
			ch:   tagsOnlyNested,
			want: []string{"child", "global", "later", "tags"},
		},
		{
			name: "missing root metadata with deep tags",
			ch:   missingMetadata,
			want: []string{"child", "global", "tags"},
		},
		{
			name: "no tags with empty condition segments",
			ch:   noTags,
			want: []string{"crds", "database", "db", "extra", "global", "metricsEnabled", "operator"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := knownTopLevelKeys(tc.ch)
			keys := make([]string, 0, len(got))
			for k := range got {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			if !slices.Equal(keys, tc.want) {
				t.Fatalf("knownTopLevelKeys keys = %v, want %v", keys, tc.want)
			}
		})
	}
}

func TestStaleValuePaths_MutableCapture(t *testing.T) {
	t.Parallel()
	input := fstest.MapFS{
		"Chart.yaml":        {Data: []byte("apiVersion: v2\nname: chart\nversion: 1.0.0\n")},
		"templates/cm.yaml": {Data: []byte("{{ .Values.first }}")},
	}
	cli, err := NewClient(cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	st := store.New()
	hr := &manifest.HelmRelease{Chart: manifest.HelmChart{Name: ".", Source: manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "repo"}}}
	st.SetArtifact(hr.ChartSource(), &store.SourceArtifact{FS: input, Key: "misleading"})
	cli.SetSourceResolver(NewStoreSourceResolver(st))
	assert.Diff(t, cli.StaleValuePaths(t.Context(), hr, vals("first", "second")), []string{"second"})
	input["templates/cm.yaml"].Data = []byte("{{ .Values.second }}")
	assert.Diff(t, cli.StaleValuePaths(t.Context(), hr, vals("first", "second")), []string{"first"})
	input["Chart.yaml"].Data = []byte("invalid")
	assert.Equal(t, len(cli.StaleValuePaths(t.Context(), hr, vals("first", "second"))), 0)
}
