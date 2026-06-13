package helm

import (
	"slices"
	"testing"
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
