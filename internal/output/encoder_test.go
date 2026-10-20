package output

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"sigs.k8s.io/yaml"
)

func TestEncoder_Documents_Bytes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		format Format
		docs   []map[string]any
		want   string
	}{
		{name: "yaml_order_empty_document", docs: []map[string]any{{"kind": "B"}, {"kind": "A"}, {}}, want: "---\nkind: B\n---\nkind: A\n---\n{}\n"},
		{name: "yaml_nil_document", docs: []map[string]any{nil}, want: "---\nnull\n"},
		{name: "yaml_empty_stream", format: FormatYAML},
		{name: "json_nil", format: FormatJSON, want: "null\n"},
		{name: "json_empty", format: FormatJSON, docs: []map[string]any{}, want: "[]\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Documents(tc.docs); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Images_Bytes(t *testing.T) {
	t.Parallel()
	images := []string{"雪:v2", "alpha:v1"}
	for _, tc := range []struct {
		name   string
		images []string
		format Format
		want   string
	}{
		{name: "plain", images: images, want: "雪:v2\nalpha:v1\n"},
		{name: "yaml", images: images, format: FormatYAML, want: "- 雪:v2\n- alpha:v1\n"},
		{name: "json", images: images, format: FormatJSON, want: "[\n  \"雪:v2\",\n  \"alpha:v1\"\n]\n"},
		{name: "yaml_nil", format: FormatYAML, want: "null\n"},
		{name: "yaml_empty", images: []string{}, format: FormatYAML, want: "[]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Images(tc.images); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Cluster_Bytes(t *testing.T) {
	t.Parallel()
	summary := map[string]any{"kustomizations": 2, "helmReleases": 1}
	for _, tc := range []struct {
		name    string
		summary map[string]any
		format  Format
		want    string
	}{
		{name: "yaml", summary: summary, want: "helmReleases: 1\nkustomizations: 2\n"},
		{name: "json", summary: summary, format: FormatJSON, want: "{\n  \"helmReleases\": 1,\n  \"kustomizations\": 2\n}\n"},
		{name: "yaml_nil", want: "null\n"},
		{name: "yaml_empty", summary: map[string]any{}, want: "{}\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Cluster(tc.summary); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_Resources_Bytes(t *testing.T) {
	t.Parallel()
	cols := []Column{{Header: "NAME", Key: "name"}, {Header: "PATH", Key: "path"}}
	rows := []map[string]string{
		{"name": "éclair", "path": "./é"},
		{"name": "雪", "path": "./雪"},
		{"path": "./missing"},
	}
	docs := []map[string]any{{"kind": "A"}}
	for _, tc := range []struct {
		format Format
		want   string
	}{
		{want: "NAME      PATH\néclair    ./é\n雪         ./雪\n          ./missing\n"},
		{format: FormatName, want: "éclair\n雪\n\n"},
		{format: FormatYAML, want: "---\nkind: A\n"},
		{format: FormatJSON, want: "[\n  {\n    \"kind\": \"A\"\n  }\n]\n"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b, Format: tc.format}).Resources(cols, rows, docs); err != nil {
				t.Fatal(err)
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_JSON_Bytes(t *testing.T) {
	t.Parallel()
	var b bytes.Buffer
	if err := (Encoder{Writer: &b}).JSON(map[string]string{"k": "v", "text": "<>&\"\n雪"}); err != nil {
		t.Fatal(err)
	}
	assert.Diff(t, b.String(), "{\n  \"k\": \"v\",\n  \"text\": \"\\u003c\\u003e\\u0026\\\"\\n雪\"\n}\n")
}

func TestEncoder_Documents_DeterministicKeyOrder(t *testing.T) {
	t.Parallel()
	spec := map[string]any{
		"nested": map[string]any{"z": 1, "a": 2, "m": 3},
		"list":   []any{"c", "b", "a"},
	}
	for i := range 40 {
		spec[fmt.Sprintf("key%02d", i)] = fmt.Sprintf("val-%d", i)
	}
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "x", "namespace": "y"},
		"spec":       spec,
	}
	var first string
	for n := range 2 {
		var b bytes.Buffer
		if err := (Encoder{Writer: &b, Format: FormatYAML}).Documents([]map[string]any{doc}); err != nil {
			t.Fatalf("run %d: %v", n, err)
		}
		if n == 0 {
			first = b.String()
			continue
		}
		if b.String() != first {
			t.Fatalf("run %d: output differs from the first run", n)
		}
	}
	if i0, i39 := strings.Index(first, "key00"), strings.Index(first, "key39"); i0 < 0 || i39 < 0 || i0 > i39 {
		t.Errorf("keys not sorted: key00@%d key39@%d", i0, i39)
	}
}

func TestEncoder_Documents_PreservesEmbeddedJSONStringVerbatim(t *testing.T) {
	t.Parallel()
	embedded := "{\n  \"z\": 1,\n  \"a\": 2,\n  \"panels\": [\n    {\"color\": \"red\"},\n  ]\n}"
	doc := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "dash"},
		"data":       map[string]any{"dashboard.json": embedded},
	}
	var b bytes.Buffer
	e := Encoder{Writer: &b, Format: FormatYAML}
	if err := e.Documents([]map[string]any{doc}); err != nil {
		t.Fatal(err)
	}
	var rt map[string]any
	if err := yaml.Unmarshal([]byte(strings.TrimPrefix(b.String(), "---\n")), &rt); err != nil {
		t.Fatalf("round-trip unmarshal: %v", err)
	}
	got, _ := rt["data"].(map[string]any)["dashboard.json"].(string)
	assert.Diff(t, got, embedded)
}

func TestEncoder_YAML_ScalarCompatibility(t *testing.T) {
	t.Parallel()
	t.Run("decoded_probe", func(t *testing.T) {
		t.Parallel()
		docs, err := manifest.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ConfigMap
metadata:
  name: scalar-probe
data:
  timestamp: !!timestamp 2026-10-20T12:34:56Z
  bigint: 18446744073709551615
  negativeZero: -0.0
  positiveFloat: 1.0
  numericString: "123"
  booleanString: "yes"
  binary: !!binary aGVsbG8=
`))
		assert.NoError(t, err)
		assert.Equal(t, len(docs), 1)
		var b bytes.Buffer
		assert.NoError(t, (Encoder{Writer: &b}).Documents(docs))
		assert.Diff(t, b.String(), "---\napiVersion: v1\ndata:\n  bigint: 18446744073709551615\n  binary: hello\n  booleanString: \"yes\"\n  negativeZero: 0\n  numericString: \"123\"\n  positiveFloat: 1\n  timestamp: \"2026-10-20T12:34:56Z\"\nkind: ConfigMap\nmetadata:\n  name: scalar-probe\n")
	})
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"positive_zero", float64(0), "0\n"},
		{"negative_zero", math.Copysign(0, -1), "0\n"},
		{"float32_negative_zero", float32(math.Copysign(0, -1)), "0\n"},
		{"integral_float", float64(1), "1\n"},
		{"float32_precision", float32(1.2), "1.2\n"},
		{"float32_integer", float32(16777217), "16777216\n"},
		{"small_exponent", float64(1e-7), "1e-07\n"},
		{"small_decimal", float64(1e-6), "1e-06\n"},
		{"large_integer", float64(1e18), "1000000000000000000\n"},
		{"large_decimal", float64(1e20), "1e+20\n"},
		{"large_exponent", float64(1e21), "1e+21\n"},
		{"below_float_precision", float64(9007199254740991), "9007199254740991\n"},
		{"float_precision", float64(9007199254740992), "9007199254740992\n"},
		{"above_float_precision", uint64(9007199254740993), "9007199254740993\n"},
		{"min_int64", int64(math.MinInt64), "-9223372036854775808\n"},
		{"max_int64", int64(math.MaxInt64), "9223372036854775807\n"},
		{"max_uint64", uint64(math.MaxUint64), "18446744073709551615\n"},
		{"max_float64", math.MaxFloat64, "1.7976931348623157e+308\n"},
		{"min_float64", math.SmallestNonzeroFloat64, "5e-324\n"},
		{"json_integral_float", json.Number("1.0"), "1\n"},
		{"json_negative_zero", json.Number("-0"), "0\n"},
		{"json_exponent", json.Number("1e20"), "1e+20\n"},
		{"json_precision", json.Number("9007199254740993"), "9007199254740993\n"},
		{"timestamp", time.Date(2026, 10, 20, 12, 34, 56, 0, time.UTC), "\"2026-10-20T12:34:56Z\"\n"},
		{"timestamp_offset", time.Date(2026, 10, 20, 12, 34, 56, 123000000, time.FixedZone("offset", 3600)), "\"2026-10-20T12:34:56.123+01:00\"\n"},
		{"binary", []byte("hello"), "aGVsbG8=\n"},
		{"unicode", "雪🙂", "\"雪\\U0001F642\"\n"},
		{"invalid_utf8", "a\xffb", "a�b\n"},
		{"multiline", "first\nsecond\n", "|\n  first\n  second\n"},
		{"opaque_json", "{\"z\":1,\"a\":2,}", "'{\"z\":1,\"a\":2,}'\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b}).YAML(tc.value))
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_YAML_ContainerCompatibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"nil", nil, "null\n"},
		{"nil_map", map[string]any(nil), "null\n"},
		{"empty_map", map[string]any{}, "{}\n"},
		{"nil_slice", []any(nil), "null\n"},
		{"empty_slice", []any{}, "[]\n"},
		{"nil_strings", []string(nil), "null\n"},
		{"empty_strings", []string{}, "[]\n"},
		{"nil_typed_map", map[string]string(nil), "null\n"},
		{"empty_typed_map", map[string]string{}, "{}\n"},
		{"nil_interface_map", map[any]any(nil), "null\n"},
		{"empty_interface_map", map[any]any{}, "{}\n"},
		{"nil_bytes", []byte(nil), "null\n"},
		{"empty_bytes", []byte{}, "\"\"\n"},
		{"string_slice", []string{"z", "a"}, "- z\n- a\n"},
		{"typed_source_ref", map[string]any{"sourceRef": map[string]string{"name": "flux-system", "kind": "GitRepository"}, "wait": true}, "sourceRef:\n  kind: GitRepository\n  name: flux-system\nwait: true\n"},
		{"nested", map[string]any{"items": []any{nil, map[string]any{}, []any{}, map[string]any{"z": false, "a": "yes"}}}, "items:\n- null\n- {}\n- []\n- a: \"yes\"\n  z: false\n"},
		{"integer_widths", []any{int(-1), int8(-2), int16(-3), int32(-4), int64(-5), uint(1), uint8(2), uint16(3), uint32(4), uint64(5)}, "- -1\n- -2\n- -3\n- -4\n- -5\n- 1\n- 2\n- 3\n- 4\n- 5\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b}).YAML(tc.value))
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_YAML_DeepSharedContainers(t *testing.T) {
	t.Parallel()
	shared := map[string]any{"value": "ok"}
	var value any = []any{shared, shared, []any(nil), map[any]any(nil)}
	for range 1002 {
		value = []any{value}
	}
	var b bytes.Buffer
	assert.NoError(t, (Encoder{Writer: &b}).YAML(value))
	assert.Equal(t, strings.Count(b.String(), "value: ok"), 2)
	assert.Equal(t, strings.Count(b.String(), "null"), 2)
}

func TestEncoder_YAML_KeyNormalization(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"string_interface_keys", map[any]any{"z": 1, "a": map[any]any{"b": 2}}, "a:\n  b: 2\nz: 1\n"},
		{"invalid_utf8_interface_key", map[any]any{"\xff": 1}, "�: 1\n"},
		{"unsigned_interface_key", map[any]any{uint64(math.MaxUint64): "max"}, "\"18446744073709551615\": max\n"},
		{"invalid_utf8_collision", map[string]any{"\xfe": 2, "\xff": 1}, "�: 1\n"},
		{"typed_map_collision", map[string]string{"\xfe": "earlier", "\xff": "later"}, "�: later\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				var b bytes.Buffer
				assert.NoError(t, (Encoder{Writer: &b}).YAML(tc.value))
				assert.Diff(t, b.String(), tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{"decoded_integer_key", "data: {1: value}\n", "---\ndata:\n  \"1\": value\n"},
		{"decoded_timestamp_key", "data: {!!timestamp '2026-10-20T12:34:56Z': value}\n", "---\ndata:\n  \"2026-10-20T12:34:56Z\": value\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			docs, err := manifest.DecodeDocs(strings.NewReader(tc.input))
			assert.NoError(t, err)
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b}).Documents(docs))
			assert.Diff(t, b.String(), tc.want)
		})
	}
	for _, key := range []string{"true", "1.5", "null"} {
		t.Run("decoded_"+key, func(t *testing.T) {
			t.Parallel()
			docs, err := manifest.DecodeDocs(strings.NewReader("data: {" + key + ": value}\n"))
			assert.NoError(t, err)
			var b bytes.Buffer
			err = (Encoder{Writer: &b}).Documents(docs)
			if _, ok := errors.AsType[*json.UnsupportedValueError](err); !ok {
				t.Fatalf("expected JSON key error, got %T: %v", err, err)
			}
			assert.Diff(t, b.String(), "")
		})
	}
	t.Run("overwritten_invalid_value", func(t *testing.T) {
		t.Parallel()
		var b bytes.Buffer
		value := map[string]any{"\xfe": make(chan int), "\xff": 1}
		if err := (Encoder{Writer: &b}).YAML(value); err == nil {
			t.Fatal("expected error for overwritten invalid value")
		}
		assert.Diff(t, b.String(), "")
	})
}

func TestEncoder_YAML_ParserLimitInputs(t *testing.T) {
	t.Parallel()
	longKey := strings.Repeat("k", 1100)
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"nel_value", map[string]any{"k": "a\u0085b"}, "k: \"a\\Nb\"\n"},
		{"del_value", map[string]any{"k": "a\u007fb"}, "k: \"a\\x7Fb\"\n"},
		{"c1_value", map[string]any{"k": "a\u0099b"}, "k: \"a\\x99b\"\n"},
		{"fffe_value", map[string]any{"k": "a\ufffeb"}, "k: \"a\\uFFFEb\"\n"},
		{"nel_key", map[string]any{"a\u0085b": "v"}, "? \"a\\Nb\"\n: v\n"},
		{"long_key", map[string]any{longKey: "v"}, "? " + longKey + "\n: v\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b}).YAML(tc.value))
			assert.Diff(t, b.String(), tc.want)
		})
	}
}

func TestEncoder_YAML_InvalidValues(t *testing.T) {
	t.Parallel()
	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap
	cyclicInterfaceMap := map[any]any{}
	cyclicInterfaceMap["self"] = cyclicInterfaceMap
	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"nan", math.NaN()},
		{"positive_infinity", math.Inf(1)},
		{"negative_infinity", math.Inf(-1)},
		{"float32_nan", float32(math.NaN())},
		{"channel", make(chan int)},
		{"function", func() {}},
		{"complex", complex(1, 2)},
		{"cyclic_map", cyclicMap},
		{"cyclic_slice", cyclicSlice},
		{"cyclic_interface_map", cyclicInterfaceMap},
		{"invalid_slice_item", []any{make(chan int)}},
		{"invalid_interface_map_item", map[any]any{"bad": make(chan int)}},
		{"timestamp_high_year", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"timestamp_negative_year", time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"timestamp_key_high_year", map[any]any{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC): "value"}},
		{"invalid_number", json.Number("invalid")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			err := (Encoder{Writer: &b}).YAML(tc.value)
			if err == nil {
				t.Fatal("expected encoding error")
			}
			if tc.name == "nan" {
				if !strings.Contains(err.Error(), "error marshaling into JSON:") {
					t.Errorf("missing marshaling context: %v", err)
				}
				if _, ok := errors.AsType[*json.UnsupportedValueError](err); !ok {
					t.Errorf("expected wrapped *json.UnsupportedValueError, got %T: %v", err, err)
				}
			}
			assert.Diff(t, b.String(), "")
		})
	}
}

func TestEncoder_YAML_DoesNotMutateInput(t *testing.T) {
	t.Parallel()
	value := func() map[string]any {
		return map[string]any{
			"nested":    map[string]any{"zero": math.Copysign(0, -1), "invalid": "\xff"},
			"list":      []any{map[string]string{"\xfe": "earlier", "\xff": "later"}, []string{"z", "a"}},
			"interface": map[any]any{"\xfe": 2, "\xff": 1},
			"clock":     time.Date(2026, 10, 20, 12, 34, 56, 0, time.UTC),
			"bytes":     []byte("hello"),
		}
	}
	input, want := value(), value()
	assert.NoError(t, (Encoder{Writer: io.Discard}).YAML(input))
	assert.Diff(t, input, want)
	if !math.Signbit(input["nested"].(map[string]any)["zero"].(float64)) {
		t.Fatal("negative zero was mutated")
	}
}

func TestEncoder_Documents_FailureTiming(t *testing.T) {
	t.Parallel()
	valid := map[string]any{"kind": "A"}
	invalid := map[string]any{"bad": math.NaN()}
	cycle := map[string]any{}
	cycle["self"] = cycle
	for _, tc := range []struct {
		name string
		docs []map[string]any
		want string
	}{
		{"first_encoding", []map[string]any{invalid, valid}, ""},
		{"later_encoding", []map[string]any{valid, invalid, valid}, "---\nkind: A\n"},
		{"later_cycle", []map[string]any{valid, cycle, valid}, "---\nkind: A\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			if err := (Encoder{Writer: &b}).Documents(tc.docs); err == nil {
				t.Fatal("expected encoding error")
			}
			assert.Diff(t, b.String(), tc.want)
		})
	}
	for _, tc := range []struct {
		name  string
		limit int
		want  string
	}{
		{"first_separator", 0, ""},
		{"partial_separator", 2, "--"},
		{"first_body", 4, "---\n"},
		{"partial_body", 8, "---\nkind"},
		{"later_separator", 12, "---\nkind: A\n"},
		{"later_body", 16, "---\nkind: A\n---\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := &prefixWriter{remaining: tc.limit}
			err := (Encoder{Writer: w}).Documents([]map[string]any{valid, valid, invalid})
			if !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("got %v, want writer error", err)
			}
			assert.Diff(t, w.buffer.String(), tc.want)
		})
	}
}

func TestEncoder_YAML_WriterErrors(t *testing.T) {
	t.Parallel()
	e := Encoder{Writer: errorWriter{io.ErrClosedPipe}, Format: FormatYAML}
	for _, tc := range []struct {
		name string
		run  func() error
	}{
		{"yaml", func() error { return e.YAML("value") }},
		{"images", func() error { return e.Images([]string{"image:v1"}) }},
		{"cluster", func() error { return e.Cluster(map[string]any{"kustomizations": 1}) }},
		{"resources", func() error { return e.Resources(nil, nil, []map[string]any{{"kind": "A"}}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.run(); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("got %v, want writer error", err)
			}
		})
	}
}

type prefixWriter struct {
	buffer    bytes.Buffer
	remaining int
}

func (w *prefixWriter) Write(p []byte) (int, error) {
	n := min(len(p), w.remaining)
	w.buffer.Write(p[:n])
	w.remaining -= n
	if n < len(p) {
		return n, io.ErrClosedPipe
	}
	return n, nil
}
