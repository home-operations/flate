package output

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"sigs.k8s.io/yaml"
)

func TestEncoder_Documents_ConstructedValues(t *testing.T) {
	t.Parallel()
	fixture := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: edge}\ndata:\n  implicit: 2026-10-28T12:34:56Z\n  explicit: !!timestamp 2026-10-28T12:34:56.123Z\n  binary: !!binary SGVsbG8=\nspec:\n  nested: {1: one, text: two}\n"
	docs, err := manifest.DecodeDocs(strings.NewReader(fixture), manifest.RequiredDocuments)
	assert.NoError(t, err)
	assert.NoError(t, docs[0].Err)
	want := "---\napiVersion: v1\ndata:\n  binary: Hello\n  explicit: \"2026-10-28T12:34:56.123Z\"\n  implicit: \"2026-10-28T12:34:56Z\"\nkind: ConfigMap\nmetadata:\n  name: edge\nspec:\n  nested:\n    \"1\": one\n    text: two\n"
	assertDocumentPaths(t, docs[0].Document, want)
	t.Run("shared_get_ks", func(t *testing.T) {
		t.Parallel()
		sourceRef := map[string]string{"kind": "GitRepository", "name": "root", "namespace": "flux-system"}
		doc := map[string]any{"kind": "Kustomization", "namespace": "apps", "name": "edge", "path": "./apps", "prune": true, "wait": false, "sourceRef": sourceRef}
		before := maps.Clone(doc)
		before["sourceRef"] = maps.Clone(sourceRef)
		want := "---\nkind: Kustomization\nname: edge\nnamespace: apps\npath: ./apps\nprune: true\nsourceRef:\n  kind: GitRepository\n  name: root\n  namespace: flux-system\nwait: false\n"
		for range 2 {
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b}).Documents([]map[string]any{doc}))
			assert.Equal(t, b.String(), want)
			assert.Diff(t, doc, before)
		}
	})

	for _, tc := range []struct {
		name  string
		value time.Time
		want  string
	}{
		{"utc", time.Date(2026, 10, 28, 12, 34, 56, 0, time.UTC), "2026-10-28T12:34:56Z"},
		{"fraction", time.Date(2026, 10, 28, 12, 34, 56, 123000000, time.UTC), "2026-10-28T12:34:56.123Z"},
		{"offset", time.Date(2026, 10, 28, 12, 34, 56, 0, time.FixedZone("", -5*3600)), "2026-10-28T12:34:56-05:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertDocumentPaths(t, map[string]any{"value": tc.value}, "---\nvalue: \""+tc.want+"\"\n")
			assertDocumentPaths(t, map[string]any{"value": map[any]any{tc.value: "timestamp"}}, "---\nvalue:\n  \""+tc.want+"\": timestamp\n")
		})
	}
	t.Run("invalid_time", func(t *testing.T) {
		t.Parallel()
		var b bytes.Buffer
		err := (Encoder{Writer: &b}).Documents([]map[string]any{{"value": time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)}})
		_, ok := errors.AsType[*json.MarshalerError](err)
		assert.Equal(t, ok, true)
		assert.Equal(t, b.Len(), 0)
	})
	for _, tc := range []struct {
		name string
		key  any
	}{{"boolean", true}, {"float", 1.5}, {"nil", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"value": map[any]any{tc.key: "invalid"}}
			raw, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: doc}})
			assert.NoError(t, err)
			replay, err := manifest.DecodeRender(raw)
			assert.NoError(t, err)
			for _, input := range []map[string]any{doc, replay[0].Document} {
				var b bytes.Buffer
				err := (Encoder{Writer: &b}).Documents([]map[string]any{input})
				_, ok := errors.AsType[*json.UnsupportedValueError](err)
				assert.Equal(t, ok, true)
				assert.Equal(t, b.Len(), 0)
			}
		})
	}
}

func assertDocumentPaths(t *testing.T, doc map[string]any, want string) {
	t.Helper()
	before, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: doc}})
	assert.NoError(t, err)
	replay, err := manifest.DecodeRender(before)
	assert.NoError(t, err)
	for _, path := range []struct {
		name string
		doc  map[string]any
	}{{"cold", doc}, {"replay", replay[0].Document}} {
		t.Run(path.name, func(t *testing.T) {
			t.Parallel()
			original, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: path.doc}})
			assert.NoError(t, err)
			for range 2 {
				var b bytes.Buffer
				assert.NoError(t, (Encoder{Writer: &b}).Documents([]map[string]any{path.doc}))
				assert.Diff(t, b.String(), want)
				after, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: path.doc}})
				assert.NoError(t, err)
				assert.Diff(t, after, original)
			}
		})
	}
}

func TestEncoder_Documents_NormalizedKeyIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		doc  map[string]any
		want string
	}{
		{"root", map[string]any{"\xff": "first", "\xfe": "second", "�": "literal"}, "---\n�: first\n"},
		{"nested", map[string]any{"value": map[string]any{"\xff": "first", "\xfe": "second", "�": "literal"}}, "---\nvalue:\n  �: first\n"},
		{"distinct", map[string]any{"a\xff": "one", "b\xfe": "two"}, "---\na�: one\nb�: two\n"},
		{"natural", map[string]any{"\xff": "first", "\xfe": "second", "�": "literal", "key2": "two", "key02": "zero", "key10": "ten"}, "---\n�: first\nkey2: two\nkey02: zero\nkey10: ten\n"},
		{"interface", map[string]any{"value": map[any]any{"\xff": "first", "\xfe": "second", "�": "literal"}}, "---\nvalue:\n  �: first\n"},
		{"integer_types", map[string]any{"value": map[any]any{int(1): "int", int64(1): "int64", uint64(1): "uint64"}}, "---\nvalue:\n  \"1\": uint64\n"},
		{"timestamp_string", map[string]any{"value": map[any]any{time.Date(2026, 10, 28, 12, 34, 56, 123000000, time.UTC): "timestamp", "2026-10-28T12:34:56.123Z": "string"}}, "---\nvalue:\n  \"2026-10-28T12:34:56.123Z\": string\n"},
	} {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); assertDocumentPaths(t, tc.doc, tc.want) })
	}
	for _, tc := range []struct {
		name  string
		value map[string]string
		want  string
	}{
		{"typed_collision", map[string]string{"\xff": "first", "\xfe": "second", "�": "literal"}, "---\nvalue:\n  �: first\n"},
		{"typed_distinct", map[string]string{"a\xff": "one", "b\xfe": "two"}, "---\nvalue:\n  a�: one\n  b�: two\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before := map[string]string{}
			maps.Copy(before, tc.value)
			for range 2 {
				var b bytes.Buffer
				assert.NoError(t, (Encoder{Writer: &b}).Documents([]map[string]any{{"value": tc.value}}))
				assert.Diff(t, b.String(), tc.want)
				if diff := cmp.Diff(before, tc.value); diff != "" {
					t.Fatal(diff)
				}
			}
		})
	}
	t.Run("integer_string", func(t *testing.T) {
		t.Parallel()
		docs, err := manifest.DecodeDocs(strings.NewReader("kind: ConfigMap\nspec:\n  nested: {01: integer, \"1\": string}\n"), manifest.RequiredDocuments)
		assert.NoError(t, err)
		assertDocumentPaths(t, docs[0].Document, "---\nkind: ConfigMap\nspec:\n  nested:\n    \"1\": string\n")
	})
	t.Run("timestamp_pair", func(t *testing.T) {
		t.Parallel()
		docs, err := manifest.DecodeDocs(strings.NewReader("kind: ConfigMap\nspec:\n  nested:\n    ? !!timestamp 2026-10-28T12:34:56Z\n    : utc\n    ? !!timestamp 2026-10-28T12:34:56+00:00\n    : offset\n"), manifest.RequiredDocuments)
		assert.NoError(t, err)
		assert.NoError(t, docs[0].Err)
		nested := docs[0].Document["spec"].(map[string]any)["nested"].(map[any]any)
		assert.Equal(t, len(nested), 2)
		assertDocumentPaths(t, docs[0].Document, "---\nkind: ConfigMap\nspec:\n  nested:\n    \"2026-10-28T12:34:56Z\": utc\n")
		withString := map[any]any{}
		maps.Copy(withString, nested)
		withString["2026-10-28T12:34:56Z"] = "string"
		assertDocumentPaths(t, map[string]any{"kind": "ConfigMap", "spec": map[string]any{"nested": withString}}, "---\nkind: ConfigMap\nspec:\n  nested:\n    \"2026-10-28T12:34:56Z\": string\n")
	})
	t.Run("invalid_loser", func(t *testing.T) {
		t.Parallel()
		for _, value := range []any{map[string]any{"\xff": "winner", "\xfe": math.NaN()}, map[any]any{"\xff": "winner", "\xfe": math.NaN()}} {
			var b bytes.Buffer
			err := (Encoder{Writer: &b}).Documents([]map[string]any{{"value": value}})
			_, ok := errors.AsType[*json.UnsupportedValueError](err)
			assert.Equal(t, ok, true)
			assert.Equal(t, b.Len(), 0)
		}
	})
}

func TestEncoder_Documents_ScalarOracle(t *testing.T) {
	t.Parallel()
	values := []any{nil, true, false, int(0), int64(math.MinInt64), int64(math.MaxInt64), uint64(math.MaxUint64), float64(2), 1e18, 1e19, 1e20, 1e21, 1e-6, 1e-7, math.Copysign(0, -1), 1.25, math.MaxFloat64, math.SmallestNonzeroFloat64, math.Nextafter(1e19, math.Inf(1)), math.Nextafter(-1e18, math.Inf(-1)), "", "yes", "true", "null", "2026-10-28", "2026-10-28T12:34:56Z", "2026-99-99", "a: b", "a # b", "a:b", "#hello", "- a", "?a", ":a", "---text", "...text", "foo'bar", " foo ", "foo\n", "foo\n\n", "foo\nbar", "  foo\nbar", "\nfoo\n", "\n", "\n\n", "foo \nbar", "foo\n bar ", "foo\r\nbar\r\n", "a\tb", "a\x00b", "a\x7fb", "\ufeff", "a\u0085b", "a\u2028b", "a\u2029b", "a\xffb", "雪 café", "on\nno", strings.Repeat("word ", 40) + "last", strings.Repeat("α ", 42) + "tail", strings.Repeat("α", 4093) + " tail next", strings.Repeat("' ", 40) + "tail", strings.Repeat("a", 79) + "  tail", strings.Repeat("a", 81) + " tail", "0123", "09", ".1_2", ".1__2", ".1e1_2", "1e_+2", "0_x_1", "0x:", "0x?", "+__0b1", "-0xffffffffffffffff", "a\u0085\u0085b", "a \u0085 b", "a\n\t b", "a\t\nb", "a\n\n\n", " a\n", "a\n ", "0b10", "0x10", "0o10", "1:20", "1:60", "1.2e3", "1e3", ".5", "1_000", []any{}, []any(nil), map[string]any{}, map[string]any(nil), map[string]string{}, map[string]string(nil)}
	for i, v := range values {
		t.Run(fmt.Sprintf("value_%03d", i), func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"value": v}
			body, err := yaml.Marshal(doc)
			var got bytes.Buffer
			actualErr := (Encoder{Writer: &got}).Documents([]map[string]any{doc})
			if err != nil {
				if actualErr == nil {
					t.Fatalf("expected baseline rejection: %v", err)
				}
				return
			}
			assert.NoError(t, actualErr)
			assert.Diff(t, got.String(), "---\n"+string(body))
		})
	}
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"explicit_sequence", []any{1, "a"}, "---\n? |-\n  a\n  b\n: - 1\n  - a\n"},
		{"explicit_mapping", map[string]any{"c": 1}, "---\n? |-\n  a\n  b\n: c: 1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"a\nb": tc.value}
			body, err := yaml.Marshal(doc)
			assert.NoError(t, err)
			assert.Diff(t, "---\n"+string(body), tc.want)
			assertDocumentPaths(t, doc, tc.want)
		})
	}
	for _, key := range []string{"", strings.Repeat("k", 128), strings.Repeat("k", 129), strings.Repeat("雪", 43), "multi\nline", "a\rb", "a\u2028b", "a\u2029b", "key2", "key02", "key10", "0000000000000000000000000000000000000002", "a١", "a٢"} {
		t.Run(fmt.Sprintf("key_%x", key), func(t *testing.T) {
			t.Parallel()
			for _, value := range []any{"value", []any{1, "a"}, map[string]any{"c": 1, "d": []any{2}}, map[string]string{"c": "value"}, []any{}, map[string]any{}, nil, []any(nil), map[string]any(nil)} {
				doc := map[string]any{key: value}
				want, err := yaml.Marshal(doc)
				assert.NoError(t, err)
				var got bytes.Buffer
				assert.NoError(t, (Encoder{Writer: &got}).Documents([]map[string]any{doc}))
				assert.Diff(t, got.String(), "---\n"+string(want))
			}
		})
	}
	for _, key := range []string{"\u0085", "\u0085a", "a\u0085", "a\u0085b", "a\u0085\u0085b"} {
		t.Run(fmt.Sprintf("nel_%x", key), func(t *testing.T) {
			t.Parallel()
			for _, doc := range []map[string]any{{key: "x"}, {"nested": map[string]any{key: "x"}}, {"nested": map[string]string{key: "x"}}, {"nested": map[any]any{key: "x"}}} {
				_, err := yaml.Marshal(doc)
				if err == nil {
					t.Fatal("expected baseline to reject raw NEL key")
				}
				for _, docs := range [][]map[string]any{{doc}, {{"kind": "A"}, doc, {"kind": "Later"}}} {
					var got bytes.Buffer
					if err := (Encoder{Writer: &got}).Documents(docs); err == nil {
						t.Fatal("expected raw NEL key rejection")
					}
					want := ""
					if len(docs) > 1 {
						want = "---\nkind: A\n"
					}
					assert.Diff(t, got.String(), want)
				}
			}
			body, err := yaml.Marshal(map[string]any{"value": key})
			assert.NoError(t, err)
			var got bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &got}).Documents([]map[string]any{{"value": key}}))
			assert.Diff(t, got.String(), "---\n"+string(body))
		})
	}
}

func TestEncoder_Documents_TreeOracle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"sequence", []any{"one", map[string]any{"two": []any{map[string]any{"a": 1, "b": 2}, nil, []any{true, false}}}, []any{[]any{"deep"}}, map[string]any{"multiline": "one\ntwo\n"}}},
		{"nested", map[string]any{"list": []any{map[string]any{"a": "one", "b": map[string]any{"empty": []any{}, "list": []any{1, 2}}}}, "empty": map[string]any{}, "nil": map[string]any(nil)}},
		{"typed", map[string]string{"yes": "yes", "2": "two", "10": "ten"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"value": tc.value}
			body, err := yaml.Marshal(doc)
			assert.NoError(t, err)
			for range 2 {
				var got bytes.Buffer
				assert.NoError(t, (Encoder{Writer: &got}).Documents([]map[string]any{doc}))
				assert.Diff(t, got.String(), "---\n"+string(body))
			}
		})
	}
}

func TestEncoder_Documents_PublicationErrors(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write failed")
	for _, tc := range []struct {
		name        string
		docs        []map[string]any
		writerErr   error
		limit       int
		short       bool
		want        string
		unsupported bool
	}{
		{"invalid_first", []map[string]any{{"bad": math.NaN()}}, nil, 1 << 20, false, "", true},
		{"invalid_later", []map[string]any{{"kind": "A"}, {"bad": math.NaN()}, {"kind": "Later"}}, nil, 1 << 20, false, "---\nkind: A\n", true},
		{"flush", []map[string]any{{"kind": "A"}}, writeErr, 0, false, "", false},
		{"joined", []map[string]any{{"kind": "A"}, {"bad": math.NaN()}}, writeErr, 0, false, "", true},
		{"short", []map[string]any{{"kind": "A"}}, nil, 0, true, "", false},
		{"large", []map[string]any{{"text": strings.Repeat("large value\n", 2048)}, {"kind": "Later"}}, writeErr, 4096, false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := &documentFailWriter{limit: tc.limit, err: tc.writerErr, short: tc.short}
			err := (Encoder{Writer: w}).Documents(tc.docs)
			if tc.unsupported {
				_, ok := errors.AsType[*json.UnsupportedValueError](err)
				assert.Equal(t, ok, true)
			}
			if tc.writerErr != nil {
				assert.Equal(t, errors.Is(err, tc.writerErr), true)
			}
			if tc.short {
				assert.Equal(t, errors.Is(err, io.ErrShortWrite), true)
			}
			if tc.name != "large" {
				assert.Equal(t, w.buf.String(), tc.want)
			} else {
				assert.Equal(t, strings.Contains(w.buf.String(), "Later"), false)
			}
			assert.Equal(t, w.afterFailure, 0)
		})
	}
}

type documentFailWriter struct {
	buf          bytes.Buffer
	limit        int
	err          error
	short        bool
	failed       bool
	afterFailure int
}

func (w *documentFailWriter) Write(p []byte) (int, error) {
	if w.failed {
		w.afterFailure++
	}
	n := min(len(p), w.limit)
	w.limit -= n
	_, _ = w.buf.Write(p[:n])
	if n < len(p) {
		w.failed = true
		if w.short {
			return n, nil
		}
		return n, w.err
	}
	return n, nil
}

func TestEncoder_Documents_ScalarRegressions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value any
		want  string
	}{
		{"supplementary_unicode", "first 🚨\nsecond\n", "---\nvalue: \"first \\U0001F6A8\\nsecond\\n\"\n"},
		{"greek_wrap_columns", strings.Repeat("α ", 42) + "tail", "---\nvalue: " + strings.Repeat("α ", 37) + "α\n  " + strings.Repeat("α ", 4) + "tail\n"},
		{"hex_punctuation", "0x?", "---\nvalue: 0x?\n"},
		{"overflow_string", "1e999", "---\nvalue: 1e999\n"},
		{"unicode_break_space", "0\u2028 0", "---\nvalue: \"0\\L 0\"\n"},
		{"unicode_nel", "a\u0085b", "---\nvalue: a b\n"},
		{"unicode_nel_multiple", "a\u0085\u0085b", "---\nvalue: |-\n  a\n  b\n"},
		{"rounded_float_integer", math.Nextafter(1e19, math.Inf(1)), "---\nvalue: 10000000000000002000\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			assert.NoError(t, (Encoder{Writer: &b}).Documents([]map[string]any{{"value": tc.value}}))
			assert.Equal(t, b.String(), tc.want)
		})
	}
}

func FuzzEncoder_Documents_Scalars(f *testing.F) {
	f.Add("a\u0085--- b")
	for _, s := range []string{"plain", "yes", "a\nb\n", "a\u0085\u0085b", "a\u2028b", strings.Repeat("a ", 60), "\x7f", "\xff", "1e999", "0xfffffffffffffffff", "2006-1-2t3:4:5Z", "0\u2028 0", "first 🚨\nsecond\n", strings.Repeat("α ", 42) + "tail", "0x?", "0_x_1", ".1e1_2", "1e_+2"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, value string) {
		doc := map[string]any{"value": value}
		want, baselineErr := yaml.Marshal(doc)
		var b bytes.Buffer
		err := (Encoder{Writer: &b}).Documents([]map[string]any{doc})
		if baselineErr != nil {
			if err == nil {
				t.Fatalf("baseline rejects %q: %v; candidate %q", value, baselineErr, b.String())
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if b.String() != "---\n"+string(want) {
			t.Fatalf("value %q: got %q; want %q", value, b.String(), "---\n"+string(want))
		}
	})
}

func TestEncoder_Documents_SteadyStateAllocations(t *testing.T) {
	doc := map[string]any{"value": map[any]any{int(1): "int", int64(1): "int64", uint64(1): "uint64", time.Date(2026, 10, 28, 12, 34, 56, 123000000, time.UTC): "timestamp", "2026-10-28T12:34:56.123Z": "string", "\xff": "first", "\xfe": "second"}, "strings": map[string]string{"yes": "true", "\xff": "winner", "\xfe": "loser"}, "float": 1.25, "emoji": "first 🚨\nsecond\n", "text": "a\u0085\u0085b", "time": time.Date(2026, 10, 28, 12, 34, 56, 0, time.UTC)}
	d := documentEmitter{writer: io.Discard}
	size, err := d.value(doc, documentPosition{})
	assert.NoError(t, err)
	d.scratch = make([]documentScratch, max(1, (size+31)/32))
	allocations := testing.AllocsPerRun(100, func() {
		d.mode = 1
		if _, err := d.value(doc, documentPosition{}); err != nil {
			t.Fatal(err)
		}
		d.mode = 2
		d.write("---\n")
		if _, err := d.value(doc, documentPosition{}); err != nil {
			t.Fatal(err)
		}
		if d.column != 0 {
			d.write("\n")
		}
		if err := d.flush(); err != nil {
			t.Fatal(err)
		}
	})
	assert.Equal(t, allocations, float64(0))
}

func TestEncoder_Documents_EmptyGroups(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		groups []DocumentGroup
		want   string
	}{
		{"none", nil, ""},
		{"single", []DocumentGroup{{Cluster: "empty"}}, ""},
		{"multiple", []DocumentGroup{{Cluster: "a", Documents: []map[string]any{{}}}, {Cluster: "empty"}, {Cluster: "b", Documents: []map[string]any{nil}}}, "# Cluster: a\n---\n{}\n# Cluster: empty\n# Cluster: b\n---\nnull\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range 2 {
				var b bytes.Buffer
				assert.NoError(t, (Encoder{Writer: &b}).DocumentGroups(tc.groups))
				assert.Equal(t, b.String(), tc.want)
			}
		})
	}
}

func TestEncoder_Documents_FailureImmutable(t *testing.T) {
	t.Parallel()
	doc := map[string]any{"value": map[string]any{"\xff": "winner", "\xfe": math.NaN()}}
	before, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: doc}})
	assert.NoError(t, err)
	for range 2 {
		var b bytes.Buffer
		err := (Encoder{Writer: &b}).Documents([]map[string]any{doc})
		_, ok := errors.AsType[*json.UnsupportedValueError](err)
		assert.Equal(t, ok, true)
		after, err := manifest.EncodeRender([]manifest.DocOccurrence{{Document: doc}})
		assert.NoError(t, err)
		assert.Diff(t, after, before)
	}
}

func TestEncoder_Documents_UnsupportedValue(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		value           any
		unsupportedType bool
	}{
		{"channel", make(chan int), true},
		{"infinity", math.Inf(1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var b bytes.Buffer
			err := (Encoder{Writer: &b}).Documents([]map[string]any{{"kind": "A"}, {"value": tc.value}})
			if tc.unsupportedType {
				_, ok := errors.AsType[*json.UnsupportedTypeError](err)
				assert.Equal(t, ok, true)
			} else {
				_, ok := errors.AsType[*json.UnsupportedValueError](err)
				assert.Equal(t, ok, true)
			}
			assert.Equal(t, b.String(), "---\nkind: A\n")
		})
	}
}

func TestEncoder_Documents_CallAllocations(t *testing.T) {
	docs := []map[string]any{{"kind": "ConfigMap"}, {"kind": "Service"}}
	groups := []DocumentGroup{{Documents: docs}}
	encoder := Encoder{Writer: io.Discard, Format: FormatYAML}
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"documents", func() error { return encoder.Documents(docs) }},
		{"groups", func() error { return encoder.DocumentGroups(groups) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocations := testing.AllocsPerRun(100, func() {
				if err := tc.call(); err != nil {
					t.Fatal(err)
				}
			})
			assert.Equal(t, allocations, float64(1))
		})
	}
}

func TestEncoder_Documents_Cycle(t *testing.T) {
	t.Parallel()
	doc := map[string]any{}
	doc["self"] = doc
	var b bytes.Buffer
	err := (Encoder{Writer: &b}).Documents([]map[string]any{doc})
	_, ok := errors.AsType[*json.UnsupportedValueError](err)
	assert.Equal(t, ok, true)
	assert.Equal(t, b.Len(), 0)
	assert.Equal(t, len(doc), 1)
}

func TestEncoder_Documents_FlowScalarIndicators(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		value  string
		reject bool
	}{
		{"dashes", "a\u0085--- b", true},
		{"dots", "a\u0085... b", true},
		{"indented", "a\u0085 --- b", false},
		{"escaped_tab", "a\u0085---\tb", false},
		{"end", "a\u0085---", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			doc := map[string]any{"value": tc.value}
			want, oracleErr := yaml.Marshal(doc)
			assert.Equal(t, oracleErr != nil, tc.reject)
			var got bytes.Buffer
			err := (Encoder{Writer: &got}).Documents([]map[string]any{doc})
			assert.Equal(t, err != nil, tc.reject)
			if tc.reject {
				assert.Equal(t, got.Len(), 0)
			} else {
				assert.Diff(t, got.String(), "---\n"+string(want))
			}
		})
	}
}
