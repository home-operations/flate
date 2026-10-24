package manifest

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
)

func TestRenderCodec_Fidelity(t *testing.T) {
	t.Parallel()
	docs, err := SplitDocs([]byte(`plain: 2001-01-01
timestamp: !!timestamp 2001-01-01T12:34:56+05:30
large: 9007199254740993
unsigned: 18446744073709551615
signed: -9223372036854775808
boolean: true
text: "true"
binary: !!binary aGVsbG8=
zero: -0.0
nan: .nan
positive: .inf
negative: -.inf
anchor: &value {empty: [], "null": null, map: {}}
alias: *value
merge: {<<: *value, other: kept}
mixed: {1: {nested: []}, other: [[], {deep: []}]}
sequence: [{nested: []}, [[], null]]
---
order: second
`))
	assert.NoError(t, err)
	docs[0]["int64"] = int64(math.MinInt64)
	docs[0]["false"] = false
	payload, err := EncodeRender(docs)
	assert.NoError(t, err)
	equalFloat := cmp.Comparer(func(a, b float64) bool {
		return math.IsNaN(a) && math.IsNaN(b) || a == b && math.Signbit(a) == math.Signbit(b)
	})
	equalTime := cmp.Comparer(func(a, b time.Time) bool {
		_, ao := a.Zone()
		_, bo := b.Zone()
		return a.Equal(b) && ao == bo
	})
	first, err := DecodeRender(payload)
	assert.NoError(t, err)
	second, err := DecodeRender(payload)
	assert.NoError(t, err)
	for _, got := range []([]map[string]any){first, second} {
		if diff := cmp.Diff(docs, got, equalFloat, equalTime); diff != "" {
			t.Fatalf("codec changed constructed values (-want +got):\n%s", diff)
		}
	}
	first[0]["anchor"].(map[string]any)["empty"] = []any{"mutated"}
	first[0]["sequence"].([]any)[0].(map[string]any)["nested"] = "mutated"
	if diff := cmp.Diff(docs, second, equalFloat, equalTime); diff != "" {
		t.Fatalf("decodes share mutable documents (-want +got):\n%s", diff)
	}
}

func TestRenderCodec_EmptyAndErrors(t *testing.T) {
	t.Parallel()
	for _, docs := range [][]map[string]any{nil, {}} {
		payload, err := EncodeRender(docs)
		assert.NoError(t, err)
		if len(payload) == 0 {
			t.Fatal("empty output has no encoded stream")
		}
		got, err := DecodeRender(payload)
		assert.NoError(t, err)
		assert.Equal(t, len(got), 0)
	}
	for _, value := range []any{
		make(chan int), []any{make(chan int)}, map[any]any{make(chan int): nil},
		map[any]any{1: make(chan int)}, time.Date(2001, 1, 1, 0, 0, 0, 0, time.FixedZone("invalid", 86400*30)),
	} {
		if data, err := EncodeRender([]map[string]any{{"unsupported": value}}); data != nil || !errors.Is(err, ErrFlux) {
			t.Fatalf("unsupported encode: %v, %v", data, err)
		}
	}
	payload, err := EncodeRender([]map[string]any{{"first": "valid prefix"}, {"last": "truncated"}})
	assert.NoError(t, err)
	for _, data := range [][]byte{nil, []byte("corrupt"), payload[:len(payload)-1]} {
		if docs, err := DecodeRender(data); docs != nil || !errors.Is(err, ErrFlux) {
			t.Fatalf("corrupt decode: %v, %v", docs, err)
		}
	}
}

func TestRenderCodec_Deterministic(t *testing.T) {
	t.Parallel()
	docs := []map[string]any{{"strings": map[string]any{}, "mixed": map[any]any{}}}
	for i := range 100 {
		docs[0]["strings"].(map[string]any)[fmt.Sprint(i)] = i
		docs[0]["mixed"].(map[any]any)[i] = i
	}
	for _, v := range []string{"first", "second"} {
		docs[0]["mixed"].(map[any]any)[math.NaN()] = v
	}
	first, err := EncodeRender(docs)
	assert.NoError(t, err)
	second, err := EncodeRender(docs)
	assert.NoError(t, err)
	if !bytes.Equal(first, second) {
		t.Fatal("equal documents have different encoded bytes")
	}
	got, err := DecodeRender(first)
	assert.NoError(t, err)
	assert.Equal(t, len(got[0]["mixed"].(map[any]any)), 102)
}

func TestRenderCodec_Corruption(t *testing.T) {
	t.Parallel()
	wrap := func(node []byte) []byte {
		return append([]byte{1, renderStringMap, 1, 1, 'x'}, node...)
	}
	deep := append(bytes.Repeat([]byte{renderSequence, 1}, 10002), renderNull)
	for name, data := range map[string][]byte{
		"document":        {1, renderNull},
		"node":            wrap(nil),
		"trailing":        {0, 0},
		"tag":             wrap([]byte{255}),
		"signed":          wrap([]byte{renderInt, 128}),
		"signed overflow": wrap(append([]byte{renderInt}, bytes.Repeat([]byte{255}, 11)...)),
		"unsigned":        wrap(append([]byte{renderUint64}, bytes.Repeat([]byte{255}, 11)...)),
		"float":           wrap([]byte{renderFloat64, 1}),
		"time":            wrap([]byte{renderTime, 0}),
		"length":          wrap(binary.AppendUvarint([]byte{renderString}, math.MaxUint64)),
		"key":             wrap([]byte{renderAnyMap, 1, renderSequence, 0, renderNull}),
		"depth":           wrap(deep),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if docs, err := DecodeRender(data); docs != nil || !errors.Is(err, ErrFlux) {
				t.Fatalf("corrupt decode: %v, %v", docs, err)
			}
		})
	}
}

func TestRenderCodec_AllocationBound(t *testing.T) {
	const depth, count, padding = 64, 2048, 4096
	for _, tc := range []struct {
		name string
		tag  byte
	}{
		{name: "sequence", tag: renderSequence},
		{name: "string map", tag: renderStringMap},
		{name: "any map", tag: renderAnyMap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte{1, renderStringMap, 1, 1, 'x'}
			for range depth {
				payload = binary.AppendUvarint(append(payload, tc.tag), count)
				switch tc.tag {
				case renderStringMap:
					payload = append(payload, 0)
				case renderAnyMap:
					payload = append(payload, renderNull)
				}
			}
			payload = append(payload, 255)
			payload = append(payload, make([]byte, padding)...)
			_, _ = DecodeRender(payload)
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			docs, err := DecodeRender(payload)
			runtime.ReadMemStats(&after)
			allocated := after.TotalAlloc - before.TotalAlloc
			limit := uint64(256*len(payload) + 64*1024)
			t.Logf("input=%d allocated=%d ratio=%.2f depth=%d count=%d ceiling=%d", len(payload), allocated, float64(allocated)/float64(len(payload)), depth, count, limit)
			if docs != nil || !errors.Is(err, ErrFlux) {
				t.Fatalf("malformed payload: %v, %v", docs, err)
			}
			if allocated > limit {
				t.Errorf("decode allocated %d bytes, exceeding input-proportional ceiling %d", allocated, limit)
			}
		})
	}
}

func TestRenderCodec_FirstError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		prefix []byte
		tail   []byte
		want   string
	}{
		{name: "sequence child", prefix: []byte{renderSequence, 2, 255}, tail: []byte{renderTrue, renderFalse}, want: "unknown render tag 255"},
		{name: "string map value", prefix: []byte{renderStringMap, 2, 0, 255}, tail: []byte{0, renderNull}, want: "unknown render tag 255"},
		{name: "string map key", prefix: []byte{renderStringMap, 1, 100}, tail: []byte{renderTrue, renderFalse}, want: "truncated render value"},
		{name: "any map value", prefix: []byte{renderAnyMap, 2, renderNull, 255}, tail: []byte{renderTrue, renderFalse}, want: "unknown render tag 255"},
		{name: "any map key", prefix: []byte{renderAnyMap, 1, 255}, tail: []byte{renderTrue, renderFalse}, want: "unknown render tag 255"},
		{name: "unhashable key", prefix: []byte{renderAnyMap, 1, renderSequence, 0}, tail: []byte{renderTrue, renderFalse}, want: "invalid render map key"},
		{name: "string length", prefix: []byte{renderString, 100}, tail: []byte{renderTrue, renderFalse}, want: "truncated render value"},
		{name: "time length", prefix: []byte{renderTime, 100}, tail: []byte{renderTrue, renderFalse}, want: "truncated render value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := append(bytes.Clone(tc.prefix), tc.tail...)
			d := renderDecoder{data: data, budget: len(data)}
			if got := d.value(0); got != nil || d.err == nil || d.err.Error() != tc.want {
				t.Fatalf("first error: %v, %v; want %q", got, d.err, tc.want)
			}
			first := d.err
			_ = d.unsigned()
			_ = d.count()
			_ = d.reserve(1)
			_ = d.value(0)
			if !errors.Is(d.err, first) || !bytes.Equal(d.data, tc.tail) {
				t.Fatalf("read after failure: error=%v tail=%v; want error=%v tail=%v", d.err, d.data, first, tc.tail)
			}
		})
	}
	for _, tc := range []struct {
		name  string
		first []byte
		want  string
	}{
		{name: "child", first: []byte{renderStringMap, 1, 0, 255}, want: "unknown render tag 255"},
		{name: "document", first: []byte{renderNull}, want: "render document is not a string-keyed map"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := append(append([]byte{2}, tc.first...), renderInt)
			if docs, err := DecodeRender(data); docs != nil || !errors.Is(err, ErrFlux) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("document failure: %v, %v; want %q", docs, err, tc.want)
			}
		})
	}
}

func TestRenderCodec_DenseAndNested(t *testing.T) {
	t.Parallel()
	stringsMap, anyMap := map[string]any{"": nil}, map[any]any{nil: nil}
	sequence := make([]any, 2048)
	for i := range sequence {
		stringsMap[fmt.Sprint(i)] = nil
		anyMap[i] = nil
	}
	var nested any = sequence
	for range 64 {
		nested = []any{map[string]any{"": map[any]any{nil: nested}}}
	}
	docs := []map[string]any{{"": nil}, {"sequence": nested, "strings": stringsMap, "any": anyMap}, {}}
	payload, err := EncodeRender(docs)
	assert.NoError(t, err)
	got, err := DecodeRender(payload)
	assert.NoError(t, err)
	encoded, err := EncodeRender(got)
	assert.NoError(t, err)
	if !bytes.Equal(encoded, payload) {
		t.Fatal("dense nested containers changed encoded values")
	}
}
