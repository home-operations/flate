package manifest

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSHA256Hex(t *testing.T) {
	t.Parallel()
	// Known vector: sha256("") and sha256("abc").
	{
		got := SHA256Hex(nil)
		assert.ErrorIf(
			t,
			got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			"SHA256Hex(nil) = %q",
			got,
		)
	}
	{
		got := SHA256Hex([]byte("abc"))
		assert.ErrorIf(
			t,
			got != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
			"SHA256Hex(abc) = %q",
			got,
		)
	}
	// Stable + deterministic across calls.
	a, b := SHA256Hex([]byte("flate")), SHA256Hex([]byte("flate"))
	{
		assert.ErrorIf(t, a != b, "not deterministic: %q != %q", a, b)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestSHA256Hex_Allocations(t *testing.T) {
	var fingerprint string
	allocs := testing.AllocsPerRun(100, func() { fingerprint = SHA256Hex([]byte("abc")) })
	{
		assert.FatalIf(t, allocs != 1, "SHA256Hex allocated %v times, want 1", allocs)
	}
	{
		assert.FatalIf(
			t,
			fingerprint != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
			"unexpected fingerprint %q",
			fingerprint,
		)
	}
}

func TestFingerprint_JSONEquivalence(t *testing.T) {
	t.Parallel()

	first := map[string]any{"z": 1, "a": "<&>\u2028\u2029"}
	second := make(map[string]any)
	second["a"] = first["a"]
	second["z"] = first["z"]
	for _, tt := range []struct {
		name    string
		payload any
	}{
		{"nil", nil},
		{"nil slice", []string(nil)},
		{"empty slice", []string{}},
		{"nil map", map[string]any(nil)},
		{"empty map", map[string]any{}},
		{"nil bytes", []byte(nil)},
		{"empty bytes", []byte{}},
		{"bytes", []byte("<&>\u2028\u2029")},
		{"map order first", first},
		{"map order second", second},
		{"source identity", struct{ Root, Path, Content string }{"source", "apps", "changed"}},
		{"nested values", map[string]any{"items": []any{true, 1.25, nil, first}, "text": "<&>\u2028\u2029\xff"}},
		{"custom marshaler", json.RawMessage(` { "z": "<>&\u2028", "a": null } `)},
		{"chart content", struct {
			Name  string
			Files [][]byte
		}{"chart<&>\u2028", [][]byte{bytes.Repeat([]byte("template\n"), 16<<10), []byte("values")}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := json.Marshal(tt.payload)
			{
				assert.NoError(t, err)
			}
			{
				got, want := Fingerprint(tt.payload), SHA256Hex(raw)
				assert.FatalIf(t, got != want, "Fingerprint = %q, want %q", got, want)
			}
		})
	}
	{
		assert.FatalIf(
			t,
			Fingerprint(first) != Fingerprint(second),
			"%s",
			"map insertion order changed the fingerprint",
		)
	}
	second["z"] = 2
	{
		assert.FatalIf(
			t,
			Fingerprint(first) == Fingerprint(second),
			"%s",
			"content change did not change the fingerprint",
		)
	}
}

func TestFingerprint_UnsupportedContent(t *testing.T) {
	t.Parallel()
	cycle := make(map[string]any)
	cycle["cycle"] = cycle
	for _, tt := range []struct {
		name    string
		payload any
	}{
		{"channel", make(chan int)},
		{"function", func() {}},
		{"cycle", cycle},
		{"partial output", []any{strings.Repeat("prefix", 16<<10), make(chan int)}},
		{"invalid marshaler", json.RawMessage(`{"incomplete":`)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			{
				got := Fingerprint(tt.payload)
				assert.FatalIf(t, got != "", "Fingerprint = %q, want empty key", got)
			}
		})
	}
}

func TestFingerprint_Concurrent(t *testing.T) {
	t.Parallel()
	const callers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range callers {
		payload := struct {
			Index int
			Files [][]byte
		}{i, [][]byte{bytes.Repeat([]byte{byte(i)}, 8<<10)}}
		raw, err := json.Marshal(payload)
		{
			assert.NoError(t, err)
		}
		want := SHA256Hex(raw)
		wg.Go(func() {
			<-start
			{
				got := Fingerprint(payload)
				assert.ErrorIf(t, got != want, "caller %d: Fingerprint = %q, want %q", i, got, want)
			}
		})
	}
	close(start)
	wg.Wait()
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestFingerprint_ChartAllocatedBytes(t *testing.T) {

	files := make([][]byte, 128)
	for i := range files {
		files[i] = bytes.Repeat([]byte{byte(i)}, 64<<10)
	}
	payload := struct{ Files [][]byte }{files}
	result := testing.Benchmark(func(b *testing.B) {
		b.Helper()
		for b.Loop() {
			Fingerprint(payload)
		}
	})
	{
		got := result.AllocedBytesPerOp()
		assert.FatalIf(t, got >= 1<<20, "Fingerprint allocated %d bytes per chart, want less than 1 MiB", got)
	}
}
