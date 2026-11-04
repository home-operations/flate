package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestAcquire_MemoryRetirement(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testAcquireMemoryRetirementCase(t, &backing)
		},
		)
	}
}

func testAcquireMemoryRetirementCase(t *testing.T, backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	files, bytesPerFile := 8, 32<<10
	if os.Getenv("FLATE_E8_SLOW") == "1" {
		files, bytesPerFile = 64, 1<<20
	}
	calls := 0
	fill := func(value string) func(context.Context, ProducerInput) (ProducerResult, error) {
		return testAcquireMemoryRetirementCaseCallback(
			value,
			testAcquireMemoryRetirementCaseCallbackInputs{files: &files, bytesPerFile: &bytesPerFile, calls: &calls},
		)
	}

	check := func(input fs.FS, value byte) {
		t.Helper()
		entries, err := fs.ReadDir(input, ".")
		assert.NoError(t, err)
		assert.Equal(t, len(entries), files)
		for _, name := range []string{"file-000", fmt.Sprintf("file-%03d", files-1)} {
			body, err := fs.ReadFile(input, name)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				len(body) != bytesPerFile || bytes.Count(body, []byte{value}) != bytesPerFile,
				"retained payload %s invalid",
				name,
			)
		}
	}
	var retained [2]uint64
	func() {
		c, err := New(t.Context(), root)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, c.Close()) }()
		key := ArchiveKey{Content: "E8 B11 retirement", Policy: "fixed"}
		first, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill("a"))
		assert.NoError(t, err)
		warm, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill("a"))
		assert.NoError(t, err)
		assert.Equal(t, calls, 1)
		check(warm, 'a')
		next, _, _, err := c.Acquire(t.Context(), key, Policy{}, fill("b"))
		assert.NoError(t, err)
		assert.Equal(t, calls, 2)
		check(first, 'a')
		check(next, 'b')
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		retained[0] = stats.HeapAlloc
		runtime.KeepAlive(first)
		runtime.KeepAlive(warm)
		runtime.KeepAlive(next)
	}()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	retained[1] = stats.HeapAlloc
	func() {
		c, err := New(t.Context(), root)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, c.Close()) }()
		input, _, _, err := c.Acquire(
			t.Context(),
			ArchiveKey{Content: "E8 B11 retirement", Policy: "fixed"},
			Policy{MaxAge: Forever},
			fill("b"),
		)
		assert.NoError(t, err)
		check(input, 'b')
		wantCalls := 3
		if (*backing) == "disk" {
			wantCalls = 2
		}
		assert.Equal(t, calls, wantCalls)
	}()
	t.Logf(
		"B11 retirement backing=%s files=%d bytes_per_file=%d fills=%d held_heap=%d released_closed_gc_heap=%d GOMEMLIMIT=%q GOGC=%q; process-wide samples, not RSS/attributed retention; persistent source reuse is distinct from render-cache build eligibility",
		(*backing),
		files,
		bytesPerFile,
		calls,
		retained[0],
		retained[1],
		os.Getenv("GOMEMLIMIT"),
		os.Getenv("GOGC"),
	)
}

type testAcquireMemoryRetirementCaseCallbackInputs struct {
	files        *int
	bytesPerFile *int
	calls        *int
}

func testAcquireMemoryRetirementCaseCallback(
	value string,
	testCaseInput testAcquireMemoryRetirementCaseCallbackInputs,
) func(context.Context, ProducerInput) (ProducerResult, error) {
	return func(_ context.Context, out ProducerInput) (ProducerResult, error) {
		(*testCaseInput.calls)++
		archive := tar.NewWriter(out)
		for n := range *testCaseInput.files {
			if err := archive.WriteHeader(
				&tar.Header{Name: fmt.Sprintf("file-%03d", n), Size: int64((*testCaseInput.bytesPerFile)), Mode: 0o600},
			); err != nil {
				return ProducerResult{}, err
			}
			if _, err := io.CopyN(
				archive,
				strings.NewReader(strings.Repeat(value, (*testCaseInput.bytesPerFile))),
				int64((*testCaseInput.bytesPerFile)),
			); err != nil {
				return ProducerResult{}, err
			}
		}
		return ProducerResult{}, archive.Close()
	}
}
