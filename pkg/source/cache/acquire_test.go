package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestAcquire_GeneratedIdentity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*ArchiveKey)
		same   bool
	}{
		{"equivalent_content", func(key *ArchiveKey) {
			key.Content = map[string]any{"operation": []string{"copy", "merge"}, "namespace": "output/v1", "input": map[string]string{"key": "family", "digest": "generation"}}
		}, true},
		{"operation_order", func(key *ArchiveKey) { key.Content.(map[string]any)["operation"] = []string{"merge", "copy"} }, false},
		{"nested_operation", func(key *ArchiveKey) { key.Content.(map[string]any)["namespace"] = "extract/v1" }, false},
		{"input_coordinate", func(key *ArchiveKey) {
			key.Content.(map[string]any)["input"] = map[string]string{"key": "family", "digest": "next"}
		}, false},
		{"source_policy", func(key *ArchiveKey) { key.Policy = "source" }, false},
		{"extract_policy", func(key *ArchiveKey) { key.Policy = "extract" }, false},
		{"ignore_policy", func(key *ArchiveKey) { key.Ignore = "ignored" }, false},
		{"required_metadata", func(key *ArchiveKey) { key.Required = []string{"upstream"} }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, "", New)
			key := ArchiveKey{Policy: "fixed", Content: map[string]any{"namespace": "output/v1", "input": map[string]string{"key": "family", "digest": "generation"}, "operation": []string{"copy", "merge"}}}
			data := orderedArchive(t, "file", "bytes")
			calls := 0
			fill := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
				calls++
				return producer(data, ocispec.Descriptor{Annotations: map[string]string{"upstream": "retained"}})(ctx, target)
			}
			first, descriptor, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
			if err != nil {
				t.Fatal(err)
			}
			descriptor.Annotations["upstream"] = "caller-mutated"
			tc.change(&key)
			second, descriptor, nextID, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
			if err != nil {
				t.Fatal(err)
			}
			if (id == nextID) != tc.same || id == "" || nextID == "" || calls != map[bool]int{true: 1, false: 2}[tc.same] {
				t.Fatalf("identities %q/%q; calls=%d same=%v", id, nextID, calls, tc.same)
			}
			if descriptor.Annotations["upstream"] != "retained" {
				t.Fatal("caller metadata mutation contaminated cache")
			}
			if tc.name == "source_policy" || tc.name == "extract_policy" || tc.name == "ignore_policy" {
				if AcquisitionIdentity(first) == AcquisitionIdentity(second) {
					t.Fatal("held reader identity omitted archive policy")
				}
			}
		})
	}
	t.Run("unknown_policy", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		called := false
		input, _, id, err := c.Acquire(t.Context(), ArchiveKey{Content: "invalid", Policy: "unknown"}, Policy{}, func(context.Context, ProducerInput) (ProducerResult, error) {
			called = true
			return ProducerResult{}, nil
		})
		if input != nil || id != "" || !errors.Is(err, manifest.ErrInput) || called {
			t.Fatalf("unknown policy = %v, %q, %v; called=%v", input, id, err, called)
		}
	})
	t.Run("marshal_failure", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		called := false
		for range 2 {
			input, _, id, err := c.Acquire(t.Context(), ArchiveKey{Policy: "fixed", Content: make(chan int)}, Policy{MaxAge: Forever}, func(context.Context, ProducerInput) (ProducerResult, error) {
				called = true
				return ProducerResult{}, nil
			})
			if input != nil || id != "" || !errors.Is(err, manifest.ErrInput) || called {
				t.Fatalf("marshal failure = %v, %q, %v; produced=%v", input, id, err, called)
			}
		}
	})
	t.Run("semantic_version", func(t *testing.T) {
		t.Parallel()
		c := cachetest.New(t, "", New)
		data := orderedArchive(t, "file", "bytes")
		key := ArchiveKey{Content: "versioned-output", Policy: "fixed"}
		_, _, seedID, err := c.Acquire(t.Context(), ArchiveKey{Content: "seed", Policy: "fixed"}, Policy{MaxAge: Forever}, producer(data, ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		seed, _ := c.entries.Load(seedID)
		otherVersion := manifest.Fingerprint(acquisitionContent{Archive: struct {
			Content        any
			Ignore, Policy string
			Version        int
		}{key.Content, key.Ignore, key.Policy, 0}})
		c.entries.Store(otherVersion, seed)
		calls := 0
		input, _, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
			calls++
			return producer(data, ocispec.Descriptor{})(ctx, target)
		})
		if err != nil || id == otherVersion || calls != 1 {
			t.Fatalf("versioned acquisition = %q, %v; productions=%d", id, err, calls)
		}
		readerVersion := manifest.Fingerprint(struct {
			Digest         digest.Digest
			Ignore, Policy string
			Version        int
		}{digest.FromBytes(data), "", "fixed", 0})
		if AcquisitionIdentity(input) == "" || AcquisitionIdentity(input) == readerVersion {
			t.Fatal("held reader omitted semantic version")
		}
	})
}

func TestAcquire_GeneratedTreeContent(t *testing.T) {
	t.Parallel()
	archive := func(order []string, execute, empty bool, mode int64, value string) []byte {
		var data bytes.Buffer
		w := tar.NewWriter(&data)
		for _, name := range order {
			header := &tar.Header{Name: name, Mode: mode, Size: int64(len(value)), Uid: 42, Gid: 43, ModTime: time.Unix(100, 0)}
			if execute {
				header.Mode |= 0o111
			}
			if err := w.WriteHeader(header); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(w, value); err != nil {
				t.Fatal(err)
			}
		}
		if empty {
			if err := w.WriteHeader(&tar.Header{Name: "empty", Typeflag: tar.TypeDir, Mode: 0o777}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return data.Bytes()
	}
	base := archive([]string{"a", "b"}, false, false, 0o644, "bytes")
	baseline := cachetest.New(t, "", New)
	_, descriptor, _, err := baseline.Acquire(t.Context(), ArchiveKey{Policy: "fixed", Content: "base"}, Policy{MaxAge: Forever}, producer(base, ocispec.Descriptor{}))
	if err != nil {
		t.Fatal(err)
	}
	want := descriptor.Annotations["flate.tree-digest"]
	for _, tc := range []struct {
		name string
		data []byte
		same bool
	}{
		{"tar_order_and_normalized_permissions", archive([]string{"b", "a"}, false, false, 0o666, "bytes"), true},
		{"executable_mode", archive([]string{"a", "b"}, true, false, 0o644, "bytes"), false},
		{"empty_directory", archive([]string{"a", "b"}, false, true, 0o644, "bytes"), false},
		{"file_content", archive([]string{"a", "b"}, false, false, 0o644, "other"), false},
		{"file_path", archive([]string{"a", "c"}, false, false, 0o644, "bytes"), false},
	} {
		for _, disk := range []bool{false, true} {
			name := tc.name + "/memory"
			if disk {
				name = tc.name + "/disk"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				root := ""
				if disk {
					root = t.TempDir()
				}
				c := cachetest.New(t, root, New)
				calls := 0
				fill := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
					calls++
					return producer(tc.data, ocispec.Descriptor{})(ctx, target)
				}
				key := ArchiveKey{Policy: "fixed", Content: tc.name}
				_, cold, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
				if err != nil {
					t.Fatal(err)
				}
				if (cold.Annotations["flate.tree-digest"] == want) != tc.same {
					t.Fatalf("tree digest %q; base %q same=%v", cold.Annotations["flate.tree-digest"], want, tc.same)
				}
				if disk {
					if err := c.Close(); err != nil {
						t.Fatal(err)
					}
					c = cachetest.New(t, root, New)
				}
				input, warm, warmID, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
				if err != nil || calls != 1 || warmID != id || !maps.Equal(cold.Annotations, warm.Annotations) {
					t.Fatalf("warm result = %+v, %q, %v; calls=%d", warm, warmID, err, calls)
				}
				if tc.name == "empty_directory" {
					if info, err := fs.Stat(input, "empty"); err != nil || !info.IsDir() {
						t.Fatalf("empty directory = %v, %v", info, err)
					}
				}
			})
		}
	}
}

func TestAcquire_GeneratedReadFailure(t *testing.T) {
	t.Parallel()
	for _, warm := range []bool{false, true} {
		name := "cold"
		if warm {
			name = "warm"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := cachetest.New(t, t.TempDir(), New)
			key := ArchiveKey{Content: "generated-read", Policy: "fixed"}
			fill := producer(orderedArchive(t, "file", "bytes"), ocispec.Descriptor{})
			_, _, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
			if err != nil {
				t.Fatal(err)
			}
			value, _ := c.entries.Load(id)
			e := value.(*entry)
			if warm {
				e.touched = time.Now()
			} else {
				e.unloaded, e.fs = true, nil
			}
			if err := c.root.Remove(blobPath(e.stored.Digest)); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				input, _, failedID, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
				if input != nil || failedID != "" || !errors.Is(err, fs.ErrNotExist) || !errors.Is(err, manifest.ErrFlux) {
					t.Fatalf("missing generated backing = %v, %q, %v", input, failedID, err)
				}
			}
		})
	}
}

func TestAcquire_ExtractModesAndTrailing(t *testing.T) {
	t.Parallel()
	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for _, header := range []tar.Header{{Name: "empty", Typeflag: tar.TypeDir, Mode: 0o700}, {Name: "file", Mode: 0o755, Size: 3}, {Name: "file", Mode: 0o600, Size: 3}} {
		if err := w.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if header.Size != 0 {
			if _, err := io.WriteString(w, "new"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data.Write(make([]byte, maxTrailingBytes+1))
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			t.Parallel()
			root := ""
			if disk {
				root = t.TempDir()
			}
			c := cachetest.New(t, root, New)
			key := ArchiveKey{Content: "extract-modes", Policy: "extract"}
			calls := 0
			fill := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
				calls++
				return producer(data.Bytes(), ocispec.Descriptor{})(ctx, target)
			}
			for range 2 {
				input, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
				if err != nil {
					t.Fatal(err)
				}
				info, err := fs.Stat(input, "file")
				if err != nil || info.Mode() != 0o755 {
					t.Fatalf("first-created mode = %v, %v", info, err)
				}
				if info, err := fs.Stat(input, "empty"); err != nil || !info.IsDir() {
					t.Fatalf("empty directory = %v, %v", info, err)
				}
				if got, err := fs.ReadFile(input, "file"); err != nil || string(got) != "new" {
					t.Fatalf("overwritten file = %q, %v", got, err)
				}
			}
			if calls != 1 {
				t.Fatalf("warm extract productions = %d", calls)
			}
			key.Policy = ""
			if input, _, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill); err == nil || input != nil || id != "" {
				t.Fatalf("normal trailing limit = %v, %q, %v", input, id, err)
			}
		})
	}
}

type zeroReader struct{}

func (zeroReader) Read(data []byte) (int, error) {
	clear(data)
	return len(data), nil
}

func TestReadArchive_ExtractDeclaredSize(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		first, second int64
		valid         bool
	}{
		{"at_cap", 0, 100 << 20, true},
		{"over_cap", 0, 100<<20 + 1, false},
		{"duplicates_count", 60 << 20, 60 << 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root, err := os.OpenRoot(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			var readers []io.Reader
			if tc.first != 0 {
				var header bytes.Buffer
				if err := tar.NewWriter(&header).WriteHeader(&tar.Header{Name: "same", Size: tc.first, Mode: 0o644}); err != nil {
					t.Fatal(err)
				}
				readers = append(readers, bytes.NewReader(header.Bytes()), io.LimitReader(zeroReader{}, tc.first))
			}
			var header bytes.Buffer
			if err := tar.NewWriter(&header).WriteHeader(&tar.Header{Name: "same", Size: tc.second, Mode: 0o644}); err != nil {
				t.Fatal(err)
			}
			readers = append(readers, bytes.NewReader(header.Bytes()))
			_, err = readArchive(io.MultiReader(readers...), ArchiveKey{Policy: "extract"}, &tarFS{root: root})
			if tc.valid {
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatalf("at-cap header rejected before its body: %v", err)
				}
			} else if err == nil || errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("declared size cap bypassed: %v", err)
			}
		})
	}
}
