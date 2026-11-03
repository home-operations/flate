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
	"testing/iotest"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
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
			testAcquireGeneratedIdentityCase(t, &tc)
		},
		)
	}
	t.Run("unknown_policy", func(t *testing.T) { t.Parallel(); testAcquireGeneratedIdentityUnknownPolicy(t) })
	t.Run("marshal_failure", func(t *testing.T) { t.Parallel(); testAcquireGeneratedIdentityMarshalFailure(t) })
	t.Run("semantic_version", func(t *testing.T) { t.Parallel(); testAcquireGeneratedIdentitySemanticVersion(t) })
}

func TestAcquire_GeneratedTreeContent(t *testing.T) {
	t.Parallel()
	archive := func(order []string, execute, empty bool, mode int64, value string) []byte {
		return testAcquireGeneratedTreeContentCallback(
			order,
			execute,
			empty,
			mode,
			value,
			testAcquireGeneratedTreeContentCallbackInputs{t: &t},
		)
	}

	base := archive([]string{"a", "b"}, false, false, 0o644, "bytes")
	baseline := cachetest.New(t, "", New)
	_, descriptor, _, err := baseline.Acquire(
		t.Context(),
		ArchiveKey{Policy: "fixed", Content: "base"},
		Policy{MaxAge: Forever},
		producer(base, &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

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
				testAcquireGeneratedTreeContentCase(

					t, &want, &tc,
					&disk)
			},
			)
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
			testAcquireGeneratedReadFailureCase(t, &warm)
		},
		)
	}
}

func TestAcquire_ExtractModesAndTrailing(t *testing.T) {
	t.Parallel()
	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for _, header := range []tar.Header{{Name: "empty", Typeflag: tar.TypeDir, Mode: 0o700}, {Name: "file", Mode: 0o755, Size: 3}, {Name: "file", Mode: 0o600, Size: 3}} {
		{
			err := w.WriteHeader(&header)
			assert.NoError(t, err)
		}

		if header.Size != 0 {
			{
				_, err := io.WriteString(w, "new")
				assert.NoError(t, err)
			}
		}
	}
	{
		err := w.Close()
		assert.NoError(t, err)
	}

	data.Write(make([]byte, maxTrailingBytes+1))
	for _, disk := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "disk"}[disk], func(t *testing.T) {
			t.Parallel()
			testAcquireExtractModesAndTrailingCase(
				t,
				&data, &disk,
			)
		},
		)
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
			testReadArchiveExtractDeclaredSizeCase(t, &tc)
		},
		)
	}
}

func TestReadArchive_LargeDeclaredSize(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"", "source", "fixed"} {
		t.Run("policy="+policy, func(t *testing.T) {
			t.Parallel()
			testReadArchiveLargeDeclaredSizeCase(t, &policy)
		},
		)
	}
}

func TestReadArchive_AcceptsLargeSource(t *testing.T) {
	t.Parallel()

	for _, policy := range []string{"", "source", "fixed"} {
		t.Run("policy="+policy, func(t *testing.T) {
			t.Parallel()
			testReadArchiveAcceptsLargeSourceCase(t, &policy)
		},
		)
	}
}

func testAcquireGeneratedIdentityCase(t *testing.T, tc *struct {
	name   string
	change func(*ArchiveKey)
	same   bool
},
) {
	t.Helper()

	c := cachetest.New(t, "", New)
	key := ArchiveKey{
		Policy: "fixed",
		Content: map[string]any{
			"namespace": "output/v1",
			"input":     map[string]string{"key": "family", "digest": "generation"},
			"operation": []string{"copy", "merge"},
		},
	}
	data := orderedArchive(t, "file", "bytes")
	calls := 0
	fill := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
		calls++
		return producer(
			data, &ocispec.Descriptor{Annotations: map[string]string{"upstream": "retained"}},
		)(
			ctx,
			target,
		)
	}
	first, descriptor, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
	assert.NoError(t, err)

	descriptor.Annotations["upstream"] = "caller-mutated"
	tc.
		change(&key)
	second, descriptor, nextID, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
	assert.NoError(t, err)

	if (id == nextID) != tc.same || id == "" || nextID == "" ||
		calls != map[bool]int{true: 1, false: 2}[tc.same] {
		t.Fatalf("identities %q/%q; calls=%d same=%v", id, nextID, calls, tc.same)
	}
	assert.FatalIf(
		t,
		descriptor.Annotations["upstream"] != "retained",
		"%s",
		"caller metadata mutation contaminated cache",
	)

	if tc.name == "source_policy" || tc.name == "extract_policy" || tc.
		name == "ignore_policy" {
		assert.FatalIf(
			t,
			AcquisitionIdentity(first) == AcquisitionIdentity(second),
			"%s",
			"held reader identity omitted archive policy",
		)
	}
}

func testAcquireGeneratedIdentityUnknownPolicy(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, "", New)
	called := false
	input, _, id, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "invalid", Policy: "unknown"},
		Policy{},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			called = true
			return ProducerResult{}, nil
		},
	)
	assert.FatalIf(
		t,
		input != nil || id != "" || !errors.Is(err, manifest.ErrInput) || called,
		"unknown policy = %v, %q, %v; called=%v",
		input,
		id,
		err,
		called,
	)
}

func testAcquireGeneratedIdentityMarshalFailure(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, "", New)
	called := false
	for range 2 {
		input, _, id, err := c.Acquire(
			t.Context(),
			ArchiveKey{Policy: "fixed", Content: make(chan int)},
			Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				called = true
				return ProducerResult{}, nil
			},
		)
		assert.FatalIf(
			t,
			input != nil || id != "" || !errors.Is(err, manifest.ErrInput) || called,
			"marshal failure = %v, %q, %v; produced=%v",
			input,
			id,
			err,
			called,
		)
	}
}

func testAcquireGeneratedIdentitySemanticVersion(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, "", New)
	data := orderedArchive(t, "file", "bytes")
	key := ArchiveKey{Content: "versioned-output", Policy: "fixed"}
	_, _, seedID, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "seed", Policy: "fixed"},
		Policy{MaxAge: Forever},
		producer(data, &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	seed, _ := c.entries.Load(seedID)
	otherVersion := manifest.Fingerprint(acquisitionContent{Archive: struct {
		Content        any
		Ignore, Policy string
		Version        int
	}{key.Content, key.Ignore, key.Policy, 0}})
	c.entries.Store(otherVersion, seed)
	calls := 0
	input, _, id, err := c.Acquire(
		t.Context(),
		key,
		Policy{MaxAge: Forever},
		func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
			calls++
			return producer(data, &ocispec.Descriptor{})(ctx, target)
		},
	)
	assert.FatalIf(
		t,
		err != nil || id == otherVersion || calls != 1,
		"versioned acquisition = %q, %v; productions=%d",
		id,
		err,
		calls,
	)
	readerVersion := manifest.Fingerprint(struct {
		Digest         digest.Digest
		Ignore, Policy string
		Version        int
	}{digest.FromBytes(data), "", "fixed", 0})
	assert.FatalIf(
		t,
		AcquisitionIdentity(input) == "" || AcquisitionIdentity(input) == readerVersion,
		"%s",
		"held reader omitted semantic version",
	)
}

func testAcquireGeneratedTreeContentCase(t *testing.T, want *string,
	tc *struct {
		name string
		data []byte
		same bool
	},
	disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	calls := 0
	fill := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
		calls++
		return producer(tc.data, &ocispec.Descriptor{})(ctx, target)
	}
	key := ArchiveKey{Policy: "fixed", Content: tc.name}
	_, cold, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
	assert.NoError(t, err)

	if (cold.Annotations["flate.tree-digest"] == (*want)) != tc.same {
		t.Fatalf(
			"tree digest %q; base %q same=%v",
			cold.Annotations["flate.tree-digest"],
			(*want), tc.
				same,
		)
	}
	if *disk {
		{
			closeErr := c.Close()
			assert.NoError(t, closeErr)
		}

		c = cachetest.New(t, root, New)
	}
	input, warm, warmID, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
	assert.FatalIf(
		t,
		err != nil || calls != 1 || warmID != id || !maps.Equal(cold.Annotations, warm.Annotations),
		"warm result = %+v, %q, %v; calls=%d",
		warm,
		warmID,
		err,
		calls,
	)
	if tc.name == "empty_directory" {
		{
			info, err := fs.Stat(input, "empty")
			assert.FatalIf(t, err != nil || !info.IsDir(), "empty directory = %v, %v", info, err)
		}
	}
}

func testAcquireGeneratedReadFailureCase(t *testing.T, warm *bool,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	key := ArchiveKey{Content: "generated-read", Policy: "fixed"}
	fill := producer(orderedArchive(t, "file", "bytes"), &ocispec.Descriptor{})
	_, _, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
	assert.NoError(t, err)

	value, _ := c.entries.Load(id)
	e := value.(*entry)
	if *warm {
		e.touched.Store(time.Now().UnixNano())
	} else {
		e.unloaded, e.fs = true, nil
	}
	{
		err := c.root.Remove(blobPath(e.stored.Digest))
		assert.NoError(t, err)
	}

	for range 2 {
		input, _, failedID, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
		assert.FatalIf(t, input != nil || failedID != "" || !errors.Is(err, fs.ErrNotExist) ||
			!errors.Is(err, manifest.ErrFlux), "missing generated backing = %v, %q, %v", input, failedID, err)
	}
}

func testAcquireExtractModesAndTrailingCase(t *testing.T, data *bytes.Buffer,
	disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	key := ArchiveKey{Content: "extract-modes", Policy: "extract"}
	calls := 0
	fill := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
		calls++
		return producer(data.Bytes(), &ocispec.Descriptor{})(ctx, target)
	}
	for range 2 {
		input, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill)
		assert.NoError(t, err)

		info, err := fs.Stat(input, "file")
		assert.FatalIf(t, err != nil || info.Mode() != 0o755, "first-created mode = %v, %v", info, err)
		{
			info, err := fs.Stat(input, "empty")
			assert.FatalIf(t, err != nil || !info.IsDir(), "empty directory = %v, %v", info, err)
		}
		{
			got, err := fs.ReadFile(input, "file")
			assert.FatalIf(t, err != nil || string(got) != "new", "overwritten file = %q, %v", got, err)
		}
	}
	assert.FatalIf(t, calls != 1, "warm extract productions = %d", calls)
	key.Policy = ""
	{
		input, _, id, err := c.Acquire(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			fill,
		)
		assert.FatalIf(t, err == nil || input != nil ||
			id != "", "normal trailing limit = %v, %q, %v", input, id, err)
	}
}

func testReadArchiveExtractDeclaredSizeCase(t *testing.T, tc *struct {
	name   string
	first  int64
	second int64
	valid  bool
},
) {
	t.Helper()

	root, err := os.OpenRoot(t.TempDir())
	assert.NoError(t, err)

	t.Cleanup(func() {
		if closeErr2 := root.Close(); closeErr2 != nil {
			t.Error(closeErr2)
		}
	})
	var readers []io.Reader
	if tc.first != 0 {
		var header bytes.Buffer
		{
			writeHeaderErr := tar.NewWriter(&header).
				WriteHeader(&tar.Header{Name: "same", Size: tc.first, Mode: 0o644})
			assert.NoError(t, writeHeaderErr)
		}

		readers = append(
			readers,
			bytes.NewReader(header.Bytes()),
			io.LimitReader(zeroReader{}, tc.first),
		)
	}
	var header bytes.Buffer
	{
		writeHeaderErr2 := tar.NewWriter(&header).
			WriteHeader(&tar.Header{Name: "same", Size: tc.second, Mode: 0o644})
		assert.NoError(t, writeHeaderErr2)
	}

	readers = append(readers, bytes.NewReader(header.Bytes()))
	_, err = readArchive(io.MultiReader(readers...), ArchiveKey{Policy: "extract"}, &tarFS{root: root})
	if tc.valid {
		assert.FatalIf(
			t,
			!errors.Is(err, io.ErrUnexpectedEOF),
			"at-cap header rejected before its body: %v",
			err,
		)
	} else {
		assert.FatalIf(
			t,
			err == nil || errors.Is(err, io.ErrUnexpectedEOF),
			"declared size cap bypassed: %v",
			err,
		)
	}
}

func testReadArchiveLargeDeclaredSizeCase(t *testing.T, policy *string,
) {
	t.Helper()

	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			cause := errors.New("large archive body")
			for _, tc := range []struct {
				name        string
				cause, want error
			}{
				{"truncated", io.EOF, io.ErrUnexpectedEOF},
				{"fault", cause, cause},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					index := &tarFS{}
					if backing == "disk" {
						root, err := os.OpenRoot(t.TempDir())
						assert.NoError(t, err)

						t.Cleanup(func() { _ = root.Close() })
						index.root = root
					}
					var header bytes.Buffer
					{
						err := tar.NewWriter(&header).
							WriteHeader(&tar.Header{Name: "large", Size: 1 << 33, Mode: 0o644})
						assert.NoError(t, err)
					}

					_, err := readArchive(
						io.MultiReader(bytes.NewReader(header.Bytes()), iotest.ErrReader(tc.cause)),
						ArchiveKey{Policy: (*policy)},
						index,
					)
					if !errors.Is(err, tc.want) {
						t.Fatalf("large declared body: %v, want %v", err, tc.want)
					}
				})
			}
		})
	}
}

func testReadArchiveAcceptsLargeSourceCase(t *testing.T, policy *string,
) {
	t.Helper()
	const size = (1 << 30) + 1
	root, err := os.OpenRoot(t.TempDir())
	assert.NoError(t, err)

	t.Cleanup(func() { _ = root.Close() })
	file, err := root.Create("large.tar")
	assert.NoError(t, err)

	defer file.Close()
	var header bytes.Buffer
	{
		err := tar.NewWriter(&header).
			WriteHeader(&tar.Header{Name: "large", Size: size, Mode: 0o644, Format: tar.FormatUSTAR})
		assert.NoError(t, err)
	}
	{
		_, err := file.Write(header.Bytes())
		assert.NoError(t, err)
	}
	{
		err := file.Truncate(alignNext(int64(header.Len())+size) + 1024)
		assert.NoError(t, err)
	}

	var previous map[string]int64
	for range 2 {
		{
			_, err := file.Seek(0, io.SeekStart)
			assert.NoError(t, err)
		}

		input, err := readArchive(file, ArchiveKey{Policy: (*policy)}, &tarFS{root: root})
		assert.FatalIf(t, err != nil, "valid large source archive: %v", err)
		got := make(map[string]int64)
		for name, node := range input.nodes {
			got[name] = node.Size()
		}
		assert.FatalIf(
			t,
			got["large"] != size || len(got) != 2 || previous != nil && !maps.Equal(got, previous),
			"large archive index changed: %v, previous %v",
			got,
			previous,
		)
		previous = got
	}
}

type testAcquireGeneratedTreeContentCallbackInputs struct {
	t **testing.T
}

func testAcquireGeneratedTreeContentCallback(
	order []string,
	execute, empty bool,
	mode int64,
	value string,
	testCaseInput testAcquireGeneratedTreeContentCallbackInputs,
) []byte {
	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for _, name := range order {
		header := &tar.Header{
			Name:    name,
			Mode:    mode,
			Size:    int64(len(value)),
			Uid:     42,
			Gid:     43,
			ModTime: time.Unix(100, 0),
		}
		if execute {
			header.Mode |= 0o111
		}
		{
			err := w.WriteHeader(header)
			assert.NoError((*testCaseInput.t), err)
		}
		{
			_, err := io.WriteString(w, value)
			assert.NoError((*testCaseInput.t), err)
		}
	}
	if empty {
		{
			err := w.WriteHeader(&tar.Header{Name: "empty", Typeflag: tar.TypeDir, Mode: 0o777})
			assert.NoError((*testCaseInput.t), err)
		}
	}
	{
		err := w.Close()
		assert.NoError((*testCaseInput.t), err)
	}

	return data.Bytes()
}
