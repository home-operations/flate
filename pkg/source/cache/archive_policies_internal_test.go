package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strconv"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAcquire_GeneratedPolicy(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(strconv.FormatBool(disk), func(t *testing.T) {
			t.Parallel()
			testAcquireGeneratedPolicyCase(t, &disk)
		},
		)
	}
}

func TestAcquire_ExtractPolicy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path string
		kind       byte
	}{
		{"symlink", "entry", tar.TypeSymlink},
		{"hardlink", "entry", tar.TypeLink},
		{"fifo", "entry", tar.TypeFifo},
		{"character_device", "entry", tar.TypeChar},
		{"block_device", "entry", tar.TypeBlock},
		{"traversal", "a/../entry", tar.TypeReg},
		{"absolute", "/entry", tar.TypeReg},
		{"drive", "C:/entry", tar.TypeReg},
		{"backslash", `a\entry`, tar.TypeReg},
	} {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disk=%v", tc.name, disk), func(t *testing.T) {
				t.Parallel()
				testAcquireExtractPolicyCase(t, &tc, &disk)
			},
			)
		}
	}
}

func testAcquireArchivePolicyCase(t *testing.T, disk *bool,
	defaults *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	data := orderedArchive(
		t,
		"dir/drop",
		"discard",
		"dir/keep",
		"first",
		"dir/.sourceignore",
		"!keep\n",
		".sourceignore",
		"dir/*\nempty/*\n",
		"dir/keep",
		"last",
		"empty/file",
		"discard",
		"image.png",
		"image",
		".git/config",
		"git",
	)
	input, _, _, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "source-test-policy", Policy: map[bool]string{true: "source"}[(*defaults)]},
		Policy{MaxAge: time.Hour},
		producer(data, &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	{
		got, err := fs.ReadFile(input, "dir/keep")
		assert.FatalIf(t, err != nil || string(got) != "last", "reincluded duplicate: %q, %v", got, err)
	}
	pin := AcquisitionIdentity(input)
	assert.FatalIf(t, pin == "", "%s", "archive policy view has no acquisition identity")

	for _, policy := range []ArchiveKey{
		{Content: "source-test-policy", Policy: map[bool]string{true: "source"}[!(*defaults)]},
		{Content: "source-test-policy", Policy: map[bool]string{true: "source"}[(*defaults)], Ignore: "image.png\n"},
	} {
		other, _, _, err := c.Acquire(
			t.Context(),
			policy,
			Policy{MaxAge: 0},
			producer(data, &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			AcquisitionIdentity(other) == pin || AcquisitionIdentity(input) != pin,
			"%s",
			"archive policy identity collision",
		)
	}
	for _, name := range []string{"dir/drop", "empty"} {
		{
			_, err := fs.Stat(input, name)
			assert.FatalIf(t, !errors.Is(err, fs.ErrNotExist), "ignored %s: %v", name, err)
		}
	}
	for _, name := range []string{".git/config"} {
		_, err := fs.Stat(input, name)
		assert.FatalIf(
			t, (*defaults) !=
				errors.Is(err, fs.ErrNotExist), "defaults=%v %s: %v", (*defaults), name,
			err,
		)
	}
}

func testAcquireArchiveFailuresAndLinksCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	for _, data := range [][]byte{orderedArchive(t, "../escape", "x"), orderedArchive(t, "file", "x", "file/child", "y"), orderedArchive(t, "dir/file", "x", "dir", "y"), []byte("truncated")} {
		{
			_, _, _, err := c.Acquire(
				t.Context(),
				ArchiveKey{Content: "source-test-invalid"},
				Policy{MaxAge: 0},
				producer(data, &ocispec.Descriptor{}),
			)
			assert.FatalIf(t, err == nil, "%s", "accepted invalid archive")
		}
	}
	for _, defaults := range []bool{false, true} {
		t.Run(strconv.FormatBool(defaults), func(t *testing.T) {
			t.Parallel()
			testAcquireArchiveFailuresAndLinksCaseCase(
				t,
				testAcquireArchiveFailuresAndLinksCaseCaseInputs{disk: &disk, root: &root, c: &c, defaults: &defaults},
			)
		},
		)
	}
}

func testAcquireGeneratedPolicyCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	headers := []tar.Header{
		{Name: "empty/", Mode: 0o700, Typeflag: tar.TypeDir},
		{Name: ".sourceignore", Mode: 0o600, Size: 4},
		{Name: "kept", Mode: 0o711, Size: 4},
		{Name: "parent/plain", Mode: 0o664, Size: 4},
	}
	for index := range headers {
		header := &headers[index]
		header.Uid, header.Gid, header.Uname, header.Gname, header.ModTime = 42, 43, "owner", "group", time.Unix(
			100,
			0,
		)
		header.AccessTime, header.ChangeTime, header.Format = time.Unix(
			101,
			0,
		), time.Unix(
			102,
			0,
		), tar.FormatPAX
		{
			err := writer.WriteHeader(header)
			assert.NoError(t, err)
		}

		if header.Size != 0 {
			{
				_, err := io.WriteString(writer, "kept")
				assert.NoError(t, err)
			}
		}
	}
	testAcquireGeneratedPolicyCaseCompletion(
		t,
		&testAcquireGeneratedPolicyCaseCompletionInputs{writer: &writer, archive: &archive, c: &c},
	)
}

func testAcquireGeneratedPolicyCaseCompletion(t *testing.T, captures *testAcquireGeneratedPolicyCaseCompletionInputs) {
	t.Helper()
	{
		err := (*captures.writer).Close()
		assert.NoError(t, err)
	}

	key := ArchiveKey{Content: "generated-fixed", Policy: "fixed"}
	calls := 0
	produce := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
		calls++
		return producer(captures.archive.Bytes(), &ocispec.Descriptor{})(ctx, target)
	}
	var identity string
	for run := range 2 {
		input, descriptor, id, err := (*captures.c).Acquire(t.Context(), key, Policy{MaxAge: Forever}, produce)
		assert.NoError(t, err)

		assert.FatalIf(
			t,
			calls != 1 || id == "" || descriptor.Annotations["flate.tree-digest"] == "",
			"run %d: calls=%d descriptor=%+v",
			run,
			calls,
			descriptor,
		)
		if run == 0 {
			identity = descriptor.Annotations["flate.tree-digest"]
		} else {
			assert.FatalIf(t, identity != descriptor.Annotations["flate.tree-digest"], "%s", "warm tree digest changed")
		}

		descriptor.Annotations["flate.tree-digest"] = "caller-mutated"
		{
			data, err := fs.ReadFile(input, "kept")
			assert.FatalIf(t, err != nil || string(data) != "kept", "fixed ignore read = %q, %v", data, err)
		}
		for _, entry := range []struct {
			name string
			mode fs.FileMode
		}{{"empty", fs.ModeDir | 0o750}, {"parent", fs.ModeDir | 0o750}, {"parent/plain", 0o600}, {"kept", 0o700}} {
			info, err := fs.Stat(input, entry.name)
			assert.FatalIf(t, err != nil || info.Mode() != entry.mode,
				"normalized %s = %v, %v; want mode %v", entry.name, info, err, entry.mode)
			assert.FatalIf(t, !info.ModTime().IsZero(),
				"normalized %s retained timestamp %v", entry.name, info.ModTime())
			if header, ok := info.Sys().(*tar.Header); ok &&
				(header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "") {
				t.Fatalf("normalized %s retained ownership: %+v", entry.name, header)
			}
			if header, ok := info.Sys().(*tar.Header); ok &&
				(!header.AccessTime.IsZero() || !header.ChangeTime.IsZero()) {
				t.Fatalf("normalized %s retained access/change times: %+v", entry.name, header)
			}
		}
	}
}

func testAcquireExtractPolicyCase(t *testing.T, tc *struct {
	name string
	path string
	kind byte
},
	disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	header := &tar.Header{
		Name:     tc.path,
		Typeflag: tc.kind,
		Linkname: "target",
		Mode:     0o755,
	}
	{
		err := writer.WriteHeader(header)
		assert.NoError(t, err)
	}
	{
		err := writer.Close()
		assert.NoError(t, err)
	}

	input, _, id, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: tc.name, Policy: "extract"},
		Policy{MaxAge: Forever},
		producer(archive.Bytes(), &ocispec.Descriptor{}),
	)
	assert.FatalIf(
		t,
		err == nil || input != nil || id != "" || !errors.Is(err, manifest.ErrFlux),
		"invalid extraction = %q, %v",
		id,
		err,
	)
	input, _, id, err = c.Acquire(
		t.Context(),
		ArchiveKey{Content: tc.name, Policy: "extract"},
		Policy{MaxAge: Forever},
		producer(orderedArchive(t, "file", "retry"), &ocispec.Descriptor{}),
	)
	assert.FatalIf(t, err != nil || input == nil || id == "", "retry invalid extraction = %q, %v", id, err)
}

type testAcquireArchiveFailuresAndLinksCaseCaseInputs struct {
	disk     **bool
	root     *string
	c        **Cache
	defaults *bool
}

func testAcquireArchiveFailuresAndLinksCaseCase(
	t *testing.T,
	testCaseInput testAcquireArchiveFailuresAndLinksCaseCaseInputs,
) {
	t.Helper()

	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for _, h := range []*tar.Header{
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../absent"},
		{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "file"},
		{Name: "dir/link", Typeflag: tar.TypeSymlink, Linkname: "../../absent"},
		{Name: "file", Typeflag: tar.TypeReg, Size: 7},
		{Name: "ignored", Typeflag: tar.TypeReg, Size: 7},
	} {
		assert.NoError(t, w.WriteHeader(h))
		if h.Typeflag == tar.TypeReg {
			_, err := io.WriteString(w, "content")
			assert.NoError(t, err)
		}
	}
	assert.NoError(t, w.Close())
	key := ArchiveKey{
		Content: fmt.Sprintf("source-test-link-%t", (*testCaseInput.defaults)),
		Ignore:  "ignored\n",
		Policy:  map[bool]string{true: "source"}[(*testCaseInput.defaults)],
	}
	input, _, _, err := (*testCaseInput.c).Acquire(
		t.Context(),
		key,
		Policy{MaxAge: Forever},
		producer(data.Bytes(), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	readers := []fs.FS{input}
	if *(*testCaseInput.disk) {
		fresh := cachetest.New(t, (*testCaseInput.root), New)
		replayed, _, _, err := fresh.Acquire(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				t.Error("fresh owner refetched cached archive")
				return ProducerResult{}, errors.New("unexpected fetch")
			},
		)
		assert.NoError(t, err)
		readers = append(readers, replayed)
	}
	for _, reader := range readers {
		entries, err := fs.ReadDir(reader, ".")
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			len(entries) != 1 || entries[0].Name() != "file",
			"archive entries = %v",
			entries,
		)
		for _, name := range []string{"link", "link/child", "hardlink", "hardlink/child", "dir/link", "dir/link/nested/child"} {
			data, readFileErr2 := fs.ReadFile(reader, name)
			assert.Equal(t, len(data), 0)
			assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
			_, readFileErr2 = fs.Stat(reader, name)
			assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
		}
		for _, name := range []string{"absent", "ignored"} {
			_, readFileErr3 := fs.ReadFile(reader, name)
			assert.Equal(t, errors.Is(readFileErr3, fs.ErrNotExist), true)
		}
		data, err := fs.ReadFile(reader, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "content")
	}
}

type testAcquireGeneratedPolicyCaseCompletionInputs struct {
	writer  **tar.Writer
	archive *bytes.Buffer
	c       **Cache
}
