package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func assertBlobRoots(t *testing.T, c *Cache, algorithms ...digest.Algorithm) {
	t.Helper()
	for i, algorithm := range blobAlgorithms {
		if (c.blobRoots[i].Root != nil) != slices.Contains(algorithms, algorithm) {
			t.Fatalf("opened %s root=%v, want algorithms %v", algorithm, c.blobRoots[i].Root != nil, algorithms)
		}
	}
}

func TestBlob_LookupAndReplacement(t *testing.T) {
	t.Parallel()
	t.Run("sha256-only", func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementSha256Only(t) })
	t.Run("unused-invalid-path", func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementUnusedInvalidPath(t) })
	t.Run("root-reuse", func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementRootReuse(t) })
	t.Run(
		"concurrent-first-read",
		func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementConcurrentFirstRead(t) },
	)
	t.Run(
		"mixed-algorithm-first-read",
		func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementMixedAlgorithmFirstRead(t) },
	)
	t.Run("recreated-parent", func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementRecreatedParent(t) })
	for _, algorithm := range []digest.Algorithm{digest.SHA256, digest.SHA384, digest.SHA512} {
		t.Run(algorithm.String(), func(t *testing.T) {
			t.Parallel()
			testBlobLookupAndReplacementCase(t, &algorithm)
		},
		)
	}
	t.Run("outcomes", func(t *testing.T) { t.Parallel(); testBlobLookupAndReplacementOutcomes(t) })

	identities := make(map[string]string)
	memory := cachetest.New(t, "", New)
	for _, payload := range []string{"first", ""} {
		r, _, _, err := memory.Blob(
			t.Context(),
			BlobKey{Content: "blob-completed"},
			&Policy{},
			producer([]byte(payload), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		identities[payload] = AcquisitionIdentity(NameBlob("chart.tgz", r))
	}
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testBlobLookupAndReplacementCase2(
				t,
				identities, &backing,
			)
		},
		)
	}
}

//nolint:paralleltest // The test changes the process build identity and restores it afterward.
func TestBlob_RenderLifetime(t *testing.T) {
	original := processIdentity
	t.Cleanup(func() { processIdentity = original })
	for _, identity := range []string{"", "identified"} {
		t.Run("identity="+identity, func(t *testing.T) {
			testBlobRenderLifetimeCase(t, &identity)
		},
		)
	}
}

//nolint:paralleltest // The test changes the process build identity and restores it afterward.
func TestBlob_RenderAcquisition(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	synctest.Test(t, func(innerT *testing.T) {
		root := innerT.TempDir()
		c := cachetest.New(innerT, root, New)
		accepted := BlobKey{Content: "render-acquisition", Render: RenderAccepted}
		generation, _, _, err := c.Blob(
			innerT.Context(),
			accepted,
			&Policy{},
			producer([]byte("accepted"), &ocispec.Descriptor{}),
		)
		assert.NoError(innerT, err)
		var current atomic.Value
		current.Store("accepted")
		pending := BlobKey{
			Content: accepted.Content,
			Render:  RenderPending,
			Validate: func(_ context.Context, input fs.FS) (bool, error) {
				data, readFileErr := fs.ReadFile(input, "content")
				return string(data) == current.Load().(string), readFileErr
			},
		}
		hit, _, _, err := c.Blob(innerT.Context(), pending, &Policy{MaxAge: Forever}, nil)
		assert.NoError(innerT, err)
		data, err := fs.ReadFile(hit, "content")
		assert.NoError(innerT, err)
		assert.Equal(innerT, string(data), "accepted")
		current.Store("pending")
		entered, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int64
		var wg sync.WaitGroup
		run := func() {
			input, _, _, blobErr := c.Blob(
				innerT.Context(),
				pending,
				&Policy{MaxAge: Forever},
				func(_ context.Context, w ProducerInput) (ProducerResult, error) {
					calls.Add(1)
					close(entered)
					<-release
					_, writeStringErr := io.WriteString(w, "pending")
					return ProducerResult{}, writeStringErr
				},
			)
			assert.NoError(innerT, blobErr)
			readFileData, blobErr := fs.ReadFile(input, "content")
			assert.NoError(innerT, blobErr)
			assert.Equal(innerT, string(readFileData), "pending")
		}
		wg.Go(run)
		<-entered
		wg.Go(run)
		synctest.Wait()
		close(release)
		wg.Wait()
		assert.Equal(innerT, calls.Load(), int64(1))
		data, err = fs.ReadFile(generation, "content")
		assert.NoError(innerT, err)
		assert.Equal(innerT, string(data), "accepted")
		assert.NoError(innerT, c.Close())
		c = cachetest.New(innerT, root, New)
		if input, _, _, err := c.Blob(
			innerT.Context(),
			pending,
			&Policy{MaxAge: Forever},
			nil,
		); input != nil ||
			!errors.Is(err, errdef.ErrNotFound) {
			innerT.Fatalf("unaccepted work survived owner close: %v", err)
		}
	})
}

//nolint:paralleltest // The test changes the process build identity and restores it afterward.
func TestBlob_RenderValidation(t *testing.T) {
	original := processIdentity
	t.Cleanup(func() { processIdentity = original })
	for _, backing := range []string{"memory", "unidentified disk", "identified disk", "identified disk due"} {
		t.Run(backing, func(t *testing.T) {
			testBlobRenderValidationCase(t, &backing)
		},
		)
	}
}

//nolint:paralleltest // The test changes the process build identity and restores it afterward.
func TestBlob_NonReusableGeneration(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	for _, backing := range []string{"memory", "disk", "disk due"} {
		t.Run(backing, func(t *testing.T) {
			testBlobNonReusableGenerationCase(t, &backing)
		},
		)
	}
}

//nolint:paralleltest // The test changes the process build identity and restores it afterward.
func TestBlob_FailuresRemainErrors(t *testing.T) {
	original := processIdentity
	processIdentity = "identified"
	t.Cleanup(func() { processIdentity = original })
	for _, root := range []string{"", t.TempDir()} {
		c := cachetest.New(t, root, New)
		modes := []RenderMode{0, RenderPending}
		if root != "" {
			modes = append(modes, RenderAccepted)
		}
		for _, render := range modes {
			key := BlobKey{Content: "blob-error", Render: render}
			failure := errors.New("failed completed producer")
			r, _, _, err := c.Blob(
				t.Context(),
				key,
				&Policy{MaxAge: 0},
				func(_ context.Context, w ProducerInput) (ProducerResult, error) {
					_, _ = io.WriteString(w, "partial")
					return ProducerResult{}, failure
				},
			)
			assert.FatalIf(
				t,
				r != nil || !errors.Is(err, failure) || !errors.Is(err, manifest.ErrFlux),
				"producer error: %v",
				err,
			)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			{
				r, _, _, err := c.Blob(
					ctx,
					key,
					&Policy{MaxAge: Forever},
					nil,
				)
				assert.FatalIf(t, r != nil || !errors.Is(err, context.Canceled) ||
					!errors.Is(err, manifest.ErrFlux), "lookup cancellation: %v", err)
			}
		}
	}
	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "blob-missing-payload"}
	r, _, id, err := c.Blob(t.Context(), key, &Policy{MaxAge: 0}, producer([]byte("payload"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	stream, err := r.Open("content")
	assert.NoError(t, err)
	assert.NoError(t, stream.Close())
	{
		err := stream.Close()
		assert.FatalIf(t, !errors.Is(err, fs.ErrClosed), "backing Close error was lost: %v", err)
	}
	input := fileFS{name: "content", size: 7, open: func() (io.ReadCloser, error) { return stream, nil }}
	_, readErr := fs.ReadFile(input, "content")
	joined, ok := readErr.(interface{ Unwrap() []error })
	assert.FatalIf(t, !ok || len(joined.Unwrap()) != 2, "read and close failures were not joined: %v", readErr)
	for i, op := range []string{"read", "close"} {
		pathErr, ok := errors.AsType[*fs.PathError](joined.Unwrap()[i])
		if !ok || pathErr.Op != op || !errors.Is(pathErr, fs.ErrClosed) {
			t.Fatalf("%s failure was lost: %v", op, joined.Unwrap()[i])
		}
	}
	e, _ := c.entries.Load(id)
	{
		err := os.Remove(filepath.Join(c.root.Name(), blobPath(e.(*entry).stored.Digest)))
		assert.NoError(t, err)
	}

	for range 2 {
		generation, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
		if err == nil {
			_, err = fs.ReadFile(generation, "content")
		}
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrNotExist) || errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux),
			"missing payload became miss: %v",
			err,
		)
	}
}

func TestBlob_ConcurrentPayloads(t *testing.T) {
	t.Parallel()
	c := cachetest.New(t, t.TempDir(), New)
	const workers = 4
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(workers)
	for i := range workers {
		joined.Go(func() {
			ready.Done()
			<-start
			payload := bytes.Repeat([]byte{byte(i)}, 2<<20)
			key := BlobKey{Content: string(rune('a' + i))}
			_, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: 0}, producer(payload, &ocispec.Descriptor{}))
			if err != nil {
				t.Error(err)
				return
			}

			r, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			got, err := fs.ReadFile(r, "content")
			assert.ErrorIf(
				t,
				err != nil || !bytes.Equal(got, payload),
				"concurrent payload %d differs: read=%v",
				i,
				err,
			)
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()
}

func TestBlob_Revalidation(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testBlobRevalidationCase(t, &backing)
		},
		)
	}
	t.Run("absent", func(t *testing.T) { t.Parallel(); testBlobRevalidationAbsent(t) })
	t.Run(
		"unchanged-parent-replacement",
		func(t *testing.T) { t.Parallel(); testBlobRevalidationUnchangedParentReplacement(t) },
	)
	for _, invalid := range []string{"missing bytes", "corrupted-size"} {
		t.Run(invalid, func(t *testing.T) {
			t.Parallel()
			testBlobRevalidationCase2(t, &invalid)
		},
		)
	}
}

func TestBlob_RepairFailureCleanup(t *testing.T) {
	t.Parallel()
	for _, failure := range []bool{false, true} {
		name := "cleanup-success"
		if failure {
			name = "cleanup-failure"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testBlobRepairFailureCleanupCase(t, &failure)
		},
		)
	}
}

func TestBlob_RevalidationFailures(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		for _, failure := range []string{"producer", "canceled", "partial", "publication", "outcome"} {
			if !disk && failure == "publication" {
				continue
			}
			t.Run(fmt.Sprintf("disk=%t/%s", disk, failure), func(t *testing.T) {
				t.Parallel()
				testBlobRevalidationFailuresCase(
					t,
					&disk, &failure,
				)
			},
			)
		}
	}
}

func TestRenderMetadata_CurrentGenerationWithoutPayloadOpen(t *testing.T) {
	t.Parallel()
	reason := RenderPersistenceDisabledReason()
	persistent := reason == ""
	t.Logf("render persistence reason=%q backing=disk", reason)
	synctest.Test(t, func(t *testing.T) {
		c := cachetest.New(t, t.TempDir(), New)
		key := BlobKey{Content: "render-metadata", Render: RenderAccepted}
		_, _, id, err := c.Blob(
			t.Context(),
			key,
			&Policy{},
			producer([]byte("first"), &ocispec.Descriptor{Annotations: map[string]string{"generation": "first"}}),
		)
		assert.NoError(t, err)

		if !persistent {
			_, known := c.entries.Load(id)
			assert.Equal(t, known, false)
			assert.Equal(t, id, key.Content.(string))
			var recording manifest.WorkRecorder
			accounting := manifest.AcquisitionWork{Scope: recording.Scope()}
			metadata, metadataErr := c.RenderMetadata(t.Context(), key.Content.(string), id, &accounting)
			assert.NoError(t, metadataErr)
			assert.Equal(t, metadata.Enabled, false)
			assert.Equal(t, metadata.Identity, "")
			assert.Equal(t, len(metadata.Descriptor.Annotations), 0)
			assert.Equal(t, accounting.Lookup(id).Counts, manifest.WorkCounts{})
			generation, _, _, lookupErr := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
			assert.Equal(t, errors.Is(lookupErr, errdef.ErrNotFound), true)
			assert.Equal(t, generation == nil, true)
			return
		}

		previous, _ := c.entries.Load(id)
		e := previous.(*entry)
		stamp := time.Now()
		assert.NoError(t, c.root.Chtimes(blobPath(e.stored.Digest), stamp, stamp))
		input := e.fs.(fileFS)
		input.open = func() (io.ReadCloser, error) {
			t.Error("metadata lookup opened the payload")
			return nil, errors.New("unexpected payload open")
		}
		e.fs = input
		var recording manifest.WorkRecorder
		accounting := manifest.AcquisitionWork{Scope: recording.Scope()}
		renderMetadataResult, err := c.RenderMetadata(t.Context(), key.Content.(string), id, &accounting)
		descriptor, enabled := renderMetadataResult.Descriptor, renderMetadataResult.Enabled
		assert.NoError(t, err)
		assert.Equal(t, enabled, true)
		assert.Equal(t, descriptor.Annotations["generation"], "first")
		assert.Equal(t, accounting.Lookup(id).Counts, manifest.WorkCounts{Attempts: 1, Completed: 1, CacheService: 1})
		started, publish := make(chan struct{}), make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			_, _, _, generationErr := c.Blob(
				t.Context(),
				key,
				&Policy{},
				func(_ context.Context, input ProducerInput) (ProducerResult, error) {
					close(started)
					<-publish
					_, writeErr := input.Write([]byte("second"))
					return ProducerResult{Annotations: map[string]string{"generation": "second"}}, writeErr
				},
			)
			finished <- generationErr
		}()
		<-started
		renderMetadataResult2, renderMetadataResult2Err := c.RenderMetadata(
			t.Context(),
			key.Content.(string),
			id,
			&accounting,
		)
		descriptor, enabled, err = renderMetadataResult2.Descriptor, renderMetadataResult2.Enabled, renderMetadataResult2Err
		close(publish)
		assert.NoError(t, err)
		assert.Equal(t, enabled, true)
		assert.Equal(t, descriptor.Annotations["generation"], "first")
		assert.NoError(t, <-finished)
		current, _ := c.entries.Load(id)
		head := current.(*entry)
		assert.NoError(t, c.root.Chtimes(blobPath(head.stored.Digest), stamp, stamp))
		renderMetadataResult3, renderMetadataResult3Err := c.RenderMetadata(
			t.Context(),
			key.Content.(string),
			id,
			&accounting,
		)
		descriptor, enabled, err = renderMetadataResult3.Descriptor, renderMetadataResult3.Enabled, renderMetadataResult3Err
		assert.NoError(t, err)
		assert.Equal(t, enabled, true)
		assert.Equal(t, descriptor.Annotations["generation"], "second")
		assert.Equal(t, e.descriptor.Annotations["generation"], "first")
		before, err := c.root.Stat(blobPath(head.stored.Digest))
		assert.NoError(t, err)
		time.Sleep(2 * accessInterval)
		_, renderMetadataResult4Err := c.RenderMetadata(
			t.Context(),
			key.Content.(string),
			id,
			&accounting,
		)
		err = renderMetadataResult4Err
		assert.NoError(t, err)
		after, err := c.root.Stat(blobPath(head.stored.Digest))
		assert.NoError(t, err)
		if !after.ModTime().After(before.ModTime()) {
			t.Fatal("metadata access did not retain the active generation")
		}

		assert.Equal(t, accounting.Lookup(id).Counts, manifest.WorkCounts{Attempts: 4, Completed: 4, CacheService: 4})
	})
}

func TestRenderMetadata_CurrentDirectory(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"removed payload", "nonregular payload", "removed parent", "replaced parent empty", "replaced parent present", "escaping parent", "canceled", "closed"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testRenderMetadataCurrentDirectoryCase(t, &name)
		},
		)
	}
}

func TestBlob_AcquisitionWaves(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			testBlobAcquisitionWavesCase(t, &backing)
		},
		)
	}
}

func testBlobLookupAndReplacementSha256Only(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	checkUnused := func() {
		t.Helper()
		for _, algorithm := range []digest.Algorithm{digest.SHA384, digest.SHA512} {
			{
				_, err := c.root.Stat(
					filepath.Join(blobDir(), algorithm.String()),
				)
				assert.FatalIf(t, !errors.Is(
					err,
					fs.ErrNotExist,
				), "SHA256-only owner created unused %s directory: %v", algorithm, err)
			}
			assert.FatalIf(
				t,
				c.blobRoots[slices.Index(blobAlgorithms[:], algorithm)].Root != nil,
				"SHA256-only owner opened unused %s root",
				algorithm,
			)
		}
	}
	checkUnused()
	assertBlobRoots(t, c)
	key := BlobKey{Content: "sha256-only"}
	payload := []byte("SHA256 payload")
	_, _, id, err := c.Blob(t.Context(), key, &Policy{}, producer(payload, &ocispec.Descriptor{}))
	assert.NoError(t, err)

	checkUnused()
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	checkUnused()
	assertBlobRoots(t, c)
	r, _, restoredID, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)
	assert.Diff(t, data, payload)
	assert.Equal(t, restoredID, id)
	assert.Equal(t, AcquisitionIdentity(r), digest.FromBytes(payload).String())
	view := NameBlob("unconventional.payload", r)
	assert.Equal(
		t,
		AcquisitionIdentity(view),
		manifest.Fingerprint([2]string{digest.FromBytes(payload).String(), "unconventional.payload"}),
	)
	assert.Equal(t, FileName(view), "unconventional.payload")
	assert.Equal(t, FileName(struct{ fs.FS }{view}), "")
	assert.Equal(t, FileName(fstest.MapFS{"chart.tgz": {Data: payload}}), "")
	contents, err := fs.ReadFile(view, FileName(view))
	assert.NoError(t, err)
	assert.Diff(t, contents, payload)
	var treeData bytes.Buffer
	testutil.WriteTar(t, &treeData, map[string]string{"demo/Chart.yaml": "metadata"}, 0o644)
	tree, _, _, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: "filename-tree"},
		&Policy{},
		producer(treeData.Bytes(), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	assert.Equal(t, FileName(tree), "")

	checkUnused()
	assert.FatalIf(t, c.blobRoots[0].Root == nil, "%s", "SHA256 read did not open its root")
}

func testBlobLookupAndReplacementUnusedInvalidPath(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, filepath.Join(blobDir(), digest.SHA384.String()), "not a directory")
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "unused-invalid-path"}
	_, _, _, err := c.Blob(t.Context(), key, &Policy{}, producer([]byte("payload"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	r, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "payload")

	assertBlobRoots(t, c, digest.SHA256)
}

func testBlobLookupAndReplacementRootReuse(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	keys := []BlobKey{{Content: "reuse-first"}, {Content: "reuse-second"}}
	for _, key := range keys {
		_, _, _, err := c.Blob(
			t.Context(),
			key,
			&Policy{},
			producer([]byte(key.Content.(string)), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)
	}
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	var retained *os.Root
	for _, key := range keys {
		r, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
		assert.NoError(t, err)

		data, err := fs.ReadFile(r, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(data), key.Content.(string))

		assertBlobRoots(t, c, digest.SHA256)
		assert.FatalIf(
			t,
			retained != nil && c.blobRoots[0].Root != retained,
			"%s",
			"distinct warm keys reopened the SHA256 root",
		)

		retained = c.blobRoots[0].Root
	}
	assert.NoError(t, c.Close())
	{
		_, err := retained.Stat(".")
		assert.FatalIf(t, !errors.Is(err, fs.ErrClosed), "last Close retained reused blob root: %v", err)
	}
}
