package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testBlobLookupAndReplacementConcurrentFirstRead(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	keys := []BlobKey{{Content: "first"}, {Content: "second"}}
	upstream := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        123,
		MediaType:   "upstream/type",
		Annotations: map[string]string{"revision": "v1"},
	}
	for _, key := range keys {
		_, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte(key.Content.(string)), &upstream))
		assert.NoError(t, err)
	}
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	start := make(chan struct{})
	var ready sync.WaitGroup
	var joined sync.WaitGroup
	ready.Add(len(keys))
	for _, key := range keys {
		joined.Go(func() {
			ready.Done()
			<-start
			r, desc, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			assert.Diff(t, desc, upstream)
			data, err := fs.ReadFile(r, "content")
			assert.ErrorIf(
				t,
				err != nil || string(data) != key.Content.(string),
				"first read=%q, read=%v",
				data,
				err,
			)
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()
	assertBlobRoots(t, c, digest.SHA256)
	assert.NoError(t, c.Close())
	for i := range c.blobRoots {
		parent := &c.blobRoots[i]
		if parent.Root == nil {
			continue
		}
		{
			_, err := parent.Stat(".")
			assert.FatalIf(t, !errors.Is(err, fs.ErrClosed), "last Close retained blob parent: %v", err)
		}
	}
}

func testBlobLookupAndReplacementMixedAlgorithmFirstRead(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	for _, algorithm := range blobAlgorithms {
		key := BlobKey{Content: algorithm.String()}
		payload := []byte(algorithm.String())
		_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, &ocispec.Descriptor{}))
		assert.NoError(t, err)

		stored, err := c.store.Resolve(t.Context(), id)
		assert.NoError(t, err)

		stored.Digest = algorithm.FromBytes(payload)
		if err := c.store.Push(
			t.Context(),
			stored,
			bytes.NewReader(payload),
		); err != nil &&
			!errors.Is(err, errdef.ErrAlreadyExists) {
			t.Fatal(err)
		}
		assert.NoError(t, c.store.Tag(t.Context(), stored, id))
	}
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	assertBlobRoots(t, c)
	start := make(chan struct{})
	var ready, joined sync.WaitGroup
	ready.Add(len(blobAlgorithms))
	for _, algorithm := range blobAlgorithms {
		joined.Go(func() {
			ready.Done()
			<-start
			r, _, _, err := c.Blob(t.Context(), BlobKey{Content: algorithm.String()}, Policy{MaxAge: Forever}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			data, err := fs.ReadFile(r, "content")
			assert.ErrorIf(
				t,
				err != nil || string(data) != algorithm.String(),
				"%s first read=%q, read=%v",
				algorithm,
				data,
				err,
			)
		})
	}
	ready.Wait()
	close(start)
	joined.Wait()
	assertBlobRoots(t, c, blobAlgorithms[:]...)
	assert.NoError(t, c.Close())
	for i := range c.blobRoots {
		if _, err := c.blobRoots[i].Stat("."); !errors.Is(err, fs.ErrClosed) {
			t.Fatalf("last Close retained %s root: %v", blobAlgorithms[i], err)
		}
	}
}

func testBlobLookupAndReplacementRecreatedParent(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	key := BlobKey{Content: "recreated-parent"}
	_, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("first"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	assert.NoError(t, os.RemoveAll(filepath.Join(c.root.Name(), blobDir())))
	r, _, _, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("second"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	data, err := fs.ReadFile(r, "content")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "second")
	view := NameBlob("payload", r)

	data, err = fs.ReadFile(view, "payload")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "second")
}

func testBlobLookupAndReplacementCase(t *testing.T, algorithm *digest.Algorithm,
) {
	t.Helper()

	if !algorithm.Available() {
		t.Skip("digest algorithm is not linked")
	}
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "indexed-digest"}
	payload := []byte("independent readers")
	upstream := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        123,
		MediaType:   "upstream/type",
		Annotations: map[string]string{"revision": "v1"},
	}
	_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer(payload, &upstream))
	assert.NoError(t, err)

	stored, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)

	stored.Digest = algorithm.FromBytes(payload)
	if pushErr := c.store.Push(
		t.Context(),
		stored,
		bytes.NewReader(payload),
	); pushErr != nil &&
		!errors.Is(pushErr, errdef.ErrAlreadyExists) {
		t.Fatal(pushErr)
	}
	assert.NoError(t, c.store.Tag(t.Context(), stored, id))
	warmKey := BlobKey{Content: "independent-sha256"}
	_, _, _, err = c.Blob(t.Context(), warmKey, Policy{}, producer([]byte("warm SHA256"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	for range 2 {
		r, _, _, blobErr3 := c.Blob(t.Context(), warmKey, Policy{MaxAge: Forever}, nil)
		assert.NoError(t, blobErr3)

		data, blobErr3 := fs.ReadFile(r, "content")
		assert.NoError(t, blobErr3)
		assert.Equal(t, string(data), "warm SHA256")
	}
	assertBlobRoots(t, c, digest.SHA256)
	var view fs.FS
	for range 2 {
		r, desc, restoredID, blobErr4 := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		assert.NoError(t, blobErr4)

		assert.Diff(t, desc, upstream)
		assert.Equal(t, restoredID, id)
		view = NameBlob("payload", r)
		assert.Equal(
			t,
			AcquisitionIdentity(view),
			manifest.Fingerprint([2]string{stored.Digest.String(), "payload"}),
		)
	}
	assertBlobRoots(t, c, digest.SHA256, (*algorithm))
	var readers [2]fs.File
	for i := range readers {
		reader, openErr := view.Open("payload")
		assert.NoError(t, openErr)

		readers[i] = reader
		t.Cleanup(func() { _ = reader.Close() })
		first := make([]byte, 1)
		{
			_, readFullErr := io.ReadFull(reader, first)
			assert.FatalIf(
				t,
				readFullErr != nil || first[0] != payload[0],
				"independent first byte=%q: %v",
				first,
				readFullErr,
			)
		}
	}
	start := make(chan struct{})
	var joined sync.WaitGroup
	for _, reader := range readers {
		joined.Go(func() {
			<-start
			data, readAllErr := io.ReadAll(reader)
			closeErr := reader.Close()
			assert.ErrorIf(
				t,
				readAllErr != nil || closeErr != nil || !bytes.Equal(data, payload[1:]),
				"held reader=%q, read=%v, close=%v",
				data,
				readAllErr,
				closeErr,
			)
		})
	}
	close(start)
	joined.Wait()
	parent := filepath.Dir(blobPath(stored.Digest))
	assert.NoError(t, c.root.Rename(parent, parent+"-held"))
	assert.NoError(t, c.root.Mkdir(parent, 0o750))
	{
		reader, openErr2 := view.Open("payload")
		assert.FatalIf(
			t,
			reader != nil || !errors.Is(openErr2, fs.ErrNotExist),
			"held view ignored the replacement directory: reader=%v, error=%v",
			reader,
			openErr2,
		)
	}
	assert.NoError(t, c.root.Remove(parent))
	assert.NoError(t, os.Symlink(t.TempDir(), filepath.Join(root, parent)))
	generation, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
	if err == nil {
		_, err = fs.ReadFile(generation, "content")
	}
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrFlux),
		"coalesced generation accepted a replaced parent: %v",
		err,
	)
	{
		reader, err := view.Open("payload")
		assert.FatalIf(
			t,
			reader != nil || err == nil,
			"held view accepted a replaced parent: reader=%v, error=%v",
			reader,
			err,
		)
	}
}

func testBlobLookupAndReplacementOutcomes(t *testing.T) {
	t.Helper()

	for _, maxAge := range []time.Duration{0, time.Hour, Forever, -2} {
		t.Run(maxAge.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				root := t.TempDir()
				c := cachetest.New(t, root, New)
				key := BlobKey{Content: "recorded"}
				calls := 0
				fail := func(context.Context, ProducerInput) (ProducerResult, error) {
					calls++
					return ProducerResult{}, connReset()
				}
				policy := Policy{MaxAge: maxAge, Retry: fastRetry(3)}
				_, _, id, err := c.Blob(
					t.Context(),
					key,
					Policy{},
					producer([]byte("original"), &ocispec.Descriptor{}),
				)
				assert.NoError(t, err)

				lastGood, err := c.store.Resolve(t.Context(), id)
				assert.NoError(t, err)
				{
					closeErr2 := c.Close()
					assert.NoError(t, closeErr2)
				}

				c = cachetest.New(t, root, New)
				_, _, _, err = c.Blob(t.Context(), key, Policy{Retry: fastRetry(3)}, fail)
				terminal, ok := errors.AsType[*TerminalError](err)
				assert.FatalIf(
					t,
					!ok || !terminal.Attempted || calls != 3,
					"initial calls=%d, error=%v",
					calls,
					err,
				)
				desc, err := c.store.Resolve(t.Context(), id)
				assert.FatalIf(
					t,
					err != nil || desc.MediaType != ocispec.MediaTypeImageManifest,
					"same-tag outcome=%+v, error=%v",
					desc,
					err,
				)
				initial, err := c.readOutcome(t.Context(), &desc)
				assert.FatalIf(t, err != nil || initial == nil || initial.record.Count != 1 ||
					initial.lastGood.Digest != lastGood.Digest, "initial outcome=%+v, error=%v", initial, err)
				for range 2 {
					{
						closeErr3 := c.Close()
						assert.NoError(t, closeErr3)
					}

					c = cachetest.New(t, root, New)
					for _, produce := range []func(context.Context, ProducerInput) (ProducerResult, error){nil, fail} {
						_, _, _, blobErr5 := c.Blob(t.Context(), key, policy, produce)
						terminal, ok := errors.AsType[*TerminalError](blobErr5)
						assert.FatalIf(t, !ok || terminal.Attempted || !errors.Is(blobErr5, syscall.ECONNRESET) ||
							!errors.Is(blobErr5, manifest.ErrFlux) ||
							calls != 3, "fresh calls=%d, error=%v", calls, blobErr5)
					}
				}
				time.Sleep(750*time.Millisecond - time.Nanosecond)
				_, _, _, err = c.Blob(t.Context(), key, policy, fail)
				{
					terminal, ok := errors.AsType[*TerminalError](err)
					assert.FatalIf(
						t,
						!ok || terminal.Attempted || calls != 3,
						"before deadline calls=%d, error=%v",
						calls,
						err,
					)
				}
				unchanged, err := c.readOutcome(t.Context(), &desc)
				assert.FatalIf(
					t,
					err != nil || unchanged == nil || unchanged.record.Count != initial.record.Count ||
						!unchanged.record.Completed.Equal(
							initial.record.Completed,
						),
					"read renewed outcome: %+v, error=%v",
					unchanged,
					err,
				)
				time.Sleep(time.Nanosecond)
				{
					_, _, _, blobErr6 := c.Blob(t.Context(), key, policy, nil)
					assert.FatalIf(t, !errors.Is(blobErr6, errdef.ErrNotFound), "due lookup: %v", blobErr6)
				}
				_, _, _, err = c.Blob(t.Context(), key, policy, fail)
				{
					terminal, ok := errors.AsType[*TerminalError](err)
					assert.FatalIf(
						t,
						!ok || !terminal.Attempted || calls != 4,
						"due calls=%d, error=%v",
						calls,
						err,
					)
				}
				nextDesc, err := c.store.Resolve(t.Context(), id)
				assert.NoError(t, err)

				next, err := c.readOutcome(t.Context(), &nextDesc)
				assert.FatalIf(
					t,
					err != nil || next == nil || next.record.Count != 2 || !next.record.Completed.Equal(time.Now()),
					"second outcome=%+v, error=%v",
					next,
					err,
				)
				time.Sleep(1500 * time.Millisecond)
				_, _, _, err = c.Blob(t.Context(), key, policy, producer([]byte("recovered"), &ocispec.Descriptor{}))
				assert.NoError(t, err)

				good, err := c.store.Resolve(t.Context(), id)
				assert.FatalIf(
					t,
					err != nil || good.MediaType != "application/octet-stream",
					"recovery descriptor=%+v, error=%v",
					good,
					err,
				)
				_, _, _, err = c.Blob(t.Context(), key, Policy{}, fail)
				assert.FatalIf(
					t,
					!errors.Is(err, syscall.ECONNRESET) || calls != 5,
					"new sequence calls=%d, error=%v",
					calls,
					err,
				)
				resetDesc, err := c.store.Resolve(t.Context(), id)
				assert.NoError(t, err)

				reset, err := c.readOutcome(t.Context(), &resetDesc)
				assert.FatalIf(
					t,
					err != nil || reset == nil || reset.record.Count != 1 || reset.lastGood.Digest != good.Digest,
					"reset outcome=%+v, error=%v",
					reset,
					err,
				)
			})
		})
	}
}

func testBlobLookupAndReplacementCase2(t *testing.T, identities *map[string]string,
	backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "blob-completed"}
	for range 2 {
		r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		assert.FatalIf(
			t,
			r != nil || !errors.Is(err, errdef.ErrNotFound) || !errors.Is(err, manifest.ErrFlux),
			"lookup miss: reader=%v, error=%v",
			r,
			err,
		)
	}
	testBlobLookupAndReplacementCase2Completion(
		t,
		&testBlobLookupAndReplacementCase2CompletionInputs{
			c:          &c,
			key:        &key,
			identities: &identities,
			backing:    &backing,
			root:       &root,
		},
	)
}

func testBlobLookupAndReplacementCase2Completion(
	t *testing.T,
	captures *testBlobLookupAndReplacementCase2CompletionInputs,
) {
	t.Helper()
	(*captures.c).
		entries.Range(func(_, _ any) bool { t.Error("lookup miss installed an entry"); return true })
	var held fs.FS
	var heldPin, heldPayload string
	for _, payload := range []string{"first", ""} {
		r, _, id, err := (*captures.c).Blob(
			t.Context(), (*captures.key), Policy{MaxAge: 0},
			producer([]byte(payload), &ocispec.Descriptor{}),
		)
		assert.NoError(t, err)

		view := NameBlob("chart.tgz", r)
		assert.Equal(t, FileName(view), "chart.tgz")
		readerPin := AcquisitionIdentity(r)
		assert.Equal(t, readerPin, digest.FromString(payload).String())
		assert.Equal(t, AcquisitionIdentity(struct{ fs.FS }{r}), "")
		pin := AcquisitionIdentity(view)
		assert.Equal(t, pin, (*(*captures.identities))[payload])
		assert.FatalIf(t, pin == "" || pin == AcquisitionIdentity(NameBlob("other.tgz", r)) ||
			AcquisitionIdentity(
				struct{ fs.FS }{view},
			) != "", "%s", "blob identity lost generation, filename or confinement")

		if held != nil {
			data, readFileErr2 := fs.ReadFile(held, "chart.tgz")
			assert.FatalIf(
				t,
				readFileErr2 != nil || string(data) != heldPayload || AcquisitionIdentity(held) != heldPin ||
					heldPin == pin,
				"held generation changed: %q, %v",
				data,
				readFileErr2,
			)
		}

		stream, err := r.Open("content")
		assert.NoError(t, err)
		assert.NoError(t, stream.Close())
		assert.Equal(t, AcquisitionIdentity(r), readerPin)
		{
			r, _, _, blobErr7 := (*captures.c).Blob(
				t.Context(), (*captures.key), Policy{MaxAge: 0},
				nil,
			)
			assert.FatalIf(t, !errors.Is(blobErr7, errdef.ErrNotFound) ||
				r != nil, "stale lookup: reader=%v, error=%v", r, blobErr7)
		}
		if (*(*captures.backing)) == "disk" {
			e, _ := (*captures.c).entries.Load(id)
			raw, readFileErr3 := os.ReadFile(
				filepath.Join((*captures.c).root.Name(), blobPath(e.(*entry).stored.Digest)),
			)
			assert.FatalIf(
				t,
				readFileErr3 != nil || string(raw) != payload,
				"plain completed payload=%q: %v",
				raw,
				readFileErr3,
			)
			{
				closeErr4 := (*captures.c).Close()
				assert.NoError(t, closeErr4)
			}
			(*captures.c) = cachetest.New(t, (*captures.root), New)
			{
				r, _, _, blobErr := (*captures.c).Blob(
					t.Context(), (*captures.key), Policy{},
					nil,
				)
				assert.FatalIf(t, r != nil ||
					!errors.Is(blobErr, errdef.ErrNotFound), "cold zero-age lookup: reader=%v, error=%v", r, blobErr)
			}
		}
		first, _, _, err := (*captures.c).Blob(t.Context(), (*captures.key), Policy{MaxAge: Forever}, nil)
		assert.NoError(t, err)

		restored := NameBlob("chart.tgz", first)
		assert.Equal(t, AcquisitionIdentity(first), readerPin)
		assert.FatalIf(t, AcquisitionIdentity(restored) != pin, "%s", "blob restoration changed identity")

		held, heldPin, heldPayload = restored, pin, payload
		var data []byte
		if (*(*captures.backing)) == "memory" {
			data, err = fs.ReadFile(view, "chart.tgz")
		}
		assert.FatalIf(
			t, (*(*captures.backing)) ==
				"memory" && (err != nil || string(data) != payload), "closed cursor invalidated view: %q, %v",
			data,
			err,
		)
		var wg sync.WaitGroup
		wg.Go(func() {
			other, _, _, blobErr8 := (*captures.c).Blob(t.Context(), (*captures.key), Policy{MaxAge: Forever}, nil)
			if blobErr8 != nil {
				t.Error(blobErr8)
				return
			}
			got, blobErr8 := fs.ReadFile(other, "content")
			assert.ErrorIf(
				t,
				blobErr8 != nil || string(got) != payload,
				"independent reader=%q, read=%v",
				got,
				blobErr8,
			)
		})
		wg.Wait()
		got, err := fs.ReadFile(first, "content")
		assert.FatalIf(t, err != nil || string(got) != payload, "held reader=%q, read=%v", got, err)
	}
}

func testBlobRenderLifetimeCase(t *testing.T, identity *string,
) {
	t.Helper()
	processIdentity = (*identity)
	for _, backing := range []string{"memory", "disk"} {
		t.Run(backing, func(t *testing.T) {
			testBlobRenderLifetimeCaseCase(
				t,
				testBlobRenderLifetimeCaseCaseInputs{identity: &identity, backing: &backing},
			)
		},
		)
	}
}

type testBlobLookupAndReplacementCase2CompletionInputs struct {
	c          **Cache
	key        *BlobKey
	identities **map[string]string
	backing    **string
	root       *string
}
