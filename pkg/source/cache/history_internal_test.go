package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test fixes the process build identity to isolate format invalidation.
func TestProducersForPaths_FrameFormat(t *testing.T) {
	original := processIdentity
	processIdentity = "fixed-build"
	t.Cleanup(func() { processIdentity = original })
	c := cachetest.New(t, t.TempDir(), New)
	owner := manifest.NamedResource{Kind: manifest.KindKustomization, Name: "emitter"}
	source := manifest.SourceIdentity{Root: "/source"}
	evidence := []manifest.SourceEvidence{{Source: source, Path: "apps/cm.yaml"}}
	history := RenderHistory{Owner: owner, Sources: evidence}
	prior := history.record("accepted")
	prior.Format = frameVersion - 1
	metadata, err := prior.metadata()
	assert.NoError(t, err)
	metadata[acceptedRenderAnnotation] = "accepted"
	_, _, id, err := c.Blob(
		t.Context(),
		BlobKey{Content: "prior-format"},
		&Policy{},
		func(_ context.Context, output ProducerInput) (ProducerResult, error) {
			_, writeErr := io.WriteString(output, "prior frame")
			return ProducerResult{Annotations: metadata}, writeErr
		},
	)
	assert.NoError(t, err)
	descriptor, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)
	priorLookup := "render-" + manifest.Fingerprint(struct {
		Phase  RenderMode
		Build  string
		Render string
	}{Phase: RenderAccepted, Build: processIdentity, Render: "format-test"})
	assert.NoError(t, c.store.Tag(t.Context(), descriptor, priorLookup))
	_, _, currentLookup, err := c.Blob(
		t.Context(),
		BlobKey{Content: "format-test", Render: RenderAccepted},
		&Policy{MaxAge: Forever},
		nil,
	)
	assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
	assert.Equal(t, currentLookup != priorLookup, true)
	owners, err := c.ProducersForPaths(t.Context(), evidence)
	assert.NoError(t, err)
	assert.Equal(t, len(owners), 0)
	frame, err := NewFrame("kustomize-output", "metadata", []byte("current frame"))
	assert.NoError(t, err)
	publication, err := frame.PreparePublication(t.Context(), FramePublicationInput{
		Owner: owner, Content: "final", Complete: true,
		Records: maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{}),
	})
	assert.NoError(t, err)
	inventory := AcceptedInventory[*FramePublication]{
		Key: "format-test", Acceptance: publication.Identity(), History: history, Content: publication,
	}
	var failures [2]error
	var writers sync.WaitGroup
	for i := range failures {
		writers.Go(func() { failures[i] = RecordInventory(t.Context(), c, inventory) })
	}
	writers.Wait()
	for _, err := range failures {
		assert.NoError(t, err)
	}
	generation, _, lookup, err := c.Blob(
		t.Context(),
		BlobKey{Content: "format-test", Render: RenderAccepted},
		&Policy{MaxAge: Forever},
		nil,
	)
	assert.NoError(t, err)
	assert.Equal(t, lookup != priorLookup, true)
	var decoded string
	current, err := OpenFrame(t.Context(), "kustomize-output", generation, &decoded)
	assert.NoError(t, err)
	_, complete := current.History(owner, "final")
	assert.Equal(t, complete, true)
	owners, err = c.ProducersForPaths(t.Context(), evidence)
	assert.NoError(t, err)
	assert.Diff(t, owners[owner], evidence)
	assert.Equal(t, len(owners), 1)
	var stored map[string]any
	assert.NoError(t, json.Unmarshal([]byte(metadata[historyAnnotation]), &stored))
	assert.Equal(t, stored["Build"], any(processIdentity))
}

func TestProducersForPaths_SourcePaths(t *testing.T) {
	t.Parallel()
	persistent := RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	cache := cachetest.New(t, root, New)
	owner := manifest.NamedResource{
		Group:     manifest.HelmReleaseDomain,
		Kind:      manifest.KindHelmRelease,
		Namespace: "apps",
		Name:      "emitter",
	}
	source := manifest.SourceIdentity{
		Kind: manifest.KindGitRepository,
		URL:  "https://fixture.invalid/one",
		Key:  "family",
	}
	history := &RenderHistory{
		Owner: owner,
		Sources: []manifest.SourceEvidence{
			{Source: source, Path: "apps/blue", Subtree: true},
			{Source: source, Path: "shared/values.yaml"},
		},
	}
	metadata, err := new(*history).record("accepted").metadata()
	assert.NoError(t, err)
	_, _, _, err = cache.Blob(
		t.Context(),
		BlobKey{Content: "history-producer", Render: RenderAccepted},
		&Policy{},
		func(_ context.Context, output ProducerInput) (ProducerResult, error) {
			_, writeStringErr := io.WriteString(output, "accepted")
			metadata[acceptedRenderAnnotation] = "accepted"
			return ProducerResult{Annotations: metadata}, writeStringErr
		},
	)
	assert.NoError(t, err)
	assert.NoError(t, cache.Close())
	cache = cachetest.New(t, root, New)
	for _, tc := range []struct {
		name   string
		source manifest.SourceIdentity
		path   string
		found  bool
	}{
		{name: "child root", source: source, path: "apps/blue/cm.yaml", found: true},
		{name: "read input", source: source, path: "shared/values.yaml", found: true},
		{name: "file boundary", source: source, path: "shared/values.yaml/other"},
		{name: "path boundary", source: source, path: "apps/blue-other/cm.yaml"},
		{name: "unclaimed", source: source, path: "notes.txt"},
		{name: "other source", source: manifest.SourceIdentity{Kind: source.Kind, URL: "https://fixture.invalid/two", Key: source.Key}, path: "apps/blue/cm.yaml"},
		{name: "other family", source: manifest.SourceIdentity{Kind: source.Kind, URL: source.URL, Key: "other"}, path: "apps/blue/cm.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			owners, err := cache.ProducersForPaths(
				t.Context(),
				[]manifest.SourceEvidence{{Source: tc.source, Path: tc.path}},
			)
			assert.NoError(t, err)
			var want []manifest.NamedResource
			if tc.found && persistent {
				want = []manifest.NamedResource{owner}
				assert.Diff(t, owners[owner], []manifest.SourceEvidence{{Source: tc.source, Path: tc.path}})
			}
			assert.Diff(t, slices.SortedFunc(maps.Keys(owners), manifest.NamedResource.Compare), want)
		})
	}
}

func TestProducersForPaths_Unavailable(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"canceled", "missing", "foreign build", "malformed", "incomplete owner", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testProducersForPathsUnavailableCase(t, &mode)
		},
		)
	}
}

func TestProducersForPaths_Acceptance(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"accepted", "pending", "source", "rejected", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testProducersForPathsAcceptanceCase(t, &mode)
		},
		)
	}
}

func TestProducersForPaths_Publication(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"multiple owners", "publication failure", "empty replacement"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testProducersForPathsPublicationCase(t, &mode)
		},
		)
	}
}

type inventoryContent func(io.Writer) (int64, error)

func (p inventoryContent) WriteTo(output io.Writer) (int64, error) { return p(output) }

func TestRecordInventory_ConcurrentReuse(t *testing.T) {
	t.Parallel()
	persistent := RenderPersistenceDisabledReason() == ""
	root := t.TempDir()
	c := cachetest.New(t, root, New)
	var writes atomic.Int64
	inventory := AcceptedInventory[inventoryContent]{
		Key:        "shared",
		Acceptance: "accepted",
		History: RenderHistory{
			Owner: manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "shared"},
			Sources: []manifest.SourceEvidence{
				{Source: manifest.SourceIdentity{Root: "/input"}, Path: "apps/shared", Subtree: true},
			},
		},
		Content: inventoryContent(func(output io.Writer) (int64, error) {
			writes.Add(1)
			n, err := io.WriteString(output, "accepted")
			return int64(n), err
		}),
	}
	var failures [2]error
	var callers sync.WaitGroup
	for i := range failures {
		callers.Go(func() { failures[i] = RecordInventory(t.Context(), c, inventory) })
	}
	callers.Wait()
	for _, err := range failures {
		assert.NoError(t, err)
	}
	wantWrites := int64(0)
	if persistent {
		wantWrites = 1
	}
	assert.Equal(t, writes.Load(), wantWrites)
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	assert.NoError(t, RecordInventory(t.Context(), c, inventory))
	assert.Equal(t, writes.Load(), wantWrites)
	owners, err := c.ProducersForPaths(
		t.Context(),
		[]manifest.SourceEvidence{{Source: manifest.SourceIdentity{Root: "/input"}, Path: "apps/shared/cm.yaml"}},
	)
	assert.NoError(t, err)
	var want []manifest.NamedResource
	if persistent {
		want = []manifest.NamedResource{inventory.History.Owner}
	}
	assert.Diff(t, slices.SortedFunc(maps.Keys(owners), manifest.NamedResource.Compare), want)
}

//nolint:paralleltest // Allocation measurements use process-wide test accounting.
//nolint:paralleltest // Allocation measurements use process-wide test accounting.
func TestRecordInventory_WithoutPersistence(t *testing.T) {
	c := cachetest.New(t, "", New)
	writes := 0
	inventory := AcceptedInventory[inventoryContent]{
		Key:        "memory",
		Acceptance: "accepted",
		History: RenderHistory{
			Owner: manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "memory"},
			Sources: []manifest.SourceEvidence{
				{Source: manifest.SourceIdentity{Root: "/input"}, Path: "apps/shared", Subtree: true},
			},
		},
		Content: inventoryContent(func(io.Writer) (int64, error) { writes++; return 0, nil }),
	}
	ctx := t.Context()
	allocations := testing.AllocsPerRun(10, func() {
		assert.NoError(t, RecordInventory(ctx, c, inventory))
	})
	assert.Equal(t, allocations, float64(0))
	assert.Equal(t, writes, 0)
}

func TestRecordInventory_EquivalentHistoryOrder(t *testing.T) {
	t.Parallel()
	persistent := RenderPersistenceDisabledReason() == ""
	c := cachetest.New(t, t.TempDir(), New)
	source := manifest.SourceIdentity{Root: "/input"}
	first := manifest.SourceEvidence{Source: source, Path: "apps/a", Subtree: true}
	second := manifest.SourceEvidence{Source: source, Path: "apps/b", Subtree: true}
	writes := 0
	inventory := AcceptedInventory[inventoryContent]{
		Key:        "equivalent",
		Acceptance: "accepted",
		History: RenderHistory{
			Owner:   manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "equivalent"},
			Sources: []manifest.SourceEvidence{second, first},
		},
		Content: inventoryContent(func(output io.Writer) (int64, error) {
			writes++
			n, err := io.WriteString(output, "accepted")
			return int64(n), err
		}),
	}
	assert.NoError(t, RecordInventory(t.Context(), c, inventory))
	inventory.History.Sources = []manifest.SourceEvidence{first, second, second}
	assert.NoError(t, RecordInventory(t.Context(), c, inventory))
	wantWrites := 0
	if persistent {
		wantWrites = 1
	}
	assert.Equal(t, writes, wantWrites)
}

func testProducersForPathsUnavailableCase(t *testing.T, mode *string,
) {
	t.Helper()
	persistent := RenderPersistenceDisabledReason() == ""

	cache := cachetest.New(t, t.TempDir(), New)
	source := manifest.SourceIdentity{Root: "/input"}
	metadata, err := new(
		(RenderHistory{Owner: manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "emitter"}, Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}}}),
	).record("accepted").
		metadata()
	assert.NoError(t, err)
	if (*mode) == "foreign build" {
		metadata[historyAnnotation] = `{"Build":"foreign"}`
	}
	if (*mode) == "malformed" {
		metadata[historyAnnotation] = "{"
	}
	if (*mode) == "incomplete owner" {
		metadata, err = new(
			(RenderHistory{Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}}}),
		).record("accepted").
			metadata()
		assert.NoError(t, err)
	}
	_, _, lookup, err := cache.Blob(
		t.Context(),
		BlobKey{Content: "history-producer", Render: RenderAccepted},
		&Policy{},
		func(_ context.Context, output ProducerInput) (ProducerResult, error) {
			_, writeStringErr2 := io.WriteString(output, "accepted")
			metadata[acceptedRenderAnnotation] = "accepted"
			return ProducerResult{Annotations: metadata}, writeStringErr2
		},
	)
	assert.NoError(t, err)
	if (*mode) == "missing" {
		descriptor, resolveErr := cache.store.Resolve(t.Context(), lookup)
		if persistent {
			assert.NoError(t, resolveErr)
			assert.NoError(t, cache.root.Remove(blobPath(descriptor.Digest)))
		} else {
			assert.Equal(t, errors.Is(resolveErr, errdef.ErrNotFound), true)
		}
	}
	if (*mode) == "disabled" {
		cache.persistBlob = false
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*mode) == "canceled" {
		cancel()
	}
	owners, err := cache.ProducersForPaths(
		ctx,
		[]manifest.SourceEvidence{{Source: source, Path: "apps/blue/cm.yaml"}},
	)
	assert.Equal(t, errors.Is(err, context.Canceled), (*mode) == "canceled")
	assert.Equal(t, len(owners), 0)
	if (*mode) != "canceled" {
		assert.NoError(t, err)
	}
}

func testProducersForPathsAcceptanceCase(t *testing.T, mode *string,
) {
	t.Helper()
	persistent := RenderPersistenceDisabledReason() == ""

	cache := cachetest.New(t, t.TempDir(), New)
	source := manifest.SourceIdentity{Root: "/input"}
	owner := manifest.NamedResource{
		Group: manifest.HelmReleaseDomain,
		Kind:  manifest.KindHelmRelease,
		Name:  "emitter",
	}
	history := &RenderHistory{
		Owner:   owner,
		Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}},
	}
	key := BlobKey{Content: "history-producer", Render: RenderAccepted}
	if (*mode) == "pending" {
		key.Render = RenderPending
	}
	if (*mode) == "source" {
		key.Render = 0
	}
	metadata, err := new(*history).record("first").metadata()
	assert.NoError(t, err)
	metadata[acceptedRenderAnnotation] = "first"
	_, _, _, err = cache.Blob(
		t.Context(),
		key,
		&Policy{},
		func(_ context.Context, output ProducerInput) (ProducerResult, error) {
			_, writeStringErr3 := io.WriteString(output, "accepted")
			return ProducerResult{Annotations: metadata}, writeStringErr3
		},
	)
	assert.NoError(t, err)
	if (*mode) == "rejected" || (*mode) == "replacement" {
		err = RecordInventory(
			t.Context(),
			cache,
			AcceptedInventory[inventoryContent]{
				Key:        "history-producer",
				Acceptance: "second",
				History:    RenderHistory{Owner: owner, Sources: []manifest.SourceEvidence{}},
				Content: inventoryContent(func(output io.Writer) (int64, error) {
					if (*mode) == "rejected" {
						return 0, errors.New("rejected successor")
					}
					n, writeStringErr4 := io.WriteString(output, "empty accepted inventory")
					return int64(n), writeStringErr4
				}),
			},
		)
		assert.Equal(t, err != nil, persistent && (*mode) == "rejected")
	}
	owners, err := cache.ProducersForPaths(
		t.Context(),
		[]manifest.SourceEvidence{{Source: source, Path: "apps/blue/cm.yaml"}},
	)
	assert.NoError(t, err)
	var want []manifest.NamedResource
	if persistent && ((*mode) == "accepted" || (*mode) == "rejected") {
		want = []manifest.NamedResource{owner}
	}
	assert.Diff(t, slices.SortedFunc(maps.Keys(owners), manifest.NamedResource.Compare), want)
}

func testProducersForPathsPublicationCase(t *testing.T, mode *string,
) {
	t.Helper()
	persistent := RenderPersistenceDisabledReason() == ""

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	source := manifest.SourceIdentity{Root: "/input"}
	owners := []manifest.NamedResource{
		{Kind: manifest.KindHelmRelease, Name: "a"},
		{Kind: manifest.KindKustomization, Name: "b"},
	}
	publish := func(owner manifest.NamedResource, paths []manifest.SourceEvidence) error {
		return RecordInventory(
			t.Context(),
			c,
			AcceptedInventory[*strings.Reader]{
				Key:        owner.Name,
				Acceptance: owner.Name,
				History:    RenderHistory{Owner: owner, Sources: paths},
				Content:    strings.NewReader(owner.Name),
			},
		)
	}
	paths := []manifest.SourceEvidence{{Source: source, Path: "removed/old.yaml"}}
	for _, owner := range owners {
		assert.NoError(t, publish(owner, paths))
	}
	if (*mode) == "empty replacement" {
		assert.NoError(t, publish(owners[0], []manifest.SourceEvidence{}))
	}
	if (*mode) == "publication failure" {
		index := filepath.Join(root, indexPath())
		assert.NoError(t, os.Rename(index, index+".saved"))
		assert.NoError(t, os.Mkdir(index, 0o750))
		err := publish(owners[0], []manifest.SourceEvidence{})
		assert.Equal(t, err != nil, persistent)
		assert.NoError(t, os.Remove(index))
		assert.NoError(t, os.Rename(index+".saved", index))
	}

	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	got, err := c.ProducersForPaths(
		t.Context(),
		[]manifest.SourceEvidence{
			{Source: source, Path: "moved/new.yaml"},
			{Source: source, Path: "removed/old.yaml"},
			{Source: source, Path: "removed/old.yaml"},
		},
	)
	assert.NoError(t, err)
	var want []manifest.NamedResource
	if persistent {
		want = owners
	}
	if (*mode) == "empty replacement" && persistent {
		want = owners[1:]
	}
	assert.Diff(t, slices.SortedFunc(maps.Keys(got), manifest.NamedResource.Compare), want)
}
