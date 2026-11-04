package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestProducersForPaths_SourcePaths(t *testing.T) {
	t.Parallel()
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
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
			if tc.found {
				want = []manifest.NamedResource{owner}
			}
			assert.Diff(t, owners, want)
		})
	}
}

func TestProducersForPaths_Unavailable(t *testing.T) {
	t.Parallel()
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
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
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
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
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
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
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
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
	assert.Equal(t, writes.Load(), int64(1))
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	assert.NoError(t, RecordInventory(t.Context(), c, inventory))
	assert.Equal(t, writes.Load(), int64(1))
	owners, err := c.ProducersForPaths(
		t.Context(),
		[]manifest.SourceEvidence{{Source: manifest.SourceIdentity{Root: "/input"}, Path: "apps/shared/cm.yaml"}},
	)
	assert.NoError(t, err)
	assert.Diff(t, owners, []manifest.NamedResource{inventory.History.Owner})
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
	if reason := RenderPersistenceDisabledReason(); reason != "" {
		t.Skip(reason)
	}
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
	assert.FatalIf(t, writes != 1, "equivalent canonical history published %d times, want 1", writes)
}

func testProducersForPathsUnavailableCase(t *testing.T, mode *string,
) {
	t.Helper()

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
		assert.NoError(t, resolveErr)
		assert.NoError(t, cache.root.Remove(blobPath(descriptor.Digest)))
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
		assert.Equal(t, err != nil, (*mode) == "rejected")
	}
	owners, err := cache.ProducersForPaths(
		t.Context(),
		[]manifest.SourceEvidence{{Source: source, Path: "apps/blue/cm.yaml"}},
	)
	assert.NoError(t, err)
	var want []manifest.NamedResource
	if (*mode) == "accepted" || (*mode) == "rejected" {
		want = []manifest.NamedResource{owner}
	}
	assert.Diff(t, owners, want)
}

func testProducersForPathsPublicationCase(t *testing.T, mode *string,
) {
	t.Helper()

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
		{
			err := publish(owners[0], []manifest.SourceEvidence{})
			assert.FatalIf(t, err == nil, "%s", "failed publication supplied accepted history")
		}

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
	want := owners
	if (*mode) == "empty replacement" {
		want = owners[1:]
	}
	assert.Diff(t, got, want)
}
