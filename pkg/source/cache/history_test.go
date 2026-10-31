package cache

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	owner := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "emitter"}
	source := manifest.SourceIdentity{Kind: manifest.KindGitRepository, URL: "https://fixture.invalid/one", Key: "family"}
	history := &RenderHistory{Owner: owner, Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}, {Source: source, Path: "shared/values.yaml"}}}
	metadata, err := history.metadata("accepted")
	assert.NoError(t, err)
	_, _, _, err = cache.Blob(t.Context(), BlobKey{Content: "history-producer", Render: RenderAccepted}, Policy{}, func(_ context.Context, output ProducerInput) (ProducerResult, error) {
		_, err := io.WriteString(output, "accepted")
		metadata[acceptedRenderAnnotation] = "accepted"
		return ProducerResult{Annotations: metadata}, err
	})
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
			owners, err := cache.ProducersForPaths(t.Context(), []manifest.SourceEvidence{{Source: tc.source, Path: tc.path}})
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
			cache := cachetest.New(t, t.TempDir(), New)
			source := manifest.SourceIdentity{Root: "/input"}
			metadata, err := (RenderHistory{Owner: manifest.NamedResource{Kind: manifest.KindHelmRelease, Name: "emitter"}, Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}}}).metadata("accepted")
			assert.NoError(t, err)
			if mode == "foreign build" {
				metadata[historyAnnotation] = `{"Build":"foreign"}`
			}
			if mode == "malformed" {
				metadata[historyAnnotation] = "{"
			}
			if mode == "incomplete owner" {
				metadata, err = (RenderHistory{Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}}}).metadata("accepted")
				assert.NoError(t, err)
			}
			_, _, lookup, err := cache.Blob(t.Context(), BlobKey{Content: "history-producer", Render: RenderAccepted}, Policy{}, func(_ context.Context, output ProducerInput) (ProducerResult, error) {
				_, err := io.WriteString(output, "accepted")
				metadata[acceptedRenderAnnotation] = "accepted"
				return ProducerResult{Annotations: metadata}, err
			})
			assert.NoError(t, err)
			if mode == "missing" {
				descriptor, err := cache.store.Resolve(t.Context(), lookup)
				assert.NoError(t, err)
				assert.NoError(t, cache.root.Remove(blobPath(descriptor.Digest)))
			}
			if mode == "disabled" {
				cache.persistBlob = false
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			owners, err := cache.ProducersForPaths(ctx, []manifest.SourceEvidence{{Source: source, Path: "apps/blue/cm.yaml"}})
			assert.Equal(t, errors.Is(err, context.Canceled), mode == "canceled")
			assert.Equal(t, len(owners), 0)
			if mode != "canceled" {
				assert.NoError(t, err)
			}
		})
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
			cache := cachetest.New(t, t.TempDir(), New)
			source := manifest.SourceIdentity{Root: "/input"}
			owner := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Name: "emitter"}
			history := &RenderHistory{Owner: owner, Sources: []manifest.SourceEvidence{{Source: source, Path: "apps/blue", Subtree: true}}}
			key := BlobKey{Content: "history-producer", Render: RenderAccepted}
			if mode == "pending" {
				key.Render = RenderPending
			}
			if mode == "source" {
				key.Render = 0
			}
			metadata, err := history.metadata("first")
			assert.NoError(t, err)
			metadata[acceptedRenderAnnotation] = "first"
			_, _, _, err = cache.Blob(t.Context(), key, Policy{}, func(_ context.Context, output ProducerInput) (ProducerResult, error) {
				_, err := io.WriteString(output, "accepted")
				return ProducerResult{Annotations: metadata}, err
			})
			assert.NoError(t, err)
			if mode == "rejected" || mode == "replacement" {
				err = RecordInventory(t.Context(), cache, AcceptedInventory[inventoryContent]{Key: "history-producer", Acceptance: "second", History: RenderHistory{Owner: owner, Sources: []manifest.SourceEvidence{}}, Content: inventoryContent(func(output io.Writer) (int64, error) {
					if mode == "rejected" {
						return 0, errors.New("rejected successor")
					}
					n, err := io.WriteString(output, "empty accepted inventory")
					return int64(n), err
				})})
				assert.Equal(t, err != nil, mode == "rejected")
			}
			owners, err := cache.ProducersForPaths(t.Context(), []manifest.SourceEvidence{{Source: source, Path: "apps/blue/cm.yaml"}})
			assert.NoError(t, err)
			var want []manifest.NamedResource
			if mode == "accepted" || mode == "rejected" {
				want = []manifest.NamedResource{owner}
			}
			assert.Diff(t, owners, want)
		})
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
			root := t.TempDir()
			c := cachetest.New(t, root, New)
			source := manifest.SourceIdentity{Root: "/input"}
			owners := []manifest.NamedResource{{Kind: manifest.KindHelmRelease, Name: "a"}, {Kind: manifest.KindKustomization, Name: "b"}}
			publish := func(owner manifest.NamedResource, paths []manifest.SourceEvidence) error {
				return RecordInventory(t.Context(), c, AcceptedInventory[*strings.Reader]{Key: owner.Name, Acceptance: owner.Name, History: RenderHistory{Owner: owner, Sources: paths}, Content: strings.NewReader(owner.Name)})
			}
			paths := []manifest.SourceEvidence{{Source: source, Path: "removed/old.yaml"}}
			for _, owner := range owners {
				assert.NoError(t, publish(owner, paths))
			}
			if mode == "empty replacement" {
				assert.NoError(t, publish(owners[0], []manifest.SourceEvidence{}))
			}
			if mode == "publication failure" {
				index := filepath.Join(root, indexPath())
				assert.NoError(t, os.Rename(index, index+".saved"))
				assert.NoError(t, os.Mkdir(index, 0o750))
				if err := publish(owners[0], []manifest.SourceEvidence{}); err == nil {
					t.Fatal("failed publication supplied accepted history")
				}
				assert.NoError(t, os.Remove(index))
				assert.NoError(t, os.Rename(index+".saved", index))
			}
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
			got, err := c.ProducersForPaths(t.Context(), []manifest.SourceEvidence{{Source: source, Path: "moved/new.yaml"}, {Source: source, Path: "removed/old.yaml"}, {Source: source, Path: "removed/old.yaml"}})
			assert.NoError(t, err)
			want := owners
			if mode == "empty replacement" {
				want = owners[1:]
			}
			assert.Diff(t, got, want)
		})
	}
}

type inventoryContent func(io.Writer) (int64, error)

func (p inventoryContent) WriteTo(output io.Writer) (int64, error) { return p(output) }
