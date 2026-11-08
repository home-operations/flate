package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestBuildIdentity_Stable(t *testing.T) {
	t.Parallel()
	info := &debug.BuildInfo{
		Main: debug.Module{Path: flateModule, Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "revision-a"},
			{Key: "GOOS", Value: "linux"},
		},
	}
	original := info.String()
	want := manifest.Fingerprint(original)
	for range 2 {
		stamp := buildIdentity(info)

		assert.Equal(t, stamp.identity, want)
		assert.Equal(t, stamp.reason, "")
	}
	assert.Equal(t, info.String(), original)
}

func TestBuildIdentity_Dirty(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		info   *debug.BuildInfo
		reason string
	}{
		{"dirty", &debug.BuildInfo{
			Main: debug.Module{Path: flateModule, Version: "v0.6.5"},
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "revision-a"},
				{Key: "vcs.modified", Value: "true"},
			},
		}, "the binary was built from a dirty checkout"},
		{"nil metadata", nil, "build metadata is unavailable"},
		{"unversioned", &debug.BuildInfo{Main: debug.Module{Path: flateModule, Version: "(devel)"}}, "the binary has no stamped build identity"},
		{"local replacement", &debug.BuildInfo{
			Main: debug.Module{Path: flateModule, Version: "v0.6.5"},
			Deps: []*debug.Module{{Path: "example.com/module", Replace: &debug.Module{Path: "../local-module"}}},
		}, "a dependency replacement has no version"},
		{"replacement before dirty", &debug.BuildInfo{
			Main:     debug.Module{Path: flateModule, Version: "v0.6.5"},
			Deps:     []*debug.Module{{Path: "example.com/module", Replace: &debug.Module{Path: "../local-module"}}},
			Settings: []debug.BuildSetting{{Key: "vcs.modified", Value: "true"}},
		}, "a dependency replacement has no version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stamp := buildIdentity(tc.info)

			assert.Equal(t, stamp.identity, "")
			assert.Equal(t, stamp.reason, tc.reason)
		})
	}
}

//nolint:paralleltest // The test changes the process build identity and restores it afterward.
func TestBlob_RenderBuildIdentity(t *testing.T) {
	original := processIdentity
	t.Cleanup(func() { processIdentity = original })
	info := &debug.BuildInfo{
		Main:     debug.Module{Path: flateModule, Version: "(devel)"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "revision-a"}},
	}
	stamp := buildIdentity(info)

	info.Settings[0].Value = "revision-b"
	stamp2 := buildIdentity(info)

	assert.Equal(t, stamp.identity == "" || stamp2.identity == "" || stamp.identity == stamp2.identity, false)
	root := t.TempDir()
	processIdentity = stamp.identity
	c := cachetest.New(t, root, New)
	assert.Equal(t, c.CanPersistRenders(), true)
	processIdentity = ""
	assert.Equal(t, c.CanPersistRenders(), true)
	processIdentity = stamp.identity
	key := BlobKey{Content: "render-input", Render: RenderAccepted}
	_, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: key.Content},
		&Policy{MaxAge: 0},
		producer([]byte("ordinary"), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)

	for _, identity := range []string{stamp.identity, stamp2.identity} {
		{
			closeErr := c.Close()
			assert.NoError(t, closeErr)
		}

		processIdentity = identity
		key.Content = "render-input"
		c = cachetest.New(t, root, New)
		assert.Equal(t, c.CanPersistRenders(), true)
		{
			r, _, _, blobErr := c.Blob(
				t.Context(),
				key,
				&Policy{MaxAge: Forever},
				nil,
			)
			assert.FatalIf(t, r != nil ||
				!errors.Is(blobErr, errdef.ErrNotFound), "foreign or ordinary identity hit: %v", blobErr)
		}
		_, _, id, blobErr2 := c.Blob(
			t.Context(),
			key,
			&Policy{MaxAge: 0},
			producer([]byte(identity), &ocispec.Descriptor{}),
		)
		assert.NoError(t, blobErr2)

		assert.Equal(t, id, blobID(identity, key.Content.(string), key.Render))
	}
	for _, identity := range []string{stamp.identity, stamp2.identity} {
		{
			closeErr2 := c.Close()
			assert.NoError(t, closeErr2)
		}

		processIdentity = identity
		key.Content = "render-input"
		c = cachetest.New(t, root, New)
		assert.Equal(t, c.CanPersistRenders(), true)
		r, _, _, blobErr3 := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
		assert.NoError(t, blobErr3)

		data, blobErr3 := fs.ReadFile(r, "content")
		assert.NoError(t, blobErr3)

		assert.Equal(t, string(data), identity)
	}
	{
		closeErr3 := c.Close()
		assert.NoError(t, closeErr3)
	}

	processIdentity = ""
	key.Render = RenderPending
	c = cachetest.New(t, root, New)
	assert.Equal(t, c.CanPersistRenders(), false)
	processIdentity = stamp.identity
	assert.Equal(t, c.CanPersistRenders(), false)
	processIdentity = ""
	index := filepath.Join(c.root.Name(), indexPath())
	before, err := os.ReadFile(index)
	assert.NoError(t, err)

	r, _, _, err := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, r != nil || !errors.Is(err, errdef.ErrNotFound), "unidentified cold lookup: %v", err)
	for _, produce := range []func(context.Context, ProducerInput) (ProducerResult, error){producer([]byte("DIRTY"), &ocispec.Descriptor{}), nil} {
		blobR, _, _, blobErr4 := c.Blob(t.Context(), key, &Policy{MaxAge: Forever}, produce)
		assert.NoError(t, blobErr4)
		data, _ := fs.ReadFile(blobR, "content")

		assert.Equal(t, string(data), "DIRTY")
	}

	after, err := os.ReadFile(index)
	assert.NoError(t, err)
	assert.FatalIf(t, !bytes.Equal(before, after), "%s", "dirty output changed persistent tags")

	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	{
		blobR2, _, _, blobErr5 := c.Blob(
			t.Context(),
			key,
			&Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, blobR2 != nil ||
			!errors.Is(blobErr5, errdef.ErrNotFound), "unidentified render survived reopen: %v", blobErr5)
	}

	r, _, _, err = c.Blob(t.Context(), BlobKey{Content: key.Content}, &Policy{MaxAge: Forever}, nil)
	assert.NoError(t, err)

	data, _ := fs.ReadFile(r, "content")

	assert.Equal(t, string(data), "ordinary")
}

func TestBuildIdentity_Dependency(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
	}{
		{"main version", &debug.BuildInfo{Main: debug.Module{Path: flateModule, Version: "v0.6.5"}}},
		{"SDK dependency", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/sdk", Version: "(devel)"},
			Deps: []*debug.Module{{Path: flateModule, Version: "v0.6.5"}},
		}},
		{"updated SDK dependency", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/sdk", Version: "(devel)"},
			Deps: []*debug.Module{{Path: flateModule, Version: "v0.6.6"}},
		}},
		{"versioned replacement", &debug.BuildInfo{
			Main: debug.Module{Path: "example.com/sdk", Version: "(devel)"},
			Deps: []*debug.Module{{Path: flateModule, Version: "v0.6.6", Replace: &debug.Module{Path: "example.com/fork", Version: "v0.6.7"}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			stamp := buildIdentity(tc.info)

			assert.Equal(t, stamp.identity, manifest.Fingerprint(tc.info.String()))
			assert.Equal(t, stamp.reason, "")
		})
	}
}
