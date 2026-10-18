package cache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
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
		assert.Equal(t, buildIdentity(info), want)
	}
	assert.Equal(t, info.String(), original)
}

func TestBuildIdentity_Dirty(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
	}{
		{"dirty", &debug.BuildInfo{
			Main: debug.Module{Path: flateModule, Version: "v0.6.5"},
			Settings: []debug.BuildSetting{
				{Key: "vcs.revision", Value: "revision-a"},
				{Key: "vcs.modified", Value: "true"},
			},
		}},
		{"nil metadata", nil},
		{"unversioned", &debug.BuildInfo{Main: debug.Module{Path: flateModule, Version: "(devel)"}}},
		{"local replacement", &debug.BuildInfo{
			Main: debug.Module{Path: flateModule, Version: "v0.6.5"},
			Deps: []*debug.Module{{Path: "example.com/module", Replace: &debug.Module{Path: "../local-module"}}},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, buildIdentity(tc.info), "")
		})
	}
}

func TestBlob_PersistOnlyBuildIdentity(t *testing.T) {
	original := processIdentity
	t.Cleanup(func() { processIdentity = original })
	info := &debug.BuildInfo{
		Main:     debug.Module{Path: flateModule, Version: "(devel)"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "revision-a"}},
	}
	identityA := buildIdentity(info)
	info.Settings[0].Value = "revision-b"
	identityB := buildIdentity(info)
	assert.Equal(t, identityA == "" || identityB == "" || identityA == identityB, false)
	root := t.TempDir()
	processIdentity = identityA
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "render-input", PersistOnly: true}
	r, _, _, err := c.Blob(t.Context(), BlobKey{Content: key.Content}, Policy{MaxAge: 0}, producer([]byte("ordinary"), ocispec.Descriptor{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	for _, identity := range []string{identityA, identityB} {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		processIdentity = identity
		c = cachetest.New(t, root, New)
		if r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil); r != nil || !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("foreign or ordinary identity hit: %v", err)
		}
		r, _, id, err := c.Blob(t.Context(), key, Policy{MaxAge: 0}, producer([]byte(identity), ocispec.Descriptor{}))
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, id, blobID(identity, key.Content.(string)))
	}
	for _, identity := range []string{identityA, identityB} {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		processIdentity = identity
		c = cachetest.New(t, root, New)
		r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if closeErr := r.Close(); err != nil || closeErr != nil {
			t.Fatalf("read: %v, close: %v", err, closeErr)
		}
		assert.Equal(t, string(data), identity)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	processIdentity = ""
	c = cachetest.New(t, root, New)
	index := filepath.Join(c.root.Name(), indexPath())
	before, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, produce := range []func(context.Context, io.Writer) (ocispec.Descriptor, error){nil, producer([]byte("DIRTY"), ocispec.Descriptor{}), nil} {
		r, _, _, err := c.Blob(t.Context(), key, Policy{MaxAge: Forever}, produce)
		if produce == nil {
			if r != nil || !errors.Is(err, errdef.ErrNotFound) {
				t.Fatalf("dirty lookup hit: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		if closeErr := r.Close(); err != nil || closeErr != nil {
			t.Fatalf("read: %v, close: %v", err, closeErr)
		}
		assert.Equal(t, string(data), "")
	}
	c.entries.Range(func(_, _ any) bool { t.Error("dirty output installed an entry"); return true })
	after, err := os.ReadFile(index)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("dirty output changed persistent tags")
	}
}

func TestBuildIdentity_Dependency(t *testing.T) {
	t.Parallel()
	info := &debug.BuildInfo{
		Main: debug.Module{Path: "example.com/sdk", Version: "(devel)"},
		Deps: []*debug.Module{{Path: flateModule, Version: "v0.6.5"}},
	}
	a := buildIdentity(info)
	if a == "" {
		t.Fatal("versioned SDK dependency is ineligible")
	}
	info.Deps[0].Version = "v0.6.6"
	if b := buildIdentity(info); b == "" || a == b {
		t.Fatal("dependency version did not change identity")
	}
	info.Deps[0].Replace = &debug.Module{Path: "example.com/fork", Version: "v0.6.7"}
	if b := buildIdentity(info); b == "" || a == b {
		t.Fatal("versioned replacement did not change identity")
	}
}
