package diskcache

import (
	"crypto/sha256"
	"encoding/hex"
	"runtime/debug"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestBuildIdentity_Stable(t *testing.T) {
	info := &debug.BuildInfo{
		Main: debug.Module{Path: flateModule, Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "revision-a"},
			{Key: "GOOS", Value: "linux"},
		},
	}
	original := info.String()
	digest := sha256.Sum256([]byte(original))
	want := hex.EncodeToString(digest[:])
	for range 2 {
		assert.Equal(t, buildIdentity(info), want)
	}
	assert.Equal(t, info.String(), original)
}

func TestBuildIdentity_Dirty(t *testing.T) {
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
			assert.Equal(t, buildIdentity(tc.info), "")
		})
	}
}

func TestBuildIdentity_StoreIsolation(t *testing.T) {
	info := &debug.BuildInfo{
		Main:     debug.Module{Path: flateModule, Version: "(devel)"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "revision-a"}},
	}
	identityA := buildIdentity(info)
	info.Settings[0].Value = "revision-b"
	identityB := buildIdentity(info)
	assert.Equal(t, identityA == "" || identityB == "" || identityA == identityB, false)
	digestA := sha256.Sum256([]byte(identityA + ":same render input"))
	digestB := sha256.Sum256([]byte(identityB + ":same render input"))
	keyA, keyB := hex.EncodeToString(digestA[:]), hex.EncodeToString(digestB[:])
	root := t.TempDir()
	a, _ := NewStore(root, 1<<30)
	b, _ := NewStore(root, 1<<30)
	a.Put(keyA, []byte("A OUTPUT"))
	_, hit := b.Get(keyB)
	assert.Equal(t, hit, false)
	b.Put(keyB, []byte("B OUTPUT"))
	outA, hitA := a.Get(keyA)
	outB, hitB := b.Get(keyB)
	assert.Equal(t, hitA && hitB, true)
	assert.Equal(t, string(outA), "A OUTPUT")
	assert.Equal(t, string(outB), "B OUTPUT")
}
