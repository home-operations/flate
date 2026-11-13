package loader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestResolveRoot_ConfinedCapturedSource(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, want string
		available        bool
	}{
		{name: "directory", path: "inputs", want: "inputs", available: true},
		{name: "source root", path: ".", want: ".", available: true},
		{name: "confined symlink", path: "link", want: "inputs", available: true},
		{name: "escaped root", path: "../outside"},
		{name: "escaped symlink", path: "escape"},
		{name: "absolute root", path: "/inputs"},
		{name: "remote root", path: "https://fixture.invalid/inputs"},
		{name: "missing root", path: "absent"},
		{name: "file root", path: "inputs/file"},
		{name: "unavailable source", path: "inputs"},
		{name: "replaced source", path: "inputs"},
		{name: "canceled", path: "inputs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			base := t.TempDir()
			root := filepath.Join(base, "source")
			testutil.WriteFile(t, root, "inputs/file", "captured")
			testutil.WriteFile(t, base, "outside/file", "excluded")
			assert.NoError(t, os.Symlink("inputs", filepath.Join(root, "link")))
			assert.NoError(t, os.Symlink("../outside", filepath.Join(root, "escape")))
			st := store.New()
			source := (&manifest.GitRepository{Name: "source", Namespace: "apps"}).Named()
			input := &store.SourceArtifact{LocalPath: root, Content: "captured", FS: os.DirFS(root)}
			if tc.name != "unavailable source" {
				st.SetArtifact(source, input)
			}
			var reads store.ReadSet
			reads.Init(st, manifest.NamedResource{}, nil)
			defer reads.Retire()
			reads.GetArtifact(source, store.ArtifactContent|store.ArtifactRevision)
			if tc.name == "replaced source" {
				replacement := *input
				replacement.Content = "replacement"
				st.SetArtifact(source, &replacement)
				assert.Equal(t, reads.Valid(), false)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.name == "canceled" {
				cancel()
			}
			resolved, available, err := New(st).ResolveRoot(ctx, &reads, source, tc.path)
			if tc.name == "canceled" {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, available, tc.available)
			if tc.available {
				assert.Equal(t, resolved, tc.want)
			}
			assert.Equal(
				t,
				st.GetArtifact(source, store.ArtifactContent) == input,
				tc.name != "unavailable source" && tc.name != "replaced source",
			)
		})
	}
}
