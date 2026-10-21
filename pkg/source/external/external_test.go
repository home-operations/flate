package external_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/external"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_FileURL(t *testing.T) {
	t.Parallel()
	for _, metadata := range []string{"directory", "gitfile"} {
		t.Run(metadata, func(t *testing.T) {
			t.Parallel()
			f := &external.Fetcher{}
			root := t.TempDir()
			testutil.WriteFile(t, root, "content.yaml", "from FS")
			if metadata == "directory" {
				testutil.WriteFile(t, root, ".git/config", "metadata")
				testutil.WriteFile(t, root, "nested/.git/config", "metadata")
			} else {
				testutil.WriteFile(t, root, ".git", "gitdir: missing\n")
				testutil.WriteFile(t, root, "nested/.git", "gitdir: missing\n")
			}
			label := filepath.Join(t.TempDir(), "input")
			if err := os.Symlink(root, label); err != nil {
				t.Fatal(err)
			}
			ea := &manifest.ExternalArtifact{
				Name: "ea", Namespace: "apps", ArtifactURL: "file://" + label,
				Revision: "v1@sha256:abc", Digest: "sha256:abc",
			}
			art, err := f.Fetch(t.Context(), ea, sourcecache.Policy{})
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if art.Kind != manifest.KindExternalArtifact || art.URL != ea.ArtifactURL || art.LocalPath != label || art.Revision != ea.Revision || art.Digest != ea.Digest {
				t.Fatalf("artifact changed authored coordinates: %+v", art)
			}
			if art.FS == nil || art.Key != "" {
				t.Fatal("file URL must admit mutable content even with a digest and revision")
			}
			data, err := fs.ReadFile(art.FS, "content.yaml")
			if err != nil || string(data) != "from FS" {
				t.Fatalf("admitted read = %q, %v", data, err)
			}
			for _, name := range []string{".git", ".git/config", "nested/.git", "nested/.git/config"} {
				if _, err := fs.Stat(art.FS, name); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("Stat(%q) = %v", name, err)
				}
				if file, err := art.FS.Open(name); !errors.Is(err, fs.ErrNotExist) {
					if file != nil {
						file.Close()
					}
					t.Fatalf("Open(%q) = %v", name, err)
				}
				if _, err := fs.ReadFile(art.FS, name); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("ReadFile(%q) = %v", name, err)
				}
			}
			for _, name := range []string{".", "nested"} {
				entries, err := fs.ReadDir(art.FS, name)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if entry.Name() == ".git" {
						t.Fatalf("ReadDir(%q) exposes Git metadata", name)
					}
				}
			}
		})
	}
}

func TestFetcher_NoArtifact(t *testing.T) {
	t.Parallel()
	for _, predicted := range []bool{false, true} {
		t.Run(fmt.Sprint(predicted), func(t *testing.T) {
			t.Parallel()
			f := &external.Fetcher{}
			ea := &manifest.ExternalArtifact{Name: "ea", Namespace: "apps"}
			s := store.New()
			if predicted {
				owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
				s.AddObject(owner)
				generator := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps", Spec: map[string]any{"artifacts": []any{map[string]any{"name": ea.Name}}}}
				s.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: generator}}, Complete: true})
				if len(s.Productions(ea.Named(), nil)) != 1 {
					t.Fatal("missing producer evidence")
				}
			}
			if s.GetObject(ea.Named()) != nil || s.GetArtifact(ea.Named()) != nil {
				t.Fatal("prediction supplied source content")
			}
			_, err := f.Fetch(t.Context(), ea, sourcecache.Policy{})
			if err == nil || !strings.Contains(err.Error(), "offline use") {
				t.Fatalf("missing-content error = %v", err)
			}
		})
	}
}

func TestFetcher_NonFileURL(t *testing.T) {
	t.Parallel()
	f := &external.Fetcher{}
	ea := &manifest.ExternalArtifact{
		Name: "ea", Namespace: "apps",
		ArtifactURL: "http://source-controller.flux-system.svc/x.tar.gz",
	}
	_, err := f.Fetch(context.Background(), ea, sourcecache.Policy{})
	if err == nil {
		t.Fatalf("expected error for non-file:// URL")
	}
	if !strings.Contains(err.Error(), "not a file://") {
		t.Errorf("error should call out the URL scheme; got %v", err)
	}
}
