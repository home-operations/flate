package external_test

import (
	"context"
	"io/fs"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/external"
)

func TestFetcher_FileURL(t *testing.T) {
	f := &external.Fetcher{}
	root := t.TempDir()
	testutil.WriteFile(t, root, "content.yaml", "from FS")
	ea := &manifest.ExternalArtifact{
		Name: "ea", Namespace: "apps",
		ArtifactURL: "file://" + root,
		Revision:    "v1@sha256:abc",
		Digest:      "sha256:abc",
	}
	art, err := f.Fetch(context.Background(), ea)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art.Kind != manifest.KindExternalArtifact {
		t.Errorf("Kind = %q, want %q", art.Kind, manifest.KindExternalArtifact)
	}
	if art.LocalPath != root {
		t.Errorf("LocalPath = %q, want admitted root", art.LocalPath)
	}
	if art.FS == nil || art.Key != "" {
		t.Fatal("file URL must admit mutable content even with a digest and revision")
	}
	data, err := fs.ReadFile(art.FS, "content.yaml")
	if err != nil || string(data) != "from FS" {
		t.Fatalf("admitted read = %q, %v", data, err)
	}
	if art.Revision != "v1@sha256:abc" {
		t.Errorf("Revision = %q", art.Revision)
	}
}

func TestFetcher_NoArtifact(t *testing.T) {
	f := &external.Fetcher{}
	ea := &manifest.ExternalArtifact{Name: "ea", Namespace: "apps"}
	_, err := f.Fetch(context.Background(), ea)
	if err == nil {
		t.Fatalf("expected error when ArtifactURL is empty")
	}
	if !strings.Contains(err.Error(), "offline use") {
		t.Errorf("error should explain the offline limitation; got %v", err)
	}
}

func TestFetcher_NonFileURL(t *testing.T) {
	f := &external.Fetcher{}
	ea := &manifest.ExternalArtifact{
		Name: "ea", Namespace: "apps",
		ArtifactURL: "http://source-controller.flux-system.svc/x.tar.gz",
	}
	_, err := f.Fetch(context.Background(), ea)
	if err == nil {
		t.Fatalf("expected error for non-file:// URL")
	}
	if !strings.Contains(err.Error(), "not a file://") {
		t.Errorf("error should call out the URL scheme; got %v", err)
	}
}
