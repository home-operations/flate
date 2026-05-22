package external_test

import (
	"context"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/external"
)

func TestFetcher_FileURL(t *testing.T) {
	f := &external.Fetcher{}
	ea := &manifest.ExternalArtifact{
		Name: "ea", Namespace: "apps",
		ArtifactURL: "file:///cache/x.tar.gz",
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
	if art.LocalPath != "/cache/x.tar.gz" {
		t.Errorf("LocalPath = %q, want /cache/x.tar.gz", art.LocalPath)
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
