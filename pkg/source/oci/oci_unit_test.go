package oci

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCachedDigest_Roundtrip covers writeCachedDigest + readCachedDigest:
// the digest written by one survives a re-read on cache hit.
func TestCachedDigest_Roundtrip(t *testing.T) {
	slot := t.TempDir()
	if err := writeCachedDigest(slot, "sha256:abc123"); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := readCachedDigest(slot)
	if got != "sha256:abc123" {
		t.Errorf("readCachedDigest = %q, want sha256:abc123", got)
	}
}

// TestReadCachedDigest_MissingReturnsEmpty pins the cache-miss path:
// no .flate-digest file → empty string (signals "no cached digest").
func TestReadCachedDigest_MissingReturnsEmpty(t *testing.T) {
	if got := readCachedDigest(t.TempDir()); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

// TestReadCachedDigest_TrimsTrailingNewline covers the trim path —
// an editor adding a trailing newline shouldn't break cache matching.
func TestReadCachedDigest_TrimsTrailingNewline(t *testing.T) {
	slot := t.TempDir()
	if err := os.WriteFile(filepath.Join(slot, cachedDigestFile), []byte("sha256:abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := readCachedDigest(slot); got != "sha256:abc" {
		t.Errorf("got %q, want trimmed digest", got)
	}
}

// TestLoadCredentials_ValidJSONLoads covers the happy path with a
// minimal docker config.
func TestLoadCredentials_ValidJSONLoads(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	if err := os.WriteFile(config, []byte(`{"auths":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := loadCredentials(config)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if store == nil {
		t.Error("expected non-nil store for valid config")
	}
}

// TestLoadCredentials_EmptyPathFallsBackToDocker covers the
// docker-default lookup arm. Either it succeeds or it gracefully
// returns (nil, nil) when no default is configured — never errors.
func TestLoadCredentials_EmptyPathFallsBackToDocker(t *testing.T) {
	_, err := loadCredentials("")
	if err != nil {
		t.Errorf("empty path should never error; got %v", err)
	}
}

// TestDescriptorFromLayer copies fields verbatim — sanity check the
// mapping shape so a future field-add to signatureLayer doesn't
// silently drop something cosign needs.
func TestDescriptorFromLayer(t *testing.T) {
	l := signatureLayer{
		MediaType: "application/vnd.dev.cosign.simplesigning.v1+json",
		Digest:    "sha256:beef",
		Size:      1024,
	}
	d := descriptorFromLayer(l)
	if d.MediaType != l.MediaType || string(d.Digest) != l.Digest || d.Size != l.Size {
		t.Errorf("descriptor lost fields: %+v from %+v", d, l)
	}
}

// TestSignatureManifestRoundtrip pins the JSON shape — cosign
// signature manifests must unmarshal cleanly into signatureManifest.
func TestSignatureManifestRoundtrip(t *testing.T) {
	raw := `{"layers":[{"mediaType":"application/vnd.dev.cosign.simplesigning.v1+json","digest":"sha256:abc","size":42,"annotations":{"dev.cosignproject.cosign/signature":"MEUC..."}}]}`
	var m signatureManifest
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(m.Layers) != 1 || !strings.Contains(m.Layers[0].Digest, "sha256:") {
		t.Errorf("unexpected layers: %+v", m.Layers)
	}
	if m.Layers[0].Annotations["dev.cosignproject.cosign/signature"] == "" {
		t.Error("annotation roundtrip lost the signature key")
	}
}
